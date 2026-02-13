## High-level file I/O API built on uring_bridge.
##
## Provides readFile/writeFile convenience procs that handle
## the file descriptor lifecycle (open, read/write, fsync, close) automatically.

import std/[posix, oserrors, monotimes, times]

import ./uring_bridge
import ./uring_raw

export uring_bridge

type Operation = enum
  close = "close"
  fsync = "fsync"
  open = "open"
  read = "read"
  statx = "statx"
  write = "write"

const ECANCELED = 125'i32

template raiseOnError(res: int32, op: Operation) =
  ## Raise CancelledError for -ECANCELED, IOError for other negative results.
  if res < 0:
    if res == -ECANCELED:
      raise (ref CancelledError)(msg: $op & " cancelled")
    raise newException(IOError, $op & " failed: " & osErrorMsg(OSErrorCode(-res)))

proc awaitOrTimeout(
    u: UringFileIO, fut: Future[int32], deadline: MonoTime, op: Operation
): Future[int32] {.async.} =
  ## Await a bridge-level Future with a deadline. If the deadline passes before
  ## `fut` completes, cancel the underlying io_uring operation and raise TimeoutError.
  let remaining = deadline - getMonoTime()
  let ms = remaining.inMilliseconds
  if ms <= 0:
    # Already past deadline — cancel immediately
    var cancelled = false
    try:
      discard await uringCancel(u, fut)
      cancelled = true
    except IOError:
      discard
    # Only drain if cancel succeeded; otherwise the operation may block indefinitely
    if cancelled:
      try:
        discard await fut
      except CatchableError:
        discard
    raise newException(TimeoutError, $op & " timed out")

  let timer = sleepMsAsync(int(ms))
  await fut or timer
  if fut.finished:
    cancelTimer(timer)
    return await fut
  else:
    # Timer fired first — cancel the operation
    var cancelled = false
    try:
      discard await uringCancel(u, fut)
      cancelled = true
    except IOError:
      discard
    # Only drain if cancel succeeded; otherwise the operation may block indefinitely
    if cancelled:
      try:
        discard await fut
      except CatchableError:
        discard
    raise newException(TimeoutError, $op & " timed out")

template awaitMaybeTimeout(
    u: UringFileIO, fut: untyped, deadline: MonoTime, op: Operation
): untyped =
  ## If deadline is default (zero), plain await. Otherwise await with timeout.
  if deadline == default(MonoTime):
    await fut
  else:
    await awaitOrTimeout(u, fut, deadline, op)

proc readFile*(
    u: UringFileIO, path: string, maxSize: int = 1048576, timeoutMs: int = 0
): Future[seq[byte]] {.async.} =
  ## Read entire file contents. Uses statx to determine file size, then reads
  ## in a loop to handle partial reads. Raises IOError if file size exceeds maxSize.
  ## maxSize must not exceed uint32 range (~4GiB).
  ## If timeoutMs > 0, raises TimeoutError if the operation exceeds the deadline.
  if maxSize > int(high(uint32)):
    raise newException(IOError, "maxSize exceeds 4GiB limit")

  let deadline =
    if timeoutMs > 0:
      getMonoTime() + initDuration(milliseconds = timeoutMs)
    else:
      default(MonoTime)

  # Open first, then statx on fd to avoid TOCTOU between statx(path) and open(path).
  let fdRes =
    awaitMaybeTimeout(u, uringOpen(u, path, O_RDONLY, 0), deadline, Operation.open)
  raiseOnError(fdRes, Operation.open)
  let fd = fdRes

  var closed = false
  try:
    # statx on the open fd to get file size
    var stx = new(Statx)
    let statxRes = awaitMaybeTimeout(
      u, uringStatxFd(u, fd.cint, STATX_SIZE, stx), deadline, Operation.statx
    )
    raiseOnError(statxRes, Operation.statx)
    let fileSize = int(stx.stxSize)

    if fileSize > maxSize:
      raise
        newException(IOError, "file size " & $fileSize & " exceeds maxSize " & $maxSize)

    if fileSize <= 0:
      closed = true
      let closeRes = await uringClose(u, fd.cint)
      raiseOnError(closeRes, Operation.close)
      return @[]

    var buf = newSeq[byte](fileSize)
    var totalRead = 0
    while totalRead < fileSize:
      let remaining = min(fileSize - totalRead, int(high(uint32)))
      let readRes = awaitMaybeTimeout(
        u,
        uringRead(
          u, fd.cint, addr buf[totalRead], uint32(remaining), uint64(totalRead), buf
        ),
        deadline,
        Operation.read,
      )
      if readRes <= 0:
        closed = true
        let closeRes = await uringClose(u, fd.cint)
        raiseOnError(readRes, Operation.read)
        raiseOnError(closeRes, Operation.close)
        buf.setLen(totalRead)
        return buf
      totalRead += readRes.int

    closed = true
    let closeRes = await uringClose(u, fd.cint)
    raiseOnError(closeRes, Operation.close)

    buf.setLen(totalRead)
    return buf
  finally:
    if not closed:
      discard await uringClose(u, fd.cint)

proc writeFile*(
    u: UringFileIO, path: string, data: seq[byte], timeoutMs: int = 0
): Future[void] {.async.} =
  ## Write data to file. Creates/truncates file. Raises IOError on failure.
  ## If timeoutMs > 0, raises TimeoutError if the operation exceeds the deadline.
  let deadline =
    if timeoutMs > 0:
      getMonoTime() + initDuration(milliseconds = timeoutMs)
    else:
      default(MonoTime)

  let flags = O_WRONLY or O_CREAT or O_TRUNC
  let fdRes = awaitMaybeTimeout(
    u, uringOpen(u, path, flags.cint, 0o644), deadline, Operation.open
  )
  raiseOnError(fdRes, Operation.open)
  let fd = fdRes

  var closed = false
  try:
    if data.len > 0:
      var dataCopy = data
      var written = 0
      while written < dataCopy.len:
        let remaining = min(dataCopy.len - written, int(high(uint32)))
        let writeRes = awaitMaybeTimeout(
          u,
          uringWrite(
            u,
            fd.cint,
            addr dataCopy[written],
            uint32(remaining),
            uint64(written),
            dataCopy,
          ),
          deadline,
          Operation.write,
        )
        if writeRes <= 0:
          closed = true
          discard await uringClose(u, fd.cint)
          raiseOnError(writeRes, Operation.write)
          raise newException(IOError, "write stalled: 0 bytes written")
        written += writeRes.int

    let fsyncRes =
      awaitMaybeTimeout(u, uringFsync(u, fd.cint), deadline, Operation.fsync)
    if fsyncRes < 0:
      closed = true
      discard await uringClose(u, fd.cint)
      raiseOnError(fsyncRes, Operation.fsync)

    closed = true
    let closeRes = await uringClose(u, fd.cint)
    raiseOnError(closeRes, Operation.close)
  finally:
    if not closed:
      discard await uringClose(u, fd.cint)

proc readFileString*(
    u: UringFileIO, path: string, maxSize: int = 1048576, timeoutMs: int = 0
): Future[string] {.async.} =
  ## Read entire file as string. Raises IOError on failure.
  ## If timeoutMs > 0, raises TimeoutError if the operation exceeds the deadline.
  let bytes = await readFile(u, path, maxSize, timeoutMs)
  var s = newString(bytes.len)
  if bytes.len > 0:
    copyMem(addr s[0], unsafeAddr bytes[0], bytes.len)
  return s

proc writeFileString*(
    u: UringFileIO, path: string, data: string, timeoutMs: int = 0
): Future[void] {.async.} =
  ## Write string to file. Creates/truncates file. Raises IOError on failure.
  ## If timeoutMs > 0, raises TimeoutError if the operation exceeds the deadline.
  var bytes = newSeq[byte](data.len)
  if data.len > 0:
    copyMem(addr bytes[0], unsafeAddr data[0], data.len)
  await writeFile(u, path, bytes, timeoutMs)
