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
    u: UringFileIO, path: string, timeoutMs: int = 0
): Future[seq[byte]] {.async.} =
  ## Read entire file contents. Uses statx to determine file size, then reads
  ## in a loop to handle partial reads.
  ## If timeoutMs > 0, raises TimeoutError if the operation exceeds the deadline.
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

    if fileSize <= 0:
      closed = true
      let closeRes = await uringClose(u, fd.cint)
      raiseOnError(closeRes, Operation.close)
      return @[]

    var bufRef = new(seq[byte])
    bufRef[] = newSeq[byte](fileSize)

    if fileSize <= int(high(uint32)):
      # Single read: chain read → close
      u.beginChain()
      let readFut =
        uringRead(u, fd.cint, addr bufRef[][0], uint32(fileSize), 0'u64, bufRef)
      let closeFut = uringClose(u, fd.cint)
      discard u.endChain()

      let readRes = awaitMaybeTimeout(u, readFut, deadline, Operation.read)
      raiseOnError(readRes, Operation.read)
      let totalRead = readRes.int
      bufRef[].setLen(totalRead)

      closed = true
      let closeRes = awaitMaybeTimeout(u, closeFut, deadline, Operation.close)
      raiseOnError(closeRes, Operation.close)
      return bufRef[]
    else:
      # Multi-read loop
      var totalRead = 0
      while totalRead < fileSize:
        let remaining = min(fileSize - totalRead, int(high(uint32)))
        let readRes = awaitMaybeTimeout(
          u,
          uringRead(
            u,
            fd.cint,
            addr bufRef[][totalRead],
            uint32(remaining),
            uint64(totalRead),
            bufRef,
          ),
          deadline,
          Operation.read,
        )
        if readRes <= 0:
          closed = true
          let closeRes = await uringClose(u, fd.cint)
          raiseOnError(readRes, Operation.read)
          raiseOnError(closeRes, Operation.close)
          bufRef[].setLen(totalRead)
          return bufRef[]
        totalRead += readRes.int

      closed = true
      let closeRes = await uringClose(u, fd.cint)
      raiseOnError(closeRes, Operation.close)

      bufRef[].setLen(totalRead)
      return bufRef[]
  finally:
    if not closed:
      discard await uringClose(u, fd.cint)

proc writeFile*(
    u: UringFileIO,
    path: string,
    data: seq[byte],
    timeoutMs: int = 0,
    fsync: bool = true,
): Future[void] {.async.} =
  ## Write data to file. Creates/truncates file. Raises IOError on failure.
  ## If fsync is false, skip the fsync call after writing.
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
    if data.len > 0 and data.len <= int(high(uint32)):
      # Single write: chain write → (fsync →) close
      var dataRef = new(seq[byte])
      dataRef[] = data
      var fsyncFut: Future[int32]

      u.beginChain()
      let writeFut =
        uringWrite(u, fd.cint, addr dataRef[][0], uint32(data.len), 0'u64, dataRef)
      if fsync:
        fsyncFut = uringFsync(u, fd.cint)
      let closeFut = uringClose(u, fd.cint)
      discard u.endChain()

      let writeRes = awaitMaybeTimeout(u, writeFut, deadline, Operation.write)
      raiseOnError(writeRes, Operation.write)
      if writeRes <= 0:
        closed = true
        raise newException(IOError, "write stalled: 0 bytes written")

      if fsync:
        let fsyncRes = awaitMaybeTimeout(u, fsyncFut, deadline, Operation.fsync)
        raiseOnError(fsyncRes, Operation.fsync)

      closed = true
      let closeRes = awaitMaybeTimeout(u, closeFut, deadline, Operation.close)
      raiseOnError(closeRes, Operation.close)
    elif data.len > 0:
      # Multi-write loop
      var dataRef = new(seq[byte])
      dataRef[] = data
      var written = 0
      while written < dataRef[].len:
        let remaining = min(dataRef[].len - written, int(high(uint32)))
        let writeRes = awaitMaybeTimeout(
          u,
          uringWrite(
            u,
            fd.cint,
            addr dataRef[][written],
            uint32(remaining),
            uint64(written),
            dataRef,
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

      # Chain fsync → close or just close
      if fsync:
        u.beginChain()
        let fsyncFut = uringFsync(u, fd.cint)
        let closeFut = uringClose(u, fd.cint)
        discard u.endChain()

        let fsyncRes = awaitMaybeTimeout(u, fsyncFut, deadline, Operation.fsync)
        raiseOnError(fsyncRes, Operation.fsync)
        closed = true
        let closeRes = awaitMaybeTimeout(u, closeFut, deadline, Operation.close)
        raiseOnError(closeRes, Operation.close)
      else:
        closed = true
        let closeRes =
          awaitMaybeTimeout(u, uringClose(u, fd.cint), deadline, Operation.close)
        raiseOnError(closeRes, Operation.close)
    else:
      # Empty data
      if fsync:
        u.beginChain()
        let fsyncFut = uringFsync(u, fd.cint)
        let closeFut = uringClose(u, fd.cint)
        discard u.endChain()

        let fsyncRes = awaitMaybeTimeout(u, fsyncFut, deadline, Operation.fsync)
        raiseOnError(fsyncRes, Operation.fsync)
        closed = true
        let closeRes = awaitMaybeTimeout(u, closeFut, deadline, Operation.close)
        raiseOnError(closeRes, Operation.close)
      else:
        closed = true
        let closeRes =
          awaitMaybeTimeout(u, uringClose(u, fd.cint), deadline, Operation.close)
        raiseOnError(closeRes, Operation.close)
  finally:
    if not closed:
      discard await uringClose(u, fd.cint)

proc readFileString*(
    u: UringFileIO, path: string, timeoutMs: int = 0
): Future[string] {.async.} =
  ## Read entire file as string. Raises IOError on failure.
  ## If timeoutMs > 0, raises TimeoutError if the operation exceeds the deadline.
  let bytes = await readFile(u, path, timeoutMs)
  var s = newString(bytes.len)
  if bytes.len > 0:
    copyMem(addr s[0], unsafeAddr bytes[0], bytes.len)
  return s

proc writeFileString*(
    u: UringFileIO, path: string, data: string, timeoutMs: int = 0, fsync: bool = true
): Future[void] {.async.} =
  ## Write string to file. Creates/truncates file. Raises IOError on failure.
  ## If fsync is false, skip the fsync call after writing.
  ## If timeoutMs > 0, raises TimeoutError if the operation exceeds the deadline.
  var bytes = newSeq[byte](data.len)
  if data.len > 0:
    copyMem(addr bytes[0], unsafeAddr data[0], data.len)
  await writeFile(u, path, bytes, timeoutMs, fsync)

# Direct descriptor variants

proc readFileDirect*(
    u: UringFileIO, path: string, timeoutMs: int = 0
): Future[seq[byte]] {.async.} =
  ## Read entire file using direct descriptors. Requires `registerFixedFileSlots`.
  ## Chains openDirect → readFixedFile → closeDirect in a single submission (2 syscalls).
  ## If timeoutMs > 0, raises TimeoutError if the operation exceeds the deadline.
  let deadline =
    if timeoutMs > 0:
      getMonoTime() + initDuration(milliseconds = timeoutMs)
    else:
      default(MonoTime)

  # Get file size via path-based statx
  var stx = new(Statx)
  let statxRes = awaitMaybeTimeout(
    u, uringStatx(u, path, 0.cint, STATX_SIZE, stx), deadline, Operation.statx
  )
  raiseOnError(statxRes, Operation.statx)
  let fileSize = int(stx.stxSize)

  if fileSize <= 0:
    return @[]

  let slot = allocFixedFileSlot(u)
  var slotHasFile = false

  try:
    if fileSize <= int(high(uint32)):
      # Single chain: openDirect → readFixedFile → closeDirect
      var bufRef = new(seq[byte])
      bufRef[] = newSeq[byte](fileSize)

      u.beginChain()
      let openFut = u.uringOpenDirect(path, O_RDONLY, 0, slot)
      let readFut = uringReadFixedFile(
        u, slot.cint, addr bufRef[][0], uint32(fileSize), 0'u64, bufRef
      )
      let closeFut = u.uringCloseDirect(slot)
      discard u.endChain()

      let openRes = awaitMaybeTimeout(u, openFut, deadline, Operation.open)
      if openRes == 0:
        slotHasFile = true
      raiseOnError(openRes, Operation.open)

      let readRes = awaitMaybeTimeout(u, readFut, deadline, Operation.read)
      raiseOnError(readRes, Operation.read)
      let totalRead = readRes.int
      bufRef[].setLen(totalRead)

      let closeRes = awaitMaybeTimeout(u, closeFut, deadline, Operation.close)
      if closeRes == 0:
        slotHasFile = false
      raiseOnError(closeRes, Operation.close)
      return bufRef[]
    else:
      # Multi-read: openDirect (await), loop reads, closeDirect
      let openRes = awaitMaybeTimeout(
        u, u.uringOpenDirect(path, O_RDONLY, 0, slot), deadline, Operation.open
      )
      if openRes == 0:
        slotHasFile = true
      raiseOnError(openRes, Operation.open)

      var bufRef = new(seq[byte])
      bufRef[] = newSeq[byte](fileSize)
      var totalRead = 0

      while totalRead < fileSize:
        let remaining = min(fileSize - totalRead, int(high(uint32)))
        let readRes = awaitMaybeTimeout(
          u,
          uringReadFixedFile(
            u,
            slot.cint,
            addr bufRef[][totalRead],
            uint32(remaining),
            uint64(totalRead),
            bufRef,
          ),
          deadline,
          Operation.read,
        )
        if readRes <= 0:
          raiseOnError(readRes, Operation.read)
          bufRef[].setLen(totalRead)
          break
        totalRead += readRes.int

      let closeRes =
        awaitMaybeTimeout(u, u.uringCloseDirect(slot), deadline, Operation.close)
      if closeRes == 0:
        slotHasFile = false
      raiseOnError(closeRes, Operation.close)

      bufRef[].setLen(totalRead)
      return bufRef[]
  finally:
    if slotHasFile:
      discard await u.uringCloseDirect(slot)
    freeFixedFileSlot(u, slot)

proc writeFileDirect*(
    u: UringFileIO,
    path: string,
    data: seq[byte],
    timeoutMs: int = 0,
    fsync: bool = true,
): Future[void] {.async.} =
  ## Write data to file using direct descriptors. Requires `registerFixedFileSlots`.
  ## Chains openDirect → writeFixedFile → fsyncFixedFile → closeDirect in a single
  ## submission (1 syscall for data ≤ 4GB).
  ## If timeoutMs > 0, raises TimeoutError if the operation exceeds the deadline.
  let deadline =
    if timeoutMs > 0:
      getMonoTime() + initDuration(milliseconds = timeoutMs)
    else:
      default(MonoTime)

  let flags = O_WRONLY or O_CREAT or O_TRUNC
  let slot = allocFixedFileSlot(u)
  var slotHasFile = false

  try:
    if data.len <= int(high(uint32)):
      # Single chain: openDirect → (write →) (fsync →) closeDirect
      var dataRef: ref seq[byte]
      if data.len > 0:
        dataRef = new(seq[byte])
        dataRef[] = data

      u.beginChain()
      let openFut = u.uringOpenDirect(path, flags.cint, 0o644, slot)
      var writeFut: Future[int32]
      if data.len > 0:
        writeFut = uringWriteFixedFile(
          u, slot.cint, addr dataRef[][0], uint32(data.len), 0'u64, dataRef
        )
      var fsyncFut: Future[int32]
      if fsync:
        fsyncFut = uringFsyncFixedFile(u, slot.cint)
      let closeFut = u.uringCloseDirect(slot)
      discard u.endChain()

      let openRes = awaitMaybeTimeout(u, openFut, deadline, Operation.open)
      if openRes == 0:
        slotHasFile = true
      raiseOnError(openRes, Operation.open)

      if data.len > 0:
        let writeRes = awaitMaybeTimeout(u, writeFut, deadline, Operation.write)
        raiseOnError(writeRes, Operation.write)
        if writeRes <= 0:
          raise newException(IOError, "write stalled: 0 bytes written")

      if fsync:
        let fsyncRes = awaitMaybeTimeout(u, fsyncFut, deadline, Operation.fsync)
        raiseOnError(fsyncRes, Operation.fsync)

      let closeRes = awaitMaybeTimeout(u, closeFut, deadline, Operation.close)
      if closeRes == 0:
        slotHasFile = false
      raiseOnError(closeRes, Operation.close)
    else:
      # Multi-write: openDirect (await), loop writes, then (fsync →) closeDirect
      let openRes = awaitMaybeTimeout(
        u, u.uringOpenDirect(path, flags.cint, 0o644, slot), deadline, Operation.open
      )
      if openRes == 0:
        slotHasFile = true
      raiseOnError(openRes, Operation.open)

      var dataRef = new(seq[byte])
      dataRef[] = data
      var written = 0
      while written < dataRef[].len:
        let remaining = min(dataRef[].len - written, int(high(uint32)))
        let writeRes = awaitMaybeTimeout(
          u,
          uringWriteFixedFile(
            u,
            slot.cint,
            addr dataRef[][written],
            uint32(remaining),
            uint64(written),
            dataRef,
          ),
          deadline,
          Operation.write,
        )
        if writeRes <= 0:
          raiseOnError(writeRes, Operation.write)
          raise newException(IOError, "write stalled: 0 bytes written")
        written += writeRes.int

      if fsync:
        u.beginChain()
        let fsyncFut = uringFsyncFixedFile(u, slot.cint)
        let closeFut = u.uringCloseDirect(slot)
        discard u.endChain()

        let fsyncRes = awaitMaybeTimeout(u, fsyncFut, deadline, Operation.fsync)
        raiseOnError(fsyncRes, Operation.fsync)
        let closeRes = awaitMaybeTimeout(u, closeFut, deadline, Operation.close)
        if closeRes == 0:
          slotHasFile = false
        raiseOnError(closeRes, Operation.close)
      else:
        let closeRes =
          awaitMaybeTimeout(u, u.uringCloseDirect(slot), deadline, Operation.close)
        if closeRes == 0:
          slotHasFile = false
        raiseOnError(closeRes, Operation.close)
  finally:
    if slotHasFile:
      discard await u.uringCloseDirect(slot)
    freeFixedFileSlot(u, slot)

proc readFileStringDirect*(
    u: UringFileIO, path: string, timeoutMs: int = 0
): Future[string] {.async.} =
  ## Read entire file as string using direct descriptors.
  ## Requires `registerFixedFileSlots`.
  ## If timeoutMs > 0, raises TimeoutError if the operation exceeds the deadline.
  let bytes = await readFileDirect(u, path, timeoutMs)
  var s = newString(bytes.len)
  if bytes.len > 0:
    copyMem(addr s[0], unsafeAddr bytes[0], bytes.len)
  return s

proc writeFileStringDirect*(
    u: UringFileIO, path: string, data: string, timeoutMs: int = 0, fsync: bool = true
): Future[void] {.async.} =
  ## Write string to file using direct descriptors.
  ## Requires `registerFixedFileSlots`.
  ## If timeoutMs > 0, raises TimeoutError if the operation exceeds the deadline.
  var bytes = newSeq[byte](data.len)
  if data.len > 0:
    copyMem(addr bytes[0], unsafeAddr data[0], data.len)
  await writeFileDirect(u, path, bytes, timeoutMs, fsync)
