## High-level file I/O API built on uring_bridge.
##
## Provides readFile/writeFile convenience procs that handle
## the file descriptor lifecycle (open, read/write, fsync, close) automatically.

import std/[posix, oserrors]

import ./uring_bridge
import ./uring_raw # for Statx, STATX_SIZE

export uring_bridge

proc readFile*(
    u: UringFileIO, path: string, maxSize: int = 1048576
): Future[seq[byte]] {.async.} =
  ## Read entire file contents. Uses statx to determine file size, then reads
  ## in a loop to handle partial reads. Raises IOError if file size exceeds maxSize.
  ## maxSize must not exceed uint32 range (~4GiB).
  if maxSize > int(high(uint32)):
    raise newException(IOError, "maxSize exceeds 4GiB limit")

  # statx to get file size
  var stx = new(Statx)
  let statxRes = await uringStatx(u, path, 0.cint, STATX_SIZE, stx)
  if statxRes < 0:
    raise newException(IOError, "statx failed: " & osErrorMsg(OSErrorCode(-statxRes)))
  let fileSize = int(stx.stxSize)

  if fileSize > maxSize:
    raise
      newException(IOError, "file size " & $fileSize & " exceeds maxSize " & $maxSize)

  if fileSize <= 0:
    return @[]

  let fdRes = await uringOpen(u, path, O_RDONLY, 0)
  if fdRes < 0:
    raise newException(IOError, "open failed: " & osErrorMsg(OSErrorCode(-fdRes)))
  let fd = fdRes

  var closed = false
  try:
    var buf = newSeq[byte](fileSize)
    var totalRead = 0
    while totalRead < fileSize:
      let remaining = min(fileSize - totalRead, int(high(uint32)))
      let readRes = await uringRead(
        u, fd.cint, addr buf[totalRead], uint32(remaining), uint64(totalRead), buf
      )
      if readRes <= 0:
        closed = true
        let closeRes = await uringClose(u, fd.cint)
        if readRes < 0:
          raise
            newException(IOError, "read failed: " & osErrorMsg(OSErrorCode(-readRes)))
        if closeRes < 0:
          raise
            newException(IOError, "close failed: " & osErrorMsg(OSErrorCode(-closeRes)))
        buf.setLen(totalRead)
        return buf
      totalRead += readRes.int

    closed = true
    let closeRes = await uringClose(u, fd.cint)
    if closeRes < 0:
      raise newException(IOError, "close failed: " & osErrorMsg(OSErrorCode(-closeRes)))

    buf.setLen(totalRead)
    return buf
  finally:
    if not closed:
      discard await uringClose(u, fd.cint)

proc writeFile*(u: UringFileIO, path: string, data: seq[byte]): Future[void] {.async.} =
  ## Write data to file. Creates/truncates file. Raises IOError on failure.
  let flags = O_WRONLY or O_CREAT or O_TRUNC
  let fdRes = await uringOpen(u, path, flags.cint, 0o644)
  if fdRes < 0:
    raise newException(IOError, "open failed: " & osErrorMsg(OSErrorCode(-fdRes)))
  let fd = fdRes

  var closed = false
  try:
    if data.len > 0:
      var dataCopy = data
      var written = 0
      while written < dataCopy.len:
        let remaining = min(dataCopy.len - written, int(high(uint32)))
        let writeRes = await uringWrite(
          u,
          fd.cint,
          addr dataCopy[written],
          uint32(remaining),
          uint64(written),
          dataCopy,
        )
        if writeRes <= 0:
          closed = true
          discard await uringClose(u, fd.cint)
          if writeRes < 0:
            raise newException(
              IOError, "write failed: " & osErrorMsg(OSErrorCode(-writeRes))
            )
          else:
            raise newException(IOError, "write stalled: 0 bytes written")
        written += writeRes.int

    let fsyncRes = await uringFsync(u, fd.cint)
    if fsyncRes < 0:
      closed = true
      discard await uringClose(u, fd.cint)
      raise newException(IOError, "fsync failed: " & osErrorMsg(OSErrorCode(-fsyncRes)))

    closed = true
    let closeRes = await uringClose(u, fd.cint)
    if closeRes < 0:
      raise newException(IOError, "close failed: " & osErrorMsg(OSErrorCode(-closeRes)))
  finally:
    if not closed:
      discard await uringClose(u, fd.cint)

proc readFileString*(
    u: UringFileIO, path: string, maxSize: int = 1048576
): Future[string] {.async.} =
  ## Read entire file as string. Raises IOError on failure.
  let bytes = await readFile(u, path, maxSize)
  var s = newString(bytes.len)
  if bytes.len > 0:
    copyMem(addr s[0], unsafeAddr bytes[0], bytes.len)
  return s

proc writeFileString*(
    u: UringFileIO, path: string, data: string
): Future[void] {.async.} =
  ## Write string to file. Creates/truncates file. Raises IOError on failure.
  var bytes = newSeq[byte](data.len)
  if data.len > 0:
    copyMem(addr bytes[0], unsafeAddr data[0], data.len)
  await writeFile(u, path, bytes)
