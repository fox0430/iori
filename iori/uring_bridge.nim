## eventfd bridge + Low-level API for io_uring.
##
## Integrates io_uring with async event loop via eventfd.
## Provides low-level API returning Future[int32] for each io_uring operation.

when not defined(linux):
  {.fatal: "uring_bridge requires Linux".}

import std/[tables, posix, oserrors]

import ./async_backend
import ./uring_raw

export async_backend

const
  EFD_NONBLOCK = 0x00000800.cint
  EFD_CLOEXEC = 0x00080000.cint

proc eventfd(initval: cuint, flags: cint): cint {.importc, header: "<sys/eventfd.h>".}

type
  CompletionKind = enum
    ckOpen
    ckRead
    ckWrite
    ckFsync
    ckClose
    ckStatx
    ckRename

  Completion = object
    future: Future[int32]
    kind: CompletionKind
    bufRef: seq[byte] # GC root for buffer
    strRef: string # GC root for path string
    strRef2: string # GC root for second path string (rename)
    statxRef: ref Statx # GC root for statx output buffer

  UringFileIO* = ref object
    ring*: IoUring
    eventFd: cint
    nextId: uint64
    pending: Table[uint64, Completion]
    closed: bool
    selfRef: UringFileIO # GC root: prevents collection while poll loop holds raw pointer
    flushScheduled: bool # Prevents duplicate callSoon scheduling
    unsubmitted: seq[uint64] # IDs not yet submitted (for failing on submit error)

proc allocId(u: UringFileIO): uint64 =
  ## ID allocation
  result = u.nextId
  u.nextId += 1

proc processCqes(u: UringFileIO) {.raises: [].} =
  ## CQE processing
  while not u.closed:
    let cqe = peekCqe(u.ring)
    if cqe == nil:
      break
    let id = cqe.userData
    let res = cqe.res
    advanceCq(u.ring)

    var comp: Completion
    if u.pending.pop(id, comp):
      # Each ID is unique and popped from pending, so double-completion is impossible.
      # cast(raises) suppresses the theoretical ValueError from Future.complete.
      {.cast(raises: []).}:
        comp.future.complete(res)

proc drainEventfd(u: UringFileIO) =
  ## Read and discard the eventfd counter. Ignores EAGAIN.
  var buf: uint64
  let ret = read(u.eventFd, addr buf, sizeof(buf))
  if ret < 0:
    let err = errno
    if err != EAGAIN:
      raiseOSError(OSErrorCode(err), "eventfd read failed")

proc startPollLoop(u: UringFileIO) =
  u.selfRef = u
  registerFdReader(
    u.eventFd,
    proc() {.gcsafe, raises: [].} =
      if u.closed:
        return
      try:
        u.drainEventfd()
      except OSError:
        discard
      u.processCqes(),
  )

proc stopPollLoop(u: UringFileIO) =
  unregisterFdReader(u.eventFd)
  u.selfRef = nil

proc flush*(u: UringFileIO) =
  ## Flush all queued SQEs to the kernel in a single io_uring_enter syscall.
  u.flushScheduled = false
  if u.closed or u.unsubmitted.len == 0:
    return

  let ret = submit(u.ring)
  if ret < 0:
    # Submit failed: fail only unsubmitted futures (already-submitted ones complete via CQE)
    for id in u.unsubmitted:
      var comp: Completion
      if u.pending.pop(id, comp):
        {.cast(raises: []).}:
          comp.future.fail(
            newException(
              IOError, "io_uring submit failed: " & osErrorMsg(OSErrorCode(-ret))
            )
          )
  u.unsubmitted.setLen(0)

proc queueSqe(u: UringFileIO, comp: Completion): Future[int32] =
  ## Queue the most recently prepared SQE for batch submission.
  ## The caller must have already called getSqe and filled the SQE fields
  ## (except userData, which this proc sets).
  ## The SQE will be submitted on the next event loop tick via callSoon.
  let fut = comp.future
  let id = u.allocId()

  setLastSqeUserData(u.ring, id)

  u.pending[id] = comp
  u.unsubmitted.add(id)

  if not u.flushScheduled:
    u.flushScheduled = true
    scheduleSoon(
      proc() {.gcsafe, raises: [].} =
        {.cast(gcsafe).}:
          u.flush()
    )

  return fut

# Low-level API

proc uringOpen*(
    u: UringFileIO, path: string, flags: cint, mode: uint32 = 0o644
): Future[int32] =
  ## Submit OPENAT operation. Returns Future with fd on success or negative errno.
  let fut = newFuture[int32]("uringOpen")

  let sqe = getSqe(u.ring)
  if sqe == nil:
    fut.fail(newException(IOError, "io_uring SQ full"))
    return fut

  # Keep path alive until CQE arrives
  let pathCopy = path

  sqe.opcode = IORING_OP_OPENAT
  sqe.fd = AT_FDCWD
  sqe.`addr` = cast[uint64](pathCopy.cstring)
  sqe.len = uint32(mode)
  sqe.opFlags = cast[uint32](flags)

  var comp = Completion(future: fut, kind: ckOpen, strRef: pathCopy)
  return queueSqe(u, comp)

proc uringRead*(
    u: UringFileIO,
    fd: cint,
    buf: pointer,
    size: uint32,
    offset: uint64,
    bufRef: seq[byte],
): Future[int32] =
  ## Submit READ operation. Returns Future with bytes read or negative errno.
  ## bufRef keeps the buffer GC-rooted until completion.
  let fut = newFuture[int32]("uringRead")

  let sqe = getSqe(u.ring)
  if sqe == nil:
    fut.fail(newException(IOError, "io_uring SQ full"))
    return fut

  sqe.opcode = IORING_OP_READ
  sqe.fd = fd
  sqe.`addr` = cast[uint64](buf)
  sqe.len = size
  sqe.off = offset

  var comp = Completion(future: fut, kind: ckRead, bufRef: bufRef)
  return queueSqe(u, comp)

proc uringWrite*(
    u: UringFileIO,
    fd: cint,
    buf: pointer,
    size: uint32,
    offset: uint64,
    bufRef: seq[byte],
): Future[int32] =
  ## Submit WRITE operation. Returns Future with bytes written or negative errno.
  ## bufRef keeps the buffer GC-rooted until completion.
  let fut = newFuture[int32]("uringWrite")

  let sqe = getSqe(u.ring)
  if sqe == nil:
    fut.fail(newException(IOError, "io_uring SQ full"))
    return fut

  sqe.opcode = IORING_OP_WRITE
  sqe.fd = fd
  sqe.`addr` = cast[uint64](buf)
  sqe.len = size
  sqe.off = offset

  var comp = Completion(future: fut, kind: ckWrite, bufRef: bufRef)
  return queueSqe(u, comp)

proc uringFsync*(u: UringFileIO, fd: cint): Future[int32] =
  ## Submit FSYNC operation. Returns Future with 0 on success or negative errno.
  let fut = newFuture[int32]("uringFsync")

  let sqe = getSqe(u.ring)
  if sqe == nil:
    fut.fail(newException(IOError, "io_uring SQ full"))
    return fut

  sqe.opcode = IORING_OP_FSYNC
  sqe.fd = fd

  var comp = Completion(future: fut, kind: ckFsync)
  return queueSqe(u, comp)

proc uringClose*(u: UringFileIO, fd: cint): Future[int32] =
  ## Submit CLOSE operation. Returns Future with 0 on success or negative errno.
  let fut = newFuture[int32]("uringClose")

  let sqe = getSqe(u.ring)
  if sqe == nil:
    fut.fail(newException(IOError, "io_uring SQ full"))
    return fut

  sqe.opcode = IORING_OP_CLOSE
  sqe.fd = fd

  var comp = Completion(future: fut, kind: ckClose)
  return queueSqe(u, comp)

proc uringStatx*(
    u: UringFileIO, path: string, flags: cint, mask: uint32, statxBuf: ref Statx
): Future[int32] =
  ## Submit STATX operation. Returns Future with 0 on success or negative errno.
  ## Results are written to statxBuf.
  let fut = newFuture[int32]("uringStatx")

  let sqe = getSqe(u.ring)
  if sqe == nil:
    fut.fail(newException(IOError, "io_uring SQ full"))
    return fut

  let pathCopy = path

  sqe.opcode = IORING_OP_STATX
  sqe.fd = AT_FDCWD
  sqe.`addr` = cast[uint64](pathCopy.cstring)
  sqe.len = mask
  sqe.off = cast[uint64](addr statxBuf[])
  sqe.opFlags = cast[uint32](flags)

  var comp =
    Completion(future: fut, kind: ckStatx, strRef: pathCopy, statxRef: statxBuf)
  return queueSqe(u, comp)

proc uringRenameat*(
    u: UringFileIO, oldPath: string, newPath: string, flags: uint32 = 0
): Future[int32] =
  ## Submit RENAMEAT operation. Returns Future with 0 on success or negative errno.
  let fut = newFuture[int32]("uringRenameat")

  let sqe = getSqe(u.ring)
  if sqe == nil:
    fut.fail(newException(IOError, "io_uring SQ full"))
    return fut

  let oldPathCopy = oldPath
  let newPathCopy = newPath

  sqe.opcode = IORING_OP_RENAMEAT
  sqe.fd = AT_FDCWD
  sqe.`addr` = cast[uint64](oldPathCopy.cstring)
  sqe.len = cast[uint32](AT_FDCWD)
  sqe.off = cast[uint64](newPathCopy.cstring)
  sqe.opFlags = flags

  var comp =
    Completion(future: fut, kind: ckRename, strRef: oldPathCopy, strRef2: newPathCopy)
  return queueSqe(u, comp)

# Lifecycle

proc newUringFileIO*(entries: uint32 = 256): UringFileIO =
  ## Create a new UringFileIO instance. Initializes io_uring and starts poll loop.
  var ring = setupRing(entries)

  let efd = eventfd(0, EFD_NONBLOCK or EFD_CLOEXEC)
  if efd < 0:
    closeRing(ring)
    raiseOSError(osLastError(), "eventfd creation failed")

  try:
    registerEventfd(ring, efd)
  except OSError:
    discard close(efd)
    closeRing(ring)
    raise

  result = UringFileIO(
    ring: ring,
    eventFd: efd,
    nextId: 1,
    pending: initTable[uint64, Completion](),
    closed: false,
  )
  startPollLoop(result)

proc close*(u: UringFileIO) =
  ## Close the UringFileIO instance. Fails all pending futures and releases resources.
  if u.closed:
    return
  u.closed = true

  stopPollLoop(u)
  u.flushScheduled = false
  u.unsubmitted.setLen(0)

  # Collect and clear pending before failing — fail() may trigger callbacks
  # that attempt to modify the table.
  var pending: seq[Completion]
  for id, comp in u.pending:
    pending.add(comp)
  u.pending.clear()
  for comp in pending:
    comp.future.fail(newException(IOError, "UringFileIO closed"))

  unregisterEventfd(u.ring)
  discard close(u.eventFd)
  u.eventFd = -1
  closeRing(u.ring)
