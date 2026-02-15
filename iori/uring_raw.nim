## io_uring raw syscall bindings and ring management.
##
## Linux-only, x86_64/aarch64. No liburing dependency — direct syscall invocation.

when not defined(linux):
  {.fatal: "uring_raw requires Linux".}

import std/[posix, oserrors]

# Syscall numbers

when defined(amd64):
  const
    SYS_io_uring_setup* = 425.clong
    SYS_io_uring_enter* = 426.clong
    SYS_io_uring_register* = 427.clong
elif defined(arm64):
  const
    SYS_io_uring_setup* = 425.clong
    SYS_io_uring_enter* = 426.clong
    SYS_io_uring_register* = 427.clong
else:
  {.fatal: "Unsupported architecture for io_uring".}

# io_uring constants

const
  # Opcodes
  IORING_OP_NOP* = 0'u8
  IORING_OP_READV* = 1'u8
  IORING_OP_WRITEV* = 2'u8
  IORING_OP_FSYNC* = 3'u8
  IORING_OP_READ_FIXED* = 4'u8
  IORING_OP_WRITE_FIXED* = 5'u8
  IORING_OP_ASYNC_CANCEL* = 14'u8
  IORING_OP_FALLOCATE* = 17'u8
  IORING_OP_OPENAT* = 18'u8
  IORING_OP_CLOSE* = 19'u8
  IORING_OP_STATX* = 21'u8
  IORING_OP_READ* = 22'u8
  IORING_OP_WRITE* = 23'u8
  IORING_OP_FADVISE* = 24'u8
  IORING_OP_RENAMEAT* = 35'u8
  IORING_OP_UNLINKAT* = 36'u8
  IORING_OP_MKDIRAT* = 37'u8
  IORING_OP_SYMLINKAT* = 38'u8
  IORING_OP_LINKAT* = 39'u8

  # SQE flags
  IOSQE_FIXED_FILE* = 1'u8 shl 0
  IOSQE_IO_DRAIN* = 1'u8 shl 1
  IOSQE_IO_LINK* = 1'u8 shl 2
  IOSQE_IO_HARDLINK* = 1'u8 shl 3
  IOSQE_ASYNC* = 1'u8 shl 4
  IOSQE_BUFFER_SELECT* = 1'u8 shl 5

  # Fsync flags (opFlags)
  IORING_FSYNC_DATASYNC* = 1'u32 shl 0

  # Setup flags
  IORING_SETUP_IOPOLL* = 1'u32 shl 0
  IORING_SETUP_SQPOLL* = 1'u32 shl 1
  IORING_SETUP_SQ_AFF* = 1'u32 shl 2

  # Enter flags
  IORING_ENTER_GETEVENTS* = 1'u32 shl 0
  IORING_ENTER_SQ_WAKEUP* = 1'u32 shl 1

  # Register opcodes
  IORING_REGISTER_BUFFERS* = 0'u32
  IORING_UNREGISTER_BUFFERS* = 1'u32
  IORING_REGISTER_FILES* = 2'u32
  IORING_UNREGISTER_FILES* = 3'u32
  IORING_REGISTER_EVENTFD* = 4'u32
  IORING_UNREGISTER_EVENTFD* = 5'u32
  IORING_REGISTER_FILES_UPDATE* = 6'u32

  # Feature flags
  IORING_FEAT_SINGLE_MMAP* = 1'u32 shl 0

  # Mmap offsets
  IORING_OFF_SQ_RING* = 0'u64
  IORING_OFF_CQ_RING* = 0x8000000'u64
  IORING_OFF_SQES* = 0x10000000'u64

  # statx mask constants
  STATX_TYPE* = 0x00000001'u32
  STATX_MODE* = 0x00000002'u32
  STATX_NLINK* = 0x00000004'u32
  STATX_UID* = 0x00000008'u32
  STATX_GID* = 0x00000010'u32
  STATX_ATIME* = 0x00000020'u32
  STATX_MTIME* = 0x00000040'u32
  STATX_CTIME* = 0x00000080'u32
  STATX_INO* = 0x00000100'u32
  STATX_SIZE* = 0x00000200'u32
  STATX_BLOCKS* = 0x00000400'u32
  STATX_BASIC_STATS* = 0x000007ff'u32
  STATX_BTIME* = 0x00000800'u32

  # AT flags
  AT_FDCWD* = -100.cint
  AT_SYMLINK_NOFOLLOW* = 0x100.cint
  AT_REMOVEDIR* = 0x200.cint
  AT_EMPTY_PATH* = 0x1000.cint

# Kernel ABI structures

type
  IoSqRingOffsets* = object ## Kernel-provided offsets for SQ ring mmap region.
    head*: uint32
    tail*: uint32
    ringMask*: uint32
    ringEntries*: uint32
    flags*: uint32
    dropped*: uint32
    array*: uint32
    resv1*: uint32
    userAddr*: uint64

  IoCqRingOffsets* = object ## Kernel-provided offsets for CQ ring mmap region.
    head*: uint32
    tail*: uint32
    ringMask*: uint32
    ringEntries*: uint32
    overflow*: uint32
    cqes*: uint32
    flags*: uint32
    resv1*: uint32
    userAddr*: uint64

  IoUringParams* = object
    ## Parameters for io_uring_setup syscall. Passed to kernel and filled on return.
    sqEntries*: uint32
    cqEntries*: uint32
    flags*: uint32
    sqThreadCpu*: uint32
    sqThreadIdle*: uint32
    features*: uint32
    wqFd*: uint32
    resv*: array[3, uint32]
    sqOff*: IoSqRingOffsets
    cqOff*: IoCqRingOffsets

  IoUringSqe* = object
    ## Submission Queue Entry. Describes a single I/O operation to submit to the kernel.
    opcode*: uint8
    flags*: uint8
    ioprio*: uint16
    fd*: int32
    off*: uint64 # offset / addr2
    `addr`*: uint64 # addr / splice_off_in
    len*: uint32
    opFlags*: uint32 # union: rw_flags, fsync_flags, etc.
    userData*: uint64
    bufInfo*: uint16 # union: buf_index, buf_group
    personality*: uint16
    spliceFdIn*: int32 # union: splice_fd_in, file_index
    addr3*: uint64
    pad2*: array[1, uint64]

  IoUringCqe* = object
    ## Completion Queue Entry. Contains the result of a completed I/O operation.
    userData*: uint64
    res*: int32
    flags*: uint32

  StatxTs* = object ## Timestamp component of the `Statx` structure.
    tvSec*: int64
    tvNsec*: uint32
    reserved: int32

  Statx* = object ## File metadata returned by the statx syscall.
    stxMask*: uint32
    stxBlksize*: uint32
    stxAttributes*: uint64
    stxNlink*: uint32
    stxUid*: uint32
    stxGid*: uint32
    stxMode*: uint16
    spare0: uint16
    stxIno*: uint64
    stxSize*: uint64
    stxBlocks*: uint64
    stxAttributesMask*: uint64
    stxAtime*: StatxTs
    stxBtime*: StatxTs
    stxCtime*: StatxTs
    stxMtime*: StatxTs
    stxRdevMajor*: uint32
    stxRdevMinor*: uint32
    stxDevMajor*: uint32
    stxDevMinor*: uint32
    stxMntId*: uint64
    stxDioMemAlign*: uint32
    stxDioOffsetAlign*: uint32
    spare3: array[12, uint64]

static:
  assert sizeof(IoUringSqe) == 64, "IoUringSqe must be 64 bytes"
  assert sizeof(IoUringCqe) == 16, "IoUringCqe must be 16 bytes"
  assert sizeof(StatxTs) == 16, "StatxTs must be 16 bytes"
  assert sizeof(Statx) == 256, "Statx must be 256 bytes"

# IoUring ring object

type IoUring* = object ## io_uring instance managing SQ/CQ rings and SQE array via mmap.
  ringFd*: cint

  # SQ ring
  sqRingPtr: pointer
  sqRingSize: int
  sqHead: ptr uint32
  sqTail: ptr uint32
  sqMask: ptr uint32
  sqEntries: ptr uint32
  sqFlags: ptr uint32
  sqArray: ptr UncheckedArray[uint32]

  # SQEs
  sqePtr: pointer
  sqeSize: int
  sqes: ptr UncheckedArray[IoUringSqe]

  # CQ ring
  cqRingPtr: pointer
  cqRingSize: int
  cqHead: ptr uint32
  cqTail: ptr uint32
  cqMask: ptr uint32
  cqEntries: ptr uint32
  cqes: ptr UncheckedArray[IoUringCqe]

  # Local state
  sqLocalTail: uint32

# Syscall wrappers

proc syscall(number: clong): clong {.importc, varargs, header: "<unistd.h>".}

proc ioUringSetup(entries: cuint, params: ptr IoUringParams): cint =
  result = cint(syscall(SYS_io_uring_setup, entries, params))

proc ioUringEnter(
    fd: cint,
    toSubmit: cuint,
    minComplete: cuint,
    flags: cuint,
    sig: pointer,
    sigSz: csize_t,
): cint =
  result =
    cint(syscall(SYS_io_uring_enter, fd, toSubmit, minComplete, flags, sig, sigSz))

proc ioUringRegister(fd: cint, opcode: cuint, arg: pointer, nrArgs: cuint): cint =
  result = cint(syscall(SYS_io_uring_register, fd, opcode, arg, nrArgs))

# Atomic helpers

proc atomicLoadAcquire(p: ptr uint32): uint32 {.inline.} =
  atomicLoadN(p, ATOMIC_ACQUIRE)

proc atomicStoreRelease(p: ptr uint32, val: uint32) {.inline.} =
  atomicStoreN(p, val, ATOMIC_RELEASE)

# Ring setup / teardown

proc setupRing*(
    entries: uint32 = 256, flags: uint32 = 0
): IoUring {.raises: [OSError].} =
  ## Initialize io_uring: setup ring and mmap 3 regions.
  ## Raises OSError on failure.
  var params: IoUringParams
  zeroMem(addr params, sizeof(params))
  params.flags = flags

  let fd = ioUringSetup(cuint(entries), addr params)
  if fd < 0:
    raiseOSError(osLastError(), "io_uring_setup failed")

  result.ringFd = fd

  # Calculate mmap sizes
  let sqRingSize =
    int(params.sqOff.array.uint64 + params.sqEntries.uint64 * sizeof(uint32).uint64)
  let cqRingSize =
    int(params.cqOff.cqes.uint64 + params.cqEntries.uint64 * sizeof(IoUringCqe).uint64)
  let sqeSize = int(params.sqEntries.uint64 * sizeof(IoUringSqe).uint64)

  # Check for SINGLE_MMAP feature (SQ and CQ share one mmap)
  let singleMmap = (params.features and IORING_FEAT_SINGLE_MMAP) != 0
  let actualSqRingSize =
    if singleMmap:
      max(sqRingSize, cqRingSize)
    else:
      sqRingSize
  let actualCqRingSize = if singleMmap: actualSqRingSize else: cqRingSize

  # mmap SQ ring
  result.sqRingPtr = mmap(
    nil,
    actualSqRingSize,
    PROT_READ or PROT_WRITE,
    MAP_SHARED or MAP_POPULATE,
    fd,
    Off(IORING_OFF_SQ_RING),
  )
  if result.sqRingPtr == MAP_FAILED:
    discard close(fd)
    raiseOSError(osLastError(), "mmap SQ ring failed")
  result.sqRingSize = actualSqRingSize

  # mmap CQ ring
  if singleMmap:
    result.cqRingPtr = result.sqRingPtr
    result.cqRingSize = 0 # Won't munmap separately
  else:
    result.cqRingPtr = mmap(
      nil,
      actualCqRingSize,
      PROT_READ or PROT_WRITE,
      MAP_SHARED or MAP_POPULATE,
      fd,
      Off(IORING_OFF_CQ_RING),
    )
    if result.cqRingPtr == MAP_FAILED:
      discard munmap(result.sqRingPtr, result.sqRingSize)
      discard close(fd)
      raiseOSError(osLastError(), "mmap CQ ring failed")
    result.cqRingSize = actualCqRingSize

  # mmap SQEs
  result.sqePtr = mmap(
    nil,
    sqeSize,
    PROT_READ or PROT_WRITE,
    MAP_SHARED or MAP_POPULATE,
    fd,
    Off(IORING_OFF_SQES),
  )
  if result.sqePtr == MAP_FAILED:
    if not singleMmap and result.cqRingPtr != nil:
      discard munmap(result.cqRingPtr, result.cqRingSize)
    discard munmap(result.sqRingPtr, result.sqRingSize)
    discard close(fd)
    raiseOSError(osLastError(), "mmap SQEs failed")
  result.sqeSize = sqeSize

  # Setup SQ ring pointers
  let sqBase = cast[uint64](result.sqRingPtr)
  result.sqHead = cast[ptr uint32](sqBase + params.sqOff.head.uint64)
  result.sqTail = cast[ptr uint32](sqBase + params.sqOff.tail.uint64)
  result.sqMask = cast[ptr uint32](sqBase + params.sqOff.ringMask.uint64)
  result.sqEntries = cast[ptr uint32](sqBase + params.sqOff.ringEntries.uint64)
  result.sqFlags = cast[ptr uint32](sqBase + params.sqOff.flags.uint64)
  result.sqArray = cast[ptr UncheckedArray[uint32]](sqBase + params.sqOff.array.uint64)

  # Setup SQEs pointer
  result.sqes = cast[ptr UncheckedArray[IoUringSqe]](result.sqePtr)

  # Setup CQ ring pointers
  let cqBase = cast[uint64](result.cqRingPtr)
  result.cqHead = cast[ptr uint32](cqBase + params.cqOff.head.uint64)
  result.cqTail = cast[ptr uint32](cqBase + params.cqOff.tail.uint64)
  result.cqMask = cast[ptr uint32](cqBase + params.cqOff.ringMask.uint64)
  result.cqEntries = cast[ptr uint32](cqBase + params.cqOff.ringEntries.uint64)
  result.cqes = cast[ptr UncheckedArray[IoUringCqe]](cqBase + params.cqOff.cqes.uint64)

  # Initialize local SQ tail from kernel
  result.sqLocalTail = atomicLoadAcquire(result.sqTail)

proc closeRing*(ring: var IoUring) =
  ## Clean up all ring resources (munmap regions and close fd).
  ## Safe to call on an already-closed ring (no-op).
  if ring.ringFd < 0:
    return

  # munmap SQEs
  if ring.sqePtr != nil:
    discard munmap(ring.sqePtr, ring.sqeSize)
    ring.sqePtr = nil

  # munmap CQ ring (if separate from SQ ring)
  if ring.cqRingPtr != nil and ring.cqRingPtr != ring.sqRingPtr and ring.cqRingSize > 0:
    discard munmap(ring.cqRingPtr, ring.cqRingSize)
  ring.cqRingPtr = nil

  # munmap SQ ring
  if ring.sqRingPtr != nil:
    discard munmap(ring.sqRingPtr, ring.sqRingSize)
    ring.sqRingPtr = nil

  # Close ring fd
  discard close(ring.ringFd)
  ring.ringFd = -1

# SQE / CQE operations

proc getSqe*(ring: var IoUring): ptr IoUringSqe =
  ## Get next available SQE slot. Returns nil if SQ is full.
  ## The returned SQE is zeroed.
  let head = atomicLoadAcquire(ring.sqHead)
  let mask = ring.sqMask[]
  let entries = ring.sqEntries[]

  if ring.sqLocalTail - head >= entries:
    return nil

  let idx = ring.sqLocalTail and mask
  result = addr ring.sqes[idx]
  zeroMem(result, sizeof(IoUringSqe))

  # Fill SQ array entry
  ring.sqArray[idx] = idx

  ring.sqLocalTail += 1

proc setLastSqeUserData*(ring: var IoUring, userData: uint64) =
  ## Set userData on the most recently obtained SQE (via getSqe).
  ## Must be called after getSqe and before submit.
  let idx = (ring.sqLocalTail - 1) and ring.sqMask[]
  ring.sqes[idx].userData = userData

proc submit*(ring: var IoUring, waitNr: uint32 = 0): cint =
  ## Submit pending SQEs to the kernel.
  ## Returns number of SQEs submitted, or negative errno.
  ## When waitNr > 0, waits for at least that many CQEs even if no new SQEs.
  let tail = ring.sqLocalTail
  let prevTail = atomicLoadAcquire(ring.sqTail)
  let toSubmit = tail - prevTail

  if toSubmit == 0 and waitNr == 0:
    return 0

  # Publish SQ tail with release ordering
  if toSubmit > 0:
    atomicStoreRelease(ring.sqTail, tail)

  var flags = 0'u32
  if waitNr > 0:
    flags = flags or IORING_ENTER_GETEVENTS

  result =
    ioUringEnter(ring.ringFd, cuint(toSubmit), cuint(waitNr), cuint(flags), nil, 0)

  if result < 0 and toSubmit > 0:
    # io_uring_enter failure with negative return guarantees 0 SQEs consumed.
    # Roll back so zombie SQEs are invisible to kernel and slots are reusable.
    # NOTE: Safe only in non-SQPOLL mode (kernel reads SQ ring only during io_uring_enter).
    atomicStoreRelease(ring.sqTail, prevTail)
    ring.sqLocalTail = prevTail

proc peekCqe*(ring: var IoUring): ptr IoUringCqe =
  ## Peek at the next completed CQE. Returns nil if none available.
  let head = ring.cqHead[]
  let tail = atomicLoadAcquire(ring.cqTail)

  if head == tail:
    return nil

  let mask = ring.cqMask[]
  result = addr ring.cqes[head and mask]

proc advanceCq*(ring: var IoUring) =
  ## Advance CQ head by one, consuming the current CQE.
  let head = ring.cqHead[] + 1
  atomicStoreRelease(ring.cqHead, head)

proc nopifySqe*(ring: var IoUring, userData: uint64) =
  ## Replace an unsubmitted SQE with NOP by its userData.
  ## Prevents the kernel from executing a locally-cancelled operation.
  let tail = atomicLoadAcquire(ring.sqTail)
  let mask = ring.sqMask[]
  var idx = tail
  while idx != ring.sqLocalTail:
    let sqeIdx = idx and mask
    if ring.sqes[sqeIdx].userData == userData:
      zeroMem(addr ring.sqes[sqeIdx], sizeof(IoUringSqe))
      ring.sqes[sqeIdx].opcode = IORING_OP_NOP
      ring.sqes[sqeIdx].userData = userData
      return
    idx += 1

proc registerEventfd*(ring: var IoUring, efd: cint) {.raises: [OSError].} =
  ## Register an eventfd with the io_uring ring. Raises OSError on failure.
  var efdVal = cint(efd)
  let res = ioUringRegister(ring.ringFd, IORING_REGISTER_EVENTFD, addr efdVal, 1)
  if res < 0:
    raiseOSError(osLastError(), "io_uring_register eventfd failed")

proc unregisterEventfd*(ring: var IoUring) =
  ## Unregister eventfd from the io_uring ring.
  discard ioUringRegister(ring.ringFd, IORING_UNREGISTER_EVENTFD, nil, 0)
