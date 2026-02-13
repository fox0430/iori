## Tests for uring_raw: struct sizes, ring setup/teardown, NOP submit/complete.

import std/[unittest, posix]

import ../iori/uring_raw
import ../iori/async_backend

proc eventfd(initval: cuint, flags: cint): cint {.importc, header: "<sys/eventfd.h>".}

const
  EFD_NONBLOCK = 0x00000800.cint
  EFD_CLOEXEC = 0x00080000.cint

suite "uring_raw":
  test "IoUringSqe is 64 bytes":
    check sizeof(IoUringSqe) == 64

  test "IoUringCqe is 16 bytes":
    check sizeof(IoUringCqe) == 16

  test "ring setup and teardown":
    var ring = setupRing(32)
    check ring.ringFd >= 0
    closeRing(ring)
    check ring.ringFd == -1

  test "NOP submit and complete":
    var ring = setupRing(32)
    defer:
      closeRing(ring)

    let sqe = getSqe(ring)
    check sqe != nil
    sqe.opcode = IORING_OP_NOP
    sqe.userData = 42

    let submitted = submit(ring)
    check submitted >= 1

    # Wait for completion
    discard submit(ring, waitNr = 1)

    let cqe = peekCqe(ring)
    check cqe != nil
    check cqe.userData == 42
    check cqe.res == 0
    advanceCq(ring)

    # After advancing, no more CQEs
    check peekCqe(ring) == nil

  test "getSqe returns nil when full":
    var ring = setupRing(4)
    defer:
      closeRing(ring)

    # Fill all SQE slots
    var count = 0
    while true:
      let sqe = getSqe(ring)
      if sqe == nil:
        break
      sqe.opcode = IORING_OP_NOP
      count += 1

    check count >= 4

  test "submit waitNr > 0 with no new SQEs waits for CQE":
    var ring = setupRing(32)
    defer:
      closeRing(ring)

    # Submit a NOP first (without waiting)
    let sqe = getSqe(ring)
    check sqe != nil
    sqe.opcode = IORING_OP_NOP
    sqe.userData = 99
    let submitted = submit(ring)
    check submitted >= 1

    # Now call submit with waitNr=1 but no new SQEs — should block until CQE ready
    let waitRes = submit(ring, waitNr = 1)
    check waitRes >= 0

    let cqe = peekCqe(ring)
    check cqe != nil
    check cqe.userData == 99
    advanceCq(ring)

  test "closeRing is idempotent":
    var ring = setupRing(4)
    check ring.ringFd >= 0
    closeRing(ring)
    check ring.ringFd == -1
    # Second close should be a no-op, not crash
    closeRing(ring)
    check ring.ringFd == -1

  test "submit with no SQEs and waitNr=0 returns 0":
    var ring = setupRing(4)
    defer:
      closeRing(ring)

    let ret = submit(ring)
    check ret == 0

  test "setLastSqeUserData sets correct field":
    var ring = setupRing(4)
    defer:
      closeRing(ring)

    let sqe = getSqe(ring)
    check sqe != nil
    sqe.opcode = IORING_OP_NOP

    setLastSqeUserData(ring, 12345)
    check sqe.userData == 12345

  test "multiple SQE batch submit":
    var ring = setupRing(32)
    defer:
      closeRing(ring)

    # Prepare 3 NOPs without submitting in between
    for i in 0 ..< 3:
      let sqe = getSqe(ring)
      check sqe != nil
      sqe.opcode = IORING_OP_NOP
      sqe.userData = uint64(100 + i)

    # Submit all 3 at once
    let submitted = submit(ring)
    check submitted == 3

    # Wait for all completions
    discard submit(ring, waitNr = 3)

    # Collect all 3 CQEs
    var ids: seq[uint64]
    for i in 0 ..< 3:
      let cqe = peekCqe(ring)
      check cqe != nil
      check cqe.res == 0
      ids.add(cqe.userData)
      advanceCq(ring)

    check peekCqe(ring) == nil

    # All 3 IDs should be present (order may vary)
    check 100'u64 in ids
    check 101'u64 in ids
    check 102'u64 in ids

  test "registerEventfd and unregisterEventfd":
    var ring = setupRing(32)
    defer:
      closeRing(ring)

    let efd = eventfd(0, EFD_NONBLOCK or EFD_CLOEXEC)
    check efd >= 0
    defer:
      discard close(efd)

    registerEventfd(ring, efd)

    # Submit a NOP and wait — eventfd should be signaled
    let sqe = getSqe(ring)
    check sqe != nil
    sqe.opcode = IORING_OP_NOP
    discard submit(ring, waitNr = 1)

    var buf: uint64
    let ret = read(efd, addr buf, sizeof(buf))
    check ret == sizeof(buf)
    check buf > 0'u64

    let cqe = peekCqe(ring)
    check cqe != nil
    advanceCq(ring)

    # Unregister — subsequent NOP should not signal eventfd
    unregisterEventfd(ring)

    let sqe2 = getSqe(ring)
    check sqe2 != nil
    sqe2.opcode = IORING_OP_NOP
    discard submit(ring, waitNr = 1)

    let ret2 = read(efd, addr buf, sizeof(buf))
    check ret2 < 0 # EAGAIN — no signal
    check errno == posix.EAGAIN

    let cqe2 = peekCqe(ring)
    check cqe2 != nil
    advanceCq(ring)

  test "all public sync functions callable from async":
    ## Compile-time regression: ensures the compiler's raises inference
    ## allows all uring_raw sync functions to be called from async procs.
    proc run() {.async.} =
      # setupRing, closeRing
      var ring = setupRing(4)

      # registerEventfd, unregisterEventfd
      let efd = eventfd(0, EFD_NONBLOCK or EFD_CLOEXEC)
      doAssert efd >= 0
      registerEventfd(ring, efd)
      unregisterEventfd(ring)
      discard close(efd)

      # getSqe, setLastSqeUserData, submit, peekCqe, advanceCq
      let sqe = getSqe(ring)
      doAssert sqe != nil
      sqe.opcode = IORING_OP_NOP
      setLastSqeUserData(ring, 1)
      let ret = submit(ring)
      doAssert ret >= 0
      discard submit(ring, waitNr = 1)
      let cqe = peekCqe(ring)
      doAssert cqe != nil
      doAssert cqe.res == 0
      advanceCq(ring)

      closeRing(ring)

    waitFor run()
