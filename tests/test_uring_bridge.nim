## Tests for uring_bridge: Low-level API and lifecycle.

import std/[unittest, os, posix, strutils, importutils]

import ../iori/[uring_bridge, uring_raw]

when hasChronos:
  import std/tables

privateAccess(UringFileIO)

suite "uring_bridge":
  var io {.threadvar.}: UringFileIO

  setup:
    io = newUringFileIO(256)

  teardown:
    io.close()

  test "open nonexistent file returns error":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let fdRes =
          await io.uringOpen("/tmp/iori_nonexistent_" & $getpid(), O_RDONLY, 0)
        doAssert fdRes < 0 # Should be negative errno (e.g. -ENOENT)

    waitFor run()

  test "close is idempotent":
    var io2 = newUringFileIO(32)
    io2.close()
    # Second close should be a no-op
    io2.close()

  test "SQ full fails with IOError":
    var io2 = newUringFileIO(1)
    defer:
      io2.close()

    # Fill all SQ slots without submitting
    var filled = 0
    while getSqe(io2.ring) != nil:
      inc filled
    doAssert filled >= 1

    proc run() {.async.} =
      {.cast(gcsafe).}:
        var raised = false
        try:
          discard await io2.uringOpen("/tmp/iori_sq_full_test", O_RDONLY, 0)
        except IOError as e:
          doAssert "SQ full" in e.msg
          raised = true
        doAssert raised

    waitFor run()

  test "batch submit: multiple SQEs queued before await":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        # Queue multiple uringOpen calls without awaiting — they should all
        # be submitted in a single flush and complete successfully.
        let fut1 = io.uringOpen("/dev/null", O_RDONLY, 0)
        let fut2 = io.uringOpen("/dev/null", O_RDONLY, 0)
        let fut3 = io.uringOpen("/dev/null", O_RDONLY, 0)

        let fd1 = await fut1
        let fd2 = await fut2
        let fd3 = await fut3

        doAssert fd1 >= 0, "fd1 should be valid: " & $fd1
        doAssert fd2 >= 0, "fd2 should be valid: " & $fd2
        doAssert fd3 >= 0, "fd3 should be valid: " & $fd3

        # Clean up
        discard await io.uringClose(fd1)
        discard await io.uringClose(fd2)
        discard await io.uringClose(fd3)

    waitFor run()

  test "flush then re-queue works":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        # First batch: open a file
        let fd1 = await io.uringOpen("/dev/null", O_RDONLY, 0)
        doAssert fd1 >= 0

        # Second batch (after flush): open another file
        let fd2 = await io.uringOpen("/dev/null", O_RDONLY, 0)
        doAssert fd2 >= 0

        discard await io.uringClose(fd1)
        discard await io.uringClose(fd2)

    waitFor run()

  test "close fails unsubmitted SQEs before flush":
    ## With batch submit, close() may run before the scheduled flush fires.
    ## The queued-but-unsubmitted SQE must still be failed via the pending table.
    var io2 = newUringFileIO(32)

    proc run() {.async.} =
      {.cast(gcsafe).}:
        let fd = cint(posix.open("/dev/zero", O_RDONLY))
        doAssert fd >= 0
        defer:
          discard posix.close(fd)

        var bufRef = new(seq[byte])
        bufRef[] = newSeq[byte](4096)
        let fut = io2.uringRead(fd, addr bufRef[][0], 4096, 0'u64, bufRef)

        # Close immediately — flush has not fired yet, SQE is unsubmitted
        io2.close()

        var raised = false
        try:
          discard await fut
        except IOError:
          raised = true
        doAssert raised

    waitFor run()

  test "manual flush submits queued SQEs":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        # Queue SQEs without awaiting
        let fut1 = io.uringOpen("/dev/null", O_RDONLY, 0)
        let fut2 = io.uringOpen("/dev/null", O_RDONLY, 0)

        # Manually flush instead of waiting for callSoon
        io.flush()

        let fd1 = await fut1
        let fd2 = await fut2

        doAssert fd1 >= 0, "fd1 should be valid: " & $fd1
        doAssert fd2 >= 0, "fd2 should be valid: " & $fd2

        discard await io.uringClose(fd1)
        discard await io.uringClose(fd2)

    waitFor run()

  test "batch mixed operation types":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        # Open two files without awaiting — both queued in same batch
        let futFd1 = io.uringOpen("/dev/null", O_RDONLY, 0)
        let futFd2 = io.uringOpen("/dev/zero", O_RDONLY, 0)

        let fd1 = await futFd1
        let fd2 = await futFd2
        doAssert fd1 >= 0
        doAssert fd2 >= 0

        # Queue a read and a close in the same batch
        var bufRef = new(seq[byte])
        bufRef[] = newSeq[byte](16)
        let futRead = io.uringRead(fd2, addr bufRef[][0], 16, 0'u64, bufRef)
        let futClose1 = io.uringClose(fd1)

        let bytesRead = await futRead
        let closeRes1 = await futClose1
        doAssert bytesRead >= 0
        doAssert closeRes1 == 0

        discard await io.uringClose(fd2)

    waitFor run()

  test "uringStatx on existing file returns size and mode":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let path = getTempDir() / "iori_test_statx.txt"
        defer:
          removeFile(path)

        # Create file with known content using posix
        let fd = posix.open(path.cstring, O_WRONLY or O_CREAT or O_TRUNC, 0o644)
        doAssert fd >= 0
        let data = "hello"
        doAssert posix.write(fd, data.cstring, data.len) == data.len
        discard posix.close(fd)

        var stx = new(Statx)
        let res = await io.uringStatx(path, 0.cint, STATX_BASIC_STATS, stx)
        doAssert res == 0
        doAssert stx.stxSize == 5
        doAssert (stx.stxMode and 0o170000'u16) == 0o100000'u16 # S_IFREG

    waitFor run()

  test "uringStatx on nonexistent file returns negative errno":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        var stx = new(Statx)
        let res = await io.uringStatx(
          "/tmp/iori_nonexistent_statx_" & $getpid(), 0.cint, STATX_BASIC_STATS, stx
        )
        doAssert res < 0 # -ENOENT

    waitFor run()

  test "path string survives after queueing (regression: dangling cstring)":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let path = getTempDir() / "iori_test_dangling.txt"
        defer:
          removeFile(path)

        let fd = posix.open(path.cstring, O_WRONLY or O_CREAT or O_TRUNC, 0o644)
        doAssert fd >= 0
        discard posix.close(fd)

        # Queue statx without awaiting — SQE holds raw cstring pointer
        var stx = new(Statx)
        let fut = io.uringStatx(path, 0.cint, STATX_BASIC_STATS, stx)

        # Allocate many strings to overwrite freed memory.
        # If the cstring pointer was dangling, this makes the kernel read garbage.
        var junk: seq[string]
        for i in 0 ..< 1000:
          junk.add("XXXXXXXXXXXXXXXXXXXXXXXXXXXX" & $i)

        let res = await fut
        doAssert res == 0, "statx failed with " & $res & " (expected 0)"

    waitFor run()

  test "write buffer survives after queueing (regression: dangling bufRef)":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let path = getTempDir() / "iori_test_dangling_buf.bin"
        defer:
          removeFile(path)

        let fdRes = await io.uringOpen(path, O_WRONLY or O_CREAT or O_TRUNC, 0o644)
        doAssert fdRes >= 0

        # Queue write without awaiting — SQE holds raw pointer into bufRef
        var writeFut: Future[int32]
        block:
          var bufRef = new(seq[byte])
          bufRef[] = @[byte 1, 2, 3, 4, 5]
          writeFut = io.uringWrite(fdRes.cint, addr bufRef[][0], 5, 0'u64, bufRef)
          # bufRef goes out of scope here; Completion must keep the data alive

        # Allocate junk to overwrite freed memory.
        # If the buffer pointer was dangling, the kernel writes garbage to the file.
        var junk: seq[seq[byte]]
        for i in 0 ..< 1000:
          var j = newSeq[byte](8)
          for k in 0 ..< j.len:
            j[k] = 0xFF'u8
          junk.add(j)

        let writeRes = await writeFut
        doAssert writeRes == 5, "write failed with " & $writeRes & " (expected 5)"
        discard await io.uringClose(fdRes.cint)

        # Read back with posix and verify the data is correct
        let rfd = posix.open(path.cstring, O_RDONLY)
        doAssert rfd >= 0
        var readBuf: array[5, byte]
        doAssert posix.read(rfd, addr readBuf[0], 5) == 5
        discard posix.close(rfd)
        doAssert readBuf == [byte 1, 2, 3, 4, 5]

    waitFor run()

  test "uringRenameat renames file":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let oldPath = getTempDir() / "iori_test_rename_old.txt"
        let newPath = getTempDir() / "iori_test_rename_new.txt"
        defer:
          removeFile(oldPath)
          removeFile(newPath)

        # Create file using posix
        let fd = posix.open(oldPath.cstring, O_WRONLY or O_CREAT or O_TRUNC, 0o644)
        doAssert fd >= 0
        let data = "rename test"
        doAssert posix.write(fd, data.cstring, data.len) == data.len
        discard posix.close(fd)

        let res = await io.uringRenameat(oldPath, newPath)
        doAssert res == 0
        doAssert not fileExists(oldPath)
        doAssert fileExists(newPath)

    waitFor run()

  test "uringRenameat on nonexistent source returns negative errno":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let res = await io.uringRenameat(
          "/tmp/iori_nonexistent_rename_" & $getpid(),
          "/tmp/iori_nonexistent_rename_dst_" & $getpid(),
        )
        doAssert res < 0 # -ENOENT

    waitFor run()

  test "uringRead on pre-closed fd returns negative errno":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        # Open a file via io_uring, then close the fd with posix.close
        # to simulate a bad fd scenario — exercises the read error path.
        let fdRes = await io.uringOpen("/dev/null", O_RDONLY, 0)
        doAssert fdRes >= 0
        discard posix.close(fdRes.cint)

        var bufRef = new(seq[byte])
        bufRef[] = newSeq[byte](64)
        let readRes =
          await io.uringRead(fdRes.cint, addr bufRef[][0], 64, 0'u64, bufRef)
        doAssert readRes == -9 # -EBADF

    waitFor run()

  test "uringFsync succeeds on regular file":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let path = getTempDir() / "iori_test_fsync.bin"
        defer:
          removeFile(path)

        let fdRes = await io.uringOpen(path, O_WRONLY or O_CREAT or O_TRUNC, 0o644)
        doAssert fdRes >= 0

        var bufRef = new(seq[byte])
        bufRef[] = @[byte 1, 2, 3, 4, 5]
        let writeRes =
          await io.uringWrite(fdRes.cint, addr bufRef[][0], 5, 0'u64, bufRef)
        doAssert writeRes == 5

        let fsyncRes = await io.uringFsync(fdRes.cint)
        doAssert fsyncRes == 0

        discard await io.uringClose(fdRes.cint)

    waitFor run()

  test "uringFsync with dataOnly (fdatasync) succeeds on regular file":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let path = getTempDir() / "iori_test_fdatasync.bin"
        defer:
          removeFile(path)

        let fdRes = await io.uringOpen(path, O_WRONLY or O_CREAT or O_TRUNC, 0o644)
        doAssert fdRes >= 0

        var bufRef = new(seq[byte])
        bufRef[] = @[byte 1, 2, 3, 4, 5]
        let writeRes =
          await io.uringWrite(fdRes.cint, addr bufRef[][0], 5, 0'u64, bufRef)
        doAssert writeRes == 5

        let fsyncRes = await io.uringFsync(fdRes.cint, dataOnly = true)
        doAssert fsyncRes == 0

        discard await io.uringClose(fdRes.cint)

    waitFor run()

  test "API calls after close fail with IOError":
    var io2 = newUringFileIO(32)
    io2.close()

    proc run() {.async.} =
      {.cast(gcsafe).}:
        # All low-level API procs should fail immediately on a closed instance.
        var raised = false
        try:
          discard await io2.uringOpen("/dev/null", O_RDONLY, 0)
        except IOError:
          raised = true
        doAssert raised

        raised = false
        var bufRef = new(seq[byte])
        bufRef[] = newSeq[byte](64)
        try:
          discard await io2.uringRead(0.cint, addr bufRef[][0], 64, 0'u64, bufRef)
        except IOError:
          raised = true
        doAssert raised

        raised = false
        try:
          discard await io2.uringClose(0.cint)
        except IOError:
          raised = true
        doAssert raised

    waitFor run()

  test "error field propagates to pending futures on close":
    var io2 = newUringFileIO(32)

    proc run() {.async.} =
      {.cast(gcsafe).}:
        # Queue an operation but don't await yet
        let fut = io2.uringOpen("/dev/null", O_RDONLY, 0)

        # Set error reason and close
        let reason = newException(OSError, "test error reason")
        io2.error = reason
        io2.close()

        var caught: ref CatchableError
        try:
          discard await fut
        except CatchableError as e:
          caught = e
        doAssert caught != nil
        doAssert caught == reason

    waitFor run()

  test "error field propagates to API calls after close":
    var io2 = newUringFileIO(32)
    let reason = newException(OSError, "injected error")
    io2.error = reason
    io2.close()

    proc run() {.async.} =
      {.cast(gcsafe).}:
        var caught: ref CatchableError
        try:
          discard await io2.uringOpen("/dev/null", O_RDONLY, 0)
        except CatchableError as e:
          caught = e
        doAssert caught == reason

    waitFor run()

  test "uringCancel cancels kernel-blocked read":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        # Create a pipe — reading from read-end blocks until data arrives
        var fds: array[2, cint]
        doAssert pipe(fds) == 0
        let readFd = fds[0]
        let writeFd = fds[1]
        defer:
          discard posix.close(readFd)
          discard posix.close(writeFd)

        var bufRef = new(seq[byte])
        bufRef[] = newSeq[byte](64)
        let readFut = io.uringRead(readFd, addr bufRef[][0], 64, 0'u64, bufRef)
        io.flush()

        # Cancel the blocked read
        let cancelRes = await io.uringCancel(readFut)
        doAssert cancelRes == 0, "cancel should succeed: " & $cancelRes

        let readRes = await readFut
        doAssert readRes == -125, "read should be -ECANCELED: " & $readRes

    waitFor run()

  test "uringCancel cancels unsubmitted operation":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        # Queue a read without flushing — it stays in unsubmitted
        let fd = cint(posix.open("/dev/zero", O_RDONLY))
        doAssert fd >= 0
        defer:
          discard posix.close(fd)

        var bufRef = new(seq[byte])
        bufRef[] = newSeq[byte](64)
        let readFut = io.uringRead(fd, addr bufRef[][0], 64, 0'u64, bufRef)

        # Cancel before flush — should be a local cancel
        let cancelRes = await io.uringCancel(readFut)
        doAssert cancelRes == 0

        let readRes = await readFut
        doAssert readRes == -125, "read should be -ECANCELED: " & $readRes

    waitFor run()

  test "uringCancel on closed instance fails with IOError":
    var io2 = newUringFileIO(32)
    io2.close()

    proc run() {.async.} =
      {.cast(gcsafe).}:
        let dummyFut = newFuture[int32]("dummy")
        var raised = false
        try:
          discard await io2.uringCancel(dummyFut)
        except IOError:
          raised = true
        doAssert raised

    waitFor run()

  test "futureToId cleaned on flush submit failure":
    var io2 = newUringFileIO(32)
    defer:
      io2.close()

    proc run() {.async.} =
      {.cast(gcsafe).}:
        # Queue an operation (stays unsubmitted)
        let fut = io2.uringOpen("/dev/null", O_RDONLY, 0)

        # Sabotage ring fd to force submit failure
        let savedFd = io2.ring.ringFd
        io2.ring.ringFd = -1
        io2.flush()
        io2.ring.ringFd = savedFd

        # fut should have been failed by flush
        var raised = false
        try:
          discard await fut
        except IOError:
          raised = true
        doAssert raised

        # futureToId must be clean — cancel should get "not found"
        raised = false
        try:
          discard await io2.uringCancel(fut)
        except IOError as e:
          doAssert "target operation not found" in e.msg
          raised = true
        doAssert raised

    waitFor run()

  test "uringCancel on completed operation fails with 'not found'":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let fdRes = await io.uringOpen("/dev/null", O_RDONLY, 0)
        doAssert fdRes >= 0

        let closeFut = io.uringClose(fdRes)
        let closeRes = await closeFut

        doAssert closeRes == 0

        # closeFut is already completed — futureToId entry removed
        var raised = false
        try:
          discard await io.uringCancel(closeFut)
        except IOError as e:
          doAssert "target operation not found" in e.msg
          raised = true
        doAssert raised

    waitFor run()

  when hasChronos:
    # Regression tests for external (chronos) cancellation of bridge futures.
    # Without the cancel callback, cancelling a bridge future does not stop the
    # kernel op: it lingers in `pending` (buffer rooted) and unsubmitted ops are
    # still flushed and executed. With it, external cancellation settles the
    # future in the Cancelled state (consistently for submitted and unsubmitted
    # ops) and the op's CQE is reaped. The `finished` guard in processCqes hardens
    # the completion path against re-finishing an externally-finished future.

    # Budget for waiting on the kernel -ECANCELED CQE (reaped via the eventfd
    # poll loop): generous so a loaded CI box does not flake, while a genuinely
    # dropped cancel still fails the assert because `pending` never drains.
    const
      reapIters = 250
      reapStepMs = 2

    test "chronos cancelAndWait of submitted bridge future reaps op safely":
      proc run() {.async.} =
        {.cast(gcsafe).}:
          # Pipe read-end blocks until data arrives — the op stays in flight.
          var fds: array[2, cint]
          doAssert pipe(fds) == 0
          let readFd = fds[0]
          let writeFd = fds[1]
          defer:
            discard posix.close(readFd)
            discard posix.close(writeFd)

          var bufRef = new(seq[byte])
          bufRef[] = newSeq[byte](64)
          let readFut = io.uringRead(readFd, addr bufRef[][0], 64, 0'u64, bufRef)
          io.flush()
          # `unsubmitted` empty proves flush actually submitted the SQE to the
          # kernel (the op is in-flight), not merely that it was queued; `pending`
          # tracks the in-flight Completion.
          doAssert io.unsubmitted.len == 0
          doAssert io.pending.len == 1

          # External cancellation: the cancel callback must issue ASYNC_CANCEL, and
          # chronos moves the future to Cancelled.
          await readFut.cancelAndWait()
          doAssert readFut.cancelled()

          # Drive the loop: the kernel cancel CQE (-ECANCELED) must be reaped by
          # processCqes (skipping completion of the finished future), not crash.
          for _ in 0 ..< reapIters:
            if io.pending.len == 0:
              break
            await sleepMsAsync(reapStepMs)
          doAssert io.pending.len == 0, "op not reaped: " & $io.pending.len

      waitFor run()

    test "chronos cancelAndWait of unsubmitted bridge future is safe":
      proc run() {.async.} =
        {.cast(gcsafe).}:
          let fd = cint(posix.open("/dev/zero", O_RDONLY))
          doAssert fd >= 0
          defer:
            discard posix.close(fd)

          var bufRef = new(seq[byte])
          bufRef[] = newSeq[byte](64)
          # No flush — the op stays unsubmitted, so cancellation neutralizes the SQE.
          let readFut = io.uringRead(fd, addr bufRef[][0], 64, 0'u64, bufRef)

          await readFut.cancelAndWait()
          doAssert readFut.cancelled()

          # Awaiting a cancelled future raises CancelledError (chronos semantics),
          # consistent with the submitted-op path — not a -125 value.
          var raised = false
          try:
            discard await readFut
          except CancelledError:
            raised = true
          doAssert raised

          # The unsubmitted path drops the Completion synchronously inside the
          # cancel callback (no kernel roundtrip, no CQE to wait for), so the
          # buffer's GC root is already released by the time cancelAndWait
          # returns. The neutralized SQE was never submitted; if a later op ever
          # flushes it as a stale NOP, processCqes ignores it (id no longer in
          # `pending`).
          doAssert io.pending.len == 0, "completion not dropped: " & $io.pending.len

      waitFor run()

    test "chronos cancel during chain build does not graft into the chain":
      # Regression: cancelling an unrelated *submitted* op while a linked chain
      # is mid-construction must NOT insert an ASYNC_CANCEL SQE into the chain.
      # io_uring's IOSQE_IO_LINK links each SQE to the one physically following
      # it, so a grafted SQE would split the chain and cancel the next user op
      # (silent data loss); endChain would also return N+1 futures. The cancel is
      # deferred until after endChain instead.
      proc run() {.async.} =
        {.cast(gcsafe).}:
          # An in-flight (submitted) blocking pipe read, cancellable mid-chain.
          var fds: array[2, cint]
          doAssert pipe(fds) == 0
          let readFd = fds[0]
          let writeFd = fds[1]
          defer:
            discard posix.close(readFd)
            discard posix.close(writeFd)

          var blkRef = new(seq[byte])
          blkRef[] = newSeq[byte](64)
          let blockingFut = io.uringRead(readFd, addr blkRef[][0], 64, 0'u64, blkRef)
          io.flush()
          doAssert io.unsubmitted.len == 0 # actually submitted (in-flight)
          doAssert io.pending.len == 1

          let fd = cint(posix.open("/dev/zero", O_RDONLY))
          doAssert fd >= 0
          defer:
            discard posix.close(fd)
          var b1 = new(seq[byte])
          b1[] = newSeq[byte](8)
          var b2 = new(seq[byte])
          b2[] = newSeq[byte](8)

          # Build a 2-op chain; cancel the unrelated submitted op between the ops.
          io.beginChain()
          let f1 = io.uringRead(fd, addr b1[][0], 8, 0'u64, b1)
          blockingFut.cancelSoon() # fires the cancel callback inline (chainActive)
          doAssert io.chainFutures.len == 1, "chain grafted: " & $io.chainFutures.len
          let f2 = io.uringRead(fd, addr b2[][0], 8, 0'u64, b2)
          let futs = io.endChain()
          doAssert futs.len == 2, "endChain returned " & $futs.len & " futures (want 2)"

          # The chain ops still run correctly (8 zero bytes each from /dev/zero).
          let r1 = await f1
          let r2 = await f2
          doAssert r1 == 8, "f1: " & $r1
          doAssert r2 == 8, "f2: " & $r2

          # The cancelled op settles Cancelled; its deferred ASYNC_CANCEL reaps it.
          doAssert blockingFut.cancelled()
          for _ in 0 ..< reapIters:
            if io.pending.len == 0:
              break
            await sleepMsAsync(reapStepMs)
          doAssert io.pending.len == 0, "op not reaped: " & $io.pending.len

      waitFor run()

    test "chronos cancel of submitted op during a tick-spanning chain still reaps":
      # Regression for the deferred-cancel drop: when the cancel of an
      # unrelated *submitted* op arrives mid-chain, it is deferred until the
      # chain closes. If the chain outlives the tick on which the cancel fired
      # (the consumer awaits while the chain is open), the deferred cancel must
      # retry — not silently give up — or the kernel op is never cancelled and
      # its buffer stays GC-rooted in `pending` forever.
      proc run() {.async.} =
        {.cast(gcsafe).}:
          var fds: array[2, cint]
          doAssert pipe(fds) == 0
          let readFd = fds[0]
          let writeFd = fds[1]
          defer:
            discard posix.close(readFd)
            discard posix.close(writeFd)

          var blkRef = new(seq[byte])
          blkRef[] = newSeq[byte](64)
          let blockingFut = io.uringRead(readFd, addr blkRef[][0], 64, 0'u64, blkRef)
          io.flush()
          doAssert io.unsubmitted.len == 0
          doAssert io.pending.len == 1

          let fd = cint(posix.open("/dev/zero", O_RDONLY))
          doAssert fd >= 0
          defer:
            discard posix.close(fd)
          var b1 = new(seq[byte])
          b1[] = newSeq[byte](8)
          var b2 = new(seq[byte])
          b2[] = newSeq[byte](8)

          io.beginChain()
          let f1 = io.uringRead(fd, addr b1[][0], 8, 0'u64, b1)
          blockingFut.cancelSoon() # deferred (chainActive)
          await sleepMsAsync(10) # chain spans a tick: deferred cancel must retry
          let f2 = io.uringRead(fd, addr b2[][0], 8, 0'u64, b2)
          let futs = io.endChain()
          doAssert futs.len == 2, "endChain returned " & $futs.len & " futures (want 2)"

          let r1 = await f1
          let r2 = await f2
          doAssert r1 == 8, "f1: " & $r1
          doAssert r2 == 8, "f2: " & $r2

          doAssert blockingFut.cancelled()
          for _ in 0 ..< reapIters:
            if io.pending.len == 0:
              break
            await sleepMsAsync(reapStepMs)
          doAssert io.pending.len == 0, "deferred cancel dropped: " & $io.pending.len

      waitFor run()

    test "chronos cancel of submitted op when SQ is full defers and reaps":
      # Regression: if the SQ ring is full at the moment of external cancellation,
      # the kernel ASYNC_CANCEL cannot be queued immediately (getSqe returns nil).
      # It must be deferred and retried after the next flush frees a slot — not
      # silently dropped, which would leave the op running with its buffer
      # GC-rooted in `pending` forever.
      proc run() {.async.} =
        {.cast(gcsafe).}:
          # entries=4 (CQ has 8 slots, ample headroom); SQ capacity equals the
          # requested entries, so 4 unsubmitted ops fill it exactly.
          const ringEntries = 4
          let io2 = newUringFileIO(ringEntries)
          defer:
            io2.close()

          var fds: array[2, cint]
          doAssert pipe(fds) == 0
          let readFd = fds[0]
          let writeFd = fds[1]
          defer:
            discard posix.close(readFd)
            discard posix.close(writeFd)

          # A submitted, in-flight blocking pipe read — the op to cancel.
          var blkRef = new(seq[byte])
          blkRef[] = newSeq[byte](64)
          let blockingFut = io2.uringRead(readFd, addr blkRef[][0], 64, 0'u64, blkRef)
          io2.flush()
          doAssert io2.unsubmitted.len == 0
          doAssert io2.pending.len == 1

          # Fill every SQ slot with unsubmitted reads so the getSqe inside
          # uringCancel returns nil (SQ full).
          let fd = cint(posix.open("/dev/zero", O_RDONLY))
          doAssert fd >= 0
          defer:
            discard posix.close(fd)
          var fillFuts: seq[Future[int32]]
          var fillBufs: seq[ref seq[byte]] # keep buffers GC-rooted
          for _ in 0 ..< ringEntries:
            var fb = new(seq[byte])
            fb[] = newSeq[byte](8)
            fillBufs.add(fb)
            fillFuts.add(io2.uringRead(fd, addr fb[][0], 8, 0'u64, fb))
          doAssert io2.unsubmitted.len == ringEntries

          # Cancel the submitted op: the kernel cancel can't be issued now (SQ
          # full), so it must be deferred rather than dropped. cancelSoon fires
          # handleExternalCancel inline, before the fill ops' scheduled flush can
          # free a slot, so the SQ-full path is exercised deterministically. A
          # deferredCancels.len of 0 here would mean the SQ was not actually full.
          blockingFut.cancelSoon()
          doAssert blockingFut.cancelled()
          doAssert io2.deferredCancels.len == 1,
            "SQ-full cancel not deferred: " & $io2.deferredCancels.len

          # Awaiting the fill reads flushes them, freeing slots and draining the
          # deferred cancel; its ASYNC_CANCEL then reaps the blocking op.
          for f in fillFuts:
            doAssert (await f) == 8
          for _ in 0 ..< reapIters:
            if io2.pending.len == 0:
              break
            await sleepMsAsync(reapStepMs)
          doAssert io2.pending.len == 0,
            "deferred SQ-full cancel dropped: " & $io2.pending.len
          doAssert io2.deferredCancels.len == 0,
            "deferred cancel not cleared: " & $io2.deferredCancels.len

      waitFor run()

    test "chronos cancel of submitted op then empty endChain still reaps":
      # Regression: a cancel deferred because a chain was open must still be
      # issued when that chain closes EMPTY (no ops queued). endChain's empty-
      # chain path used to return before draining, stranding the kernel op with
      # its buffer GC-rooted in `pending` forever.
      proc run() {.async.} =
        {.cast(gcsafe).}:
          var fds: array[2, cint]
          doAssert pipe(fds) == 0
          let readFd = fds[0]
          let writeFd = fds[1]
          defer:
            discard posix.close(readFd)
            discard posix.close(writeFd)

          var blkRef = new(seq[byte])
          blkRef[] = newSeq[byte](64)
          let blockingFut = io.uringRead(readFd, addr blkRef[][0], 64, 0'u64, blkRef)
          io.flush()
          doAssert io.unsubmitted.len == 0
          doAssert io.pending.len == 1

          # Open a chain, cancel the unrelated submitted op (deferred because
          # chainActive), then close the chain WITHOUT queueing any op.
          io.beginChain()
          blockingFut.cancelSoon()
          doAssert blockingFut.cancelled()
          doAssert io.deferredCancels.len == 1,
            "cancel not deferred: " & $io.deferredCancels.len
          let futs = io.endChain()
          doAssert futs.len == 0

          # The empty endChain must have drained the deferred cancel and reaped
          # the op — not stranded it.
          for _ in 0 ..< reapIters:
            if io.pending.len == 0:
              break
            await sleepMsAsync(reapStepMs)
          doAssert io.pending.len == 0,
            "empty endChain stranded the cancel: " & $io.pending.len
          doAssert io.deferredCancels.len == 0

      waitFor run()

    test "chronos cancel of unsubmitted chain member is excluded from endChain":
      # Regression: cancelling a future that belongs to the chain being built
      # drops it from the returned future set (its SQE becomes a linked NOP)
      # while the rest of the chain still executes. endChain previously returned
      # the already-Cancelled member future and an off-by-one count.
      proc run() {.async.} =
        {.cast(gcsafe).}:
          let fd = cint(posix.open("/dev/zero", O_RDONLY))
          doAssert fd >= 0
          defer:
            discard posix.close(fd)
          var b1 = new(seq[byte])
          b1[] = newSeq[byte](8)
          var b2 = new(seq[byte])
          b2[] = newSeq[byte](8)

          io.beginChain()
          let f1 = io.uringRead(fd, addr b1[][0], 8, 0'u64, b1)
          let f2 = io.uringRead(fd, addr b2[][0], 8, 0'u64, b2)
          f1.cancelSoon() # cancel a member mid-build
          doAssert f1.cancelled()
          let futs = io.endChain()
          doAssert futs.len == 1,
            "endChain should exclude the cancelled member: " & $futs.len

          # The surviving op still runs (8 zero bytes from /dev/zero); the chain
          # link is intact (the cancelled member is a linked NOP).
          let r2 = await f2
          doAssert r2 == 8, "f2: " & $r2

          for _ in 0 ..< reapIters:
            if io.pending.len == 0:
              break
            await sleepMsAsync(reapStepMs)
          doAssert io.pending.len == 0, "pending not drained: " & $io.pending.len

      waitFor run()

    test "chronos cancel of sole chain member then empty endChain unblocks later ops":
      # Regression: cancelling the ONLY member of an in-build chain empties
      # chainFutures while its id stays in chainIds (its SQE is now a linked NOP).
      # endChain must NOT take the truly-empty early return — it has to clear the
      # dangling IOSQE_IO_LINK and re-arm flushScheduled, or later ops never flush.
      proc run() {.async.} =
        {.cast(gcsafe).}:
          let fd = cint(posix.open("/dev/zero", O_RDONLY))
          doAssert fd >= 0
          defer:
            discard posix.close(fd)

          var b1 = new(seq[byte])
          b1[] = newSeq[byte](8)
          io.beginChain()
          let f1 = io.uringRead(fd, addr b1[][0], 8, 0'u64, b1)
          f1.cancelSoon() # sole member cancelled mid-build
          doAssert f1.cancelled()
          let futs = io.endChain()
          doAssert futs.len == 0

          # A later unrelated op must still submit and complete.
          var b2 = new(seq[byte])
          b2[] = newSeq[byte](8)
          let g = io.uringRead(fd, addr b2[][0], 8, 0'u64, b2)
          let rg = await g
          doAssert rg == 8, "later op did not run: " & $rg

      waitFor run()

    test "chronos cancel of all chain members across a tick does not wedge submission":
      # Regression for the endChain empty-chain wedge: once the chain's scheduled
      # flush has already fired (returning early because chainActive) and then
      # every member is cancelled, the old `chainFutures.len == 0` early return
      # skipped the flushScheduled re-arm — leaving flushScheduled stuck true with
      # no flush pending, so every subsequent op stayed unsubmitted forever.
      proc run() {.async.} =
        {.cast(gcsafe).}:
          let fd = cint(posix.open("/dev/zero", O_RDONLY))
          doAssert fd >= 0
          defer:
            discard posix.close(fd)

          var b1 = new(seq[byte])
          b1[] = newSeq[byte](8)
          io.beginChain()
          let f1 = io.uringRead(fd, addr b1[][0], 8, 0'u64, b1)
          # Span a tick so the chain's scheduled flush fires while chainActive
          # (it returns early, leaving flushScheduled = true with no flush queued).
          await sleepMsAsync(10)
          f1.cancelSoon() # cancel the sole member
          doAssert f1.cancelled()
          let futs = io.endChain()
          doAssert futs.len == 0

          # A later op must submit and complete within a bounded budget; if the
          # instance is wedged it never finishes.
          var b2 = new(seq[byte])
          b2[] = newSeq[byte](8)
          let g = io.uringRead(fd, addr b2[][0], 8, 0'u64, b2)
          var done = false
          for _ in 0 ..< reapIters:
            if g.finished:
              done = true
              break
            await sleepMsAsync(reapStepMs)
          doAssert done, "submission wedged: later op never completed"
          doAssert g.read() == 8

      waitFor run()

    test "chronos cancel survives a submit() failure that rolls back its ASYNC_CANCEL":
      # Regression: when the kernel ASYNC_CANCEL for an externally-cancelled
      # submitted op is queued but the very next submit() fails, io_uring_enter
      # rolls the SQ ring back and the ASYNC_CANCEL is discarded. tryKernelCancel
      # had already reported success (so the target was not in deferredCancels), so
      # without the flush-failure re-defer the cancel was lost forever: the kernel
      # op kept running and its Completion+buffer stayed GC-rooted in `pending`.
      proc run() {.async.} =
        {.cast(gcsafe).}:
          let io2 = newUringFileIO(32)
          defer:
            io2.close()

          # A submitted, in-flight blocking pipe read — the op to cancel.
          var fds: array[2, cint]
          doAssert pipe(fds) == 0
          let readFd = fds[0]
          let writeFd = fds[1]
          defer:
            discard posix.close(readFd)
            discard posix.close(writeFd)

          var blkRef = new(seq[byte])
          blkRef[] = newSeq[byte](64)
          let blockingFut = io2.uringRead(readFd, addr blkRef[][0], 64, 0'u64, blkRef)
          io2.flush()
          doAssert io2.unsubmitted.len == 0
          doAssert io2.pending.len == 1

          # Cancel inline: handleExternalCancel queues an ASYNC_CANCEL (now sitting
          # in `unsubmitted`); the target is NOT in deferredCancels because
          # tryKernelCancel reported the queued cancel as success.
          blockingFut.cancelSoon()
          doAssert blockingFut.cancelled()
          doAssert io2.unsubmitted.len == 1
          doAssert io2.deferredCancels.len == 0

          # Force the flush that would submit the ASYNC_CANCEL to fail: the SQ ring
          # rolls back and the ASYNC_CANCEL is discarded. The re-defer must put the
          # still-pending Cancelled target back into deferredCancels.
          let savedFd = io2.ring.ringFd
          io2.ring.ringFd = -1
          io2.flush()
          io2.ring.ringFd = savedFd

          # The ASYNC_CANCEL op's own future was failed and dropped, but the target
          # must have been re-deferred (or already re-issued by the in-flush drain),
          # so its kernel op is eventually cancelled and `pending` drains. Without
          # the fix it stays at 1 forever.
          for _ in 0 ..< reapIters:
            if io2.pending.len == 0:
              break
            await sleepMsAsync(reapStepMs)
          doAssert io2.pending.len == 0,
            "submit-failure dropped the cancel; op leaked in pending: " &
              $io2.pending.len

      waitFor run()

  test "uringStatxFd returns correct size for open fd":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let path = getTempDir() / "iori_test_statxfd.txt"
        defer:
          removeFile(path)

        let fd = posix.open(path.cstring, O_WRONLY or O_CREAT or O_TRUNC, 0o644)
        doAssert fd >= 0
        let data = "hello"
        doAssert posix.write(fd, data.cstring, data.len) == data.len
        discard posix.close(fd)

        let fdRes = await io.uringOpen(path, O_RDONLY, 0)
        doAssert fdRes >= 0

        var stx = new(Statx)
        let res = await io.uringStatxFd(fdRes.cint, STATX_BASIC_STATS, stx)
        doAssert res == 0
        doAssert stx.stxSize == 5
        doAssert (stx.stxMode and 0o170000'u16) == 0o100000'u16 # S_IFREG

        discard await io.uringClose(fdRes.cint)

    waitFor run()

  test "uringStatxFd reflects fd not path after file replacement":
    ## TOCTOU proof: after opening a file, replacing the file at the same path
    ## must not affect statx on the original fd.
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let path = getTempDir() / "iori_test_statxfd_toctou.txt"
        defer:
          removeFile(path)

        # Create original file (5 bytes)
        let fd = posix.open(path.cstring, O_WRONLY or O_CREAT or O_TRUNC, 0o644)
        doAssert fd >= 0
        let data = "hello"
        doAssert posix.write(fd, data.cstring, data.len) == data.len
        discard posix.close(fd)

        # Open via io_uring (holds reference to original inode)
        let fdRes = await io.uringOpen(path, O_RDONLY, 0)
        doAssert fdRes >= 0

        # Replace file at same path: unlink + create new inode with larger content
        removeFile(path)
        let fd2 = posix.open(path.cstring, O_WRONLY or O_CREAT, 0o644)
        doAssert fd2 >= 0
        let data2 = "hello world!!"
        doAssert posix.write(fd2, data2.cstring, data2.len) == data2.len
        discard posix.close(fd2)

        # statxFd on the original fd must return 5, not 13
        var stx = new(Statx)
        let res = await io.uringStatxFd(fdRes.cint, STATX_SIZE, stx)
        doAssert res == 0
        doAssert stx.stxSize == 5, "expected 5 but got " & $stx.stxSize

        discard await io.uringClose(fdRes.cint)

    waitFor run()

  test "newUringFileIO, flush, and close callable from async":
    ## Compile-time regression: ensures all public sync functions in
    ## uring_bridge can be called from async procs without raises errors.
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let io2 = newUringFileIO()
        io2.flush()
        io2.close()

    waitFor run()

  test "chain: write + fsync succeeds":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let path = getTempDir() / "iori_test_chain_write_fsync.bin"
        defer:
          removeFile(path)

        let fdRes = await io.uringOpen(path, O_WRONLY or O_CREAT or O_TRUNC, 0o644)
        doAssert fdRes >= 0

        var bufRef = new(seq[byte])
        bufRef[] = @[byte 1, 2, 3, 4, 5]

        io.beginChain()
        let writeFut = io.uringWrite(fdRes.cint, addr bufRef[][0], 5, 0'u64, bufRef)
        let fsyncFut = io.uringFsync(fdRes.cint)
        let futs = io.endChain()

        doAssert futs.len == 2
        doAssert futs[0] == writeFut
        doAssert futs[1] == fsyncFut

        let writeRes = await writeFut
        let fsyncRes = await fsyncFut

        doAssert writeRes == 5, "write failed: " & $writeRes
        doAssert fsyncRes == 0, "fsync failed: " & $fsyncRes

        discard await io.uringClose(fdRes.cint)

    waitFor run()

  test "chain: failure propagation (bad fd)":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        # Use an invalid fd to trigger write failure; fsync should get -ECANCELED
        let badFd = 9999.cint

        var bufRef = new(seq[byte])
        bufRef[] = @[byte 1, 2, 3]

        io.beginChain()
        let writeFut = io.uringWrite(badFd, addr bufRef[][0], 3, 0'u64, bufRef)
        let fsyncFut = io.uringFsync(badFd)
        discard io.endChain()

        let writeRes = await writeFut
        let fsyncRes = await fsyncFut

        doAssert writeRes < 0, "write should fail: " & $writeRes
        doAssert fsyncRes == -125, "fsync should be -ECANCELED: " & $fsyncRes

    waitFor run()

  test "chain: SQ-full rolls back entire chain":
    var io2 = newUringFileIO(4)
    defer:
      io2.close()

    proc run() {.async.} =
      {.cast(gcsafe).}:
        # Fill SQ slots leaving room for only 1 more
        var filled = 0
        while true:
          let sqe = getSqe(io2.ring)
          if sqe == nil:
            break
          inc filled

        # Roll back the dummy SQEs we just allocated, but keep only 1 slot
        rollbackSqes(io2.ring, uint32(filled))
        # Now fill all but 1
        for i in 0 ..< filled - 1:
          let sqe = getSqe(io2.ring)
          doAssert sqe != nil

        # Start chain needing 2 slots but only 1 is available
        var bufRef = new(seq[byte])
        bufRef[] = @[byte 1, 2, 3]

        io2.beginChain()
        let writeFut = io2.uringWrite(1.cint, addr bufRef[][0], 3, 0'u64, bufRef)
        let fsyncFut = io2.uringFsync(1.cint)
        let futs = io2.endChain()

        doAssert futs.len == 2

        # Both futures should fail
        var writeRaised = false
        try:
          discard await writeFut
        except IOError:
          writeRaised = true
        doAssert writeRaised

        var fsyncRaised = false
        try:
          discard await fsyncFut
        except IOError:
          fsyncRaised = true
        doAssert fsyncRaised

        # Ring should still be usable after rollback —
        # roll back the dummy fills first
        rollbackSqes(io2.ring, uint32(filled - 1))
        let fd = await io2.uringOpen("/dev/null", O_RDONLY, 0)
        doAssert fd >= 0
        discard await io2.uringClose(fd.cint)

    waitFor run()

  test "chain: single element (no IOSQE_IO_LINK)":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let path = getTempDir() / "iori_test_chain_single.bin"
        defer:
          removeFile(path)

        let fdRes = await io.uringOpen(path, O_WRONLY or O_CREAT or O_TRUNC, 0o644)
        doAssert fdRes >= 0

        var bufRef = new(seq[byte])
        bufRef[] = @[byte 42]

        io.beginChain()
        let writeFut = io.uringWrite(fdRes.cint, addr bufRef[][0], 1, 0'u64, bufRef)
        let futs = io.endChain()

        doAssert futs.len == 1
        doAssert futs[0] == writeFut

        let writeRes = await writeFut
        doAssert writeRes == 1

        discard await io.uringClose(fdRes.cint)

    waitFor run()

  test "chain: empty chain returns empty seq":
    io.beginChain()
    let futs = io.endChain()
    doAssert futs.len == 0

  test "chain: cancel preserves link flag (NOP keeps IOSQE_IO_LINK)":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let path = getTempDir() / "iori_test_chain_cancel.bin"
        defer:
          removeFile(path)

        let fdRes = await io.uringOpen(path, O_WRONLY or O_CREAT or O_TRUNC, 0o644)
        doAssert fdRes >= 0

        var bufRef = new(seq[byte])
        bufRef[] = @[byte 10, 20, 30]

        io.beginChain()
        let writeFut = io.uringWrite(fdRes.cint, addr bufRef[][0], 3, 0'u64, bufRef)
        let fsyncFut = io.uringFsync(fdRes.cint)
        discard io.endChain()

        # Cancel the first operation (write) — NOP should keep IOSQE_IO_LINK
        let cancelRes = await io.uringCancel(writeFut)
        doAssert cancelRes == 0

        let writeRes = await writeFut
        doAssert writeRes == -125, "write should be -ECANCELED: " & $writeRes

        # The fsync should still execute since NOP with IOSQE_IO_LINK succeeds
        # and the link continues
        let fsyncRes = await fsyncFut
        doAssert fsyncRes == 0, "fsync should succeed: " & $fsyncRes

        discard await io.uringClose(fdRes.cint)

    waitFor run()

  test "local cancel prevents stale SQE submission":
    ## After local cancel of an unsubmitted operation, the SQE must not be
    ## submitted to the kernel on the next flush. Uses a pipe to detect
    ## whether the cancelled write was actually executed by the kernel.
    proc run() {.async.} =
      {.cast(gcsafe).}:
        var fds: array[2, cint]
        doAssert pipe(fds) == 0
        let readFd = fds[0]
        let writeFd = fds[1]
        defer:
          discard posix.close(readFd)
          discard posix.close(writeFd)

        # Make read end non-blocking for the final check
        let fl = fcntl(readFd, F_GETFL)
        doAssert fcntl(readFd, F_SETFL, fl or O_NONBLOCK) == 0

        # Queue a write to the pipe — SQE prepared but not yet submitted
        var bufRef = new(seq[byte])
        bufRef[] = @[byte 0xDE, 0xAD, 0xBE, 0xEF]
        let writeFut = io.uringWrite(writeFd, addr bufRef[][0], 4, 0'u64, bufRef)

        # Cancel locally before flush fires
        let cancelRes = await io.uringCancel(writeFut)
        doAssert cancelRes == 0
        doAssert (await writeFut) == -125 # ECANCELED

        # Trigger flush by queuing + awaiting another operation.
        # submit() sends ALL pending SQEs including the stale cancelled one.
        let fd = await io.uringOpen("/dev/null", O_RDONLY, 0)
        doAssert fd >= 0
        discard await io.uringClose(fd.cint)

        # If the stale write SQE was submitted, data is now in the pipe.
        var checkBuf: array[4, byte]
        let n = posix.read(readFd, addr checkBuf[0], 4)
        doAssert n <= 0,
          "stale SQE was submitted to kernel: pipe contains " & $n & " bytes"

    waitFor run()

  test "empty endChain re-arms a pre-chain flush (no submission wedge)":
    ## Regression: a pre-chain unsubmitted op whose scheduled flush already fired
    ## (and early-returned because chainActive) while a chain was open must still
    ## be submitted when that chain closes empty. endChain's old empty-chain early
    ## return skipped the flushScheduled re-arm, stranding the op in `unsubmitted`
    ## and wedging every later submission (flushScheduled stuck true forever).
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let fd = cint(posix.open("/dev/null", O_WRONLY))
        doAssert fd >= 0
        defer:
          discard posix.close(fd)

        var bufRef = new(seq[byte])
        bufRef[] = @[byte 1, 2, 3, 4]

        # Queue a write OUTSIDE any chain — schedules a flush (flushScheduled=true).
        let a = io.uringWrite(fd, addr bufRef[][0], 4, 0'u64, bufRef)
        # Open an empty chain, then span a tick so the pre-chain flush fires while
        # chainActive (it early-returns, leaving flushScheduled stuck true and `a`
        # unsubmitted).
        io.beginChain()
        await sleepMsAsync(10)
        # Close the chain WITHOUT queueing any op — the empty-chain path must still
        # re-arm the flush so `a` is submitted.
        let futs = io.endChain()
        doAssert futs.len == 0

        # `a` must complete within a bounded budget; a wedge leaves it pending forever.
        var done = false
        for _ in 0 ..< 250:
          if a.finished:
            done = true
            break
          await sleepMsAsync(2)
        doAssert done, "pre-chain op stranded: empty endChain did not re-arm flush"
        doAssert (await a) == 4, "write should have completed with 4 bytes"

    waitFor run()

  test "uringCancel of submitted op during chain construction is rejected":
    ## Regression: cancelling a *submitted* op via the public uringCancel while a
    ## chain is being built must NOT graft an ASYNC_CANCEL SQE into the chain
    ## (IOSQE_IO_LINK would link it in, splitting the chain and making endChain
    ## return N+1 futures). It is rejected with IOError; the chain stays intact.
    proc run() {.async.} =
      {.cast(gcsafe).}:
        # A submitted, in-flight blocking pipe read — the op to cancel.
        var fds: array[2, cint]
        doAssert pipe(fds) == 0
        let readFd = fds[0]
        let writeFd = fds[1]
        defer:
          discard posix.close(readFd)
          discard posix.close(writeFd)

        var blkRef = new(seq[byte])
        blkRef[] = newSeq[byte](64)
        let blockingFut = io.uringRead(readFd, addr blkRef[][0], 64, 0'u64, blkRef)
        io.flush()
        doAssert io.unsubmitted.len == 0 # actually submitted (in-flight)

        let fd = cint(posix.open("/dev/zero", O_RDONLY))
        doAssert fd >= 0
        defer:
          discard posix.close(fd)
        var b1 = new(seq[byte])
        b1[] = newSeq[byte](8)
        var b2 = new(seq[byte])
        b2[] = newSeq[byte](8)

        io.beginChain()
        let f1 = io.uringRead(fd, addr b1[][0], 8, 0'u64, b1)
        # Public uringCancel of the submitted op mid-chain must be rejected
        # synchronously, not grafted: the returned future is already failed and
        # chainFutures stays at 1 (only f1), not 2. Check synchronously (no await)
        # so the unfixed/grafted case fails cleanly here instead of deadlocking on
        # an await of a future stranded inside the still-open chain.
        let cancelFut = io.uringCancel(blockingFut)
        doAssert cancelFut.failed,
          "uringCancel of submitted op mid-chain should be rejected synchronously"
        doAssert io.chainFutures.len == 1,
          "cancel grafted into chain: " & $io.chainFutures.len
        var rejected = false
        try:
          discard await cancelFut
        except IOError:
          rejected = true
        doAssert rejected, "rejected cancel future should carry IOError"
        let f2 = io.uringRead(fd, addr b2[][0], 8, 0'u64, b2)
        let futs = io.endChain()
        doAssert futs.len == 2, "endChain returned " & $futs.len & " futures (want 2)"

        # The chain ops still run correctly (8 zero bytes each from /dev/zero).
        doAssert (await f1) == 8, "f1"
        doAssert (await f2) == 8, "f2"

        # The blocking op was never cancelled (the cancel was rejected) and is
        # still in flight; cancel it now, outside the chain, to drain it.
        doAssert (await io.uringCancel(blockingFut)) == 0
        doAssert (await blockingFut) == -125

    waitFor run()

  test "fixed buffer register and unregister":
    io.registerFixedBuffers(@[4096, 8192])
    doAssert io.fixedBufferCount == 2
    doAssert io.fixedBufferSize(0) == 4096
    doAssert io.fixedBufferSize(1) == 8192
    doAssert io.fixedBufferAddr(0) != nil
    doAssert io.fixedBufferAddr(1) != nil
    io.unregisterFixedBuffers()
    doAssert io.fixedBufferCount == 0

  test "fixed buffer double register raises IOError":
    io.registerFixedBuffers(@[4096])
    defer:
      io.unregisterFixedBuffers()

    var raised = false
    try:
      io.registerFixedBuffers(@[4096])
    except IOError:
      raised = true
    doAssert raised

  test "fixed buffer register on closed instance raises IOError":
    var io2 = newUringFileIO(32)
    io2.close()

    var raised = false
    try:
      io2.registerFixedBuffers(@[4096])
    except IOError:
      raised = true
    doAssert raised

  test "fixed buffer read/write round trip":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let path = getTempDir() / "iori_test_fixed_buf.bin"
        defer:
          removeFile(path)

        io.registerFixedBuffers(@[4096])
        defer:
          io.unregisterFixedBuffers()

        let fdRes = await io.uringOpen(path, O_WRONLY or O_CREAT or O_TRUNC, 0o644)
        doAssert fdRes >= 0

        # Write data using fixed buffer
        let writeBuf = io.fixedBufferAddr(0)
        let data = "Hello, fixed buffers!"
        copyMem(writeBuf, data.cstring, data.len)

        let writeRes =
          await io.uringWriteFixed(fdRes.cint, writeBuf, uint32(data.len), 0'u64, 0'u16)
        doAssert writeRes == int32(data.len), "write failed: " & $writeRes

        discard await io.uringClose(fdRes.cint)

        # Read back using fixed buffer
        let fdRes2 = await io.uringOpen(path, O_RDONLY, 0)
        doAssert fdRes2 >= 0

        # Clear the buffer first
        zeroMem(writeBuf, 4096)

        let readRes =
          await io.uringReadFixed(fdRes2.cint, writeBuf, uint32(data.len), 0'u64, 0'u16)
        doAssert readRes == int32(data.len), "read failed: " & $readRes

        # Verify content
        var readBack = newString(data.len)
        copyMem(addr readBack[0], writeBuf, data.len)
        doAssert readBack == data

        discard await io.uringClose(fdRes2.cint)

    waitFor run()

  test "fixed buffer chain: write + fsync":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let path = getTempDir() / "iori_test_fixed_buf_chain.bin"
        defer:
          removeFile(path)

        io.registerFixedBuffers(@[4096])
        defer:
          io.unregisterFixedBuffers()

        let fdRes = await io.uringOpen(path, O_WRONLY or O_CREAT or O_TRUNC, 0o644)
        doAssert fdRes >= 0

        let writeBuf = io.fixedBufferAddr(0)
        let data = "chained fixed write"
        copyMem(writeBuf, data.cstring, data.len)

        io.beginChain()
        let writeFut =
          io.uringWriteFixed(fdRes.cint, writeBuf, uint32(data.len), 0'u64, 0'u16)
        let fsyncFut = io.uringFsync(fdRes.cint)
        let futs = io.endChain()

        doAssert futs.len == 2

        let writeRes = await writeFut
        let fsyncRes = await fsyncFut

        doAssert writeRes == int32(data.len), "write failed: " & $writeRes
        doAssert fsyncRes == 0, "fsync failed: " & $fsyncRes

        discard await io.uringClose(fdRes.cint)

    waitFor run()

  test "uringReadFixed on closed instance fails with IOError":
    var io2 = newUringFileIO(32)
    io2.close()

    proc run() {.async.} =
      {.cast(gcsafe).}:
        var raised = false
        try:
          discard await io2.uringReadFixed(0.cint, nil, 64, 0'u64, 0'u16)
        except IOError:
          raised = true
        doAssert raised

    waitFor run()

  test "uringWriteFixed on closed instance fails with IOError":
    var io2 = newUringFileIO(32)
    io2.close()

    proc run() {.async.} =
      {.cast(gcsafe).}:
        var raised = false
        try:
          discard await io2.uringWriteFixed(0.cint, nil, 64, 0'u64, 0'u16)
        except IOError:
          raised = true
        doAssert raised

    waitFor run()

  test "fixed buffer register with empty sizes raises IOError":
    var raised = false
    try:
      io.registerFixedBuffers(@[])
    except IOError:
      raised = true
    doAssert raised

  test "fixed buffer register with zero size raises IOError":
    var raised = false
    try:
      io.registerFixedBuffers(@[0])
    except IOError:
      raised = true
    doAssert raised

  test "fixed buffer register with negative size raises IOError":
    var raised = false
    try:
      io.registerFixedBuffers(@[-1])
    except IOError:
      raised = true
    doAssert raised

  test "unregisterFixedBuffers without registration is no-op":
    io.unregisterFixedBuffers()
    doAssert io.fixedBufferCount == 0

  test "fixed buffer re-register after unregister":
    io.registerFixedBuffers(@[4096])
    io.unregisterFixedBuffers()
    # Should succeed after unregister
    io.registerFixedBuffers(@[8192])
    doAssert io.fixedBufferCount == 1
    doAssert io.fixedBufferSize(0) == 8192
    io.unregisterFixedBuffers()

  test "fixed buffer read/write with multiple buffer indices":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let path = getTempDir() / "iori_test_fixed_multi_idx.bin"
        defer:
          removeFile(path)

        io.registerFixedBuffers(@[4096, 4096])
        defer:
          io.unregisterFixedBuffers()

        let fdRes = await io.uringOpen(path, O_WRONLY or O_CREAT or O_TRUNC, 0o644)
        doAssert fdRes >= 0

        # Write using buffer index 1
        let buf1 = io.fixedBufferAddr(1)
        let data = "buffer index 1"
        copyMem(buf1, data.cstring, data.len)

        let writeRes =
          await io.uringWriteFixed(fdRes.cint, buf1, uint32(data.len), 0'u64, 1'u16)
        doAssert writeRes == int32(data.len), "write failed: " & $writeRes

        discard await io.uringClose(fdRes.cint)

        # Read back using buffer index 0
        let fdRes2 = await io.uringOpen(path, O_RDONLY, 0)
        doAssert fdRes2 >= 0

        let buf0 = io.fixedBufferAddr(0)
        zeroMem(buf0, 4096)

        let readRes =
          await io.uringReadFixed(fdRes2.cint, buf0, uint32(data.len), 0'u64, 0'u16)
        doAssert readRes == int32(data.len), "read failed: " & $readRes

        var readBack = newString(data.len)
        copyMem(addr readBack[0], buf0, data.len)
        doAssert readBack == data

        discard await io.uringClose(fdRes2.cint)

    waitFor run()

  test "close unregisters fixed buffers":
    var io2 = newUringFileIO(32)
    io2.registerFixedBuffers(@[4096])
    doAssert io2.fixedBufsRegistered == true
    io2.close()
    doAssert io2.fixedBufsRegistered == false
    doAssert io2.fixedBufferCount == 0

  test "fixed file register and unregister":
    let path = getTempDir() / "iori_test_fixed_file_reg.txt"
    let fd = posix.open(path.cstring, O_WRONLY or O_CREAT or O_TRUNC, 0o644)
    doAssert fd >= 0
    defer:
      discard posix.close(fd)
      removeFile(path)

    io.registerFixedFiles(@[fd])
    doAssert io.fixedFileCount == 1
    io.unregisterFixedFiles()
    doAssert io.fixedFileCount == 0

  test "fixed file double register raises IOError":
    let path = getTempDir() / "iori_test_fixed_file_double.txt"
    let fd = posix.open(path.cstring, O_WRONLY or O_CREAT or O_TRUNC, 0o644)
    doAssert fd >= 0
    defer:
      discard posix.close(fd)
      removeFile(path)

    io.registerFixedFiles(@[fd])
    defer:
      io.unregisterFixedFiles()

    var raised = false
    try:
      io.registerFixedFiles(@[fd])
    except IOError:
      raised = true
    doAssert raised

  test "fixed file register on closed instance raises IOError":
    var io2 = newUringFileIO(32)
    io2.close()

    var raised = false
    try:
      io2.registerFixedFiles(@[0.cint])
    except IOError:
      raised = true
    doAssert raised

  test "fixed file register with empty array raises IOError":
    var raised = false
    try:
      io.registerFixedFiles(@[])
    except IOError:
      raised = true
    doAssert raised

  test "unregisterFixedFiles without registration is no-op":
    io.unregisterFixedFiles()
    doAssert io.fixedFileCount == 0

  test "fixed file re-register after unregister":
    let path = getTempDir() / "iori_test_fixed_file_rereg.txt"
    let fd = posix.open(path.cstring, O_WRONLY or O_CREAT or O_TRUNC, 0o644)
    doAssert fd >= 0
    defer:
      discard posix.close(fd)
      removeFile(path)

    io.registerFixedFiles(@[fd])
    io.unregisterFixedFiles()
    # Should succeed after unregister
    io.registerFixedFiles(@[fd])
    doAssert io.fixedFileCount == 1
    io.unregisterFixedFiles()

  test "fixed file read/write round trip":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let path = getTempDir() / "iori_test_fixed_file_rw.bin"
        defer:
          removeFile(path)

        let fd = posix.open(path.cstring, O_RDWR or O_CREAT or O_TRUNC, 0o644)
        doAssert fd >= 0
        defer:
          discard posix.close(fd)

        io.registerFixedFiles(@[fd])
        defer:
          io.unregisterFixedFiles()

        # Write using fixed file index
        var writeBufRef = new(seq[byte])
        writeBufRef[] = @[byte 72, 101, 108, 108, 111] # "Hello"

        let writeRes = await io.uringWriteFixedFile(
          0.cint, addr writeBufRef[][0], uint32(writeBufRef[].len), 0'u64, writeBufRef
        )
        doAssert writeRes == 5, "write failed: " & $writeRes

        # Read back using fixed file index
        var readBufRef = new(seq[byte])
        readBufRef[] = newSeq[byte](5)

        let readRes = await io.uringReadFixedFile(
          0.cint, addr readBufRef[][0], 5, 0'u64, readBufRef
        )
        doAssert readRes == 5, "read failed: " & $readRes
        doAssert readBufRef[] == @[byte 72, 101, 108, 108, 111]

    waitFor run()

  test "fixed file chain: write + fsync":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let path = getTempDir() / "iori_test_fixed_file_chain.bin"
        defer:
          removeFile(path)

        let fd = posix.open(path.cstring, O_RDWR or O_CREAT or O_TRUNC, 0o644)
        doAssert fd >= 0
        defer:
          discard posix.close(fd)

        io.registerFixedFiles(@[fd])
        defer:
          io.unregisterFixedFiles()

        var bufRef = new(seq[byte])
        bufRef[] = @[byte 10, 20, 30]

        io.beginChain()
        let writeFut = io.uringWriteFixedFile(
          0.cint, addr bufRef[][0], uint32(bufRef[].len), 0'u64, bufRef
        )
        let fsyncFut = io.uringFsyncFixedFile(0.cint)
        let futs = io.endChain()

        doAssert futs.len == 2

        let writeRes = await writeFut
        let fsyncRes = await fsyncFut

        doAssert writeRes == 3, "write failed: " & $writeRes
        doAssert fsyncRes == 0, "fsync failed: " & $fsyncRes

    waitFor run()

  test "uringReadFixedFile on closed instance fails with IOError":
    var io2 = newUringFileIO(32)
    io2.close()

    proc run() {.async.} =
      {.cast(gcsafe).}:
        var raised = false
        var bufRef = new(seq[byte])
        bufRef[] = newSeq[byte](64)
        try:
          discard
            await io2.uringReadFixedFile(0.cint, addr bufRef[][0], 64, 0'u64, bufRef)
        except IOError:
          raised = true
        doAssert raised

    waitFor run()

  test "uringWriteFixedFile on closed instance fails with IOError":
    var io2 = newUringFileIO(32)
    io2.close()

    proc run() {.async.} =
      {.cast(gcsafe).}:
        var raised = false
        var bufRef = new(seq[byte])
        bufRef[] = newSeq[byte](64)
        try:
          discard
            await io2.uringWriteFixedFile(0.cint, addr bufRef[][0], 64, 0'u64, bufRef)
        except IOError:
          raised = true
        doAssert raised

    waitFor run()

  test "uringFsyncFixedFile on closed instance fails with IOError":
    var io2 = newUringFileIO(32)
    io2.close()

    proc run() {.async.} =
      {.cast(gcsafe).}:
        var raised = false
        try:
          discard await io2.uringFsyncFixedFile(0.cint)
        except IOError:
          raised = true
        doAssert raised

    waitFor run()

  test "fixed file read/write with multiple file indices":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let path1 = getTempDir() / "iori_test_fixed_file_idx0.bin"
        let path2 = getTempDir() / "iori_test_fixed_file_idx1.bin"
        defer:
          removeFile(path1)
          removeFile(path2)

        let fd1 = posix.open(path1.cstring, O_RDWR or O_CREAT or O_TRUNC, 0o644)
        let fd2 = posix.open(path2.cstring, O_RDWR or O_CREAT or O_TRUNC, 0o644)
        doAssert fd1 >= 0
        doAssert fd2 >= 0
        defer:
          discard posix.close(fd1)
          discard posix.close(fd2)

        io.registerFixedFiles(@[fd1, fd2])
        defer:
          io.unregisterFixedFiles()

        # Write different data to each file via different indices
        var buf1Ref = new(seq[byte])
        buf1Ref[] = @[byte 0xAA, 0xBB]
        var buf2Ref = new(seq[byte])
        buf2Ref[] = @[byte 0xCC, 0xDD, 0xEE]

        let w1 = await io.uringWriteFixedFile(
          0.cint, addr buf1Ref[][0], uint32(buf1Ref[].len), 0'u64, buf1Ref
        )
        let w2 = await io.uringWriteFixedFile(
          1.cint, addr buf2Ref[][0], uint32(buf2Ref[].len), 0'u64, buf2Ref
        )
        doAssert w1 == 2, "write to index 0 failed: " & $w1
        doAssert w2 == 3, "write to index 1 failed: " & $w2

        # Read back from each file using the opposite order
        var read2Ref = new(seq[byte])
        read2Ref[] = newSeq[byte](3)
        let r2 =
          await io.uringReadFixedFile(1.cint, addr read2Ref[][0], 3, 0'u64, read2Ref)
        doAssert r2 == 3, "read from index 1 failed: " & $r2
        doAssert read2Ref[] == @[byte 0xCC, 0xDD, 0xEE]

        var read1Ref = new(seq[byte])
        read1Ref[] = newSeq[byte](2)
        let r1 =
          await io.uringReadFixedFile(0.cint, addr read1Ref[][0], 2, 0'u64, read1Ref)
        doAssert r1 == 2, "read from index 0 failed: " & $r1
        doAssert read1Ref[] == @[byte 0xAA, 0xBB]

    waitFor run()

  test "close unregisters fixed files":
    var io2 = newUringFileIO(32)

    let path = getTempDir() / "iori_test_fixed_file_close.txt"
    let fd = posix.open(path.cstring, O_WRONLY or O_CREAT or O_TRUNC, 0o644)
    doAssert fd >= 0
    defer:
      discard posix.close(fd)
      removeFile(path)

    io2.registerFixedFiles(@[fd])
    doAssert io2.fixedFilesRegistered == true
    io2.close()
    doAssert io2.fixedFilesRegistered == false
    doAssert io2.fixedFileCount == 0

  # Fixed file slots (direct descriptors)

  test "registerFixedFileSlots: basic register and unregister":
    io.registerFixedFileSlots(4)
    doAssert io.fixedFileSlotsAvailable == 4
    doAssert io.fixedFileCount == 4
    io.unregisterFixedFiles()
    doAssert io.fixedFileSlotsAvailable == 0
    doAssert io.fixedFileCount == 0

  test "registerFixedFileSlots: double register raises IOError":
    io.registerFixedFileSlots(2)
    defer:
      io.unregisterFixedFiles()

    var raised = false
    try:
      io.registerFixedFileSlots(2)
    except IOError:
      raised = true
    doAssert raised

  test "registerFixedFileSlots: closed instance raises IOError":
    var io2 = newUringFileIO(32)
    io2.close()

    var raised = false
    try:
      io2.registerFixedFileSlots(2)
    except IOError:
      raised = true
    doAssert raised

  test "allocFixedFileSlot/freeFixedFileSlot round trip":
    io.registerFixedFileSlots(2)
    defer:
      io.unregisterFixedFiles()

    doAssert io.fixedFileSlotsAvailable == 2
    let s1 = io.allocFixedFileSlot()
    let s2 = io.allocFixedFileSlot()
    doAssert io.fixedFileSlotsAvailable == 0
    doAssert s1 != s2
    io.freeFixedFileSlot(s1)
    doAssert io.fixedFileSlotsAvailable == 1
    io.freeFixedFileSlot(s2)
    doAssert io.fixedFileSlotsAvailable == 2

  test "allocFixedFileSlot: exhaustion raises IOError":
    io.registerFixedFileSlots(1)
    defer:
      io.unregisterFixedFiles()

    discard io.allocFixedFileSlot()
    var raised = false
    try:
      discard io.allocFixedFileSlot()
    except IOError:
      raised = true
    doAssert raised

  test "uringOpenDirect + uringCloseDirect round trip":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        io.registerFixedFileSlots(2)
        defer:
          io.unregisterFixedFiles()

        let slot = io.allocFixedFileSlot()
        let openRes = await io.uringOpenDirect("/dev/null", O_RDONLY, 0, slot)
        doAssert openRes == 0, "openDirect failed: " & $openRes

        let closeRes = await io.uringCloseDirect(slot)
        doAssert closeRes == 0, "closeDirect failed: " & $closeRes

        io.freeFixedFileSlot(slot)

    waitFor run()

  test "uringOpenDirect: nonexistent file returns negative errno":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        io.registerFixedFileSlots(1)
        defer:
          io.unregisterFixedFiles()

        let slot = io.allocFixedFileSlot()
        let res = await io.uringOpenDirect(
          "/tmp/iori_nonexistent_direct_" & $getpid(), O_RDONLY, 0, slot
        )
        doAssert res < 0 # -ENOENT
        io.freeFixedFileSlot(slot)

    waitFor run()

  test "uringOpenDirect + uringReadFixedFile: read file content":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let path = getTempDir() / "iori_test_open_direct_read.txt"
        defer:
          removeFile(path)

        let fd = posix.open(path.cstring, O_WRONLY or O_CREAT or O_TRUNC, 0o644)
        doAssert fd >= 0
        let data = "direct read"
        doAssert posix.write(fd, data.cstring, data.len) == data.len
        discard posix.close(fd)

        io.registerFixedFileSlots(1)
        defer:
          io.unregisterFixedFiles()

        let slot = io.allocFixedFileSlot()
        let openRes = await io.uringOpenDirect(path, O_RDONLY, 0, slot)
        doAssert openRes == 0

        var bufRef = new(seq[byte])
        bufRef[] = newSeq[byte](data.len)
        let readRes = await io.uringReadFixedFile(
          slot.cint, addr bufRef[][0], uint32(data.len), 0'u64, bufRef
        )
        doAssert readRes == int32(data.len), "read failed: " & $readRes

        var readBack = newString(data.len)
        copyMem(addr readBack[0], addr bufRef[][0], data.len)
        doAssert readBack == data

        let closeRes = await io.uringCloseDirect(slot)
        doAssert closeRes == 0
        io.freeFixedFileSlot(slot)

    waitFor run()

  test "chain: openDirect → readFixedFile → closeDirect (3-op)":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let path = getTempDir() / "iori_test_chain_3op_direct.txt"
        defer:
          removeFile(path)

        let fd = posix.open(path.cstring, O_WRONLY or O_CREAT or O_TRUNC, 0o644)
        doAssert fd >= 0
        let data = "chain3"
        doAssert posix.write(fd, data.cstring, data.len) == data.len
        discard posix.close(fd)

        io.registerFixedFileSlots(1)
        defer:
          io.unregisterFixedFiles()

        let slot = io.allocFixedFileSlot()

        var bufRef = new(seq[byte])
        bufRef[] = newSeq[byte](data.len)

        io.beginChain()
        let openFut = io.uringOpenDirect(path, O_RDONLY, 0, slot)
        let readFut = io.uringReadFixedFile(
          slot.cint, addr bufRef[][0], uint32(data.len), 0'u64, bufRef
        )
        let closeFut = io.uringCloseDirect(slot)
        let futs = io.endChain()

        doAssert futs.len == 3

        let openRes = await openFut
        let readRes = await readFut
        let closeRes = await closeFut

        doAssert openRes == 0, "open failed: " & $openRes
        doAssert readRes == int32(data.len), "read failed: " & $readRes
        doAssert closeRes == 0, "close failed: " & $closeRes

        var readBack = newString(data.len)
        copyMem(addr readBack[0], addr bufRef[][0], data.len)
        doAssert readBack == data

        io.freeFixedFileSlot(slot)

    waitFor run()

  test "chain: openDirect → writeFixedFile → fsyncFixedFile → closeDirect (4-op)":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let path = getTempDir() / "iori_test_chain_4op_direct.bin"
        defer:
          removeFile(path)

        io.registerFixedFileSlots(1)
        defer:
          io.unregisterFixedFiles()

        let slot = io.allocFixedFileSlot()

        var bufRef = new(seq[byte])
        bufRef[] = @[byte 0xCA, 0xFE, 0xBA, 0xBE]

        io.beginChain()
        let openFut =
          io.uringOpenDirect(path, O_WRONLY or O_CREAT or O_TRUNC, 0o644, slot)
        let writeFut = io.uringWriteFixedFile(
          slot.cint, addr bufRef[][0], uint32(bufRef[].len), 0'u64, bufRef
        )
        let fsyncFut = io.uringFsyncFixedFile(slot.cint)
        let closeFut = io.uringCloseDirect(slot)
        let futs = io.endChain()

        doAssert futs.len == 4

        let openRes = await openFut
        let writeRes = await writeFut
        let fsyncRes = await fsyncFut
        let closeRes = await closeFut

        doAssert openRes == 0, "open failed: " & $openRes
        doAssert writeRes == 4, "write failed: " & $writeRes
        doAssert fsyncRes == 0, "fsync failed: " & $fsyncRes
        doAssert closeRes == 0, "close failed: " & $closeRes

        # Verify file content with posix
        let rfd = posix.open(path.cstring, O_RDONLY)
        doAssert rfd >= 0
        var readBuf: array[4, byte]
        doAssert posix.read(rfd, addr readBuf[0], 4) == 4
        discard posix.close(rfd)
        doAssert readBuf == [byte 0xCA, 0xFE, 0xBA, 0xBE]

        io.freeFixedFileSlot(slot)

    waitFor run()

  test "chain failure propagation: bad path → openDirect fails → subsequent -ECANCELED":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        io.registerFixedFileSlots(1)
        defer:
          io.unregisterFixedFiles()

        let slot = io.allocFixedFileSlot()

        var bufRef = new(seq[byte])
        bufRef[] = newSeq[byte](64)

        io.beginChain()
        let openFut = io.uringOpenDirect(
          "/tmp/iori_nonexistent_chain_" & $getpid(), O_RDONLY, 0, slot
        )
        let readFut =
          io.uringReadFixedFile(slot.cint, addr bufRef[][0], 64, 0'u64, bufRef)
        let closeFut = io.uringCloseDirect(slot)
        discard io.endChain()

        let openRes = await openFut
        let readRes = await readFut
        let closeRes = await closeFut

        doAssert openRes < 0, "open should fail: " & $openRes
        doAssert readRes == -125, "read should be -ECANCELED: " & $readRes
        doAssert closeRes == -125, "close should be -ECANCELED: " & $closeRes

        io.freeFixedFileSlot(slot)

    waitFor run()
