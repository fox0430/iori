## Tests for uring_bridge: Low-level API and lifecycle.

import std/[unittest, os, posix, strutils, importutils]

import ../iori/uring_bridge
import ../iori/uring_raw

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
