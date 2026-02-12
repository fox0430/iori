## Tests for uring_bridge: Low-level API and lifecycle.

import std/[unittest, os, posix, strutils]

import ../iori/uring_bridge
import ../iori/uring_raw

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

        var buf = newSeq[byte](4096)
        let fut = io2.uringRead(fd, addr buf[0], 4096, 0'u64, buf)

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
        var buf = newSeq[byte](16)
        let futRead = io.uringRead(fd2, addr buf[0], 16, 0'u64, buf)
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

        var buf = newSeq[byte](64)
        let readRes = await io.uringRead(fdRes.cint, addr buf[0], 64, 0'u64, buf)
        doAssert readRes == -9 # -EBADF

    waitFor run()
