## Tests for uring_file_io: High-level file I/O API.

import std/[unittest, os, posix, importutils, monotimes, times]

import ../iori/uring_file_io
import ../iori/uring_raw

privateAccess(UringFileIO)

suite "uring_file_io":
  var io {.threadvar.}: UringFileIO

  setup:
    io = newUringFileIO(256)

  teardown:
    io.close()

  test "writeFile and readFile roundtrip":
    let path = getTempDir() / "iori_test_roundtrip.bin"
    defer:
      removeFile(path)

    let data = @[byte 1, 2, 3, 4, 5, 0xDE, 0xAD, 0xBE, 0xEF]

    proc run() {.async.} =
      {.cast(gcsafe).}:
        await io.writeFile(path, data)
        let readResult = await io.readFile(path, 4096)
        doAssert readResult == data

    waitFor run()

  test "writeFileString and readFileString roundtrip":
    let path = getTempDir() / "iori_test_roundtrip.txt"
    defer:
      removeFile(path)

    let content = "Hello, io_uring!\nLine 2\n"

    proc run() {.async.} =
      {.cast(gcsafe).}:
        await io.writeFileString(path, content)
        let readResult = await io.readFileString(path, 4096)
        doAssert readResult == content

    waitFor run()

  test "readFile on nonexistent file raises IOError":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        var raised = false
        try:
          discard await io.readFile("/tmp/iori_nonexistent_" & $getpid())
        except IOError:
          raised = true
        doAssert raised

    waitFor run()

  test "large data roundtrip":
    let path = getTempDir() / "iori_test_large.bin"
    defer:
      removeFile(path)

    var data = newSeq[byte](65536)
    for i in 0 ..< data.len:
      data[i] = byte(i mod 256)

    proc run() {.async.} =
      {.cast(gcsafe).}:
        await io.writeFile(path, data)
        let readResult = await io.readFile(path, 65536)
        doAssert readResult.len == data.len
        doAssert readResult == data

    waitFor run()

  test "empty data writeFile and readFile":
    let path = getTempDir() / "iori_test_empty.bin"
    defer:
      removeFile(path)

    proc run() {.async.} =
      {.cast(gcsafe).}:
        await io.writeFile(path, @[])
        let readResult = await io.readFile(path, 4096)
        doAssert readResult.len == 0

    waitFor run()

  test "empty string writeFileString and readFileString":
    let path = getTempDir() / "iori_test_empty.txt"
    defer:
      removeFile(path)

    proc run() {.async.} =
      {.cast(gcsafe).}:
        await io.writeFileString(path, "")
        let readResult = await io.readFileString(path, 4096)
        doAssert readResult.len == 0

    waitFor run()

  test "readFile with maxSize=0 on empty file returns empty":
    let path = getTempDir() / "iori_test_maxsize0_empty.bin"
    defer:
      removeFile(path)

    proc run() {.async.} =
      {.cast(gcsafe).}:
        await io.writeFile(path, @[])
        let readBack = await io.readFile(path, maxSize = 0)
        doAssert readBack.len == 0

    waitFor run()

  test "readFile raises IOError when file exceeds maxSize":
    let path = getTempDir() / "iori_test_exceeds_maxsize.bin"
    defer:
      removeFile(path)

    proc run() {.async.} =
      {.cast(gcsafe).}:
        await io.writeFile(path, @[byte 1, 2, 3])
        var raised = false
        try:
          discard await io.readFile(path, maxSize = 2)
        except IOError:
          raised = true
        doAssert raised

    waitFor run()

  test "readFile with maxSize=0 on non-empty file raises IOError":
    let path = getTempDir() / "iori_test_maxsize0_nonempty.bin"
    defer:
      removeFile(path)

    proc run() {.async.} =
      {.cast(gcsafe).}:
        await io.writeFile(path, @[byte 1, 2, 3])
        var raised = false
        try:
          discard await io.readFile(path, maxSize = 0)
        except IOError:
          raised = true
        doAssert raised

    waitFor run()

  test "writeFile overwrites existing file":
    let path = getTempDir() / "iori_test_overwrite.bin"
    defer:
      removeFile(path)

    proc run() {.async.} =
      {.cast(gcsafe).}:
        await io.writeFile(path, @[byte 1, 2, 3, 4, 5])
        await io.writeFile(path, @[byte 10, 20])
        let readBack = await io.readFile(path, 4096)
        doAssert readBack == @[byte 10, 20]

    waitFor run()

  test "concurrent read and write operations":
    let pathA = getTempDir() / "iori_test_concurrent_a.bin"
    let pathB = getTempDir() / "iori_test_concurrent_b.bin"
    defer:
      removeFile(pathA)
      removeFile(pathB)

    let dataA = @[byte 0xAA, 0xBB, 0xCC]
    let dataB = @[byte 0x11, 0x22, 0x33, 0x44]

    proc run() {.async.} =
      {.cast(gcsafe).}:
        # Write both files concurrently
        let futA = io.writeFile(pathA, dataA)
        let futB = io.writeFile(pathB, dataB)
        await futA
        await futB

        # Read both files concurrently
        let futReadA = io.readFile(pathA, 4096)
        let futReadB = io.readFile(pathB, 4096)
        let resultA = await futReadA
        let resultB = await futReadB

        doAssert resultA == dataA
        doAssert resultB == dataB

    waitFor run()

  test "writeFile to read-only path raises IOError":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        var raised = false
        try:
          await io.writeFile("/proc/nonexistent_iori_test", @[byte 1])
        except IOError:
          raised = true
        doAssert raised

    waitFor run()

  test "readFile with maxSize > uint32 max raises IOError":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        var raised = false
        try:
          discard await io.readFile("/dev/null", maxSize = int(high(uint32)) + 1)
        except IOError:
          raised = true
        doAssert raised

    waitFor run()

  test "full lifecycle inside async proc":
    ## Regression: newUringFileIO() must be callable from async procs.
    ## Without proper {.raises.} annotations, Chronos rejects sync calls
    ## that the compiler treats as raising Exception.
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let io2 = newUringFileIO()
        let path = getTempDir() / "iori_test_async_lifecycle.txt"
        defer:
          removeFile(path)

        await io2.writeFileString(path, "async lifecycle test")
        let content = await io2.readFileString(path)
        doAssert content == "async lifecycle test"
        io2.close()

    waitFor run()

  test "close during pending operations does not crash":
    ## Exercises the processCqes closed-safety guard.
    ## Submit multiple operations, then close before completions arrive.
    var io2 = newUringFileIO(32)
    let path = getTempDir() / "iori_test_close_pending.bin"
    defer:
      removeFile(path)

    proc run() {.async.} =
      {.cast(gcsafe).}:
        # Write a file first so we can read it
        await io2.writeFile(path, @[byte 1, 2, 3, 4])

        # Start multiple reads concurrently
        var futs: seq[Future[seq[byte]]]
        for i in 0 ..< 4:
          futs.add(io2.readFile(path, 4096))

        # Close while operations are in flight
        io2.close()

        # All futures should either complete or fail with IOError
        for fut in futs:
          try:
            discard await fut
          except IOError:
            discard

    waitFor run()

  test "CancelledError is distinct from IOError":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        # CancelledError must not be caught by except IOError
        var caughtCancel = false
        var caughtIO = false
        try:
          raise (ref CancelledError)(msg: "test")
        except IOError:
          caughtIO = true
        except CancelledError:
          caughtCancel = true
        doAssert caughtCancel
        doAssert not caughtIO

    waitFor run()

  test "submit failure in readFile raises IOError not CancelledError":
    var io2 = newUringFileIO(32)
    defer:
      io2.close()

    proc run() {.async.} =
      {.cast(gcsafe).}:
        let path = getTempDir() / "iori_test_cancel_read.txt"
        defer:
          removeFile(path)

        # Create file so path exists
        let fd = posix.open(path.cstring, O_WRONLY or O_CREAT or O_TRUNC, 0o644)
        doAssert fd >= 0
        discard posix.close(fd)

        # Queue readFile — it starts with statx, which becomes unsubmitted
        let readFileFut = io2.readFile(path)

        # Sabotage ring fd to force submit failure, then flush
        let savedFd = io2.ring.ringFd
        io2.ring.ringFd = -1
        io2.flush()
        io2.ring.ringFd = savedFd

        # Submit failure is IOError, not CancelledError
        var caughtIO = false
        try:
          discard await readFileFut
        except CancelledError:
          doAssert false, "should not be CancelledError"
        except IOError:
          caughtIO = true
        doAssert caughtIO

    waitFor run()

  test "readFile with timeout succeeds for normal file":
    let path = getTempDir() / "iori_test_timeout_read.bin"
    defer:
      removeFile(path)

    let data = @[byte 1, 2, 3, 4, 5]

    proc run() {.async.} =
      {.cast(gcsafe).}:
        await io.writeFile(path, data)
        let result = await io.readFile(path, 4096, timeoutMs = 5000)
        doAssert result == data

    waitFor run()

  test "writeFile with timeout succeeds for normal file":
    let path = getTempDir() / "iori_test_timeout_write.bin"
    defer:
      removeFile(path)

    let data = @[byte 10, 20, 30]

    proc run() {.async.} =
      {.cast(gcsafe).}:
        await io.writeFile(path, data, timeoutMs = 5000)
        let result = await io.readFile(path, 4096)
        doAssert result == data

    waitFor run()

  test "readFileString with timeout succeeds":
    let path = getTempDir() / "iori_test_timeout_readstr.txt"
    defer:
      removeFile(path)

    proc run() {.async.} =
      {.cast(gcsafe).}:
        await io.writeFileString(path, "timeout test")
        let result = await io.readFileString(path, 4096, timeoutMs = 5000)
        doAssert result == "timeout test"

    waitFor run()

  test "writeFileString with timeout succeeds":
    let path = getTempDir() / "iori_test_timeout_writestr.txt"
    defer:
      removeFile(path)

    proc run() {.async.} =
      {.cast(gcsafe).}:
        await io.writeFileString(path, "timeout test", timeoutMs = 5000)
        let result = await io.readFileString(path, 4096)
        doAssert result == "timeout test"

    waitFor run()

  test "TimeoutError is distinct from IOError and CancelledError":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        var caughtTimeout = false
        var caughtIO = false
        var caughtCancel = false
        try:
          raise newException(TimeoutError, "test")
        except IOError:
          caughtIO = true
        except CancelledError:
          caughtCancel = true
        except TimeoutError:
          caughtTimeout = true
        doAssert caughtTimeout
        doAssert not caughtIO
        doAssert not caughtCancel

    waitFor run()

  test "timer + cancel pattern cancels blocked bridge read":
    ## Tests the timeout mechanism primitives: sleepMsAsync timer fires,
    ## uringCancel cancels the blocked kernel operation, read gets -ECANCELED.
    proc run() {.async.} =
      {.cast(gcsafe).}:
        var fds: array[2, cint]
        doAssert pipe(fds) == 0
        let readFd = fds[0]
        let writeFd = fds[1]
        defer:
          discard posix.close(readFd)
          discard posix.close(writeFd)

        var buf = newSeq[byte](64)
        let readFut = io.uringRead(readFd, addr buf[0], 64, 0'u64, buf)
        io.flush()

        # Manually replicate the awaitOrTimeout pattern
        let deadline = getMonoTime() + initDuration(milliseconds = 100)
        let remaining = deadline - getMonoTime()
        let timer = sleepMsAsync(int(remaining.inMilliseconds))
        await readFut or timer
        doAssert not readFut.finished, "read should still be blocked on empty pipe"

        # Cancel and drain
        try:
          discard await io.uringCancel(readFut)
        except IOError:
          discard
        let readRes = await readFut
        doAssert readRes == -125, "read should be -ECANCELED: " & $readRes

    waitFor run()
