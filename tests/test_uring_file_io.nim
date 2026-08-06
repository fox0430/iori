## Tests for uring_file_io: High-level file I/O API.

import std/[unittest, os, posix, importutils, monotimes, times]

import ../iori/uring_file_io
import ../iori/uring_raw

when hasChronos:
  # `io.pending.len` resolves through std/tables (pending is a Table).
  import std/tables

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
        let readResult = await io.readFile(path)
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
        let readResult = await io.readFileString(path)
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
        let readResult = await io.readFile(path)
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
        let readResult = await io.readFile(path)
        doAssert readResult.len == 0

    waitFor run()

  test "empty string writeFileString and readFileString":
    let path = getTempDir() / "iori_test_empty.txt"
    defer:
      removeFile(path)

    proc run() {.async.} =
      {.cast(gcsafe).}:
        await io.writeFileString(path, "")
        let readResult = await io.readFileString(path)
        doAssert readResult.len == 0

    waitFor run()

  test "writeFile overwrites existing file":
    let path = getTempDir() / "iori_test_overwrite.bin"
    defer:
      removeFile(path)

    proc run() {.async.} =
      {.cast(gcsafe).}:
        await io.writeFile(path, @[byte 1, 2, 3, 4, 5])
        await io.writeFile(path, @[byte 10, 20])
        let readBack = await io.readFile(path)
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
        let futReadA = io.readFile(pathA)
        let futReadB = io.readFile(pathB)
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

  test "writeFile with fsync=false skips fsync":
    let path = getTempDir() / "iori_test_no_fsync.bin"
    defer:
      removeFile(path)

    let data = @[byte 1, 2, 3, 4, 5]

    proc run() {.async.} =
      {.cast(gcsafe).}:
        await io.writeFile(path, data, fsync = false)
        let readResult = await io.readFile(path)
        doAssert readResult == data

    waitFor run()

  test "writeFileString with fsync=false skips fsync":
    let path = getTempDir() / "iori_test_no_fsync.txt"
    defer:
      removeFile(path)

    proc run() {.async.} =
      {.cast(gcsafe).}:
        await io.writeFileString(path, "no fsync test", fsync = false)
        let readResult = await io.readFileString(path)
        doAssert readResult == "no fsync test"

    waitFor run()

  test "writeFile with dataOnly uses fdatasync":
    let path = getTempDir() / "iori_test_datasync.bin"
    defer:
      removeFile(path)

    let data = @[byte 1, 2, 3, 4, 5]

    proc run() {.async.} =
      {.cast(gcsafe).}:
        await io.writeFile(path, data, dataOnly = true)
        let readResult = await io.readFile(path)
        doAssert readResult == data

    waitFor run()

  test "writeFileString with dataOnly uses fdatasync":
    let path = getTempDir() / "iori_test_datasync.txt"
    defer:
      removeFile(path)

    proc run() {.async.} =
      {.cast(gcsafe).}:
        await io.writeFileString(path, "datasync test", dataOnly = true)
        let readResult = await io.readFileString(path)
        doAssert readResult == "datasync test"

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
          futs.add(io2.readFile(path))

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
        let r = await io.readFile(path, timeoutMs = 5000)
        doAssert r == data

    waitFor run()

  test "writeFile with timeout succeeds for normal file":
    let path = getTempDir() / "iori_test_timeout_write.bin"
    defer:
      removeFile(path)

    let data = @[byte 10, 20, 30]

    proc run() {.async.} =
      {.cast(gcsafe).}:
        await io.writeFile(path, data, timeoutMs = 5000)
        let r = await io.readFile(path)
        doAssert r == data

    waitFor run()

  test "readFileString with timeout succeeds":
    let path = getTempDir() / "iori_test_timeout_readstr.txt"
    defer:
      removeFile(path)

    proc run() {.async.} =
      {.cast(gcsafe).}:
        await io.writeFileString(path, "timeout test")
        let r = await io.readFileString(path, timeoutMs = 5000)
        doAssert r == "timeout test"

    waitFor run()

  test "writeFileString with timeout succeeds":
    let path = getTempDir() / "iori_test_timeout_writestr.txt"
    defer:
      removeFile(path)

    proc run() {.async.} =
      {.cast(gcsafe).}:
        await io.writeFileString(path, "timeout test", timeoutMs = 5000)
        let r = await io.readFileString(path)
        doAssert r == "timeout test"

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

        var bufRef = new(seq[byte])
        bufRef[] = newSeq[byte](64)
        let readFut = io.uringRead(readFd, addr bufRef[][0], 64, 0'u64, bufRef)
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

  test "writeFile on FIFO with timeout raises TimeoutError":
    ## Writing to a FIFO with no reader blocks. Tests that the shared deadline
    ## across all sub-operations (open, write, fsync, close) triggers TimeoutError.
    let path = getTempDir() / "iori_test_fifo_write_timeout"
    defer:
      removeFile(path)

    doAssert mkfifo(path.cstring, 0o644) == 0

    proc run() {.async.} =
      {.cast(gcsafe).}:
        var raised = false
        try:
          await io.writeFile(path, @[byte 1, 2, 3], timeoutMs = 100)
        except TimeoutError:
          raised = true
        doAssert raised, "should have raised TimeoutError"

    waitFor run()

  test "writeFile on FIFO with large data times out during write step":
    ## Open a FIFO reader so the write-side open succeeds quickly. Then write
    ## data larger than the pipe buffer (64KB default), which blocks the kernel
    ## write. With timeoutMs=1, the deadline expires during or before the write
    ## step, exercising the near-expiry / already-expired (ms <= 0) path in
    ## awaitOrTimeout.
    let path = getTempDir() / "iori_test_fifo_write_step_timeout"
    defer:
      removeFile(path)

    doAssert mkfifo(path.cstring, 0o644) == 0

    proc run() {.async.} =
      {.cast(gcsafe).}:
        # Open reader with O_NONBLOCK so writer's open doesn't block
        let readerFd = posix.open(path.cstring, O_RDONLY or O_NONBLOCK)
        doAssert readerFd >= 0
        defer:
          discard posix.close(readerFd)

        # 128KB exceeds default pipe buffer (64KB), so write blocks in kernel
        var bigData = newSeq[byte](128 * 1024)
        for i in 0 ..< bigData.len:
          bigData[i] = byte(i mod 256)

        var raised = false
        try:
          await io.writeFile(path, bigData, timeoutMs = 1, fsync = false)
        except TimeoutError:
          raised = true
        doAssert raised, "should have raised TimeoutError"

    waitFor run()

  test "readFile on unreadable file with timeout raises IOError":
    ## statx succeeds (file exists, size > 0) but open fails with EACCES.
    ## IOError must be raised — the timeout wrapper on uringOpen must not mask it.
    let path = getTempDir() / "iori_test_unreadable.bin"
    defer:
      discard chmod(path.cstring, 0o644)
      removeFile(path)

    proc run() {.async.} =
      {.cast(gcsafe).}:
        await io.writeFile(path, @[byte 1, 2, 3])
        discard chmod(path.cstring, 0o000)
        var caughtIO = false
        try:
          discard await io.readFile(path, timeoutMs = 5000)
        except TimeoutError:
          doAssert false, "should not be TimeoutError"
        except IOError:
          caughtIO = true
        doAssert caughtIO

    waitFor run()

  test "writeFile to read-only path with timeout raises IOError":
    ## When open fails immediately (EACCES), IOError must be raised even with
    ## a timeout set — the timeout wrapper must not mask the error.
    proc run() {.async.} =
      {.cast(gcsafe).}:
        var caughtIO = false
        try:
          await io.writeFile("/proc/nonexistent_iori_test", @[byte 1], timeoutMs = 5000)
        except TimeoutError:
          doAssert false, "should not be TimeoutError"
        except IOError:
          caughtIO = true
        doAssert caughtIO

    waitFor run()

  # Direct descriptor variants

  test "writeFileDirect and readFileDirect roundtrip":
    let path = getTempDir() / "iori_test_direct_roundtrip.bin"
    defer:
      removeFile(path)

    let data = @[byte 1, 2, 3, 4, 5, 0xDE, 0xAD, 0xBE, 0xEF]

    proc run() {.async.} =
      {.cast(gcsafe).}:
        io.registerFixedFileSlots(4)
        defer:
          io.unregisterFixedFiles()

        await io.writeFileDirect(path, data)
        let readResult = await io.readFileDirect(path)
        doAssert readResult == data

    waitFor run()

  test "writeFileStringDirect and readFileStringDirect roundtrip":
    let path = getTempDir() / "iori_test_direct_roundtrip.txt"
    defer:
      removeFile(path)

    let content = "Hello, direct descriptors!\nLine 2\n"

    proc run() {.async.} =
      {.cast(gcsafe).}:
        io.registerFixedFileSlots(4)
        defer:
          io.unregisterFixedFiles()

        await io.writeFileStringDirect(path, content)
        let readResult = await io.readFileStringDirect(path)
        doAssert readResult == content

    waitFor run()

  test "readFileDirect on nonexistent file raises IOError":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        io.registerFixedFileSlots(4)
        defer:
          io.unregisterFixedFiles()

        var raised = false
        try:
          discard await io.readFileDirect("/tmp/iori_nonexistent_direct_" & $getpid())
        except IOError:
          raised = true
        doAssert raised

    waitFor run()

  test "writeFileDirect and readFileDirect large data":
    let path = getTempDir() / "iori_test_direct_large.bin"
    defer:
      removeFile(path)

    var data = newSeq[byte](65536)
    for i in 0 ..< data.len:
      data[i] = byte(i mod 256)

    proc run() {.async.} =
      {.cast(gcsafe).}:
        io.registerFixedFileSlots(4)
        defer:
          io.unregisterFixedFiles()

        await io.writeFileDirect(path, data)
        let readResult = await io.readFileDirect(path)
        doAssert readResult.len == data.len
        doAssert readResult == data

    waitFor run()

  test "writeFileDirect and readFileDirect empty data":
    let path = getTempDir() / "iori_test_direct_empty.bin"
    defer:
      removeFile(path)

    proc run() {.async.} =
      {.cast(gcsafe).}:
        io.registerFixedFileSlots(4)
        defer:
          io.unregisterFixedFiles()

        await io.writeFileDirect(path, @[])
        let readResult = await io.readFileDirect(path)
        doAssert readResult.len == 0

    waitFor run()

  test "writeFileDirect overwrites existing file":
    let path = getTempDir() / "iori_test_direct_overwrite.bin"
    defer:
      removeFile(path)

    proc run() {.async.} =
      {.cast(gcsafe).}:
        io.registerFixedFileSlots(4)
        defer:
          io.unregisterFixedFiles()

        await io.writeFileDirect(path, @[byte 1, 2, 3, 4, 5])
        await io.writeFileDirect(path, @[byte 10, 20])
        let readBack = await io.readFileDirect(path)
        doAssert readBack == @[byte 10, 20]

    waitFor run()

  test "writeFileDirect with fsync=false":
    let path = getTempDir() / "iori_test_direct_no_fsync.bin"
    defer:
      removeFile(path)

    let data = @[byte 1, 2, 3, 4, 5]

    proc run() {.async.} =
      {.cast(gcsafe).}:
        io.registerFixedFileSlots(4)
        defer:
          io.unregisterFixedFiles()

        await io.writeFileDirect(path, data, fsync = false)
        let readResult = await io.readFileDirect(path)
        doAssert readResult == data

    waitFor run()

  test "writeFileDirect with dataOnly uses fdatasync":
    let path = getTempDir() / "iori_test_direct_datasync.bin"
    defer:
      removeFile(path)

    let data = @[byte 1, 2, 3, 4, 5]

    proc run() {.async.} =
      {.cast(gcsafe).}:
        io.registerFixedFileSlots(4)
        defer:
          io.unregisterFixedFiles()

        await io.writeFileDirect(path, data, dataOnly = true)
        let readResult = await io.readFileDirect(path)
        doAssert readResult == data

    waitFor run()

  test "concurrent direct read and write operations":
    let pathA = getTempDir() / "iori_test_direct_concurrent_a.bin"
    let pathB = getTempDir() / "iori_test_direct_concurrent_b.bin"
    defer:
      removeFile(pathA)
      removeFile(pathB)

    let dataA = @[byte 0xAA, 0xBB, 0xCC]
    let dataB = @[byte 0x11, 0x22, 0x33, 0x44]

    proc run() {.async.} =
      {.cast(gcsafe).}:
        io.registerFixedFileSlots(4)
        defer:
          io.unregisterFixedFiles()

        let futA = io.writeFileDirect(pathA, dataA)
        let futB = io.writeFileDirect(pathB, dataB)
        await futA
        await futB

        let futReadA = io.readFileDirect(pathA)
        let futReadB = io.readFileDirect(pathB)
        let resultA = await futReadA
        let resultB = await futReadB

        doAssert resultA == dataA
        doAssert resultB == dataB

    waitFor run()

  test "writeFileDirect to read-only path raises IOError":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        io.registerFixedFileSlots(4)
        defer:
          io.unregisterFixedFiles()

        var raised = false
        try:
          await io.writeFileDirect("/proc/nonexistent_iori_test", @[byte 1])
        except IOError:
          raised = true
        doAssert raised

    waitFor run()

  test "readFileDirect with timeout succeeds for normal file":
    let path = getTempDir() / "iori_test_direct_timeout_read.bin"
    defer:
      removeFile(path)

    let data = @[byte 1, 2, 3, 4, 5]

    proc run() {.async.} =
      {.cast(gcsafe).}:
        io.registerFixedFileSlots(4)
        defer:
          io.unregisterFixedFiles()

        await io.writeFileDirect(path, data)
        let r = await io.readFileDirect(path, timeoutMs = 5000)
        doAssert r == data

    waitFor run()

  test "writeFileDirect with timeout succeeds for normal file":
    let path = getTempDir() / "iori_test_direct_timeout_write.bin"
    defer:
      removeFile(path)

    let data = @[byte 10, 20, 30]

    proc run() {.async.} =
      {.cast(gcsafe).}:
        io.registerFixedFileSlots(4)
        defer:
          io.unregisterFixedFiles()

        await io.writeFileDirect(path, data, timeoutMs = 5000)
        let r = await io.readFileDirect(path)
        doAssert r == data

    waitFor run()

  test "writeFileDirect without registered slots raises IOError":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        var raised = false
        try:
          await io.writeFileDirect("/tmp/iori_test_direct_no_slots.bin", @[byte 1])
        except IOError:
          raised = true
        doAssert raised

    waitFor run()

  test "readFileDirect slot not leaked on error":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        io.registerFixedFileSlots(1)
        defer:
          io.unregisterFixedFiles()

        doAssert io.fixedFileSlotsAvailable == 1

        var raised = false
        try:
          discard await io.readFileDirect("/tmp/iori_nonexistent_leak_" & $getpid())
        except IOError:
          raised = true
        doAssert raised

        # Slot must have been returned to the pool
        doAssert io.fixedFileSlotsAvailable == 1

    waitFor run()

  test "writeFileDirect slot not leaked on error":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        io.registerFixedFileSlots(1)
        defer:
          io.unregisterFixedFiles()

        doAssert io.fixedFileSlotsAvailable == 1

        var raised = false
        try:
          await io.writeFileDirect("/proc/nonexistent_iori_leak_test", @[byte 1])
        except IOError:
          raised = true
        doAssert raised

        # Slot must have been returned to the pool
        doAssert io.fixedFileSlotsAvailable == 1

    waitFor run()

  test "readFileDirect without registered slots raises IOError":
    let path = getTempDir() / "iori_test_direct_no_slots.txt"
    defer:
      removeFile(path)

    # Create a file with content so statx returns size > 0
    let fd = posix.open(path.cstring, O_WRONLY or O_CREAT or O_TRUNC, 0o644)
    doAssert fd >= 0
    let data = "test"
    doAssert posix.write(fd, data.cstring, data.len) == data.len
    discard posix.close(fd)

    proc run() {.async.} =
      {.cast(gcsafe).}:
        var raised = false
        try:
          discard await io.readFileDirect(path)
        except IOError:
          raised = true
        doAssert raised

    waitFor run()

  when hasChronos:
    # Regression tests for external (chronos) cancellation of HIGH-LEVEL futures.
    # The timeout composition awaits the bridge future directly (never via `or`),
    # so cancelling a readFile/writeFile future — including with timeoutMs > 0,
    # which chronos `withTimeout` also exercises via cancelSoon — must propagate
    # into the kernel op. Without that, the op runs to completion while the
    # caller believes it was cancelled.

    # Budget for waiting on kernel -ECANCELED CQEs (reaped via the eventfd poll
    # loop): generous so a loaded CI box does not flake, while a genuinely
    # dropped cancel still fails the assert because `pending` never drains.
    const
      reapIters = 250
      reapStepMs = 2

    template drainPending() =
      for _ in 0 ..< reapIters:
        if io.pending.len == 0:
          break
        await sleepMsAsync(reapStepMs)
      doAssert io.pending.len == 0, "op not reaped: " & $io.pending.len

    proc countOpenFds(): int =
      ## Count open fds via /proc/self/fd. Linux-only, like the io_uring tests
      ## themselves. The dir fd walkDir opens for the iteration shows up in the
      ## listing, but both snapshots include it, so the delta stays exact.
      var n = 0
      for _ in walkDir("/proc/self/fd"):
        inc n
      n

    test "chronos cancel of writeFile (timeoutMs > 0) reaches the kernel open":
      ## writeFile on a FIFO with no reader blocks in the open step. Cancelling
      ## the high-level future must cancel the in-flight open and reap its CQE.
      let path = getTempDir() / "iori_test_cancel_hl_open"
      doAssert mkfifo(path.cstring, 0o644) == 0
      defer:
        removeFile(path)

      proc run() {.async.} =
        {.cast(gcsafe).}:
          let fut = io.writeFile(path, @[byte 1, 2, 3], timeoutMs = 60_000)
          await sleepMsAsync(100)
          doAssert io.pending.len == 1, "open should be in flight"

          await fut.cancelAndWait()
          doAssert fut.cancelled()
          drainPending()

      waitFor run()

    test "chronos cancel of writeFile (timeoutMs > 0) reaches the kernel write":
      ## Open a reader so the FIFO open succeeds, fill the pipe buffer so the
      ## kernel write must block, then cancel. The write must stop and the
      ## cleanup close (fd lifecycle) must run, reaping every op.
      let path = getTempDir() / "iori_test_cancel_hl_write"
      doAssert mkfifo(path.cstring, 0o644) == 0
      defer:
        removeFile(path)

      proc run() {.async.} =
        {.cast(gcsafe).}:
          let readerFd = posix.open(path.cstring, O_RDONLY or O_NONBLOCK)
          doAssert readerFd >= 0
          defer:
            discard posix.close(readerFd)

          # Fill the pipe buffer (64KB) so the next write blocks instead of
          # short-completing into free buffer space.
          let fillerFd = posix.open(path.cstring, O_WRONLY or O_NONBLOCK)
          doAssert fillerFd >= 0
          defer:
            discard posix.close(fillerFd)
          # Pin the pipe capacity to 64KB: the default is PIPE_DEF_BUFFERS(16)
          # x PAGE_SIZE, so on 16KB/64KB page systems the 128KB write below
          # would fit and never block, breaking the in-flight assertion.
          const F_SETPIPE_SZ = 1031
          doAssert fcntl(fillerFd, F_SETPIPE_SZ, 64 * 1024) >= 64 * 1024
          var fill = newSeq[byte](64 * 1024)
          var filled = 0
          while filled < fill.len:
            let n = posix.write(fillerFd, addr fill[filled], fill.len - filled)
            if n <= 0:
              break
            filled += n
          doAssert filled == fill.len

          var bigData = newSeq[byte](128 * 1024)
          for i in 0 ..< bigData.len:
            bigData[i] = byte(i mod 256)

          let fdsBaseline = countOpenFds()
          let fut = io.writeFile(path, bigData, timeoutMs = 60_000, fsync = false)
          await sleepMsAsync(100)
          # write + linked close, both submitted
          doAssert io.pending.len == 2, "write chain should be in flight"

          await fut.cancelAndWait()
          doAssert fut.cancelled()
          # The write CQE, the linked close CQE (auto-cancelled by IO_LINK) and
          # the cleanup close CQE must all be reaped. `pending.len == 0` alone
          # cannot tell the cleanup close apart (2 -> 0 without it as well), so
          # the fd count is checked against the pre-writeFile baseline too: a
          # missing cleanup close would leave the FIFO write end open.
          drainPending()
          doAssert countOpenFds() == fdsBaseline, "writeFile fd not closed after cancel"

      waitFor run()

    test "chronos cancel of writeFileDirect (timeoutMs > 0) releases the slot":
      ## writeFileDirect on a FIFO with no reader blocks in openDirect. Cancelling
      ## must reap the whole chain and return the fixed file slot to the pool.
      let path = getTempDir() / "iori_test_cancel_hl_direct"
      doAssert mkfifo(path.cstring, 0o644) == 0
      defer:
        removeFile(path)

      proc run() {.async.} =
        {.cast(gcsafe).}:
          io.registerFixedFileSlots(4)
          defer:
            io.unregisterFixedFiles()
          doAssert io.fixedFileSlotsAvailable == 4

          let fut =
            io.writeFileDirect(path, @[byte 1, 2, 3], timeoutMs = 60_000, fsync = false)
          await sleepMsAsync(100)
          # openDirect + writeFixedFile + closeDirect
          doAssert io.pending.len == 3, "direct chain should be in flight"
          doAssert io.fixedFileSlotsAvailable == 3

          await fut.cancelAndWait()
          doAssert fut.cancelled()
          drainPending()
          doAssert io.fixedFileSlotsAvailable == 4, "slot not released"

      waitFor run()

    test "chronos withTimeout on writeFile cancels the kernel op":
      ## chronos withTimeout cancels the target via cancelSoon on expiry — the
      ## same external-cancel path, exercised through the std-compatible API.
      let path = getTempDir() / "iori_test_withtimeout_hl"
      doAssert mkfifo(path.cstring, 0o644) == 0
      defer:
        removeFile(path)

      proc run() {.async.} =
        {.cast(gcsafe).}:
          let fut = io.writeFile(path, @[byte 1, 2, 3], timeoutMs = 60_000)
          await sleepMsAsync(100)
          doAssert io.pending.len == 1, "open should be in flight"

          let completed = await fut.withTimeout(chronos.milliseconds(50))
          doAssert not completed
          drainPending()

      waitFor run()

    # No readFile-cancel regression test: readFile opens O_RDONLY, and an
    # O_RDONLY open of a FIFO does not block in io_uring (observed on kernel
    # 7.1.5; only the write end blocks), while statx/read of a FIFO complete
    # instantly and regular files never block. No readFile phase can be made
    # to stall deterministically, so any such test would pass even on a
    # regression that drops the cancel. The composition machinery is covered
    # by the writeFile tests above and the white-box tests in
    # test_uring_file_io_internal.nim.
