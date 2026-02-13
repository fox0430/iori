## Tests for uring_file_io: High-level file I/O API.

import std/[unittest, os, posix]

import ../iori/uring_file_io

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
