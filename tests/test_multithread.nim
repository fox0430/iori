## Tests for multithread usage: each thread uses its own UringFileIO instance.

import std/[unittest, os]

import ../iori/uring_file_io

const NumThreads = 4

var channels: array[NumThreads, Channel[bool]]

let tmpDir = getTempDir()

proc threadBasicWorker(id: int) {.thread.} =
  var io = newUringFileIO(256)
  var success = true

  proc run() {.async.} =
    {.cast(gcsafe).}:
      let path = tmpDir / "iori_mt_" & $id & "_basic.txt"
      defer:
        removeFile(path)

      let content = "thread " & $id & " data"
      await io.writeFileString(path, content)
      let readResult = await io.readFileString(path, 4096)
      doAssert readResult == content

  try:
    waitFor run()
  except CatchableError:
    success = false

  io.close()
  channels[id].send(success)

proc threadConcurrentWorker(id: int) {.thread.} =
  var io = newUringFileIO(256)
  var success = true

  proc run() {.async.} =
    {.cast(gcsafe).}:
      const numFiles = 4
      var paths: seq[string]
      for i in 0 ..< numFiles:
        paths.add(tmpDir / "iori_mt_" & $id & "_concurrent_" & $i & ".txt")

      defer:
        for p in paths:
          removeFile(p)

      # Write all files concurrently
      var writeFuts: seq[Future[void]]
      for i in 0 ..< numFiles:
        let content = "thread " & $id & " file " & $i
        writeFuts.add(io.writeFileString(paths[i], content))
      for fut in writeFuts:
        await fut

      # Read all files concurrently and verify
      var readFuts: seq[Future[string]]
      for i in 0 ..< numFiles:
        readFuts.add(io.readFileString(paths[i], 4096))
      for i in 0 ..< numFiles:
        let got = await readFuts[i]
        let expected = "thread " & $id & " file " & $i
        doAssert got == expected

  try:
    waitFor run()
  except CatchableError:
    success = false

  io.close()
  channels[id].send(success)

suite "multithread":
  test "basic multithread read/write":
    var threads: array[NumThreads, Thread[int]]
    for i in 0 ..< NumThreads:
      channels[i].open()

    for i in 0 ..< NumThreads:
      createThread(threads[i], threadBasicWorker, i)

    for i in 0 ..< NumThreads:
      let success = channels[i].recv()
      check success

    joinThreads(threads)

    for i in 0 ..< NumThreads:
      channels[i].close()

  test "concurrent multithread read/write":
    var threads: array[NumThreads, Thread[int]]
    for i in 0 ..< NumThreads:
      channels[i].open()

    for i in 0 ..< NumThreads:
      createThread(threads[i], threadConcurrentWorker, i)

    for i in 0 ..< NumThreads:
      let success = channels[i].recv()
      check success

    joinThreads(threads)

    for i in 0 ..< NumThreads:
      channels[i].close()
