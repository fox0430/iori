## White-box tests for the timeout/cancel machinery in uring_file_io.
##
## The module is included (not imported) so the private helpers are reachable:
## `releaseSlotOnSettle` and `awaitBounded` (and the completion race of
## `awaitOrTimeout`) can only trigger when a closeDirect outlives its deadline
## in the kernel — a close never blocks on a regular file, so no public-API test
## can reach those branches. This file exercises them directly.

import std/[unittest, os, posix, monotimes, times, importutils]

include ../iori/uring_file_io

privateAccess(UringFileIO)

suite "uring_file_io internal":
  var io {.threadvar.}: UringFileIO

  setup:
    io = newUringFileIO(256)

  teardown:
    io.close()

  test "releaseSlotOnSettle holds the slot until the close settles":
    ## A closeDirect that outlives its deadline keeps running in the
    ## background; the fixed-file slot must stay held until it settles, so a
    ## new openDirect can never install a file that the lingering close then
    ## closes.
    proc run() {.async.} =
      {.cast(gcsafe).}:
        io.registerFixedFileSlots(1)
        defer:
          io.unregisterFixedFiles()
        doAssert io.fixedFileSlotsAvailable == 1

        let slot = allocFixedFileSlot(io)
        doAssert io.fixedFileSlotsAvailable == 0
        let closeFut = newFuture[int32]("testCloseDirect")
        releaseSlotOnSettle(io, slot, closeFut)
        doAssert io.fixedFileSlotsAvailable == 0, "slot held while close in flight"

        closeFut.complete(0'i32)
        await sleepMsAsync(10)
        doAssert io.fixedFileSlotsAvailable == 1, "slot freed once the close settles"

    waitFor run()

  test "releaseSlotOnSettle frees the slot immediately for a settled close":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        io.registerFixedFileSlots(1)
        defer:
          io.unregisterFixedFiles()

        let slot = allocFixedFileSlot(io)
        let closeFut = newFuture[int32]("testCloseDirectDone")
        closeFut.complete(0'i32)
        releaseSlotOnSettle(io, slot, closeFut)
        doAssert io.fixedFileSlotsAvailable == 1, "settled close frees immediately"

    waitFor run()

  test "awaitBounded drains a settled future even past the deadline":
    ## The ms <= 0 + fut.finished branch: a close that already settled must
    ## return its result, not raise TimeoutError, when the deadline passed.
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let fut = newFuture[int32]("testClose")
        fut.complete(42'i32)
        let past = getMonoTime() - initDuration(milliseconds = 10)
        let res = await awaitBounded(io, fut, past, Operation.close)
        doAssert res == 42

    waitFor run()

  test "awaitBounded raises TimeoutError for an unsettled future past the deadline":
    ## The ms <= 0 + not finished branch: the wait ends with TimeoutError
    ## without cancelling the underlying close.
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let fut = newFuture[int32]("testClose")
        let past = getMonoTime() - initDuration(milliseconds = 10)
        var raised = false
        try:
          discard await awaitBounded(io, fut, past, Operation.close)
        except TimeoutError:
          raised = true
        doAssert raised
        doAssert not fut.finished, "op must keep running in the background"

    waitFor run()

  test "awaitBounded past the deadline still submits a queued close":
    ## A close queued but not yet submitted when the deadline expires must be
    ## flushed so it runs in the background — raising without flushing would
    ## leave the SQE unsubmitted, to be dropped when the ring is torn down
    ## (fd leak).
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let path = getTempDir() / "iori_test_internal_past_close.bin"
        defer:
          removeFile(path)
        let fd = posix.open(path.cstring, O_WRONLY or O_CREAT or O_TRUNC, 0o644)
        doAssert fd >= 0

        let closeFut = io.uringClose(fd.cint)
        let past = getMonoTime() - initDuration(milliseconds = 10)
        var raised = false
        try:
          discard await awaitBounded(io, closeFut, past, Operation.close)
        except TimeoutError:
          raised = true
        doAssert raised
        doAssert not closeFut.finished, "close must keep running in the background"
        doAssert io.unsubmitted.len == 0, "close must reach the kernel, not sit queued"

        for _ in 0 ..< 250:
          if fcntl(fd.cint, F_GETFD) < 0:
            break
          await sleepMsAsync(2)
        doAssert fcntl(fd.cint, F_GETFD) == -1, "fd must be closed"

    waitFor run()

  test "awaitBounded times out while the op keeps running":
    ## The ms > 0 timer branch: TimeoutError after the deadline, without
    ## cancelling the underlying future.
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let fut = newFuture[int32]("testClose")
        var raised = false
        try:
          discard await awaitBounded(
            io, fut, getMonoTime() + initDuration(milliseconds = 50), Operation.close
          )
        except TimeoutError:
          raised = true
        doAssert raised
        doAssert not fut.finished, "op must keep running in the background"

    waitFor run()

  test "awaitBounded returns the result when the future settles before the deadline":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let fut = newFuture[int32]("testClose")
        let bridge = awaitBounded(
          io, fut, getMonoTime() + initDuration(milliseconds = 50), Operation.close
        )
        await sleepMsAsync(10)
        fut.complete(7'i32)
        doAssert (await bridge) == 7

    waitFor run()

  when hasChronos:
    test "awaitBounded default path: caller cancel must not kill the close":
      ## timeoutMs = 0 uses the default-deadline path. External cancellation of
      ## the caller must end the wait but leave the close running — killing it
      ## would leak the fd (the `closed = true` fast paths skip the finally).
      proc run() {.async.} =
        {.cast(gcsafe).}:
          let path = getTempDir() / "iori_test_internal_default_close.bin"
          defer:
            removeFile(path)
          let fd = posix.open(path.cstring, O_WRONLY or O_CREAT or O_TRUNC, 0o644)
          doAssert fd >= 0

          let closeFut = io.uringClose(fd.cint)
          let waitFut = awaitBounded(io, closeFut, default(MonoTime), Operation.close)
          doAssert not closeFut.finished

          await waitFut.cancelAndWait()
          doAssert waitFut.cancelled()
          doAssert not closeFut.cancelled(), "close must keep running"

          let closeRes = await closeFut
          doAssert closeRes == 0
          doAssert fcntl(fd.cint, F_GETFD) == -1, "fd must be closed"

      waitFor run()

  test "awaitOrTimeout adopts a real result completing after the deadline":
    ## The completion race: the deadline expired (the timeout mark was set),
    ## but the op then completed with a real result — it must be adopted, not
    ## turned into TimeoutError.
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let fut = newFuture[int32]("testRead")
        let past = getMonoTime() - initDuration(milliseconds = 10)
        let waitFut = awaitOrTimeout(io, fut, past, Operation.read)
        await sleepMsAsync(10)
        doAssert not waitFut.finished
        fut.complete(42'i32)
        doAssert (await waitFut) == 42

    waitFor run()

  test "awaitOrTimeout raises TimeoutError for a cancel result after the deadline":
    proc run() {.async.} =
      {.cast(gcsafe).}:
        let fut = newFuture[int32]("testRead")
        let past = getMonoTime() - initDuration(milliseconds = 10)
        let waitFut = awaitOrTimeout(io, fut, past, Operation.read)
        await sleepMsAsync(10)
        fut.complete(-125'i32)
        var raised = false
        try:
          discard await waitFut
        except TimeoutError:
          raised = true
        doAssert raised

    waitFor run()
