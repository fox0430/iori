## Async backend configuration module.
##
## This module provides the async backend configuration and exports the appropriate
## async framework (asyncdispatch or chronos) based on compile-time flags.
## Select the backend at compile time with `-d:asyncBackend=asyncdispatch|chronos`.

# Async backend configuration. `-d:asyncBackend=asyncdispatch|chronos`

const asyncBackend {.strdefine.} = "asyncdispatch"

const hasAsyncDispatch* = asyncBackend == "asyncdispatch"
  ## `true` when the asyncdispatch backend is selected.
const hasChronos* = asyncBackend == "chronos"
  ## `true` when the chronos backend is selected.

type TimeoutError* = object of CatchableError
  ## Raised when an async operation times out.

when hasChronos:
  import chronos
  export chronos

  proc sleepMsAsync*(ms: int): Future[void] =
    ## Sleep for `ms` milliseconds. Wrapper around chronos Duration-based API.
    sleepAsync(milliseconds(ms))

  proc cancelTimer*(fut: Future[void]) =
    ## Cancel a pending timer future to prevent future tracking warnings.
    if not fut.finished():
      fut.cancelSoon()

  type BridgeCancelCallback* = proc(arg: pointer) {.gcsafe, raises: [].}
    ## Callback invoked when a bridge future is cancelled via chronos. `arg` is the
    ## cancelled future itself (a raw pointer; cast it to `Future[int32]`).

  proc setCancelCallback*(fut: Future[int32], cb: BridgeCancelCallback) =
    ## Register `cb` to run if `fut` is cancelled via chronos
    ## (`cancel`/`cancelAndWait`/`cancelSoon`); `cb` receives `fut` as a raw
    ## pointer. `cb` is assigned directly (no wrapper closure) — pass a single
    ## shared callback to avoid per-op allocation. `cb` runs before chronos moves
    ## `fut` to Cancelled. Must be called before `fut` is finished.
    fut.cancelCallback = cb

  proc onComplete*(fut: FutureBase, cb: proc() {.gcsafe, raises: [].}) =
    ## Run `cb` when `fut` finishes (completed, failed or cancelled); if already
    ## finished, on a later event loop tick.
    fut.addCallback(
      proc(udata: pointer) {.gcsafe, raises: [].} =
        cb(),
      nil,
    )

  proc registerFdReader*(fd: cint, cb: proc() {.gcsafe, raises: [].}) =
    ## Register a file descriptor for read-readiness notifications on the event loop.
    ## `cb` is called whenever the fd becomes readable.
    let afd = AsyncFD(fd)
    register2(afd).tryGet()

    try:
      addReader2(
        afd,
        proc(udata: pointer) {.raises: [].} =
          cb(),
        nil,
      )
        .tryGet()
    except CatchableError as e:
      discard unregister2(afd)
      raise e

  proc unregisterFdReader*(fd: cint) =
    ## Remove a previously registered read-readiness watcher from the event loop.
    let afd = AsyncFD(fd)
    discard removeReader2(afd)
    discard unregister2(afd)

  proc scheduleSoon*(cb: proc() {.gcsafe, raises: [].}) =
    ## Schedule `cb` to run on the next event loop tick.
    callSoon(
      proc(udata: pointer) {.raises: [].} =
        cb(),
      nil,
    )

elif hasAsyncDispatch:
  import std/asyncdispatch
  export asyncdispatch

  type CancelledError* = object of CatchableError
    ## Raised when an async operation is cancelled.

  proc sleepMsAsync*(ms: int): Future[void] =
    ## Sleep for `ms` milliseconds.
    sleepAsync(ms)

  proc cancelTimer*(fut: Future[void]) =
    ## No-op: asyncdispatch timers complete harmlessly and are GC'd.
    discard

  type BridgeCancelCallback* = proc(arg: pointer) {.gcsafe, raises: [].}
    ## Callback type kept for API parity with the chronos backend.

  proc setCancelCallback*(fut: Future[int32], cb: BridgeCancelCallback) =
    ## No-op: std/asyncdispatch has no external cancellation callback mechanism,
    ## so bridge futures cannot be cancelled out from under the completion path.
    discard

  proc onComplete*(fut: FutureBase, cb: proc() {.gcsafe, raises: [].}) =
    ## Run `cb` when `fut` finishes (completed, failed or cancelled); if already
    ## finished, on a later event loop tick.
    fut.addCallback(
      proc() {.closure, gcsafe.} =
        cb()
    )

  proc registerFdReader*(fd: cint, cb: proc() {.gcsafe, raises: [].}) =
    ## Register a file descriptor for read-readiness notifications on the event loop.
    ## `cb` is called whenever the fd becomes readable.
    let afd = AsyncFD(fd)
    register(afd)
    try:
      addRead(
        afd,
        proc(fd: AsyncFD): bool =
          cb()
          return false # keep watching; unregister via unregisterFdReader
        ,
      )
    except CatchableError as e:
      unregister(afd)
      raise e

  proc unregisterFdReader*(fd: cint) =
    ## Remove a previously registered read-readiness watcher from the event loop.
    unregister(AsyncFD(fd))

  proc scheduleSoon*(cb: proc() {.gcsafe, raises: [].}) =
    ## Schedule `cb` to run on the next event loop tick.
    callSoon(
      proc() =
        cb()
    )

else:
  {.fatal: "Unknown asyncBackend. Use -d:asyncBackend=asyncdispatch|chronos".}
