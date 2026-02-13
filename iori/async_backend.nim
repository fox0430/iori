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

when hasChronos:
  import chronos
  export chronos

  proc registerFdReader*(fd: cint, cb: proc() {.gcsafe, raises: [].}) =
    ## Register a file descriptor for read-readiness notifications on the event loop.
    ## `cb` is called whenever the fd becomes readable.
    let afd = AsyncFD(fd)
    register2(afd).tryGet()

    try:
      addReader2(
        afd,
        proc(udata: pointer) {.gcsafe, raises: [].} =
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
      proc(udata: pointer) {.gcsafe, raises: [].} =
        cb(),
      nil,
    )

elif hasAsyncDispatch:
  import std/asyncdispatch
  export asyncdispatch

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
      proc() {.gcsafe.} =
        cb()
    )

else:
  {.fatal: "Unknown asyncBackend. Use -d:asyncBackend=asyncdispatch|chronos".}
