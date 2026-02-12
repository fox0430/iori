#### Async backend configuration module.
##
## This module provides the async backend configuration and exports the appropriate
## async framework (asyncdispatch or chronos) based on compile-time flags.

# Async backend configuration. `-d:asyncBackend=asyncdispatch|chronos`

const asyncBackend {.strdefine.} = "asyncdispatch"

const hasAsyncDispatch* = asyncBackend == "asyncdispatch"
const hasChronos* = asyncBackend == "chronos"

when hasChronos:
  import chronos
  export chronos

  proc registerFdReader*(fd: cint, cb: proc() {.gcsafe, raises: [].}) =
    let afd = AsyncFD(fd)
    register2(afd).tryGet()

    addReader2(
      afd,
      proc(udata: pointer) {.gcsafe, raises: [].} =
        cb(),
      nil,
    )
    .tryGet()

  proc unregisterFdReader*(fd: cint) =
    let afd = AsyncFD(fd)
    discard removeReader2(afd)
    discard unregister2(afd)

  proc scheduleSoon*(cb: proc() {.gcsafe, raises: [].}) =
    callSoon(
      proc(udata: pointer) {.gcsafe, raises: [].} =
        cb(),
      nil,
    )

elif hasAsyncDispatch:
  import std/asyncdispatch
  export asyncdispatch

  proc registerFdReader*(fd: cint, cb: proc() {.gcsafe, raises: [].}) =
    let afd = AsyncFD(fd)
    register(afd)
    addRead(
      afd,
      proc(fd: AsyncFD): bool =
        cb()
        return false # keep watching; unregister via unregisterFdReader
      ,
    )

  proc unregisterFdReader*(fd: cint) =
    unregister(AsyncFD(fd))

  proc scheduleSoon*(cb: proc() {.gcsafe, raises: [].}) =
    callSoon(
      proc() {.gcsafe.} =
        cb()
    )

else:
  {.fatal: "Unknown asyncBackend. Use -d:asyncBackend=asyncdispatch|chronos".}
