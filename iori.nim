## Iori - Async file I/O library for Nim using io_uring.
##
## Provides asynchronous file I/O operations through
## Linux's io_uring interface via direct syscalls (no liburing dependency).
##
## Supports both `asyncdispatch` and `chronos` async backend,
## selectable at compile time with `-d:asyncBackend=asyncdispatch|chronos`.
##
## High-level API
## ==============
##
## .. code-block:: nim
##   let io = newUringFileIO()
##
##   await io.writeFileString("/tmp/hello.txt", "Hello, io_uring!")
##   let content = await io.readFileString("/tmp/hello.txt")
##
##   io.close()
##
## Low-level API
## =============
##
## For finer control, use the low-level procs that return `Future[int32]`:
## `uringOpen`, `uringRead`, `uringWrite`, `uringFsync`, `uringClose`,
## `uringStatx`, `uringRenameat`.
##
## Requirements
## ============
## - Linux 5.6+ (6.1+ recommended)
## - x86_64 or aarch64
## - Nim >= 2.0.2

import iori/uring_file_io
export uring_file_io
