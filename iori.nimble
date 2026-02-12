# Package

version = "0.1.0"
author = "fox0430"
description = "Async file I/O through io_uring"
license = "MIT"

# Dependencies

requires "nim >= 2.0.2"

task test, "test":
  exec "nim c -d:asyncBackend=asyncdispatch -r tests/all_tests.nim"
  exec "nim c -d:asyncBackend=chronos -r tests/all_tests.nim"
