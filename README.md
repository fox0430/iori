# Iori

Async file I/O library in Nim using io_uring.

- Linux only (io_uring)
- Supports [asyncdispatch](https://nim-lang.org/docs/asyncdispatch.html) or [Chronos](https://github.com/status-im/nim-chronos)

## Requirements

- Nim >= 2.0.2
- Linux 5.6+ (6.1+ recommended)

## Install

```bash
nimble install iori
```

## Usage

```nim
# example.nim

import pkg/iori

proc main() {.async.} =
  let io = newUringFileIO()

  # Write
  await io.writeFileString("/tmp/hello.txt", "Hello, iori!")

  # Read
  let content = await io.readFileString("/tmp/hello.txt")
  echo content

  io.close()

waitFor main()
```

Compile with an async backend:

```bash
# Use std/asyncdispatch
nim c -d:asyncBackend=asyncdispatch -r example.nim

# Use Chronos
nimble install chronos
nim c -d:asyncBackend=chronos -r example.nim
```

## TODO

- Linked SQE (open → read → close in a single chain)
- Fixed buffers (`io_uring_register_buffers`)
- Fixed files (`io_uring_register_files`)
- SQ polling (`IORING_SETUP_SQPOLL`)
- Additional opcodes: `READV`, `WRITEV`, `FALLOCATE`, `UNLINKAT`, `MKDIRAT`

## License

MIT
