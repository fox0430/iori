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
# examples/simple.nim

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
nim c -d:asyncBackend=asyncdispatch -r examples/simple.nim

# Use Chronos
nimble install chronos
nim c -d:asyncBackend=chronos -r examples/simple.nim
```

## Documentation

https://fox0430.github.io/iori/iori.html

## See also

If you use [Chronos](https://github.com/status-im/nim-chronos) and want broader
platform support, check out [chronos-file](https://github.com/fox0430/chronos-file).
It is an async file I/O library for Chronos that works on POSIX systems using
`pread`/`pwrite`, with an optional io_uring backend (Linux 5.6+) for truly async
seekable I/O.

## TODO

- SQ polling (`IORING_SETUP_SQPOLL`)
- Additional opcodes: `READV`, `WRITEV`, `FALLOCATE`, `UNLINKAT`, `MKDIRAT`

## License

MIT
