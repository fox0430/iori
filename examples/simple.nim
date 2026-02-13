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
