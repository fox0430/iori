## eventfd bridge + Low-level API for io_uring.
##
## Integrates io_uring with async event loop via eventfd.
## Provides low-level API returning `Future[int32]` for each io_uring operation.

when not defined(linux):
  {.fatal: "uring_bridge requires Linux".}

import std/[tables, posix, oserrors, sequtils]

import async_backend, uring_raw

when hasChronos:
  import std/sets

export async_backend

proc eventfd(initval: cuint, flags: cint): cint {.importc, header: "<sys/eventfd.h>".}

type
  CompletionKind = enum
    ckOpen
    ckRead
    ckWrite
    ckFsync
    ckClose
    ckStatx
    ckRename
    ckCancel

  Completion = object
    future: Future[int32]
    kind: CompletionKind
    bufRef: ref seq[byte] # GC root for buffer
    strRef: ref string
      # GC root for path string (ref ensures stable cstring through copies)
    strRef2: ref string # GC root for second path string (rename)
    statxRef: ref Statx # GC root for statx output buffer

  UringFileIO* = ref object
    ## Handle for async file I/O via io_uring.
    ## Create with `newUringFileIO`, release with `close`.
    ring: IoUring
    eventFd: cint
    nextId: uint64
    pending: Table[uint64, Completion]
    futureToId: Table[pointer, uint64]
    closed: bool
    error: ref CatchableError # Propagated to pending futures
    selfRef: UringFileIO # GC root: prevents collection while poll loop holds raw pointer
    flushScheduled: bool # Prevents duplicate callSoon scheduling
    unsubmitted: seq[uint64] # IDs not yet submitted (for failing on submit error)
    chainActive: bool
    chainIds: seq[uint64] # SQE IDs queued during current chain (for rollback)
    chainFutures: seq[Future[int32]] # Futures queued during current chain
    chainFailed: bool # Whether a getSqe returned nil during chain
    deferredCancels: seq[Future[int32]]
      # Submitted ops whose external-cancel ASYNC_CANCEL could not be issued yet
      # (a chain was open, or the SQ was full); retried by drainDeferredCancels
      # after endChain / flush.
    cancelCb: BridgeCancelCallback
      # Shared chronos cancel callback (one per instance, not per op)
    fixedBufs: seq[seq[byte]] # Registered fixed buffers (GC root)
    fixedBufsRegistered: bool
    fixedFiles: seq[cint] # Registered fixed file descriptors (copy)
    fixedFilesRegistered: bool
    fixedFileSlotsFree: seq[int32] # Direct open: free slot stack

const
  EFD_NONBLOCK = 0x00000800.cint
  EFD_CLOEXEC = 0x00080000.cint

proc allocId(u: UringFileIO): uint64 =
  ## ID allocation
  result = u.nextId
  u.nextId += 1

proc stopPollLoop(u: UringFileIO) {.raises: [].} =
  try:
    unregisterFdReader(u.eventFd)
  except CatchableError as e:
    if u.error == nil:
      u.error = e
  u.selfRef = nil

template castGcsafeNoRaise(body: untyped) =
  ## Assert to the compiler that `body` is gcsafe and non-raising. The bridge runs
  ## on a single-threaded event loop, so the captured `UringFileIO` is never touched
  ## concurrently (gcsafe), and the io_uring SQE/queue calls inside do not actually
  ## raise — but the compiler cannot prove either through the chronos cancel-callback
  ## boundary. Centralizes the `{.cast(gcsafe).}: {.cast(raises: []).}:` pair used by
  ## the external-cancellation paths (tryKernelCancel / queueAsyncCancel /
  ## handleExternalCancel).
  {.cast(gcsafe).}:
    {.cast(raises: []).}:
      body

proc settleIfPending(fut: Future[int32], val: int32) {.raises: [].} =
  ## Complete `fut` with `val` unless it is already finished. A bridge future can
  ## be moved to Cancelled by external chronos cancellation while its kernel op is
  ## still in flight; re-completing such a finished future would raise a
  ## FutureDefect (chronos no-ops complete/fail only for the *cancelled* state,
  ## and asyncdispatch is not cancel-safe either). The cast suppresses the
  ## theoretical ValueError from Future.complete.
  {.cast(raises: []).}:
    if not fut.finished:
      fut.complete(val)

proc failIfPending(fut: Future[int32], err: ref CatchableError) {.raises: [].} =
  ## Fail `fut` with `err` unless it is already finished — the error-path mirror of
  ## settleIfPending. An externally-cancelled future is already Cancelled and must
  ## not be re-failed (chronos no-ops fail() only for the cancelled state; any other
  ## finished state raises FutureDefect).
  {.cast(raises: []).}:
    if not fut.finished:
      fut.fail(err)

proc processCqes(u: UringFileIO) {.raises: [].} =
  ## CQE processing
  while not u.closed:
    let cqe = peekCqe(u.ring)
    if cqe == nil:
      break
    let id = cqe.userData
    let res = cqe.res
    advanceCq(u.ring)

    var comp: Completion
    if u.pending.pop(id, comp):
      u.futureToId.del(cast[pointer](comp.future))
      # The future may already be finished when its CQE arrives if a consumer
      # cancelled it externally (see handleExternalCancel): chronos has moved it
      # to Cancelled while the Completion stayed rooted in `pending` until the
      # kernel was done. We still pop here to release the GC roots, but
      # settleIfPending must not re-finish it.
      settleIfPending(comp.future, res)

proc close*(u: UringFileIO) {.raises: [].} =
  ## Close the UringFileIO instance. Fails all pending futures and releases resources.
  ## If `u.error` is set, pending futures are failed with that error.
  if u.closed:
    return
  u.closed = true

  if u.chainActive:
    u.chainActive = false
    if u.chainIds.len > 0:
      rollbackSqes(u.ring, uint32(u.chainIds.len))

  stopPollLoop(u)
  u.flushScheduled = false
  u.unsubmitted.setLen(0)
  u.deferredCancels.setLen(0)
  u.cancelCb = nil # break the u -> cancelCb -> u reference cycle

  let err =
    if u.error != nil:
      u.error
    else:
      newException(IOError, "UringFileIO closed")

  # Collect and clear pending before failing — fail() may trigger callbacks
  # that attempt to modify the table.
  var pending: seq[Completion]
  for id, comp in u.pending:
    pending.add(comp)
  u.pending.clear()
  u.futureToId.clear()
  for comp in pending:
    # A pending future may already be finished if a consumer cancelled it
    # externally (chronos marks it Cancelled while its Completion stayed rooted
    # here until the kernel was done): failIfPending skips those.
    failIfPending(comp.future, err)

  # Fail chain futures not in pending (e.g. from chainFailed path)
  for fut in u.chainFutures:
    failIfPending(fut, err)
  u.chainIds.setLen(0)
  u.chainFutures.setLen(0)

  if u.fixedFilesRegistered:
    unregisterFiles(u.ring)
    u.fixedFilesRegistered = false
    u.fixedFiles.setLen(0)
    u.fixedFileSlotsFree.setLen(0)

  if u.fixedBufsRegistered:
    unregisterBuffers(u.ring)
    u.fixedBufsRegistered = false
    u.fixedBufs.setLen(0)

  unregisterEventfd(u.ring)
  discard close(u.eventFd)
  u.eventFd = -1
  closeRing(u.ring)

proc drainEventfd(u: UringFileIO) =
  ## Read and discard the eventfd counter. Ignores EAGAIN.
  var buf: uint64
  let ret = read(u.eventFd, addr buf, sizeof(buf))
  if ret < 0:
    let err = errno
    if err != posix.EAGAIN and err != posix.EINTR:
      raiseOSError(OSErrorCode(err), "eventfd read failed")

proc startPollLoop(u: UringFileIO) {.raises: [OSError].} =
  u.selfRef = u
  try:
    registerFdReader(
      u.eventFd,
      proc() {.raises: [].} =
        if u.closed:
          return
        try:
          u.drainEventfd()
        except OSError as e:
          u.error = e
          u.close()
        u.processCqes(),
    )
  except CatchableError as e:
    u.selfRef = nil
    raise (ref OSError)(msg: "eventfd registration failed: " & e.msg)

# Single forward declaration: the submit/cancel machinery forms a genuine cycle —
#   flush -> drainDeferredCancels -> tryKernelCancel -> queueAsyncCancel ->
#   queueSqe -> flush (queueSqe re-arms flush via scheduleSoon).
# Every other proc below is defined before its callers; flush is defined early
# (it is the public entry point queueSqe schedules), so the one edge that must
# point forward is flush -> drainDeferredCancels.
proc drainDeferredCancels(u: UringFileIO) {.gcsafe, raises: [].}

proc flush*(u: UringFileIO) {.raises: [].} =
  ## Flush all queued SQEs to the kernel in a single io_uring_enter syscall.
  if u.chainActive:
    return # Keep flushScheduled; endChain will allow flush
  u.flushScheduled = false
  if u.closed:
    return

  # submit() makes no syscall when nothing is pending, but still publishes SQEs
  # that a cancel neutralized into NOPs (those are no longer tracked in
  # `unsubmitted`): flushing them frees their SQ slots so the deferred-cancel
  # retry below is not wedged behind stale NOPs.
  let ret = submit(u.ring)
  if ret < 0:
    # Submit failed: io_uring_enter rolled the SQ ring back (uring_raw submit()),
    # discarding every SQE queued this batch — including any ASYNC_CANCEL. Fail the
    # unsubmitted user futures (already-submitted ones still complete via CQE).
    let err =
      newException(IOError, "io_uring submit failed: " & osErrorMsg(OSErrorCode(-ret)))
    for id in u.unsubmitted:
      var comp: Completion
      if u.pending.pop(id, comp):
        u.futureToId.del(cast[pointer](comp.future))
        failIfPending(comp.future, err)
    when hasChronos:
      # A rolled-back ASYNC_CANCEL for an externally-cancelled submitted op is now
      # lost, and tryKernelCancel already dropped that target from deferredCancels
      # (it reported the queued-but-unsubmitted cancel as success). Without this it
      # would never retry, stranding the op's Completion (and its buffer) in
      # `pending` until close(). Re-defer the cancel of every still-pending op whose
      # future was already settled externally (a finished future left in `pending`
      # can only be one chronos cancelled out from under its in-flight kernel op).
      # Snapshot already-deferred targets into a set so the dedup is O(n+m), not
      # O(n*m) (one anyIt scan of deferredCancels per pending entry).
      # containsOrIncl folds the membership check and insert, so a finished pending
      # future is re-deferred only the first time it is seen.
      var deferred = initHashSet[pointer]()
      for fut in u.deferredCancels:
        deferred.incl(cast[pointer](fut))
      for comp in u.pending.values:
        if comp.future.finished and
            not deferred.containsOrIncl(cast[pointer](comp.future)):
          u.deferredCancels.add(comp.future)
  u.unsubmitted.setLen(0)

  # Retry kernel cancels deferred because the SQ ring was full — submitting above
  # freed slots. Run this even when there was nothing to submit: a deferred
  # cancel must not depend on this particular flush carrying work.
  u.drainDeferredCancels()

proc removeUnsubmitted(u: UringFileIO, id: uint64): bool {.raises: [].} =
  ## Remove `id` from the unsubmitted queue if present. Returns true if it was
  ## found and removed. Shared by dropUnsubmitted and endChain's rollback so the
  ## linear scan lives in one place.
  for i, qid in u.unsubmitted:
    if qid == id:
      u.unsubmitted.delete(i)
      return true
  false

proc dropUnsubmitted(u: UringFileIO, id: uint64): bool {.raises: [].} =
  ## If `id` is still queued-but-unsubmitted, neutralize it locally without a
  ## kernel roundtrip: remove it from `unsubmitted`, turn its queued SQE into a
  ## NOP, and drop its Completion (releasing its GC roots). The stale NOP, if it
  ## is ever flushed, is ignored by processCqes since its id is no longer in
  ## `pending`. Returns true if the op was unsubmitted (handled here), false if
  ## it had already been submitted to the kernel.
  ##
  ## The caller decides how to settle the future: `uringCancel` completes it with
  ## -ECANCELED (its low-level contract); external cancellation leaves it for
  ## chronos to mark Cancelled.
  if not u.removeUnsubmitted(id):
    return false
  nopifySqe(u.ring, id)
  var comp: Completion
  if u.pending.pop(id, comp):
    u.futureToId.del(cast[pointer](comp.future))
    # If the dropped op is a member of the chain currently being built, remove
    # its future from chainFutures so endChain does not hand back an already
    # settled future. Its id stays in chainIds: the SQE slot is still allocated
    # (now a NOP), so endChain's rollback count must keep counting it.
    if u.chainActive:
      let p = cast[pointer](comp.future)
      u.chainFutures.keepItIf(cast[pointer](it) != p)
  true

proc queueSqe(u: UringFileIO, comp: Completion, armCancel = true): Future[int32] =
  ## Queue the most recently prepared SQE for batch submission.
  ## The caller must have already called getSqe and filled the SQE fields
  ## (except userData, which this proc sets).
  ## The SQE will be submitted on the next event loop tick via callSoon.
  ## Pass `armCancel = false` for the internal ASYNC_CANCEL op: cancelling a
  ## cancel has no useful meaning and would re-enter the cancel machinery.
  let fut = comp.future
  let id = u.allocId()

  setLastSqeUserData(u.ring, id)

  if u.chainActive:
    setLastSqeFlags(u.ring, IOSQE_IO_LINK)
    u.chainIds.add(id)
    u.chainFutures.add(fut)

  u.pending[id] = comp
  u.futureToId[cast[pointer](fut)] = id
  u.unsubmitted.add(id)
  if armCancel:
    # Wire external (chronos) cancellation to handleExternalCancel via the shared
    # per-instance `u.cancelCb` (built once in newUringFileIO), so arming allocates
    # nothing. No-op on asyncdispatch: `u.cancelCb` is nil and setCancelCallback
    # discards both args, keeping that backend's hot path allocation-free.
    setCancelCallback(fut, u.cancelCb)

  if not u.flushScheduled:
    u.flushScheduled = true
    scheduleSoon(
      proc() {.raises: [].} =
        u.flush()
    )

  return fut

proc queueAsyncCancel(
    u: UringFileIO, targetId: uint64, cancelFut: Future[int32]
): bool {.gcsafe, raises: [].} =
  ## Build and queue an io_uring ASYNC_CANCEL SQE for the submitted op `targetId`,
  ## using `cancelFut` as the cancel op's own result future. Returns false (queuing
  ## nothing) if the SQ ring was full. Preconditions: not closed, not chainActive,
  ## target already submitted. Shared by the public `uringCancel` and the internal
  ## `tryKernelCancel` so the ASYNC_CANCEL SQE is constructed in exactly one place.
  castGcsafeNoRaise:
    let sqe = getSqe(u.ring)
    if sqe == nil:
      return false
    sqe.opcode = IORING_OP_ASYNC_CANCEL
    sqe.`addr` = targetId
    var comp = Completion(future: cancelFut, kind: ckCancel)
    discard queueSqe(u, comp, armCancel = false)
    return true

proc tryKernelCancel(
    u: UringFileIO, target: Future[int32], tid: uint64
): bool {.gcsafe, raises: [].} =
  ## Try to issue an io_uring ASYNC_CANCEL for an externally-cancelled *submitted*
  ## op right now. `tid` is the target's pending id from futureToId (0 if already
  ## reaped); callers pass it so the lookup is not repeated. Returns true if it was
  ## issued (or the op was already reaped by its CQE — nothing to do), false if it
  ## must be retried later because either:
  ##   * a linked chain is mid-construction — inserting an ASYNC_CANCEL SQE would
  ##     split it (IOSQE_IO_LINK links each SQE to the one physically following
  ##     it, so the next user op would be cancelled — data loss); or
  ##   * the SQ ring is full — getSqe returned nil, so uringCancel could not queue
  ##     the ASYNC_CANCEL.
  ## drainDeferredCancels drives the retry (after endChain and after flush).
  if tid == 0'u64:
    return true # already reaped by its CQE — nothing to cancel
  if u.chainActive:
    return false # chain still open — defer to avoid grafting into the chain
  castGcsafeNoRaise:
    # The cancel op's own future is internal: its CQE result is intentionally
    # discarded (we follow chronos cancellation, not the -125 contract).
    let cancelFut = newFuture[int32]("kernelCancel")
    result = u.queueAsyncCancel(tid, cancelFut)
    if not result:
      # SQ full: nothing was queued, so cancelFut would be an unfinished orphan.
      # Settle it so it does not leak; the target stays in deferredCancels and
      # the next drain retries.
      cancelFut.complete(0'i32)

proc drainDeferredCancels(u: UringFileIO) {.gcsafe, raises: [].} =
  ## Retry kernel cancels that could not be issued when their future was first
  ## cancelled (a linked chain was open, or the SQ ring was full). Called after
  ## endChain finalizes a chain and after flush frees SQ slots, so retries are
  ## driven by the event that unblocks them — no per-tick polling. Entries that
  ## still cannot be issued are kept for the next drain.
  if u.deferredCancels.len == 0:
    return
  # Keep only targets whose cancel still can't be issued, compacting in place
  # (keepItIf does not reallocate). Callers (flush, endChain) never invoke this on
  # a closed instance and close() already empties deferredCancels, so no per-entry
  # closed check is needed. An entry may have been reaped since it was deferred, so
  # re-resolve its id here (0 = reaped → tryKernelCancel reports nothing to do).
  u.deferredCancels.keepItIf(
    not u.tryKernelCancel(it, u.futureToId.getOrDefault(cast[pointer](it), 0'u64))
  )

when hasChronos:
  # Chronos-only: this is the body of the per-instance `cancelCb` wired in
  # newUringFileIO. asyncdispatch has no external-cancellation callback
  # (setCancelCallback is a no-op there), so under that backend the proc would be
  # dead code — Nim flags it XDeclaredButNotUsed. Gating it here keeps it out of
  # the asyncdispatch build entirely.
  proc handleExternalCancel(
      u: UringFileIO, target: Future[int32]
  ) {.gcsafe, raises: [].} =
    ## Make an externally-cancelled bridge future stop its kernel op and settle in
    ## the *Cancelled* state — consistently for submitted and unsubmitted ops.
    ##
    ## Runs when a consumer cancels the future through the async backend (chronos
    ## `cancel`/`cancelAndWait`/`cancelSoon`); wired as the shared cancel callback in
    ## queueSqe via setCancelCallback. An unsubmitted
    ## op — whose buffer the kernel never saw — is neutralized into a NOP and its
    ## Completion dropped immediately. A submitted op is cancelled with io_uring
    ## ASYNC_CANCEL and its Completion is left in `pending` (keeping the buffer
    ## GC-rooted) until the -ECANCELED CQE reaps it; if the cancel cannot be issued
    ## right now (a chain is open, or the SQ is full) it is queued in
    ## `deferredCancels` and retried by drainDeferredCancels. In no case do we
    ## complete the future here: chronos moves it to Cancelled right after this
    ## callback.
    ##
    ## Deliberately different from the public `uringCancel`, which *completes* the
    ## target with -ECANCELED; here we follow chronos cancellation semantics.
    if u.closed:
      return
    castGcsafeNoRaise:
      # Single futureToId probe: ids start at 1, so 0 means "not tracked" —
      # already reaped by its CQE, nothing to cancel.
      let tid = u.futureToId.getOrDefault(cast[pointer](target), 0'u64)
      if tid == 0'u64:
        return
      if u.dropUnsubmitted(tid):
        discard # unsubmitted: handled locally; chronos marks it Cancelled
      elif not u.tryKernelCancel(target, tid):
        # `tid` is still valid: dropUnsubmitted returned false without touching
        # futureToId (the op was already submitted, not in `unsubmitted`).
        u.deferredCancels.add(target) # submitted but blocked — retry later

proc uringOpen*(
    u: UringFileIO, path: string, flags: cint, mode: uint32 = 0o644
): Future[int32] =
  ## Submit OPENAT operation. Returns Future with fd on success or negative errno.
  let fut = newFuture[int32]("uringOpen")

  if u.closed:
    fut.fail(
      if u.error != nil:
        u.error
      else:
        newException(IOError, "UringFileIO closed")
    )
    return fut

  let sqe = getSqe(u.ring)
  if sqe == nil:
    if u.chainActive:
      u.chainFailed = true
      u.chainFutures.add(fut)
      return fut
    fut.fail(newException(IOError, "io_uring SQ full"))
    return fut

  # Keep path alive until CQE arrives (ref string ensures stable cstring pointer)
  var pathRef: ref string
  new(pathRef)
  pathRef[] = path

  sqe.opcode = IORING_OP_OPENAT
  sqe.fd = AT_FDCWD
  sqe.`addr` = cast[uint64](pathRef[].cstring)
  sqe.len = uint32(mode)
  sqe.opFlags = cast[uint32](flags)

  var comp = Completion(future: fut, kind: ckOpen, strRef: pathRef)
  return queueSqe(u, comp)

proc uringRead*(
    u: UringFileIO,
    fd: cint,
    buf: pointer,
    size: uint32,
    offset: uint64,
    bufRef: ref seq[byte],
): Future[int32] =
  ## Submit READ operation. Returns Future with bytes read or negative errno.
  ## bufRef keeps the buffer GC-rooted until completion.
  let fut = newFuture[int32]("uringRead")

  if u.closed:
    fut.fail(
      if u.error != nil:
        u.error
      else:
        newException(IOError, "UringFileIO closed")
    )
    return fut

  let sqe = getSqe(u.ring)
  if sqe == nil:
    if u.chainActive:
      u.chainFailed = true
      u.chainFutures.add(fut)
      return fut
    fut.fail(newException(IOError, "io_uring SQ full"))
    return fut

  sqe.opcode = IORING_OP_READ
  sqe.fd = fd
  sqe.`addr` = cast[uint64](buf)
  sqe.len = size
  sqe.off = offset

  var comp = Completion(future: fut, kind: ckRead, bufRef: bufRef)
  return queueSqe(u, comp)

proc uringWrite*(
    u: UringFileIO,
    fd: cint,
    buf: pointer,
    size: uint32,
    offset: uint64,
    bufRef: ref seq[byte],
): Future[int32] =
  ## Submit WRITE operation. Returns Future with bytes written or negative errno.
  ## bufRef keeps the buffer GC-rooted until completion.
  let fut = newFuture[int32]("uringWrite")

  if u.closed:
    fut.fail(
      if u.error != nil:
        u.error
      else:
        newException(IOError, "UringFileIO closed")
    )
    return fut

  let sqe = getSqe(u.ring)
  if sqe == nil:
    if u.chainActive:
      u.chainFailed = true
      u.chainFutures.add(fut)
      return fut
    fut.fail(newException(IOError, "io_uring SQ full"))
    return fut

  sqe.opcode = IORING_OP_WRITE
  sqe.fd = fd
  sqe.`addr` = cast[uint64](buf)
  sqe.len = size
  sqe.off = offset

  var comp = Completion(future: fut, kind: ckWrite, bufRef: bufRef)
  return queueSqe(u, comp)

proc uringFsync*(u: UringFileIO, fd: cint, dataOnly: bool = false): Future[int32] =
  ## Submit FSYNC operation. Returns Future with 0 on success or negative errno.
  ## If dataOnly is true, only file data is flushed (fdatasync semantics) by
  ## setting the IORING_FSYNC_DATASYNC flag, leaving non-essential metadata
  ## (e.g. mtime) unsynced.
  let fut = newFuture[int32]("uringFsync")

  if u.closed:
    fut.fail(
      if u.error != nil:
        u.error
      else:
        newException(IOError, "UringFileIO closed")
    )
    return fut

  let sqe = getSqe(u.ring)
  if sqe == nil:
    if u.chainActive:
      u.chainFailed = true
      u.chainFutures.add(fut)
      return fut
    fut.fail(newException(IOError, "io_uring SQ full"))
    return fut

  sqe.opcode = IORING_OP_FSYNC
  sqe.fd = fd
  if dataOnly:
    sqe.opFlags = IORING_FSYNC_DATASYNC

  var comp = Completion(future: fut, kind: ckFsync)
  return queueSqe(u, comp)

proc uringClose*(u: UringFileIO, fd: cint): Future[int32] =
  ## Submit CLOSE operation. Returns Future with 0 on success or negative errno.
  let fut = newFuture[int32]("uringClose")

  if u.closed:
    fut.fail(
      if u.error != nil:
        u.error
      else:
        newException(IOError, "UringFileIO closed")
    )
    return fut

  let sqe = getSqe(u.ring)
  if sqe == nil:
    if u.chainActive:
      u.chainFailed = true
      u.chainFutures.add(fut)
      return fut
    fut.fail(newException(IOError, "io_uring SQ full"))
    return fut

  sqe.opcode = IORING_OP_CLOSE
  sqe.fd = fd

  var comp = Completion(future: fut, kind: ckClose)
  return queueSqe(u, comp)

proc uringStatx*(
    u: UringFileIO, path: string, flags: cint, mask: uint32, statxBuf: ref Statx
): Future[int32] =
  ## Submit STATX operation. Returns Future with 0 on success or negative errno.
  ## Results are written to statxBuf.
  let fut = newFuture[int32]("uringStatx")

  if u.closed:
    fut.fail(
      if u.error != nil:
        u.error
      else:
        newException(IOError, "UringFileIO closed")
    )
    return fut

  let sqe = getSqe(u.ring)
  if sqe == nil:
    if u.chainActive:
      u.chainFailed = true
      u.chainFutures.add(fut)
      return fut
    fut.fail(newException(IOError, "io_uring SQ full"))
    return fut

  var pathRef: ref string
  new(pathRef)
  pathRef[] = path

  sqe.opcode = IORING_OP_STATX
  sqe.fd = AT_FDCWD
  sqe.`addr` = cast[uint64](pathRef[].cstring)
  sqe.len = mask
  sqe.off = cast[uint64](addr statxBuf[])
  sqe.opFlags = cast[uint32](flags)

  var comp = Completion(future: fut, kind: ckStatx, strRef: pathRef, statxRef: statxBuf)
  return queueSqe(u, comp)

proc uringStatxFd*(
    u: UringFileIO, fd: cint, mask: uint32, statxBuf: ref Statx
): Future[int32] =
  ## Submit STATX on an open fd (AT_EMPTY_PATH). Returns Future with 0 on success
  ## or negative errno. Results are written to statxBuf.
  let fut = newFuture[int32]("uringStatxFd")

  if u.closed:
    fut.fail(
      if u.error != nil:
        u.error
      else:
        newException(IOError, "UringFileIO closed")
    )
    return fut

  let sqe = getSqe(u.ring)
  if sqe == nil:
    if u.chainActive:
      u.chainFailed = true
      u.chainFutures.add(fut)
      return fut
    fut.fail(newException(IOError, "io_uring SQ full"))
    return fut

  # AT_EMPTY_PATH with empty path string performs statx on the fd itself.
  var pathRef: ref string
  new(pathRef)
  pathRef[] = ""

  sqe.opcode = IORING_OP_STATX
  sqe.fd = fd
  sqe.`addr` = cast[uint64](pathRef[].cstring)
  sqe.len = mask
  sqe.off = cast[uint64](addr statxBuf[])
  sqe.opFlags = cast[uint32](AT_EMPTY_PATH)

  var comp = Completion(future: fut, kind: ckStatx, strRef: pathRef, statxRef: statxBuf)
  return queueSqe(u, comp)

proc uringRenameat*(
    u: UringFileIO, oldPath: string, newPath: string, flags: uint32 = 0
): Future[int32] =
  ## Submit RENAMEAT operation. Returns Future with 0 on success or negative errno.
  let fut = newFuture[int32]("uringRenameat")

  if u.closed:
    fut.fail(
      if u.error != nil:
        u.error
      else:
        newException(IOError, "UringFileIO closed")
    )
    return fut

  let sqe = getSqe(u.ring)
  if sqe == nil:
    if u.chainActive:
      u.chainFailed = true
      u.chainFutures.add(fut)
      return fut
    fut.fail(newException(IOError, "io_uring SQ full"))
    return fut

  var oldPathRef: ref string
  new(oldPathRef)
  oldPathRef[] = oldPath
  var newPathRef: ref string
  new(newPathRef)
  newPathRef[] = newPath

  sqe.opcode = IORING_OP_RENAMEAT
  sqe.fd = AT_FDCWD
  sqe.`addr` = cast[uint64](oldPathRef[].cstring)
  sqe.len = cast[uint32](AT_FDCWD)
  sqe.off = cast[uint64](newPathRef[].cstring)
  sqe.opFlags = flags

  var comp =
    Completion(future: fut, kind: ckRename, strRef: oldPathRef, strRef2: newPathRef)
  return queueSqe(u, comp)

proc uringCancel*(u: UringFileIO, target: Future[int32]): Future[int32] =
  ## Submit ASYNC_CANCEL for a pending operation. Returns Future with 0 on success
  ## or negative errno. The cancelled target Future will complete with -ECANCELED
  ## (-125) — unless it was already settled by external (chronos) cancellation, in
  ## which case it stays in its Cancelled state and this call leaves it untouched
  ## (awaiting it raises CancelledError rather than yielding -125).
  let fut = newFuture[int32]("uringCancel")

  if u.closed:
    fut.fail(
      if u.error != nil:
        u.error
      else:
        newException(IOError, "UringFileIO closed")
    )
    return fut

  let targetPtr = cast[pointer](target)
  if targetPtr notin u.futureToId:
    fut.fail(newException(IOError, "target operation not found"))
    return fut

  let targetId = u.futureToId[targetPtr]

  # Check if target is still unsubmitted — cancel locally without kernel roundtrip.
  # Safe during chain construction: dropUnsubmitted neutralizes the SQE in place
  # (NOP) without queueing a new one, so the chain is not split.
  if u.dropUnsubmitted(targetId):
    # Guard against a target already finished by external cancellation.
    settleIfPending(target, -125'i32)
    fut.complete(0'i32)
    return fut

  # Target is already submitted — a kernel ASYNC_CANCEL SQE is required. We cannot
  # queue one mid-chain: IOSQE_IO_LINK links each SQE to the one physically
  # following it, so the cancel would graft into the open chain — splitting it
  # (the next user op gets cancelled — data loss) and making endChain return N+1
  # futures. Reject rather than corrupt the chain; cancel after endChain, or rely
  # on external (chronos) cancellation, which defers via handleExternalCancel.
  if u.chainActive:
    fut.fail(
      newException(
        IOError, "cannot cancel a submitted operation during chain construction"
      )
    )
    return fut

  # Target is already submitted — ask the kernel to cancel it
  if not u.queueAsyncCancel(targetId, fut):
    fut.fail(newException(IOError, "io_uring SQ full"))
  return fut

proc beginChain*(u: UringFileIO) =
  ## Start a linked SQE chain. Subsequent uring* calls will have IOSQE_IO_LINK set.
  ## Call endChain to finalize the chain.
  if u.closed:
    raise newException(IOError, "UringFileIO closed")
  if u.chainActive:
    raise newException(IOError, "chain already active")
  u.chainActive = true
  u.chainFailed = false
  u.chainIds.setLen(0)
  u.chainFutures.setLen(0)

proc endChain*(u: UringFileIO): seq[Future[int32]] =
  ## Finalize a linked SQE chain. Returns the futures for all operations in the chain.
  ## If any getSqe failed during the chain, all SQEs are rolled back and all futures fail.
  if not u.chainActive:
    raise newException(IOError, "no active chain")
  u.chainActive = false

  # Note: do NOT early-return for an "empty" chain (chainFutures and/or chainIds
  # empty). The common tail below — clearLastSqeFlags when chainIds is non-empty,
  # the flushScheduled reset/re-arm, and drainDeferredCancels — must run in every
  # case:
  #   * A chain whose sole member was cancelled (handleExternalCancel ->
  #     dropUnsubmitted) empties chainFutures but keeps the member's id in
  #     chainIds (its SQE is now a NOP, possibly still carrying IOSQE_IO_LINK):
  #     it needs clearLastSqeFlags or the dangling link cancels the next unrelated
  #     op. It also needs the unconditional `flushScheduled = false` reset below:
  #     the chain's scheduled flush may have already fired (returning early while
  #     chainActive), leaving flushScheduled stuck true with no flush queued, which
  #     would wedge every later submission. (The NOP's own SQE slot is then left
  #     unpublished, but the next queued op's flush — submit() now runs even with an
  #     empty `unsubmitted` — reclaims it; processCqes ignores the stale NOP.)
  #   * A genuinely empty chain (no ops queued) can still have unsubmitted ops
  #     queued *before* beginChain whose scheduled flush already fired and
  #     early-returned while chainActive: it needs the conditional flush re-arm
  #     (`unsubmitted.len > 0`) or those ops are stranded and every later
  #     submission wedges.
  # The branches below guard their SQE work on `chainIds.len > 0`, so an empty
  # chain falls through harmlessly and returns @[].

  if u.chainFailed:
    # Roll back all successfully allocated SQEs
    if u.chainIds.len > 0:
      rollbackSqes(u.ring, uint32(u.chainIds.len))
      # Remove from pending/futureToId/unsubmitted
      for id in u.chainIds:
        var comp: Completion
        if u.pending.pop(id, comp):
          u.futureToId.del(cast[pointer](comp.future))
        discard u.removeUnsubmitted(id)
    # Fail all futures
    let err = newException(IOError, "chain aborted: io_uring SQ full")
    for fut in u.chainFutures:
      if not fut.finished:
        fut.fail(err)
    result = u.chainFutures
  else:
    # Clear IOSQE_IO_LINK from the last SQE (it must not link to the next unrelated SQE)
    if u.chainIds.len > 0:
      clearLastSqeFlags(u.ring, IOSQE_IO_LINK)
    result = u.chainFutures

  u.chainIds.setLen(0)
  u.chainFutures.setLen(0)

  # Allow pending flush to proceed
  if u.flushScheduled:
    u.flushScheduled = false
    if u.unsubmitted.len > 0:
      u.flushScheduled = true
      scheduleSoon(
        proc() {.raises: [].} =
          u.flush()
      )

  # The chain is finalized, so kernel cancels deferred while it was open can now
  # be issued without grafting into it. (Any uringCancel queued here is picked up
  # by the flush scheduled above, or schedules its own.)
  u.drainDeferredCancels()

proc newUringFileIO*(entries: uint32 = 256): UringFileIO {.raises: [OSError].} =
  ## Create a new UringFileIO instance. Initializes io_uring and starts poll loop.
  var ring = setupRing(entries)

  let efd = eventfd(0, EFD_NONBLOCK or EFD_CLOEXEC)
  if efd < 0:
    closeRing(ring)
    raiseOSError(osLastError(), "eventfd creation failed")

  try:
    registerEventfd(ring, efd)
  except OSError as e:
    discard close(efd)
    closeRing(ring)
    raise e

  result = UringFileIO(
    ring: ring,
    eventFd: efd,
    nextId: 1,
    pending: initTable[uint64, Completion](),
    closed: false,
  )
  when hasChronos:
    # One shared cancel callback per instance (assigned to every op's future),
    # so arming an op allocates nothing. It recovers `u` from this closure and
    # the cancelled future from its own argument (chronos passes the future as a
    # raw pointer).
    let u = result
    u.cancelCb = proc(arg: pointer) {.gcsafe, raises: [].} =
      handleExternalCancel(u, cast[Future[int32]](arg))
  try:
    startPollLoop(result)
  except OSError as e:
    unregisterEventfd(ring)
    discard close(efd)
    closeRing(ring)
    raise e

# Fixed buffers

proc registerFixedBuffers*(
    u: UringFileIO, sizes: openArray[int]
) {.raises: [IOError].} =
  ## Register fixed buffers with the kernel. `sizes` specifies the size of each buffer.
  ## Buffers are allocated and managed by UringFileIO.
  if u.closed:
    raise newException(IOError, "UringFileIO closed")
  if u.fixedBufsRegistered:
    raise newException(IOError, "fixed buffers already registered")
  if sizes.len == 0:
    raise newException(IOError, "sizes must not be empty")
  for s in sizes:
    if s <= 0:
      raise newException(IOError, "buffer size must be positive")

  u.fixedBufs = newSeq[seq[byte]](sizes.len)
  var iovecs = newSeq[IOVec](sizes.len)
  for i in 0 ..< sizes.len:
    u.fixedBufs[i] = newSeq[byte](sizes[i])
    iovecs[i].iov_base = addr u.fixedBufs[i][0]
    iovecs[i].iov_len = csize_t(sizes[i])

  try:
    registerBuffers(u.ring, addr iovecs[0], cuint(sizes.len))
  except OSError as e:
    u.fixedBufs.setLen(0)
    raise newException(IOError, "registerFixedBuffers failed: " & e.msg)

  u.fixedBufsRegistered = true

proc unregisterFixedBuffers*(u: UringFileIO) =
  ## Unregister fixed buffers from the kernel and free them.
  if not u.fixedBufsRegistered:
    return
  unregisterBuffers(u.ring)
  u.fixedBufsRegistered = false
  u.fixedBufs.setLen(0)

proc fixedBufferCount*(u: UringFileIO): int =
  ## Return the number of registered fixed buffers.
  u.fixedBufs.len

proc fixedBufferAddr*(u: UringFileIO, index: int): pointer =
  ## Return the address of a registered fixed buffer.
  addr u.fixedBufs[index][0]

proc fixedBufferSize*(u: UringFileIO, index: int): int =
  ## Return the size of a registered fixed buffer.
  u.fixedBufs[index].len

proc uringReadFixed*(
    u: UringFileIO,
    fd: cint,
    buf: pointer,
    size: uint32,
    offset: uint64,
    bufIndex: uint16,
): Future[int32] =
  ## Submit READ_FIXED operation using a pre-registered fixed buffer.
  ## `buf` must point into the registered buffer at `bufIndex`.
  ## No bufRef needed — UringFileIO owns the buffer.
  let fut = newFuture[int32]("uringReadFixed")

  if u.closed:
    fut.fail(
      if u.error != nil:
        u.error
      else:
        newException(IOError, "UringFileIO closed")
    )
    return fut

  let sqe = getSqe(u.ring)
  if sqe == nil:
    if u.chainActive:
      u.chainFailed = true
      u.chainFutures.add(fut)
      return fut
    fut.fail(newException(IOError, "io_uring SQ full"))
    return fut

  sqe.opcode = IORING_OP_READ_FIXED
  sqe.fd = fd
  sqe.`addr` = cast[uint64](buf)
  sqe.len = size
  sqe.off = offset
  sqe.bufInfo = bufIndex

  var comp = Completion(future: fut, kind: ckRead)
  return queueSqe(u, comp)

proc uringWriteFixed*(
    u: UringFileIO,
    fd: cint,
    buf: pointer,
    size: uint32,
    offset: uint64,
    bufIndex: uint16,
): Future[int32] =
  ## Submit WRITE_FIXED operation using a pre-registered fixed buffer.
  ## `buf` must point into the registered buffer at `bufIndex`.
  ## No bufRef needed — UringFileIO owns the buffer.
  let fut = newFuture[int32]("uringWriteFixed")

  if u.closed:
    fut.fail(
      if u.error != nil:
        u.error
      else:
        newException(IOError, "UringFileIO closed")
    )
    return fut

  let sqe = getSqe(u.ring)
  if sqe == nil:
    if u.chainActive:
      u.chainFailed = true
      u.chainFutures.add(fut)
      return fut
    fut.fail(newException(IOError, "io_uring SQ full"))
    return fut

  sqe.opcode = IORING_OP_WRITE_FIXED
  sqe.fd = fd
  sqe.`addr` = cast[uint64](buf)
  sqe.len = size
  sqe.off = offset
  sqe.bufInfo = bufIndex

  var comp = Completion(future: fut, kind: ckWrite)
  return queueSqe(u, comp)

# Fixed files

proc registerFixedFiles*(u: UringFileIO, fds: openArray[cint]) {.raises: [IOError].} =
  ## Register fixed files with the kernel. `fds` are file descriptors opened by the caller.
  ## The caller retains ownership of the fds (open/close responsibility).
  if u.closed:
    raise newException(IOError, "UringFileIO closed")
  if u.fixedFilesRegistered:
    raise newException(IOError, "fixed files already registered")
  if fds.len == 0:
    raise newException(IOError, "fds must not be empty")

  u.fixedFiles = @fds # Copy
  try:
    registerFiles(u.ring, addr u.fixedFiles[0], cuint(fds.len))
  except OSError as e:
    u.fixedFiles.setLen(0)
    raise newException(IOError, "registerFixedFiles failed: " & e.msg)

  u.fixedFilesRegistered = true

proc unregisterFixedFiles*(u: UringFileIO) =
  ## Unregister fixed files from the kernel. Does not close the fds.
  if not u.fixedFilesRegistered:
    return
  unregisterFiles(u.ring)
  u.fixedFilesRegistered = false
  u.fixedFiles.setLen(0)
  u.fixedFileSlotsFree.setLen(0)

proc fixedFileCount*(u: UringFileIO): int =
  ## Return the number of registered fixed files.
  u.fixedFiles.len

proc uringReadFixedFile*(
    u: UringFileIO,
    fileIndex: cint,
    buf: pointer,
    size: uint32,
    offset: uint64,
    bufRef: ref seq[byte],
): Future[int32] =
  ## Submit READ operation using a fixed file index.
  ## `fileIndex` is the index into the registered file table (not a real fd).
  ## bufRef keeps the buffer GC-rooted until completion.
  let fut = newFuture[int32]("uringReadFixedFile")

  if u.closed:
    fut.fail(
      if u.error != nil:
        u.error
      else:
        newException(IOError, "UringFileIO closed")
    )
    return fut

  let sqe = getSqe(u.ring)
  if sqe == nil:
    if u.chainActive:
      u.chainFailed = true
      u.chainFutures.add(fut)
      return fut
    fut.fail(newException(IOError, "io_uring SQ full"))
    return fut

  sqe.opcode = IORING_OP_READ
  sqe.flags = IOSQE_FIXED_FILE
  sqe.fd = fileIndex
  sqe.`addr` = cast[uint64](buf)
  sqe.len = size
  sqe.off = offset

  var comp = Completion(future: fut, kind: ckRead, bufRef: bufRef)
  return queueSqe(u, comp)

proc uringWriteFixedFile*(
    u: UringFileIO,
    fileIndex: cint,
    buf: pointer,
    size: uint32,
    offset: uint64,
    bufRef: ref seq[byte],
): Future[int32] =
  ## Submit WRITE operation using a fixed file index.
  ## `fileIndex` is the index into the registered file table (not a real fd).
  ## bufRef keeps the buffer GC-rooted until completion.
  let fut = newFuture[int32]("uringWriteFixedFile")

  if u.closed:
    fut.fail(
      if u.error != nil:
        u.error
      else:
        newException(IOError, "UringFileIO closed")
    )
    return fut

  let sqe = getSqe(u.ring)
  if sqe == nil:
    if u.chainActive:
      u.chainFailed = true
      u.chainFutures.add(fut)
      return fut
    fut.fail(newException(IOError, "io_uring SQ full"))
    return fut

  sqe.opcode = IORING_OP_WRITE
  sqe.flags = IOSQE_FIXED_FILE
  sqe.fd = fileIndex
  sqe.`addr` = cast[uint64](buf)
  sqe.len = size
  sqe.off = offset

  var comp = Completion(future: fut, kind: ckWrite, bufRef: bufRef)
  return queueSqe(u, comp)

proc uringFsyncFixedFile*(
    u: UringFileIO, fileIndex: cint, dataOnly: bool = false
): Future[int32] =
  ## Submit FSYNC operation using a fixed file index.
  ## `fileIndex` is the index into the registered file table (not a real fd).
  ## If dataOnly is true, only file data is flushed (fdatasync semantics) by
  ## setting the IORING_FSYNC_DATASYNC flag, leaving non-essential metadata
  ## (e.g. mtime) unsynced.
  let fut = newFuture[int32]("uringFsyncFixedFile")

  if u.closed:
    fut.fail(
      if u.error != nil:
        u.error
      else:
        newException(IOError, "UringFileIO closed")
    )
    return fut

  let sqe = getSqe(u.ring)
  if sqe == nil:
    if u.chainActive:
      u.chainFailed = true
      u.chainFutures.add(fut)
      return fut
    fut.fail(newException(IOError, "io_uring SQ full"))
    return fut

  sqe.opcode = IORING_OP_FSYNC
  sqe.flags = IOSQE_FIXED_FILE
  sqe.fd = fileIndex
  if dataOnly:
    sqe.opFlags = IORING_FSYNC_DATASYNC

  var comp = Completion(future: fut, kind: ckFsync)
  return queueSqe(u, comp)

# Fixed file slots (for direct descriptors)

proc registerFixedFileSlots*(u: UringFileIO, count: int) {.raises: [IOError].} =
  ## Register `count` empty fixed file slots (fd=-1) for use with direct descriptors.
  ## Mutually exclusive with `registerFixedFiles` (io_uring supports one file table).
  if u.closed:
    raise newException(IOError, "UringFileIO closed")
  if u.fixedFilesRegistered:
    raise newException(IOError, "fixed files already registered")
  if count <= 0:
    raise newException(IOError, "count must be positive")

  u.fixedFiles = newSeq[cint](count)
  for i in 0 ..< count:
    u.fixedFiles[i] = -1

  try:
    registerFiles(u.ring, addr u.fixedFiles[0], cuint(count))
  except OSError as e:
    u.fixedFiles.setLen(0)
    raise newException(IOError, "registerFixedFileSlots failed: " & e.msg)

  u.fixedFilesRegistered = true
  u.fixedFileSlotsFree = newSeq[int32](count)
  for i in 0 ..< count:
    u.fixedFileSlotsFree[i] = int32(i)

proc allocFixedFileSlot*(u: UringFileIO): int32 {.raises: [IOError].} =
  ## Pop a free fixed file slot index. Raises IOError if none available.
  if u.fixedFileSlotsFree.len == 0:
    raise newException(IOError, "no free fixed file slots")
  result = u.fixedFileSlotsFree.pop()

proc freeFixedFileSlot*(u: UringFileIO, slot: int32) =
  ## Return a fixed file slot to the free pool.
  u.fixedFileSlotsFree.add(slot)

proc fixedFileSlotsAvailable*(u: UringFileIO): int =
  ## Return the number of free fixed file slots.
  u.fixedFileSlotsFree.len

# Direct descriptor operations

proc uringOpenDirect*(
    u: UringFileIO, path: string, flags: cint, mode: uint32, fileSlot: int32
): Future[int32] =
  ## Submit OPENAT operation that installs the fd directly into a fixed file slot.
  ## `fileSlot` is the 0-indexed slot from `allocFixedFileSlot`.
  ## CQE result: 0 on success (not an fd), negative errno on failure.
  let fut = newFuture[int32]("uringOpenDirect")

  if u.closed:
    fut.fail(
      if u.error != nil:
        u.error
      else:
        newException(IOError, "UringFileIO closed")
    )
    return fut

  let sqe = getSqe(u.ring)
  if sqe == nil:
    if u.chainActive:
      u.chainFailed = true
      u.chainFutures.add(fut)
      return fut
    fut.fail(newException(IOError, "io_uring SQ full"))
    return fut

  var pathRef: ref string
  new(pathRef)
  pathRef[] = path

  sqe.opcode = IORING_OP_OPENAT
  sqe.fd = AT_FDCWD
  sqe.`addr` = cast[uint64](pathRef[].cstring)
  sqe.len = uint32(mode)
  sqe.opFlags = cast[uint32](flags)
  sqe.spliceFdIn = fileSlot + 1 # 1-indexed: 0 means "normal fd"

  var comp = Completion(future: fut, kind: ckOpen, strRef: pathRef)
  return queueSqe(u, comp)

proc uringCloseDirect*(u: UringFileIO, fileSlot: int32): Future[int32] =
  ## Submit CLOSE operation on a direct descriptor slot.
  ## Closes the fd inside the slot and resets the slot to -1.
  ## CQE result: 0 on success, negative errno on failure.
  let fut = newFuture[int32]("uringCloseDirect")

  if u.closed:
    fut.fail(
      if u.error != nil:
        u.error
      else:
        newException(IOError, "UringFileIO closed")
    )
    return fut

  let sqe = getSqe(u.ring)
  if sqe == nil:
    if u.chainActive:
      u.chainFailed = true
      u.chainFutures.add(fut)
      return fut
    fut.fail(newException(IOError, "io_uring SQ full"))
    return fut

  sqe.opcode = IORING_OP_CLOSE
  sqe.fd = 0 # unused for direct close
  sqe.spliceFdIn = fileSlot + 1 # 1-indexed

  var comp = Completion(future: fut, kind: ckClose)
  return queueSqe(u, comp)
