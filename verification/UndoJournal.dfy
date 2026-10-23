// Verification-only byte model; this file is not compiled into the library.
// Correspondence: StreamPageStore.WriteAsync/SetLengthAsync call
// PersistentRollbackJournal.RecordBeforeWrite/RecordBeforeTruncate first;
// those methods save original pages and flush their records before mutation.
// Page alignment is abstracted to byte intervals. Journal parsing, checksums,
// path identity, locks, namespace persistence and C# refinement are not proved.
module UndoJournal {
  type Byte = n: int | 0 <= n < 256 witness 0

  function Min(a: nat, b: nat): nat { if a < b then a else b }

  predicate ValidUndo(original: seq<Byte>, saved: map<nat, Byte>) {
    forall i: nat :: i in saved ==> i < |original| && saved[i] == original[i]
  }

  // Every original byte that is missing or changed has a durable exact copy.
  // Uncaptured original bytes must still exist, unchanged, in the database.
  predicate Recoverable(original: seq<Byte>, current: seq<Byte>, saved: map<nat, Byte>) {
    ValidUndo(original, saved) &&
    forall i: nat :: i < |original| && i !in saved ==>
      i < |current| && current[i] == original[i]
  }

  // Capture denotes completed durable publication of exact original bytes.
  // Its storage and manifest-validation premises are a refinement obligation.
  function Capture(original: seq<Byte>, saved: map<nat, Byte>, lo: nat, hi: nat): map<nat, Byte>
    requires ValidUndo(original, saved)
  {
    map i: nat | i < |original| && (i in saved || lo <= i < hi) :: original[i]
  }

  lemma CaptureIsExact(original: seq<Byte>, saved: map<nat, Byte>, lo: nat, hi: nat)
    requires ValidUndo(original, saved)
    ensures ValidUndo(original, Capture(original, saved, lo, hi))
    ensures forall i: nat :: i in saved ==> i in Capture(original, saved, lo, hi)
    ensures forall i: nat :: lo <= i < hi && i < |original| ==>
      i in Capture(original, saved, lo, hi)
  { }

  lemma InitialImageIsRecoverable(original: seq<Byte>)
    ensures Recoverable(original, original, map[])
  { }

  // A write may tear at arbitrary byte positions, including repeated writes.
  // Recovery needs no assumption that a page write is atomic: all changes in
  // the protected interval are allowed here. Payload validation is below.
  lemma ProtectedWritePreservesRecoverability(
      original: seq<Byte>, current: seq<Byte>, saved: map<nat, Byte>,
      lo: nat, hi: nat, torn: seq<Byte>)
    requires Recoverable(original, current, saved)
    requires |current| <= |torn|
    requires forall i: nat :: i < |current| && !(lo <= i < hi) ==> torn[i] == current[i]
    ensures Recoverable(original, torn, Capture(original, saved, lo, hi))
  {
    CaptureIsExact(original, saved, lo, hi);
    forall i: nat | i < |original| && i !in Capture(original, saved, lo, hi)
      ensures i < |torn| && torn[i] == original[i]
    {
      assert i !in saved;
      assert !(lo <= i < hi);
    }
  }

  // Capturing the complete removed original tail, not merely touched pages,
  // makes truncation safe even when it removes previously untouched bytes.
  lemma ProtectedTruncationPreservesRecoverability(
      original: seq<Byte>, current: seq<Byte>, saved: map<nat, Byte>, length: nat)
    requires Recoverable(original, current, saved)
    requires length <= |current|
    ensures Recoverable(original, current[..length], Capture(original, saved, length, |original|))
  {
    CaptureIsExact(original, saved, length, |original|);
    forall i: nat | i < |original| && i !in Capture(original, saved, length, |original|)
      ensures i < length && current[..length][i] == original[i]
    {
      assert i !in saved;
      assert i < length;
    }
  }

  // Zero filling during replay can extend a truncated file. Every original
  // byte in that gap must already have a before-image. Existing unsaved
  // original bytes are preserved. A torn restore may leave any subset
  // unrestored. Final SetLength may remove transaction-appended bytes, so
  // replay preserves only the shorter of current and original lengths.
  predicate ReplayMutation(original: seq<Byte>, before: seq<Byte>, after: seq<Byte>, saved: map<nat, Byte>) {
    |after| >= Min(|before|, |original|) &&
    forall i: nat :: i < |before| && i < |original| && i !in saved ==> after[i] == before[i]
  }

  lemma InterruptedReplayPreservesRecoverability(
      original: seq<Byte>, before: seq<Byte>, after: seq<Byte>, saved: map<nat, Byte>)
    requires Recoverable(original, before, saved)
    requires ReplayMutation(original, before, after, saved)
    ensures Recoverable(original, after, saved)
  { }

  // SetLength(originalLength) may remove appended bytes after replay. It may
  // also extend with zeros: all missing original positions have saved bytes.
  function Resize(current: seq<Byte>, length: nat): seq<Byte> {
    seq(length, i => if 0 <= i < |current| then current[i] else 0)
  }

  lemma RestoreLengthIsReplayMutation(
      original: seq<Byte>, current: seq<Byte>, saved: map<nat, Byte>)
    ensures ReplayMutation(original, current, Resize(current, |original|), saved)
  { }

  lemma RestoreLengthPreservesRecoverability(
      original: seq<Byte>, current: seq<Byte>, saved: map<nat, Byte>)
    requires Recoverable(original, current, saved)
    ensures Recoverable(original, Resize(current, |original|), saved)
  {
    RestoreLengthIsReplayMutation(original, current, saved);
    InterruptedReplayPreservesRecoverability(original, current, Resize(current, |original|), saved);
  }

  function Restore(original: seq<Byte>, current: seq<Byte>, saved: map<nat, Byte>): seq<Byte>
    requires Recoverable(original, current, saved)
  {
    seq(|original|, i => if i in saved then saved[i] else if 0 <= i < |current| then current[i] else 0)
  }

  lemma CompleteReplayRestoresExactOriginal(
      original: seq<Byte>, current: seq<Byte>, saved: map<nat, Byte>)
    requires Recoverable(original, current, saved)
    ensures Restore(original, current, saved) == original
  {
    assert forall i: nat :: i < |original| ==> Restore(original, current, saved)[i] == original[i];
  }

  // A finite but unbounded number of interruptions does not consume undo
  // evidence. Progress still requires an eventually uninterrupted replay.
  lemma RepeatedReplayPreservesRecoverability(
      original: seq<Byte>, images: seq<seq<Byte>>, saved: map<nat, Byte>)
    requires 0 < |images|
    requires Recoverable(original, images[0], saved)
    requires forall j: nat :: j + 1 < |images| ==>
      ReplayMutation(original, images[j], images[j + 1], saved)
    ensures Recoverable(original, images[|images| - 1], saved)
    ensures Restore(original, images[|images| - 1], saved) == original
    decreases |images|
  {
    if |images| > 1 {
      InterruptedReplayPreservesRecoverability(original, images[0], images[1], saved);
      RepeatedReplayPreservesRecoverability(original, images[1..], saved);
    }
    CompleteReplayRestoresExactOriginal(original, images[|images| - 1], saved);
  }

  // An explicit two-state trace covers interruption after the real final
  // SetLength removes appended transaction bytes, as well as byte replay.
  lemma FinalAppendRemovalIsCoveredByReplayTrace(
      original: seq<Byte>, current: seq<Byte>, saved: map<nat, Byte>)
    requires Recoverable(original, current, saved)
    requires |original| < |current|
    ensures |Resize(current, |original|)| < |current|
    ensures Recoverable(original, Resize(current, |original|), saved)
    ensures Restore(original, Resize(current, |original|), saved) == original
  {
    RestoreLengthIsReplayMutation(original, current, saved);
    RepeatedReplayPreservesRecoverability(original, [current, Resize(current, |original|)], saved);
  }

  // Models ValidateCurrentContent at byte granularity. An intent authorizes
  // one or more values at each offset; checksummed records and manifest
  // validation must establish this abstract map before these lemmas apply.
  predicate Allowed(original: seq<Byte>, intents: map<nat, set<Byte>>,
      minimumLength: nat, recovering: bool, offset: nat, value: Byte) {
    (offset < |original| && value == original[offset]) ||
    (offset >= |original| && value == 0) ||
    (offset in intents && value in intents[offset]) ||
    (recovering && offset >= minimumLength && value == 0)
  }

  predicate Authorized(original: seq<Byte>, current: seq<Byte>, intents: map<nat, set<Byte>>,
      minimumLength: nat, recovering: bool) {
    forall i: nat :: i < |current| ==> Allowed(original, intents, minimumLength, recovering, i, current[i])
  }

  lemma TornMixtureRemainsAuthorized(
      original: seq<Byte>, oldImage: seq<Byte>, intended: seq<Byte>, torn: seq<Byte>,
      intents: map<nat, set<Byte>>, minimumLength: nat, recovering: bool)
    requires |oldImage| == |intended| == |torn|
    requires Authorized(original, oldImage, intents, minimumLength, recovering)
    requires Authorized(original, intended, intents, minimumLength, recovering)
    requires forall i: nat :: i < |torn| ==> torn[i] == oldImage[i] || torn[i] == intended[i]
    ensures Authorized(original, torn, intents, minimumLength, recovering)
  { }

  lemma ReplayMixtureRemainsAuthorized(
      original: seq<Byte>, before: seq<Byte>, after: seq<Byte>,
      intents: map<nat, set<Byte>>, minimumLength: nat, recovering: bool)
    requires Authorized(original, before, intents, minimumLength, recovering)
    requires forall i: nat :: i < |after| ==>
      (i < |before| && after[i] == before[i]) ||
      (i < |original| && after[i] == original[i]) ||
      (recovering && i >= minimumLength && after[i] == 0)
    ensures Authorized(original, after, intents, minimumLength, recovering)
  { }

  // Combines the undo invariant with content validation, including zero-filled
  // gaps created by partial replay. Capturing every byte of the removed tail
  // prevents the recovery marker from masking an uncaptured original byte.
  lemma InterruptedReplayPreservesBothInvariants(
      original: seq<Byte>, before: seq<Byte>, after: seq<Byte>, saved: map<nat, Byte>,
      intents: map<nat, set<Byte>>, minimumLength: nat, recovering: bool)
    requires Recoverable(original, before, saved)
    requires Authorized(original, before, intents, minimumLength, recovering)
    requires forall i: nat :: minimumLength <= i < |original| ==> i in saved
    requires Min(|before|, |original|) <= |after|
    requires forall i: nat :: i < |after| ==>
      (i < |before| && after[i] == before[i]) ||
      (i < |original| && after[i] == original[i]) ||
      (recovering && i >= minimumLength && after[i] == 0)
    ensures Recoverable(original, after, saved)
    ensures Authorized(original, after, intents, minimumLength, recovering)
  {
    forall i: nat | i < |before| && i < |original| && i !in saved
      ensures after[i] == before[i]
    {
      if i < |original| {
        assert i < minimumLength;
      }
    }
    assert ReplayMutation(original, before, after, saved);
    InterruptedReplayPreservesRecoverability(original, before, after, saved);
    ReplayMixtureRemainsAuthorized(original, before, after, intents, minimumLength, recovering);
  }

  lemma RecoveryMarkerPermitsZeroFilledReextension(
      original: seq<Byte>, before: seq<Byte>, saved: map<nat, Byte>,
      intents: map<nat, set<Byte>>, minimumLength: nat)
    requires Recoverable(original, before, saved)
    requires Authorized(original, before, intents, minimumLength, false)
    requires minimumLength <= |before| <= |original|
    ensures Recoverable(original, Resize(before, |original|), saved)
    ensures Authorized(original, Resize(before, |original|), intents, minimumLength, true)
  {
    RestoreLengthPreservesRecoverability(original, before, saved);
  }

  // These are proved counterexamples, rather than axioms about a desirable
  // implementation: dropping before-image coverage really loses the invariant.
  lemma MissingWriteCoverageIsUnsafe()
    ensures Recoverable([1], [1], map[])
    ensures !Recoverable([1], [2], map[])
  {
    var original: seq<Byte> := [1];
    var current: seq<Byte> := [2];
    var saved: map<nat, Byte> := map[];
    if Recoverable(original, current, saved) {
      assert current[0] == original[0];
    }
  }

  lemma MissingTruncatedTailCoverageIsUnsafe()
    ensures Recoverable([1, 2], [1, 2], map[])
    ensures !Recoverable([1, 2], [1], map[])
  {
    var original: seq<Byte> := [1, 2];
    var current: seq<Byte> := [1];
    var saved: map<nat, Byte> := map[];
    if Recoverable(original, current, saved) {
      assert 1 < |original| && 1 !in saved ==> 1 < |current| && current[1] == original[1];
    }
  }
  // RecordRecoveryStart is flushed before replay may re-extend a removed
  // tail. Without it, an original nonzero tail byte with no zero-valued intent
  // is refused. The marker adds authorization only at/after the minimum tail.
  lemma ZeroExtensionRequiresRecoveryAuthorization(
      original: seq<Byte>, intents: map<nat, set<Byte>>, minimumLength: nat, offset: nat)
    requires minimumLength <= offset < |original|
    requires original[offset] != 0
    requires offset !in intents || 0 !in intents[offset]
    ensures !Allowed(original, intents, minimumLength, false, offset, 0)
    ensures Allowed(original, intents, minimumLength, true, offset, 0)
  { }

  lemma RecoveryMarkerDoesNotAuthorizeRetainedPrefix(
      original: seq<Byte>, intents: map<nat, set<Byte>>, minimumLength: nat, offset: nat, value: Byte)
    requires offset < minimumLength
    ensures Allowed(original, intents, minimumLength, true, offset, value) ==
      Allowed(original, intents, minimumLength, false, offset, value)
  { }

  // Durable decisions are modeled as complete atomic logical records. The
  // parser must map incomplete records to None and refuse invalid complete
  // records; this file does not prove that parser or hash collision bounds.
  datatype Decision = None | Committed(image: seq<Byte>)
  datatype CommitState = State(current: seq<Byte>, durable: seq<Byte>, decision: Decision, success: bool, faulted: bool)

  predicate CommitInvariant(s: CommitState) {
    (s.decision.Committed? ==> s.durable == s.decision.image) &&
    (s.success ==> s.decision.Committed? && !s.faulted)
  }

  // PhysicalRollback.CommitAsync first flushes database bytes, marks the
  // owner faulted, then calls Complete. An uncertain decision flush permits
  // either durable outcome, but cannot return success or start rollback.
  predicate CommitStep(before: CommitState, after: CommitState) {
    (!before.success && !before.faulted && before.decision.None? &&
      after == State(before.current, before.current, None, false, false)) ||
    (!before.success && !before.faulted && before.decision.None? && before.current == before.durable &&
      (after == State(before.current, before.durable, Committed(before.current), true, false) ||
       after == State(before.current, before.durable, Committed(before.current), false, true) ||
       after == State(before.current, before.durable, None, false, true)))
  }

  lemma CommitOrderingPreservesInvariant(before: CommitState, after: CommitState)
    requires CommitInvariant(before)
    requires CommitStep(before, after)
    ensures CommitInvariant(after)
    ensures after.success ==> after.decision == Committed(after.current) && after.durable == after.current
    ensures !before.faulted
  { }

  function Selected(original: seq<Byte>, state: CommitState): seq<Byte> {
    if state.decision.Committed? then state.decision.image else original
  }

  lemma DurableDecisionSelectsFlushedImage(original: seq<Byte>, state: CommitState)
    requires CommitInvariant(state)
    ensures Selected(original, state) == original || Selected(original, state) == state.durable
    ensures state.success ==> Selected(original, state) == state.durable
  { }
}
