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

  // Composed logical-record protocol. The database image denotes the bytes
  // surviving at each cut; writes may tear at any byte and length changes may
  // stop at any requested intermediate length. The journal has complete records,
  // a durable complete-record prefix, and at most one incomplete trailing record.
  // Record encoding/classification is separate. The buffered refinement below
  // composes this protocol with delayed/reordered database effects; neither
  // layer is a physical filesystem or C# proof.
  datatype JournalRecord = BeforeImage(offset: nat, value: Byte)
    | WriteIntent(start: nat, payload: seq<Byte>) | LengthIntent(length: nat)
    | CommitRecord(image: seq<Byte>) | RecoveryRecord

  function Saved(records: seq<JournalRecord>): map<nat, Byte>
    decreases |records|
  {
    if |records| == 0 then map[]
    else if records[|records|-1].BeforeImage? then
      Saved(records[..|records|-1])[records[|records|-1].offset := records[|records|-1].value]
    else Saved(records[..|records|-1])
  }

  predicate ExactRecords(original: seq<Byte>, records: seq<JournalRecord>) {
    forall j | 0 <= j < |records| && records[j].BeforeImage? ::
      records[j].offset < |original| && records[j].value == original[records[j].offset]
  }

  predicate HasCommit(records: seq<JournalRecord>) {
    exists j | 0 <= j < |records| :: records[j].CommitRecord?
  }

  function RecordDecision(records: seq<JournalRecord>): Decision
    decreases |records|
  {
    if |records| == 0 then None
    else if records[|records|-1].CommitRecord? then Committed(records[|records|-1].image)
    else RecordDecision(records[..|records|-1])
  }

  predicate Covered(records: seq<JournalRecord>, originalLength: nat, lo: nat, hi: nat) {
    forall i: nat :: lo <= i < hi && i < originalLength ==> i in Saved(records)
  }

  lemma SavedRecordsAreExact(original: seq<Byte>, records: seq<JournalRecord>)
    requires ExactRecords(original, records)
    ensures ValidUndo(original, Saved(records))
    decreases |records|
  {
    if |records| > 0 {
      SavedRecordsAreExact(original, records[..|records|-1]);
    }
  }

  lemma SavedPrefixGrows(original: seq<Byte>, records: seq<JournalRecord>, count: nat)
    requires ExactRecords(original, records)
    requires count <= |records|
    ensures forall i: nat :: i in Saved(records[..count]) ==>
      i in Saved(records) && Saved(records)[i] == Saved(records[..count])[i]
    decreases |records| - count
  {
    if count < |records| {
      SavedPrefixGrows(original, records[..|records|-1], count);
      assert (records[..|records|-1])[..count] == records[..count];
      SavedRecordsAreExact(original, records);
      SavedRecordsAreExact(original, records[..count]);
      forall i: nat | i in Saved(records[..count])
        ensures i in Saved(records) && Saved(records)[i] == Saved(records[..count])[i]
      {
        assert i in Saved(records[..|records|-1]);
        if records[|records|-1].BeforeImage? && records[|records|-1].offset == i {
          assert Saved(records)[i] == original[i];
        }
      }
    } else {
      assert records[..count] == records;
    }
  }

  lemma MoreDurableRecordsPreserveRecoverability(original: seq<Byte>, db: seq<Byte>,
      records: seq<JournalRecord>, oldCount: nat, newCount: nat)
    requires ExactRecords(original, records)
    requires oldCount <= newCount <= |records|
    requires Recoverable(original, db, Saved(records[..oldCount]))
    ensures Recoverable(original, db, Saved(records[..newCount]))
  {
    SavedPrefixGrows(original, records[..newCount], oldCount);
    assert (records[..newCount])[..oldCount] == records[..oldCount];
    SavedRecordsAreExact(original, records[..newCount]);
    forall i: nat | i < |original| && i !in Saved(records[..newCount])
      ensures i < |db| && db[i] == original[i]
    {
      assert i !in Saved(records[..oldCount]);
    }
  }

  lemma RecordDecisionProperties(records: seq<JournalRecord>)
    ensures RecordDecision(records).Committed? == HasCommit(records)
    ensures RecordDecision(records).Committed? ==>
      exists j | 0 <= j < |records| :: records[j] == CommitRecord(RecordDecision(records).image)
    decreases |records|
  {
    if |records| > 0 {
      RecordDecisionProperties(records[..|records|-1]);
    }
  }

  lemma AppendedRecordProperties(original: seq<Byte>, records: seq<JournalRecord>, record: JournalRecord)
    ensures HasCommit(records + [record]) == (HasCommit(records) || record.CommitRecord?)
    ensures Saved(records + [record]) == (if record.BeforeImage? then
      Saved(records)[record.offset := record.value] else Saved(records))
    ensures (ExactRecords(original, records) &&
      (!record.BeforeImage? || (record.offset < |original| && record.value == original[record.offset]))) ==>
      ExactRecords(original, records + [record])
  {
    assert (records + [record])[..|records|] == records;
    if record.CommitRecord? {
      assert (records + [record])[|records|].CommitRecord?;
    }
    if HasCommit(records) {
      var j :| 0 <= j < |records| && records[j].CommitRecord?;
      assert (records + [record])[j].CommitRecord?;
    }
  }

  lemma PrefixRecordProperties(original: seq<Byte>, records: seq<JournalRecord>, count: nat)
    requires count <= |records|
    ensures ExactRecords(original, records) ==> ExactRecords(original, records[..count])
    ensures HasCommit(records[..count]) ==> HasCommit(records)
  { }
  datatype ProtocolMode = Active | Completing | Faulted | Inspecting | Marking | Replaying | Finished
  datatype ProtocolState = ProtocolState(
    db: seq<Byte>, records: seq<JournalRecord>, durableCount: nat,
    incompleteTail: bool, mode: ProtocolMode, dataFlushed: bool,
    acknowledged: bool, replaySized: bool, replayCursor: nat)
  datatype ProtocolAction = AppendBefore(offset: nat)
    | AppendWrite(start: nat, payload: seq<Byte>) | AppendLength(length: nat)
    | FlushJournal | FailJournalFlush(through: nat, partial: bool)
    | MutateWrite(start: nat, payload: seq<Byte>, result: seq<Byte>)
    | MutateLength(length: nat, resultLength: nat) | FlushDatabase | FailDatabaseFlush
    | AppendCommit | Crash(through: nat, partial: bool) | DiscardIncompleteTail | BeginUndo | FinishCommitted
    | ReplayResize(length: nat) | ReplayNext(written: bool) | FinishUndo

  function DurableRecords(s: ProtocolState): seq<JournalRecord>
    requires s.durableCount <= |s.records|
  { s.records[..s.durableCount] }

  function InitialProtocol(original: seq<Byte>): ProtocolState {
    ProtocolState(original, [], 0, false, Active, false, false, false, 0)
  }

  predicate ProtocolInvariant(original: seq<Byte>, s: ProtocolState) {
    s.durableCount <= |s.records| &&
    ExactRecords(original, s.records) &&
    Recoverable(original, s.db, Saved(DurableRecords(s))) &&
    (forall j | 0 <= j < |s.records| && s.records[j].CommitRecord? :: s.records[j].image == s.db) &&
    (s.mode == Active || s.mode == Marking || s.mode == Replaying ==> !HasCommit(s.records)) &&
    (s.mode == Completing ==> HasCommit(s.records) && s.dataFlushed) &&
    (s.mode == Inspecting ==> s.durableCount == |s.records|) &&
    (s.mode == Marking ==> RecoveryRecord in s.records) &&
    (s.mode == Replaying ==> RecoveryRecord in DurableRecords(s)) &&
    (s.acknowledged ==> HasCommit(DurableRecords(s))) &&
    (s.incompleteTail ==> s.mode == Faulted || s.mode == Inspecting) &&
    s.replayCursor <= |original| &&
    (!s.replaySized ==> s.replayCursor == 0) &&
    (s.mode == Active || s.mode == Completing || s.mode == Marking ==> !s.replaySized) &&
    (s.mode == Replaying && s.replaySized ==>
      |s.db| == |original| && forall i | 0 <= i < s.replayCursor :: s.db[i] == original[i]) &&
    (s.mode == Finished ==> (if HasCommit(DurableRecords(s)) then
      RecordDecision(DurableRecords(s)) == Committed(s.db) else s.db == original))
  }

  // Partial writes keep all old bytes outside the intended interval; growth
  // exposes only the payload or the runtime's zero-filled gap. The length may
  // stop before all payload bytes have been written.
  predicate TornWrite(before: seq<Byte>, start: nat, payload: seq<Byte>, after: seq<Byte>) {
    |before| <= |after| <= (if |before| > start + |payload| then |before| else start + |payload|) &&
    (forall i: nat :: i < |before| && !(start <= i < start + |payload|) ==> after[i] == before[i]) &&
    (forall i: nat :: i < |after| ==>
      (i < |before| && after[i] == before[i]) ||
      (start <= i < start + |payload| && after[i] == payload[i-start]) ||
      (|before| <= i < start && after[i] == 0))
  }

  predicate ProtocolEnabled(original: seq<Byte>, s: ProtocolState, a: ProtocolAction) {
    s.durableCount <= |s.records| &&
    (match a
      case AppendBefore(offset) => s.mode == Active && offset < |original| &&
        offset < |s.db| && offset !in Saved(s.records)
      case AppendWrite(start, payload) => s.mode == Active
      case AppendLength(length) => s.mode == Active
      case FlushJournal => s.mode == Active || s.mode == Completing || s.mode == Marking
      case FailJournalFlush(through, partial) =>
        (s.mode == Active || s.mode == Completing || s.mode == Marking) &&
        s.durableCount <= through <= |s.records| && (partial ==> through < |s.records|)
      case MutateWrite(start, payload, result) => s.mode == Active &&
        WriteIntent(start, payload) in DurableRecords(s) &&
        Covered(DurableRecords(s), |original|, start, start + |payload|) &&
        TornWrite(s.db, start, payload, result)
      case MutateLength(length, resultLength) => s.mode == Active && LengthIntent(length) in DurableRecords(s) &&
        Covered(DurableRecords(s), |original|, length, |original|) &&
        Min(|s.db|, length) <= resultLength <= (if |s.db| > length then |s.db| else length)
      case FlushDatabase => s.mode == Active || s.mode == Replaying
      case FailDatabaseFlush => s.mode == Active || s.mode == Replaying
      case AppendCommit => s.mode == Active && s.dataFlushed
      case Crash(through, partial) => s.durableCount <= through <= |s.records| &&
        (partial ==> through < |s.records|)
      case DiscardIncompleteTail => s.mode == Inspecting && s.incompleteTail
      case BeginUndo => s.mode == Inspecting && !s.incompleteTail && !HasCommit(s.records)
      case FinishCommitted => s.mode == Inspecting && !s.incompleteTail && HasCommit(DurableRecords(s))
      case ReplayResize(length) => s.mode == Replaying && !s.replaySized &&
        Min(|s.db|, |original|) <= length <= (if |s.db| > |original| then |s.db| else |original|)
      case ReplayNext(written) => s.mode == Replaying && s.replaySized &&
        s.replayCursor < |original| && s.replayCursor < |s.db|
      case FinishUndo => (s.mode == Replaying && s.replaySized &&
        s.replayCursor == |original| && s.dataFlushed))
  }

  function ProtocolApply(original: seq<Byte>, s: ProtocolState, a: ProtocolAction): ProtocolState
    requires ProtocolEnabled(original, s, a)
  {
    (match a
      case AppendBefore(offset) => s.(records := s.records + [BeforeImage(offset, s.db[offset])])
      case AppendWrite(start, payload) => s.(records := s.records + [WriteIntent(start, payload)])
      case AppendLength(length) => s.(records := s.records + [LengthIntent(length)])
      case FlushJournal => s.(durableCount := |s.records|,
        mode := if s.mode == Completing then Finished else if s.mode == Marking then Replaying else Active,
        acknowledged := s.acknowledged || s.mode == Completing)
      case FailJournalFlush(through, partial) =>
        s.(durableCount := through, incompleteTail := partial, mode := Faulted)
      case MutateWrite(start, payload, result) => s.(db := result, dataFlushed := false)
      case MutateLength(length, resultLength) => s.(db := Resize(s.db, resultLength), dataFlushed := false)
      case FlushDatabase => s.(dataFlushed := true)
      case FailDatabaseFlush => s.(mode := Faulted, dataFlushed := false)
      case AppendCommit => s.(records := s.records + [CommitRecord(s.db)], mode := Completing)
      case Crash(through, partial) => s.(records := s.records[..through], durableCount := through,
        incompleteTail := s.incompleteTail || partial, mode := Inspecting,
        dataFlushed := false, replaySized := false, replayCursor := 0)
      case DiscardIncompleteTail => s.(incompleteTail := false)
      case BeginUndo => s.(records := s.records + [RecoveryRecord], mode := Marking,
        replaySized := false, replayCursor := 0)
      case FinishCommitted => s.(mode := Finished)
      case ReplayResize(length) => s.(db := Resize(s.db, length),
        replaySized := length == |original|, dataFlushed := false)
      case ReplayNext(written) =>
        if !written then s else
          s.(db := if s.replayCursor in Saved(DurableRecords(s)) then
            s.db[s.replayCursor := Saved(DurableRecords(s))[s.replayCursor]] else s.db,
            replayCursor := s.replayCursor + 1, dataFlushed := false)
      case FinishUndo => s.(mode := Finished))
  }

  lemma InitialProtocolIsValid(original: seq<Byte>)
    ensures ProtocolInvariant(original, InitialProtocol(original))
  {
    InitialImageIsRecoverable(original);
  }

  lemma ProtocolStepPreserves(original: seq<Byte>, s: ProtocolState, a: ProtocolAction)
    requires ProtocolInvariant(original, s)
    requires ProtocolEnabled(original, s, a)
    ensures ProtocolInvariant(original, ProtocolApply(original, s, a))
    ensures s.acknowledged ==> ProtocolApply(original, s, a).acknowledged
    ensures HasCommit(DurableRecords(s)) ==>
      RecordDecision(DurableRecords(ProtocolApply(original, s, a))) == RecordDecision(DurableRecords(s))
  {
    var next := ProtocolApply(original, s, a);
    RecordDecisionProperties(DurableRecords(s));
    PrefixRecordProperties(original, s.records, s.durableCount);
    match a {
      case AppendBefore(offset) =>
        SavedPrefixGrows(original, s.records, s.durableCount);
        assert offset !in Saved(DurableRecords(s));
        assert s.db[offset] == original[offset];
        AppendedRecordProperties(original, s.records, BeforeImage(offset, s.db[offset]));
        assert DurableRecords(next) == DurableRecords(s);
      case AppendWrite(start, payload) =>
        AppendedRecordProperties(original, s.records, WriteIntent(start, payload));
        assert DurableRecords(next) == DurableRecords(s);
      case AppendLength(length) =>
        AppendedRecordProperties(original, s.records, LengthIntent(length));
        assert DurableRecords(next) == DurableRecords(s);
      case FlushJournal =>
        MoreDurableRecordsPreserveRecoverability(original, s.db, s.records, s.durableCount, |s.records|);
        RecordDecisionProperties(s.records);
      case FailJournalFlush(through, partial) =>
        MoreDurableRecordsPreserveRecoverability(original, s.db, s.records, s.durableCount, through);
      case MutateWrite(start, payload, result) =>
        forall i: nat | i < |original| && i !in Saved(DurableRecords(s))
          ensures i < |result| && result[i] == original[i]
        {
          assert !(start <= i < start + |payload|);
        }
        assert Recoverable(original, next.db, Saved(DurableRecords(next)));
      case MutateLength(length, resultLength) =>
        forall i: nat | i < |original| && i !in Saved(DurableRecords(s))
          ensures i < |next.db| && next.db[i] == original[i]
        {
          assert i < length;
          assert i < resultLength;
        }
        assert Recoverable(original, next.db, Saved(DurableRecords(next)));
      case FlushDatabase =>
      case FailDatabaseFlush =>
      case AppendCommit =>
        assert !HasCommit(s.records);
        AppendedRecordProperties(original, s.records, CommitRecord(s.db));
        assert DurableRecords(next) == DurableRecords(s);
      case Crash(through, partial) =>
        PrefixRecordProperties(original, s.records, through);
        MoreDurableRecordsPreserveRecoverability(original, s.db, s.records, s.durableCount, through);
        assert DurableRecords(next) == s.records[..through];
      case DiscardIncompleteTail =>
      case BeginUndo =>
        AppendedRecordProperties(original, s.records, RecoveryRecord);
        assert DurableRecords(next) == DurableRecords(s);
      case FinishCommitted =>
        RecordDecisionProperties(DurableRecords(s));
    PrefixRecordProperties(original, s.records, s.durableCount);
      case ReplayResize(length) =>
        assert ReplayMutation(original, s.db, next.db, Saved(DurableRecords(s)));
        InterruptedReplayPreservesRecoverability(original, s.db, next.db, Saved(DurableRecords(s)));
      case ReplayNext(written) =>
        if written {
          var cursor := s.replayCursor;
          if cursor in Saved(DurableRecords(s)) {
            assert Saved(DurableRecords(s))[cursor] == original[cursor];
          } else {
            assert s.db[cursor] == original[cursor];
          }
          assert next.db[cursor] == original[cursor];
          forall i | 0 <= i < next.replayCursor
            ensures next.db[i] == original[i]
          {
            if i != cursor {
              assert i < s.replayCursor;
              assert next.db[i] == s.db[i];
            }
          }
          assert ReplayMutation(original, s.db, next.db, Saved(DurableRecords(s)));
          InterruptedReplayPreservesRecoverability(original, s.db, next.db, Saved(DurableRecords(s)));
        }
      case FinishUndo =>
        assert s.db == original;
    }
    assert next.durableCount <= |next.records|;
    RecordDecisionProperties(DurableRecords(next));
    assert ExactRecords(original, next.records);
    SavedRecordsAreExact(original, DurableRecords(next));
    assert Recoverable(original, next.db, Saved(DurableRecords(next)));
    assert next.mode == Active || next.mode == Marking || next.mode == Replaying ==> !HasCommit(next.records);
    assert next.mode == Completing ==> HasCommit(next.records);
    assert next.mode == Replaying && next.replaySized ==>
      |next.db| == |original| && forall i | 0 <= i < next.replayCursor :: next.db[i] == original[i];
  }

  predicate ProtocolPath(original: seq<Byte>, states: seq<ProtocolState>, actions: seq<ProtocolAction>) {
    |states| == |actions| + 1 &&
    forall i | 0 <= i < |actions| :: ProtocolEnabled(original, states[i], actions[i]) &&
      states[i+1] == ProtocolApply(original, states[i], actions[i])
  }

  lemma ProtocolTracePreserves(original: seq<Byte>, states: seq<ProtocolState>, actions: seq<ProtocolAction>)
    requires ProtocolPath(original, states, actions)
    requires ProtocolInvariant(original, states[0])
    ensures ProtocolInvariant(original, states[|states|-1])
    ensures states[0].acknowledged ==> states[|states|-1].acknowledged
    ensures HasCommit(DurableRecords(states[0])) ==>
      RecordDecision(DurableRecords(states[|states|-1])) == RecordDecision(DurableRecords(states[0]))
    decreases |actions|
  {
    if |actions| > 0 {
      ProtocolStepPreserves(original, states[0], actions[0]);
      RecordDecisionProperties(DurableRecords(states[0]));
      RecordDecisionProperties(DurableRecords(states[1]));
      assert HasCommit(DurableRecords(states[0])) ==> HasCommit(DurableRecords(states[1]));
      ProtocolTracePreserves(original, states[1..], actions[1..]);
    }
  }

  // A finite mixed trace may include capture, failed flush, torn mutation,
  // uncertain commit, crash, repeated replay, and restart. Completion follows
  // the durable decision; acknowledged commits cannot become undo selections.
  lemma CompletedProtocolSelectsBeforeOrCommitted(original: seq<Byte>,
      states: seq<ProtocolState>, actions: seq<ProtocolAction>)
    requires ProtocolPath(original, states, actions)
    requires states[0] == InitialProtocol(original)
    requires states[|states|-1].mode == Finished
    ensures states[|states|-1].durableCount <= |states[|states|-1].records|
    ensures states[|states|-1].db == original ||
      RecordDecision(DurableRecords(states[|states|-1])) == Committed(states[|states|-1].db)
    ensures states[|states|-1].acknowledged ==>
      RecordDecision(DurableRecords(states[|states|-1])) == Committed(states[|states|-1].db)
  {
    InitialProtocolIsValid(original);
    ProtocolTracePreserves(original, states, actions);
  }

  function ProtocolRecoveryAction(original: seq<Byte>, s: ProtocolState): ProtocolAction
    requires ProtocolInvariant(original, s)
    requires s.mode == Inspecting || s.mode == Marking || s.mode == Replaying
  {
    if s.mode == Inspecting then
      if s.incompleteTail then DiscardIncompleteTail
      else if HasCommit(DurableRecords(s)) then FinishCommitted else BeginUndo
    else if s.mode == Marking then FlushJournal
    else if !s.replaySized then ReplayResize(|original|)
    else if s.replayCursor < |original| then ReplayNext(true)
    else if !s.dataFlushed then FlushDatabase
    else FinishUndo
  }

  function ProtocolRecoveryRank(original: seq<Byte>, s: ProtocolState): nat
    requires ProtocolInvariant(original, s)
    requires s.mode == Inspecting || s.mode == Marking || s.mode == Replaying || s.mode == Finished
  {
    if s.mode == Finished then 0
    else if s.mode == Inspecting then 2 * |original| + 5 + (if s.incompleteTail then 1 else 0)
    else if s.mode == Marking then 2 * |original| + 4
    else if !s.replaySized then 2 * |original| + 3
    else 2 * (|original| - s.replayCursor) + (if s.dataFlushed then 1 else 2)
  }

  // An uninterrupted successful schedule has a decreasing rank: repair a
  // trailing fragment, establish the durable marker, resize, restore each byte,
  // flush, finish. Any interruption may restart the same schedule. This proves
  // abstract algorithmic progress, not a bound or refinement for C# syscalls.
  lemma ProtocolRecoveryProgress(original: seq<Byte>, s: ProtocolState)
    requires ProtocolInvariant(original, s)
    requires s.mode == Inspecting || s.mode == Marking || s.mode == Replaying
    ensures ProtocolEnabled(original, s, ProtocolRecoveryAction(original, s))
    ensures ProtocolInvariant(original, ProtocolApply(original, s, ProtocolRecoveryAction(original, s)))
    ensures ProtocolApply(original, s, ProtocolRecoveryAction(original, s)).mode == Inspecting ||
      ProtocolApply(original, s, ProtocolRecoveryAction(original, s)).mode == Marking ||
      ProtocolApply(original, s, ProtocolRecoveryAction(original, s)).mode == Replaying ||
      ProtocolApply(original, s, ProtocolRecoveryAction(original, s)).mode == Finished
    ensures RecordDecision(DurableRecords(ProtocolApply(original, s, ProtocolRecoveryAction(original, s)))) ==
      RecordDecision(DurableRecords(s))
    ensures ProtocolRecoveryRank(original, ProtocolApply(original, s, ProtocolRecoveryAction(original, s))) <
      ProtocolRecoveryRank(original, s)
  {
    var a := ProtocolRecoveryAction(original, s);
    if s.mode == Inspecting {
      assert DurableRecords(s) == s.records;
    }
    assert ProtocolEnabled(original, s, a);
    ProtocolStepPreserves(original, s, a);
    RecordDecisionProperties(DurableRecords(s));
    RecordDecisionProperties(DurableRecords(ProtocolApply(original, s, a)));
  }

  lemma CompleteProtocolRecovery(original: seq<Byte>, s: ProtocolState)
    returns (states: seq<ProtocolState>, actions: seq<ProtocolAction>)
    requires ProtocolInvariant(original, s)
    requires s.mode == Inspecting || s.mode == Marking || s.mode == Replaying || s.mode == Finished
    ensures ProtocolPath(original, states, actions) && states[0] == s
    ensures ProtocolInvariant(original, states[|states|-1])
    ensures states[|states|-1].mode == Finished
    ensures RecordDecision(DurableRecords(states[|states|-1])) == RecordDecision(DurableRecords(s))
    ensures if HasCommit(DurableRecords(s)) then
      RecordDecision(DurableRecords(s)) == Committed(states[|states|-1].db)
      else states[|states|-1].db == original
    decreases ProtocolRecoveryRank(original, s)
  {
    if s.mode == Finished {
      states, actions := [s], [];
    } else {
      ProtocolRecoveryProgress(original, s);
      var a := ProtocolRecoveryAction(original, s);
      var next := ProtocolApply(original, s, a);
      var tailStates, tailActions := CompleteProtocolRecovery(original, next);
      states, actions := [s] + tailStates, [a] + tailActions;
      forall i | 0 <= i < |actions|
        ensures ProtocolEnabled(original, states[i], actions[i]) &&
          states[i+1] == ProtocolApply(original, states[i], actions[i])
      {
        if i != 0 {
          assert states[i] == tailStates[i-1];
          assert actions[i] == tailActions[i-1];
        }
      }
      RecordDecisionProperties(DurableRecords(s));
      RecordDecisionProperties(DurableRecords(next));
    }
  }

  lemma AcknowledgedImageSurvivesMixedTrace(original: seq<Byte>,
      states: seq<ProtocolState>, actions: seq<ProtocolAction>)
    requires ProtocolPath(original, states, actions)
    requires ProtocolInvariant(original, states[0]) && states[0].acknowledged
    requires states[|states|-1].mode == Finished
    ensures states[|states|-1].acknowledged
    ensures states[|states|-1].db == states[0].db
  {
    ProtocolTracePreserves(original, states, actions);
    RecordDecisionProperties(DurableRecords(states[0]));
    RecordDecisionProperties(DurableRecords(states[|states|-1]));
  }

  // Counterexamples expose the actual guards rather than assuming successful
  // capture/flush as a premise of the end-to-end theorem.
  lemma PendingCaptureCannotEnableWrite()
    ensures ProtocolInvariant([1], ProtocolState([1],
      [BeforeImage(0, 1), WriteIntent(0, [2])], 0, false, Active, false, false, false, 0))
    ensures !ProtocolEnabled([1], ProtocolState([1],
      [BeforeImage(0, 1), WriteIntent(0, [2])], 0, false, Active, false, false, false, 0),
      MutateWrite(0, [2], [2]))
    ensures !ProtocolEnabled([1], ProtocolState([1],
      [BeforeImage(0, 1), WriteIntent(0, [2])], 1, false, Active, false, false, false, 0),
      MutateWrite(0, [2], [2]))
  { }

  lemma MissingTailRecordDisablesTruncation()
    ensures !ProtocolEnabled([1, 2], ProtocolState([1, 2],
      [BeforeImage(0, 1), LengthIntent(0)], 2, false, Active, false, false, false, 0), MutateLength(0, 0))
    ensures ProtocolEnabled([1, 2], ProtocolState([1, 2],
      [BeforeImage(0, 1), BeforeImage(1, 2), LengthIntent(0)], 3,
      false, Active, false, false, false, 0), MutateLength(0, 0))
  {
    var first: seq<JournalRecord> := [BeforeImage(0, 1)];
    var missing := first + [LengthIntent(0)];
    var captured := first + [BeforeImage(1, 2)];
    var complete := captured + [LengthIntent(0)];
    AppendedRecordProperties([1, 2], [], BeforeImage(0, 1));
    AppendedRecordProperties([1, 2], first, LengthIntent(0));
    AppendedRecordProperties([1, 2], first, BeforeImage(1, 2));
    AppendedRecordProperties([1, 2], captured, LengthIntent(0));
    assert missing == [BeforeImage(0, 1), LengthIntent(0)];
    assert Saved(missing) == map[0 := 1];
    assert Saved(complete) == map[0 := 1, 1 := 2];
    assert 1 !in Saved(missing);
    assert !Covered(missing, 2, 0, 2);
    assert Covered(complete, 2, 0, 2);
    assert complete == [BeforeImage(0, 1), BeforeImage(1, 2), LengthIntent(0)];
    assert LengthIntent(0) in complete;
    var bad := ProtocolState([1, 2], missing, 2, false, Active, false, false, false, 0);
    assert DurableRecords(bad) == missing;
    if ProtocolEnabled([1, 2], bad, MutateLength(0, 0)) {
      assert Covered(DurableRecords(bad), 2, 0, 2);
      assert 1 in Saved(DurableRecords(bad));
    }
  }

  lemma CommitNeedsDataFlush(original: seq<Byte>, s: ProtocolState)
    requires ProtocolInvariant(original, s) && !s.dataFlushed
    ensures !ProtocolEnabled(original, s, AppendCommit)
  { }

  lemma FailedCommitCannotRollbackInPlace(original: seq<Byte>, s: ProtocolState, through: nat, partial: bool)
    requires ProtocolInvariant(original, s) && s.mode == Completing
    requires ProtocolEnabled(original, s, FailJournalFlush(through, partial))
    ensures ProtocolApply(original, s, FailJournalFlush(through, partial)).mode == Faulted
    ensures !ProtocolApply(original, s, FailJournalFlush(through, partial)).acknowledged || s.acknowledged
    ensures !ProtocolEnabled(original, ProtocolApply(original, s, FailJournalFlush(through, partial)), BeginUndo)
    ensures forall length: nat, resultLength: nat :: !ProtocolEnabled(original,
      ProtocolApply(original, s, FailJournalFlush(through, partial)), MutateLength(length, resultLength))
  { }

  lemma CommittedRestartCannotSelectUndo(original: seq<Byte>, s: ProtocolState)
    requires ProtocolInvariant(original, s) && s.mode == Inspecting
    requires HasCommit(DurableRecords(s))
    ensures !ProtocolEnabled(original, s, BeginUndo)
  {
    assert DurableRecords(s) == s.records;
  }

  // Buffered refinement of the composed protocol. The logical protocol's db
  // is the current readable/volatile image; durableDb is independently stored.
  // An issued effect may persist later, out of order, repeatedly, or only in
  // torn pieces. A crash drops unpersisted effects and reloads durableDb.
  // Successful FlushDatabase synchronizes the exact current bytes AND length,
  // and fences all older effects so none can reappear afterwards. This storage
  // contract, logical journal framing and exclusive I/O are explicit premises;
  // this model does not establish that a filesystem/device satisfies them.
  datatype DatabaseEffect = BufferedWrite(start: nat, payload: seq<Byte>)
    | BufferedLength(length: nat)
  datatype BufferedState = BufferedState(protocol: ProtocolState,
    durableDb: seq<Byte>, pending: seq<DatabaseEffect>)
  datatype BufferedAction = Issue(action: ProtocolAction)
    | Persist(index: nat, result: seq<Byte>)

  predicate ProtectedEffect(originalLength: nat, records: seq<JournalRecord>, effect: DatabaseEffect) {
    match effect
      case BufferedWrite(start, payload) => Covered(records, originalLength, start, start + |payload|)
      case BufferedLength(length) => Covered(records, originalLength, length, originalLength)
  }

  predicate EffectPersistence(before: seq<Byte>, effect: DatabaseEffect, after: seq<Byte>) {
    match effect
      case BufferedWrite(start, payload) => TornWrite(before, start, payload, after)
      case BufferedLength(length) =>
        Min(|before|, length) <= |after| <= (if |before| > length then |before| else length) &&
        after == Resize(before, |after|)
  }

  lemma ProtectedEffectPreserves(original: seq<Byte>, records: seq<JournalRecord>,
      before: seq<Byte>, effect: DatabaseEffect, after: seq<Byte>)
    requires Recoverable(original, before, Saved(records))
    requires ProtectedEffect(|original|, records, effect)
    requires EffectPersistence(before, effect, after)
    ensures Recoverable(original, after, Saved(records))
  {
    forall i: nat | i < |original| && i !in Saved(records)
      ensures i < |after| && after[i] == original[i]
    {
      match effect {
        case BufferedWrite(start, payload) =>
          assert !(start <= i < start + |payload|);
        case BufferedLength(length) =>
          assert i < length;
          assert i < |after|;
      }
    }
  }

  function InitialBufferedProtocol(original: seq<Byte>): BufferedState {
    BufferedState(InitialProtocol(original), original, [])
  }

  predicate BufferedInvariant(original: seq<Byte>, b: BufferedState) {
    ProtocolInvariant(original, b.protocol) &&
    Recoverable(original, b.durableDb, Saved(DurableRecords(b.protocol))) &&
    (forall j | 0 <= j < |b.pending| ::
      ProtectedEffect(|original|, DurableRecords(b.protocol), b.pending[j])) &&
    (b.protocol.dataFlushed || HasCommit(b.protocol.records) || b.protocol.mode == Finished ==>
      b.durableDb == b.protocol.db && |b.pending| == 0)
  }

  predicate BufferedEnabled(original: seq<Byte>, b: BufferedState, a: BufferedAction) {
    match a
      case Issue(action) => ProtocolEnabled(original, b.protocol, action)
      case Persist(index, result) => (index < |b.pending| &&
        EffectPersistence(b.durableDb, b.pending[index], result))
  }

  function QueuedEffects(s: ProtocolState, a: ProtocolAction): seq<DatabaseEffect>
    requires s.durableCount <= |s.records|
  {
    match a
      case MutateWrite(start, payload, result) => [BufferedWrite(start, payload)]
      case MutateLength(length, resultLength) => [BufferedLength(length)]
      case ReplayResize(length) => [BufferedLength(length)]
      case ReplayNext(written) =>
        if written && s.replayCursor in Saved(DurableRecords(s)) then
          [BufferedWrite(s.replayCursor, [Saved(DurableRecords(s))[s.replayCursor]])] else []
      case _ => []
  }

  function BufferedApply(original: seq<Byte>, b: BufferedState, a: BufferedAction): BufferedState
    requires BufferedEnabled(original, b, a)
  {
    match a
      case Persist(index, result) => b.(durableDb := result)
      case Issue(action) =>
        var next := ProtocolApply(original, b.protocol, action);
        if action.Crash? then
          BufferedState(next.(db := b.durableDb), b.durableDb, [])
        else if action.FlushDatabase? then
          BufferedState(next, b.protocol.db, [])
        else BufferedState(next, b.durableDb, b.pending + QueuedEffects(b.protocol, action))
  }

  lemma InitialBufferedProtocolIsValid(original: seq<Byte>)
    ensures BufferedInvariant(original, InitialBufferedProtocol(original))
  {
    InitialProtocolIsValid(original);
  }

  lemma MoreRecordsProtectEffects(original: seq<Byte>, records: seq<JournalRecord>,
      oldCount: nat, newCount: nat, effect: DatabaseEffect)
    requires ExactRecords(original, records)
    requires oldCount <= newCount <= |records|
    requires ProtectedEffect(|original|, records[..oldCount], effect)
    ensures ProtectedEffect(|original|, records[..newCount], effect)
  {
    SavedPrefixGrows(original, records[..newCount], oldCount);
    assert (records[..newCount])[..oldCount] == records[..oldCount];
  }

  lemma BufferedStepPreserves(original: seq<Byte>, b: BufferedState, a: BufferedAction)
    requires BufferedInvariant(original, b)
    requires BufferedEnabled(original, b, a)
    ensures BufferedInvariant(original, BufferedApply(original, b, a))
    ensures b.protocol.acknowledged ==> BufferedApply(original, b, a).protocol.acknowledged
    ensures HasCommit(DurableRecords(b.protocol)) ==>
      RecordDecision(DurableRecords(BufferedApply(original, b, a).protocol)) ==
        RecordDecision(DurableRecords(b.protocol))
  {
    var s := b.protocol;
    var next := BufferedApply(original, b, a);
    match a {
      case Persist(index, result) =>
        ProtectedEffectPreserves(original, DurableRecords(s), b.durableDb, b.pending[index], result);
        assert |b.pending| > 0;
        assert !s.dataFlushed && !HasCommit(s.records) && s.mode != Finished;
      case Issue(action) =>
        ProtocolStepPreserves(original, s, action);
        var logical := ProtocolApply(original, s, action);
        assert DurableRecords(next.protocol) == DurableRecords(logical);
        if action.FlushJournal? || action.FailJournalFlush? || action.Crash? {
          var count := logical.durableCount;
          MoreDurableRecordsPreserveRecoverability(original, b.durableDb, s.records, s.durableCount, count);
          assert DurableRecords(logical) == s.records[..count];
          if !action.Crash? {
            forall j | 0 <= j < |b.pending|
              ensures ProtectedEffect(|original|, DurableRecords(logical), b.pending[j])
            {
              MoreRecordsProtectEffects(original, s.records, s.durableCount, count, b.pending[j]);
            }
          }
        } else {
          assert DurableRecords(logical) == DurableRecords(s);
        }
        if action.Crash? {
          assert next.protocol.mode == Inspecting;
          assert !next.protocol.replaySized && next.protocol.replayCursor == 0;
          assert HasCommit(s.records) ==> b.durableDb == s.db;
          assert ProtocolInvariant(original, next.protocol);
        } else if action.FlushDatabase? {
          assert Recoverable(original, next.durableDb, Saved(DurableRecords(next.protocol)));
        } else {
          var extra := QueuedEffects(s, action);
          match action {
            case MutateWrite(start, payload, result) =>
            case MutateLength(length, resultLength) =>
            case ReplayResize(length) =>
              forall i: nat | length <= i < |original|
                ensures i in Saved(DurableRecords(s))
              {
                if i !in Saved(DurableRecords(s)) {
                  assert i < |s.db|;
                  assert |s.db| <= length;
                }
              }
            case ReplayNext(written) =>
              if written && s.replayCursor in Saved(DurableRecords(s)) {
                assert Covered(DurableRecords(s), |original|, s.replayCursor, s.replayCursor + 1);
              }
            case _ =>
          }
          assert forall j | 0 <= j < |extra| ::
            ProtectedEffect(|original|, DurableRecords(next.protocol), extra[j]);
          forall j | 0 <= j < |next.pending|
            ensures ProtectedEffect(|original|, DurableRecords(next.protocol), next.pending[j])
          {
            if j < |b.pending| {
              assert next.pending[j] == b.pending[j];
            } else {
              assert next.pending[j] == extra[j - |b.pending|];
            }
          }
        }
    }
  }

  predicate BufferedPath(original: seq<Byte>, states: seq<BufferedState>, actions: seq<BufferedAction>) {
    |states| == |actions| + 1 &&
    forall i | 0 <= i < |actions| :: BufferedEnabled(original, states[i], actions[i]) &&
      states[i+1] == BufferedApply(original, states[i], actions[i])
  }

  lemma BufferedTracePreserves(original: seq<Byte>, states: seq<BufferedState>, actions: seq<BufferedAction>)
    requires BufferedPath(original, states, actions)
    requires BufferedInvariant(original, states[0])
    ensures BufferedInvariant(original, states[|states|-1])
    ensures states[0].protocol.acknowledged ==> states[|states|-1].protocol.acknowledged
    ensures HasCommit(DurableRecords(states[0].protocol)) ==>
      RecordDecision(DurableRecords(states[|states|-1].protocol)) ==
        RecordDecision(DurableRecords(states[0].protocol))
    decreases |actions|
  {
    if |actions| > 0 {
      BufferedStepPreserves(original, states[0], actions[0]);
      RecordDecisionProperties(DurableRecords(states[0].protocol));
      RecordDecisionProperties(DurableRecords(states[1].protocol));
      BufferedTracePreserves(original, states[1..], actions[1..]);
    }
  }

  lemma CompletedBufferedProtocolSelectsBeforeOrCommitted(original: seq<Byte>,
      states: seq<BufferedState>, actions: seq<BufferedAction>)
    requires BufferedPath(original, states, actions)
    requires states[0] == InitialBufferedProtocol(original)
    requires states[|states|-1].protocol.mode == Finished
    ensures states[|states|-1].protocol.durableCount <= |states[|states|-1].protocol.records|
    ensures states[|states|-1].durableDb == original ||
      RecordDecision(DurableRecords(states[|states|-1].protocol)) == Committed(states[|states|-1].durableDb)
    ensures states[|states|-1].protocol.acknowledged ==>
      RecordDecision(DurableRecords(states[|states|-1].protocol)) == Committed(states[|states|-1].durableDb)
  {
    InitialBufferedProtocolIsValid(original);
    BufferedTracePreserves(original, states, actions);
  }

  lemma AcknowledgedBufferedImageSurvivesMixedTrace(original: seq<Byte>,
      states: seq<BufferedState>, actions: seq<BufferedAction>)
    requires BufferedPath(original, states, actions)
    requires BufferedInvariant(original, states[0]) && states[0].protocol.acknowledged
    requires states[|states|-1].protocol.mode == Finished
    ensures states[|states|-1].protocol.acknowledged
    ensures states[|states|-1].durableDb == states[0].durableDb
  {
    BufferedTracePreserves(original, states, actions);
    RecordDecisionProperties(DurableRecords(states[0].protocol));
    RecordDecisionProperties(DurableRecords(states[|states|-1].protocol));
  }

  lemma CompleteBufferedRecovery(original: seq<Byte>, b: BufferedState)
    returns (states: seq<BufferedState>, actions: seq<BufferedAction>)
    requires BufferedInvariant(original, b)
    requires b.protocol.mode == Inspecting || b.protocol.mode == Marking ||
      b.protocol.mode == Replaying || b.protocol.mode == Finished
    ensures BufferedPath(original, states, actions) && states[0] == b
    ensures BufferedInvariant(original, states[|states|-1])
    ensures states[|states|-1].protocol.mode == Finished
    ensures RecordDecision(DurableRecords(states[|states|-1].protocol)) == RecordDecision(DurableRecords(b.protocol))
    ensures if HasCommit(DurableRecords(b.protocol)) then
      RecordDecision(DurableRecords(b.protocol)) == Committed(states[|states|-1].durableDb)
      else states[|states|-1].durableDb == original
    decreases ProtocolRecoveryRank(original, b.protocol)
  {
    if b.protocol.mode == Finished {
      states, actions := [b], [];
    } else {
      ProtocolRecoveryProgress(original, b.protocol);
      var a := Issue(ProtocolRecoveryAction(original, b.protocol));
      assert BufferedEnabled(original, b, a);
      BufferedStepPreserves(original, b, a);
      var next := BufferedApply(original, b, a);
      assert next.protocol == ProtocolApply(original, b.protocol, a.action);
      var tailStates, tailActions := CompleteBufferedRecovery(original, next);
      states, actions := [b] + tailStates, [a] + tailActions;
      forall i | 0 <= i < |actions|
        ensures BufferedEnabled(original, states[i], actions[i]) &&
          states[i+1] == BufferedApply(original, states[i], actions[i])
      {
        if i != 0 {
          assert states[i] == tailStates[i-1];
          assert actions[i] == tailActions[i-1];
        }
      }
      RecordDecisionProperties(DurableRecords(b.protocol));
      RecordDecisionProperties(DurableRecords(next.protocol));
    }
  }

  lemma SuccessfulFlushFencesPendingEffects(original: seq<Byte>, b: BufferedState)
    requires BufferedInvariant(original, b)
    requires BufferedEnabled(original, b, Issue(FlushDatabase))
    ensures BufferedApply(original, b, Issue(FlushDatabase)).durableDb == b.protocol.db
    ensures BufferedApply(original, b, Issue(FlushDatabase)).protocol.db == b.protocol.db
    ensures |BufferedApply(original, b, Issue(FlushDatabase)).pending| == 0
    ensures forall index: nat, result: seq<Byte> ::
      !BufferedEnabled(original, BufferedApply(original, b, Issue(FlushDatabase)), Persist(index, result))
  { }

  // From an invariant prepared state with two pending writes, persist the
  // later write first, then the earlier write. Crash reads the stale durable
  // image, not the latest volatile bytes. This is a transition witness; the
  // initial-state theorem above establishes safety for all reachable traces.
  lemma ReorderedPendingWritesAreAdmitted()
    returns (states: seq<BufferedState>, actions: seq<BufferedAction>)
    ensures BufferedPath([1], states, actions)
    ensures |states| == 4
    ensures BufferedInvariant([1], states[0])
    ensures states[2].protocol.db == [3] && states[2].durableDb == [2]
    ensures states[3].protocol.db == [2] && states[3].durableDb == [2]
    ensures states[3].protocol.mode == Inspecting && |states[3].pending| == 0
    ensures BufferedInvariant([1], states[3])
  {
    var r0: seq<JournalRecord> := [];
    var r1 := r0 + [BeforeImage(0, 1)];
    var r2 := r1 + [WriteIntent(0, [2])];
    var records := r2 + [WriteIntent(0, [3])];
    AppendedRecordProperties([1], [], BeforeImage(0, 1));
    AppendedRecordProperties([1], r1, WriteIntent(0, [2]));
    AppendedRecordProperties([1], r2, WriteIntent(0, [3]));
    assert Saved(records) == map[0 := 1];
    var s := ProtocolState([3], records, 3, false, Active, false, false, false, 0);
    assert |records| == 3 && DurableRecords(s) == records;
    SavedRecordsAreExact([1], records);
    assert Recoverable([1], [3], Saved(records));
    assert Recoverable([1], [1], Saved(records));
    assert Covered(records, 1, 0, 1);
    assert ProtocolInvariant([1], s);
    var b0 := BufferedState(s, [1], [BufferedWrite(0, [2]), BufferedWrite(0, [3])]);
    assert BufferedInvariant([1], b0);
    var a0 := Persist(1, [3]);
    BufferedStepPreserves([1], b0, a0);
    var b1 := BufferedApply([1], b0, a0);
    var a1 := Persist(0, [2]);
    BufferedStepPreserves([1], b1, a1);
    var b2 := BufferedApply([1], b1, a1);
    var a2 := Issue(Crash(3, false));
    BufferedStepPreserves([1], b2, a2);
    var b3 := BufferedApply([1], b2, a2);
    states := [b0, b1, b2, b3];
    actions := [a0, a1, a2];
  }

  // The successful-flush fence is essential, rather than a theorem about
  // arbitrary devices: replaying an older queued write could replace C.
  lemma UnfencedStaleWriteCanDestroyCommittedImage()
    ensures EffectPersistence([3], BufferedWrite(0, [2]), [2])
    ensures [2] != [3]
  { }
}
