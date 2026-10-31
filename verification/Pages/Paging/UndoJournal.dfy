// Verification-only byte model; this file is not compiled into the library.
// Correspondence: StreamPageStore.WriteAsync/SetLengthAsync call
// PersistentRollbackJournal.RecordBeforeWrite/RecordBeforeTruncate first;
// those methods save original pages and flush their records before mutation.
// Aligned page payloads expand into logical byte records. Journal parsing, checksums,
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

  // Page-record refinement. A page body is expanded into the existing logical
  // byte captures; its tag, framing and checksum are still outside this model.
  // The construction reads only uncaptured current bytes. Their original value
  // follows from BufferedInvariant, rather than being supplied as a premise.
  datatype PageCapture = PageCapture(pageOffset: nat, pageBytes: seq<Byte>)

  predicate RecordsExtend(before: seq<JournalRecord>, after: seq<JournalRecord>) {
    |before| <= |after| && after[..|before|] == before
  }

  opaque predicate ExactPagePayload(original: seq<Byte>, pageSize: nat, page: PageCapture) {
    0 < pageSize && page.pageOffset % pageSize == 0 && page.pageOffset < |original| &&
    |page.pageBytes| == pageSize &&
    forall i | 0 <= i < pageSize :: page.pageBytes[i] ==
      (if page.pageOffset + i < |original| then original[page.pageOffset + i] else 0)
  }

  // At public page-capture boundaries, every original page is either fully
  // captured or wholly uncaptured. Intermediate byte appends need not satisfy
  // this property; they cannot enable a protected page mutation until flushed.
  opaque predicate WholePageCaptures(originalLength: nat, records: seq<JournalRecord>, pageSize: nat)
    requires 0 < pageSize
  {
    forall start: nat | start < originalLength && start % pageSize == 0 ::
      Covered(records, originalLength, start, start + pageSize) ||
      (forall i: nat :: start <= i < start + pageSize && i < originalLength ==> i !in Saved(records))
  }

  lemma RecordExtensionPreservesCoverage(original: seq<Byte>, before: seq<JournalRecord>,
      after: seq<JournalRecord>, lo: nat, hi: nat)
    requires ExactRecords(original, after)
    requires RecordsExtend(before, after)
    requires Covered(before, |original|, lo, hi)
    ensures Covered(after, |original|, lo, hi)
  {
    SavedPrefixGrows(original, after, |before|);
  }

  lemma RecordExtensionsAreTransitive(first: seq<JournalRecord>, second: seq<JournalRecord>, third: seq<JournalRecord>)
    requires RecordsExtend(first, second) && RecordsExtend(second, third)
    ensures RecordsExtend(first, third)
  {
    assert third[..|first|] == (third[..|second|])[..|first|];
  }

  lemma SameSavedPreservesWholePages(originalLength: nat, before: seq<JournalRecord>,
      after: seq<JournalRecord>, pageSize: nat)
    requires 0 < pageSize && WholePageCaptures(originalLength, before, pageSize)
    requires Saved(before) == Saved(after)
    ensures WholePageCaptures(originalLength, after, pageSize)
  {
    reveal WholePageCaptures();
  }

  lemma JoinBufferedPaths(original: seq<Byte>, left: seq<BufferedState>, first: seq<BufferedAction>,
      right: seq<BufferedState>, second: seq<BufferedAction>)
    requires BufferedPath(original, left, first)
    requires BufferedPath(original, right, second)
    requires left[|left|-1] == right[0]
    ensures BufferedPath(original, left[..|left|-1] + right, first + second)
  {
    forall i | 0 <= i < |first + second|
      ensures BufferedEnabled(original, (left[..|left|-1] + right)[i], (first + second)[i]) &&
        (left[..|left|-1] + right)[i+1] ==
          BufferedApply(original, (left[..|left|-1] + right)[i], (first + second)[i])
    {
      if i < |first| {
        assert (left[..|left|-1] + right)[i] == left[i];
        assert (left[..|left|-1] + right)[i+1] == left[i+1];
      } else {
        assert (left[..|left|-1] + right)[i] == right[i-|first|];
        assert (left[..|left|-1] + right)[i+1] == right[i-|first|+1];
      }
    }
  }

  lemma AppendBufferedPath(original: seq<Byte>, states: seq<BufferedState>, actions: seq<BufferedAction>,
      action: BufferedAction)
    requires BufferedPath(original, states, actions)
    requires BufferedEnabled(original, states[|states|-1], action)
    ensures BufferedPath(original,
      states + [BufferedApply(original, states[|states|-1], action)], actions + [action])
  {
    var next := BufferedApply(original, states[|states|-1], action);
    assert BufferedPath(original, [states[|states|-1], next], [action]);
    JoinBufferedPaths(original, states, actions, [states[|states|-1], next], [action]);
    assert states[..|states|-1] + [states[|states|-1], next] == states + [next];
  }

  lemma CaptureCurrentByteRange(original: seq<Byte>, b: BufferedState, lo: nat, hi: nat)
    returns (next: BufferedState, bytes: seq<Byte>, states: seq<BufferedState>, actions: seq<BufferedAction>)
    requires BufferedInvariant(original, b) && b.protocol.mode == Active
    requires lo <= hi <= |original|
    requires forall i: nat :: lo <= i < hi ==> i !in Saved(b.protocol.records)
    ensures BufferedPath(original, states, actions) && states[0] == b && states[|states|-1] == next
    ensures BufferedInvariant(original, next) && next.protocol.mode == Active
    ensures next.protocol.db == b.protocol.db && next.durableDb == b.durableDb && next.pending == b.pending
    ensures RecordsExtend(b.protocol.records, next.protocol.records)
    ensures bytes == original[lo..hi]
    ensures |next.protocol.records| == |b.protocol.records| + hi - lo
    ensures forall j | 0 <= j < hi - lo ::
      next.protocol.records[|b.protocol.records| + j] == BeforeImage(lo + j, bytes[j])
    ensures forall i: nat :: i in Saved(next.protocol.records) <==>
      i in Saved(b.protocol.records) || lo <= i < hi
    decreases hi - lo
  {
    if lo == hi {
      next, bytes, states, actions := b, [], [b], [];
    } else {
      SavedPrefixGrows(original, b.protocol.records, b.protocol.durableCount);
      assert lo !in Saved(DurableRecords(b.protocol));
      assert lo < |b.protocol.db| && b.protocol.db[lo] == original[lo];
      var a := Issue(AppendBefore(lo));
      BufferedStepPreserves(original, b, a);
      var step := BufferedApply(original, b, a);
      AppendedRecordProperties(original, b.protocol.records, BeforeImage(lo, b.protocol.db[lo]));
      assert Saved(step.protocol.records) == Saved(b.protocol.records)[lo := b.protocol.db[lo]];
      var tail: seq<Byte>;
      var tailStates: seq<BufferedState>;
      var tailActions: seq<BufferedAction>;
      next, tail, tailStates, tailActions := CaptureCurrentByteRange(original, step, lo + 1, hi);
      bytes := [b.protocol.db[lo]] + tail;
      assert original[lo..hi] == [original[lo]] + original[lo+1..hi];
      states, actions := [b] + tailStates, [a] + tailActions;
      assert BufferedPath(original, [b, step], [a]);
      JoinBufferedPaths(original, [b, step], [a], tailStates, tailActions);
      RecordExtensionsAreTransitive(b.protocol.records, step.protocol.records, next.protocol.records);
      forall j | 0 <= j < hi - lo
        ensures next.protocol.records[|b.protocol.records| + j] == BeforeImage(lo + j, bytes[j])
      {
        if j == 0 {
          assert next.protocol.records[|b.protocol.records|] == step.protocol.records[|b.protocol.records|];
        } else {
          assert next.protocol.records[|b.protocol.records| + j] ==
            next.protocol.records[|step.protocol.records| + j - 1];
        }
      }
    }
  }

  lemma AlignedPageIntervalsAreDisjoint(first: nat, second: nat, pageSize: nat)
    requires 0 < pageSize && first % pageSize == 0 && second % pageSize == 0 && first != second
    ensures first + pageSize <= second || second + pageSize <= first
  {
    assert first == (first / pageSize) * pageSize;
    assert second == (second / pageSize) * pageSize;
    if first < second {
      assert first / pageSize < second / pageSize;
      assert (first / pageSize + 1) * pageSize <= (second / pageSize) * pageSize;
    } else {
      assert second / pageSize < first / pageSize;
      assert (second / pageSize + 1) * pageSize <= (first / pageSize) * pageSize;
    }
  }

  lemma CaptureAlignedPage(original: seq<Byte>, b: BufferedState, pageSize: nat, start: nat)
    returns (next: BufferedState, pages: seq<PageCapture>, states: seq<BufferedState>, actions: seq<BufferedAction>)
    requires BufferedInvariant(original, b) && b.protocol.mode == Active
    requires 0 < pageSize && start % pageSize == 0 && start < |original|
    requires WholePageCaptures(|original|, b.protocol.records, pageSize)
    ensures BufferedPath(original, states, actions) && states[0] == b && states[|states|-1] == next
    ensures BufferedInvariant(original, next) && next.protocol.mode == Active
    ensures next.protocol.db == b.protocol.db && next.durableDb == b.durableDb && next.pending == b.pending
    ensures RecordsExtend(b.protocol.records, next.protocol.records)
    ensures WholePageCaptures(|original|, next.protocol.records, pageSize)
    ensures Covered(next.protocol.records, |original|, start, start + pageSize)
    ensures forall j | 0 <= j < |pages| :: ExactPagePayload(original, pageSize, pages[j])
    ensures |pages| <= 1 && (|pages| == 1 ==> pages[0].pageOffset == start)
    ensures |pages| == 1 ==> start !in Saved(b.protocol.records)
    ensures |pages| == 1 ==> |pages[0].pageBytes| == pageSize
    ensures |pages| == 0 ==> next.protocol.records == b.protocol.records
    ensures |pages| == 1 ==>
      |next.protocol.records| == |b.protocol.records| + Min(pageSize, |original| - start)
    ensures |pages| == 1 ==> forall j | 0 <= j < Min(pageSize, |original| - start) ::
      next.protocol.records[|b.protocol.records| + j] == BeforeImage(start + j, pages[0].pageBytes[j])
  {
    reveal WholePageCaptures();
    reveal ExactPagePayload();
    if Covered(b.protocol.records, |original|, start, start + pageSize) {
      next, pages, states, actions := b, [], [b], [];
    } else {
      var end := Min(start + pageSize, |original|);
      var bytes: seq<Byte>;
      next, bytes, states, actions := CaptureCurrentByteRange(original, b, start, end);
      var padded := bytes + seq(pageSize - |bytes|, i => 0 as Byte);
      pages := [PageCapture(start, padded)];
      assert ExactPagePayload(original, pageSize, pages[0]);
      forall other: nat | other < |original| && other % pageSize == 0
        ensures Covered(next.protocol.records, |original|, other, other + pageSize) ||
          (forall i: nat :: other <= i < other + pageSize && i < |original| ==> i !in Saved(next.protocol.records))
      {
        if other == start {
          assert Covered(next.protocol.records, |original|, other, other + pageSize);
        } else if Covered(b.protocol.records, |original|, other, other + pageSize) {
          RecordExtensionPreservesCoverage(original, b.protocol.records, next.protocol.records, other, other + pageSize);
        } else {
          AlignedPageIntervalsAreDisjoint(start, other, pageSize);
        }
      }
    }
  }

  // Keep the recursive schedule's path/coverage composition independent of
  // the buffered invariant's quantified persistence obligations. These opaque
  // summaries are unfolded and proved by the adapters, never trusted inputs.
  opaque predicate CaptureBoundary(original: seq<Byte>, b: BufferedState, pageSize: nat)
    requires 0 < pageSize
  {
    BufferedInvariant(original, b) && b.protocol.mode == Active &&
    WholePageCaptures(|original|, b.protocol.records, pageSize)
  }

  opaque predicate CapturePath(original: seq<Byte>, before: BufferedState, after: BufferedState,
      states: seq<BufferedState>, actions: seq<BufferedAction>)
  {
    BufferedPath(original, states, actions) && states[0] == before && states[|states|-1] == after &&
    after.protocol.db == before.protocol.db && after.durableDb == before.durableDb && after.pending == before.pending &&
    RecordsExtend(before.protocol.records, after.protocol.records)
  }

  lemma CaptureBoundaryPage(original: seq<Byte>, b: BufferedState, pageSize: nat, start: nat)
    returns (next: BufferedState, pages: seq<PageCapture>, states: seq<BufferedState>, actions: seq<BufferedAction>)
    requires 0 < pageSize && CaptureBoundary(original, b, pageSize)
    requires start % pageSize == 0 && start < |original|
    ensures CaptureBoundary(original, next, pageSize) && CapturePath(original, b, next, states, actions)
    ensures 0 < |states|
    ensures Covered(next.protocol.records, |original|, start, start + pageSize)
    ensures forall j | 0 <= j < |pages| :: ExactPagePayload(original, pageSize, pages[j])
    ensures |pages| <= 1 && (|pages| == 1 ==> pages[0].pageOffset == start)
    ensures |pages| == 1 ==> start !in Saved(b.protocol.records)
  {
    reveal CaptureBoundary();
    reveal CapturePath();
    next, pages, states, actions := CaptureAlignedPage(original, b, pageSize, start);
  }

  lemma ComposeCapturePaths(original: seq<Byte>, before: BufferedState, middle: BufferedState, after: BufferedState,
      left: seq<BufferedState>, first: seq<BufferedAction>, right: seq<BufferedState>, second: seq<BufferedAction>)
    requires CapturePath(original, before, middle, left, first) && 0 < |left|
    requires CapturePath(original, middle, after, right, second)
    ensures CapturePath(original, before, after, left[..|left|-1] + right, first + second)
  {
    reveal CapturePath();
    JoinBufferedPaths(original, left, first, right, second);
    RecordExtensionsAreTransitive(before.protocol.records, middle.protocol.records, after.protocol.records);
  }

  lemma CapturePathRecordsGrow(original: seq<Byte>, before: BufferedState, after: BufferedState, pageSize: nat,
      states: seq<BufferedState>, actions: seq<BufferedAction>)
    requires 0 < pageSize && CaptureBoundary(original, after, pageSize)
    requires CapturePath(original, before, after, states, actions)
    ensures ExactRecords(original, after.protocol.records)
    ensures RecordsExtend(before.protocol.records, after.protocol.records)
    ensures forall i: nat :: i in Saved(before.protocol.records) ==> i in Saved(after.protocol.records)
  {
    reveal CaptureBoundary();
    reveal CapturePath();
    SavedPrefixGrows(original, after.protocol.records, |before.protocol.records|);
  }

  lemma JoinCaptureCoverage(records: seq<JournalRecord>, originalLength: nat, start: nat, middle: nat, limit: nat)
    requires Covered(records, originalLength, start, middle)
    requires Covered(records, originalLength, middle, limit)
    ensures Covered(records, originalLength, start, limit)
  { }

  lemma JoinCapturedPageLists(original: seq<Byte>, pageSize: nat, start: nat, limit: nat,
      head: seq<PageCapture>, tail: seq<PageCapture>, before: map<nat, Byte>, middle: map<nat, Byte>)
    requires 0 < pageSize && start < limit && start < |original|
    requires |head| <= 1 && (|head| == 1 ==> head[0].pageOffset == start && start !in before)
    requires forall j | 0 <= j < |head| :: ExactPagePayload(original, pageSize, head[j])
    requires forall j | 0 <= j < |tail| :: ExactPagePayload(original, pageSize, tail[j])
    requires forall j | 0 <= j < |tail| :: start + pageSize <= tail[j].pageOffset < limit
    requires forall j | 0 <= j < |tail| :: tail[j].pageOffset < |original| && tail[j].pageOffset !in middle
    requires forall i, j | 0 <= i < j < |tail| :: tail[i].pageOffset < tail[j].pageOffset
    requires forall i: nat :: i in before ==> i in middle
    ensures forall j | 0 <= j < |head + tail| :: ExactPagePayload(original, pageSize, (head + tail)[j])
    ensures forall j | 0 <= j < |head + tail| :: start <= (head + tail)[j].pageOffset < limit
    ensures forall j | 0 <= j < |head + tail| ::
      (head + tail)[j].pageOffset < |original| && (head + tail)[j].pageOffset !in before
    ensures forall i, j | 0 <= i < j < |head + tail| :: (head + tail)[i].pageOffset < (head + tail)[j].pageOffset
  {
    forall j | 0 <= j < |head + tail|
      ensures ExactPagePayload(original, pageSize, (head + tail)[j])
      ensures start <= (head + tail)[j].pageOffset < limit
      ensures (head + tail)[j].pageOffset < |original| && (head + tail)[j].pageOffset !in before
    {
      if j < |head| {
        assert j == 0 && (head + tail)[j].pageOffset == start;
      } else {
        assert (head + tail)[j] == tail[j-|head|];
      }
    }
    forall i, j | 0 <= i < j < |head + tail|
      ensures (head + tail)[i].pageOffset < (head + tail)[j].pageOffset
    {
      if i < |head| {
        assert i == 0 && (head + tail)[i].pageOffset == start;
        assert j >= |head|;
      } else {
        assert (head + tail)[i] == tail[i-|head|];
        assert (head + tail)[j] == tail[j-|head|];
      }
    }
  }
  lemma AlignedProductIsMonotone(smaller: nat, larger: nat, factor: nat)
    requires smaller <= larger
    ensures smaller * factor <= larger * factor
  {
    var difference: nat := larger - smaller;
    assert larger == smaller + difference;
    assert larger * factor == smaller * factor + difference * factor;
    assert 0 <= difference * factor;
  }
  lemma NextPageIsAligned(start: nat, pageSize: nat)
    requires 0 < pageSize && start % pageSize == 0
    ensures (start + pageSize) % pageSize == 0
  {
    var quotient := start / pageSize;
    var nextQuotient := (start + pageSize) / pageSize;
    var remainder := (start + pageSize) % pageSize;
    assert start == quotient * pageSize;
    assert start + pageSize == nextQuotient * pageSize + remainder;
    assert 0 <= remainder < pageSize;
    if nextQuotient <= quotient {
      AlignedProductIsMonotone(nextQuotient, quotient, pageSize);
      assert nextQuotient * pageSize + remainder < quotient * pageSize + pageSize;
      assert false;
    } else if quotient + 1 < nextQuotient {
      AlignedProductIsMonotone(quotient + 2, nextQuotient, pageSize);
      assert false;
    }
    assert nextQuotient == quotient + 1;
  }
  lemma {:induction false} CaptureAlignedPageRangeSchedule(original: seq<Byte>, b: BufferedState, pageSize: nat,
      start: nat, limit: nat)
    returns (next: BufferedState, pages: seq<PageCapture>, states: seq<BufferedState>, actions: seq<BufferedAction>)
    requires 0 < pageSize && start % pageSize == 0 && CaptureBoundary(original, b, pageSize)
    ensures CaptureBoundary(original, next, pageSize) && CapturePath(original, b, next, states, actions)
    ensures 0 < |states|
    ensures Covered(next.protocol.records, |original|, start, limit)
    ensures forall j | 0 <= j < |pages| :: ExactPagePayload(original, pageSize, pages[j])
    ensures forall j | 0 <= j < |pages| :: start <= pages[j].pageOffset < limit
    ensures forall j | 0 <= j < |pages| ::
      pages[j].pageOffset < |original| && pages[j].pageOffset !in Saved(b.protocol.records)
    ensures Covered(b.protocol.records, |original|, start, limit) ==> |pages| == 0
    ensures forall i, j | 0 <= i < j < |pages| :: pages[i].pageOffset < pages[j].pageOffset
    decreases if start < Min(limit, |original|) then Min(limit, |original|) - start else 0
  {
    if start >= limit || start >= |original| {
      next, pages, states, actions := b, [], [b], [];
      reveal CapturePath();
    } else {
      var captured: BufferedState;
      var headPages: seq<PageCapture>;
      var headStates: seq<BufferedState>;
      var headActions: seq<BufferedAction>;
      captured, headPages, headStates, headActions := CaptureBoundaryPage(original, b, pageSize, start);
      var tailPages: seq<PageCapture>;
      var tailStates: seq<BufferedState>;
      var tailActions: seq<BufferedAction>;
      next, tailPages, tailStates, tailActions :=
        CaptureAlignedPageRangeSchedule(original, captured, pageSize, start + pageSize, limit) by {
          NextPageIsAligned(start, pageSize);
        }
      ComposeCapturePaths(original, b, captured, next, headStates, headActions, tailStates, tailActions);
      states, actions := headStates[..|headStates|-1] + tailStates, headActions + tailActions;
      pages := headPages + tailPages;
      CapturePathRecordsGrow(original, b, captured, pageSize, headStates, headActions);
      CapturePathRecordsGrow(original, captured, next, pageSize, tailStates, tailActions);
      RecordExtensionPreservesCoverage(original, captured.protocol.records, next.protocol.records, start, start + pageSize);
      JoinCaptureCoverage(next.protocol.records, |original|, start, start + pageSize, limit);
      JoinCapturedPageLists(original, pageSize, start, limit, headPages, tailPages,
        Saved(b.protocol.records), Saved(captured.protocol.records));
    }
  }
  // Capture every original aligned page intersecting [start, limit), skipping
  // pages already captured. The rank counts remaining original bytes, so the
  // schedule is finite even when the requested interval extends past EOF.
  lemma CaptureAlignedPageRange(original: seq<Byte>, b: BufferedState, pageSize: nat,
      start: nat, limit: nat)
    returns (next: BufferedState, pages: seq<PageCapture>, states: seq<BufferedState>, actions: seq<BufferedAction>)
    requires BufferedInvariant(original, b) && b.protocol.mode == Active
    requires 0 < pageSize && start % pageSize == 0
    requires WholePageCaptures(|original|, b.protocol.records, pageSize)
    ensures BufferedPath(original, states, actions) && states[0] == b && states[|states|-1] == next
    ensures BufferedInvariant(original, next) && next.protocol.mode == Active
    ensures next.protocol.db == b.protocol.db && next.durableDb == b.durableDb && next.pending == b.pending
    ensures RecordsExtend(b.protocol.records, next.protocol.records)
    ensures WholePageCaptures(|original|, next.protocol.records, pageSize)
    ensures Covered(next.protocol.records, |original|, start, limit)
    ensures forall j | 0 <= j < |pages| :: ExactPagePayload(original, pageSize, pages[j])
    ensures forall j | 0 <= j < |pages| :: start <= pages[j].pageOffset < limit
    ensures forall j | 0 <= j < |pages| ::
      pages[j].pageOffset < |original| && pages[j].pageOffset !in Saved(b.protocol.records)
    ensures Covered(b.protocol.records, |original|, start, limit) ==> |pages| == 0
    ensures forall i, j | 0 <= i < j < |pages| :: pages[i].pageOffset < pages[j].pageOffset
    decreases if start < Min(limit, |original|) then Min(limit, |original|) - start else 0
  {
    reveal CaptureBoundary();
    reveal CapturePath();
    next, pages, states, actions := CaptureAlignedPageRangeSchedule(original, b, pageSize, start, limit);
  }
  lemma InitialBufferedProtocolHasWholePages(original: seq<Byte>, pageSize: nat)
    requires 0 < pageSize
    ensures WholePageCaptures(|original|, InitialBufferedProtocol(original).protocol.records, pageSize)
  { reveal WholePageCaptures(); }

  // A successful preparation appends only a write/length intent, then flushes
  // the journal. Capture, database mutation and parser/flush implementation are
  // separate boundaries. The helper retains the constructed durable coverage.
  lemma FlushCapturedPageIntent(original: seq<Byte>, b: BufferedState, pageSize: nat,
      intent: ProtocolAction, lo: nat, hi: nat)
    returns (next: BufferedState, states: seq<BufferedState>, actions: seq<BufferedAction>)
    requires BufferedInvariant(original, b) && b.protocol.mode == Active
    requires 0 < pageSize && WholePageCaptures(|original|, b.protocol.records, pageSize)
    requires intent.AppendWrite? || intent.AppendLength?
    requires Covered(b.protocol.records, |original|, lo, hi)
    ensures BufferedPath(original, states, actions) && states[0] == b && states[|states|-1] == next
    ensures BufferedInvariant(original, next) && next.protocol.mode == Active
    ensures next.protocol.db == b.protocol.db && next.durableDb == b.durableDb && next.pending == b.pending
    ensures RecordsExtend(b.protocol.records, next.protocol.records)
    ensures WholePageCaptures(|original|, next.protocol.records, pageSize)
    ensures Covered(DurableRecords(next.protocol), |original|, lo, hi)
    ensures intent.AppendWrite? ==> WriteIntent(intent.start, intent.payload) in DurableRecords(next.protocol)
    ensures intent.AppendLength? ==> LengthIntent(intent.length) in DurableRecords(next.protocol)
  {
    var append := Issue(intent);
    BufferedStepPreserves(original, b, append);
    var intended := BufferedApply(original, b, append);
    match intent {
      case AppendWrite(start, payload) =>
        AppendedRecordProperties(original, b.protocol.records, WriteIntent(start, payload));
      case AppendLength(length) =>
        AppendedRecordProperties(original, b.protocol.records, LengthIntent(length));
      case _ =>
    }
    SameSavedPreservesWholePages(|original|, b.protocol.records, intended.protocol.records, pageSize);
    assert RecordsExtend(b.protocol.records, intended.protocol.records);
    RecordExtensionPreservesCoverage(original, b.protocol.records, intended.protocol.records, lo, hi);
    var flush := Issue(FlushJournal);
    BufferedStepPreserves(original, intended, flush);
    next := BufferedApply(original, intended, flush);
    states, actions := [b, intended, next], [append, flush];
    assert BufferedPath(original, [b, intended], [append]);
    AppendBufferedPath(original, [b, intended], [append], flush);
    assert DurableRecords(next.protocol) == intended.protocol.records;
  }

  // General preparation constructs all intersecting original page captures,
  // then appends and durably flushes the write intent before database mutation.
  // Empty writes and writes entirely beyond the original EOF need no capture.
  lemma PreparePageWrite(original: seq<Byte>, b: BufferedState, pageSize: nat,
      start: nat, payload: seq<Byte>)
    returns (next: BufferedState, pages: seq<PageCapture>, states: seq<BufferedState>, actions: seq<BufferedAction>)
    requires BufferedInvariant(original, b) && b.protocol.mode == Active
    requires 0 < pageSize && start % pageSize == 0
    requires WholePageCaptures(|original|, b.protocol.records, pageSize)
    ensures BufferedPath(original, states, actions) && states[0] == b && states[|states|-1] == next
    ensures BufferedInvariant(original, next) && next.protocol.mode == Active
    ensures next.protocol.db == b.protocol.db && next.durableDb == b.durableDb && next.pending == b.pending
    ensures RecordsExtend(b.protocol.records, next.protocol.records)
    ensures WholePageCaptures(|original|, next.protocol.records, pageSize)
    ensures WriteIntent(start, payload) in DurableRecords(next.protocol)
    ensures Covered(DurableRecords(next.protocol), |original|, start, start + |payload|)
    ensures forall j | 0 <= j < |pages| :: ExactPagePayload(original, pageSize, pages[j])
    ensures forall i, j | 0 <= i < j < |pages| :: pages[i].pageOffset < pages[j].pageOffset
    ensures forall result: seq<Byte> :: TornWrite(next.protocol.db, start, payload, result) ==>
      BufferedEnabled(original, next, Issue(MutateWrite(start, payload, result)))
  {
    var captured: BufferedState;
    captured, pages, states, actions := CaptureAlignedPageRange(original, b, pageSize, start, start + |payload|);
    assert Covered(captured.protocol.records, |original|, start, start + |payload|);
    var tailStates: seq<BufferedState>;
    var tailActions: seq<BufferedAction>;
    next, tailStates, tailActions :=
      FlushCapturedPageIntent(original, captured, pageSize, AppendWrite(start, payload), start, start + |payload|);
    JoinBufferedPaths(original, states, actions, tailStates, tailActions);
    states, actions := states[..|states|-1] + tailStates, actions + tailActions;
    RecordExtensionsAreTransitive(b.protocol.records, captured.protocol.records, next.protocol.records);
  }

  // Production admits aligned truncations no longer than the readable file.
  // Capturing [length, original EOF) protects every removed original byte,
  // including arbitrarily many pages and a zero-padded partial final page.
  // Already protected pages and a tail wholly beyond original EOF are skipped.
  lemma PreparePageTruncate(original: seq<Byte>, b: BufferedState, pageSize: nat, length: nat)
    returns (next: BufferedState, pages: seq<PageCapture>, states: seq<BufferedState>, actions: seq<BufferedAction>)
    requires BufferedInvariant(original, b) && b.protocol.mode == Active
    requires 0 < pageSize && length % pageSize == 0 && length <= |b.protocol.db|
    requires WholePageCaptures(|original|, b.protocol.records, pageSize)
    ensures BufferedPath(original, states, actions) && states[0] == b && states[|states|-1] == next
    ensures BufferedInvariant(original, next) && next.protocol.mode == Active
    ensures next.protocol.db == b.protocol.db && next.durableDb == b.durableDb && next.pending == b.pending
    ensures RecordsExtend(b.protocol.records, next.protocol.records)
    ensures WholePageCaptures(|original|, next.protocol.records, pageSize)
    ensures LengthIntent(length) in DurableRecords(next.protocol)
    ensures Covered(DurableRecords(next.protocol), |original|, length, |original|)
    ensures forall j | 0 <= j < |pages| :: ExactPagePayload(original, pageSize, pages[j])
    ensures forall j | 0 <= j < |pages| :: length <= pages[j].pageOffset < |original|
    ensures forall i, j | 0 <= i < j < |pages| :: pages[i].pageOffset < pages[j].pageOffset
    ensures forall resultLength: nat ::
      length <= resultLength <= |next.protocol.db| ==>
        BufferedEnabled(original, next, Issue(MutateLength(length, resultLength)))
  {
    var captured: BufferedState;
    captured, pages, states, actions := CaptureAlignedPageRange(original, b, pageSize, length, |original|);
    var tailStates: seq<BufferedState>;
    var tailActions: seq<BufferedAction>;
    next, tailStates, tailActions :=
      FlushCapturedPageIntent(original, captured, pageSize, AppendLength(length), length, |original|);
    JoinBufferedPaths(original, states, actions, tailStates, tailActions);
    states, actions := states[..|states|-1] + tailStates, actions + tailActions;
    RecordExtensionsAreTransitive(b.protocol.records, captured.protocol.records, next.protocol.records);
  }

  // A write within one aligned original page uses a constructed page body,
  // appends its intent and flushes the journal before any database mutation.
  // The page body includes zero padding only beyond a partial original tail.
  lemma PrepareSinglePageWrite(original: seq<Byte>, b: BufferedState, pageSize: nat,
      pageOffset: nat, start: nat, payload: seq<Byte>)
    returns (next: BufferedState, pages: seq<PageCapture>, states: seq<BufferedState>, actions: seq<BufferedAction>)
    requires BufferedInvariant(original, b) && b.protocol.mode == Active
    requires 0 < pageSize && pageOffset % pageSize == 0 && pageOffset < |original|
    requires pageOffset <= start && start + |payload| <= pageOffset + pageSize
    requires WholePageCaptures(|original|, b.protocol.records, pageSize)
    ensures BufferedPath(original, states, actions) && states[0] == b && states[|states|-1] == next
    ensures BufferedInvariant(original, next) && next.protocol.mode == Active
    ensures next.protocol.db == b.protocol.db && next.durableDb == b.durableDb && next.pending == b.pending
    ensures RecordsExtend(b.protocol.records, next.protocol.records)
    ensures WholePageCaptures(|original|, next.protocol.records, pageSize)
    ensures WriteIntent(start, payload) in DurableRecords(next.protocol)
    ensures Covered(DurableRecords(next.protocol), |original|, start, start + |payload|)
    ensures forall j | 0 <= j < |pages| :: ExactPagePayload(original, pageSize, pages[j])
    ensures forall result: seq<Byte> :: TornWrite(next.protocol.db, start, payload, result) ==>
      BufferedEnabled(original, next, Issue(MutateWrite(start, payload, result)))
  {
    var captured: BufferedState;
    captured, pages, states, actions := CaptureAlignedPage(original, b, pageSize, pageOffset);
    assert Covered(captured.protocol.records, |original|, start, start + |payload|);
    var tailStates: seq<BufferedState>;
    var tailActions: seq<BufferedAction>;
    next, tailStates, tailActions :=
      FlushCapturedPageIntent(original, captured, pageSize, AppendWrite(start, payload), start, start + |payload|);
    JoinBufferedPaths(original, states, actions, tailStates, tailActions);
    states, actions := states[..|states|-1] + tailStates, actions + tailActions;
    RecordExtensionsAreTransitive(b.protocol.records, captured.protocol.records, next.protocol.records);
  }

  // The single-page specialization retains its narrower caller contract.
  // PreparePageTruncate above constructs complete arbitrary removed tails.
  lemma PrepareSinglePageTruncate(original: seq<Byte>, b: BufferedState, pageSize: nat, length: nat)
    returns (next: BufferedState, pages: seq<PageCapture>, states: seq<BufferedState>, actions: seq<BufferedAction>)
    requires BufferedInvariant(original, b) && b.protocol.mode == Active
    requires 0 < pageSize && length % pageSize == 0 && length < |original| <= length + pageSize
    requires length <= |b.protocol.db|
    requires WholePageCaptures(|original|, b.protocol.records, pageSize)
    ensures BufferedPath(original, states, actions) && states[0] == b && states[|states|-1] == next
    ensures BufferedInvariant(original, next) && next.protocol.mode == Active
    ensures next.protocol.db == b.protocol.db && next.durableDb == b.durableDb && next.pending == b.pending
    ensures RecordsExtend(b.protocol.records, next.protocol.records)
    ensures WholePageCaptures(|original|, next.protocol.records, pageSize)
    ensures LengthIntent(length) in DurableRecords(next.protocol)
    ensures Covered(DurableRecords(next.protocol), |original|, length, |original|)
    ensures forall j | 0 <= j < |pages| :: ExactPagePayload(original, pageSize, pages[j])
    ensures forall resultLength: nat ::
      Min(|next.protocol.db|, length) <= resultLength <= (if |next.protocol.db| > length then |next.protocol.db| else length) ==>
        BufferedEnabled(original, next, Issue(MutateLength(length, resultLength)))
  {
    var captured: BufferedState;
    captured, pages, states, actions := CaptureAlignedPage(original, b, pageSize, length);
    assert Covered(captured.protocol.records, |original|, length, |original|);
    var tailStates: seq<BufferedState>;
    var tailActions: seq<BufferedAction>;
    next, tailStates, tailActions :=
      FlushCapturedPageIntent(original, captured, pageSize, AppendLength(length), length, |original|);
    JoinBufferedPaths(original, states, actions, tailStates, tailActions);
    states, actions := states[..|states|-1] + tailStates, actions + tailActions;
    RecordExtensionsAreTransitive(b.protocol.records, captured.protocol.records, next.protocol.records);
  }
  // Logical content admission is derived from the persisted record sequence,
  // rather than an externally supplied map of after-image values. Record
  // acquisition, checksum validation and payload identity remain assumptions
  // of the caller that supplies these already classified logical records.
  function RecordedValues(records: seq<JournalRecord>, offset: nat): set<Byte>
    decreases |records|
  {
    if |records| == 0 then {}
    else (RecordedValues(records[..|records|-1], offset) +
      (if records[|records|-1].WriteIntent? &&
          records[|records|-1].start <= offset < records[|records|-1].start + |records[|records|-1].payload|
        then {records[|records|-1].payload[offset-records[|records|-1].start]} else {}))
  }

  function MaximumWriteEnd(records: seq<JournalRecord>): nat
    decreases |records|
  {
    if |records| == 0 then 0
    else if records[|records|-1].WriteIntent? &&
        MaximumWriteEnd(records[..|records|-1]) < records[|records|-1].start + |records[|records|-1].payload|
      then records[|records|-1].start + |records[|records|-1].payload|
    else MaximumWriteEnd(records[..|records|-1])
  }

  function RecordedIntents(records: seq<JournalRecord>): map<nat, set<Byte>> {
    map i: nat | i < MaximumWriteEnd(records) && RecordedValues(records, i) != {} :: RecordedValues(records, i)
  }

  // Mirrors the handwritten content loop: start with original/replay-zero
  // admission, then OR in the byte of each matching complete write record.
  function FoldRecordedAdmission(original: seq<Byte>, records: seq<JournalRecord>,
      minimumLength: nat, recovering: bool, offset: nat, value: Byte): bool
    decreases |records|
  {
    if |records| == 0 then Allowed(original, map[], minimumLength, recovering, offset, value)
    else (FoldRecordedAdmission(original, records[..|records|-1], minimumLength, recovering, offset, value) ||
      (records[|records|-1].WriteIntent? &&
        records[|records|-1].start <= offset < records[|records|-1].start + |records[|records|-1].payload| &&
        value == records[|records|-1].payload[offset-records[|records|-1].start]))
  }

  lemma RecordedValuesAreBounded(records: seq<JournalRecord>, offset: nat)
    ensures RecordedValues(records, offset) != {} ==> offset < MaximumWriteEnd(records)
    decreases |records|
  {
    if |records| > 0 {
      RecordedValuesAreBounded(records[..|records|-1], offset);
    }
  }

  lemma RecordedIntentLookup(records: seq<JournalRecord>, offset: nat, value: Byte)
    ensures (offset in RecordedIntents(records) && value in RecordedIntents(records)[offset]) ==
      (value in RecordedValues(records, offset))
  {
    RecordedValuesAreBounded(records, offset);
  }

  lemma RecordFoldAdmitsExactlyRecordedValues(original: seq<Byte>, records: seq<JournalRecord>,
      minimumLength: nat, recovering: bool, offset: nat, value: Byte)
    ensures FoldRecordedAdmission(original, records, minimumLength, recovering, offset, value) ==
      (Allowed(original, map[], minimumLength, recovering, offset, value) || value in RecordedValues(records, offset))
    ensures FoldRecordedAdmission(original, records, minimumLength, recovering, offset, value) ==
      Allowed(original, RecordedIntents(records), minimumLength, recovering, offset, value)
    decreases |records|
  {
    if |records| > 0 {
      RecordFoldAdmitsExactlyRecordedValues(original, records[..|records|-1], minimumLength, recovering, offset, value);
    }
    RecordedIntentLookup(records, offset, value);
  }

  predicate AdmittedByRecords(original: seq<Byte>, current: seq<Byte>, records: seq<JournalRecord>,
      minimumLength: nat, recovering: bool) {
    forall i: nat :: i < |current| ==>
      FoldRecordedAdmission(original, records, minimumLength, recovering, i, current[i])
  }

  lemma RecordedAdmissionIsAuthorization(original: seq<Byte>, current: seq<Byte>, records: seq<JournalRecord>,
      minimumLength: nat, recovering: bool)
    ensures AdmittedByRecords(original, current, records, minimumLength, recovering) ==
      Authorized(original, current, RecordedIntents(records), minimumLength, recovering)
  {
    forall i: nat | i < |current|
      ensures FoldRecordedAdmission(original, records, minimumLength, recovering, i, current[i]) ==
        Allowed(original, RecordedIntents(records), minimumLength, recovering, i, current[i])
    {
      RecordFoldAdmitsExactlyRecordedValues(original, records, minimumLength, recovering, i, current[i]);
    }
  }

  lemma AppendedRecordRetainsAdmission(original: seq<Byte>, records: seq<JournalRecord>, record: JournalRecord,
      minimumLength: nat, recovering: bool, offset: nat, value: Byte)
    requires FoldRecordedAdmission(original, records, minimumLength, recovering, offset, value)
    ensures FoldRecordedAdmission(original, records + [record], minimumLength, recovering, offset, value)
  {
    assert (records + [record])[..|records|] == records;
  }

  // A torn sparse write may expose zeros in a previously removed original
  // tail. Those zeros need explicit record admission before mutation; merely
  // having complete undo coverage does not establish content authorization.
  lemma RecordedTornWritePreservesAdmission(original: seq<Byte>, before: seq<Byte>, after: seq<Byte>,
      records: seq<JournalRecord>, minimumLength: nat, recovering: bool, start: nat, payload: seq<Byte>)
    requires AdmittedByRecords(original, before, records, minimumLength, recovering)
    requires TornWrite(before, start, payload, after)
    requires forall i: nat :: |before| <= i < start && i < |after| ==>
      FoldRecordedAdmission(original, records, minimumLength, recovering, i, 0)
    ensures AdmittedByRecords(original, after, records + [WriteIntent(start, payload)], minimumLength, recovering)
  {
    var next := records + [WriteIntent(start, payload)];
    assert next[..|records|] == records;
    forall i: nat | i < |after|
      ensures FoldRecordedAdmission(original, next, minimumLength, recovering, i, after[i])
    {
      if i < |before| && after[i] == before[i] {
        AppendedRecordRetainsAdmission(original, records, WriteIntent(start, payload), minimumLength, recovering, i, after[i]);
      } else if start <= i < start + |payload| && after[i] == payload[i-start] {
        assert next[|records|] == WriteIntent(start, payload);
      } else {
        assert |before| <= i < start && after[i] == 0;
        AppendedRecordRetainsAdmission(original, records, WriteIntent(start, payload), minimumLength, recovering, i, 0);
      }
    }
  }

  // This concrete counterexample distinguishes recoverable bytes from an
  // image that the content classifier can admit. A sound preparation step
  // must record the sparse zero gap rather than invent a replay marker.
  lemma SparseGapNeedsRecordedAuthorization()
    ensures Recoverable([1, 2, 3], [1, 0, 4], map[1 := 2, 2 := 3])
    ensures !AdmittedByRecords([1, 2, 3], [1, 0, 4], [WriteIntent(2, [4])], 1, false)
  {
    var original: seq<Byte> := [1, 2, 3];
    var current: seq<Byte> := [1, 0, 4];
    var records: seq<JournalRecord> := [WriteIntent(2, [4])];
    RecordFoldAdmitsExactlyRecordedValues(original, records, 1, false, 1, 0);
    assert !FoldRecordedAdmission(original, records, 1, false, 1, 0);
    if AdmittedByRecords(original, current, records, 1, false) {
      assert 1 < |current| && current[1] == 0;
      assert FoldRecordedAdmission(original, records, 1, false, 1, current[1]);
    }
  }
  function ZeroBytes(length: nat): seq<Byte> { seq(length, i => 0 as Byte) }

  // Explicit zero-valued after-images close the sparse-write gap without
  // granting recovery-marker privileges before replay actually begins.
  // A physical implementation may split this logical span into page records.
  lemma RecordedGapAndWritePreserveAdmission(original: seq<Byte>, before: seq<Byte>, after: seq<Byte>,
      records: seq<JournalRecord>, minimumLength: nat, start: nat, payload: seq<Byte>)
    requires AdmittedByRecords(original, before, records, minimumLength, false)
    requires |before| <= start
    requires TornWrite(before, start, payload, after)
    ensures AdmittedByRecords(original, after,
      records + [WriteIntent(|before|, ZeroBytes(start-|before|)), WriteIntent(start, payload)],
      minimumLength, false)
  {
    var gap := WriteIntent(|before|, ZeroBytes(start-|before|));
    var prepared := records + [gap];
    assert prepared[..|records|] == records;
    forall i: nat | i < |before|
      ensures FoldRecordedAdmission(original, prepared, minimumLength, false, i, before[i])
    {
      AppendedRecordRetainsAdmission(original, records, gap, minimumLength, false, i, before[i]);
    }
    forall i: nat | |before| <= i < start && i < |after|
      ensures FoldRecordedAdmission(original, prepared, minimumLength, false, i, 0)
    { }
    RecordedTornWritePreservesAdmission(original, before, after, prepared, minimumLength, false, start, payload);
    assert prepared + [WriteIntent(start, payload)] == records + [gap, WriteIntent(start, payload)];
  }

  lemma RecordedInterruptedReplayPreservesBoth(original: seq<Byte>, before: seq<Byte>, after: seq<Byte>,
      saved: map<nat, Byte>, records: seq<JournalRecord>, minimumLength: nat, recovering: bool)
    requires Recoverable(original, before, saved)
    requires AdmittedByRecords(original, before, records, minimumLength, recovering)
    requires forall i: nat :: minimumLength <= i < |original| ==> i in saved
    requires Min(|before|, |original|) <= |after|
    requires forall i: nat :: i < |after| ==>
      (i < |before| && after[i] == before[i]) ||
      (i < |original| && after[i] == original[i]) ||
      (recovering && i >= minimumLength && after[i] == 0)
    ensures Recoverable(original, after, saved)
    ensures AdmittedByRecords(original, after, records, minimumLength, recovering)
  {
    RecordedAdmissionIsAuthorization(original, before, records, minimumLength, recovering);
    InterruptedReplayPreservesBothInvariants(original, before, after, saved,
      RecordedIntents(records), minimumLength, recovering);
    RecordedAdmissionIsAuthorization(original, after, records, minimumLength, recovering);
  }

  predicate AdmittedReplayMutation(original: seq<Byte>, before: seq<Byte>, after: seq<Byte>,
      minimumLength: nat, recovering: bool) {
    Min(|before|, |original|) <= |after| &&
    forall i: nat :: i < |after| ==>
      (i < |before| && after[i] == before[i]) ||
      (i < |original| && after[i] == original[i]) ||
      (recovering && i >= minimumLength && after[i] == 0)
  }

  // Arbitrarily many finite interruptions retain both record-derived content
  // admission and exact original recovery. Progress still needs an eventually
  // uninterrupted replay and successful I/O; this is not a production-loop proof.
  lemma RecordedReplayTracePreservesBoth(original: seq<Byte>, images: seq<seq<Byte>>,
      saved: map<nat, Byte>, records: seq<JournalRecord>, minimumLength: nat, recovering: bool)
    requires 0 < |images|
    requires Recoverable(original, images[0], saved)
    requires AdmittedByRecords(original, images[0], records, minimumLength, recovering)
    requires forall i: nat :: minimumLength <= i < |original| ==> i in saved
    requires forall j: nat :: j + 1 < |images| ==>
      AdmittedReplayMutation(original, images[j], images[j+1], minimumLength, recovering)
    ensures Recoverable(original, images[|images|-1], saved)
    ensures AdmittedByRecords(original, images[|images|-1], records, minimumLength, recovering)
    ensures Restore(original, images[|images|-1], saved) == original
    decreases |images|
  {
    if |images| > 1 {
      RecordedInterruptedReplayPreservesBoth(original, images[0], images[1], saved, records, minimumLength, recovering);
      RecordedReplayTracePreservesBoth(original, images[1..], saved, records, minimumLength, recovering);
    }
    CompleteReplayRestoresExactOriginal(original, images[|images|-1], saved);
  }

  // The logical parser derives both content-control inputs from complete
  // durable records. Length intents follow all captures for the removed tail;
  // an incomplete trailing record supplies no minimum or replay authority.
  function RecordedMinimum(originalLength: nat, records: seq<JournalRecord>): nat
    ensures RecordedMinimum(originalLength, records) <= originalLength
    decreases |records|
  {
    if |records| == 0 then originalLength
    else if records[|records|-1].LengthIntent? then
      Min(RecordedMinimum(originalLength, records[..|records|-1]), records[|records|-1].length)
    else RecordedMinimum(originalLength, records[..|records|-1])
  }

  predicate OrderedTailRecords(originalLength: nat, records: seq<JournalRecord>)
    decreases |records|
  {
    |records| == 0 ||
      (OrderedTailRecords(originalLength, records[..|records|-1]) &&
        (records[|records|-1].LengthIntent? ==>
          Covered(records[..|records|-1], originalLength, records[|records|-1].length, originalLength)))
  }

  predicate ContentAdmitted(original: seq<Byte>, current: seq<Byte>, records: seq<JournalRecord>) {
    AdmittedByRecords(original, current, records,
      RecordedMinimum(|original|, records), RecoveryRecord in records)
  }

  lemma RecordedControlPrefix(originalLength: nat, records: seq<JournalRecord>, count: nat)
    requires count <= |records|
    ensures RecordedMinimum(originalLength, records) <= RecordedMinimum(originalLength, records[..count]) <= originalLength
    ensures OrderedTailRecords(originalLength, records) ==> OrderedTailRecords(originalLength, records[..count])
    decreases |records| - count
  {
    if count < |records| {
      RecordedControlPrefix(originalLength, records[..|records|-1], count);
      assert (records[..|records|-1])[..count] == records[..count];
      assert RecordedMinimum(originalLength, records) <= RecordedMinimum(originalLength, records[..|records|-1]);
      assert OrderedTailRecords(originalLength, records) ==> OrderedTailRecords(originalLength, records[..|records|-1]);
    } else {
      assert records[..count] == records;
    }
  }

  lemma OrderedTailRecordsCoverMinimum(original: seq<Byte>, records: seq<JournalRecord>)
    requires ExactRecords(original, records)
    requires OrderedTailRecords(|original|, records)
    ensures Covered(records, |original|, RecordedMinimum(|original|, records), |original|)
    decreases |records|
  {
    if |records| > 0 {
      var prefix := records[..|records|-1];
      OrderedTailRecordsCoverMinimum(original, prefix);
      SavedPrefixGrows(original, records, |records|-1);
      forall i: nat | RecordedMinimum(|original|, records) <= i < |original|
        ensures i in Saved(records)
      {
        if i < RecordedMinimum(|original|, prefix) {
          assert records[|records|-1].LengthIntent?;
          assert records[|records|-1].length <= i;
          assert i in Saved(prefix);
        }
      }
    }
  }

  lemma RecordedValuesGrow(records: seq<JournalRecord>, count: nat, offset: nat)
    requires count <= |records|
    ensures RecordedValues(records[..count], offset) <= RecordedValues(records, offset)
    decreases |records| - count
  {
    if count < |records| {
      RecordedValuesGrow(records[..|records|-1], count, offset);
      assert (records[..|records|-1])[..count] == records[..count];
      assert RecordedValues(records[..|records|-1], offset) <= RecordedValues(records, offset);
    } else {
      assert records[..count] == records;
    }
  }

  lemma RecordedPrefixRetainsContent(original: seq<Byte>, current: seq<Byte>, records: seq<JournalRecord>, count: nat)
    requires count <= |records|
    requires ContentAdmitted(original, current, records[..count])
    ensures ContentAdmitted(original, current, records)
  {
    var prefix := records[..count];
    RecordedControlPrefix(|original|, records, count);
    forall i: nat | i < |current|
      ensures FoldRecordedAdmission(original, records, RecordedMinimum(|original|, records),
        RecoveryRecord in records, i, current[i])
    {
      RecordFoldAdmitsExactlyRecordedValues(original, prefix, RecordedMinimum(|original|, prefix),
        RecoveryRecord in prefix, i, current[i]);
      RecordedValuesGrow(records, count, i);
      RecordFoldAdmitsExactlyRecordedValues(original, records, RecordedMinimum(|original|, records),
        RecoveryRecord in records, i, current[i]);
    }
  }

  lemma RecordedWriteAdmitsPayload(original: seq<Byte>, records: seq<JournalRecord>,
      start: nat, payload: seq<Byte>, offset: nat)
    requires WriteIntent(start, payload) in records
    requires start <= offset < start + |payload|
    ensures FoldRecordedAdmission(original, records, RecordedMinimum(|original|, records),
      RecoveryRecord in records, offset, payload[offset-start])
    decreases |records|
  {
    RecordedValuesContainWrite(records, start, payload, offset);
    RecordFoldAdmitsExactlyRecordedValues(original, records, RecordedMinimum(|original|, records),
      RecoveryRecord in records, offset, payload[offset-start]);
  }
  lemma RecordedValuesContainWrite(records: seq<JournalRecord>, start: nat, payload: seq<Byte>, offset: nat)
    requires WriteIntent(start, payload) in records
    requires start <= offset < start + |payload|
    ensures payload[offset-start] in RecordedValues(records, offset)
    decreases |records|
  {
    if records[|records|-1] != WriteIntent(start, payload) {
      RecordedValuesContainWrite(records[..|records|-1], start, payload, offset);
    }
  }

  // This refinement restricts the existing logical protocol to production's
  // truncation-only SetLength calls and explicitly authorized sparse gaps.
  // It does not assume that an arbitrary mutation result passes validation.
  predicate ContentProtocolInvariant(original: seq<Byte>, s: ProtocolState) {
    ProtocolInvariant(original, s) &&
    OrderedTailRecords(|original|, s.records) &&
    RecordedMinimum(|original|, DurableRecords(s)) <= |s.db| &&
    ContentAdmitted(original, s.db, DurableRecords(s))
  }

  predicate ContentProtocolEnabled(original: seq<Byte>, s: ProtocolState, a: ProtocolAction) {
    ProtocolEnabled(original, s, a) &&
    (match a
      case AppendLength(length) => Covered(s.records, |original|, length, |original|)
      case MutateWrite(start, payload, result) =>
        forall i: nat :: |s.db| <= i < start && i < |result| ==>
          FoldRecordedAdmission(original, DurableRecords(s), RecordedMinimum(|original|, DurableRecords(s)),
            RecoveryRecord in DurableRecords(s), i, 0)
      case MutateLength(length, resultLength) => length <= |s.db|
      case _ => true)
  }

  lemma InitialContentProtocolIsValid(original: seq<Byte>)
    ensures ContentProtocolInvariant(original, InitialProtocol(original))
  {
    InitialProtocolIsValid(original);
  }

  lemma ContentProtocolStepPreserves(original: seq<Byte>, s: ProtocolState, a: ProtocolAction)
    requires ContentProtocolInvariant(original, s)
    requires ContentProtocolEnabled(original, s, a)
    ensures ContentProtocolInvariant(original, ProtocolApply(original, s, a))
  {
    ProtocolStepPreserves(original, s, a);
    var next := ProtocolApply(original, s, a);
    var records := DurableRecords(s);
    var nextRecords := DurableRecords(next);
    RecordedControlPrefix(|original|, records, |records|);
    if !a.FlushJournal? && !a.FailJournalFlush? && !a.Crash? {
      assert nextRecords == records;
    } else {
      assert next.db == s.db;
      assert s.durableCount <= next.durableCount <= |s.records|;
      assert nextRecords == s.records[..next.durableCount];
      assert nextRecords[..s.durableCount] == records;
      RecordedPrefixRetainsContent(original, s.db, nextRecords, s.durableCount);
      RecordedControlPrefix(|original|, nextRecords, s.durableCount);
    }
    if !a.MutateWrite? && !a.MutateLength? && !a.ReplayResize? && !a.ReplayNext? {
      assert next.db == s.db;
      assert ContentAdmitted(original, next.db, nextRecords);
      assert RecordedMinimum(|original|, nextRecords) <= |next.db|;
    }
    match a {
      case FlushJournal =>
        RecordedPrefixRetainsContent(original, s.db, s.records, s.durableCount);
        RecordedControlPrefix(|original|, s.records, s.durableCount);
      case FailJournalFlush(through, partial) =>
        assert s.records[..through][..s.durableCount] == records;
        RecordedPrefixRetainsContent(original, s.db, s.records[..through], s.durableCount);
        RecordedControlPrefix(|original|, s.records[..through], s.durableCount);
      case Crash(through, partial) =>
        assert s.records[..through][..s.durableCount] == records;
        RecordedPrefixRetainsContent(original, s.db, s.records[..through], s.durableCount);
        RecordedControlPrefix(|original|, s.records, through);
        RecordedControlPrefix(|original|, s.records[..through], s.durableCount);
      case AppendBefore(offset) =>
        assert next.records[..|s.records|] == s.records;
      case AppendWrite(start, payload) =>
        assert next.records[..|s.records|] == s.records;
      case AppendLength(length) =>
        assert next.records[..|s.records|] == s.records;
      case AppendCommit =>
        assert next.records[..|s.records|] == s.records;
      case BeginUndo =>
        assert next.records[..|s.records|] == s.records;
      case MutateWrite(start, payload, result) =>
        forall i: nat | i < |result|
          ensures FoldRecordedAdmission(original, records, RecordedMinimum(|original|, records),
            RecoveryRecord in records, i, result[i])
        {
          if i < |s.db| && result[i] == s.db[i] {
          } else if start <= i < start + |payload| && result[i] == payload[i-start] {
            RecordedWriteAdmitsPayload(original, records, start, payload, i);
          } else {
            assert |s.db| <= i < start && result[i] == 0;
          }
        }
        assert ContentAdmitted(original, next.db, nextRecords);
        assert RecordedMinimum(|original|, nextRecords) <= |next.db|;
      case MutateLength(length, resultLength) =>
        assert next.db == s.db[..resultLength];
        RecordedLengthBoundsMinimum(|original|, records, length);
        assert ContentAdmitted(original, next.db, nextRecords);
        assert RecordedMinimum(|original|, nextRecords) <= |next.db|;
      case ReplayResize(length) =>
        assert RecoveryRecord in records;
        forall i: nat | i < |next.db|
          ensures FoldRecordedAdmission(original, records, RecordedMinimum(|original|, records),
            true, i, next.db[i])
        {
          if i >= |s.db| {
            assert next.db[i] == 0;
            RecordFoldAdmitsExactlyRecordedValues(original, records, RecordedMinimum(|original|, records), true, i, 0);
          }
        }
        assert ContentAdmitted(original, next.db, nextRecords);
        assert RecordedMinimum(|original|, nextRecords) <= |next.db|;
      case ReplayNext(written) =>
        forall i: nat | i < |next.db|
          ensures FoldRecordedAdmission(original, records, RecordedMinimum(|original|, records),
            RecoveryRecord in records, i, next.db[i])
        {
          if next.db[i] != s.db[i] {
            assert i == s.replayCursor;
            assert next.db[i] == original[i];
            RecordFoldAdmitsExactlyRecordedValues(original, records, RecordedMinimum(|original|, records),
              RecoveryRecord in records, i, next.db[i]);
          }
        }
        assert ContentAdmitted(original, next.db, nextRecords);
        assert RecordedMinimum(|original|, nextRecords) <= |next.db|;
      case _ =>
    }
    assert OrderedTailRecords(|original|, next.records);
    assert RecordedMinimum(|original|, nextRecords) <= |next.db|;
    assert ContentAdmitted(original, next.db, nextRecords);
  }

  lemma RecordedLengthBoundsMinimum(originalLength: nat, records: seq<JournalRecord>, length: nat)
    requires LengthIntent(length) in records
    ensures RecordedMinimum(originalLength, records) <= length
    decreases |records|
  {
    if records[|records|-1] != LengthIntent(length) {
      RecordedLengthBoundsMinimum(originalLength, records[..|records|-1], length);
    }
  }

  predicate ContentProtocolPath(original: seq<Byte>, states: seq<ProtocolState>, actions: seq<ProtocolAction>) {
    |states| == |actions| + 1 &&
    forall i | 0 <= i < |actions| :: ContentProtocolEnabled(original, states[i], actions[i]) &&
      states[i+1] == ProtocolApply(original, states[i], actions[i])
  }

  lemma ContentProtocolTracePreserves(original: seq<Byte>, states: seq<ProtocolState>, actions: seq<ProtocolAction>)
    requires ContentProtocolPath(original, states, actions)
    requires ContentProtocolInvariant(original, states[0])
    ensures ProtocolPath(original, states, actions)
    ensures ContentProtocolInvariant(original, states[|states|-1])
    ensures Recoverable(original, states[|states|-1].db, Saved(DurableRecords(states[|states|-1])))
    ensures ContentAdmitted(original, states[|states|-1].db, DurableRecords(states[|states|-1]))
    decreases |actions|
  {
    if |actions| > 0 {
      ContentProtocolStepPreserves(original, states[0], actions[0]);
      ContentProtocolTracePreserves(original, states[1..], actions[1..]);
    }
  }

  lemma AppendContentProtocolPath(original: seq<Byte>, states: seq<ProtocolState>, actions: seq<ProtocolAction>,
      action: ProtocolAction)
    requires ContentProtocolPath(original, states, actions)
    requires ContentProtocolEnabled(original, states[|states|-1], action)
    ensures ContentProtocolPath(original,
      states + [ProtocolApply(original, states[|states|-1], action)], actions + [action])
  {
    forall i | 0 <= i < |actions + [action]|
      ensures ContentProtocolEnabled(original, (states + [ProtocolApply(original, states[|states|-1], action)])[i],
        (actions + [action])[i]) &&
        (states + [ProtocolApply(original, states[|states|-1], action)])[i+1] ==
          ProtocolApply(original, (states + [ProtocolApply(original, states[|states|-1], action)])[i],
            (actions + [action])[i])
    { }
  }

  // Construct captures from readable uncaptured bytes, rather than supplying
  // exact before-images as an external premise. Arbitrarily many original
  // bytes are admitted; page serialization remains a separate refinement.
  lemma CaptureContentRange(original: seq<Byte>, s: ProtocolState, lo: nat, hi: nat)
    returns (next: ProtocolState, states: seq<ProtocolState>, actions: seq<ProtocolAction>)
    requires ContentProtocolInvariant(original, s) && s.mode == Active
    requires lo <= hi <= |original|
    ensures ContentProtocolPath(original, states, actions) && states[0] == s && states[|states|-1] == next
    ensures ContentProtocolInvariant(original, next) && next.mode == Active
    ensures next.db == s.db && DurableRecords(next) == DurableRecords(s)
    ensures |s.records| <= |next.records| && next.records[..|s.records|] == s.records
    ensures Covered(next.records, |original|, lo, hi)
    decreases hi - lo
  {
    if lo == hi {
      next, states, actions := s, [s], [];
    } else {
      var captured := s;
      if lo !in Saved(s.records) {
        SavedPrefixGrows(original, s.records, s.durableCount);
        assert lo !in Saved(DurableRecords(s));
        assert lo < |s.db|;
        var action := AppendBefore(lo);
        assert ContentProtocolEnabled(original, s, action);
        ContentProtocolStepPreserves(original, s, action);
        captured := ProtocolApply(original, s, action);
        AppendedRecordProperties(original, s.records, BeforeImage(lo, s.db[lo]));
      }
      var tailStates: seq<ProtocolState>;
      var tailActions: seq<ProtocolAction>;
      next, tailStates, tailActions := CaptureContentRange(original, captured, lo + 1, hi);
      if captured == s {
        states, actions := tailStates, tailActions;
      } else {
        states, actions := [s] + tailStates, [AppendBefore(lo)] + tailActions;
        forall i | 0 <= i < |actions|
          ensures ContentProtocolEnabled(original, states[i], actions[i]) &&
            states[i+1] == ProtocolApply(original, states[i], actions[i])
        {
          if i > 0 {
            assert states[i] == tailStates[i-1];
            assert actions[i] == tailActions[i-1];
          }
        }
      }
      SavedPrefixGrows(original, next.records, |captured.records|);
      assert lo in Saved(captured.records);
      assert lo in Saved(next.records);
      assert next.records[..|s.records|] == s.records;
    }
  }

  // Construct a useful admitted mutation for every finite payload and start.
  // Sparse regrowth first records its zero gap, as RecordBeforeWrite does.
  // The returned state admits every modeled torn write, without testing its
  // eventual result or borrowing recovery-marker authority while Active.
  lemma PrepareContentWrite(original: seq<Byte>, s: ProtocolState, start: nat, payload: seq<Byte>)
    returns (next: ProtocolState, states: seq<ProtocolState>, actions: seq<ProtocolAction>)
    requires ContentProtocolInvariant(original, s) && s.mode == Active
    ensures ContentProtocolPath(original, states, actions) && states[0] == s && states[|states|-1] == next
    ensures ContentProtocolInvariant(original, next) && next.mode == Active && next.db == s.db
    ensures forall result: seq<Byte> :: TornWrite(next.db, start, payload, result) ==>
      ContentProtocolEnabled(original, next, MutateWrite(start, payload, result))
  {
    next, states, actions := CaptureContentRange(original, s,
      Min(start, |original|), Min(start + |payload|, |original|));
    var capturedRecords := next.records;
    if |s.db| < start {
      var gap := AppendWrite(|s.db|, ZeroBytes(start - |s.db|));
      AppendedRecordProperties(original, next.records, WriteIntent(gap.start, gap.payload));
      AppendContentProtocolPath(original, states, actions, gap);
      ContentProtocolStepPreserves(original, next, gap);
      var beforeGap := next;
      next := ProtocolApply(original, next, gap);
      assert Saved(next.records) == Saved(beforeGap.records) == Saved(capturedRecords);
      states, actions := states + [next], actions + [gap];
    }
    var write := AppendWrite(start, payload);
    AppendedRecordProperties(original, next.records, WriteIntent(start, payload));
    AppendContentProtocolPath(original, states, actions, write);
    ContentProtocolStepPreserves(original, next, write);
    var beforeWrite := next;
    next := ProtocolApply(original, next, write);
    assert Saved(next.records) == Saved(beforeWrite.records) == Saved(capturedRecords);
    states, actions := states + [next], actions + [write];
    AppendedRecordProperties(original, states[|states|-2].records, WriteIntent(start, payload));
    AppendContentProtocolPath(original, states, actions, FlushJournal);
    ContentProtocolStepPreserves(original, next, FlushJournal);
    var preparedRecords := next.records;
    next := ProtocolApply(original, next, FlushJournal);
    assert DurableRecords(next) == preparedRecords;
    assert Saved(preparedRecords) == Saved(capturedRecords);
    states, actions := states + [next], actions + [FlushJournal];
    assert WriteIntent(start, payload) in DurableRecords(next);
    assert Saved(DurableRecords(next)) == Saved(capturedRecords);
    assert Covered(DurableRecords(next), |original|, start, start + |payload|);
    forall result: seq<Byte> | TornWrite(next.db, start, payload, result)
      ensures ContentProtocolEnabled(original, next, MutateWrite(start, payload, result))
    {
      forall i: nat | |next.db| <= i < start && i < |result|
        ensures FoldRecordedAdmission(original, DurableRecords(next), RecordedMinimum(|original|, DurableRecords(next)),
          RecoveryRecord in DurableRecords(next), i, 0)
      {
        assert |s.db| < start;
        assert WriteIntent(|s.db|, ZeroBytes(start - |s.db|)) in DurableRecords(next);
        RecordedWriteAdmitsPayload(original, DurableRecords(next), |s.db|, ZeroBytes(start - |s.db|), i);
      }
    }
  }

  lemma PrepareContentTruncation(original: seq<Byte>, s: ProtocolState, length: nat)
    returns (next: ProtocolState, states: seq<ProtocolState>, actions: seq<ProtocolAction>)
    requires ContentProtocolInvariant(original, s) && s.mode == Active && length <= |s.db|
    ensures ContentProtocolPath(original, states, actions) && states[0] == s && states[|states|-1] == next
    ensures ContentProtocolInvariant(original, next) && next.mode == Active && next.db == s.db
    ensures forall resultLength: nat :: length <= resultLength <= |next.db| ==>
      ContentProtocolEnabled(original, next, MutateLength(length, resultLength))
  {
    next, states, actions := CaptureContentRange(original, s, Min(length, |original|), |original|);
    var capturedRecords := next.records;
    var truncate := AppendLength(length);
    AppendedRecordProperties(original, next.records, LengthIntent(length));
    assert ContentProtocolEnabled(original, next, truncate);
    AppendContentProtocolPath(original, states, actions, truncate);
    ContentProtocolStepPreserves(original, next, truncate);
    var beforeTruncate := next;
    next := ProtocolApply(original, next, truncate);
    assert Saved(next.records) == Saved(beforeTruncate.records) == Saved(capturedRecords);
    states, actions := states + [next], actions + [truncate];
    AppendedRecordProperties(original, states[|states|-2].records, LengthIntent(length));
    AppendContentProtocolPath(original, states, actions, FlushJournal);
    ContentProtocolStepPreserves(original, next, FlushJournal);
    var preparedRecords := next.records;
    next := ProtocolApply(original, next, FlushJournal);
    assert DurableRecords(next) == preparedRecords;
    assert Saved(preparedRecords) == Saved(capturedRecords);
    states, actions := states + [next], actions + [FlushJournal];
    assert LengthIntent(length) in DurableRecords(next);
    assert Saved(DurableRecords(next)) == Saved(capturedRecords);
    assert Covered(DurableRecords(next), |original|, length, |original|);
  }

  // Lift the existing decreasing replay schedule through the content
  // refinement. Its zero-filled reextension is admitted only after the
  // complete durable marker, whose minimum comes from protected tail intents.
  // The abstract schedule still always publishes a marker; production's
  // conditional marker and actual replay loop remain correspondence work.
  lemma CompleteContentRecovery(original: seq<Byte>, s: ProtocolState)
    returns (states: seq<ProtocolState>, actions: seq<ProtocolAction>)
    requires ContentProtocolInvariant(original, s)
    requires s.mode == Inspecting || s.mode == Marking || s.mode == Replaying || s.mode == Finished
    ensures ContentProtocolPath(original, states, actions) && states[0] == s
    ensures ContentProtocolInvariant(original, states[|states|-1]) && states[|states|-1].mode == Finished
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
      var action := ProtocolRecoveryAction(original, s);
      assert ContentProtocolEnabled(original, s, action);
      ContentProtocolStepPreserves(original, s, action);
      var next := ProtocolApply(original, s, action);
      var tailStates, tailActions := CompleteContentRecovery(original, next);
      states, actions := [s] + tailStates, [action] + tailActions;
      forall i | 0 <= i < |actions|
        ensures ContentProtocolEnabled(original, states[i], actions[i]) &&
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

  // Content refinement of buffered effects. Payload values and length floors
  // come from durable complete records. Reordered persistence may expose a
  // zero-filled gap absent from the issuing operation's readable image. The
  // additional zero-exposure contract below is therefore explicit: it cannot
  // be inferred from write-time admission or undo coverage alone.
  predicate ContentEffect(original: seq<Byte>, records: seq<JournalRecord>, effect: DatabaseEffect) {
    match effect
      case BufferedWrite(start, payload) =>
        forall j: nat :: j < |payload| ==>
          FoldRecordedAdmission(original, records, RecordedMinimum(|original|, records),
            RecoveryRecord in records, start + j, payload[j])
      case BufferedLength(length) => RecordedMinimum(|original|, records) <= length
  }

  // Only newly exposed zeros are constrained, rather than assuming admission
  // of the complete persistence result. Torn payload bytes remain arbitrary
  // old/recorded mixtures under the existing EffectPersistence relation.
  predicate ZeroExposureAllowed(original: seq<Byte>, records: seq<JournalRecord>,
      beforeLength: nat, effect: DatabaseEffect, afterLength: nat) {
    match effect
      case BufferedWrite(start, payload) =>
        forall i: nat :: beforeLength <= i < start && i < afterLength ==>
          FoldRecordedAdmission(original, records, RecordedMinimum(|original|, records),
            RecoveryRecord in records, i, 0)
      case BufferedLength(length) =>
        forall i: nat :: beforeLength <= i < afterLength ==>
          FoldRecordedAdmission(original, records, RecordedMinimum(|original|, records),
            RecoveryRecord in records, i, 0)
  }

  lemma RecordedPrefixRetainsValue(original: seq<Byte>, records: seq<JournalRecord>, count: nat,
      offset: nat, value: Byte)
    requires count <= |records|
    requires FoldRecordedAdmission(original, records[..count], RecordedMinimum(|original|, records[..count]),
      RecoveryRecord in records[..count], offset, value)
    ensures FoldRecordedAdmission(original, records, RecordedMinimum(|original|, records),
      RecoveryRecord in records, offset, value)
  {
    RecordedControlPrefix(|original|, records, count);
    RecordFoldAdmitsExactlyRecordedValues(original, records[..count], RecordedMinimum(|original|, records[..count]),
      RecoveryRecord in records[..count], offset, value);
    RecordedValuesGrow(records, count, offset);
    RecordFoldAdmitsExactlyRecordedValues(original, records, RecordedMinimum(|original|, records),
      RecoveryRecord in records, offset, value);
  }

  lemma MoreRecordsAdmitEffect(original: seq<Byte>, records: seq<JournalRecord>, count: nat,
      effect: DatabaseEffect)
    requires count <= |records|
    requires ContentEffect(original, records[..count], effect)
    ensures ContentEffect(original, records, effect)
  {
    RecordedControlPrefix(|original|, records, count);
    match effect {
      case BufferedWrite(start, payload) =>
        forall j: nat | j < |payload|
          ensures FoldRecordedAdmission(original, records, RecordedMinimum(|original|, records),
            RecoveryRecord in records, start + j, payload[j])
        {
          RecordedPrefixRetainsValue(original, records, count, start + j, payload[j]);
        }
      case BufferedLength(length) =>
    }
  }

  lemma ContentEffectPersistencePreserves(original: seq<Byte>, records: seq<JournalRecord>,
      before: seq<Byte>, effect: DatabaseEffect, after: seq<Byte>)
    requires ContentAdmitted(original, before, records)
    requires RecordedMinimum(|original|, records) <= |before|
    requires ContentEffect(original, records, effect)
    requires EffectPersistence(before, effect, after)
    requires ZeroExposureAllowed(original, records, |before|, effect, |after|)
    ensures ContentAdmitted(original, after, records)
    ensures RecordedMinimum(|original|, records) <= |after|
  {
    forall i: nat | i < |after|
      ensures FoldRecordedAdmission(original, records, RecordedMinimum(|original|, records),
        RecoveryRecord in records, i, after[i])
    {
      match effect {
        case BufferedWrite(start, payload) =>
          if i < |before| && after[i] == before[i] {
          } else if start <= i < start + |payload| && after[i] == payload[i-start] {
            assert i-start < |payload|;
          } else {
            assert |before| <= i < start && after[i] == 0;
          }
        case BufferedLength(length) =>
          if i >= |before| {
            assert after[i] == 0;
          }
      }
    }
  }

  predicate ContentBufferedInvariant(original: seq<Byte>, b: BufferedState) {
    BufferedInvariant(original, b) && ContentProtocolInvariant(original, b.protocol) &&
    RecordedMinimum(|original|, DurableRecords(b.protocol)) <= |b.durableDb| &&
    ContentAdmitted(original, b.durableDb, DurableRecords(b.protocol)) &&
    forall j | 0 <= j < |b.pending| :: ContentEffect(original, DurableRecords(b.protocol), b.pending[j])
  }

  predicate ContentBufferedEnabled(original: seq<Byte>, b: BufferedState, a: BufferedAction) {
    b.protocol.durableCount <= |b.protocol.records| && BufferedEnabled(original, b, a) &&
    (match a
      case Issue(action) => ContentProtocolEnabled(original, b.protocol, action)
      case Persist(index, result) =>
        ZeroExposureAllowed(original, DurableRecords(b.protocol), |b.durableDb|, b.pending[index], |result|))
  }

  lemma InitialContentBufferedProtocolIsValid(original: seq<Byte>)
    ensures ContentBufferedInvariant(original, InitialBufferedProtocol(original))
  {
    InitialBufferedProtocolIsValid(original);
    InitialContentProtocolIsValid(original);
  }

  lemma ContentBufferedStepPreserves(original: seq<Byte>, b: BufferedState, a: BufferedAction)
    requires ContentBufferedInvariant(original, b)
    requires ContentBufferedEnabled(original, b, a)
    ensures ContentBufferedInvariant(original, BufferedApply(original, b, a))
  {
    BufferedStepPreserves(original, b, a);
    var s := b.protocol;
    var records := DurableRecords(s);
    var next := BufferedApply(original, b, a);
    var nextRecords := DurableRecords(next.protocol);
    match a {
      case Persist(index, result) =>
        ContentEffectPersistencePreserves(original, records, b.durableDb, b.pending[index], result);
      case Issue(action) =>
        ContentProtocolStepPreserves(original, s, action);
        var logical := ProtocolApply(original, s, action);
        assert nextRecords == DurableRecords(logical);
        if action.FlushJournal? || action.FailJournalFlush? || action.Crash? {
          assert s.durableCount <= logical.durableCount <= |s.records|;
          assert nextRecords == s.records[..logical.durableCount];
          assert nextRecords[..s.durableCount] == records;
          RecordedPrefixRetainsContent(original, b.durableDb, nextRecords, s.durableCount);
          RecordedControlPrefix(|original|, nextRecords, s.durableCount);
          if !action.Crash? {
            forall j | 0 <= j < |b.pending|
              ensures ContentEffect(original, nextRecords, b.pending[j])
            {
              MoreRecordsAdmitEffect(original, nextRecords, s.durableCount, b.pending[j]);
            }
          }
        } else {
          assert nextRecords == records;
        }
        if action.Crash? {
          assert next.protocol.db == b.durableDb;
          assert ContentProtocolInvariant(original, next.protocol);
        } else if action.FlushDatabase? {
          assert next.durableDb == logical.db;
        } else {
          var extra := QueuedEffects(s, action);
          match action {
            case MutateWrite(start, payload, result) =>
              forall j: nat | j < |payload|
                ensures FoldRecordedAdmission(original, records, RecordedMinimum(|original|, records),
                  RecoveryRecord in records, start + j, payload[j])
              {
                RecordedWriteAdmitsPayload(original, records, start, payload, start + j);
              }
            case MutateLength(length, resultLength) =>
              RecordedLengthBoundsMinimum(|original|, records, length);
            case ReplayResize(length) =>
              assert RecordedMinimum(|original|, records) <= length;
            case ReplayNext(written) =>
              if written && s.replayCursor in Saved(records) {
                SavedRecordsAreExact(original, records);
                assert Saved(records)[s.replayCursor] == original[s.replayCursor];
                RecordFoldAdmitsExactlyRecordedValues(original, records, RecordedMinimum(|original|, records),
                  RecoveryRecord in records, s.replayCursor, original[s.replayCursor]);
              }
            case _ =>
          }
          assert forall j | 0 <= j < |extra| :: ContentEffect(original, nextRecords, extra[j]);
          forall j | 0 <= j < |next.pending|
            ensures ContentEffect(original, nextRecords, next.pending[j])
          {
            if j < |b.pending| {
              assert next.pending[j] == b.pending[j];
            } else {
              assert next.pending[j] == extra[j-|b.pending|];
            }
          }
        }
    }
  }

  predicate ContentBufferedPath(original: seq<Byte>, states: seq<BufferedState>, actions: seq<BufferedAction>) {
    |states| == |actions| + 1 &&
    forall i | 0 <= i < |actions| :: ContentBufferedEnabled(original, states[i], actions[i]) &&
      states[i+1] == BufferedApply(original, states[i], actions[i])
  }

  lemma ContentBufferedTracePreserves(original: seq<Byte>, states: seq<BufferedState>, actions: seq<BufferedAction>)
    requires ContentBufferedPath(original, states, actions)
    requires ContentBufferedInvariant(original, states[0])
    ensures BufferedPath(original, states, actions)
    ensures ContentBufferedInvariant(original, states[|states|-1])
    ensures ContentAdmitted(original, states[|states|-1].protocol.db, DurableRecords(states[|states|-1].protocol))
    ensures ContentAdmitted(original, states[|states|-1].durableDb, DurableRecords(states[|states|-1].protocol))
    decreases |actions|
  {
    if |actions| > 0 {
      ContentBufferedStepPreserves(original, states[0], actions[0]);
      ContentBufferedTracePreserves(original, states[1..], actions[1..]);
    }
  }

  // Once a complete replay marker is durable, every zero exposure above the
  // record-derived minimum is admitted. Pending effects remain unrestricted
  // in index, order, repetition and torn payload choice within that contract.
  lemma DurableReplayMarkerAdmitsReorderedPersistence(original: seq<Byte>, b: BufferedState,
      index: nat, result: seq<Byte>)
    requires ContentBufferedInvariant(original, b)
    requires RecoveryRecord in DurableRecords(b.protocol)
    requires BufferedEnabled(original, b, Persist(index, result))
    ensures ContentBufferedEnabled(original, b, Persist(index, result))
  {
    var records := DurableRecords(b.protocol);
    forall i: nat | |b.durableDb| <= i
      ensures FoldRecordedAdmission(original, records, RecordedMinimum(|original|, records), true, i, 0)
    {
      RecordFoldAdmitsExactlyRecordedValues(original, records, RecordedMinimum(|original|, records), true, i, 0);
    }
  }

  // Crash quiescence drops every old pending effect. A restarted recovery
  // reads the admitted durable image; no older write can reappear afterwards.
  lemma ContentCrashQuiescesPendingEffects(original: seq<Byte>, b: BufferedState, through: nat, partial: bool)
    requires ContentBufferedInvariant(original, b)
    requires ContentBufferedEnabled(original, b, Issue(Crash(through, partial)))
    ensures ContentBufferedInvariant(original, BufferedApply(original, b, Issue(Crash(through, partial))))
    ensures BufferedApply(original, b, Issue(Crash(through, partial))).protocol.db == b.durableDb
    ensures |BufferedApply(original, b, Issue(Crash(through, partial))).pending| == 0
    ensures forall index: nat, result: seq<Byte> ::
      !BufferedEnabled(original, BufferedApply(original, b, Issue(Crash(through, partial))), Persist(index, result))
  {
    ContentBufferedStepPreserves(original, b, Issue(Crash(through, partial)));
  }

  lemma CompleteContentBufferedRecovery(original: seq<Byte>, b: BufferedState)
    returns (states: seq<BufferedState>, actions: seq<BufferedAction>)
    requires ContentBufferedInvariant(original, b)
    requires b.protocol.mode == Inspecting || b.protocol.mode == Marking ||
      b.protocol.mode == Replaying || b.protocol.mode == Finished
    ensures ContentBufferedPath(original, states, actions) && states[0] == b
    ensures ContentBufferedInvariant(original, states[|states|-1])
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
      var action := ProtocolRecoveryAction(original, b.protocol);
      assert ContentProtocolEnabled(original, b.protocol, action);
      var a := Issue(action);
      assert ContentBufferedEnabled(original, b, a);
      ContentBufferedStepPreserves(original, b, a);
      var next := BufferedApply(original, b, a);
      assert next.protocol == ProtocolApply(original, b.protocol, action);
      var tailStates, tailActions := CompleteContentBufferedRecovery(original, next);
      states, actions := [b] + tailStates, [a] + tailActions;
      forall i | 0 <= i < |actions|
        ensures ContentBufferedEnabled(original, states[i], actions[i]) &&
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

  // Undo coverage alone permits a pre-marker zero-filled gap that content
  // validation correctly refuses. Retrying an old write after truncation is
  // not a new logical write with an automatically recorded sparse-gap intent.
  lemma UnadmittedByteRefusesContent(original: seq<Byte>, current: seq<Byte>, records: seq<JournalRecord>, offset: nat)
    requires offset < |current|
    requires !FoldRecordedAdmission(original, records, RecordedMinimum(|original|, records),
      RecoveryRecord in records, offset, current[offset])
    ensures !ContentAdmitted(original, current, records)
  {
    if ContentAdmitted(original, current, records) {
      assert FoldRecordedAdmission(original, records, RecordedMinimum(|original|, records),
        RecoveryRecord in records, offset, current[offset]);
    }
  }

  lemma ReorderedWriteCanExposeUnrecordedZeros()
    ensures EffectPersistence([1], BufferedWrite(2, [4]), [1, 0, 4])
    ensures ContentAdmitted([1, 2, 3], [1], [WriteIntent(2, [4]), LengthIntent(1)])
    ensures !ContentAdmitted([1, 2, 3], [1, 0, 4], [WriteIntent(2, [4]), LengthIntent(1)])
    ensures !ZeroExposureAllowed([1, 2, 3], [WriteIntent(2, [4]), LengthIntent(1)],
      1, BufferedWrite(2, [4]), 3)
  {
    var records := [WriteIntent(2, [4]), LengthIntent(1)];
    var current: seq<Byte> := [1, 0, 4];
    assert RecordedMinimum(3, records) == 1;
    assert !FoldRecordedAdmission([1, 2, 3], records, 1, false, 1, 0);
    UnadmittedByteRefusesContent([1, 2, 3], current, records, 1);
  }

  lemma PreparedReorderingCounterexample()
    returns (b: BufferedState)
    ensures ContentBufferedInvariant([1, 2, 3], b)
    ensures b.protocol.records == [BeforeImage(1, 2), BeforeImage(2, 3), WriteIntent(2, [4]), LengthIntent(1)]
    ensures b.protocol.durableCount == 4 && b.protocol.db == [1] && b.durableDb == [1]
    ensures b.protocol.mode == Active
    ensures b.pending == [BufferedWrite(2, [4]), BufferedLength(1)]
  {
    var original: seq<Byte> := [1, 2, 3];
    var first: seq<JournalRecord> := [BeforeImage(1, 2)];
    var second := first + [BeforeImage(2, 3)];
    var third := second + [WriteIntent(2, [4])];
    var records := third + [LengthIntent(1)];
    AppendedRecordProperties(original, [], BeforeImage(1, 2));
    AppendedRecordProperties(original, first, BeforeImage(2, 3));
    AppendedRecordProperties(original, second, WriteIntent(2, [4]));
    AppendedRecordProperties(original, third, LengthIntent(1));
    assert Saved(records) == map[1 := 2, 2 := 3];
    assert Covered(third, 3, 1, 3);
    assert OrderedTailRecords(3, first);
    assert OrderedTailRecords(3, second);
    assert OrderedTailRecords(3, third);
    assert OrderedTailRecords(3, records);
    assert RecordedMinimum(3, first) == 3;
    assert RecordedMinimum(3, second) == 3;
    assert RecordedMinimum(3, third) == 3;
    assert RecordedMinimum(3, records) == 1;
    var protocol := ProtocolState([1], records, 4, false, Active, false, false, false, 0);
    b := BufferedState(protocol, [1], [BufferedWrite(2, [4]), BufferedLength(1)]);
    assert DurableRecords(protocol) == records;
    SavedRecordsAreExact(original, records);
    RecordedWriteAdmitsPayload(original, records, 2, [4], 2);
    RecordFoldAdmitsExactlyRecordedValues(original, records, 1, false, 0, 1);
    assert ContentAdmitted(original, [1], records);
    assert ContentEffect(original, records, BufferedWrite(2, [4]));
    assert ContentBufferedInvariant(original, b);
  }

  lemma ReorderingCounterexampleRefuses(records: seq<JournalRecord>)
    requires records == [BeforeImage(1, 2), BeforeImage(2, 3), WriteIntent(2, [4]), LengthIntent(1)]
    ensures RecordedMinimum(3, records) == 1 && RecoveryRecord !in records
    ensures !FoldRecordedAdmission([1, 2, 3], records, 1, false, 1, 0)
    ensures !ContentAdmitted([1, 2, 3], [1, 0, 4], records)
  {
    assert (records[..3])[2] == WriteIntent(2, [4]);
    assert (records[..3])[..2] == records[..2];
    assert RecordedMinimum(3, records[..1]) == 3;
    assert RecordedMinimum(3, records[..2]) == 3;
    assert RecordedMinimum(3, records[..3]) == RecordedMinimum(3, (records[..3])[..2]);
    assert RecordedMinimum(3, records[..3]) == 3;
    assert RecordedMinimum(3, records) == 1;
    assert RecordedValues(records[..1], 1) == {};
    assert RecordedValues(records[..2], 1) == {};
    assert RecordedValues(records[..3], 1) == RecordedValues((records[..3])[..2], 1);
    assert RecordedValues(records[..3], 1) == {};
    assert RecordedValues(records, 1) == {};
    RecordFoldAdmitsExactlyRecordedValues([1, 2, 3], records, 1, false, 1, 0);
    UnadmittedByteRefusesContent([1, 2, 3], [1, 0, 4], records, 1);
  }

  lemma ReorderedWriteCanLeaveRefusingCrashSurvivor()
    returns (states: seq<BufferedState>, actions: seq<BufferedAction>)
    ensures BufferedPath([1, 2, 3], states, actions) && |states| == 3
    ensures ContentBufferedInvariant([1, 2, 3], states[0])
    ensures BufferedInvariant([1, 2, 3], states[2])
    ensures states[2].protocol.mode == Inspecting && states[2].protocol.db == [1, 0, 4]
    ensures |states[2].pending| == 0
    ensures !ContentAdmitted([1, 2, 3], states[2].protocol.db, DurableRecords(states[2].protocol))
    ensures !ContentBufferedEnabled([1, 2, 3], states[0], actions[0])
  {
    var original: seq<Byte> := [1, 2, 3];
    var b := PreparedReorderingCounterexample();
    var records := b.protocol.records;
    assert DurableRecords(b.protocol) == records;
    ReorderingCounterexampleRefuses(records);
    var persist := Persist(0, [1, 0, 4]);
    if ZeroExposureAllowed(original, records, 1, BufferedWrite(2, [4]), 3) {
      assert FoldRecordedAdmission(original, records, 1, false, 1, 0);
      assert false;
    }
    assert !ZeroExposureAllowed(original, DurableRecords(b.protocol), |b.durableDb|, b.pending[0], 3);
    assert !ContentBufferedEnabled(original, b, persist);
    assert BufferedEnabled(original, b, persist);
    BufferedStepPreserves(original, b, persist);
    var persisted := BufferedApply(original, b, persist);
    var crash := Issue(Crash(4, false));
    BufferedStepPreserves(original, persisted, crash);
    var survivor := BufferedApply(original, persisted, crash);
    assert DurableRecords(survivor.protocol) == records;
    states, actions := [b, persisted, survivor], [persist, crash];
  }

  // Production-order replay control. Page payload acquisition, exact saved-map
  // coverage and successful I/O are separate contracts. Unlike the earlier
  // byte-wise protocol schedule, this control writes captured pages first,
  // then restores length, flushes and publishes the completion decision. Its
  // marker is conditional on an original tail having been removed.
  datatype PageReplayControl = PageReplayControl(minimumLength: nat,
    recovering: bool, remainingPages: nat, phase: nat)

  predicate PageReplayNeedsMarker(originalLength: nat, c: PageReplayControl) {
    c.minimumLength < originalLength && !c.recovering
  }

  predicate ValidPageReplayControl(originalLength: nat, c: PageReplayControl) {
    c.minimumLength <= originalLength && c.phase <= 3 &&
    (c.phase > 0 ==> c.remainingPages == 0 && !PageReplayNeedsMarker(originalLength, c))
  }

  // 0 refuses/finishes; 1 marker; 2 page; 3 resize; 4 flush; 5 decision.
  function PageReplayAction(originalLength: nat, c: PageReplayControl): nat {
    if !ValidPageReplayControl(originalLength, c) || c.phase == 3 then 0
    else if c.phase == 0 then
      (if PageReplayNeedsMarker(originalLength, c) then 1
       else if c.remainingPages > 0 then 2 else 3)
    else if c.phase == 1 then 4 else 5
  }

  function SuccessfulPageReplayControl(originalLength: nat, c: PageReplayControl): PageReplayControl
    requires PageReplayAction(originalLength, c) != 0
  {
    var action := PageReplayAction(originalLength, c);
    if action == 1 then c.(recovering := true)
    else if action == 2 then c.(remainingPages := c.remainingPages - 1)
    else c.(phase := c.phase + 1)
  }

  function PageReplayRank(originalLength: nat, c: PageReplayControl): nat
    requires ValidPageReplayControl(originalLength, c)
  {
    if c.phase == 0 then c.remainingPages + 3 + (if PageReplayNeedsMarker(originalLength, c) then 1 else 0)
    else 3 - c.phase
  }

  lemma SuccessfulPageReplayControlMakesProgress(originalLength: nat, c: PageReplayControl)
    requires ValidPageReplayControl(originalLength, c) && c.phase < 3
    ensures 1 <= PageReplayAction(originalLength, c) <= 5
    ensures ValidPageReplayControl(originalLength, SuccessfulPageReplayControl(originalLength, c))
    ensures PageReplayRank(originalLength, SuccessfulPageReplayControl(originalLength, c)) ==
      PageReplayRank(originalLength, c) - 1
    ensures PageReplayAction(originalLength, c) == 1 <==> PageReplayNeedsMarker(originalLength, c)
    ensures PageReplayAction(originalLength, c) >= 2 ==> !PageReplayNeedsMarker(originalLength, c)
    ensures PageReplayAction(originalLength, c) >= 3 ==> c.remainingPages == 0
    ensures PageReplayAction(originalLength, c) == 5 ==> c.phase == 2
  { }

  predicate PageReplayControlPath(originalLength: nat,
      states: seq<PageReplayControl>, actions: seq<nat>) {
    |states| == |actions| + 1 &&
    (forall i | 0 <= i < |states| :: ValidPageReplayControl(originalLength, states[i])) &&
    forall i | 0 <= i < |actions| ::
      actions[i] != 0 && actions[i] == PageReplayAction(originalLength, states[i]) &&
      states[i+1] == SuccessfulPageReplayControl(originalLength, states[i])
  }

  // Constructive termination assumes every selected effect succeeds; it does
  // not promise termination during infinitely many I/O failures. A page action
  // consumes one exact map entry only after its write returns successfully.
  lemma CompletePageReplayControl(originalLength: nat, c: PageReplayControl)
    returns (states: seq<PageReplayControl>, actions: seq<nat>)
    requires ValidPageReplayControl(originalLength, c)
    ensures PageReplayControlPath(originalLength, states, actions)
    ensures states[0] == c && states[|states|-1].phase == 3
    ensures |actions| == PageReplayRank(originalLength, c)
    decreases PageReplayRank(originalLength, c)
  {
    if c.phase == 3 {
      states, actions := [c], [];
    } else {
      SuccessfulPageReplayControlMakesProgress(originalLength, c);
      var next := SuccessfulPageReplayControl(originalLength, c);
      var tailStates, tailActions := CompletePageReplayControl(originalLength, next);
      states, actions := [c] + tailStates, [PageReplayAction(originalLength, c)] + tailActions;
      forall i | 0 <= i < |actions|
        ensures actions[i] != 0 && actions[i] == PageReplayAction(originalLength, states[i]) &&
          states[i+1] == SuccessfulPageReplayControl(originalLength, states[i])
      {
        if i > 0 {
          assert states[i] == tailStates[i-1];
          assert actions[i] == tailActions[i-1];
        }
      }
    }
  }

  // Failure consumes no in-memory control step; reacquisition restarts all
  // captures. A marker whose append reported failure may nevertheless survive.
  // Existing exact evidence and parser admission remain entry contracts.
  predicate InterruptedPageReplayControl(originalLength: nat, captures: nat,
      before: PageReplayControl, after: PageReplayControl) {
    after == before ||
    after == PageReplayControl(before.minimumLength, before.recovering, captures, 0) ||
    (before.minimumLength < originalLength &&
      after == PageReplayControl(before.minimumLength, true, captures, 0))
  }

  lemma InterruptedPageReplayControlRemainsAdmitted(originalLength: nat, captures: nat,
      before: PageReplayControl, after: PageReplayControl)
    requires ValidPageReplayControl(originalLength, before)
    requires InterruptedPageReplayControl(originalLength, captures, before, after)
    ensures ValidPageReplayControl(originalLength, after)
  { }

  // The control cannot establish payload identity or physical write semantics.
  // Given the existing replay-effect contract, even a torn selected page keeps
  // all exact undo evidence usable, and a selected resize preserves it too.
  lemma SelectedPageReplayEffectPreservesRecoverability(original: seq<Byte>,
      before: seq<Byte>, after: seq<Byte>, saved: map<nat, Byte>, c: PageReplayControl)
    requires ValidPageReplayControl(|original|, c)
    requires PageReplayAction(|original|, c) == 2
    requires Recoverable(original, before, saved)
    requires ReplayMutation(original, before, after, saved)
    ensures Recoverable(original, after, saved)
    ensures !PageReplayNeedsMarker(|original|, c)
  {
    InterruptedReplayPreservesRecoverability(original, before, after, saved);
  }

  lemma SelectedPageReplayResizePreservesRecoverability(original: seq<Byte>,
      current: seq<Byte>, saved: map<nat, Byte>, c: PageReplayControl)
    requires ValidPageReplayControl(|original|, c)
    requires PageReplayAction(|original|, c) == 3
    requires Recoverable(original, current, saved)
    ensures Recoverable(original, Resize(current, |original|), saved)
    ensures c.remainingPages == 0 && !PageReplayNeedsMarker(|original|, c)
  {
    RestoreLengthPreservesRecoverability(original, current, saved);
  }

  // Recovery hashes and copies only the original-page prefix. Bytes beyond
  // original EOF remain checksum-covered record padding, but need not be zero
  // to admit replay and must never enter the restored image.
  opaque predicate ExactReplayPagePayload(original: seq<Byte>, pageSize: nat, page: PageCapture) {
    0 < pageSize && page.pageOffset % pageSize == 0 && page.pageOffset < |original| &&
    |page.pageBytes| == pageSize &&
    forall i | 0 <= i < pageSize && page.pageOffset + i < |original| ::
      page.pageBytes[i] == original[page.pageOffset + i]
  }

  lemma ConstructedPagePayloadAdmitsSerializedReplay(original: seq<Byte>, pageSize: nat,
      page: PageCapture)
    requires ExactPagePayload(original, pageSize, page)
    ensures ExactReplayPagePayload(original, pageSize, page)
  {
    reveal ExactPagePayload();
    reveal ExactReplayPagePayload();
  }

  // Serialized capture inventory refinement. Each item denotes a complete,
  // checksum-admitted original-page record selected by the bounded cursor;
  // its position is the actual record source, not an enumeration ordinal.
  // ExactReplayPagePayload is the manifest/hash-to-original identity contract.
  datatype SerializedPage = SerializedPage(recordPosition: nat, page: PageCapture)

  predicate ExactSerializedPages(original: seq<Byte>, pageSize: nat,
      pages: seq<SerializedPage>) {
    0 < pageSize &&
    (forall i | 0 <= i < |pages| :: ExactReplayPagePayload(original, pageSize, pages[i].page)) &&
    (forall i, j | 0 <= i < j < |pages| ::
      pages[i].recordPosition < pages[j].recordPosition &&
      pages[i].page.pageOffset != pages[j].page.pageOffset)
  }

  function SerializedPageInventory(pages: seq<SerializedPage>): map<nat, nat> {
    if |pages| == 0 then map[]
    else SerializedPageInventory(pages[1..])[pages[0].page.pageOffset := pages[0].recordPosition]
  }

  lemma SerializedInventoryContainsExactlyPages(pages: seq<SerializedPage>)
    ensures forall offset: nat :: offset in SerializedPageInventory(pages) <==>
      exists i | 0 <= i < |pages| :: pages[i].page.pageOffset == offset
    decreases |pages|
  {
    if |pages| > 0 {
      SerializedInventoryContainsExactlyPages(pages[1..]);
      forall offset: nat
        ensures offset in SerializedPageInventory(pages) <==>
          exists i | 0 <= i < |pages| :: pages[i].page.pageOffset == offset
      {
        if offset in SerializedPageInventory(pages[1..]) {
          var i :| 0 <= i < |pages[1..]| && pages[1..][i].page.pageOffset == offset;
          assert pages[i+1].page.pageOffset == offset;
        }
        if exists i | 0 <= i < |pages| :: pages[i].page.pageOffset == offset {
          var i :| 0 <= i < |pages| && pages[i].page.pageOffset == offset;
          if i > 0 {
            assert pages[1..][i-1].page.pageOffset == offset;
          }
        }
      }
    }
  }

  lemma ExactSerializedInventoryHasExactSources(original: seq<Byte>, pageSize: nat,
      pages: seq<SerializedPage>)
    requires ExactSerializedPages(original, pageSize, pages)
    ensures |SerializedPageInventory(pages)| == |pages|
    ensures forall i | 0 <= i < |pages| ::
      pages[i].page.pageOffset in SerializedPageInventory(pages) &&
      SerializedPageInventory(pages)[pages[i].page.pageOffset] == pages[i].recordPosition
    decreases |pages|
  {
    SerializedInventoryContainsExactlyPages(pages);
    if |pages| > 0 {
      assert ExactSerializedPages(original, pageSize, pages[1..]);
      ExactSerializedInventoryHasExactSources(original, pageSize, pages[1..]);
      SerializedInventoryContainsExactlyPages(pages[1..]);
      assert pages[0].page.pageOffset !in SerializedPageInventory(pages[1..]);
      forall i | 0 <= i < |pages|
        ensures pages[i].page.pageOffset in SerializedPageInventory(pages) &&
          SerializedPageInventory(pages)[pages[i].page.pageOffset] == pages[i].recordPosition
      {
        if i > 0 {
          assert pages[1..][i-1] == pages[i];
        }
      }
    }
  }

  // The production preflight checks each serialized source against the
  // supplied dictionary, then requires exact exhaustion of its Count. The
  // complete source-position identity rejects duplicate original-page keys.
  lemma SerializedInventoryPreflightEstablishesEquality(original: seq<Byte>, pageSize: nat,
      pages: seq<SerializedPage>, inventory: map<nat, nat>)
    requires ExactSerializedPages(original, pageSize, pages)
    requires |inventory| == |pages|
    requires forall i | 0 <= i < |pages| ::
      pages[i].page.pageOffset in inventory &&
      inventory[pages[i].page.pageOffset] == pages[i].recordPosition
    ensures inventory == SerializedPageInventory(pages)
  {
    ExactSerializedInventoryHasExactSources(original, pageSize, pages);
    SerializedInventoryContainsExactlyPages(pages);
    assert SerializedPageInventory(pages).Keys <= inventory.Keys;
    assert |SerializedPageInventory(pages).Keys| == |SerializedPageInventory(pages)|;
    assert |inventory.Keys| == |inventory|;
    assert inventory.Keys == SerializedPageInventory(pages).Keys + (inventory.Keys - SerializedPageInventory(pages).Keys);
    assert SerializedPageInventory(pages).Keys * (inventory.Keys - SerializedPageInventory(pages).Keys) == {};
    assert |inventory.Keys - SerializedPageInventory(pages).Keys| == 0;
    assert SerializedPageInventory(pages).Keys == inventory.Keys;
    assert forall offset | offset in inventory ::
      SerializedPageInventory(pages)[offset] == inventory[offset];
  }

  function SerializedSaved(original: seq<Byte>, pageSize: nat,
      pages: seq<SerializedPage>): map<nat, Byte> {
    map offset: nat | offset < |original| &&
      (exists i | 0 <= i < |pages| ::
        pages[i].page.pageOffset <= offset < pages[i].page.pageOffset + pageSize) :: original[offset]
  }

  lemma SerializedSavedIsExact(original: seq<Byte>, pageSize: nat,
      pages: seq<SerializedPage>)
    ensures ValidUndo(original, SerializedSaved(original, pageSize, pages))
  { }

  // A successful page write has this exact bytes-and-length effect. Real I/O
  // and cryptographic identity implement explicit contracts; neither is made
  // a trusted external declaration. Failed/torn effects retain the full saved
  // evidence and satisfy the existing ReplayMutation contract separately.
  function ApplySerializedPage(original: seq<Byte>, current: seq<Byte>, pageSize: nat,
      page: PageCapture): seq<Byte>
    requires ExactReplayPagePayload(original, pageSize, page)
  {
    reveal ExactReplayPagePayload();
    var end := Min(|original|, page.pageOffset + pageSize);
    seq((if |current| < end then end else |current|), i =>
      if page.pageOffset <= i < end then page.pageBytes[i-page.pageOffset]
      else if 0 <= i < |current| then current[i] else 0)
  }

  lemma SerializedPageSuccessConsumesExactSavedRange(original: seq<Byte>, current: seq<Byte>,
      pageSize: nat, pages: seq<SerializedPage>)
    requires ExactSerializedPages(original, pageSize, pages) && |pages| > 0
    requires Recoverable(original, current, SerializedSaved(original, pageSize, pages))
    ensures Recoverable(original, ApplySerializedPage(original, current, pageSize, pages[0].page),
      SerializedSaved(original, pageSize, pages[1..]))
  {
    reveal ExactReplayPagePayload();
    SerializedSavedIsExact(original, pageSize, pages[1..]);
    var next := ApplySerializedPage(original, current, pageSize, pages[0].page);
    var page := pages[0].page;
    forall offset: nat | offset < |original| && offset !in SerializedSaved(original, pageSize, pages[1..])
      ensures offset < |next| && next[offset] == original[offset]
    {
      if page.pageOffset <= offset < page.pageOffset + pageSize {
        assert offset < Min(|original|, page.pageOffset + pageSize);
        assert page.pageBytes[offset-page.pageOffset] == original[offset];
      } else {
        assert !(exists i | 0 <= i < |pages| ::
          pages[i].page.pageOffset <= offset < pages[i].page.pageOffset + pageSize) by {
          if exists i | 0 <= i < |pages| ::
              pages[i].page.pageOffset <= offset < pages[i].page.pageOffset + pageSize {
            var i :| 0 <= i < |pages| &&
              pages[i].page.pageOffset <= offset < pages[i].page.pageOffset + pageSize;
            if i > 0 {
              assert pages[1..][i-1] == pages[i];
            }
          }
        }
        assert offset !in SerializedSaved(original, pageSize, pages);
      }
    }
  }

  // A serialized cursor consumes each capture exactly once. Its suffix map is
  // the remaining control inventory, so every successful page action lowers
  // the existing production-order rank by one and cannot consume a different
  // dictionary entry or the zero padding after a partial final original page.
  lemma SerializedPageSelectionConsumesInventory(original: seq<Byte>, pageSize: nat,
      pages: seq<SerializedPage>, c: PageReplayControl)
    requires ExactSerializedPages(original, pageSize, pages) && |pages| > 0
    requires ValidPageReplayControl(|original|, c)
    requires c.remainingPages == |SerializedPageInventory(pages)|
    requires PageReplayAction(|original|, c) == 2
    ensures SerializedPageInventory(pages) - {pages[0].page.pageOffset} ==
      SerializedPageInventory(pages[1..])
    ensures SuccessfulPageReplayControl(|original|, c).remainingPages ==
      |SerializedPageInventory(pages[1..])|
    ensures PageReplayRank(|original|, SuccessfulPageReplayControl(|original|, c)) ==
      PageReplayRank(|original|, c) - 1
  {
    ExactSerializedInventoryHasExactSources(original, pageSize, pages);
    assert ExactSerializedPages(original, pageSize, pages[1..]);
    ExactSerializedInventoryHasExactSources(original, pageSize, pages[1..]);
    SerializedInventoryContainsExactlyPages(pages[1..]);
    assert pages[0].page.pageOffset !in SerializedPageInventory(pages[1..]);
    SuccessfulPageReplayControlMakesProgress(|original|, c);
  }

  lemma CompleteSerializedPageReplay(original: seq<Byte>, current: seq<Byte>,
      pageSize: nat, pages: seq<SerializedPage>) returns (restored: seq<Byte>)
    requires ExactSerializedPages(original, pageSize, pages)
    requires Recoverable(original, current, SerializedSaved(original, pageSize, pages))
    ensures restored == original
    decreases |pages|
  {
    if |pages| == 0 {
      restored := Resize(current, |original|);
      assert SerializedSaved(original, pageSize, pages) == map[];
    } else {
      SerializedPageSuccessConsumesExactSavedRange(original, current, pageSize, pages);
      assert ExactSerializedPages(original, pageSize, pages[1..]);
      restored := CompleteSerializedPageReplay(original,
        ApplySerializedPage(original, current, pageSize, pages[0].page), pageSize, pages[1..]);
    }
  }
}
