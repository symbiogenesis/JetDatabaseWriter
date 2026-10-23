// Verification-only abstraction of EncryptionReplacementIntent. No code is extracted.
// Images are arbitrary integer sequences; equality represents validated complete
// bytes, not a proof that SHA-256 collisions or malicious edits are impossible.
// The transition model starts after a valid Prepared header and exclusive
// generation ownership. The separate byte classifier checks bounded framing and
// fields against a supplied digest and path identity; it does not establish leases.
// Stable names, immutable copies and exclusive access remain storage premises.
// Lost discovery names, OS durability, hashing and C# refinement remain unproved.
module ReplacementProtocol {
  datatype Image = Missing | Present(bytes: seq<int>)
  datatype Decision = Prepared | PartialCommit | Committed
  datatype Snapshot = Snapshot(
    decision: Decision, destination: Image, oldCopy: bool, newCopy: bool,
    publication: Image, intent: bool)
  datatype Action = Stage(useNew: bool, count: nat) | Publish(useNew: bool)
    | RemovePublication | RemoveOld | RemoveNew | RemoveIntent
    | StartDecision | FinishDecision | MissingDestination | Interrupt

  function Selected(original: seq<int>, next: seq<int>, s: Snapshot): seq<int> {
    if s.decision == Committed then next else original
  }

  function Chosen(original: seq<int>, next: seq<int>, useNew: bool): seq<int> {
    if useNew then next else original
  }

  predicate IsPrefix(part: seq<int>, whole: seq<int>) {
    |part| <= |whole| && part == whole[..|part|]
  }

  predicate Authorized(original: seq<int>, next: seq<int>, image: Image) {
    image == Missing || image == Present(original) || image == Present(next)
  }

  // Full publication images are validated against the recorded complete hash.
  // A partial image needs an actually surviving complete byte witness, matching
  // ValidatePublication: an immutable generation copy or the canonical image.
  predicate PublicationIndependentOfDestination(original: seq<int>, next: seq<int>, s: Snapshot) {
    s.publication == Missing || s.publication == Present(original) ||
    s.publication == Present(next) ||
    (s.oldCopy && IsPrefix(s.publication.bytes, original)) ||
    (s.newCopy && IsPrefix(s.publication.bytes, next))
  }

  predicate PublicationAuthorized(original: seq<int>, next: seq<int>, s: Snapshot) {
    PublicationIndependentOfDestination(original, next, s) ||
    (s.publication.Present? && s.destination.Present? &&
      IsPrefix(s.publication.bytes, s.destination.bytes))
  }

  predicate Valid(original: seq<int>, next: seq<int>, s: Snapshot) {
    Authorized(original, next, s.destination) &&
    PublicationAuthorized(original, next, s) &&
    (if s.intent then
      s.destination == Present(Selected(original, next, s)) ||
      (if s.decision == Committed then s.newCopy else s.oldCopy)
    else
      s.destination == Present(Selected(original, next, s)) &&
      !s.oldCopy && !s.newCopy && s.publication == Missing)
  }

  predicate Enabled(original: seq<int>, next: seq<int>, s: Snapshot, a: Action) {
    match a
      case Stage(useNew, count) => s.intent &&
        (if useNew then s.newCopy else s.oldCopy) &&
        count <= |Chosen(original, next, useNew)|
      case Publish(useNew) => s.intent &&
        (if useNew then s.newCopy else s.oldCopy) &&
        s.publication == Present(Chosen(original, next, useNew)) &&
        // Publishing a live new image before its decision must retain original undo.
        (Chosen(original, next, useNew) == Selected(original, next, s) ||
          (if s.decision == Committed then s.newCopy else s.oldCopy))
      case RemovePublication => s.intent && s.destination == Present(Selected(original, next, s))
      case RemoveOld => s.intent && s.destination == Present(Selected(original, next, s)) &&
        s.publication == Missing
      case RemoveNew => s.intent && s.destination == Present(Selected(original, next, s)) &&
        s.publication == Missing
      case RemoveIntent => s.intent && s.destination == Present(Selected(original, next, s)) &&
        !s.oldCopy && !s.newCopy && s.publication == Missing
      case StartDecision => s.intent && s.decision == Prepared && s.destination == Present(next)
      case FinishDecision => s.intent && s.decision != Committed && s.destination == Present(next)
      case MissingDestination => s.intent &&
        (if s.decision == Committed then s.newCopy else s.oldCopy) &&
        PublicationIndependentOfDestination(original, next, s)
      case Interrupt => true
  }

  function Apply(original: seq<int>, next: seq<int>, s: Snapshot, a: Action): Snapshot
    requires Enabled(original, next, s, a)
  {
    match a
      case Stage(useNew, count) => s.(publication := Present(Chosen(original, next, useNew)[..count]))
      case Publish(useNew) => s.(destination := Present(Chosen(original, next, useNew)), publication := Missing)
      case RemovePublication => s.(publication := Missing)
      case RemoveOld => s.(oldCopy := false)
      case RemoveNew => s.(newCopy := false)
      case RemoveIntent => s.(intent := false)
      case StartDecision => s.(decision := PartialCommit)
      case FinishDecision => s.(decision := Committed)
      case MissingDestination => s.(destination := Missing)
      case Interrupt => s
  }

  // Every physical publication prefix remains authorized; the selected full
  // image survives each action until the canonical destination holds it.
  lemma StepPreserves(original: seq<int>, next: seq<int>, s: Snapshot, a: Action)
    requires Valid(original, next, s)
    requires Enabled(original, next, s, a)
    ensures Valid(original, next, Apply(original, next, s, a))
    ensures s.decision == Committed ==> Apply(original, next, s, a).decision == Committed
  {
    match a {
      case Stage(useNew, count) =>
      case Publish(useNew) =>
      case RemovePublication =>
      case RemoveOld =>
      case RemoveNew =>
      case RemoveIntent =>
      case StartDecision =>
      case FinishDecision =>
      case MissingDestination =>
      case Interrupt =>
    }
  }

  predicate Path(original: seq<int>, next: seq<int>, states: seq<Snapshot>, actions: seq<Action>) {
    |states| == |actions| + 1 &&
    forall i | 0 <= i < |actions| ::
      Enabled(original, next, states[i], actions[i]) &&
      states[i+1] == Apply(original, next, states[i], actions[i])
  }

  // Arbitrarily many finite interruptions are covered, rather than one sampled
  // crash point. Liveness additionally needs eventual successful recovery I/O.
  lemma TracePreserves(original: seq<int>, next: seq<int>, states: seq<Snapshot>, actions: seq<Action>)
    requires Path(original, next, states, actions)
    requires Valid(original, next, states[0])
    ensures Valid(original, next, states[|states|-1])
    ensures states[0].decision == Committed ==> states[|states|-1].decision == Committed
    decreases |actions|
  {
    if |actions| != 0 {
      StepPreserves(original, next, states[0], actions[0]);
      TracePreserves(original, next, states[1..], actions[1..]);
    }
  }

  function RecoveryAction(original: seq<int>, next: seq<int>, s: Snapshot): Action
    requires Valid(original, next, s) && s.intent
  {
    var selected := Selected(original, next, s);
    var useNew := s.decision == Committed;
    if s.destination != Present(selected) then
      if s.publication.Present? && IsPrefix(s.publication.bytes, selected) then
        if |s.publication.bytes| < |selected| then Stage(useNew, |s.publication.bytes| + 1)
        else Publish(useNew)
      else Stage(useNew, 0)
    else if s.publication != Missing then RemovePublication
    else if s.oldCopy then RemoveOld
    else if s.newCopy then RemoveNew
    else RemoveIntent
  }

  function RecoveryRank(original: seq<int>, next: seq<int>, s: Snapshot): nat
    requires Valid(original, next, s)
  {
    var selected := Selected(original, next, s);
    if s.destination != Present(selected) then
      if s.publication.Present? && IsPrefix(s.publication.bytes, selected) then
        |selected| - |s.publication.bytes| + 4
      else |selected| + 5
    else ((if s.publication.Present? then 1 else 0) +
      (if s.oldCopy then 1 else 0) + (if s.newCopy then 1 else 0) +
      (if s.intent then 1 else 0))
  }

  lemma RecoveryProgress(original: seq<int>, next: seq<int>, s: Snapshot)
    requires Valid(original, next, s) && s.intent
    ensures Enabled(original, next, s, RecoveryAction(original, next, s))
    ensures Valid(original, next, Apply(original, next, s, RecoveryAction(original, next, s)))
    ensures Apply(original, next, s, RecoveryAction(original, next, s)).decision == s.decision
    ensures RecoveryRank(original, next, Apply(original, next, s, RecoveryAction(original, next, s))) <
      RecoveryRank(original, next, s)
  {
    var a := RecoveryAction(original, next, s);
    var selected := Selected(original, next, s);
    if s.destination != Present(selected) && s.publication.Present? &&
        IsPrefix(s.publication.bytes, selected) && |s.publication.bytes| == |selected| {
      assert s.publication.bytes == selected;
    }
    assert Enabled(original, next, s, a);
    StepPreserves(original, next, s, a);
  }

  // Constructs a finite valid recovery path. The decreasing rank includes each
  // byte copied, publication, and every cleanup step; no fairness is hidden in
  // the termination claim. Interrupted runs can restart from any valid state.
  lemma CompleteRecovery(original: seq<int>, next: seq<int>, s: Snapshot)
    returns (states: seq<Snapshot>, actions: seq<Action>)
    requires Valid(original, next, s)
    ensures Path(original, next, states, actions) && states[0] == s
    ensures !states[|states|-1].intent
    ensures states[|states|-1].decision == s.decision
    ensures states[|states|-1].destination == Present(Selected(original, next, s))
    decreases RecoveryRank(original, next, s)
  {
    if !s.intent {
      states, actions := [s], [];
    } else {
      RecoveryProgress(original, next, s);
      var a := RecoveryAction(original, next, s);
      var successor := Apply(original, next, s, a);
      var tailStates, tailActions := CompleteRecovery(original, next, successor);
      states, actions := [s] + tailStates, [a] + tailActions;
      forall i | 0 <= i < |actions|
        ensures Enabled(original, next, states[i], actions[i]) &&
          states[i+1] == Apply(original, next, states[i], actions[i])
      {
        if i != 0 {
          assert states[i] == tailStates[i-1];
          assert actions[i] == tailActions[i-1];
        }
      }
    }
  }

  predicate RecoveryOnly(original: seq<int>, next: seq<int>, states: seq<Snapshot>, actions: seq<Action>) {
    |states| == |actions| + 1 &&
    forall i | 0 <= i < |actions| ::
      actions[i] == Interrupt ||
      (Valid(original, next, states[i]) && states[i].intent &&
        actions[i] == RecoveryAction(original, next, states[i]))
  }

  lemma InterruptedRecoveryKeepsDecision(original: seq<int>, next: seq<int>,
    states: seq<Snapshot>, actions: seq<Action>)
    requires Path(original, next, states, actions)
    requires RecoveryOnly(original, next, states, actions)
    requires Valid(original, next, states[0])
    ensures Valid(original, next, states[|states|-1])
    ensures states[|states|-1].decision == states[0].decision
    ensures !states[|states|-1].intent ==>
      states[|states|-1].destination == Present(Selected(original, next, states[0]))
    decreases |actions|
  {
    if |actions| != 0 {
      if actions[0] != Interrupt {
        RecoveryProgress(original, next, states[0]);
      }
      StepPreserves(original, next, states[0], actions[0]);
      InterruptedRecoveryKeepsDecision(original, next, states[1..], actions[1..]);
    }
  }
  lemma PreparedEstablishesInvariant(original: seq<int>, next: seq<int>)
    ensures Valid(original, next, Snapshot(Prepared, Present(original), true, true, Missing, true))
  {}

  lemma ReturnedCommitSurvivesRecovery(original: seq<int>, next: seq<int>, s: Snapshot)
    requires Valid(original, next, s) && s.decision == Committed
    ensures forall states: seq<Snapshot>, actions: seq<Action> |
      Path(original, next, states, actions) && states[0] == s && !states[|states|-1].intent ::
      states[|states|-1].destination == Present(next)
  {
    forall states: seq<Snapshot>, actions: seq<Action> |
      Path(original, next, states, actions) && states[0] == s && !states[|states|-1].intent
      ensures states[|states|-1].destination == Present(next)
    {
      TracePreserves(original, next, states, actions);
    }
  }

  // Negative proof obligations expose why the ordering guards are needed.
  lemma DeletingUndoBeforeDecisionIsUnsafe(original: seq<int>, next: seq<int>)
    requires original != next
    ensures Valid(original, next, Snapshot(Prepared, Present(next), true, true, Missing, true))
    ensures !Valid(original, next, Snapshot(Prepared, Present(next), false, true, Missing, true))
  {}

  lemma PublishingWithoutUndoIsDisabled(original: seq<int>, next: seq<int>)
    requires original != next
    ensures Valid(original, next, Snapshot(Prepared, Present(original), false, true, Present(next), true))
    ensures !Enabled(original, next,
      Snapshot(Prepared, Present(original), false, true, Present(next), true), Publish(true))
    ensures !Enabled(original, next,
      Snapshot(Prepared, Present(original), true, true, Missing, true), FinishDecision)
  {}

  lemma OrphanPublicationPrefixIsRejected()
    ensures !Valid([1, 2], [3, 4],
      Snapshot(Prepared, Present([1, 2]), false, false, Present([3]), true))
    ensures Valid([1, 2], [3, 4],
      Snapshot(Prepared, Present([1, 2]), false, true, Present([3]), true))
    ensures Valid([1, 2], [3, 4],
      Snapshot(Prepared, Present([1, 2]), false, false, Present([1]), true))
    ensures Valid([1, 2], [3, 4],
      Snapshot(Prepared, Present([1, 2]), false, false, Present([3, 4]), true))
  {
    assert [1, 2][..1] == [1];
    assert [3] != [1];
    assert !IsPrefix([3], [1, 2]);
    assert !PublicationIndependentOfDestination([1, 2], [3, 4],
      Snapshot(Prepared, Present([1, 2]), false, false, Present([3]), true));
  }

  lemma PartialPublicationWitnessCannotBeRemoved(original: seq<int>, next: seq<int>, s: Snapshot)
    requires s.publication != Missing
    ensures !Enabled(original, next, s, RemoveOld)
    ensures !Enabled(original, next, s, RemoveNew)
  {}

  lemma PublicationNeedsSurvivingSource(original: seq<int>, next: seq<int>, s: Snapshot,
    useNew: bool, count: nat)
    requires !(if useNew then s.newCopy else s.oldCopy)
    ensures !Enabled(original, next, s, Stage(useNew, count))
    ensures !Enabled(original, next, s, Publish(useNew))
  {}
  // Admission is deliberately distinct from recovery. Complete invalid evidence
  // refuses; a partial header never silently becomes a valid Prepared state.
  datatype HeaderEvidence = IncompleteHeader | InvalidHeader | ValidHeader
  datatype DecisionEvidence = NoDecision | IncompleteDecision | ValidDecision | InvalidCompleteDecision
  datatype Admission = Refuse | Admit(decision: Decision)

  function ReadEvidence(header: HeaderEvidence, decision: DecisionEvidence): Admission {
    if header != ValidHeader || decision == InvalidCompleteDecision then Refuse
    else if decision == ValidDecision then Admit(Committed)
    else Admit(Prepared)
  }

  lemma InvalidEvidenceNeverAuthorizes(header: HeaderEvidence, decision: DecisionEvidence)
    ensures header != ValidHeader || decision == InvalidCompleteDecision ==>
      ReadEvidence(header, decision) == Refuse
    ensures header == ValidHeader && decision == IncompleteDecision ==>
      ReadEvidence(header, decision) == Admit(Prepared)
  {}

  // Byte-level counterpart of ReadState, parameterized by the digest and
  // already canonicalized path identity. The digest is arbitrary: neither
  // SHA-256 implementation correctness nor collision freedom is presumed.
  // Successful reads supply the complete observed byte sequence. I/O errors,
  // GUID serialization, path canonicalization and exclusive leases stay outside.
  type RecordByte = value: int | 0 <= value < 256 witness 0

  function HeaderMagic(): seq<RecordByte> { [74, 68, 87, 82, 69, 80, 76, 49] }
  function DecisionMagic(): seq<RecordByte> { [74, 68, 87, 67, 77, 84, 48, 49] }

  function LittleEndian64(bytes: seq<RecordByte>): nat
    requires |bytes| == 8
    ensures LittleEndian64(bytes) < 18446744073709551616
  {
    (bytes[0] as int) + 256 * (bytes[1] as int) + 65536 * (bytes[2] as int) + 16777216 * (bytes[3] as int) +
      4294967296 * (bytes[4] as int) + 1099511627776 * (bytes[5] as int) +
      281474976710656 * (bytes[6] as int) + 72057594037927936 * (bytes[7] as int)
  }

  function SignedLittleEndian64(bytes: seq<RecordByte>): int
    requires |bytes| == 8
  {
    if bytes[7] < 128 then LittleEndian64(bytes)
    else LittleEndian64(bytes) - 18446744073709551616
  }

  // The nonnegative 2 GiB bound makes unsigned weighted decoding equivalent
  // to the signed little-endian length check performed by ReadState.
  lemma LengthEncodingMatchesSignedBounds(bytes: seq<RecordByte>)
    requires |bytes| == 8
    ensures (LittleEndian64(bytes) <= 2147483648) <==>
      (0 <= SignedLittleEndian64(bytes) <= 2147483648)
  {
    assert LittleEndian64(bytes) >= 72057594037927936 * (bytes[7] as int);
  }
  predicate ValidHeaderBytes(header: seq<RecordByte>, pathIdentity: seq<RecordByte>,
      digest: seq<RecordByte> -> seq<RecordByte>) {
    |header| == 256 && |pathIdentity| == 32 &&
    header[..8] == HeaderMagic() && header[8..12] == [1, 0, 0, 0] &&
    header[12..16] == [0, 1, 0, 0] &&
    (exists i | 16 <= i < 32 :: header[i] != 0) &&
    header[32..64] == pathIdentity && header[224..256] == digest(header[..224]) &&
    LittleEndian64(header[64..72]) <= 2147483648 &&
    LittleEndian64(header[72..80]) <= 2147483648
  }

  predicate ValidDecisionBytes(header: seq<RecordByte>, decision: seq<RecordByte>,
      digest: seq<RecordByte> -> seq<RecordByte>)
    requires |header| == 256
  {
    |decision| == 96 && decision[..8] == DecisionMagic() &&
    decision[8..24] == header[16..32] && decision[24..56] == header[112..144] &&
    decision[64..96] == digest(decision[..64])
  }

  // The zero/reserved fields are deliberately not checked beyond ReadState's
  // actual checks. In particular arbitrary partial decision contents, even an
  // invalid prefix, remain Prepared; a complete invalid decision must refuse.
  function ReadRecordBytes(bytes: seq<RecordByte>, pathIdentity: seq<RecordByte>,
      digest: seq<RecordByte> -> seq<RecordByte>): Admission {
    if |bytes| < 256 || |bytes| > 352 then Refuse
    else if !ValidHeaderBytes(bytes[..256], pathIdentity, digest) then Refuse
    else if |bytes| < 352 then Admit(Prepared)
    else if ValidDecisionBytes(bytes[..256], bytes[256..], digest) then Admit(Committed)
    else Refuse
  }

  function HeaderClassification(bytes: seq<RecordByte>, pathIdentity: seq<RecordByte>,
      digest: seq<RecordByte> -> seq<RecordByte>): HeaderEvidence {
    if |bytes| < 256 then IncompleteHeader
    else if ValidHeaderBytes(bytes[..256], pathIdentity, digest) then ValidHeader
    else InvalidHeader
  }

  function DecisionClassification(bytes: seq<RecordByte>, digest: seq<RecordByte> -> seq<RecordByte>): DecisionEvidence {
    if |bytes| <= 256 then NoDecision
    else if |bytes| < 352 then IncompleteDecision
    else if |bytes| == 352 && ValidDecisionBytes(bytes[..256], bytes[256..], digest) then ValidDecision
    else InvalidCompleteDecision
  }

  lemma BytesRefineEvidenceAdmission(bytes: seq<RecordByte>, pathIdentity: seq<RecordByte>,
      digest: seq<RecordByte> -> seq<RecordByte>)
    ensures ReadRecordBytes(bytes, pathIdentity, digest) ==
      ReadEvidence(HeaderClassification(bytes, pathIdentity, digest), DecisionClassification(bytes, digest))
  {}

  lemma AdmittedBytesEstablishCheckedFields(bytes: seq<RecordByte>, pathIdentity: seq<RecordByte>,
      digest: seq<RecordByte> -> seq<RecordByte>)
    requires ReadRecordBytes(bytes, pathIdentity, digest) != Refuse
    ensures 256 <= |bytes| <= 352
    ensures ValidHeaderBytes(bytes[..256], pathIdentity, digest)
    ensures exists i | 16 <= i < 32 :: bytes[i] != 0
    ensures LittleEndian64(bytes[64..72]) <= 2147483648
    ensures LittleEndian64(bytes[72..80]) <= 2147483648
    ensures ReadRecordBytes(bytes, pathIdentity, digest).decision == Committed <==>
      |bytes| == 352 && ValidDecisionBytes(bytes[..256], bytes[256..], digest)
    ensures ReadRecordBytes(bytes, pathIdentity, digest).decision != PartialCommit
  {}

  lemma PartialDecisionCannotCommit(header: seq<RecordByte>, partial: seq<RecordByte>,
      pathIdentity: seq<RecordByte>, digest: seq<RecordByte> -> seq<RecordByte>)
    requires ValidHeaderBytes(header, pathIdentity, digest)
    requires |partial| < 96
    ensures ReadRecordBytes(header + partial, pathIdentity, digest) == Admit(Prepared)
  {
    assert (header + partial)[..256] == header;
  }

  lemma CompleteDecisionRequiresEveryCheckedField(header: seq<RecordByte>, decision: seq<RecordByte>,
      pathIdentity: seq<RecordByte>, digest: seq<RecordByte> -> seq<RecordByte>)
    requires ValidHeaderBytes(header, pathIdentity, digest)
    requires |decision| == 96
    ensures ReadRecordBytes(header + decision, pathIdentity, digest) == Admit(Committed) <==>
      ValidDecisionBytes(header, decision, digest)
    ensures !ValidDecisionBytes(header, decision, digest) ==>
      ReadRecordBytes(header + decision, pathIdentity, digest) == Refuse
  {
    assert (header + decision)[..256] == header;
    assert (header + decision)[256..] == decision;
  }

  lemma InvalidHeaderCannotBorrowDecisionAuthority(header: seq<RecordByte>, suffix: seq<RecordByte>,
      pathIdentity: seq<RecordByte>, digest: seq<RecordByte> -> seq<RecordByte>)
    requires |header| == 256
    requires !ValidHeaderBytes(header, pathIdentity, digest)
    ensures ReadRecordBytes(header + suffix, pathIdentity, digest) == Refuse
  {
    assert (header + suffix)[..256] == header;
  }

  lemma NegativeSignedLengthIsRejected(bytes: seq<RecordByte>, pathIdentity: seq<RecordByte>,
      digest: seq<RecordByte> -> seq<RecordByte>)
    requires |bytes| >= 256
    requires bytes[71] >= 128 || bytes[79] >= 128
    ensures ReadRecordBytes(bytes, pathIdentity, digest) == Refuse
  {
    if |bytes| <= 352 {
      var header := bytes[..256];
      assert header[64..72][7] == bytes[71];
      assert header[72..80][7] == bytes[79];
      assert LittleEndian64(header[64..72]) >= 72057594037927936 * (bytes[71] as int);
      assert LittleEndian64(header[72..80]) >= 72057594037927936 * (bytes[79] as int);
    }
  }

  // Decision bytes determine the same image as the recovery protocol. This
  // composes classification with the existing invariant; admission alone does
  // not establish validated complete copies, destination bytes or generation
  // ownership. Recovery actions never append a new transaction decision.
  lemma AdmittedBytesSelectRecovery(bytes: seq<RecordByte>, pathIdentity: seq<RecordByte>,
      digest: seq<RecordByte> -> seq<RecordByte>, original: seq<int>, next: seq<int>, s: Snapshot)
    returns (states: seq<Snapshot>, actions: seq<Action>)
    requires ReadRecordBytes(bytes, pathIdentity, digest).Admit?
    requires s.decision == ReadRecordBytes(bytes, pathIdentity, digest).decision
    requires Valid(original, next, s)
    ensures Path(original, next, states, actions) && states[0] == s
    ensures !states[|states|-1].intent
    ensures states[|states|-1].decision == s.decision
    ensures states[|states|-1].destination ==
      Present(if |bytes| == 352 then next else original)
  {
    AdmittedBytesEstablishCheckedFields(bytes, pathIdentity, digest);
    states, actions := CompleteRecovery(original, next, s);
  }

  lemma HeaderBootstrapRequiresCompleteBoundEvidence(bytes: seq<RecordByte>, pathIdentity: seq<RecordByte>,
      digest: seq<RecordByte> -> seq<RecordByte>)
    requires |bytes| < 256 || |bytes| > 352 || |pathIdentity| != 32 ||
      (|bytes| >= 256 && forall i | 16 <= i < 32 :: bytes[i] == 0)
    ensures ReadRecordBytes(bytes, pathIdentity, digest) == Refuse
  {}

  lemma ForeignCompleteDecisionIsRejected(header: seq<RecordByte>, decision: seq<RecordByte>,
      pathIdentity: seq<RecordByte>, digest: seq<RecordByte> -> seq<RecordByte>)
    requires ValidHeaderBytes(header, pathIdentity, digest)
    requires |decision| == 96
    requires decision[..8] != DecisionMagic() || decision[8..24] != header[16..32] ||
      decision[24..56] != header[112..144] || decision[64..96] != digest(decision[..64])
    ensures ReadRecordBytes(header + decision, pathIdentity, digest) == Refuse
  {
    CompleteDecisionRequiresEveryCheckedField(header, decision, pathIdentity, digest);
  }

  // Quantifies over every possible byte cut of a complete valid append. No
  // prefix, including a full-looking magic/generation/hash prefix, commits early.
  lemma DecisionAppendCutsHaveExactSelection(header: seq<RecordByte>, decision: seq<RecordByte>,
      pathIdentity: seq<RecordByte>, digest: seq<RecordByte> -> seq<RecordByte>, count: nat)
    requires ValidHeaderBytes(header, pathIdentity, digest)
    requires ValidDecisionBytes(header, decision, digest)
    requires count <= 96
    ensures ReadRecordBytes(header + decision[..count], pathIdentity, digest) ==
      Admit(if count == 96 then Committed else Prepared)
  {
    if count < 96 {
      PartialDecisionCannotCommit(header, decision[..count], pathIdentity, digest);
    } else {
      assert decision[..count] == decision;
      CompleteDecisionRequiresEveryCheckedField(header, decision, pathIdentity, digest);
    }
  }

  lemma AdmittedBytesSurviveInterruptedRecovery(bytes: seq<RecordByte>, pathIdentity: seq<RecordByte>,
      digest: seq<RecordByte> -> seq<RecordByte>, original: seq<int>, next: seq<int>,
      states: seq<Snapshot>, actions: seq<Action>)
    requires ReadRecordBytes(bytes, pathIdentity, digest).Admit?
    requires Path(original, next, states, actions)
    requires RecoveryOnly(original, next, states, actions)
    requires states[0].decision == ReadRecordBytes(bytes, pathIdentity, digest).decision
    requires Valid(original, next, states[0])
    ensures Valid(original, next, states[|states|-1])
    ensures states[|states|-1].decision == ReadRecordBytes(bytes, pathIdentity, digest).decision
    ensures !states[|states|-1].intent ==>
      states[|states|-1].destination == Present(if |bytes| == 352 then next else original)
  {
    AdmittedBytesEstablishCheckedFields(bytes, pathIdentity, digest);
    InterruptedRecoveryKeepsDecision(original, next, states, actions);
  }
}
