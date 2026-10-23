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

  // Copy/Publish refinement under a retained, validated immutable generation
  // source and exclusive authority. Reads return its exact bounded slice;
  // writes append either the full pending buffer or, on failure, a prefix.
  // Visible and durable publication contents are separate. Background drains
  // and uncertain flushes preserve a contiguous prefix; successful Flush(true)
  // fences all copied bytes. This storage contract is explicit, not an OS or
  // power-loss theorem: reordered holes and arbitrary corruption may refuse.
  // Retry closes/retires the failed attempt before recreating its output; no
  // old buffered effect may reappear in that newly empty publication file.
  datatype CopyPhase = CopyReading | CopyWriting | CopyFlushing | CopyReady | CopyFailed
  datatype CopySnapshot = CopySnapshot(protocol: Snapshot, useNew: bool,
    cursor: nat, buffer: seq<int>, durable: seq<int>, phase: CopyPhase)
  datatype CopyAction = ReadChunk(count: nat) | WriteChunk | FailChunk(written: nat)
    | DrainCopy(count: nat) | BeginCopyFlush | CompleteCopyFlush
    | UncertainCopyFlush(count: nat) | AbortCopy | CrashCopy | RetryCopy

  function CopyOutput(c: CopySnapshot): seq<int> {
    if c.protocol.publication.Present? then c.protocol.publication.bytes else []
  }

  predicate CopyFrame(before: CopySnapshot, after: CopySnapshot) {
    after.useNew == before.useNew &&
    after.protocol.decision == before.protocol.decision &&
    after.protocol.destination == before.protocol.destination &&
    after.protocol.oldCopy == before.protocol.oldCopy &&
    after.protocol.newCopy == before.protocol.newCopy &&
    after.protocol.intent == before.protocol.intent
  }

  predicate CopyValid(original: seq<int>, next: seq<int>, c: CopySnapshot) {
    var source := Chosen(original, next, c.useNew);
    var output := CopyOutput(c);
    Valid(original, next, c.protocol) && c.protocol.intent &&
    (if c.useNew then c.protocol.newCopy else c.protocol.oldCopy) &&
    |source| <= 2147483648 && c.protocol.publication == Present(output) &&
    IsPrefix(output, source) && IsPrefix(c.durable, output) &&
    |output| <= c.cursor <= |source| && |c.buffer| <= 65536 &&
    (match c.phase
      case CopyReading => c.buffer == [] && |output| == c.cursor
      case CopyWriting => 0 < |c.buffer| && c.cursor == |output| + |c.buffer| &&
        c.buffer == source[|output|..c.cursor]
      case CopyFlushing => c.buffer == [] && c.cursor == |source| && output == source
      case CopyReady => c.buffer == [] && c.cursor == |source| && output == source && c.durable == source
      case CopyFailed => true)
  }

  predicate CopyEnabled(original: seq<int>, next: seq<int>, c: CopySnapshot, a: CopyAction) {
    var source := Chosen(original, next, c.useNew);
    c.protocol.intent && (if c.useNew then c.protocol.newCopy else c.protocol.oldCopy) &&
    |source| <= 2147483648 &&
    (match a
      case ReadChunk(count) => c.phase == CopyReading && 0 < count <= 65536 && c.cursor + count <= |source|
      case WriteChunk => c.phase == CopyWriting
      case FailChunk(written) => c.phase == CopyWriting && written <= |c.buffer|
      case DrainCopy(count) => |c.durable| <= count <= |CopyOutput(c)|
      case BeginCopyFlush => c.phase == CopyReading && c.cursor == |source|
      case CompleteCopyFlush => c.phase == CopyFlushing
      case UncertainCopyFlush(count) => c.phase == CopyFlushing && |c.durable| <= count <= |CopyOutput(c)|
      case AbortCopy => c.phase != CopyReady && c.phase != CopyFailed
      case CrashCopy => true
      case RetryCopy => c.phase == CopyFailed)
  }

  function ApplyCopy(original: seq<int>, next: seq<int>, c: CopySnapshot, a: CopyAction): CopySnapshot
    requires CopyEnabled(original, next, c, a)
  {
    var source := Chosen(original, next, c.useNew);
    var output := CopyOutput(c);
    match a
      case ReadChunk(count) => c.(cursor := c.cursor + count,
        buffer := source[c.cursor..c.cursor + count], phase := CopyWriting)
      case WriteChunk => c.(protocol := c.protocol.(publication := Present(output + c.buffer)),
        buffer := [], phase := CopyReading)
      case FailChunk(written) => c.(protocol := c.protocol.(publication := Present(output + c.buffer[..written])),
        buffer := [], phase := CopyFailed)
      case DrainCopy(count) => c.(durable := output[..count])
      case BeginCopyFlush => c.(phase := CopyFlushing)
      case CompleteCopyFlush => c.(durable := output, phase := CopyReady)
      case UncertainCopyFlush(count) => c.(durable := output[..count], phase := CopyFailed)
      case AbortCopy => c.(phase := CopyFailed)
      case CrashCopy => c.(protocol := c.protocol.(publication := Present(c.durable)),
        cursor := |c.durable|, buffer := [], phase := CopyFailed)
      case RetryCopy => c.(protocol := c.protocol.(publication := Present([])),
        cursor := 0, buffer := [], durable := [], phase := CopyReading)
  }

  function CopyAbstractAction(c: CopySnapshot, a: CopyAction): Action {
    match a
      case WriteChunk => Stage(c.useNew, |CopyOutput(c)| + |c.buffer|)
      case FailChunk(written) => Stage(c.useNew, |CopyOutput(c)| + written)
      case CrashCopy => Stage(c.useNew, |c.durable|)
      case RetryCopy => Stage(c.useNew, 0)
      case _ => Interrupt
  }

  lemma PrefixOfPrefix(part: seq<int>, middle: seq<int>, whole: seq<int>)
    requires IsPrefix(part, middle) && IsPrefix(middle, whole)
    ensures IsPrefix(part, whole)
  {}

  lemma AppendedReadBytesAreExact(source: seq<int>, output: seq<int>, buffer: seq<int>, cursor: nat, written: nat)
    requires IsPrefix(output, source)
    requires cursor == |output| + |buffer| && cursor <= |source|
    requires buffer == source[|output|..cursor]
    requires written <= |buffer|
    ensures output + buffer[..written] == source[..|output| + written]
    ensures IsPrefix(output + buffer[..written], source)
    ensures IsPrefix(output, output + buffer[..written])
  {
    assert source[..|output|] + source[|output|..|output| + written] == source[..|output| + written];
  }

  // Derives the existing Stage action from actual buffered slices and append
  // effects. No read/write step receives an arbitrary exact-prefix premise.
  lemma CopyStepRefinesStaging(original: seq<int>, next: seq<int>, c: CopySnapshot, a: CopyAction)
    requires CopyValid(original, next, c)
    requires CopyEnabled(original, next, c, a)
    ensures CopyValid(original, next, ApplyCopy(original, next, c, a))
    ensures CopyFrame(c, ApplyCopy(original, next, c, a))
    ensures Enabled(original, next, c.protocol, CopyAbstractAction(c, a))
    ensures ApplyCopy(original, next, c, a).protocol ==
      Apply(original, next, c.protocol, CopyAbstractAction(c, a))
  {
    var source := Chosen(original, next, c.useNew);
    var output := CopyOutput(c);
    match a {
      case WriteChunk =>
        AppendedReadBytesAreExact(source, output, c.buffer, c.cursor, |c.buffer|);
        PrefixOfPrefix(c.durable, output, output + c.buffer);
      case FailChunk(written) =>
        AppendedReadBytesAreExact(source, output, c.buffer, c.cursor, written);
        PrefixOfPrefix(c.durable, output, output + c.buffer[..written]);
      case CrashCopy =>
        PrefixOfPrefix(c.durable, output, source);
      case _ =>
    }
    assert Enabled(original, next, c.protocol, CopyAbstractAction(c, a));
    StepPreserves(original, next, c.protocol, CopyAbstractAction(c, a));
  }

  function StartCopy(original: seq<int>, next: seq<int>, s: Snapshot, useNew: bool): CopySnapshot
    requires Enabled(original, next, s, Stage(useNew, 0))
  {
    CopySnapshot(Apply(original, next, s, Stage(useNew, 0)), useNew, 0, [], [], CopyReading)
  }

  lemma StartCopyEstablishesInvariant(original: seq<int>, next: seq<int>, s: Snapshot, useNew: bool)
    requires Valid(original, next, s) && Enabled(original, next, s, Stage(useNew, 0))
    requires |Chosen(original, next, useNew)| <= 2147483648
    ensures CopyValid(original, next, StartCopy(original, next, s, useNew))
  {
    StepPreserves(original, next, s, Stage(useNew, 0));
  }

  predicate CopyPath(original: seq<int>, next: seq<int>, copies: seq<CopySnapshot>, actions: seq<CopyAction>) {
    |copies| == |actions| + 1 &&
    forall i | 0 <= i < |actions| ::
      CopyEnabled(original, next, copies[i], actions[i]) &&
      copies[i+1] == ApplyCopy(original, next, copies[i], actions[i])
  }

  lemma CopyTracePreserves(original: seq<int>, next: seq<int>, copies: seq<CopySnapshot>, actions: seq<CopyAction>)
    requires CopyPath(original, next, copies, actions)
    requires CopyValid(original, next, copies[0])
    ensures CopyValid(original, next, copies[|copies|-1])
    ensures CopyFrame(copies[0], copies[|copies|-1])
    decreases |actions|
  {
    if |actions| != 0 {
      CopyStepRefinesStaging(original, next, copies[0], actions[0]);
      CopyTracePreserves(original, next, copies[1..], actions[1..]);
    }
  }

  lemma CopyTraceRefinesStaging(original: seq<int>, next: seq<int>, copies: seq<CopySnapshot>, copyActions: seq<CopyAction>)
    returns (states: seq<Snapshot>, actions: seq<Action>)
    requires CopyPath(original, next, copies, copyActions)
    requires CopyValid(original, next, copies[0])
    ensures |states| == |copies| && |actions| == |copyActions|
    ensures forall i | 0 <= i < |states| :: states[i] == copies[i].protocol
    ensures Path(original, next, states, actions)
    decreases |copyActions|
  {
    if |copyActions| == 0 {
      states, actions := [copies[0].protocol], [];
    } else {
      CopyStepRefinesStaging(original, next, copies[0], copyActions[0]);
      var tailStates, tailActions := CopyTraceRefinesStaging(original, next, copies[1..], copyActions[1..]);
      states, actions := [copies[0].protocol] + tailStates, [CopyAbstractAction(copies[0], copyActions[0])] + tailActions;
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

  function CopyRank(original: seq<int>, next: seq<int>, c: CopySnapshot): nat
    requires CopyValid(original, next, c)
  {
    var length := |Chosen(original, next, c.useNew)|;
    match c.phase
      case CopyFailed => 2 * length + 3
      case CopyReading => 2 * (length - c.cursor) + 2
      case CopyWriting => 2 * (length - c.cursor) + 3
      case CopyFlushing => 1
      case CopyReady => 0
  }

  function SuccessfulCopyAction(original: seq<int>, next: seq<int>, c: CopySnapshot): CopyAction
    requires CopyValid(original, next, c) && c.phase != CopyReady
  {
    var remaining := |Chosen(original, next, c.useNew)| - c.cursor;
    if c.phase == CopyFailed then RetryCopy
    else if c.phase == CopyWriting then WriteChunk
    else if c.phase == CopyFlushing then CompleteCopyFlush
    else if remaining == 0 then BeginCopyFlush
    else ReadChunk(if remaining <= 65536 then remaining else 65536)
  }

  // Positive short reads also progress; completion does not depend on Read
  // filling the requested 65536-byte buffer on every successful invocation.
  lemma SuccessfulCopyStepDecreasesRank(original: seq<int>, next: seq<int>, c: CopySnapshot, a: CopyAction)
    requires CopyValid(original, next, c) && CopyEnabled(original, next, c, a)
    requires a.ReadChunk? || a.WriteChunk? || a.BeginCopyFlush? || a.CompleteCopyFlush? || a.RetryCopy?
    ensures CopyValid(original, next, ApplyCopy(original, next, c, a))
    ensures CopyRank(original, next, ApplyCopy(original, next, c, a)) < CopyRank(original, next, c)
  {
    CopyStepRefinesStaging(original, next, c, a);
  }
  lemma SuccessfulCopyProgress(original: seq<int>, next: seq<int>, c: CopySnapshot)
    requires CopyValid(original, next, c) && c.phase != CopyReady
    ensures CopyEnabled(original, next, c, SuccessfulCopyAction(original, next, c))
    ensures CopyValid(original, next, ApplyCopy(original, next, c, SuccessfulCopyAction(original, next, c)))
    ensures CopyRank(original, next, ApplyCopy(original, next, c, SuccessfulCopyAction(original, next, c))) <
      CopyRank(original, next, c)
  {
    assert CopyEnabled(original, next, c, SuccessfulCopyAction(original, next, c));
    SuccessfulCopyStepDecreasesRank(original, next, c, SuccessfulCopyAction(original, next, c));
  }

  // After any finite interruption trace, one eventually successful retry has
  // a finite decreasing schedule. It restarts failed copies at byte zero, as
  // Publish does; it does not claim progress through infinitely many failures.
  lemma CompleteCopySchedule(original: seq<int>, next: seq<int>, c: CopySnapshot)
    returns (copies: seq<CopySnapshot>, actions: seq<CopyAction>)
    requires CopyValid(original, next, c)
    ensures CopyPath(original, next, copies, actions) && copies[0] == c
    ensures CopyValid(original, next, copies[|copies|-1]) && copies[|copies|-1].phase == CopyReady
    ensures CopyFrame(c, copies[|copies|-1])
    ensures |actions| <= CopyRank(original, next, c)
    decreases CopyRank(original, next, c)
  {
    if c.phase == CopyReady {
      copies, actions := [c], [];
    } else {
      SuccessfulCopyProgress(original, next, c);
      var a := SuccessfulCopyAction(original, next, c);
      var successor := ApplyCopy(original, next, c, a);
      var tailCopies, tailActions := CompleteCopySchedule(original, next, successor);
      copies, actions := [c] + tailCopies, [a] + tailActions;
      forall i | 0 <= i < |actions|
        ensures CopyEnabled(original, next, copies[i], actions[i]) &&
          copies[i+1] == ApplyCopy(original, next, copies[i], actions[i])
      {
        if i != 0 {
          assert copies[i] == tailCopies[i-1];
          assert actions[i] == tailActions[i-1];
        }
      }
    }
  }

  // Copy returns normally only after its successful fence. This gate refines
  // the caller's ordering; the older abstract Publish action alone has no
  // volatile/durable distinction and must not imply a completed copy flush.
  predicate CopyPublicationEnabled(original: seq<int>, next: seq<int>, c: CopySnapshot) {
    CopyValid(original, next, c) && c.phase == CopyReady &&
    Enabled(original, next, c.protocol, Publish(c.useNew))
  }

  function PublishCopied(original: seq<int>, next: seq<int>, c: CopySnapshot): Snapshot
    requires CopyPublicationEnabled(original, next, c)
  {
    Apply(original, next, c.protocol, Publish(c.useNew))
  }

  lemma FencedCopyPublicationRefines(original: seq<int>, next: seq<int>, c: CopySnapshot)
    requires CopyPublicationEnabled(original, next, c)
    ensures Valid(original, next, PublishCopied(original, next, c))
    ensures c.durable == Chosen(original, next, c.useNew)
    ensures PublishCopied(original, next, c).destination == Present(c.durable)
    ensures PublishCopied(original, next, c).decision == c.protocol.decision
  {
    StepPreserves(original, next, c.protocol, Publish(c.useNew));
  }

  lemma UnfencedOrFailedCopyCannotPublish(original: seq<int>, next: seq<int>, c: CopySnapshot)
    requires CopyValid(original, next, c)
    requires c.durable != Chosen(original, next, c.useNew) || c.phase == CopyFailed
    ensures !CopyPublicationEnabled(original, next, c)
  {}

  lemma CopyReadyTraceRequiresSuccessfulFlush(original: seq<int>, next: seq<int>,
      copies: seq<CopySnapshot>, actions: seq<CopyAction>)
    requires CopyPath(original, next, copies, actions)
    requires copies[0].phase != CopyReady && copies[|copies|-1].phase == CopyReady
    ensures exists i | 0 <= i < |actions| :: actions[i] == CompleteCopyFlush
    decreases |actions|
  {
    if actions[0] != CompleteCopyFlush {
      assert copies[1].phase != CopyReady;
      CopyReadyTraceRequiresSuccessfulFlush(original, next, copies[1..], actions[1..]);
      var i :| 0 <= i < |actions[1..]| && actions[1..][i] == CompleteCopyFlush;
      assert actions[i+1] == CompleteCopyFlush;
    }
  }
  // Composition includes the copy/flush schedule before abstract publication
  // and cleanup. Rename/replace, destination flush, metadata, discovery and
  // source/hash validation still require their separate production contracts.
  lemma CompleteSelectedCopyRecovery(original: seq<int>, next: seq<int>, c: CopySnapshot)
    returns (copies: seq<CopySnapshot>, copyActions: seq<CopyAction>, published: Snapshot,
      cleanupStates: seq<Snapshot>, cleanupActions: seq<Action>)
    requires CopyValid(original, next, c)
    requires c.useNew == (c.protocol.decision == Committed)
    ensures CopyPath(original, next, copies, copyActions) && copies[0] == c
    ensures CopyValid(original, next, copies[|copies|-1]) && copies[|copies|-1].phase == CopyReady
    ensures Enabled(original, next, copies[|copies|-1].protocol, Publish(c.useNew))
    ensures CopyPublicationEnabled(original, next, copies[|copies|-1])
    ensures published == PublishCopied(original, next, copies[|copies|-1])
    ensures Path(original, next, cleanupStates, cleanupActions) && cleanupStates[0] == published
    ensures !cleanupStates[|cleanupStates|-1].intent
    ensures cleanupStates[|cleanupStates|-1].decision == c.protocol.decision
    ensures cleanupStates[|cleanupStates|-1].destination == Present(Selected(original, next, c.protocol))
  {
    copies, copyActions := CompleteCopySchedule(original, next, c);
    var ready := copies[|copies|-1];
    assert Enabled(original, next, ready.protocol, Publish(c.useNew));
    assert CopyPublicationEnabled(original, next, ready);
    published := PublishCopied(original, next, ready);
    FencedCopyPublicationRefines(original, next, ready);
    cleanupStates, cleanupActions := CompleteRecovery(original, next, published);
  }

  lemma FiniteCopyFailuresAllowSelectedRecovery(original: seq<int>, next: seq<int>,
      failedCopies: seq<CopySnapshot>, failedActions: seq<CopyAction>)
    returns (copies: seq<CopySnapshot>, copyActions: seq<CopyAction>, published: Snapshot,
      cleanupStates: seq<Snapshot>, cleanupActions: seq<Action>)
    requires CopyPath(original, next, failedCopies, failedActions)
    requires CopyValid(original, next, failedCopies[0])
    requires failedCopies[0].useNew == (failedCopies[0].protocol.decision == Committed)
    ensures CopyPath(original, next, copies, copyActions) && copies[0] == failedCopies[|failedCopies|-1]
    ensures CopyPublicationEnabled(original, next, copies[|copies|-1])
    ensures published == PublishCopied(original, next, copies[|copies|-1])
    ensures Path(original, next, cleanupStates, cleanupActions) && cleanupStates[0] == published
    ensures !cleanupStates[|cleanupStates|-1].intent
    ensures cleanupStates[|cleanupStates|-1].decision == failedCopies[0].protocol.decision
    ensures cleanupStates[|cleanupStates|-1].destination == Present(Selected(original, next, failedCopies[0].protocol))
  {
    CopyTracePreserves(original, next, failedCopies, failedActions);
    copies, copyActions, published, cleanupStates, cleanupActions :=
      CompleteSelectedCopyRecovery(original, next, failedCopies[|failedCopies|-1]);
  }
  // A file with holes/changed bytes is not silently granted this prefix-copy
  // contract. ValidatePublication can refuse it while retaining all evidence.
  lemma ConflictingPublicationCannotEnterCopyModel(original: seq<int>, next: seq<int>, s: Snapshot)
    requires s.publication.Present?
    requires s.publication != Present(original) && s.publication != Present(next)
    requires !IsPrefix(s.publication.bytes, original) && !IsPrefix(s.publication.bytes, next)
    requires !s.destination.Present? || !IsPrefix(s.publication.bytes, s.destination.bytes)
    ensures !Valid(original, next, s)
    ensures forall c: CopySnapshot | c.protocol == s :: !CopyValid(original, next, c)
  {}
}
