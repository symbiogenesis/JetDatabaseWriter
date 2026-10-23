// Verification-only abstraction of EncryptionReplacementIntent. No code is extracted.
// Images are arbitrary integer sequences; equality represents validated complete
// bytes, not a proof that SHA-256 collisions or malicious edits are impossible.
// Entry starts after a complete valid Prepared header and exclusive generation
// ownership. Stable names, immutable copies and cooperating exclusive access are
// storage-model premises. Bootstrap tears, lost discovery names, OS durability,
// parser correctness, hash implementation and C# refinement remain unproved.
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
}