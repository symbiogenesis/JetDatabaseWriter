// Verification-only abstraction of generation-bound cleanup. No code is extracted.
// A generation token represents an already validated exact receipt/lock-byte pair,
// not merely matching filenames. Live-handle exclusion and canonical database
// ownership are storage-model premises. Generations and owner-object IDs are not
// reused. Random GUID uniqueness, receipt parsing/hashing, OS handle semantics,
// malicious namespace edits and C# refinement are outside this proof.
module GenerationOwnership {
  datatype Name = Absent | Generation(value: nat)
  datatype Holder = Free | Owned(id: nat)
  datatype Snapshot = Snapshot(nativeName: Name, receiptName: Name,
    holder: Holder, cleaning: Holder, owners: map<nat,nat>, attempted: set<nat>)
  datatype Action = AcquireFresh(id: nat, generation: nat)
    | AcquireRecovery(id: nat, generation: nat) | StartDispose(id: nat)
    | DeleteNative(id: nat) | DeleteReceipt(id: nat) | Release(id: nat)
    | RepeatDispose(id: nat) | Crash | Interrupt

  predicate Valid(s: Snapshot) {
    s.attempted <= s.owners.Keys &&
    (s.nativeName != Absent && s.receiptName != Absent ==>
      s.nativeName == s.receiptName) &&
    (s.holder.Owned? ==>
      s.holder.id in s.owners &&
      (s.nativeName == Absent || s.nativeName == Generation(s.owners[s.holder.id])) &&
      (s.receiptName == Absent || s.receiptName == Generation(s.owners[s.holder.id]))) &&
    (s.cleaning.Owned? ==> s.cleaning == s.holder && s.cleaning.id in s.attempted)
  }

  predicate Enabled(s: Snapshot, a: Action) {
    match a
      case AcquireFresh(id, generation) => s.holder == Free &&
        s.nativeName == Absent && s.receiptName == Absent &&
        id !in s.owners && generation !in s.owners.Values
      case AcquireRecovery(id, generation) => s.holder == Free &&
        s.receiptName == Generation(generation) &&
        (s.nativeName == Absent || s.nativeName == Generation(generation)) &&
        id !in s.owners
      case StartDispose(id) => s.holder == Owned(id) && id !in s.attempted
      case DeleteNative(id) => s.cleaning == Owned(id) && s.holder == Owned(id) &&
        id in s.owners && s.nativeName == Generation(s.owners[id])
      case DeleteReceipt(id) => s.cleaning == Owned(id) && s.holder == Owned(id) &&
        id in s.owners && s.nativeName == Absent && s.receiptName == Generation(s.owners[id])
      case Release(id) => s.cleaning == Owned(id) && s.holder == Owned(id)
      case RepeatDispose(id) => id in s.attempted
      case Crash => true
      case Interrupt => true
  }

  function Apply(s: Snapshot, a: Action): Snapshot
    requires Enabled(s, a)
  {
    match a
      case AcquireFresh(id, generation) =>
        s.(nativeName := Generation(generation), receiptName := Generation(generation),
          holder := Owned(id), owners := s.owners[id := generation])
      case AcquireRecovery(id, generation) =>
        s.(nativeName := Generation(generation), holder := Owned(id),
          owners := s.owners[id := generation])
      case StartDispose(id) => s.(cleaning := Owned(id), attempted := s.attempted + {id})
      case DeleteNative(id) => s.(nativeName := Absent)
      case DeleteReceipt(id) => s.(receiptName := Absent)
      case Release(id) => s.(holder := Free, cleaning := Free)
      case RepeatDispose(id) => s
      case Crash => s.(holder := Free, cleaning := Free)
      case Interrupt => s
  }

  lemma StepPreserves(s: Snapshot, a: Action)
    requires Valid(s) && Enabled(s, a)
    ensures Valid(Apply(s, a))
    ensures s.owners.Keys <= Apply(s, a).owners.Keys
    ensures s.attempted <= Apply(s, a).attempted
    ensures forall id | id in s.owners :: Apply(s, a).owners[id] == s.owners[id]
  {
    match a {
      case AcquireFresh(id, generation) =>
      case AcquireRecovery(id, generation) =>
      case StartDispose(id) =>
      case DeleteNative(id) =>
      case DeleteReceipt(id) =>
      case Release(id) =>
      case RepeatDispose(id) =>
      case Crash =>
      case Interrupt =>
    }
  }

  predicate Path(states: seq<Snapshot>, actions: seq<Action>) {
    |states| == |actions| + 1 &&
    forall i | 0 <= i < |actions| :: Enabled(states[i], actions[i]) &&
      states[i+1] == Apply(states[i], actions[i])
  }

  lemma TracePreserves(states: seq<Snapshot>, actions: seq<Action>)
    requires Path(states, actions) && Valid(states[0])
    ensures Valid(states[|states|-1])
    ensures states[0].attempted <= states[|states|-1].attempted
    ensures states[0].owners.Keys <= states[|states|-1].owners.Keys
    ensures forall id | id in states[0].owners ::
      states[|states|-1].owners[id] == states[0].owners[id]
    decreases |actions|
  {
    if |actions| != 0 {
      StepPreserves(states[0], actions[0]);
      TracePreserves(states[1..], actions[1..]);
    }
  }

  // The native name can disappear only under the held exact-generation lease.
  // Closing handles before deletion is represented by lease ownership retained
  // through receipt/database coordination, not by pretending a name is a lock.
  lemma DeletionRequiresGenerationAuthority(s: Snapshot, a: Action)
    requires Valid(s) && Enabled(s, a)
    ensures s.nativeName != Absent && Apply(s, a).nativeName == Absent ==>
      a.DeleteNative? && s.holder == Owned(a.id) &&
      s.nativeName == Generation(s.owners[a.id])
    ensures s.receiptName != Absent && Apply(s, a).receiptName == Absent ==>
      a.DeleteReceipt? && s.holder == Owned(a.id) &&
      s.receiptName == Generation(s.owners[a.id]) && s.nativeName == Absent
  {
    match a {
      case AcquireFresh(id, generation) =>
      case AcquireRecovery(id, generation) =>
      case StartDispose(id) =>
      case DeleteNative(id) =>
      case DeleteReceipt(id) =>
      case Release(id) =>
      case RepeatDispose(id) =>
      case Crash =>
      case Interrupt =>
    }
  }

  lemma StaleOwnerCannotDelete(s: Snapshot, stale: nat, successor: nat)
    requires Valid(s) && s.holder == Owned(successor) && stale != successor
    ensures !Enabled(s, DeleteNative(stale))
    ensures !Enabled(s, DeleteReceipt(stale))
    ensures !Enabled(s, StartDispose(stale))
    ensures !Enabled(s, Release(stale))
  {}

  // The attempted bit belongs to the owner object, not the reusable pathname.
  // Once set, every later Dispose of that object is a no-op, including after a
  // successor has acquired the same names with a distinct generation.
  lemma RepeatedDisposePreservesSuccessor(s: Snapshot, stale: nat, successor: nat)
    requires Valid(s) && stale in s.attempted && s.holder == Owned(successor)
    requires stale != successor
    ensures Enabled(s, RepeatDispose(stale))
    ensures Apply(s, RepeatDispose(stale)) == s
    ensures Apply(s, RepeatDispose(stale)).nativeName == s.nativeName
    ensures Apply(s, RepeatDispose(stale)).receiptName == s.receiptName
  {}

  lemma RepeatedDisposalsPreserveNames(s: Snapshot, stale: nat, repetitions: nat)
    returns (states: seq<Snapshot>, actions: seq<Action>)
    requires Valid(s) && stale in s.attempted
    ensures Path(states, actions) && |actions| == repetitions
    ensures states[0] == s && states[|states|-1] == s
    decreases repetitions
  {
    if repetitions == 0 {
      states, actions := [s], [];
    } else {
      var tailStates, tailActions := RepeatedDisposalsPreserveNames(s, stale, repetitions-1);
      states, actions := [s] + tailStates, [RepeatDispose(stale)] + tailActions;
      forall i | 0 <= i < |actions|
        ensures Enabled(states[i], actions[i]) && states[i+1] == Apply(states[i], actions[i])
      {
        if i != 0 {
          assert states[i] == tailStates[i-1];
          assert actions[i] == tailActions[i-1];
        }
      }
    }
  }

  lemma FailedCleanupRetainsEvidence(s: Snapshot, id: nat, nextId: nat, nextGeneration: nat)
    requires Valid(s) && Enabled(s, Release(id))
    requires s.nativeName != Absent || s.receiptName != Absent
    ensures Apply(s, Release(id)).nativeName == s.nativeName
    ensures Apply(s, Release(id)).receiptName == s.receiptName
    ensures !Enabled(Apply(s, Release(id)), AcquireFresh(nextId, nextGeneration))
  {}

  lemma NativeOnlyBootstrapRefusesRecovery(s: Snapshot, id: nat, generation: nat)
    requires Valid(s) && s.holder == Free && s.receiptName == Absent && s.nativeName != Absent
    ensures !Enabled(s, AcquireFresh(id, generation))
    ensures !Enabled(s, AcquireRecovery(id, generation))
  {}

  // These booleans describe the production validation boundary. No theorem here
  // claims that the receipt parser establishes them, or that locks exclude an
  // uncooperative process. Complete invalid or missing authority is refusal.
  predicate RecoveryAdmission(receiptComplete: bool, receiptValid: bool,
    pathMatches: bool, nativePresent: bool, nativeBytesMatch: bool, exclusive: bool) {
    receiptComplete && receiptValid && pathMatches && exclusive &&
      (!nativePresent || nativeBytesMatch)
  }

  lemma InvalidReceiptCannotAuthorize(receiptComplete: bool, receiptValid: bool,
    pathMatches: bool, nativePresent: bool, nativeBytesMatch: bool, exclusive: bool)
    requires !receiptComplete || !receiptValid || !pathMatches || !exclusive ||
      (nativePresent && !nativeBytesMatch)
    ensures !RecoveryAdmission(receiptComplete, receiptValid, pathMatches,
      nativePresent, nativeBytesMatch, exclusive)
  {}

  lemma ForeignGenerationCannotBeValidated(first: nat, second: nat)
    requires first != second
    ensures !Valid(Snapshot(Generation(first), Generation(second), Free, Free, map[], {}))
  {}

  lemma ReceiptCannotBeDeletedBeforeNative(s: Snapshot, id: nat)
    requires Valid(s) && s.nativeName != Absent
    ensures !Enabled(s, DeleteReceipt(id))
  {}
  lemma EmptyEstablishesInvariant()
    ensures Valid(Snapshot(Absent, Absent, Free, Free, map[], {}))
  {}

  predicate HeldGenerationAuthority(s: Snapshot, id: nat, generation: nat) {
    Valid(s) && s.holder == Owned(id) && s.cleaning == Free &&
    id in s.owners && id !in s.attempted && s.owners[id] == generation &&
    s.nativeName == Generation(generation) && s.receiptName == Generation(generation)
  }

  // The two handles alone are insufficient: receipt preparation and native
  // reconstruction can fail before an exact pair has been validated and fenced.
  // Evidence tokens stand for parsed bytes, not merely a pathname. A partial
  // token is deliberately never upgraded by a crash or by a retry's admission.
  datatype AcquisitionEvidence = NoEvidence | PartialEvidence(generation: nat)
    | ExactEvidence(generation: nat)
  datatype AcquisitionPhase = Dormant | NativeWriting | NativeReady | ReceiptWriting | AuthorityReady
  datatype AcquisitionSnapshot = AcquisitionSnapshot(native: AcquisitionEvidence,
    receipt: AcquisitionEvidence, holder: Holder, phase: AcquisitionPhase, generation: nat)
  datatype AcquisitionAction = BeginFresh(id: nat, generation: nat) | BeginRecorded(id: nat)
    | CompleteNative | BeginReceipt | CompleteReceipt | AcquisitionFailure | AcquisitionInterrupt

  predicate AcquisitionValid(s: AcquisitionSnapshot) {
    (s.phase == Dormant <==> s.holder == Free) &&
    (s.phase == NativeWriting ==> (s.native == PartialEvidence(s.generation) &&
      (s.receipt == NoEvidence || s.receipt == ExactEvidence(s.generation)))) &&
    (s.phase == NativeReady ==> s.native == ExactEvidence(s.generation) && s.receipt == NoEvidence) &&
    (s.phase == ReceiptWriting ==> (s.native == ExactEvidence(s.generation) &&
      s.receipt == PartialEvidence(s.generation))) &&
    (s.phase == AuthorityReady ==> (s.native == ExactEvidence(s.generation) &&
      s.receipt == ExactEvidence(s.generation)))
  }

  predicate AcquisitionEnabled(s: AcquisitionSnapshot, a: AcquisitionAction) {
    match a
      case BeginFresh(id, generation) => s.phase == Dormant &&
        s.native == NoEvidence && s.receipt == NoEvidence
      case BeginRecorded(id) => s.phase == Dormant && s.receipt.ExactEvidence? &&
        (s.native == NoEvidence || s.native == ExactEvidence(s.receipt.generation))
      case CompleteNative => s.phase == NativeWriting
      case BeginReceipt => s.phase == NativeReady
      case CompleteReceipt => s.phase == ReceiptWriting
      case AcquisitionFailure => true
      case AcquisitionInterrupt => true
  }

  function ApplyAcquisition(s: AcquisitionSnapshot, a: AcquisitionAction): AcquisitionSnapshot
    requires AcquisitionEnabled(s, a)
  {
    match a
      case BeginFresh(id, generation) => AcquisitionSnapshot(PartialEvidence(generation),
        NoEvidence, Owned(id), NativeWriting, generation)
      case BeginRecorded(id) =>
        if s.native == NoEvidence then AcquisitionSnapshot(PartialEvidence(s.receipt.generation),
          s.receipt, Owned(id), NativeWriting, s.receipt.generation)
        else AcquisitionSnapshot(s.native, s.receipt, Owned(id), AuthorityReady, s.receipt.generation)
      case CompleteNative => s.(native := ExactEvidence(s.generation),
        phase := if s.receipt == NoEvidence then NativeReady else AuthorityReady)
      case BeginReceipt => s.(receipt := PartialEvidence(s.generation), phase := ReceiptWriting)
      case CompleteReceipt => s.(receipt := ExactEvidence(s.generation), phase := AuthorityReady)
      case AcquisitionFailure => s.(holder := Free, phase := Dormant)
      case AcquisitionInterrupt => s
  }

  predicate AcquisitionPath(states: seq<AcquisitionSnapshot>, actions: seq<AcquisitionAction>) {
    |states| == |actions| + 1 &&
    forall i | 0 <= i < |actions| :: AcquisitionEnabled(states[i], actions[i]) &&
      states[i+1] == ApplyAcquisition(states[i], actions[i])
  }

  lemma AcquisitionStepPreserves(s: AcquisitionSnapshot, a: AcquisitionAction)
    requires AcquisitionValid(s) && AcquisitionEnabled(s, a)
    ensures AcquisitionValid(ApplyAcquisition(s, a))
  {
    match a {
      case BeginFresh(id, generation) =>
      case BeginRecorded(id) =>
      case CompleteNative =>
      case BeginReceipt =>
      case CompleteReceipt =>
      case AcquisitionFailure =>
      case AcquisitionInterrupt =>
    }
  }

  lemma AcquisitionTracePreserves(states: seq<AcquisitionSnapshot>, actions: seq<AcquisitionAction>)
    requires AcquisitionPath(states, actions) && AcquisitionValid(states[0])
    ensures AcquisitionValid(states[|states|-1])
    decreases |actions|
  {
    if |actions| != 0 {
      AcquisitionStepPreserves(states[0], actions[0]);
      AcquisitionTracePreserves(states[1..], actions[1..]);
    }
  }

  predicate AcquisitionAuthority(s: AcquisitionSnapshot, id: nat) {
    AcquisitionValid(s) && s.phase == AuthorityReady && s.holder == Owned(id)
  }

  function AcquiredOwnership(s: AcquisitionSnapshot, id: nat): Snapshot
    requires AcquisitionAuthority(s, id)
  {
    Snapshot(Generation(s.generation), Generation(s.generation), Owned(id), Free,
      map[id := s.generation], {})
  }

  lemma CompletedBootstrapEstablishesAuthority(s: AcquisitionSnapshot, id: nat)
    requires AcquisitionAuthority(s, id)
    ensures HeldGenerationAuthority(AcquiredOwnership(s, id), id, s.generation)
  {}

  lemma IncompleteBootstrapRefusesAuthority(s: AcquisitionSnapshot, id: nat, generation: nat)
    requires AcquisitionValid(s)
    requires s.native.PartialEvidence? || s.receipt.PartialEvidence? ||
      (s.native != NoEvidence && s.receipt == NoEvidence)
    ensures !AcquisitionAuthority(s, id)
    ensures !AcquisitionEnabled(s, BeginFresh(id, generation))
    ensures s.phase == Dormant ==> !AcquisitionEnabled(s, BeginRecorded(id))
  {}

  lemma FailedBootstrapRetainsEvidence(s: AcquisitionSnapshot, id: nat, generation: nat)
    requires AcquisitionValid(s)
    ensures ApplyAcquisition(s, AcquisitionFailure).native == s.native
    ensures ApplyAcquisition(s, AcquisitionFailure).receipt == s.receipt
    ensures !AcquisitionAuthority(ApplyAcquisition(s, AcquisitionFailure), id)
    ensures s.native != NoEvidence || s.receipt != NoEvidence ==>
      !AcquisitionEnabled(ApplyAcquisition(s, AcquisitionFailure), BeginFresh(id, generation))
  {}

  // A valid recorded receipt can reconstruct a missing native name. A partial
  // existing native file is a conflict, not permission to overwrite that file.
  lemma RecordedBootstrapCompletes(s: AcquisitionSnapshot, id: nat)
    returns (states: seq<AcquisitionSnapshot>, actions: seq<AcquisitionAction>)
    requires AcquisitionValid(s) && AcquisitionEnabled(s, BeginRecorded(id))
    ensures AcquisitionPath(states, actions) && states[0] == s
    ensures AcquisitionAuthority(states[|states|-1], id)
    ensures states[|states|-1].generation == s.receipt.generation
    ensures states[|states|-1].receipt == s.receipt
    ensures |actions| <= 2
  {
    var begun := ApplyAcquisition(s, BeginRecorded(id));
    AcquisitionStepPreserves(s, BeginRecorded(id));
    if begun.phase == NativeWriting {
      AcquisitionStepPreserves(begun, CompleteNative);
      states, actions := [s, begun, ApplyAcquisition(begun, CompleteNative)],
        [BeginRecorded(id), CompleteNative];
    } else {
      states, actions := [s, begun], [BeginRecorded(id)];
    }
  }

  lemma FreshBootstrapCompletes(s: AcquisitionSnapshot, id: nat, generation: nat)
    returns (states: seq<AcquisitionSnapshot>, actions: seq<AcquisitionAction>)
    requires AcquisitionValid(s) && AcquisitionEnabled(s, BeginFresh(id, generation))
    ensures AcquisitionPath(states, actions) && states[0] == s
    ensures AcquisitionAuthority(states[|states|-1], id)
    ensures states[|states|-1].generation == generation && |actions| == 4
  {
    var nativeWriting := ApplyAcquisition(s, BeginFresh(id, generation));
    AcquisitionStepPreserves(s, BeginFresh(id, generation));
    var nativeReady := ApplyAcquisition(nativeWriting, CompleteNative);
    AcquisitionStepPreserves(nativeWriting, CompleteNative);
    var receiptWriting := ApplyAcquisition(nativeReady, BeginReceipt);
    AcquisitionStepPreserves(nativeReady, BeginReceipt);
    var ready := ApplyAcquisition(receiptWriting, CompleteReceipt);
    AcquisitionStepPreserves(receiptWriting, CompleteReceipt);
    states, actions := [s, nativeWriting, nativeReady, receiptWriting, ready],
      [BeginFresh(id, generation), CompleteNative, BeginReceipt, CompleteReceipt];
  }

  lemma AuthorityTraceRequiresCompleteReceipt(states: seq<AcquisitionSnapshot>,
    actions: seq<AcquisitionAction>)
    requires AcquisitionPath(states, actions) && AcquisitionValid(states[0])
    requires !states[0].receipt.ExactEvidence? && states[|states|-1].phase == AuthorityReady
    ensures exists i | 0 <= i < |actions| :: actions[i] == CompleteReceipt
    decreases |actions|
  {
    AcquisitionTracePreserves(states, actions);
    if actions[0] != CompleteReceipt {
      AcquisitionStepPreserves(states[0], actions[0]);
      assert !states[1].receipt.ExactEvidence?;
      AuthorityTraceRequiresCompleteReceipt(states[1..], actions[1..]);
      var i :| 0 <= i < |actions[1..]| && actions[1..][i] == CompleteReceipt;
      assert actions[i+1] == CompleteReceipt;
    }
  }
  // The supported Windows adapter additionally fences the directory after
  // complete receipt/native contents. A successful named fence is an explicit
  // host effect, not a proof of FlushFileBuffers or a device power-loss claim.
  datatype DirectoryAcquisitionSnapshot = DirectoryAcquisitionSnapshot(
    contents: AcquisitionSnapshot, namesFenced: bool)
  datatype DirectoryAcquisitionAction = DirectoryAcquireContent(action: AcquisitionAction)
    | FenceAcquisitionNames(success: bool) | DirectoryAcquisitionCrash

  predicate DirectoryAcquisitionValid(s: DirectoryAcquisitionSnapshot) {
    AcquisitionValid(s.contents) && (s.namesFenced ==> s.contents.phase == AuthorityReady)
  }

  predicate DirectoryAcquisitionEnabled(s: DirectoryAcquisitionSnapshot, a: DirectoryAcquisitionAction) {
    match a
      case DirectoryAcquireContent(action) => !s.namesFenced && AcquisitionEnabled(s.contents, action)
      case FenceAcquisitionNames(success) => s.contents.phase == AuthorityReady
      case DirectoryAcquisitionCrash => true
  }

  function ApplyDirectoryAcquisition(s: DirectoryAcquisitionSnapshot,
      a: DirectoryAcquisitionAction): DirectoryAcquisitionSnapshot
    requires DirectoryAcquisitionEnabled(s, a)
  {
    match a
      case DirectoryAcquireContent(action) => DirectoryAcquisitionSnapshot(ApplyAcquisition(s.contents, action), false)
      case FenceAcquisitionNames(success) => s.(namesFenced := success)
      case DirectoryAcquisitionCrash => DirectoryAcquisitionSnapshot(ApplyAcquisition(s.contents, AcquisitionFailure), false)
  }

  predicate DirectoryAcquisitionAuthority(s: DirectoryAcquisitionSnapshot, id: nat) {
    s.namesFenced && AcquisitionAuthority(s.contents, id)
  }

  lemma DirectoryAcquisitionStepPreserves(s: DirectoryAcquisitionSnapshot, a: DirectoryAcquisitionAction)
    requires DirectoryAcquisitionValid(s) && DirectoryAcquisitionEnabled(s, a)
    ensures DirectoryAcquisitionValid(ApplyDirectoryAcquisition(s, a))
    ensures a == FenceAcquisitionNames(false) ==> (!ApplyDirectoryAcquisition(s, a).namesFenced &&
      ApplyDirectoryAcquisition(s, a).contents == s.contents)
  {
    match a {
      case DirectoryAcquireContent(action) => AcquisitionStepPreserves(s.contents, action);
      case FenceAcquisitionNames(success) =>
      case DirectoryAcquisitionCrash => AcquisitionStepPreserves(s.contents, AcquisitionFailure);
    }
  }

  predicate DirectoryAcquisitionPath(states: seq<DirectoryAcquisitionSnapshot>, actions: seq<DirectoryAcquisitionAction>) {
    |states| == |actions| + 1 && forall i | 0 <= i < |actions| ::
      DirectoryAcquisitionEnabled(states[i], actions[i]) && states[i+1] == ApplyDirectoryAcquisition(states[i], actions[i])
  }

  lemma DirectoryAcquisitionTracePreserves(states: seq<DirectoryAcquisitionSnapshot>, actions: seq<DirectoryAcquisitionAction>)
    requires DirectoryAcquisitionPath(states, actions) && DirectoryAcquisitionValid(states[0])
    ensures forall i | 0 <= i < |states| :: DirectoryAcquisitionValid(states[i])
    decreases |actions|
  {
    if |actions| > 0 {
      DirectoryAcquisitionStepPreserves(states[0], actions[0]);
      DirectoryAcquisitionTracePreserves(states[1..], actions[1..]);
    }
  }

  lemma DirectoryAuthorityRequiresAcknowledgedFence(states: seq<DirectoryAcquisitionSnapshot>, actions: seq<DirectoryAcquisitionAction>)
    requires DirectoryAcquisitionPath(states, actions) && DirectoryAcquisitionValid(states[0])
    requires !states[0].namesFenced && states[|states|-1].namesFenced
    ensures exists i | 0 <= i < |actions| :: actions[i] == FenceAcquisitionNames(true)
    decreases |actions|
  {
    DirectoryAcquisitionTracePreserves(states, actions);
    assert |actions| > 0;
    if actions[0] != FenceAcquisitionNames(true) {
      assert !states[1].namesFenced;
      DirectoryAuthorityRequiresAcknowledgedFence(states[1..], actions[1..]);
      var i :| 0 <= i < |actions[1..]| && actions[1..][i] == FenceAcquisitionNames(true);
      assert actions[i+1] == FenceAcquisitionNames(true);
    }
  }

  // RecoveryLease.Cleanup and ordinary disposal retain the receipt lease while
  // native deletion completes and its directory fence is acknowledged. Receipt
  // deletion and its separate acknowledgement follow before successful release.
  // Failed fences retain the available evidence; physical namespace rollback,
  // successor interleavings and unsuccessful retirement remain separate scopes.
  datatype DirectoryRetirementSnapshot = DirectoryRetirementSnapshot(
    protocol: Snapshot, nativeFenced: bool, receiptFenced: bool)
  datatype DirectoryRetirementAction = RetireOwnership(action: Action)
    | FenceNativeRemoval(success: bool) | FenceReceiptRemoval(success: bool)

  predicate DirectoryRetirementValid(s: DirectoryRetirementSnapshot) {
    Valid(s.protocol) && (s.nativeFenced ==> s.protocol.nativeName == Absent) &&
      (s.receiptFenced ==> s.nativeFenced && s.protocol.receiptName == Absent)
  }

  predicate DirectoryRetirementEnabled(s: DirectoryRetirementSnapshot, a: DirectoryRetirementAction) {
    match a
      case RetireOwnership(action) => Enabled(s.protocol, action) &&
        ((action.StartDispose?) || (action.DeleteNative? && !s.nativeFenced) ||
          (action.DeleteReceipt? && s.nativeFenced && !s.receiptFenced) ||
          (action.Release? && s.receiptFenced) || action.Crash? || action.Interrupt? || action.RepeatDispose?)
      case FenceNativeRemoval(success) => s.protocol.nativeName == Absent
      case FenceReceiptRemoval(success) => s.nativeFenced && s.protocol.receiptName == Absent
  }

  function ApplyDirectoryRetirement(s: DirectoryRetirementSnapshot, a: DirectoryRetirementAction): DirectoryRetirementSnapshot
    requires DirectoryRetirementEnabled(s, a)
  {
    match a
      case RetireOwnership(action) => s.(protocol := Apply(s.protocol, action))
      case FenceNativeRemoval(success) => s.(nativeFenced := s.nativeFenced || success)
      case FenceReceiptRemoval(success) => s.(receiptFenced := s.receiptFenced || success)
  }

  lemma DirectoryRetirementStepPreserves(s: DirectoryRetirementSnapshot, a: DirectoryRetirementAction)
    requires DirectoryRetirementValid(s) && DirectoryRetirementEnabled(s, a)
    ensures DirectoryRetirementValid(ApplyDirectoryRetirement(s, a))
    ensures a.FenceNativeRemoval? && !a.success ==> ApplyDirectoryRetirement(s, a) == s
    ensures a.FenceReceiptRemoval? && !a.success ==> ApplyDirectoryRetirement(s, a) == s
  {
    match a {
      case RetireOwnership(action) => StepPreserves(s.protocol, action);
      case FenceNativeRemoval(success) =>
      case FenceReceiptRemoval(success) =>
    }
  }

  predicate DirectoryRetirementPath(states: seq<DirectoryRetirementSnapshot>, actions: seq<DirectoryRetirementAction>) {
    |states| == |actions| + 1 && forall i | 0 <= i < |actions| ::
      DirectoryRetirementEnabled(states[i], actions[i]) && states[i+1] == ApplyDirectoryRetirement(states[i], actions[i])
  }

  lemma DirectoryRetirementTracePreserves(states: seq<DirectoryRetirementSnapshot>, actions: seq<DirectoryRetirementAction>)
    requires DirectoryRetirementPath(states, actions) && DirectoryRetirementValid(states[0])
    ensures forall i | 0 <= i < |states| :: DirectoryRetirementValid(states[i])
    decreases |actions|
  {
    if |actions| > 0 {
      DirectoryRetirementStepPreserves(states[0], actions[0]);
      DirectoryRetirementTracePreserves(states[1..], actions[1..]);
    }
  }

  lemma ReceiptRetirementRequiresNativeFence(s: DirectoryRetirementSnapshot, id: nat)
    requires DirectoryRetirementEnabled(s, RetireOwnership(DeleteReceipt(id)))
    ensures s.nativeFenced
  { }

  lemma GenerationReleaseRequiresBothFences(s: DirectoryRetirementSnapshot, id: nat)
    requires DirectoryRetirementValid(s) && DirectoryRetirementEnabled(s, RetireOwnership(Release(id)))
    ensures s.nativeFenced && s.receiptFenced
    ensures s.protocol.nativeName == Absent && s.protocol.receiptName == Absent
  { }

  function CleanupRank(s: Snapshot, id: nat): nat
    requires Valid(s) && (s.holder == Owned(id) || s.holder == Free)
  {
    (if s.holder == Free then 0 else 1) +
      (if s.holder == Owned(id) && s.cleaning == Free then 1 else 0) +
      (if s.nativeName == Absent then 0 else 1) + (if s.receiptName == Absent then 0 else 1)
  }

  lemma CompleteGenerationCleanup(s: Snapshot, id: nat)
    returns (states: seq<Snapshot>, actions: seq<Action>)
    requires Valid(s) && s.holder == Owned(id)
    requires s.cleaning == Owned(id) || (s.cleaning == Free && id !in s.attempted)
    ensures Path(states, actions) && states[0] == s
    ensures Valid(states[|states|-1]) && states[|states|-1].holder == Free
    ensures states[|states|-1].nativeName == Absent && states[|states|-1].receiptName == Absent
    decreases CleanupRank(s, id)
  {
    var a := if s.cleaning == Free then StartDispose(id)
      else if s.nativeName != Absent then DeleteNative(id)
      else if s.receiptName != Absent then DeleteReceipt(id) else Release(id);
    assert Enabled(s, a);
    StepPreserves(s, a);
    var successor := Apply(s, a);
    if a.Release? {
      states, actions := [s, successor], [a];
    } else {
      assert CleanupRank(successor, id) < CleanupRank(s, id);
      var tailStates, tailActions := CompleteGenerationCleanup(successor, id);
      states, actions := [s] + tailStates, [a] + tailActions;
      forall i | 0 <= i < |actions|
        ensures Enabled(states[i], actions[i]) && states[i+1] == Apply(states[i], actions[i])
      {
        if i != 0 {
          assert states[i] == tailStates[i-1];
          assert actions[i] == tailActions[i-1];
        }
      }
    }
  }
}
