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
}