// Verification-only cooperating-process model. No code is extracted.
// Each process is a distinct client of one canonical database identity. A ticket
// is a non-reused acquisition attempt, including after restarting the same client.
// Coordinate is an assumed atomic canonical-identity exclusion boundary. Its
// composite production acquisition, including an initially absent destination,
// remains unrefined. Cleanup effects model RecoveryLease.Cleanup after a selected
// database exists, with its lease retained while name handles close and names
// disappear. Exact evidence denotes validated generation/path bytes and
// completed fences; cryptography, directory durability, OS locks, native Access,
// malicious edits and delayed effects after crash quiescence are not proved.
// Ordinary RecoverableWriterLock.Dispose after the database handle closes is
// outside this schedule; its held-handle deletion has a separate model boundary.
module MultiProcessOwnership {
  datatype Evidence = Absent | Partial(generation: nat) | Exact(generation: nat)
  datatype Lease = Free | Held(process: nat, ticket: nat)
  datatype Phase = Idle | Queued | Coordinating | NativeWriting | NativeReady
    | ReceiptWriting | Active | Replaying | CleanupReady | NativeGone
    | NamesGone | Dead
  datatype Client = Client(phase: Phase, ticket: nat, generation: nat,
    observedNative: Evidence, observedReceipt: Evidence)
  datatype Snapshot = Snapshot(clients: map<nat,Client>, coordinator: Lease,
    native: Evidence, receipt: Evidence, usedTickets: set<nat>, generations: set<nat>)
  datatype Operation = Register | Queue | Observe | Coordinate | Fresh(generation: nat)
    | Recorded | CompleteNative | BeginReceipt | CompleteReceipt | BeginRecovery
    | Replay | CompleteRecovery | DeleteNative | DeleteReceipt | Release
    | Crash | Restart | Wait
  datatype Action = Action(process: nat, ticket: nat, operation: Operation)

  predicate Holding(phase: Phase) {
    phase != Idle && phase != Queued && phase != Dead
  }

  predicate Owns(s: Snapshot, process: nat, ticket: nat) {
    process in s.clients && Holding(s.clients[process].phase) &&
      s.clients[process].ticket == ticket && s.coordinator == Held(process,ticket)
  }

  predicate AuthorityOperation(operation: Operation) {
    operation.Fresh? || operation in {Recorded,CompleteNative,BeginReceipt,
      CompleteReceipt,BeginRecovery,Replay,CompleteRecovery,DeleteNative,
      DeleteReceipt,Release}
  }

  predicate GenerationPair(s: Snapshot, generation: nat) {
    s.native == Exact(generation) && s.receipt == Exact(generation)
  }

  predicate Valid(s: Snapshot) {
    (s.native != Absent ==> s.native.generation in s.generations) &&
    (s.receipt != Absent ==> s.receipt.generation in s.generations) &&
    (s.native != Absent && s.receipt != Absent ==>
      s.native.generation == s.receipt.generation) &&
    (s.coordinator.Held? ==> Owns(s,s.coordinator.process,s.coordinator.ticket)) &&
    (forall process | process in s.clients ::
      var c := s.clients[process];
      (Holding(c.phase) ==> Owns(s,process,c.ticket) && c.ticket in s.usedTickets) &&
      (c.phase == NativeWriting ==> (s.native == Partial(c.generation) &&
        (s.receipt == Absent || s.receipt == Exact(c.generation)))) &&
      (c.phase == NativeReady ==> s.native == Exact(c.generation) && s.receipt == Absent) &&
      (c.phase == ReceiptWriting ==> (s.native == Exact(c.generation) &&
        s.receipt == Partial(c.generation))) &&
      (c.phase == Active || c.phase == Replaying || c.phase == CleanupReady ==>
        GenerationPair(s,c.generation)) &&
      (c.phase == NativeGone ==> s.native == Absent && s.receipt == Exact(c.generation)) &&
      (c.phase == NamesGone ==> s.native == Absent && s.receipt == Absent))
  }

  predicate Enabled(s: Snapshot, a: Action) {
    if a.operation == Register then a.process !in s.clients
    else (a.process in s.clients &&
      (var c := s.clients[a.process];
       match a.operation
        case Register => false
        case Queue => c.phase == Idle
        case Observe => c.phase != Dead
        case Coordinate => c.phase == Queued && s.coordinator == Free &&
          a.ticket !in s.usedTickets
        case Crash => c.phase != Dead
        case Restart => c.phase == Dead
        case Wait => c.phase == Queued && s.coordinator != Free
        case _ => Owns(s,a.process,a.ticket) && (match a.operation
          case Fresh(generation) => c.phase == Coordinating && s.native == Absent &&
            s.receipt == Absent && generation !in s.generations
          case Recorded => c.phase == Coordinating && s.receipt.Exact? &&
            (s.native == Absent || s.native == Exact(s.receipt.generation))
          case CompleteNative => c.phase == NativeWriting
          case BeginReceipt => c.phase == NativeReady
          case CompleteReceipt => c.phase == ReceiptWriting
          case BeginRecovery => c.phase == Active
          case Replay => c.phase == Replaying
          case CompleteRecovery => c.phase == Replaying
          case DeleteNative => c.phase == CleanupReady
          case DeleteReceipt => c.phase == NativeGone
          case Release => true
          case _ => false)))
  }

  function Apply(s: Snapshot, a: Action): Snapshot
    requires Enabled(s,a)
  {
    if a.operation == Register then
      s.(clients := s.clients[a.process := Client(Idle,0,0,Absent,Absent)])
    else
      var c := s.clients[a.process];
      match a.operation
        case Register => s
        case Queue => s.(clients := s.clients[a.process := c.(phase := Queued)])
        case Observe => s.(clients := s.clients[a.process :=
          c.(observedNative := s.native, observedReceipt := s.receipt)])
        case Coordinate => s.(coordinator := Held(a.process,a.ticket),
          usedTickets := s.usedTickets + {a.ticket},
          clients := s.clients[a.process := c.(phase := Coordinating,ticket := a.ticket)])
        case Fresh(generation) => s.(native := Partial(generation),
          generations := s.generations + {generation},
          clients := s.clients[a.process := c.(phase := NativeWriting,generation := generation)])
        case Recorded => s.(native := if s.native == Absent then
            Partial(s.receipt.generation) else s.native,
          clients := s.clients[a.process := c.(generation := s.receipt.generation,
            phase := if s.native == Absent then NativeWriting else Active)])
        case CompleteNative => s.(native := Exact(c.generation),
          clients := s.clients[a.process := c.(phase :=
            if s.receipt == Absent then NativeReady else Active)])
        case BeginReceipt => s.(receipt := Partial(c.generation),
          clients := s.clients[a.process := c.(phase := ReceiptWriting)])
        case CompleteReceipt => s.(receipt := Exact(c.generation),
          clients := s.clients[a.process := c.(phase := Active)])
        case BeginRecovery => s.(clients := s.clients[a.process := c.(phase := Replaying)])
        case Replay => s
        // This successful effect stands for selected-image validation and a
        // completed destination flush. Failed or uncertain I/O cannot take it.
        case CompleteRecovery => s.(clients := s.clients[a.process := c.(phase := CleanupReady)])
        // Closing native before unlink does not release database coordination.
        case DeleteNative => s.(native := Absent,
          clients := s.clients[a.process := c.(phase := NativeGone)])
        // Closing receipt before unlink likewise retains database coordination.
        case DeleteReceipt => s.(receipt := Absent,
          clients := s.clients[a.process := c.(phase := NamesGone)])
        case Release => s.(coordinator := Free,
          clients := s.clients[a.process := c.(phase := Idle)])
        case Crash => s.(coordinator := if s.coordinator.Held? &&
            s.coordinator.process == a.process then Free else s.coordinator,
          clients := s.clients[a.process := c.(phase := Dead)])
        case Restart => s.(clients := s.clients[a.process := c.(phase := Idle,
          observedNative := Absent,observedReceipt := Absent)])
        case Wait => s
  }

  lemma StepPreserves(s: Snapshot, a: Action)
    requires Valid(s) && Enabled(s,a)
    ensures Valid(Apply(s,a))
    ensures s.clients.Keys <= Apply(s,a).clients.Keys
    ensures s.usedTickets <= Apply(s,a).usedTickets
    ensures s.generations <= Apply(s,a).generations
    ensures forall other | other in s.clients && other != a.process ::
      Apply(s,a).clients[other] == s.clients[other]
  {
    var next := Apply(s,a);
    if a.process in s.clients && Holding(s.clients[a.process].phase) {
      assert forall other | other in s.clients && other != a.process ::
        !Holding(s.clients[other].phase);
    }
    forall other | other in next.clients
      ensures var c := next.clients[other];
        (Holding(c.phase) ==> Owns(next,other,c.ticket) && c.ticket in next.usedTickets) &&
        (c.phase == NativeWriting ==> (next.native == Partial(c.generation) &&
          (next.receipt == Absent || next.receipt == Exact(c.generation)))) &&
        (c.phase == NativeReady ==> next.native == Exact(c.generation) && next.receipt == Absent) &&
        (c.phase == ReceiptWriting ==> (next.native == Exact(c.generation) &&
          next.receipt == Partial(c.generation))) &&
        (c.phase == Active || c.phase == Replaying || c.phase == CleanupReady ==>
          GenerationPair(next,c.generation)) &&
        (c.phase == NativeGone ==> next.native == Absent && next.receipt == Exact(c.generation)) &&
        (c.phase == NamesGone ==> next.native == Absent && next.receipt == Absent)
    {
      if other != a.process {
        assert other in s.clients;
      }
    }
  }

  predicate Path(states: seq<Snapshot>, actions: seq<Action>) {
    |states| == |actions| + 1 &&
    forall i | 0 <= i < |actions| :: Enabled(states[i],actions[i]) &&
      states[i+1] == Apply(states[i],actions[i])
  }

  lemma TracePreserves(states: seq<Snapshot>, actions: seq<Action>)
    requires Path(states,actions) && Valid(states[0])
    ensures forall i | 0 <= i < |states| :: Valid(states[i])
    ensures states[0].usedTickets <= states[|states|-1].usedTickets
    ensures states[0].generations <= states[|states|-1].generations
    decreases |actions|
  {
    if |actions| != 0 {
      StepPreserves(states[0],actions[0]);
      TracePreserves(states[1..],actions[1..]);
    }
  }

  lemma EmptyEstablishesInvariant()
    ensures Valid(Snapshot(map[],Free,Absent,Absent,{},{}))
  {}

  lemma AtMostOneCoordinatedClient(s: Snapshot, first: nat, second: nat)
    requires Valid(s) && first in s.clients && second in s.clients
    requires Holding(s.clients[first].phase) && Holding(s.clients[second].phase)
    ensures first == second
  {}

  lemma MutationRequiresCurrentTicket(s: Snapshot, a: Action)
    requires Valid(s) && Enabled(s,a)
    ensures s.native != Apply(s,a).native || s.receipt != Apply(s,a).receipt ==>
      Owns(s,a.process,a.ticket)
  {}

  lemma MissingNamesDoNotAuthorizeSuccessor(s: Snapshot, owner: nat, other: nat, ticket: nat)
    requires Valid(s) && owner in s.clients && other in s.clients && owner != other
    requires s.clients[owner].phase == NamesGone && s.clients[other].phase == Queued
    ensures s.native == Absent && s.receipt == Absent
    ensures !Enabled(s,Action(other,ticket,Coordinate))
    ensures Enabled(s,Action(other,ticket,Wait))
    ensures Apply(s,Action(other,ticket,Wait)) == s
  {}

  lemma ObservationDoesNotGrantAuthority(s: Snapshot, process: nat, ticket: nat)
    requires Valid(s) && Enabled(s,Action(process,ticket,Observe))
    ensures Apply(s,Action(process,ticket,Observe)).coordinator == s.coordinator
    ensures Owns(Apply(s,Action(process,ticket,Observe)),process,ticket) <==>
      Owns(s,process,ticket)
  {}

  lemma StaleTicketCannotAct(s: Snapshot, process: nat, stale: nat, operation: Operation)
    requires process in s.clients && s.clients[process].ticket != stale
    requires AuthorityOperation(operation)

    ensures !Enabled(s,Action(process,stale,operation))
  {}

  lemma UsedTicketCannotBeReacquired(states: seq<Snapshot>, actions: seq<Action>,
    process: nat, ticket: nat)
    requires Path(states,actions) && Valid(states[0])
    requires ticket in states[0].usedTickets
    ensures !Enabled(states[|states|-1],Action(process,ticket,Coordinate))
  {
    TracePreserves(states,actions);
  }

  lemma CrashRevokesAllEffectAuthority(s: Snapshot, process: nat, ticket: nat,
    operation: Operation)
    requires Enabled(s,Action(process,ticket,Crash)) && AuthorityOperation(operation)
    ensures !Enabled(Apply(s,Action(process,ticket,Crash)),Action(process,ticket,operation))
  {}

  lemma IncompleteBootstrapRefuses(s: Snapshot, process: nat, ticket: nat, generation: nat)
    requires s.native.Partial? || s.receipt.Partial? ||
      (s.native != Absent && s.receipt == Absent)
    ensures !Enabled(s,Action(process,ticket,Fresh(generation)))
    ensures !Enabled(s,Action(process,ticket,Recorded))
  {}

  lemma CrashRetainsEvidence(s: Snapshot, process: nat, ticket: nat)
    requires Valid(s) && Enabled(s,Action(process,ticket,Crash))
    ensures Apply(s,Action(process,ticket,Crash)).native == s.native
    ensures Apply(s,Action(process,ticket,Crash)).receipt == s.receipt
    ensures Apply(s,Action(process,ticket,Crash)).clients[process].phase == Dead
    ensures s.coordinator.Held? && s.coordinator.process != process ==>
      Apply(s,Action(process,ticket,Crash)).coordinator == s.coordinator
  {}

  lemma FailedOperationRetainsEvidence(s: Snapshot, process: nat, ticket: nat)
    requires Valid(s) && Owns(s,process,ticket)
    ensures Enabled(s,Action(process,ticket,Release))
    ensures Apply(s,Action(process,ticket,Release)).native == s.native
    ensures Apply(s,Action(process,ticket,Release)).receipt == s.receipt
    ensures !Owns(Apply(s,Action(process,ticket,Release)),process,ticket)
  {}

  lemma SuccessfulCleanupCompletes(s: Snapshot, process: nat, ticket: nat)
    returns (states: seq<Snapshot>, actions: seq<Action>)
    requires Valid(s) && Owns(s,process,ticket)
    requires s.clients[process].phase == Active
    ensures Path(states,actions) && states[0] == s && |actions| == 5
    ensures Valid(states[|states|-1]) && states[|states|-1].coordinator == Free
    ensures states[|states|-1].native == Absent && states[|states|-1].receipt == Absent
    ensures process in states[|states|-1].clients
    ensures states[|states|-1].clients[process].phase == Idle
  {
    var begin := Action(process,ticket,BeginRecovery);
    var replaying := Apply(s,begin);
    StepPreserves(s,begin);
    var finish := Action(process,ticket,CompleteRecovery);
    var ready := Apply(replaying,finish);
    StepPreserves(replaying,finish);
    var unlinkNative := Action(process,ticket,DeleteNative);
    var nativeGone := Apply(ready,unlinkNative);
    StepPreserves(ready,unlinkNative);
    var unlinkReceipt := Action(process,ticket,DeleteReceipt);
    var namesGone := Apply(nativeGone,unlinkReceipt);
    StepPreserves(nativeGone,unlinkReceipt);
    var release := Action(process,ticket,Release);
    var done := Apply(namesGone,release);
    StepPreserves(namesGone,release);
    states, actions := [s,replaying,ready,nativeGone,namesGone,done],
      [begin,finish,unlinkNative,unlinkReceipt,release];
  }

  lemma FreshAcquisitionCompletes(s: Snapshot, process: nat, ticket: nat, generation: nat)
    returns (states: seq<Snapshot>, actions: seq<Action>)
    requires Valid(s) && Enabled(s,Action(process,ticket,Fresh(generation)))
    ensures Path(states,actions) && states[0] == s && |actions| == 4
    ensures Valid(states[|states|-1]) && Owns(states[|states|-1],process,ticket)
    ensures states[|states|-1].clients[process].phase == Active
    ensures GenerationPair(states[|states|-1],generation)
  {
    var begin := Action(process,ticket,Fresh(generation));
    var writing := Apply(s,begin);
    StepPreserves(s,begin);
    var native := Action(process,ticket,CompleteNative);
    var nativeReady := Apply(writing,native);
    StepPreserves(writing,native);
    var receipt := Action(process,ticket,BeginReceipt);
    var receiptWriting := Apply(nativeReady,receipt);
    StepPreserves(nativeReady,receipt);
    var complete := Action(process,ticket,CompleteReceipt);
    var ready := Apply(receiptWriting,complete);
    StepPreserves(receiptWriting,complete);
    states, actions := [s,writing,nativeReady,receiptWriting,ready],
      [begin,native,receipt,complete];
  }

  lemma RecordedAcquisitionCompletes(s: Snapshot, process: nat, ticket: nat)
    returns (states: seq<Snapshot>, actions: seq<Action>)
    requires Valid(s) && Enabled(s,Action(process,ticket,Recorded))
    ensures Path(states,actions) && states[0] == s && 1 <= |actions| <= 2
    ensures Valid(states[|states|-1]) && Owns(states[|states|-1],process,ticket)
    ensures states[|states|-1].clients[process].phase == Active
    ensures states[|states|-1].receipt == s.receipt
  {
    var begin := Action(process,ticket,Recorded);
    var acquired := Apply(s,begin);
    StepPreserves(s,begin);
    if acquired.clients[process].phase == NativeWriting {
      var complete := Action(process,ticket,CompleteNative);
      var ready := Apply(acquired,complete);
      StepPreserves(acquired,complete);
      states, actions := [s,acquired,ready],[begin,complete];
    } else {
      states, actions := [s,acquired],[begin];
    }
  }

  // A checked negative control for dropping database coordination before the
  // cleanup callback has returned: both names are absent, but an old client is
  // still in its cleanup scope. Admitting another client violates exclusion.
  lemma EarlyCoordinatorReleaseAllowsOverlappingScopes()
    returns (before: Snapshot, weakened: Snapshot)
    ensures Valid(before)
    ensures 0 in before.clients && 1 in before.clients
    ensures 0 in weakened.clients && 1 in weakened.clients
    ensures before.clients[0].phase == NamesGone && before.clients[1].phase == Queued
    ensures before.native == Absent && before.receipt == Absent
    ensures !Valid(weakened)
    ensures Holding(weakened.clients[0].phase) && Holding(weakened.clients[1].phase)
  {
    var clients := map[0 := Client(NamesGone,10,100,Absent,Absent),
      1 := Client(Queued,0,0,Absent,Absent)];
    before := Snapshot(clients,Held(0,10),Absent,Absent,{10},{100});
    weakened := before.(coordinator := Held(1,11),usedTickets := {10,11},
      clients := clients[1 := clients[1].(phase := Coordinating,ticket := 11)]);
    assert !Owns(weakened,0,10);
  }

  // A stale pathname cleanup that ignores exact-generation/current-ticket
  // authority would erase a successor. The admitted transition refuses it.
  lemma StaleCleanupCounterexample()
    returns (s: Snapshot, erased: Snapshot)
    ensures Valid(s) && GenerationPair(s,200)
    ensures 0 in s.clients
    ensures s.clients[0].observedReceipt == Exact(100)
    ensures !Enabled(s,Action(0,10,DeleteReceipt))
    ensures erased.receipt == Absent && !Valid(erased)
  {
    var clients := map[0 := Client(Idle,10,100,Exact(100),Exact(100)),
      1 := Client(Active,11,200,Exact(200),Exact(200))];
    s := Snapshot(clients,Held(1,11),Exact(200),Exact(200),{10,11},{100,200});
    erased := s.(receipt := Absent);
  }
}
