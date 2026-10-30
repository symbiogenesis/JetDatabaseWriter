// Verification-only ownership ledger for one immutable pooled page frame.
// Actions describe reference-count transitions and producer-to-cache handoff;
// cache lookup/acquisition, CLR object identity, allocation, pooling and actual
// C# synchronization remain manual correspondence obligations. No code ships.
module PageFrameOwnership {
  const MaxReferences: nat := 2147483647

  datatype Snapshot = Snapshot(bytes: seq<bv8>, generation: nat,
    cached: bool, producer: bool, leases: set<nat>, issued: nat,
    references: nat, poolReturns: nat)
  datatype Action = Acquire | Release(ticket: nat) | Retain(current: nat)
    | Detach | DropProducer

  predicate Valid(s: Snapshot) {
    s.references == |s.leases| + (if s.cached || s.producer then 1 else 0) &&
    !(s.cached && s.producer) &&
    s.references <= MaxReferences && s.poolReturns <= 1 &&
    (s.poolReturns == 1 <==> s.references == 0) &&
    (forall ticket | ticket in s.leases :: ticket < s.issued)
  }

  function Cached(bytes: seq<bv8>, generation: nat): Snapshot {
    Snapshot(bytes, generation, true, false, {}, 0, 1, 0)
  }

  function Uncached(bytes: seq<bv8>, generation: nat): Snapshot {
    Snapshot(bytes, generation, false, false, {0}, 1, 1, 0)
  }

  function Created(bytes: seq<bv8>, generation: nat): Snapshot {
    Snapshot(bytes, generation, false, true, {}, 0, 1, 0)
  }

  predicate Enabled(s: Snapshot, a: Action) {
    match a
      case Acquire => 0 < s.references < MaxReferences
      case Release(_) => true
      case Retain(current) => s.producer && !s.cached && s.generation == current
      case Detach => true
      case DropProducer => true
  }

  function Apply(s: Snapshot, a: Action): Snapshot
    requires Valid(s) && Enabled(s,a)
  {
    match a
      case Acquire => s.(leases := s.leases + {s.issued}, issued := s.issued+1,
        references := s.references+1)
      case Release(ticket) =>
        if ticket !in s.leases then s
        else s.(leases := s.leases - {ticket}, references := s.references-1,
          poolReturns := if s.references == 1 then 1 else 0)
      case Retain(_) => s.(cached := true, producer := false)
      case Detach =>
        if !s.cached then s
        else s.(cached := false, references := s.references-1,
          poolReturns := if s.references == 1 then 1 else 0)
      case DropProducer =>
        if !s.producer then s
        else s.(producer := false, references := s.references-1,
          poolReturns := if s.references == 1 then 1 else 0)
  }

  lemma InitialFramesAreValid(bytes: seq<bv8>, generation: nat)
    ensures Valid(Cached(bytes,generation))
    ensures Valid(Uncached(bytes,generation))
    ensures Valid(Created(bytes,generation))
  {}

  lemma StepPreserves(s: Snapshot, a: Action)
    requires Valid(s) && Enabled(s,a)
    ensures Valid(Apply(s,a))
    ensures Apply(s,a).bytes == s.bytes
    ensures Apply(s,a).generation == s.generation
    ensures s.poolReturns <= Apply(s,a).poolReturns <= 1
  {
    match a
      case Acquire =>
        assert s.issued !in s.leases;
        assert |s.leases + {s.issued}| == |s.leases|+1;
      case Release(ticket) =>
        if ticket in s.leases {
          assert |s.leases - {ticket}| == |s.leases|-1;
        }
      case Retain(_) =>
      case Detach =>
      case DropProducer =>
  }

  predicate Admitted(s: Snapshot, actions: seq<Action>)
    decreases |actions|
  {
    Valid(s) && (|actions| == 0 ||
      (Enabled(s,actions[0]) && Admitted(Apply(s,actions[0]),actions[1..])))
  }

  function Run(s: Snapshot, actions: seq<Action>): Snapshot
    requires Admitted(s,actions)
    decreases |actions|
  {
    if |actions| == 0 then s else Run(Apply(s,actions[0]),actions[1..])
  }

  lemma TracePreserves(s: Snapshot, actions: seq<Action>)
    requires Admitted(s,actions)
    ensures Valid(Run(s,actions))
    ensures Run(s,actions).bytes == s.bytes
    ensures Run(s,actions).generation == s.generation
    ensures s.poolReturns <= Run(s,actions).poolReturns <= 1
    decreases |actions|
  {
    if |actions| > 0 {
      StepPreserves(s,actions[0]);
      TracePreserves(Apply(s,actions[0]),actions[1..]);
    }
  }

  lemma PinnedFrameCannotReturn(s: Snapshot, ticket: nat)
    requires Valid(s) && ticket in s.leases
    ensures s.references > 0 && s.poolReturns == 0
  {}

  lemma LastOwnerReturnsExactlyOnce(s: Snapshot, a: Action)
    requires Valid(s) && Enabled(s,a)
    requires s.references == 1 && Apply(s,a).references == 0
    ensures s.poolReturns == 0 && Apply(s,a).poolReturns == 1
  {
    StepPreserves(s,a);
  }

  lemma RepeatedLeaseDisposalIsInert(s: Snapshot, ticket: nat)
    requires Valid(s)
    ensures Apply(Apply(s,Release(ticket)),Release(ticket)) == Apply(s,Release(ticket))
  {
    StepPreserves(s,Release(ticket));
  }

  lemma RepeatedCacheDetachIsInert(s: Snapshot)
    requires Valid(s)
    ensures Apply(Apply(s,Detach),Detach) == Apply(s,Detach)
  {
    StepPreserves(s,Detach);
  }

  lemma ReturnedOrFullFrameRefusesAcquisition(s: Snapshot)
    requires Valid(s) && (s.references == 0 || s.references == MaxReferences)
    ensures !Enabled(s,Acquire)
  {}

  lemma OldGenerationCannotBeRetained(s: Snapshot, current: nat)
    requires Valid(s) && s.generation != current
    ensures !Enabled(s,Retain(current))
  {}

  lemma ProducerHandoffPreservesReferenceCount(s: Snapshot, current: nat)
    requires Valid(s) && Enabled(s,Retain(current))
    ensures Apply(s,Retain(current)).references == s.references
    ensures Apply(s,Retain(current)).cached
    ensures !Apply(s,Retain(current)).producer
  {}

  lemma EvictionRetainsExistingLease(s: Snapshot, ticket: nat)
    requires Valid(s) && ticket in s.leases
    ensures ticket in Apply(s,Detach).leases
    ensures Apply(s,Detach).bytes == s.bytes
    ensures Apply(s,Detach).poolReturns == 0
  {
    StepPreserves(s,Detach);
    PinnedFrameCannotReturn(Apply(s,Detach),ticket);
  }

  lemma LeaseLifetimeCompletes(bytes: seq<bv8>, generation: nat)
    ensures Admitted(Cached(bytes,generation),
      [Acquire,Acquire,Detach,Release(0),Release(0),Release(1),Release(1)])
    ensures Run(Cached(bytes,generation),
      [Acquire,Acquire,Detach,Release(0),Release(0),Release(1),Release(1)]).poolReturns == 1
    ensures !Enabled(Run(Cached(bytes,generation),
      [Acquire,Acquire,Detach,Release(0),Release(0),Release(1),Release(1)]),Acquire)
  {}

  lemma EarlyReturnWithLiveLeaseIsInvalid(bytes: seq<bv8>, generation: nat)
    ensures !Valid(Uncached(bytes,generation).(poolReturns := 1))
  {}

  lemma RetainedProductionReadCompletes(bytes: seq<bv8>, generation: nat)
    ensures Admitted(Created(bytes,generation),
      [Acquire,Retain(generation),Acquire,Detach,Release(0),Release(1)])
    ensures Run(Created(bytes,generation),
      [Acquire,Retain(generation),Acquire,Detach,Release(0),Release(1)]).poolReturns == 1
  {
    var actions := [Acquire,Retain(generation),Acquire,Detach,Release(0),Release(1)];
    var s0 := Created(bytes,generation);
    InitialFramesAreValid(bytes,generation);
    StepPreserves(s0,Acquire);
    var s1 := Apply(s0,Acquire);
    StepPreserves(s1,Retain(generation));
    var s2 := Apply(s1,Retain(generation));
    StepPreserves(s2,Acquire);
    var s3 := Apply(s2,Acquire);
    StepPreserves(s3,Detach);
    var s4 := Apply(s3,Detach);
    StepPreserves(s4,Release(0));
    var s5 := Apply(s4,Release(0));
    StepPreserves(s5,Release(1));
    var s6 := Apply(s5,Release(1));
    assert Admitted(s5,actions[5..]);
    assert Admitted(s4,actions[4..]);
    assert Admitted(s3,actions[3..]);
    assert Admitted(s2,actions[2..]);
    assert Admitted(s1,actions[1..]);
    assert Admitted(s0,actions);
    assert Run(s5,actions[5..]) == s6;
    assert Run(s4,actions[4..]) == s6;
    assert Run(s3,actions[3..]) == s6;
    assert Run(s2,actions[2..]) == s6;
    assert Run(s1,actions[1..]) == s6;
    assert Run(s0,actions) == s6;
  }

  lemma UnretainedProductionReadCompletes(bytes: seq<bv8>, generation: nat)
    ensures Admitted(Created(bytes,generation),[Acquire,DropProducer,Release(0)])
    ensures Run(Created(bytes,generation),[Acquire,DropProducer,Release(0)]).poolReturns == 1
  {}
}
