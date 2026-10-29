// Verification-only lifecycle specification. Images and auxiliary writer-state
// tokens denote exact logical snapshots, not physical I/O. Calls are serialized;
// successful restore/commit effects, exclusive leases and non-reused IDs are
// premises. GenerationOwnership supplies exact-generation deletion authority.
// No code is extracted, and no OS lock, cache-pin or C# refinement is claimed.
module WriterLifecycle {
  import G = GenerationOwnership

  datatype Phase = Open | Faulted | Disposing | Disposed
  datatype Status = Active | Committed | RolledBack | Failed
  datatype Completion = Commit | Rollback | DisposeTransaction
  datatype Saved = Saved(image: seq<int>, auxiliary: nat)
  datatype Statement = NoStatement | Savepoint(saved: Saved)
  datatype Frame = Frame(id: nat, saved: Saved)
  datatype Snapshot = Snapshot(phase: Phase, owner: nat, ownership: G.Snapshot,
    scopes: seq<Frame>, status: map<nat,Status>, committed: seq<int>, view: Saved,
    known: bool, statement: Statement, generation: nat, retained: set<nat>)
  datatype Action = Begin(id: nat) | BeginStatement | Change(next: Saved)
    | EndStatement(success: bool) | Finish(id: nat, operation: Completion, result: Status)
    | RepeatTransactionDispose(id: nat) | Invalidate | Retain(ticket: nat)
    | StartWriterDispose | DisposeTop | AbandonFailedDisposal
    | DeleteNative | DeleteReceipt | FinishWriterDispose | RepeatWriterDispose

  const MaxGeneration: nat := 9223372036854775807

  predicate Held(s: Snapshot) {
    s.ownership.holder == G.Owned(s.owner) && s.owner in s.ownership.owners
  }

  predicate IsTop(s: Snapshot, id: nat) {
    |s.scopes| > 0 && s.scopes[|s.scopes|-1].id == id
  }

  predicate Valid(s: Snapshot) {
    G.Valid(s.ownership) && |s.scopes| <= 7 && s.generation <= MaxGeneration &&
    (s.phase == Open ==> s.known) &&
    (s.phase == Open || s.phase == Faulted ==> s.owner !in s.ownership.attempted) &&
    (s.phase != Disposed ==> Held(s)) &&
    (s.phase == Disposed ==> s.ownership.holder == G.Free && |s.scopes| == 0) &&
    (s.phase == Faulted ==> |s.scopes| == 0 && !s.known) &&
    (s.phase == Disposing ==> s.ownership.cleaning == G.Owned(s.owner)) &&
    (s.phase != Disposing ==> s.ownership.cleaning == G.Free) &&
    (s.statement.Savepoint? ==> s.phase == Open && 1 <= |s.scopes| <= 5) &&
    (forall i | 0 <= i < |s.scopes| ::
      s.scopes[i].id in s.status && s.status[s.scopes[i].id] == Active) &&
    (forall i,j | 0 <= i < j < |s.scopes| :: s.scopes[i].id != s.scopes[j].id) &&
    (forall id | id in s.status && s.status[id] == Active ::
      exists i :: 0 <= i < |s.scopes| && s.scopes[i].id == id) &&
    (s.known && |s.scopes| == 0 ==> s.view.image == s.committed) &&
    (s.known && |s.scopes| > 0 ==> s.scopes[0].saved.image == s.committed) &&
    s.retained <= {s.generation}
  }

  predicate AdvancesGeneration(a: Action) {
    a == Invalidate || a == DisposeTop || a == AbandonFailedDisposal ||
    (a.EndStatement? && !a.success) || (a.Finish? && a.result != Committed)
  }

  predicate Enabled(s: Snapshot, a: Action) {
    (AdvancesGeneration(a) ==> s.generation < MaxGeneration) && (match a
      case Begin(id) => s.phase == Open && s.statement == NoStatement &&
        |s.scopes| < 7 && id !in s.status
      case BeginStatement => s.phase == Open && s.statement == NoStatement &&
        1 <= |s.scopes| <= 5
      case Change(next) => s.phase == Open && s.statement.Savepoint?
      case EndStatement(success) => s.phase == Open && s.statement.Savepoint?
      case Finish(id, operation, result) => s.phase == Open &&
        s.statement == NoStatement && IsTop(s,id) && result != Active &&
        (operation != Commit ==> result != Committed) &&
        (result == Failed ==> |s.scopes| == 1)
      case RepeatTransactionDispose(id) => id in s.status && s.status[id] != Active
      case Invalidate => s.phase != Disposed
      case Retain(ticket) => s.phase != Disposed && ticket == s.generation
      case StartWriterDispose => (s.phase == Open || s.phase == Faulted) &&
        s.statement == NoStatement && G.Enabled(s.ownership,G.StartDispose(s.owner))
      case DisposeTop => s.phase == Disposing && |s.scopes| > 0
      case AbandonFailedDisposal => s.phase == Disposing && |s.scopes| == 1
      case DeleteNative => s.phase == Disposing && |s.scopes| == 0 &&
        G.Enabled(s.ownership,G.DeleteNative(s.owner))
      case DeleteReceipt => s.phase == Disposing && |s.scopes| == 0 &&
        G.Enabled(s.ownership,G.DeleteReceipt(s.owner))
      case FinishWriterDispose => s.phase == Disposing && |s.scopes| == 0 &&
        G.Enabled(s.ownership,G.Release(s.owner))
      case RepeatWriterDispose => s.phase == Disposed)
  }

  function ClearGeneration(s: Snapshot): Snapshot {
    s.(generation := s.generation+1, retained := {})
  }

  function Resolve(s: Snapshot, result: Status): Snapshot
    requires |s.scopes| > 0
    requires result != Active
  {
    var top := s.scopes[|s.scopes|-1];
    var ended := s.(scopes := s.scopes[..|s.scopes|-1], status := s.status[top.id := result]);
    if result == Committed then
      if |s.scopes| == 1 then ended.(committed := s.view.image) else ended
    else if result == RolledBack then ClearGeneration(ended.(view := top.saved))
    else ClearGeneration(ended.(phase := Faulted, known := false))
  }

  function Apply(s: Snapshot, a: Action): Snapshot
    requires Enabled(s,a)
  {
    match a
      case Begin(id) => s.(scopes := s.scopes + [Frame(id,s.view)], status := s.status[id := Active])
      case BeginStatement => s.(statement := Savepoint(s.view))
      case Change(next) => s.(view := next)
      case EndStatement(success) =>
        if success then s.(statement := NoStatement)
        else ClearGeneration(s.(statement := NoStatement, view := s.statement.saved))
      case Finish(id, operation, result) => Resolve(s,result)
      case RepeatTransactionDispose(id) => s
      case Invalidate => ClearGeneration(s)
      case Retain(ticket) => s.(retained := s.retained + {ticket})
      case StartWriterDispose => s.(phase := Disposing,
        ownership := G.Apply(s.ownership,G.StartDispose(s.owner)))
      case DisposeTop => Resolve(s,RolledBack)
      case AbandonFailedDisposal => ClearGeneration(s.(scopes := [], known := false,
        status := map id | id in s.status :: if s.status[id] == Active then Failed else s.status[id]))
      case DeleteNative => s.(ownership := G.Apply(s.ownership,G.DeleteNative(s.owner)))
      case DeleteReceipt => s.(ownership := G.Apply(s.ownership,G.DeleteReceipt(s.owner)))
      case FinishWriterDispose => s.(phase := Disposed,
        ownership := G.Apply(s.ownership,G.Release(s.owner)))
      case RepeatWriterDispose => s
  }

  lemma ResolvePreserves(s: Snapshot, result: Status)
    requires Valid(s) && |s.scopes| > 0 && result != Active && s.statement == NoStatement
    requires result != Committed ==> s.generation < MaxGeneration
    requires result == Failed ==> s.phase == Open && |s.scopes| == 1
    ensures Valid(Resolve(s,result))
    ensures Resolve(s,result).status.Keys == s.status.Keys
    ensures |Resolve(s,result).scopes|+1 == |s.scopes|
  {
    var top := s.scopes[|s.scopes|-1];
    assert top.id in s.status;
    forall id | id in Resolve(s,result).status && Resolve(s,result).status[id] == Active
      ensures exists i :: 0 <= i < |Resolve(s,result).scopes| && Resolve(s,result).scopes[i].id == id
    {
      var i :| 0 <= i < |s.scopes| && s.scopes[i].id == id;
      assert id != top.id;
      assert i < |s.scopes|-1;
    }
  }

  lemma StepPreserves(s: Snapshot, a: Action)
    requires Valid(s) && Enabled(s,a)
    ensures Valid(Apply(s,a))
    ensures Apply(s,a).owner == s.owner
    ensures s.status.Keys <= Apply(s,a).status.Keys
    ensures s.generation <= Apply(s,a).generation
    ensures forall id | id in s.status && s.status[id] != Active ::
      Apply(s,a).status[id] == s.status[id]
  {
    match a {
      case Begin(id) =>
        forall token | token in Apply(s,a).status && Apply(s,a).status[token] == Active
          ensures exists i :: 0 <= i < |Apply(s,a).scopes| && Apply(s,a).scopes[i].id == token
        {
          if token == id {
            assert Apply(s,a).scopes[|s.scopes|].id == token;
          } else {
            var i :| 0 <= i < |s.scopes| && s.scopes[i].id == token;
            assert Apply(s,a).scopes[i].id == token;
          }
        }
      case BeginStatement =>
      case Change(next) =>
      case EndStatement(success) =>
      case Finish(id, operation, result) => ResolvePreserves(s,result);
      case RepeatTransactionDispose(id) =>
      case Invalidate =>
      case Retain(ticket) =>
      case StartWriterDispose => G.StepPreserves(s.ownership,G.StartDispose(s.owner));
      case DisposeTop => ResolvePreserves(s,RolledBack);
      case AbandonFailedDisposal =>
      case DeleteNative => G.StepPreserves(s.ownership,G.DeleteNative(s.owner));
      case DeleteReceipt => G.StepPreserves(s.ownership,G.DeleteReceipt(s.owner));
      case FinishWriterDispose => G.StepPreserves(s.ownership,G.Release(s.owner));
      case RepeatWriterDispose =>
    }
  }

  function Initial(owner: nat, ownership: G.Snapshot, image: seq<int>, auxiliary: nat): Snapshot {
    Snapshot(Open,owner,ownership,[],map[],image,Saved(image,auxiliary),true,NoStatement,0,{})
  }

  lemma ValidInitial(owner: nat, ownership: G.Snapshot, image: seq<int>, auxiliary: nat)
    requires G.Valid(ownership) && ownership.holder == G.Owned(owner)
    requires ownership.cleaning == G.Free && owner !in ownership.attempted
    ensures Valid(Initial(owner,ownership,image,auxiliary))
  {}

  lemma SurvivingScopeSnapshotIsImmutable(s: Snapshot, a: Action, index: nat)
    requires Valid(s) && Enabled(s,a)
    requires index < |s.scopes| && index < |Apply(s,a).scopes|
    ensures Apply(s,a).scopes[index] == s.scopes[index]
  {}

  lemma DepthLimitsRefuseWithoutResolution(s: Snapshot, id: nat)
    requires Valid(s)
    ensures |s.scopes| == 7 ==> !Enabled(s,Begin(id))
    ensures |s.scopes| > 5 ==> !Enabled(s,BeginStatement)
  {}

  predicate Path(states: seq<Snapshot>, actions: seq<Action>) {
    |states| == |actions|+1 &&
    forall i | 0 <= i < |actions| :: Enabled(states[i],actions[i]) &&
      states[i+1] == Apply(states[i],actions[i])
  }

  lemma TracePreserves(states: seq<Snapshot>, actions: seq<Action>)
    requires Path(states,actions) && Valid(states[0])
    ensures Valid(states[|states|-1])
    ensures states[0].owner == states[|states|-1].owner
    ensures states[0].generation <= states[|states|-1].generation
    ensures states[0].status.Keys <= states[|states|-1].status.Keys
    ensures forall id | id in states[0].status && states[0].status[id] != Active ::
      states[|states|-1].status[id] == states[0].status[id]
    decreases |actions|
  {
    if |actions| > 0 {
      StepPreserves(states[0],actions[0]);
      TracePreserves(states[1..],actions[1..]);
    }
  }

  lemma ResolutionRequiresInnermostToken(s: Snapshot, id: nat, operation: Completion, result: Status)
    requires Valid(s) && !IsTop(s,id)
    ensures !Enabled(s,Finish(id,operation,result))
  {}

  lemma ReleasedChildDoesNotPublish(s: Snapshot, id: nat)
    requires Valid(s) && Enabled(s,Finish(id,Commit,Committed)) && |s.scopes| > 1
    ensures Apply(s,Finish(id,Commit,Committed)).committed == s.committed
    ensures Apply(s,Finish(id,Commit,Committed)).view == s.view
    ensures Apply(s,Finish(id,Commit,Committed)).generation == s.generation
    ensures Apply(s,Finish(id,Commit,Committed)).scopes[0].saved == s.scopes[0].saved
  {}

  lemma RollbackRestoresExactScopeSnapshot(s: Snapshot, id: nat, operation: Completion)
    requires Valid(s) && Enabled(s,Finish(id,operation,RolledBack))
    ensures Apply(s,Finish(id,operation,RolledBack)).view == s.scopes[|s.scopes|-1].saved
    ensures Apply(s,Finish(id,operation,RolledBack)).committed == s.committed
    ensures Apply(s,Finish(id,operation,RolledBack)).generation > s.generation
    ensures Apply(s,Finish(id,operation,RolledBack)).retained == {}
    ensures |s.scopes| == 1 ==> Apply(s,Finish(id,operation,RolledBack)).view.image == s.committed
  {}

  lemma FailedStatementPreservesScopeAndSnapshot(s: Snapshot)
    requires Valid(s) && Enabled(s,EndStatement(false))
    ensures Apply(s,EndStatement(false)).scopes == s.scopes
    ensures Apply(s,EndStatement(false)).status == s.status
    ensures Apply(s,EndStatement(false)).view == s.statement.saved
    ensures Apply(s,EndStatement(false)).generation > s.generation
  {}

  lemma TerminalDisposeIsIdempotent(s: Snapshot, id: nat)
    requires Valid(s) && id in s.status && s.status[id] != Active
    ensures Enabled(s,RepeatTransactionDispose(id))
    ensures Apply(s,RepeatTransactionDispose(id)) == s
    ensures s.phase == Disposed ==> Enabled(s,RepeatWriterDispose) && Apply(s,RepeatWriterDispose) == s
  {}

  lemma FaultedAndDisposedRefuseMutation(s: Snapshot, id: nat, next: Saved)
    requires Valid(s) && (s.phase == Faulted || s.phase == Disposed)
    ensures !Enabled(s,Begin(id)) && !Enabled(s,BeginStatement) && !Enabled(s,Change(next))
    ensures s.phase == Disposed ==> !Enabled(s,Retain(s.generation))
  {}

  lemma InvalidatedReadCannotRetain(s: Snapshot, ticket: nat)
    requires Valid(s) && Enabled(s,Invalidate) && ticket <= s.generation
    ensures !Enabled(Apply(s,Invalidate),Retain(ticket))
    ensures Apply(s,Invalidate).retained == {}
  {}

  lemma FaultedResolutionEndsScope(s: Snapshot, id: nat, operation: Completion)
    requires Valid(s) && Enabled(s,Finish(id,operation,Failed))
    ensures Apply(s,Finish(id,operation,Failed)).phase == Faulted
    ensures !Apply(s,Finish(id,operation,Failed)).known
    ensures |Apply(s,Finish(id,operation,Failed)).scopes| == 0
    ensures Apply(s,Finish(id,operation,Failed)).status[id] == Failed
  {}

  lemma LeaseDeletionRetainsExactAuthority(s: Snapshot, a: Action)
    requires Valid(s) && Enabled(s,a) && (a == DeleteNative || a == DeleteReceipt)
    ensures Held(Apply(s,a))
    ensures s.owner in s.ownership.owners &&
      Apply(s,a).ownership.owners[s.owner] == s.ownership.owners[s.owner]
    ensures |s.scopes| == 0 && s.phase == Disposing
  {
    StepPreserves(s,a);
  }

  function DisposalRank(s: Snapshot): nat {
    var names := (if s.ownership.nativeName == G.Absent then 0 else 1) +
      (if s.ownership.receiptName == G.Absent then 0 else 1);
    if s.phase == Disposed then 0 else if s.phase == Disposing then |s.scopes|+names+1 else |s.scopes|+names+2
  }

  lemma CompleteWriterDisposal(s: Snapshot) returns (states: seq<Snapshot>, actions: seq<Action>)
    requires Valid(s) && s.statement == NoStatement
    requires s.generation + |s.scopes| <= MaxGeneration
    requires s.phase == Disposed ==> s.ownership.nativeName == G.Absent && s.ownership.receiptName == G.Absent
    requires s.phase == Open || s.phase == Faulted || s.phase == Disposing || s.phase == Disposed
    requires s.phase == Open || s.phase == Faulted ==>
      G.Enabled(s.ownership,G.StartDispose(s.owner))
    ensures Path(states,actions) && states[0] == s
    ensures states[|states|-1].phase == Disposed && Valid(states[|states|-1])
    ensures states[|states|-1].ownership.holder == G.Free
    ensures states[|states|-1].ownership.nativeName == G.Absent
    ensures states[|states|-1].ownership.receiptName == G.Absent
    ensures |actions| == DisposalRank(s)
    decreases DisposalRank(s)
  {
    if s.phase == Disposed {
      states,actions := [s],[];
    } else {
      var a := if s.phase != Disposing then StartWriterDispose
        else if |s.scopes| > 0 then DisposeTop
        else if s.ownership.nativeName != G.Absent then DeleteNative
        else if s.ownership.receiptName != G.Absent then DeleteReceipt else FinishWriterDispose;
      assert Enabled(s,a);
      StepPreserves(s,a);
      var next := Apply(s,a);
      assert DisposalRank(next)+1 == DisposalRank(s);
      var tailStates,tailActions := CompleteWriterDisposal(next);
      states,actions := [s]+tailStates,[a]+tailActions;
      forall i | 0 <= i < |actions|
        ensures Enabled(states[i],actions[i]) && states[i+1] == Apply(states[i],actions[i])
      {
        if i != 0 {
          assert states[i] == tailStates[i-1] && actions[i] == tailActions[i-1];
        }
      }
    }
  }
}
