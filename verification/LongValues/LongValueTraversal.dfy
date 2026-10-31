// Verification-only model of CopyPayloadAsync's external-row surface.
// Row acquisition returns the described bounded borrowed page; successful writes
// consume the supplied prefix. Locator/destination internals and CLR object
// headers are separate contracts. Allocation supplies distinct zero-filled
// storage; actual CLR allocation/failure remains an adapter effect. Native
// pointer ownership is not established.
module LongValueTraversal {
  import K = LongValueKernel
  import opened LongValueNativeTypes

  datatype Row = Row(next: U32, pageLength: U32, start: U32, size: U32)
  datatype Result = Result(copied: nat, acquisitions: nat, visited: set<U32>,
    capacity: nat, allocatedSlots: nat, acquiredBytes: nat, probes: nat,
    scannedSlots: nat, peakSlots: nat, refused: bool)

  // One final duplicate/refusal lookup is charged in addition to acquired rows.
  // The two products separate preparation probes from geometric rehash work.
  ghost function ProbeBudget(rows: nat): nat {
    (rows + 1) * (16 + 4 * rows) + (2 * rows + 1) * (32 + 8 * rows)
  }

  lemma ScalePreservesBound(scale: nat, lower: nat, upper: nat)
    requires lower <= upper
    ensures scale * lower <= scale * upper
  {
    assert scale * upper == scale * lower + scale * (upper - lower);
  }

  lemma AccumulatePreparationWork(rows: nat, probes: nat, capacity: nat, additional: nat)
    requires probes <= rows * (16 + 4 * rows)
    requires capacity <= 16 + 4 * rows && additional <= capacity
    ensures probes + additional <= (rows + 1) * (16 + 4 * rows)
    ensures probes + additional <= (rows + 1) * (16 + 4 * (rows + 1))
  {
    assert (rows + 1) * (16 + 4 * rows) <= (rows + 1) * (16 + 4 * (rows + 1));
  }

  lemma AccumulateGrowthWork(rows: nat, probes: nat, allocatedSlots: nat, capacity: nat, additional: nat)
    requires probes <= (2 * rows + 1) * allocatedSlots
    requires additional <= (2 * rows + 1) * capacity
    ensures probes + additional <= (2 * rows + 1) * (allocatedSlots + capacity)
    ensures probes + additional <= (2 * (rows + 1) + 1) * (allocatedSlots + capacity)
  {
    assert (2 * rows + 1) * (allocatedSlots + capacity) <= (2 * (rows + 1) + 1) * (allocatedSlots + capacity);
  }

  lemma ResourceMetersFit(rows: nat, capacity: nat, allocatedSlots: nat,
      preparationProbes: nat, growthProbes: nat, peakSlots: nat)
    requires capacity <= 16 + 4 * rows
    requires allocatedSlots <= 32 + 8 * rows
    requires preparationProbes <= (rows + 1) * (16 + 4 * rows)
    requires growthProbes <= (2 * rows + 1) * allocatedSlots
    requires 2 * peakSlots <= 3 * capacity
    ensures preparationProbes + growthProbes <= ProbeBudget(rows)
    ensures peakSlots <= 24 + 6 * rows
  {
    ScalePreservesBound(2 * rows + 1,allocatedSlots,32 + 8 * rows);
  }

  ghost predicate ValidResult(r: Result, total: nat, budget: nat, pageSize: nat) {
    r.copied <= total && r.acquisitions <= budget && r.acquisitions <= total &&
    |r.visited| == r.acquisitions && 0 !in r.visited &&
    r.acquiredBytes <= pageSize * r.acquisitions &&
    r.capacity <= 16 + 4 * r.acquisitions &&
    r.allocatedSlots <= 32 + 8 * r.acquisitions &&
    (r.capacity == 0 ==> r.allocatedSlots == 0) &&
    (r.capacity > 0 ==> r.allocatedSlots < 2 * r.capacity) &&
    r.probes <= ProbeBudget(r.acquisitions) &&
    r.scannedSlots <= r.allocatedSlots &&
    r.peakSlots <= r.allocatedSlots && r.peakSlots <= 24 + 6 * r.acquisitions &&
    (!r.refused ==> r.copied == total)
  }

  lemma ProbeRankDecreases(remaining: nat)
    requires remaining > 0
    ensures remaining - 1 < remaining
  { }

  lemma ProbeResiduesAreDistinct(start: nat, capacity: nat, i: nat, j: nat)
    requires capacity > 0 && start < capacity && i < j < capacity
    ensures (start + i) % capacity != (start + j) % capacity
  {
    assert (start+i) % capacity == (if start+i < capacity then start+i else start+i-capacity);
    assert (start+j) % capacity == (if start+j < capacity then start+j else start+j-capacity);
  }

  lemma CircularProbeCoversEverySlot(start: nat, capacity: nat, target: nat) returns (step: nat)
    requires capacity > 0 && start < capacity && target < capacity
    ensures step < capacity && (start + step) % capacity == target
  {
    step := if target >= start then target-start else capacity-start+target;
    if target < start {
      assert start + step == capacity + target;
      assert (capacity+target) % capacity == target;
    }
  }

  lemma NonfullProbeFindsEmpty(table: seq<U32>, start: nat) returns (step: nat)
    requires |table| > 0 && start < |table|
    requires exists slot: nat :: slot < |table| && table[slot] == 0
    ensures step < |table| && table[(start+step) % |table|] == 0
  {
    var slot: nat :| slot < |table| && table[slot] == 0;
    step := CircularProbeCoversEverySlot(start, |table|, slot);
  }

  lemma NativeProbeMatchesCircularStep(slot: U32, capacity: U32)
    requires 0 < capacity && slot < capacity
    ensures K.NextVisitedSlot(slot,capacity) as int == ((slot as int) + 1) % (capacity as int)
  {
    var c: int := capacity as int;
    var s: int := slot as int;
    assert 0 < c && 0 <= s < c;
    if s + 1 == c {
      assert c % c == 0;
      assert K.NextVisitedSlot(slot,capacity) == 0;
    } else {
      assert s + 1 < c;
      assert (s+1) % c == s+1;
    }
  }
  // Every successful iteration adds one distinct nonzero identity before a
  // bounded acquisition. Null/repeated/absent rows, invalid or zero-payload
  // slices and exhausted row/growth budgets refuse; none can spin forever.
  // Array slots count cumulative allocation, including every superseded array.
  // This verification-only driver executes the generated controller on arrays,
  // rather than assuming its membership/growth effects. Only row acquisition
  // and destination writes remain the abstract borrowed-page contracts above.
  // Peak slots charge old/new overlap before producer handoff. They do not
  // include unreachable arrays retained by GC, object headers or caller state.
  method Traverse(graph: map<U32,Row>, first: U32, total: U32,
      budget: U32, pageSize: U32) returns (ghost result: Result)
    requires total <= 1073741823 && pageSize > 0
    ensures ValidResult(result,total as int,budget as int,pageSize as int)
  {
    hide K.TrackedVisited, K.VisitedTable, K.DistinctVisited, K.OccupiedCount,
      K.VisitedSlot, K.NextVisitedSlot, K.ProbeDistance, K.VisitedAddPayload,
      K.VisitedAddHasHighFlag, K.VisitedAddResult, K.VisitedInsertedDecision,
      K.VisitedGrowthDecision, K.ValidVisitedDecision;
    assert K.TrackedVisited([],0) by { reveal K.TrackedVisited(); }
    var current := first;
    var copied: U32 := 0;
    var count: U32 := 0;
    ghost var seen: set<U32> := {};
    var entries: array?<U32> := null;
    var capacity: U32 := 0;
    ghost var allocationSlots: nat := 0;
    ghost var acquired: nat := 0;
    ghost var preparationProbes: nat := 0;
    ghost var growthProbes: nat := 0;
    ghost var scannedSlots: nat := 0;
    ghost var peakSlots: nat := 0;
    var refused := false;
    while copied < total
      invariant copied <= total
      invariant count <= budget && count <= copied
      invariant |seen| == count as int && 0 !in seen
      invariant K.TrackedVisited(K.VisitedSnapshot(entries), count)
      invariant entries != null ==> fresh(entries)
      invariant seen == K.VisitedMembers(K.VisitedSnapshot(entries))
      invariant capacity as int == |K.VisitedSnapshot(entries)|
      invariant capacity == 0 ==> count == 0 && allocationSlots == 0
      invariant capacity > 0 ==> 16 <= capacity && count <= capacity / 2 && allocationSlots < 2 * (capacity as int)
      invariant capacity as int <= 16 + 4 * (count as int)
      invariant capacity as int <= allocationSlots
      invariant acquired <= (pageSize as int) * (count as int)
      invariant preparationProbes <= (count as int) * (16 + 4 * (count as int))
      invariant growthProbes <= (2 * (count as int) + 1) * allocationSlots
      invariant scannedSlots <= allocationSlots && peakSlots <= allocationSlots
      invariant 2 * peakSlots <= 3 * (capacity as int)
      decreases budget - count
    {
      if K.ChainVisitAdmission(current,count,budget,false) != 0 {
        refused := true;
        break;
      }
      var decision, lookupProbes := K.PrepareVisitedAdd(entries,count,current);
      K.TrackedVisitedResourceFacts(K.VisitedSnapshot(entries),K.EffectiveVisitedAddCount(decision,count));
      ghost var previousCount := count as int;
      AccumulatePreparationWork(count as int,preparationProbes,capacity as int,lookupProbes);
      preparationProbes := preparationProbes + lookupProbes;
      assert preparationProbes <= ((count as int) + 1) * (16 + 4 * (count as int));
      var operation := K.VisitedAddResult(decision);
      if operation == 0 || operation == 3 {
        refused := true;
        break;
      }
      if operation == 2 {
        var next := K.VisitedAddPayload(decision);
        K.VisitedAllocationAndProbeBounds(current,capacity,0);
        if capacity > 0 {
          assert count == capacity / 2;
          assert next == 2 * capacity;
        }
        assert capacity as int == 2 * (count as int);
        var destination := new U32[next as int](_ => 0);
        if (capacity as int) + (next as int) > peakSlots {
          peakSlots := (capacity as int) + (next as int);
        }
        var nextCount, rehashProbes, rehashScans := K.CompleteVisitedGrowth(entries,destination,count,current);
        K.TrackedVisitedResourceFacts(destination[..],nextCount);
        AccumulateGrowthWork(count as int,growthProbes,allocationSlots,next as int,rehashProbes);
        growthProbes := growthProbes + rehashProbes;
        scannedSlots := scannedSlots + rehashScans;
        allocationSlots := allocationSlots + next as int;
        entries := destination;
        capacity := next;
        count := nextCount;
      } else {
        count := K.VisitedAddPayload(decision);
      }
      assert count as int == previousCount + 1;
      seen := seen + {current};
      assert preparationProbes <= (count as int) * (16 + 4 * (count as int));
      // Acquisition's bounded page is borrowed rather than retained per row.
      acquired := acquired + pageSize as int;
      if current !in graph {
        ResourceMetersFit(count as int,capacity as int,allocationSlots,preparationProbes,growthProbes,peakSlots);
        result := Result(copied as int,count as int,seen,capacity as int,allocationSlots,acquired,
          preparationProbes + growthProbes,scannedSlots,peakSlots,true);
        return {:isolate};
      }
      var row := graph[current];
      var effectiveLength := if row.pageLength < pageSize then row.pageLength else pageSize;
      var consumed := K.ChainPayloadCount(total,copied,effectiveLength,row.start,row.size,4,false);
      if consumed == 0 {
        ResourceMetersFit(count as int,capacity as int,allocationSlots,preparationProbes,growthProbes,peakSlots);
        result := Result(copied as int,count as int,seen,capacity as int,allocationSlots,acquired,
          preparationProbes + growthProbes,scannedSlots,peakSlots,true);
        return {:isolate};
      }
      K.ChainPayloadMakesBoundedProgress(total,copied,effectiveLength,row.start,row.size,4,false);
      copied := copied + consumed;
      current := row.next;
    }
    ResourceMetersFit(count as int,capacity as int,allocationSlots,preparationProbes,growthProbes,peakSlots);
    result := Result(copied as int,count as int,seen,capacity as int,allocationSlots,acquired,
      preparationProbes + growthProbes,scannedSlots,peakSlots,refused);
    return {:isolate};
  }
}
