// Verification-only model of CopyPayloadAsync's external-row surface.
// Row acquisition returns the described bounded borrowed page; successful writes
// consume the supplied prefix. Locator/destination internals and CLR object
// headers are separate contracts. Native pointer ownership is not established.
module LongValueTraversal {
  import K = LongValueKernel
  import opened LongValueNativeTypes

  datatype Row = Row(next: U32, pageLength: U32, start: U32, size: U32)
  datatype Result = Result(copied: nat, acquisitions: nat, visited: set<U32>,
    capacity: nat, allocatedSlots: nat, acquiredBytes: nat, refused: bool)

  predicate ValidResult(r: Result, total: nat, budget: nat, pageSize: nat) {
    r.copied <= total && r.acquisitions <= budget && r.acquisitions <= total &&
    |r.visited| == r.acquisitions && 0 !in r.visited &&
    r.acquiredBytes <= pageSize * r.acquisitions &&
    r.capacity <= 16 + 4 * r.acquisitions &&
    r.allocatedSlots <= 32 + 8 * r.acquisitions &&
    (r.capacity == 0 ==> r.allocatedSlots == 0) &&
    (r.capacity > 0 ==> r.allocatedSlots < 2 * r.capacity) &&
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
  // During growth the only referenced arrays are old/new, with combined slots
  // bounded by allocatedSlots; borrowed pages are released before the next row.
  ghost method Traverse(graph: map<U32,Row>, first: U32, total: U32,
      budget: U32, pageSize: U32) returns (result: Result)
    requires total <= 1073741823 && pageSize > 0
    ensures ValidResult(result,total as int,budget as int,pageSize as int)
  {
    var current := first;
    var copied: U32 := 0;
    var count: U32 := 0;
    var seen: set<U32> := {};
    var capacity: U32 := 0;
    var allocationSlots: nat := 0;
    var acquired: nat := 0;
    var refused := false;
    while copied < total
      invariant copied <= total
      invariant count <= budget && count <= copied
      invariant |seen| == count as int && 0 !in seen
      invariant capacity == 0 ==> count == 0 && allocationSlots == 0
      invariant capacity > 0 ==> 16 <= capacity && count <= capacity / 2 && allocationSlots < 2 * (capacity as int)
      invariant capacity as int <= 16 + 4 * (count as int)
      invariant acquired <= (pageSize as int) * (count as int)
      decreases budget - count
    {
      if K.ChainVisitAdmission(current,count,budget,current in seen) != 0 {
        refused := true;
        break;
      }
      if capacity == 0 || count >= capacity / 2 {
        var next := K.NextVisitedCapacity(capacity);
        if next == 0 {
          refused := true;
          break;
        }
        if capacity > 0 {
          assert count == capacity / 2;
          assert capacity <= 2 * count + 1;
        }
        allocationSlots := allocationSlots + next as int;
        capacity := next;
      }
      seen := seen + {current};
      count := count + 1;
      // Acquisition's bounded page is borrowed rather than retained per row.
      acquired := acquired + pageSize as int;
      if current !in graph {
        result := Result(copied as int,count as int,seen,capacity as int,allocationSlots,acquired,true);
        return;
      }
      var row := graph[current];
      var effectiveLength := if row.pageLength < pageSize then row.pageLength else pageSize;
      var consumed := K.ChainPayloadCount(total,copied,effectiveLength,row.start,row.size,4,false);
      if consumed == 0 {
        result := Result(copied as int,count as int,seen,capacity as int,allocationSlots,acquired,true);
        return;
      }
      K.ChainPayloadMakesBoundedProgress(total,copied,effectiveLength,row.start,row.size,4,false);
      copied := copied + consumed;
      current := row.next;
    }
    result := Result(copied as int,count as int,seen,capacity as int,allocationSlots,acquired,refused);
  }
}
