// Executable bounded JET long-value descriptor acquisition and serialization.
// Exact byte/pointer codecs do not establish external-page ownership, token
// semantics, payload validity or native interoperability. No external C#
// contracts are trusted.
module LongValueNativeTypes {
  newtype {:nativeType "ulong"} U64 = x: int | 0 <= x < 18446744073709551616
  newtype {:nativeType "uint"} U32 = x: int | 0 <= x < 4294967296
  newtype {:nativeType "byte"} Byte = x: int | 0 <= x < 256
}

module LongValueKernel {
  import opened LongValueNativeTypes

  function DecodeWord(b0: Byte, b1: Byte, b2: Byte, b3: Byte): U32 {
    (b0 as U32) + 256 * (b1 as U32) + 65536 * (b2 as U32) + 16777216 * (b3 as U32)
  }

  function DecodeLength(word: U32): U32 {
    word % 1073741824
  }

  // All four raw storage classes are retained at acquisition. The reserved
  // 192 class remains available for the production payload validator to refuse.
  function DecodeStorageMode(word: U32): Byte {
    ((word as U64) / 1073741824 * 64) as Byte
  }

  function IsSupportedStorageMode(mode: Byte): bool {
    mode == 0 || mode == 64 || mode == 128
  }

  // The refusal sentinel is outside every serializable 32-bit word. In
  // particular an arbitrary byte cannot silently contribute bits to length.
  function EncodeLengthAndMode(length: U32, mode: Byte): U64 {
    if length > 1073741823 || !IsSupportedStorageMode(mode) then 4294967296
    else (length as U64) + (mode as U64) * 16777216
  }

  // The checked .NET array adapters use these native decisions before any
  // access. No addition can overflow, even for hostile full-width inputs.
  function HasDescriptorRange(bufferLength: U32, start: U32, length: U32): bool {
    start <= bufferLength && length <= bufferLength - start && length >= 12
  }

  function WordByte(word: U32, position: Byte): Byte {
    if position == 0 then (word % 256) as Byte
    else if position == 1 then (word / 256 % 256) as Byte
    else if position == 2 then (word / 65536 % 256) as Byte
    else if position == 3 then (word / 16777216) as Byte
    else 0
  }

  // Pointer/token words are opaque unsigned fields. The row-pointer split is
  // a separate bounded representation; this does not grant page ownership.
  function PointerPage(pointer: U32): U32 { pointer / 256 }
  function PointerRow(pointer: U32): Byte { (pointer % 256) as Byte }
  function EncodeRowPointer(page: U64, row: U32): U64 {
    if page > 16777215 || row > 255 then 4294967296
    else page * 256 + row as U64
  }

  // Priority is observable: invalid encoding, configured byte limit, inline
  // truncation, then external-row budget. Zero means admitted, not traversed.
  function DescriptorAdmission(length: U32, mode: Byte, inlineLength: U32,
      maxValueBytes: U32, maxChainRows: U32): Byte {
    if length > 1073741823 || !IsSupportedStorageMode(mode) then 1
    else if length > maxValueBytes then 2
    else if mode == 128 && length > inlineLength then 3
    else if mode != 128 && length > 0 && maxChainRows == 0 then 4
    else 0
  }

  // Traversal admission precedes acquisition. A repeated identity never grants
  // another lookup; exhaustion and null pointers preserve their error priority.
  function ChainVisitAdmission(pointer: U32, rows: U32, budget: U32, repeated: bool): Byte {
    if pointer == 0 then 1 else if rows >= budget then 2 else if repeated then 3 else 0
  }

  // Zero is refusal, so every admitted row strictly consumes the byte budget.
  function ChainPayloadCount(total: U32, copied: U32, pageLength: U32,
      start: U32, size: U32, prefix: U32, single: bool): U32 {
    if copied >= total || start > pageLength || size > pageLength - start ||
       size <= prefix || (single && size - prefix < total - copied) then 0
    else if size - prefix < total - copied then size - prefix else total - copied
  }

  // Bounded geometric arrays replace undocumented HashSet allocation policy.
  // Zero refuses growth before checked array-length arithmetic can overflow.
  function NextVisitedCapacity(capacity: U32): U32 {
    if capacity == 0 then 16
    else if capacity < 16 || capacity >= 1073741824 then 0
    else capacity * 2
  }

  function VisitedSlot(pointer: U32, capacity: U32): U32 {
    if capacity == 0 then 0
    else (((pointer as U64) * 2654435761 + (pointer / 256) as U64 + (pointer / 65536) as U64) % (capacity as U64)) as U32
  }

  function NextVisitedSlot(slot: U32, capacity: U32): U32 {
    if capacity == 0 || slot >= capacity - 1 then 0 else slot + 1
  }

  ghost function ProbeDistance(start: nat, capacity: nat, target: nat): nat
    requires start < capacity && target < capacity
  { if target >= start then target - start else capacity - start + target }

  ghost predicate VisitedTable(table: seq<U32>) {
    0 < |table| <= 2147483647 &&
    forall target: nat :: target < |table| && table[target] != 0 ==>
      forall previous: nat :: (previous < |table| &&
        ProbeDistance(VisitedSlot(table[target], |table| as U32) as int, |table|, previous) <
        ProbeDistance(VisitedSlot(table[target], |table| as U32) as int, |table|, target)) ==> table[previous] != 0
  }
  ghost function OccupiedCount(table: seq<U32>): nat {
    if |table| == 0 then 0 else (if table[0] == 0 then 0 else 1) + OccupiedCount(table[1..])
  }
  ghost function VisitedMembers(table: seq<U32>): set<U32> {
    set value: U32 | value in table && value != 0
  }
  ghost predicate DistinctVisited(table: seq<U32>) {
    forall first: nat, second: nat :: first < second < |table| && table[first] != 0 ==>
      table[first] != table[second]
  }
  lemma DistinctOccupiedCount(table: seq<U32>)
    requires DistinctVisited(table)
    ensures OccupiedCount(table) == |VisitedMembers(table)|
  {
    if |table| > 0 {
      DistinctOccupiedCount(table[1..]);
      if table[0] != 0 {
        assert table[0] !in table[1..];
        assert VisitedMembers(table) == VisitedMembers(table[1..]) + {table[0]};
      } else {
        assert VisitedMembers(table) == VisitedMembers(table[1..]);
      }
    }
  }
  lemma EmptyInsertionPreservesDistinct(table: seq<U32>, slot: nat, pointer: U32)
    requires DistinctVisited(table) && slot < |table| && table[slot] == 0
    requires pointer != 0 && pointer !in table
    ensures DistinctVisited(table[slot := pointer])
  {
    var after := table[slot := pointer];
    forall first: nat, second: nat |
        first < second < |table| && after[first] != 0
      ensures after[first] != after[second]
    {
      if first == slot { assert table[second] != pointer; }
      else if second == slot { assert table[first] != pointer; }
    }
  }

  lemma EmptyInsertionIncreasesCount(table: seq<U32>, slot: nat, pointer: U32)
    requires slot < |table| && table[slot] == 0 && pointer != 0
    ensures OccupiedCount(table[slot := pointer]) == OccupiedCount(table) + 1
  {
    if slot > 0 {
      EmptyInsertionIncreasesCount(table[1..], slot - 1, pointer);
      assert table[slot := pointer][1..] == table[1..][slot - 1 := pointer];
    }
  }

  lemma EmptyInsertionHasExactMembers(table: seq<U32>, slot: nat, pointer: U32)
    requires slot < |table| && table[slot] == 0 && pointer != 0
    ensures forall value: U32 ::
      (value in table[slot := pointer] && value != 0) <==> ((value in table && value != 0) || value == pointer)
  {
    var after := table[slot := pointer];
    forall value: U32
      ensures (value in after && value != 0) <==> ((value in table && value != 0) || value == pointer)
    {
      if value in after && value != 0 {
        var memberIndex: nat :| memberIndex < |after| && after[memberIndex] == value;
        if memberIndex != slot { assert table[memberIndex] == value; }
      }
      if value in table && value != 0 {
        var memberIndex: nat :| memberIndex < |table| && table[memberIndex] == value;
        assert memberIndex != slot;
        assert after[memberIndex] == value;
      }
      if value == pointer { assert after[slot] == value; }
    }
  }

  lemma AllOccupiedCount(table: seq<U32>)
    requires forall i: nat :: i < |table| ==> table[i] != 0
    ensures OccupiedCount(table) == |table|
  {
    if |table| > 0 { AllOccupiedCount(table[1..]); }
  }

  lemma ZeroOccupiedCount(table: seq<U32>)
    requires forall i: nat :: i < |table| ==> table[i] == 0
    ensures OccupiedCount(table) == 0
  {
    if |table| > 0 { ZeroOccupiedCount(table[1..]); }
  }

  lemma EmptyInsertionPreservesTable(table: seq<U32>, slot: nat, pointer: U32)
    requires VisitedTable(table) && slot < |table| && table[slot] == 0
    requires forall previous: nat :: (previous < |table| &&
      ProbeDistance(VisitedSlot(pointer, |table| as U32) as int, |table|, previous) <
      ProbeDistance(VisitedSlot(pointer, |table| as U32) as int, |table|, slot)) ==> table[previous] != 0
    ensures VisitedTable(table[slot := pointer])
  {
    var after := table[slot := pointer];
    forall target: nat | target < |table| && after[target] != 0
      ensures forall previous: nat :: (previous < |table| &&
        ProbeDistance(VisitedSlot(after[target], |table| as U32) as int, |table|, previous) <
        ProbeDistance(VisitedSlot(after[target], |table| as U32) as int, |table|, target)) ==> after[previous] != 0
    {
      if target != slot { assert after[target] == table[target]; }
    }
  }
  lemma ProbeDistanceBounds(start: nat, capacity: nat, target: nat)
    requires start < capacity && target < capacity
    ensures ProbeDistance(start, capacity, target) < capacity
    ensures ProbeDistance(start, capacity, target) == 0 <==> start == target
  { }

  lemma ProbeDistanceIsInjective(start: nat, capacity: nat, first: nat, second: nat)
    requires start < capacity && first < capacity && second < capacity
    requires ProbeDistance(start, capacity, first) == ProbeDistance(start, capacity, second)
    ensures first == second
  { }

  lemma ProbeDistanceAdvances(start: nat, capacity: U32, slot: U32)
    requires start < capacity as int && slot < capacity
    ensures ProbeDistance(start, capacity as int, slot as int) + 1 < capacity as int ==>
      ProbeDistance(start, capacity as int, NextVisitedSlot(slot, capacity) as int) ==
        ProbeDistance(start, capacity as int, slot as int) + 1
  { }
  // Lookup stops at an empty slot only under the insertion-established chain
  // invariant. No random-hash or expected-time assumption enters correctness.
  method ContainsVisited(entries: array<U32>, pointer: U32) returns (found: bool)
    requires VisitedTable(entries[..])
    ensures found <==> pointer != 0 && pointer in entries[..]
  {
    found := false;
    if pointer == 0 { return; }
    var capacity := entries.Length as U32;
    var start := VisitedSlot(pointer, capacity);
    VisitedAllocationAndProbeBounds(pointer, capacity, 0);
    var slot := start;
    var step: U32 := 0;
    while step < capacity
      invariant slot < capacity
      invariant step < capacity ==> ProbeDistance(start as int, capacity as int, slot as int) == step as int
      invariant step <= capacity
      invariant forall previous: nat :: (previous < entries.Length &&
        ProbeDistance(start as int, capacity as int, previous) < step as int) ==> entries[previous] != pointer
      decreases capacity - step
    {
      var candidate := entries[slot];
      if candidate == 0 {
        forall target: nat | target < entries.Length
          ensures entries[target] != pointer
        {
          ProbeDistanceBounds(start as int, capacity as int, target);
          var distance := ProbeDistance(start as int, capacity as int, target);
          if entries[target] == pointer {
            if distance < step as int { }
            else if distance == step as int {
              ProbeDistanceIsInjective(start as int, capacity as int, target, slot as int);
            }
            else {
              assert entries[slot] != 0;
            }
          }
        }
        return;
      }
      if candidate == pointer { found := true; return; }
      ProbeDistanceAdvances(start as int, capacity, slot);
      slot := NextVisitedSlot(slot, capacity);
      step := step + 1;
    }
    forall target: nat | target < entries.Length
      ensures entries[target] != pointer
    {
      ProbeDistanceBounds(start as int, capacity as int, target);
    }
  }

  // One bounded probe adds an absent identity or observes an existing one.
  // Zero denotes full, one present, and two inserted. Only insertion mutates.
  method AddVisited(entries: array<U32>, pointer: U32) returns (result: Byte)
    requires VisitedTable(entries[..]) && pointer != 0
    modifies entries
    ensures result <= 2
    ensures VisitedTable(entries[..])
    ensures DistinctVisited(old(entries[..])) ==> DistinctVisited(entries[..])
    ensures result == 1 <==> pointer in old(entries[..])
    ensures result != 2 ==> entries[..] == old(entries[..])
    ensures result == 2 ==> pointer !in old(entries[..])
    ensures forall value: U32 :: value != 0 ==>
      (value in entries[..] <==> value in old(entries[..]) || (result == 2 && value == pointer))
    ensures OccupiedCount(entries[..]) == OccupiedCount(old(entries[..])) + (if result == 2 then 1 else 0)
    ensures result == 0 ==> forall i: nat :: i < entries.Length ==> entries[i] != 0
  {
    result := 0;
    var capacity := entries.Length as U32;
    var start := VisitedSlot(pointer, capacity);
    VisitedAllocationAndProbeBounds(pointer, capacity, 0);
    var slot := start;
    var step: U32 := 0;
    while step < capacity
      invariant entries[..] == old(entries[..])
      invariant slot < capacity
      invariant step < capacity ==> ProbeDistance(start as int, capacity as int, slot as int) == step as int
      invariant step <= capacity
      invariant forall previous: nat :: (previous < entries.Length &&
        ProbeDistance(start as int, capacity as int, previous) < step as int) ==>
          entries[previous] != 0 && entries[previous] != pointer
      decreases capacity - step
    {
      var candidate := entries[slot];
      if candidate == 0 {
        forall target: nat | target < entries.Length
          ensures entries[target] != pointer
        {
          ProbeDistanceBounds(start as int, capacity as int, target);
          var distance := ProbeDistance(start as int, capacity as int, target);
          if entries[target] == pointer {
            if distance < step as int { }
            else if distance == step as int {
              ProbeDistanceIsInjective(start as int, capacity as int, target, slot as int);
            }
            else {
              assert entries[slot] != 0;
            }
          }
        }
        ghost var before := entries[..];
        EmptyInsertionPreservesTable(before, slot as int, pointer);
        EmptyInsertionIncreasesCount(before, slot as int, pointer);
        EmptyInsertionHasExactMembers(before, slot as int, pointer);
        if DistinctVisited(before) {
          EmptyInsertionPreservesDistinct(before, slot as int, pointer);
        }
        entries[slot] := pointer;
        assert entries[..] == before[slot := pointer];
        result := 2;
        return;
      }
      if candidate == pointer { result := 1; return; }
      ProbeDistanceAdvances(start as int, capacity, slot);
      slot := NextVisitedSlot(slot, capacity);
      step := step + 1;
    }
    forall target: nat | target < entries.Length
      ensures entries[target] != 0 && entries[target] != pointer
    {
      ProbeDistanceBounds(start as int, capacity as int, target);
    }
  }
  // Growth passes a distinct zero-filled destination larger than the source.
  // Each nonzero source slot adds at most one identity, so duplicate source
  // identities require no unproved occupancy premise from the source.
  method RehashVisited(source: array<U32>, destination: array<U32>)
    requires source != destination
    requires source.Length < destination.Length <= 2147483647
    requires forall i: nat :: i < destination.Length ==> destination[i] == 0
    modifies destination
    ensures VisitedTable(destination[..])
    ensures DistinctVisited(destination[..])
    ensures OccupiedCount(destination[..]) == |VisitedMembers(source[..])|
    ensures forall value: U32 :: value != 0 ==>
      (value in destination[..] <==> value in source[..])
  {
    var index: U32 := 0;
    ZeroOccupiedCount(destination[..]);
    while index < source.Length as U32
      invariant index as int <= source.Length
      invariant VisitedTable(destination[..])
      invariant DistinctVisited(destination[..])
      invariant forall value: U32 :: value != 0 ==>
        (value in destination[..] <==> value in source[..index])
      invariant OccupiedCount(destination[..]) <= index as int
      decreases source.Length - index as int
    {
      if source[index] != 0 {
        var added := AddVisited(destination, source[index]);
        if added == 0 {
          AllOccupiedCount(destination[..]);
          assert false;
        }
      }
      index := index + 1;
    }
    DistinctOccupiedCount(destination[..]);
    assert VisitedMembers(destination[..]) == VisitedMembers(source[..]);
  }

  lemma ChainPayloadMakesBoundedProgress(total: U32, copied: U32, pageLength: U32,
      start: U32, size: U32, prefix: U32, single: bool)
    ensures ChainPayloadCount(total,copied,pageLength,start,size,prefix,single) > 0 ==>
      copied < total && start <= pageLength && size <= pageLength - start && prefix < size
    ensures ChainPayloadCount(total,copied,pageLength,start,size,prefix,single) > 0 ==>
      0 < ChainPayloadCount(total,copied,pageLength,start,size,prefix,single) <= total - copied &&
      ChainPayloadCount(total,copied,pageLength,start,size,prefix,single) <= size - prefix
  { }

  lemma ValidChainBoundsAdmitProgress(total: U32, copied: U32, pageLength: U32,
      start: U32, size: U32, prefix: U32, single: bool)
    requires copied < total && start <= pageLength && size <= pageLength - start
    requires prefix < size && (single ==> size - prefix >= total - copied)
    ensures ChainPayloadCount(total,copied,pageLength,start,size,prefix,single) > 0
  { }
  lemma VisitedAllocationAndProbeBounds(pointer: U32, capacity: U32, slot: U32)
    ensures capacity > 0 ==> VisitedSlot(pointer,capacity) < capacity
    ensures capacity > 0 ==> NextVisitedSlot(slot,capacity) < capacity
    ensures NextVisitedCapacity(capacity) != 0 ==>
      16 <= NextVisitedCapacity(capacity) <= 2147483646
    ensures capacity > 0 && NextVisitedCapacity(capacity) != 0 ==>
      NextVisitedCapacity(capacity) == 2 * capacity
  { }
  lemma DescriptorAdmissionIsExact(length: U32, mode: Byte, inlineLength: U32,
      maxValueBytes: U32, maxChainRows: U32)
    ensures DescriptorAdmission(length, mode, inlineLength, maxValueBytes, maxChainRows) == 0 <==>
      length <= 1073741823 && IsSupportedStorageMode(mode) && length <= maxValueBytes &&
      (mode == 128 ==> length <= inlineLength) && (mode != 128 && length > 0 ==> maxChainRows > 0)
    ensures DescriptorAdmission(length, mode, inlineLength, maxValueBytes, maxChainRows) <= 4
    ensures DescriptorAdmission(length, mode, inlineLength, maxValueBytes, maxChainRows) == 1 <==>
      length > 1073741823 || !IsSupportedStorageMode(mode)
  { }

  lemma DescriptorRefusalPriority(length: U32, mode: Byte, inlineLength: U32,
      maxValueBytes: U32, maxChainRows: U32)
    ensures DescriptorAdmission(length, mode, inlineLength, maxValueBytes, maxChainRows) == 2 <==>
      length <= 1073741823 && IsSupportedStorageMode(mode) && length > maxValueBytes
    ensures DescriptorAdmission(length, mode, inlineLength, maxValueBytes, maxChainRows) == 3 <==>
      length <= 1073741823 && IsSupportedStorageMode(mode) && length <= maxValueBytes && mode == 128 && length > inlineLength
    ensures DescriptorAdmission(length, mode, inlineLength, maxValueBytes, maxChainRows) == 4 <==>
      length <= 1073741823 && IsSupportedStorageMode(mode) && length <= maxValueBytes && mode != 128 && length > 0 && maxChainRows == 0
  { }

  method DecodeFields(b0: Byte, b1: Byte, b2: Byte, b3: Byte,
      b4: Byte, b5: Byte, b6: Byte, b7: Byte,
      b8: Byte, b9: Byte, b10: Byte, b11: Byte)
      returns (length: U32, mode: Byte, firstDp: U32, token: U32)
    ensures length == DecodeLength(DecodeWord(b0, b1, b2, b3))
    ensures mode == DecodeStorageMode(DecodeWord(b0, b1, b2, b3))
    ensures firstDp == DecodeWord(b4, b5, b6, b7)
    ensures token == DecodeWord(b8, b9, b10, b11)
  {
    var word := DecodeWord(b0, b1, b2, b3);
    length := DecodeLength(word);
    mode := DecodeStorageMode(word);
    firstDp := DecodeWord(b4, b5, b6, b7);
    token := DecodeWord(b8, b9, b10, b11);
  }

  // CLR arrays have signed-32-bit lengths; this checked adapter precondition
  // permits native length/index compilation without a verifier runtime.
  method ReadDescriptor(source: array<Byte>, start: U32, count: U32)
      returns (valid: bool, length: U32, mode: Byte, firstDp: U32, token: U32)
    requires source.Length <= 2147483647
    ensures valid == HasDescriptorRange(source.Length as U32, start, count)
    ensures !valid ==> length == 0 && mode == 0 && firstDp == 0 && token == 0
    ensures valid ==> length == DecodeLength(DecodeWord(source[start], source[start + 1], source[start + 2], source[start + 3]))
    ensures valid ==> mode == DecodeStorageMode(DecodeWord(source[start], source[start + 1], source[start + 2], source[start + 3]))
    ensures valid ==> firstDp == DecodeWord(source[start + 4], source[start + 5], source[start + 6], source[start + 7])
    ensures valid ==> token == DecodeWord(source[start + 8], source[start + 9], source[start + 10], source[start + 11])
  {
    valid := HasDescriptorRange(source.Length as U32, start, count);
    length, mode, firstDp, token := 0, 0, 0, 0;
    if valid {
      length, mode, firstDp, token := DecodeFields(source[start], source[start + 1], source[start + 2], source[start + 3],
        source[start + 4], source[start + 5], source[start + 6], source[start + 7],
        source[start + 8], source[start + 9], source[start + 10], source[start + 11]);
    }
  }

  method WriteDescriptor(destination: array<Byte>, start: U32, count: U32,
      length: U32, mode: Byte, firstDp: U32, token: U32) returns (valid: bool)
    requires destination.Length <= 2147483647
    modifies destination
    ensures valid == (HasDescriptorRange(destination.Length as U32, start, count) && EncodeLengthAndMode(length, mode) < 4294967296)
    ensures !valid ==> destination[..] == old(destination[..])
    ensures valid ==> forall i: int :: 0 <= i < destination.Length && (i < start as int || start as int + 12 <= i) ==> destination[i] == old(destination[i])
    ensures valid ==> DecodeWord(destination[start], destination[start + 1], destination[start + 2], destination[start + 3]) as U64 == EncodeLengthAndMode(length, mode)
    ensures valid ==> DecodeLength(DecodeWord(destination[start], destination[start + 1], destination[start + 2], destination[start + 3])) == length
    ensures valid ==> DecodeStorageMode(DecodeWord(destination[start], destination[start + 1], destination[start + 2], destination[start + 3])) == mode
    ensures valid ==> DecodeWord(destination[start + 4], destination[start + 5], destination[start + 6], destination[start + 7]) == firstDp
    ensures valid ==> DecodeWord(destination[start + 8], destination[start + 9], destination[start + 10], destination[start + 11]) == token
  {
    var packed := EncodeLengthAndMode(length, mode);
    valid := HasDescriptorRange(destination.Length as U32, start, count) && packed < 4294967296;
    if valid {
      var word := packed as U32;
      destination[start] := WordByte(word, 0);
      destination[start + 1] := WordByte(word, 1);
      destination[start + 2] := WordByte(word, 2);
      destination[start + 3] := WordByte(word, 3);
      destination[start + 4] := WordByte(firstDp, 0);
      destination[start + 5] := WordByte(firstDp, 1);
      destination[start + 6] := WordByte(firstDp, 2);
      destination[start + 7] := WordByte(firstDp, 3);
      destination[start + 8] := WordByte(token, 0);
      destination[start + 9] := WordByte(token, 1);
      destination[start + 10] := WordByte(token, 2);
      destination[start + 11] := WordByte(token, 3);
      SupportedFieldsRoundTrip(length, mode);
      WordBytesRoundTrip(word);
      WordBytesRoundTrip(firstDp);
      WordBytesRoundTrip(token);
    }
  }

  lemma DescriptorRangeBounds(bufferLength: U32, start: U32, length: U32)
    ensures HasDescriptorRange(bufferLength, start, length) <==>
      12 <= length as int && start as int + length as int <= bufferLength as int
    ensures HasDescriptorRange(bufferLength, start, length) ==>
      start as int + 11 < bufferLength as int && start as int + 12 <= start as int + length as int
  { }

  lemma WordBytesRoundTrip(word: U32)
    ensures DecodeWord(WordByte(word, 0), WordByte(word, 1), WordByte(word, 2), WordByte(word, 3)) == word
  {
    assert word as int == 256 * (word as int / 256) + word as int % 256;
    assert word as int / 256 == 256 * (word as int / 65536) + word as int / 256 % 256;
    assert word as int / 65536 == 256 * (word as int / 16777216) + word as int / 65536 % 256;
  }

  lemma BytesWordRoundTrip(b0: Byte, b1: Byte, b2: Byte, b3: Byte)
    ensures WordByte(DecodeWord(b0, b1, b2, b3), 0) == b0
    ensures WordByte(DecodeWord(b0, b1, b2, b3), 1) == b1
    ensures WordByte(DecodeWord(b0, b1, b2, b3), 2) == b2
    ensures WordByte(DecodeWord(b0, b1, b2, b3), 3) == b3
  { }

  lemma RowPointerRoundTrip(page: U64, row: U32)
    ensures EncodeRowPointer(page, row) == 4294967296 <==> page > 16777215 || row > 255
    ensures EncodeRowPointer(page, row) < 4294967296 ==>
      PointerPage(EncodeRowPointer(page, row) as U32) as U64 == page &&
      PointerRow(EncodeRowPointer(page, row) as U32) as U32 == row
  { }

  lemma EveryPointerRoundTrips(pointer: U32)
    ensures PointerPage(pointer) <= 16777215
    ensures EncodeRowPointer(PointerPage(pointer) as U64, PointerRow(pointer) as U32) == pointer as U64
  { }

  lemma CompleteDescriptorRoundTrip(length: U32, mode: Byte, firstDp: U32, token: U32)
    requires length <= 1073741823 && IsSupportedStorageMode(mode)
    ensures DecodeLength(DecodeWord(WordByte(EncodeLengthAndMode(length, mode) as U32, 0),
      WordByte(EncodeLengthAndMode(length, mode) as U32, 1), WordByte(EncodeLengthAndMode(length, mode) as U32, 2),
      WordByte(EncodeLengthAndMode(length, mode) as U32, 3))) == length
    ensures DecodeStorageMode(DecodeWord(WordByte(EncodeLengthAndMode(length, mode) as U32, 0),
      WordByte(EncodeLengthAndMode(length, mode) as U32, 1), WordByte(EncodeLengthAndMode(length, mode) as U32, 2),
      WordByte(EncodeLengthAndMode(length, mode) as U32, 3))) == mode
    ensures DecodeWord(WordByte(firstDp, 0), WordByte(firstDp, 1), WordByte(firstDp, 2), WordByte(firstDp, 3)) == firstDp
    ensures DecodeWord(WordByte(token, 0), WordByte(token, 1), WordByte(token, 2), WordByte(token, 3)) == token
  {
    SupportedFieldsRoundTrip(length, mode);
    WordBytesRoundTrip(EncodeLengthAndMode(length, mode) as U32);
    WordBytesRoundTrip(firstDp);
    WordBytesRoundTrip(token);
  }

  lemma DecodeWordHasExactLittleEndianValue(b0: Byte, b1: Byte, b2: Byte, b3: Byte)
    ensures DecodeWord(b0, b1, b2, b3) as int ==
      b0 as int + 256 * b1 as int + 65536 * b2 as int + 16777216 * b3 as int
    ensures DecodeWord(b0, b1, b2, b3) as int % 256 == b0 as int
    ensures DecodeWord(b0, b1, b2, b3) as int / 16777216 == b3 as int
  { }

  lemma EveryWordHasExactLengthAndMode(word: U32)
    ensures 0 <= DecodeLength(word) <= 1073741823
    ensures DecodeStorageMode(word) == 0 || DecodeStorageMode(word) == 64 ||
      DecodeStorageMode(word) == 128 || DecodeStorageMode(word) == 192
    ensures word as int == DecodeLength(word) as int + DecodeStorageMode(word) as int * 16777216
    ensures IsSupportedStorageMode(DecodeStorageMode(word)) <==> word < 3221225472
  {
    var quotient := word as int / 1073741824;
    assert 0 <= quotient <= 3;
    if quotient == 0 { } else if quotient == 1 { } else if quotient == 2 { } else { assert quotient == 3; }
    assert word as int == quotient * 1073741824 + word as int % 1073741824;
  }

  lemma EncodingRefusesExactlyInvalidFields(length: U32, mode: Byte)
    ensures EncodeLengthAndMode(length, mode) == 4294967296 <==>
      length > 1073741823 || !IsSupportedStorageMode(mode)
    ensures EncodeLengthAndMode(length, mode) != 4294967296 ==>
      EncodeLengthAndMode(length, mode) <= 3221225471
  { }

  lemma SupportedFieldsRoundTrip(length: U32, mode: Byte)
    requires length <= 1073741823
    requires IsSupportedStorageMode(mode)
    ensures EncodeLengthAndMode(length, mode) < 4294967296
    ensures DecodeLength(EncodeLengthAndMode(length, mode) as U32) == length
    ensures DecodeStorageMode(EncodeLengthAndMode(length, mode) as U32) == mode
  {
    if mode == 0 { } else if mode == 64 { } else { assert mode == 128; }
  }

  lemma SupportedWordsRoundTrip(word: U32)
    requires IsSupportedStorageMode(DecodeStorageMode(word))
    ensures EncodeLengthAndMode(DecodeLength(word), DecodeStorageMode(word)) == word as U64
  {
    EveryWordHasExactLengthAndMode(word);
  }
}
