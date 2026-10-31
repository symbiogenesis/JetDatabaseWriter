// Allocation-free ordered bitmap enumeration. Native page acquisition, list
// append effects, cancellation and storage ownership remain caller contracts.
module UsageMapNativeTypes {
  newtype {:nativeType "uint"} U32 = x: int | 0 <= x < 4294967296
  newtype {:nativeType "int"} I32 = x: int | -2147483648 <= x < 2147483648
  newtype {:nativeType "long"} I64 = x: int | -9223372036854775808 <= x < 9223372036854775808
  newtype {:nativeType "byte"} Byte = x: int | 0 <= x < 256
}

module UsageMapKernel {
  import opened UsageMapNativeTypes

  function HasScanRange(length: U32, offset: U32, count: U32, basePage: I64, cursor: U32): bool {
    offset <= length && count <= length - offset && count <= 536870911 &&
    cursor <= count * 8 && basePage <= 9223372036854775807 - (count as I64) * 8
  }

  function Marked(value: Byte, bit: U32): bool
    requires bit < 8
  {
    ((value as bv8) & ((1 as bv8) << (bit as bv8))) != 0
  }

  opaque ghost predicate Selected(bytes: seq<Byte>, offset: U32, count: U32,
      basePage: I64, total: I64, minimum: I64, strict: bool, bit: nat)
    requires offset as int + count as int <= |bytes|
  {
    bit < count as int * 8 && Marked(bytes[offset as int + bit / 8], (bit % 8) as U32) &&
    (strict || minimum as int <= basePage as int + bit < total as int)
  }

  ghost predicate ExactPrefix(bytes: seq<Byte>, offset: U32, count: U32,
      basePage: I64, total: I64, minimum: I64, cursor: U32, emitted: seq<I64>)
    requires offset as int + count as int <= |bytes| && cursor as int <= count as int * 8
  {
    (forall first: nat, second: nat :: first < second < |emitted| ==> emitted[first] < emitted[second]) &&
    (forall page: I64 :: page in emitted <==>
      exists bit: nat :: bit < cursor as int &&
        Selected(bytes, offset, count, basePage, total, minimum, false, bit) && page as int == basePage as int + bit)
  }

  lemma EmptyPrefixIsExact(bytes: seq<Byte>, offset: U32, count: U32,
      basePage: I64, total: I64, minimum: I64)
    requires offset as int + count as int <= |bytes|
    ensures ExactPrefix(bytes, offset, count, basePage, total, minimum, 0, [])
  { }

  // This is the list-effect contract used by the small production adapter:
  // appending a returned member preserves exact membership and strict order.
  // Every emission decreases the same finite bit-window rank.
  lemma AppendMemberPreservesExactPrefix(bytes: seq<Byte>, offset: U32, count: U32,
      basePage: I64, total: I64, minimum: I64, cursor: U32, next: U32,
      emitted: seq<I64>, page: I64)
    requires offset as int + count as int <= |bytes| && cursor < next && next as int <= count as int * 8
    requires ExactPrefix(bytes, offset, count, basePage, total, minimum, cursor, emitted)
    requires page as int == basePage as int + next as int - 1
    requires Selected(bytes, offset, count, basePage, total, minimum, false, next as int - 1)
    requires forall bit: nat :: cursor as int <= bit < next as int - 1 ==>
      !Selected(bytes, offset, count, basePage, total, minimum, false, bit)
    ensures ExactPrefix(bytes, offset, count, basePage, total, minimum, next, emitted + [page])
    ensures count as int * 8 - next as int < count as int * 8 - cursor as int
  {
    forall previous: I64 | previous in emitted
      ensures previous < page
    {
      var bit: nat :| bit < cursor as int &&
        Selected(bytes, offset, count, basePage, total, minimum, false, bit) && previous as int == basePage as int + bit;
    }
    var appended := emitted + [page];
    forall first: nat, second: nat | first < second < |appended|
      ensures appended[first] < appended[second]
    {
      if second < |emitted| {
        assert appended[first] == emitted[first] && appended[second] == emitted[second];
      } else {
        assert second == |emitted| && appended[second] == page;
        assert appended[first] in emitted;
      }
    }
    forall member: I64
      ensures member in emitted + [page] <==>
        exists bit: nat :: bit < next as int &&
          Selected(bytes, offset, count, basePage, total, minimum, false, bit) && member as int == basePage as int + bit
    {
      if member in emitted {
        var bit: nat :| bit < cursor as int &&
          Selected(bytes, offset, count, basePage, total, minimum, false, bit) && member as int == basePage as int + bit;
      } else if member == page {
        assert Selected(bytes, offset, count, basePage, total, minimum, false, next as int - 1);
      } else if (exists bit: nat :: (bit < next as int &&
          Selected(bytes, offset, count, basePage, total, minimum, false, bit) && member as int == basePage as int + bit)) {
        var bit: nat :| bit < next as int &&
          Selected(bytes, offset, count, basePage, total, minimum, false, bit) && member as int == basePage as int + bit;
        if bit >= cursor as int { assert bit == next as int - 1; }
      }
    }
  }

  lemma ExhaustionCompletesExactMembership(bytes: seq<Byte>, offset: U32, count: U32,
      basePage: I64, total: I64, minimum: I64, cursor: U32, emitted: seq<I64>)
    requires offset as int + count as int <= |bytes| && cursor as int <= count as int * 8 < 4294967296
    requires ExactPrefix(bytes, offset, count, basePage, total, minimum, cursor, emitted)
    requires forall bit: nat :: cursor as int <= bit < count as int * 8 ==>
      !Selected(bytes, offset, count, basePage, total, minimum, false, bit)
    ensures ExactPrefix(bytes, offset, count, basePage, total, minimum, (count as int * 8) as U32, emitted)
  { }

  // Compose an ordered, faithful mask expansion with the already emitted
  // prefix. ComposeByteBatch supplies exactly this interval-membership law;
  // the C# adapter's eight ordered appends remain its explicit list effect.
  lemma AppendBatchPreservesExactPrefix(bytes: seq<Byte>, offset: U32, count: U32,
      basePage: I64, total: I64, minimum: I64, cursor: U32, next: U32,
      emitted: seq<I64>, batch: seq<I64>)
    requires offset as int + count as int <= |bytes| && cursor <= next && next as int <= count as int * 8
    requires ExactPrefix(bytes, offset, count, basePage, total, minimum, cursor, emitted)
    requires forall first: nat, second: nat :: first < second < |batch| ==> batch[first] < batch[second]
    requires forall page: I64 :: page in batch <==>
      exists bit: nat :: cursor as int <= bit < next as int &&
        Selected(bytes, offset, count, basePage, total, minimum, false, bit) && page as int == basePage as int + bit
    ensures ExactPrefix(bytes, offset, count, basePage, total, minimum, next, emitted + batch)
  {
    var combined := emitted + batch;
    forall first: nat, second: nat | first < second < |combined|
      ensures combined[first] < combined[second]
    {
      if second < |emitted| {
        assert combined[first] == emitted[first] && combined[second] == emitted[second];
      } else if first >= |emitted| {
        assert combined[first] == batch[first - |emitted|] && combined[second] == batch[second - |emitted|];
      } else {
        assert combined[first] in emitted && combined[second] in batch;
        var before: nat :| before < cursor as int &&
          Selected(bytes, offset, count, basePage, total, minimum, false, before) && combined[first] as int == basePage as int + before;
        var after: nat :| cursor as int <= after < next as int &&
          Selected(bytes, offset, count, basePage, total, minimum, false, after) && combined[second] as int == basePage as int + after;
      }
    }
    forall page: I64
      ensures page in combined <==>
        exists bit: nat :: bit < next as int &&
          Selected(bytes, offset, count, basePage, total, minimum, false, bit) && page as int == basePage as int + bit
    {
      if page in emitted {
        var bit: nat :| bit < cursor as int &&
          Selected(bytes, offset, count, basePage, total, minimum, false, bit) && page as int == basePage as int + bit;
      } else if page in batch {
        var bit: nat :| cursor as int <= bit < next as int &&
          Selected(bytes, offset, count, basePage, total, minimum, false, bit) && page as int == basePage as int + bit;
      } else if (exists bit: nat :: (bit < next as int &&
          Selected(bytes, offset, count, basePage, total, minimum, false, bit) && page as int == basePage as int + bit)) {
        var bit: nat :| bit < next as int &&
          Selected(bytes, offset, count, basePage, total, minimum, false, bit) && page as int == basePage as int + bit;
        if bit < cursor as int { assert page in emitted; }
        else { assert page in batch; }
      }
    }
  }

  ghost function BatchStart(next: U32): U32 {
    if next == 0 then 0 else (next - 1) / 8 * 8
  }
  function PageOffset(firstPage: I64, bit: Byte): I64
    requires bit < 8 && firstPage <= 9223372036854775800
  { firstPage + bit as I64 }
  function AddMember(members: Byte, bit: U32): Byte
    requires bit < 8
  { ((members as bv8) | ((1 as bv8) << (bit as bv8))) as Byte }

  lemma AddedMemberIsExact(members: Byte, bit: U32)
    requires bit < 8
    ensures forall candidate: U32 :: candidate < 8 ==>
      (Marked(AddMember(members, bit), candidate) <==> candidate == bit || Marked(members, candidate))
  { }
  lemma ByteSelectionIsExact(bytes: seq<Byte>, offset: U32, count: U32, index: U32,
      basePage: I64, total: I64, minimum: I64, strict: bool, bit: U32)
    requires offset as int + count as int <= |bytes| && index < count && bit < 8
    ensures Selected(bytes, offset, count, basePage, total, minimum, strict, index as int * 8 + bit as int) <==>
      Marked(bytes[offset as int + index as int], bit) && (strict || minimum as int <= basePage as int + index as int * 8 + bit as int < total as int)
  { reveal Selected(); }

  lemma AdmittedSelectionIsRelevant(bytes: seq<Byte>, offset: U32, count: U32,
      basePage: I64, total: I64, minimum: I64, strict: bool, bit: nat)
    requires offset as int + count as int <= |bytes|
    ensures Selected(bytes, offset, count, basePage, total, minimum, false, bit) ==>
      Selected(bytes, offset, count, basePage, total, minimum, strict, bit)
  { reveal Selected(); }

  lemma ComposeByteBatch(bytes: seq<Byte>, offset: U32, count: U32, index: U32,
      basePage: I64, total: I64, minimum: I64, strict: bool, cursor: U32, upto: U32, members: Byte)
    requires offset as int + count as int <= |bytes| && index < count && upto <= 8
    requires cursor % 8 == 0 && cursor as int <= index as int * 8
    requires forall bit: nat :: cursor as int <= bit < index as int * 8 ==>
      !Selected(bytes, offset, count, basePage, total, minimum, strict, bit)
    requires forall local: U32 :: local < 8 ==>
      (Marked(members, local) <==> (local < upto &&
        Selected(bytes, offset, count, basePage, total, minimum, false, index as int * 8 + local as int)))
    requires forall local: U32 {:trigger Marked(bytes[offset as int + index as int], local)} :: local < upto && Marked(bytes[offset as int + index as int], local) ==>
      !strict || minimum as int <= basePage as int + index as int * 8 + local as int < total as int
    ensures forall bit: nat :: cursor as int <= bit < index as int * 8 + upto as int ==>
      (Selected(bytes, offset, count, basePage, total, minimum, false, bit) <==>
        index as int * 8 <= bit < index as int * 8 + 8 && Marked(members, (bit % 8) as U32))
    ensures forall bit: nat :: cursor as int <= bit < index as int * 8 + upto as int ==>
      (Selected(bytes, offset, count, basePage, total, minimum, strict, bit) <==>
       Selected(bytes, offset, count, basePage, total, minimum, false, bit))
  {
    forall bit: nat | cursor as int <= bit < index as int * 8 + upto as int
      ensures Selected(bytes, offset, count, basePage, total, minimum, false, bit) <==>
        index as int * 8 <= bit < index as int * 8 + 8 && Marked(members, (bit % 8) as U32)
      ensures Selected(bytes, offset, count, basePage, total, minimum, strict, bit) <==>
        Selected(bytes, offset, count, basePage, total, minimum, false, bit)
    {
      if bit < index as int * 8 {
        assert !Selected(bytes, offset, count, basePage, total, minimum, strict, bit);
        AdmittedSelectionIsRelevant(bytes, offset, count, basePage, total, minimum, strict, bit);
      } else {
        var local: U32 := (bit % 8) as U32;
        assert bit == index as int * 8 + local as int && local < upto;
        assert Marked(members, local) <==>
          Selected(bytes, offset, count, basePage, total, minimum, false, index as int * 8 + local as int);
        ByteSelectionIsExact(bytes, offset, count, index, basePage, total, minimum, strict, local);
        ByteSelectionIsExact(bytes, offset, count, index, basePage, total, minimum, false, local);
      }
    }
  }

  // Complete range admission precedes this hot scanner exactly once. State1
  // exhausts the window, state2 emits one complete byte's admitted mask and
  // state3 refuses its first invalid marked bit. A refusing result can carry
  // earlier members from that same byte: the adapter appends its mask before
  // observing terminal status. Continuing cursors are always byte aligned.
  opaque ghost predicate BatchResult(bytes: seq<Byte>, offset: U32, count: U32,
      basePage: I64, total: I64, minimum: I64, strict: bool, cursor: U32, next: U32, members: Byte, state: Byte)
    requires offset as int + count as int <= |bytes| && count <= 536870911 && cursor <= count * 8
  {
    (1 <= state <= 3) &&
    (cursor <= next <= count * 8) &&
    (state == 1 ==> next == count * 8 && members == 0) &&
    (state == 2 ==>
      cursor < next && next % 8 == 0 && members != 0) &&
    (state == 3 ==> (strict && cursor < next &&
      Selected(bytes, offset, count, basePage, total, minimum, true, next as int - 1) &&
      !Selected(bytes, offset, count, basePage, total, minimum, false, next as int - 1))) &&
    (members != 0 ==>
      next > 0 && basePage <= 9223372036854775807 - (BatchStart(next) as I64) - 7) &&
    (forall bit: U32 :: bit < 8 ==>
      (Marked(members, bit) <==>
        cursor <= BatchStart(next) + bit <
          (if state == 3 then next - 1 else next) &&
        Selected(bytes, offset, count, basePage, total, minimum, false, BatchStart(next) as int + bit as int))) &&
    (forall bit: nat :: (cursor as int <= bit <
        (if state == 3 then next as int - 1 else next as int)) ==>
      (Selected(bytes, offset, count, basePage, total, minimum, false, bit) <==>
        BatchStart(next) as int <= bit < BatchStart(next) as int + 8 &&
          Marked(members, (bit % 8) as U32))) &&
    (forall bit: nat :: (cursor as int <= bit <
        (if state == 3 then next as int - 1 else next as int)) ==>
      (Selected(bytes, offset, count, basePage, total, minimum, strict, bit) <==>
       Selected(bytes, offset, count, basePage, total, minimum, false, bit)))
  }

  lemma FullByteBatchResult(bytes: seq<Byte>, offset: U32, count: U32, index: U32,
      basePage: I64, total: I64, minimum: I64, strict: bool, cursor: U32, members: Byte)
    requires offset as int + count as int <= |bytes| && count <= 536870911 && index < count
    requires basePage as int <= 9223372036854775807 - count as int * 8
    requires cursor % 8 == 0 && cursor as int <= index as int * 8 && members != 0
    requires forall bit: nat :: cursor as int <= bit < index as int * 8 ==>
      !Selected(bytes, offset, count, basePage, total, minimum, strict, bit)
    requires forall local: U32 :: local < 8 ==>
      (Marked(members, local) <==> Selected(bytes, offset, count, basePage, total, minimum, false, index as int * 8 + local as int))
    requires forall local: U32 {:trigger Marked(bytes[offset as int + index as int], local)} :: local < 8 && Marked(bytes[offset as int + index as int], local) ==>
      !strict || minimum as int <= basePage as int + index as int * 8 + local as int < total as int
    ensures BatchResult(bytes, offset, count, basePage, total, minimum, strict, cursor, (index + 1) * 8, members, 2)
  {
    reveal BatchResult();
    ComposeByteBatch(bytes, offset, count, index, basePage, total, minimum, strict, cursor, 8, members);
    assert BatchStart((index + 1) * 8) == index * 8;
  }

  lemma RefusedByteBatchResult(bytes: seq<Byte>, offset: U32, count: U32, index: U32,
      basePage: I64, total: I64, minimum: I64, cursor: U32, upto: U32, members: Byte)
    requires offset as int + count as int <= |bytes| && count <= 536870911 && index < count && upto < 8
    requires basePage as int <= 9223372036854775807 - count as int * 8
    requires cursor % 8 == 0 && cursor as int <= index as int * 8
    requires forall bit: nat :: cursor as int <= bit < index as int * 8 ==>
      !Selected(bytes, offset, count, basePage, total, minimum, true, bit)
    requires forall local: U32 :: local < 8 ==>
      (Marked(members, local) <==> (local < upto && Selected(bytes, offset, count, basePage, total, minimum, false, index as int * 8 + local as int)))
    requires forall local: U32 {:trigger Marked(bytes[offset as int + index as int], local)} :: local < upto && Marked(bytes[offset as int + index as int], local) ==>
      minimum as int <= basePage as int + index as int * 8 + local as int < total as int
    requires Marked(bytes[offset as int + index as int], upto)
    requires basePage as int + index as int * 8 + upto as int < minimum as int || basePage as int + index as int * 8 + upto as int >= total as int
    ensures BatchResult(bytes, offset, count, basePage, total, minimum, true, cursor, index * 8 + upto + 1, members, 3)
  {
    reveal BatchResult();
    ComposeByteBatch(bytes, offset, count, index, basePage, total, minimum, true, cursor, upto, members);
    ByteSelectionIsExact(bytes, offset, count, index, basePage, total, minimum, true, upto);
    ByteSelectionIsExact(bytes, offset, count, index, basePage, total, minimum, false, upto);
    assert BatchStart(index * 8 + upto + 1) == index * 8;
  }

  lemma FilteredByteIsEmpty(bytes: seq<Byte>, offset: U32, count: U32, index: U32,
      basePage: I64, total: I64, minimum: I64, strict: bool, members: Byte)
    requires offset as int + count as int <= |bytes| && count <= 536870911 && index < count && members == 0
    requires forall local: U32 :: local < 8 ==>
      (Marked(members, local) <==> Selected(bytes, offset, count, basePage, total, minimum, false, index as int * 8 + local as int))
    requires forall local: U32 {:trigger Marked(bytes[offset as int + index as int], local)} :: local < 8 && Marked(bytes[offset as int + index as int], local) ==>
      !strict || minimum as int <= basePage as int + index as int * 8 + local as int < total as int
    ensures forall bit: nat :: index as int * 8 <= bit < (index as int + 1) * 8 ==>
      !Selected(bytes, offset, count, basePage, total, minimum, strict, bit) &&
      !Selected(bytes, offset, count, basePage, total, minimum, false, bit)
  {
    ComposeByteBatch(bytes, offset, count, index, basePage, total, minimum, strict, index * 8, 8, members);
  }

  lemma ZeroByteIsEmpty(bytes: seq<Byte>, offset: U32, count: U32, index: U32,
      basePage: I64, total: I64, minimum: I64, strict: bool)
    requires offset as int + count as int <= |bytes| && index < count && bytes[offset as int + index as int] == 0
    ensures forall bit: nat :: index as int * 8 <= bit < (index as int + 1) * 8 ==>
      !Selected(bytes, offset, count, basePage, total, minimum, strict, bit) &&
      !Selected(bytes, offset, count, basePage, total, minimum, false, bit)
  {
    forall bit: nat | index as int * 8 <= bit < (index as int + 1) * 8
      ensures !Selected(bytes, offset, count, basePage, total, minimum, strict, bit)
      ensures !Selected(bytes, offset, count, basePage, total, minimum, false, bit)
    {
      var local: U32 := (bit % 8) as U32;
      assert local < 8 && bit == index as int * 8 + local as int;
      ByteSelectionIsExact(bytes, offset, count, index, basePage, total, minimum, strict, local);
      ByteSelectionIsExact(bytes, offset, count, index, basePage, total, minimum, false, local);
    }
  }

  method NextBitmapPage(bytes: array<Byte>, offset: U32, count: U32,
      basePage: I64, total: I64, minimum: I64, strict: bool, cursor: U32)
      returns (next: U32, members: Byte, state: Byte, firstPage: I64)
    requires bytes.Length <= 2147483647
    requires HasScanRange(bytes.Length as U32, offset, count, basePage, cursor)
    requires cursor % 8 == 0
    ensures BatchResult(bytes[..], offset, count, basePage, total, minimum, strict, cursor, next, members, state)
    ensures state == 1 ==> firstPage == 0
    ensures state != 1 ==> next > 0 && firstPage as int == basePage as int + BatchStart(next) as int && firstPage <= 9223372036854775800
  {
    next, members, state, firstPage := cursor, 0, 1, 0;
    var start: I32 := offset as I32;
    var limit: I32 := count as I32;
    var scanIndex: I32 := (cursor / 8) as I32;
    while scanIndex < limit
      invariant cursor as int / 8 <= scanIndex as int <= count as int
      invariant members == 0 && state == 1
      invariant forall bit: nat :: cursor as int <= bit < scanIndex as int * 8 ==>
        !Selected(bytes[..], offset, count, basePage, total, minimum, strict, bit)
      invariant forall bit: nat :: cursor as int <= bit < scanIndex as int * 8 ==>
        !Selected(bytes[..], offset, count, basePage, total, minimum, false, bit)
      decreases limit - scanIndex
    {
      ghost var index: U32 := scanIndex as U32;
      var value := bytes[start + scanIndex];
      if value != 0 {
        firstPage := basePage + (scanIndex as I64) * 8;
        if firstPage >= minimum && firstPage + 7 < total {
          forall local: U32 {:trigger Marked(value, local)} | local < 8
            ensures Marked(value, local) <==>
              Selected(bytes[..], offset, count, basePage, total, minimum, false, index as int * 8 + local as int)
            ensures Selected(bytes[..], offset, count, basePage, total, minimum, strict, index as int * 8 + local as int) <==>
              Selected(bytes[..], offset, count, basePage, total, minimum, false, index as int * 8 + local as int)
          {
            ByteSelectionIsExact(bytes[..], offset, count, index, basePage, total, minimum, strict, local);
            ByteSelectionIsExact(bytes[..], offset, count, index, basePage, total, minimum, false, local);
          }
          FullByteBatchResult(bytes[..], offset, count, index, basePage, total, minimum, strict, cursor, value);
          next, members, state := ((scanIndex as U32) + 1) * 8, value, 2;
          return {:isolate};
        }
        members := 0;
        var bit: U32 := 0;
        while bit < 8
          invariant bit <= 8
          invariant forall previous: U32 :: previous < bit ==>
            (Marked(members, previous) <==>
              Selected(bytes[..], offset, count, basePage, total, minimum, false, index as int * 8 + previous as int))
          invariant forall following: U32 :: bit <= following < 8 ==> !Marked(members, following)
          invariant forall previous: U32 :: previous < bit && Marked(value, previous) ==>
            !strict || minimum as int <= firstPage as int + previous as int < total as int
          decreases 8 - bit
        {
          ghost var prior := members;
          ByteSelectionIsExact(bytes[..], offset, count, index, basePage, total, minimum, strict, bit);
          ByteSelectionIsExact(bytes[..], offset, count, index, basePage, total, minimum, false, bit);
          if Marked(value, bit) {
            var candidate: I64 := firstPage + bit as I64;
            if candidate < minimum || candidate >= total {
              if strict {
                RefusedByteBatchResult(bytes[..], offset, count, index, basePage, total, minimum, cursor, bit, members);
                next, state := (scanIndex as U32) * 8 + bit + 1, 3;
                return {:isolate};
              }
            } else {
              AddedMemberIsExact(members, bit);
              members := AddMember(members, bit);
            }
          }
          assert Selected(bytes[..], offset, count, basePage, total, minimum, strict, index as int * 8 + bit as int) <==>
            Selected(bytes[..], offset, count, basePage, total, minimum, false, index as int * 8 + bit as int);
          assert Marked(members, bit) <==>
            Selected(bytes[..], offset, count, basePage, total, minimum, false, index as int * 8 + bit as int);
          forall previous: U32 | previous <= bit
            ensures Marked(members, previous) <==>
              Selected(bytes[..], offset, count, basePage, total, minimum, false, index as int * 8 + previous as int)
          {
            if previous < bit { assert Marked(members, previous) == Marked(prior, previous); }
          }
          forall following: U32 | bit < following < 8
            ensures !Marked(members, following)
          { assert Marked(members, following) == Marked(prior, following); }
          forall previous: U32 | previous <= bit && Marked(value, previous)
            ensures !strict || minimum as int <= firstPage as int + previous as int < total as int
          { }
          bit := bit + 1;
        }
        if members != 0 {
          FullByteBatchResult(bytes[..], offset, count, index, basePage, total, minimum, strict, cursor, members);
          next, state := ((scanIndex as U32) + 1) * 8, 2;
          return {:isolate};
        }
        FilteredByteIsEmpty(bytes[..], offset, count, index, basePage, total, minimum, strict, members);
      } else {
        ZeroByteIsEmpty(bytes[..], offset, count, index, basePage, total, minimum, strict);
      }
      scanIndex := scanIndex + 1;
    }
    next, firstPage := count * 8, 0;
    reveal BatchResult();
    return {:isolate};
  }
}
