// Allocation-free ordered bitmap enumeration. Native page acquisition, list
// append effects, cancellation and storage ownership remain caller contracts.
module UsageMapNativeTypes {
  newtype {:nativeType "ulong"} U64 = x: int | 0 <= x < 18446744073709551616
  newtype {:nativeType "uint"} U32 = x: int | 0 <= x < 4294967296
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

  function Shifted(value: Byte, bit: U32): Byte
    requires bit < 8
  {
    ((value as bv8) >> (bit as bv8)) as Byte
  }

  lemma ShiftedBit(value: Byte, shift: U32, bit: U32)
    requires shift <= bit < 8
    ensures Marked(Shifted(value, shift), bit - shift) == Marked(value, bit)
  { }

  lemma ShiftedNext(value: Byte, bit: U32)
    requires bit < 7
    ensures Shifted(value, bit + 1) == Shifted(value, bit) / 2
  { }

  ghost predicate Selected(bytes: seq<Byte>, offset: U32, count: U32,
      basePage: I64, total: I64, minimum: I64, strict: bool, bit: nat)
    requires offset as int + count as int <= |bytes|
  {
    bit < count as int * 8 && Marked(bytes[offset as int + bit / 8], (bit % 8) as U32) &&
    (strict || minimum as int <= basePage as int + bit < total as int)
  }

  ghost predicate Continuation(bytes: seq<Byte>, offset: U32, count: U32,
      cursor: U32, remaining: Byte)
    requires offset as int + count as int <= |bytes| && cursor as int <= count as int * 8
  {
    cursor as int == count as int * 8 || cursor % 8 == 0 ||
      remaining == Shifted(bytes[offset as int + cursor as int / 8], cursor % 8)
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

  // The low word is the consumed-bit cursor; the next byte retains the
  // shifted current bitmap byte. The final byte is the operation status.
  function Pack(cursor: U32, remaining: Byte, state: Byte): U64 {
    cursor as U64 + (remaining as U64) * 4294967296 + (state as U64) * 1099511627776
  }
  function Cursor(decision: U64): U32 { (decision % 4294967296) as U32 }
  function Remaining(decision: U64): Byte { (decision / 4294967296 % 256) as Byte }
  function State(decision: U64): Byte { (decision / 1099511627776 % 256) as Byte }
  function Page(basePage: I64, decision: U64): I64
    requires Cursor(decision) > 0
    requires basePage <= 9223372036854775807 - ((Cursor(decision) - 1) as I64)
  { basePage + ((Cursor(decision) - 1) as I64) }

  lemma PackedFieldsAreExact(cursor: U32, remaining: Byte, state: Byte)
    ensures Cursor(Pack(cursor, remaining, state)) == cursor
    ensures Remaining(Pack(cursor, remaining, state)) == remaining
    ensures State(Pack(cursor, remaining, state)) == state
  { }

  // Complete range admission precedes this hot scanner exactly once. State
  // 1 exhausts the window, 2 emits its first admitted member and 3 refuses
  // its first invalid marked member in strict mode. Only primitive state is
  // retained between calls; C# appends each returned member to its list.
  method NextBitmapPage(bytes: array<Byte>, offset: U32, count: U32,
      basePage: I64, total: I64, minimum: I64, strict: bool, continuation: U64)
      returns (decision: U64)
    requires bytes.Length <= 2147483647
    requires HasScanRange(bytes.Length as U32, offset, count, basePage, Cursor(continuation))
    requires Continuation(bytes[..], offset, count, Cursor(continuation), Remaining(continuation))
    ensures 1 <= State(decision) <= 3
    ensures Cursor(continuation) <= Cursor(decision) <= count * 8
    ensures Continuation(bytes[..], offset, count, Cursor(decision), Remaining(decision))
    ensures State(decision) == 1 ==> Cursor(decision) == count * 8
    ensures State(decision) == 2 || State(decision) == 3 ==>
      Cursor(continuation) < Cursor(decision) &&
      Selected(bytes[..], offset, count, basePage, total, minimum, strict, Cursor(decision) as int - 1)
    ensures State(decision) == 2 ==> minimum <= Page(basePage, decision) < total
    ensures State(decision) == 3 ==> strict && (Page(basePage, decision) < minimum || Page(basePage, decision) >= total)
    ensures forall bit: nat :: (Cursor(continuation) as int <= bit <
        (if State(decision) == 1 then Cursor(decision) as int else Cursor(decision) as int - 1)) ==>
      !Selected(bytes[..], offset, count, basePage, total, minimum, strict, bit)
  {
    var cursor := Cursor(continuation);
    var next := cursor;
    var limit: U32 := count * 8;
    var value: Byte := Remaining(continuation);
    while next < limit
      invariant cursor <= next <= limit
      invariant Continuation(bytes[..], offset, count, next, value)
      invariant forall bit: nat :: cursor as int <= bit < next as int ==>
        !Selected(bytes[..], offset, count, basePage, total, minimum, strict, bit)
      decreases limit - next
    {
      var index: U32 := next / 8;
      if next % 8 == 0 { value := bytes[offset + index]; }
      assert value == Shifted(bytes[offset + index], next % 8);
      if value == 0 {
        var following: U32 := (index + 1) * 8;
        forall bit: nat | next as int <= bit < following as int
          ensures !Selected(bytes[..], offset, count, basePage, total, minimum, strict, bit)
        {
          assert bit / 8 == index as int;
          ShiftedBit(bytes[offset + index], next % 8, (bit % 8) as U32);
        }
        next := following;
      } else {
        var bit: U32 := next % 8;
        var candidate: I64 := basePage + next as I64;
        ShiftedBit(bytes[offset + index], bit, bit);
        var marked := Marked(value, 0);
        if bit < 7 { ShiftedNext(bytes[offset + index], bit); }
        value := value / 2;
        next := next + 1;
        if marked {
          if candidate < minimum || candidate >= total {
            if strict {
              PackedFieldsAreExact(next, value, 3);
              decision := Pack(next, value, 3);
              return;
            }
          } else {
            PackedFieldsAreExact(next, value, 2);
            decision := Pack(next, value, 2);
            return;
          }
        }
      }
    }
    PackedFieldsAreExact(next, 0, 1);
    decision := Pack(next, 0, 1);
  }
}
