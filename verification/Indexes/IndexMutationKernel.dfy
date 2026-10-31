// Executing native index-page emission and monotone bookmark relocation.
// Key-suffix copies and immutable ledger acquisition remain adapter effects;
// key correspondence, collation and tree ownership are separate obligations.
module IndexMutationNativeTypes {
  newtype {:nativeType "uint"} U32 = x: int | 0 <= x < 4294967296
  newtype {:nativeType "ulong"} U64 = x: int | 0 <= x < 18446744073709551616
  newtype {:nativeType "long"} I64 = x: int | -9223372036854775808 <= x < 9223372036854775808
  newtype {:nativeType "byte"} Byte = x: int | 0 <= x < 256
}

module IndexMutationKernel {
  import opened IndexMutationNativeTypes

  // The disk-backed relocation adapter supplies complete immutable records.
  // These controls retain a monotone injective identity map without allocating
  // an executing sequence or retaining its contents in memory.
  function AdmitRelocationPair(source: U32, destination: U32, priorSource: U32, priorDestination: U32): bool {
    source / 256 != 0 && destination / 256 != 0 && priorSource < source && priorDestination < destination
  }

  ghost predicate MonotoneRelocation(sources: seq<U32>, destinations: seq<U32>) {
    |sources| == |destinations| &&
    (forall index: nat :: index < |sources| ==> sources[index] / 256 != 0 && destinations[index] / 256 != 0) &&
    (forall first: nat, second: nat :: first < second < |sources| ==>
      sources[first] < sources[second] && destinations[first] < destinations[second])
  }

  lemma AppendRelocationPreservesIdentity(sources: seq<U32>, destinations: seq<U32>, source: U32, destination: U32)
    requires MonotoneRelocation(sources, destinations)
    requires AdmitRelocationPair(source, destination, if |sources| == 0 then 0 else sources[|sources| - 1],
      if |destinations| == 0 then 0 else destinations[|destinations| - 1])
    ensures MonotoneRelocation(sources + [source], destinations + [destination])
  {
    forall first: nat, second: nat | first < second < |sources + [source]|
      ensures (sources + [source])[first] < (sources + [source])[second] &&
        (destinations + [destination])[first] < (destinations + [destination])[second]
    {
      if second == |sources| {
        assert first <= |sources| - 1;
        if first < |sources| - 1 {
          assert sources[first] < sources[|sources| - 1];
          assert destinations[first] < destinations[|destinations| - 1];
        }
      }
    }
  }

  lemma RelocationIsInjectiveAndOrdered(sources: seq<U32>, destinations: seq<U32>, first: nat, second: nat)
    requires MonotoneRelocation(sources, destinations)
    requires first < |sources| && second < |sources|
    ensures sources[first] == sources[second] <==> first == second
    ensures destinations[first] == destinations[second] <==> first == second
    ensures sources[first] < sources[second] <==> destinations[first] < destinations[second]
  {
    if first < second {
      assert sources[first] < sources[second] && destinations[first] < destinations[second];
    } else if second < first {
      assert sources[second] < sources[first] && destinations[second] < destinations[first];
    }
  }

  function RelocationMiddle(low: I64, high: I64): I64
    requires 0 <= low < high
    ensures low <= RelocationMiddle(low, high) < high
  { low + (((high - low) as U64 / 2) as I64) }

  ghost predicate RelocationSearchContains(sources: seq<U32>, target: U32, low: I64, high: I64) {
    0 <= low <= high && high as int <= |sources| &&
    (forall index: nat :: index < |sources| && sources[index] == target ==> low as int <= index < high as int)
  }

  // Ghost source contents describe the already validated immutable stream;
  // only the native interval, just-read identity and decision outputs execute.
  method AdvanceRelocationSearch(low: I64, high: I64, middle: I64, source: U32, target: U32,
      ghost sources: seq<U32>) returns (nextLow: I64, nextHigh: I64, found: bool)
    requires 0 <= low < high
    requires middle == RelocationMiddle(low, high)
    requires forall first: nat, second: nat :: first < second < |sources| ==> sources[first] < sources[second]
    requires RelocationSearchContains(sources, target, low, high)
    requires source == sources[middle as int]
    ensures found <==> source == target
    ensures found ==> nextLow == low && nextHigh == high
    ensures found ==> forall index: nat :: index < |sources| && sources[index] == target ==> index == middle as int
    ensures !found ==> RelocationSearchContains(sources, target, nextLow, nextHigh)
    ensures !found ==> low <= nextLow <= nextHigh <= high && nextHigh - nextLow < high - low
    ensures !found ==> nextHigh - nextLow <= (high - low) / 2
    ensures source < target ==> nextLow == middle + 1 && nextHigh == high
    ensures target < source ==> nextLow == low && nextHigh == middle
  {
    found := source == target;
    nextLow := low;
    nextHigh := high;
    if source < target {
      nextLow := middle + 1;
    } else if target < source {
      nextHigh := middle;
    }
    forall index: nat | index < |sources| && sources[index] == target
      ensures if found then index == middle as int else nextLow as int <= index < nextHigh as int
    {
      if index < middle as int {
        assert sources[index] < sources[middle as int];
      } else if middle as int < index {
        assert sources[middle as int] < sources[index];
      }
    }
  }

  lemma ExhaustedRelocationSearchRefuses(sources: seq<U32>, target: U32, cursor: I64)
    requires RelocationSearchContains(sources, target, cursor, cursor)
    ensures target !in sources
  { }

  lemma InitialRelocationSearchRetainsAllCandidates(sources: seq<U32>, target: U32)
    requires |sources| <= 9223372036854775807
    ensures RelocationSearchContains(sources, target, 0, |sources| as I64)
  { }

  lemma FoundRelocationRetainsExactIdentity(sources: seq<U32>, destinations: seq<U32>, target: U32, index: nat)
    requires MonotoneRelocation(sources, destinations)
    requires index < |sources| && sources[index] == target
    ensures destinations[index] / 256 != 0
    ensures forall other: nat :: other < |sources| && sources[other] == target ==>
      other == index && destinations[other] == destinations[index]
  {
    forall other: nat | other < |sources| && sources[other] == target
      ensures other == index && destinations[other] == destinations[index]
    {
      RelocationIsInjectiveAndOrdered(sources, destinations, index, other);
    }
  }

  lemma ReachableRelocationAndRefusal()
    ensures MonotoneRelocation([], [])
    ensures MonotoneRelocation([256, 511, 512, 4294967295], [256, 512, 768, 4294967295])
    ensures RelocationSearchContains([256, 511, 512, 4294967295], 511, 0, 4)
    ensures AdmitRelocationPair(4294967295, 4294967295, 512, 768)
    ensures !AdmitRelocationPair(512, 768, 512, 768)
    ensures !AdmitRelocationPair(512, 768, 511, 768)
    ensures !AdmitRelocationPair(255, 768, 0, 0)
    ensures RelocationMiddle(9223372036854775806, 9223372036854775807) == 9223372036854775806
  { }

  ghost function WordByte(word: U32, position: U32): Byte {
    if position == 0 then (word % 256) as Byte
    else if position == 1 then (word / 256 % 256) as Byte
    else if position == 2 then (word / 65536 % 256) as Byte
    else if position == 3 then (word / 16777216) as Byte else 0
  }

  function HasRange(length: U32, start: U32, count: U32): bool {
    start <= length && count <= length - start
  }

  ghost function PutWord(bytes: seq<Byte>, start: nat, word: U32): seq<Byte>
    requires start + 4 <= |bytes|
  { bytes[start := WordByte(word, 0)][start + 1 := WordByte(word, 1)]
      [start + 2 := WordByte(word, 2)][start + 3 := WordByte(word, 3)] }

  method WriteWord(page: array<Byte>, start: U32, word: U32)
    requires page.Length <= 2147483647 && HasRange(page.Length as U32, start, 4)
    modifies page
    ensures page[..] == PutWord(old(page[..]), start as int, word)
  {
    page[start] := (word % 256) as Byte;
    page[start + 1] := (word / 256 % 256) as Byte;
    page[start + 2] := (word / 65536 % 256) as Byte;
    page[start + 3] := (word / 16777216) as Byte;
  }

  // Complete bounded shared-prefix scan. Its maximality is needed for exact
  // compressed sizing, rather than merely a safe arbitrary shorter prefix.
  method CommonPrefix(first: array<Byte>, key: array<Byte>, limit: U32) returns (matched: U32)
    requires first.Length <= 2147483647 && key.Length <= 2147483647
    requires limit as int <= first.Length
    ensures matched <= limit && matched as int <= key.Length
    ensures forall i: nat :: i < matched as int ==> first[i] == key[i]
    ensures matched == limit || matched as int == key.Length || first[matched] != key[matched]
  {
    matched := 0;
    while matched < limit && matched < key.Length as U32 && first[matched] == key[matched]
      invariant matched <= limit && matched as int <= key.Length
      invariant forall i: nat :: i < matched as int ==> first[i] == key[i]
      decreases limit - matched
    { matched := matched + 1; }
  }

  // Zero refuses before allocation or emission; every admitted entry strictly
  // advances the cursor and has a representable nonfirst boundary bit.
  function AdvanceEntry(pageSize: U32, bitmap: U32, first: U32, cursor: U32,
      keyLength: U32, prefix: U32, initial: bool, intermediate: bool): U32 {
    if first == 0 || bitmap >= first || first > pageSize || cursor < first || cursor > pageSize ||
        (!initial && (prefix > keyLength || bitmap + (cursor - first) / 8 >= first)) then 0
    else if (if initial then keyLength else keyLength - prefix) > pageSize - cursor then 0
    else if (if intermediate then 8 else 4) > pageSize - cursor - (if initial then keyLength else keyLength - prefix) then 0
    else cursor + (if initial then keyLength else keyLength - prefix) + (if intermediate then 8 else 4)
  }

  lemma AdvanceEntryExact(pageSize: U32, bitmap: U32, first: U32, cursor: U32,
      keyLength: U32, prefix: U32, initial: bool, intermediate: bool)
    requires pageSize <= 2147483647
    ensures AdvanceEntry(pageSize, bitmap, first, cursor, keyLength, prefix, initial, intermediate) != 0 ==>
      first <= cursor < AdvanceEntry(pageSize, bitmap, first, cursor, keyLength, prefix, initial, intermediate) <= pageSize &&
      AdvanceEntry(pageSize, bitmap, first, cursor, keyLength, prefix, initial, intermediate) as int ==
        cursor as int + keyLength as int - (if initial then 0 else prefix as int) + (if intermediate then 8 else 4)
  {}

  // Complete page-header and prefix emission after the adapter's metadata
  // preflight. Bounds are total and refusal preserves the whole private page.
  method InitializePage(page: array<Byte>, intermediate: bool, parent: U32,
      previous: U32, next: U32, tail: U32, previousOffset: U32, nextOffset: U32,
      tailOffset: U32, prefixOffset: U32, prefix: U32, jet3: bool) returns (ok: bool)
    requires page.Length <= 2147483647
    modifies page
    ensures ok <==> (page.Length >= 8 && HasRange(page.Length as U32, previousOffset, 4) &&
      HasRange(page.Length as U32, nextOffset, 4) && HasRange(page.Length as U32, tailOffset, 4) &&
      HasRange(page.Length as U32, prefixOffset, if jet3 then 1 else 2) && prefix <= (if jet3 then 255 else 65535))
    ensures ok ==> (page.Length >= 8 && previousOffset as int + 4 <= page.Length && nextOffset as int + 4 <= page.Length
      && tailOffset as int + 4 <= page.Length && prefixOffset as int + (if jet3 then 1 else 2) <= page.Length)
    ensures !ok ==> page[..] == old(page[..])
    ensures ok ==> page[..] == Header(old(page[..]), intermediate, parent, previous, next, tail,
      previousOffset as int, nextOffset as int, tailOffset as int, prefixOffset as int, prefix, jet3)
  {
    var length := page.Length as U32;
    if length < 8 || !HasRange(length, previousOffset, 4) || !HasRange(length, nextOffset, 4) ||
        !HasRange(length, tailOffset, 4) || !HasRange(length, prefixOffset, if jet3 then 1 else 2) ||
        prefix > (if jet3 then 255 else 65535) {
      return false;
    }
    page[0 as U32] := if intermediate then 3 else 4;
    page[1 as U32] := 1;
    WriteWord(page, 4, parent);
    WriteWord(page, previousOffset, previous);
    WriteWord(page, nextOffset, next);
    WriteWord(page, tailOffset, tail);
    page[prefixOffset] := (prefix % 256) as Byte;
    if !jet3 { page[prefixOffset + 1] := (prefix / 256 % 256) as Byte; }
    ok := true;
  }

  ghost function Header(bytes: seq<Byte>, intermediate: bool, parent: U32,
      previous: U32, next: U32, tail: U32, previousOffset: nat, nextOffset: nat,
      tailOffset: nat, prefixOffset: nat, prefix: U32, jet3: bool): seq<Byte>
    requires |bytes| >= 8 && previousOffset + 4 <= |bytes| && nextOffset + 4 <= |bytes|
    requires tailOffset + 4 <= |bytes| && prefixOffset + (if jet3 then 1 else 2) <= |bytes|
  {
    var numbered := PutWord(PutWord(PutWord(PutWord(bytes[0 := if intermediate then 3 else 4][1 := 1],
      4, parent), previousOffset, previous), nextOffset, next), tailOffset, tail);
    var low := numbered[prefixOffset := WordByte(prefix, 0)];
    if jet3 then low else low[prefixOffset + 1 := WordByte(prefix, 1)]
  }

  ghost function BitMaskValue(bit: U32): Byte
    requires bit < 8
  { if bit == 0 then 1 else if bit == 1 then 2 else if bit == 2 then 4 else if bit == 3 then 8
    else if bit == 4 then 16 else if bit == 5 then 32 else if bit == 6 then 64 else 128 }

  opaque function BitMask(bit: U32): Byte
    requires bit < 8
    ensures BitMask(bit) == BitMaskValue(bit)
  { ((1 as bv8) << bit) as Byte }

  ghost function Trailer(bytes: seq<Byte>, pointer: nat, dataPage: U32,
      row: Byte, child: U32, intermediate: bool): seq<Byte>
    requires pointer + (if intermediate then 8 else 4) <= |bytes|
  {
    var bookmark := bytes[pointer := WordByte(dataPage, 2)][pointer + 1 := WordByte(dataPage, 1)]
      [pointer + 2 := WordByte(dataPage, 0)][pointer + 3 := row];
    if intermediate then bookmark[pointer + 4 := WordByte(child, 3)][pointer + 5 := WordByte(child, 2)]
      [pointer + 6 := WordByte(child, 1)][pointer + 7 := WordByte(child, 0)] else bookmark
  }

  ghost function EntryMetadata(bytes: seq<Byte>, bitmap: nat, first: nat, cursor: nat,
      suffixLength: nat, dataPage: U32, row: Byte, child: U32, initial: bool, intermediate: bool): seq<Byte>
    requires bitmap < first <= cursor && cursor + suffixLength + (if intermediate then 8 else 4) <= |bytes|
    requires !initial ==> bitmap + (cursor - first) / 8 < first
  {
    var trailed := Trailer(bytes, cursor + suffixLength, dataPage, row, child, intermediate);
    if initial then trailed else
      trailed[bitmap + (cursor - first) / 8 := (trailed[bitmap + (cursor - first) / 8] as bv8 |
        BitMask(((cursor - first) % 8) as U32) as bv8) as Byte]
  }

  // The adapter copies the admitted key suffix with Buffer.BlockCopy. This
  // method emits all remaining entry bytes and its exact boundary bit without
  // allocating or scanning the suffix again.
  method EmitEntryMetadata(page: array<Byte>, bitmap: U32, first: U32, cursor: U32,
      suffixLength: U32, dataPage: U32, row: Byte, child: U32, initial: bool, intermediate: bool)
    requires page.Length <= 2147483647 && dataPage < 16777216
    requires bitmap < first <= cursor && cursor as int <= page.Length
    requires suffixLength as int <= page.Length - cursor as int
    requires (if intermediate then 8 else 4) <= page.Length - cursor as int - suffixLength as int
    requires !initial ==> bitmap + (cursor - first) / 8 < first
    modifies page
    ensures page[..] == EntryMetadata(old(page[..]), bitmap as int, first as int, cursor as int,
      suffixLength as int, dataPage, row, child, initial, intermediate)
    ensures forall i: nat :: cursor as int <= i < cursor as int + suffixLength as int ==> page[i] == old(page[i])
  {
    var pointer := cursor + suffixLength;
    page[pointer] := (dataPage / 65536 % 256) as Byte;
    page[pointer + 1] := (dataPage / 256 % 256) as Byte;
    page[pointer + 2] := (dataPage % 256) as Byte;
    page[pointer + 3] := row;
    if intermediate {
      page[pointer + 4] := (child / 16777216) as Byte;
      page[pointer + 5] := (child / 65536 % 256) as Byte;
      page[pointer + 6] := (child / 256 % 256) as Byte;
      page[pointer + 7] := (child % 256) as Byte;
    }
    if !initial {
      var position := bitmap + (cursor - first) / 8;
      page[position] := (page[position] as bv8 | BitMask((cursor - first) % 8) as bv8) as Byte;
    }
  }

  ghost function Finished(bytes: seq<Byte>, bitmap: nat, first: nat, cursor: nat,
      hasEntries: bool): seq<Byte>
    requires |bytes| >= 4 && bitmap < first <= cursor <= |bytes| && |bytes| - cursor <= 65535
  {
    var marked := if hasEntries && bitmap + (cursor - first) / 8 < first then
      bytes[bitmap + (cursor - first) / 8 := (bytes[bitmap + (cursor - first) / 8] as bv8 |
        BitMask(((cursor - first) % 8) as U32) as bv8) as Byte] else bytes;
    marked[2 := WordByte((|bytes| - cursor) as U32, 0)][3 := WordByte((|bytes| - cursor) as U32, 1)]
  }

  // A full payload may have no representable sentinel; the implicit first
  // entry and empty-page bitmap behavior exactly match the native encoder.
  method FinishPage(page: array<Byte>, bitmap: U32, first: U32, cursor: U32, hasEntries: bool)
    requires 4 <= page.Length <= 2147483647 && bitmap < first <= cursor && cursor as int <= page.Length
    requires page.Length - cursor as int <= 65535
    modifies page
    ensures page[..] == Finished(old(page[..]), bitmap as int, first as int, cursor as int, hasEntries)
  {
    var position := bitmap + (cursor - first) / 8;
    if hasEntries && position < first {
      page[position] := (page[position] as bv8 | BitMask((cursor - first) % 8) as bv8) as Byte;
    }
    var free := (page.Length as U32) - cursor;
    page[2 as U32] := (free % 256) as Byte;
    page[3 as U32] := (free / 256 % 256) as Byte;
  }

  ghost function StoredEntry(key: seq<Byte>, prefix: nat, initial: bool, dataPage: U32,
      row: Byte, child: U32, intermediate: bool): seq<Byte>
    requires prefix <= |key|
  {
    key[(if initial then 0 else prefix)..] +
      [WordByte(dataPage, 2), WordByte(dataPage, 1), WordByte(dataPage, 0), row] +
      (if intermediate then [WordByte(child, 3), WordByte(child, 2), WordByte(child, 1), WordByte(child, 0)] else [])
  }

  // Exact successful BlockCopy effect, declared as an input to composition;
  // this is not an unchecked external declaration or a proof of the BCL.
  ghost function CopySuffix(bytes: seq<Byte>, cursor: nat, suffix: seq<Byte>): seq<Byte>
    requires cursor + |suffix| <= |bytes|
  { bytes[..cursor] + suffix + bytes[cursor + |suffix|..] }

  ghost function EmissionStep(bytes: seq<Byte>, bitmap: nat, first: nat, cursor: nat,
      key: seq<Byte>, prefix: nat, initial: bool, dataPage: U32, row: Byte,
      child: U32, intermediate: bool): seq<Byte>
  {
    if prefix > |key| || bitmap >= first || first > cursor ||
        cursor + |key| - (if initial then 0 else prefix) + (if intermediate then 8 else 4) > |bytes| ||
        (!initial && bitmap + (cursor - first) / 8 >= first) then bytes else
      EntryMetadata(CopySuffix(bytes, cursor, key[(if initial then 0 else prefix)..]), bitmap, first, cursor,
        |key| - (if initial then 0 else prefix), dataPage, row, child, initial, intermediate)
  }
  lemma EmissionStepExact(bytes: seq<Byte>, bitmap: nat, first: nat, cursor: nat,
      key: seq<Byte>, prefix: nat, initial: bool, dataPage: U32, row: Byte,
      child: U32, intermediate: bool)
    requires prefix <= |key| && bitmap < first <= cursor
    requires cursor + |key| - (if initial then 0 else prefix) + (if intermediate then 8 else 4) <= |bytes|
    requires !initial ==> bitmap + (cursor - first) / 8 < first
    ensures |EmissionStep(bytes, bitmap, first, cursor, key, prefix, initial, dataPage, row, child, intermediate)| == |bytes|
    ensures EmissionStep(bytes, bitmap, first, cursor, key, prefix, initial, dataPage, row, child, intermediate)
      [cursor..cursor + |StoredEntry(key, prefix, initial, dataPage, row, child, intermediate)|] ==
        StoredEntry(key, prefix, initial, dataPage, row, child, intermediate)
    ensures EmissionStep(bytes, bitmap, first, cursor, key, prefix, initial, dataPage, row, child, intermediate)[first..cursor] == bytes[first..cursor]
  {
    var after := EmissionStep(bytes, bitmap, first, cursor, key, prefix, initial, dataPage, row, child, intermediate);
    var encoded := StoredEntry(key, prefix, initial, dataPage, row, child, intermediate);
    forall i: nat | i < |encoded|
      ensures after[cursor + i] == encoded[i]
    {}
    assert after[cursor..cursor + |encoded|] == encoded;
  }

  lemma TrailerPreservesPrefix(bytes: seq<Byte>, pointer: nat, dataPage: U32,
      row: Byte, child: U32, intermediate: bool, boundary: nat)
    requires boundary <= pointer && pointer + (if intermediate then 8 else 4) <= |bytes|
    ensures Trailer(bytes, pointer, dataPage, row, child, intermediate)[..boundary] == bytes[..boundary]
  {}

  lemma InitialEmissionPreservesPrefix(bytes: seq<Byte>, bitmap: nat, first: nat, cursor: nat,
      key: seq<Byte>, prefix: nat, dataPage: U32, row: Byte, child: U32, intermediate: bool)
    requires prefix <= |key| && bitmap < first <= cursor
    requires cursor + |key| + (if intermediate then 8 else 4) <= |bytes|
    ensures EmissionStep(bytes, bitmap, first, cursor, key, prefix, true, dataPage, row, child, intermediate)[..cursor] == bytes[..cursor]
  {
    var copied := CopySuffix(bytes, cursor, key);
    assert copied[..cursor] == bytes[..cursor];
    TrailerPreservesPrefix(copied, cursor + |key|, dataPage, row, child, intermediate, cursor);
  }

  lemma EmissionStepBitmapByte(bytes: seq<Byte>, bitmap: nat, first: nat, cursor: nat,
      key: seq<Byte>, prefix: nat, initial: bool, dataPage: U32, row: Byte,
      child: U32, intermediate: bool, location: nat)
    requires prefix <= |key| && bitmap < first <= cursor && location < first
    requires cursor + |key| - (if initial then 0 else prefix) + (if intermediate then 8 else 4) <= |bytes|
    requires !initial ==> bitmap + (cursor - first) / 8 < first
    ensures EmissionStep(bytes, bitmap, first, cursor, key, prefix, initial, dataPage, row, child, intermediate)[location] ==
      (if !initial && location == bitmap + (cursor - first) / 8 then
        (bytes[location] as bv8 | BitMask(((cursor - first) % 8) as U32) as bv8) as Byte else bytes[location])
  {
    var suffix := key[(if initial then 0 else prefix)..];
    var copied := CopySuffix(bytes, cursor, suffix);
    assert copied[..first] == bytes[..first];
    TrailerPreservesPrefix(copied, cursor + |suffix|, dataPage, row, child, intermediate, first);
  }

  ghost function EncodedPayload(keys: seq<seq<Byte>>, pages: seq<U32>, rows: seq<Byte>,
      children: seq<U32>, prefix: nat, intermediate: bool, n: nat): seq<Byte>
    requires n <= |keys| == |pages| == |rows| == |children|
    requires forall i: nat :: i < n ==> prefix <= |keys[i]|
    decreases n
  {
    if n == 0 then [] else (EncodedPayload(keys, pages, rows, children, prefix, intermediate, n - 1) +
      StoredEntry(keys[n - 1], prefix, n == 1, pages[n - 1], rows[n - 1], children[n - 1], intermediate))
  }

  // Arbitrarily many admitted emission steps compose. images retain the exact
  // acquired list and bulk-copy effects. Production's generic C# list loop is
  // manually mapped to these premises; no theorem grants I/O/tree authority.
  lemma {:induction false} EmissionTraceExact(keys: seq<seq<Byte>>, pages: seq<U32>, rows: seq<Byte>,
      children: seq<U32>, prefix: nat, intermediate: bool, bitmap: U32, first: U32,
      cursors: seq<U32>, images: seq<seq<Byte>>, n: nat)
    requires n <= |keys| == |pages| == |rows| == |children|
    requires prefix <= 2147483647
    requires |cursors| == |images| == n + 1 && cursors[0] == first
    requires bitmap < first && forall i: nat :: i <= n ==> cursors[i] as int <= |images[i]| <= 2147483647
    requires forall i: nat :: i < n ==> prefix <= |keys[i]| && |keys[i]| <= 2147483647
    requires forall i: nat {:trigger cursors[i]} :: 0 < i <= n ==>
      cursors[i] == AdvanceEntry(|images[i - 1]| as U32, bitmap, first, cursors[i - 1], |keys[i - 1]| as U32,
        prefix as U32, i == 1, intermediate) && cursors[i] != 0
    requires forall i: nat :: i < n ==> images[i + 1] == EmissionStep(images[i], bitmap as int,
      first as int, cursors[i] as int, keys[i], prefix, i == 0, pages[i], rows[i], children[i], intermediate)
    ensures first <= cursors[n]
    ensures images[n][first as int..cursors[n] as int] == EncodedPayload(keys, pages, rows, children, prefix, intermediate, n)
    ensures n > 0 ==> cursors[n] > first
    decreases n
  {
    hide EmissionStep;
    hide EntryMetadata;
    hide Trailer;
    hide BitMask;
    hide BitMaskValue;
    if n > 0 {
      EmissionTraceExact(keys, pages, rows, children, prefix, intermediate, bitmap, first,
        cursors[..n], images[..n], n - 1);
      assert cursors[n] == AdvanceEntry(|images[n - 1]| as U32, bitmap, first, cursors[n - 1],
        |keys[n - 1]| as U32, prefix as U32, n == 1, intermediate) && cursors[n] != 0;
      AdvanceEntryExact(|images[n - 1]| as U32, bitmap, first, cursors[n - 1], |keys[n - 1]| as U32,
        prefix as U32, n == 1, intermediate);
      assert first <= cursors[n - 1] < cursors[n];
      assert cursors[n] as int == cursors[n - 1] as int + |keys[n - 1]| -
        (if n == 1 then 0 else prefix) + (if intermediate then 8 else 4);
      EmissionStepExact(images[n - 1], bitmap as int, first as int, cursors[n - 1] as int,
        keys[n - 1], prefix, n == 1, pages[n - 1], rows[n - 1], children[n - 1], intermediate);
      assert images[n] == EmissionStep(images[n - 1], bitmap as int, first as int, cursors[n - 1] as int,
        keys[n - 1], prefix, n == 1, pages[n - 1], rows[n - 1], children[n - 1], intermediate);
      assert images[n][first as int..cursors[n - 1] as int] == images[n - 1][first as int..cursors[n - 1] as int];
      assert images[n][cursors[n - 1] as int..cursors[n] as int] ==
        StoredEntry(keys[n - 1], prefix, n == 1, pages[n - 1], rows[n - 1], children[n - 1], intermediate);
      assert images[n][first as int..cursors[n] as int] ==
        images[n][first as int..cursors[n - 1] as int] + images[n][cursors[n - 1] as int..cursors[n] as int];
    }
  }

  lemma SharedPrefixReconstructsKey(firstKey: seq<Byte>, key: seq<Byte>, prefix: nat)
    requires prefix <= |firstKey| && prefix <= |key|
    requires firstKey[..prefix] == key[..prefix]
    ensures firstKey[..prefix] + key[prefix..] == key
  {}

  lemma FinishPreservesPayload(bytes: seq<Byte>, bitmap: nat, first: nat, cursor: nat, hasEntries: bool)
    requires |bytes| >= 4 && 4 <= first <= cursor <= |bytes| && bitmap < first && |bytes| - cursor <= 65535
    ensures |Finished(bytes, bitmap, first, cursor, hasEntries)| == |bytes|
    ensures Finished(bytes, bitmap, first, cursor, hasEntries)[first..cursor] == bytes[first..cursor]
    ensures Finished(bytes, bitmap, first, cursor, hasEntries)[2] == WordByte((|bytes| - cursor) as U32, 0)
    ensures Finished(bytes, bitmap, first, cursor, hasEntries)[3] == WordByte((|bytes| - cursor) as U32, 1)
  {}

  lemma FinishBitmapByte(bytes: seq<Byte>, bitmap: nat, first: nat, cursor: nat, hasEntries: bool, location: nat)
    requires |bytes| >= 4 && bitmap < first <= cursor <= |bytes| && |bytes| - cursor <= 65535
    requires 4 <= location < first
    ensures Finished(bytes, bitmap, first, cursor, hasEntries)[location] ==
      (if hasEntries && bitmap + (cursor - first) / 8 < first && location == bitmap + (cursor - first) / 8 then
        (bytes[location] as bv8 | BitMask(((cursor - first) % 8) as U32) as bv8) as Byte else bytes[location])
  {}

  lemma PutWordPreservesSuffix(bytes: seq<Byte>, start: nat, word: U32, boundary: nat)
    requires start + 4 <= boundary <= |bytes|
    ensures PutWord(bytes, start, word)[boundary..] == bytes[boundary..]
  {}

  lemma HeaderPreservesSuffix(bytes: seq<Byte>, intermediate: bool, parent: U32,
      previous: U32, next: U32, tail: U32, previousOffset: nat, nextOffset: nat,
      tailOffset: nat, prefixOffset: nat, prefix: U32, jet3: bool, boundary: nat)
    requires 8 <= boundary <= |bytes| && previousOffset + 4 <= boundary && nextOffset + 4 <= boundary
    requires tailOffset + 4 <= boundary && prefixOffset + (if jet3 then 1 else 2) <= boundary
    ensures |Header(bytes, intermediate, parent, previous, next, tail, previousOffset,
      nextOffset, tailOffset, prefixOffset, prefix, jet3)| == |bytes|
    ensures Header(bytes, intermediate, parent, previous, next, tail, previousOffset,
      nextOffset, tailOffset, prefixOffset, prefix, jet3)[boundary..] == bytes[boundary..]
  {
    var typed := bytes[0 := if intermediate then 3 else 4][1 := 1];
    PutWordPreservesSuffix(typed, 4, parent, boundary);
    var parented := PutWord(typed, 4, parent);
    PutWordPreservesSuffix(parented, previousOffset, previous, boundary);
    var linkedPrevious := PutWord(parented, previousOffset, previous);
    PutWordPreservesSuffix(linkedPrevious, nextOffset, next, boundary);
    var linkedNext := PutWord(linkedPrevious, nextOffset, next);
    PutWordPreservesSuffix(linkedNext, tailOffset, tail, boundary);
  }

  lemma PointerFieldsExact(page: U32, child: U32)
    requires page < 16777216
    ensures (WordByte(page, 2) as U32) * 65536 + (WordByte(page, 1) as U32) * 256 + WordByte(page, 0) as U32 == page
    ensures (WordByte(child, 3) as U32) * 16777216 + (WordByte(child, 2) as U32) * 65536 +
      (WordByte(child, 1) as U32) * 256 + WordByte(child, 0) as U32 == child
  {}

  lemma ReachableCompleteEmission()
  {
    hide Header;
    hide PutWord;
    hide EmissionStep;
    hide EntryMetadata;
    hide Trailer;
    hide Finished;
    var blank := seq(64, i => 0 as Byte);
    var header := Header(blank, false, 7, 1, 2, 3, 8, 12, 16, 20, 2, false);
    HeaderPreservesSuffix(blank, false, 7, 1, 2, 3, 8, 12, 16, 20, 2, false, 22);
    assert blank[22] == 0 && blank[23] == 0;
    assert header[22..] == blank[22..];
    assert header[22] == 0 && header[23] == 0;
    var keys: seq<seq<Byte>> := [[17, 34, 51], [17, 34, 68]];
    var pages: seq<U32> := [16777215, 0];
    var rows: seq<Byte> := [255, 0];
    var children: seq<U32> := [0, 0];
    var first := EmissionStep(header, 22, 32, 32, keys[0], 2, true, pages[0], rows[0], 0, false);
    EmissionStepExact(header, 22, 32, 32, keys[0], 2, true, pages[0], rows[0], 0, false);
    InitialEmissionPreservesPrefix(header, 22, 32, 32, keys[0], 2, pages[0], rows[0], 0, false);
    assert first[..32] == header[..32];
    assert first[22] == 0 && first[23] == 0;
    var second := EmissionStep(first, 22, 32, 39, keys[1], 2, false, pages[1], rows[1], 0, false);
    EmissionStepExact(first, 22, 32, 39, keys[1], 2, false, pages[1], rows[1], 0, false);
    EmissionStepBitmapByte(first, 22, 32, 39, keys[1], 2, false, pages[1], rows[1], 0, false, 22);
    EmissionStepBitmapByte(first, 22, 32, 39, keys[1], 2, false, pages[1], rows[1], 0, false, 23);
    assert second[22] == 128 && second[23] == 0;
    EmissionTraceExact(keys, pages, rows, children, 2, false, 22, 32, [32, 39, 44], [header, first, second], 2);
    FinishPreservesPayload(second, 22, 32, 44, true);
    FinishBitmapByte(second, 22, 32, 44, true, 22);
    FinishBitmapByte(second, 22, 32, 44, true, 23);
    var finished := Finished(second, 22, 32, 44, true);
    assert finished[32..44] == [17, 34, 51, 255, 255, 255, 255, 68, 0, 0, 0, 0];
    assert finished[2] == 20 && finished[3] == 0;
    assert BitMask(7) == 128 && BitMask(4) == 16;
    assert finished[22] == 128 && finished[23] == 16;
    SharedPrefixReconstructsKey(keys[0], keys[1], 2);
  }

  lemma ReachableEmissionAndRefusal()
    ensures AdvanceEntry(4096, 27, 480, 480, 3, 2, true, false) == 487
    ensures AdvanceEntry(4096, 27, 480, 487, 3, 2, false, false) == 492
    ensures AdvanceEntry(2048, 22, 248, 248, 2048, 0, true, false) == 0
    ensures AdvanceEntry(64, 8, 16, 16, 44, 0, true, false) == 64
    ensures AdvanceEntry(64, 15, 16, 24, 0, 0, false, false) == 0
  {}
}
