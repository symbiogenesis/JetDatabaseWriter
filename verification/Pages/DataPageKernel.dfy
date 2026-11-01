// Complete physical row-reclamation admission and directory rewriting.
// CLR overlapping bulk copy/clear are explicit byte-effect adapters; their
// implementation, page ownership and surrounding statement rollback are not proved.
module DataPageNativeTypes {
  newtype {:nativeType "uint"} U32 = x: int | 0 <= x < 4294967296
  newtype {:nativeType "byte"} Byte = x: int | 0 <= x < 256
}

module DataPageKernel {
  import opened DataPageNativeTypes

  // Admission is immediate: callers never wait for capacity while holding
  // existing leases. The adapter atomically installs the returned count.
  method TryReserveCount(current: U32, limit: U32) returns (admitted: bool, next: U32)
    ensures admitted <==> current < limit
    ensures admitted ==> next as int == current as int + 1 && next <= limit
    ensures !admitted ==> next == current
  {
    admitted := current < limit;
    next := current;
    if admitted { next := current + 1; }
  }

  ghost function Word(bytes: seq<Byte>, position: int): U32
    requires 0 <= position && position + 2 <= |bytes|
  { bytes[position] as U32 + 256 * (bytes[position + 1] as U32) }

  function ReadWord(page: array<Byte>, position: U32): U32
    requires page.Length <= 2147483647
    requires position as int + 2 <= page.Length
    reads page
  { page[position] as U32 + 256 * (page[position + 1] as U32) }

  ghost function Slot(bytes: seq<Byte>, rowsStart: U32, index: int): U32
    requires 0 <= index && rowsStart as int + index * 2 + 2 <= |bytes|
  { Word(bytes, rowsStart as int + index * 2) % 8192 }

  ghost predicate Layout(length: int, pageSize: U32, countOffset: U32, rowsStart: U32) {
    4 <= countOffset < rowsStart <= pageSize <= 8191 &&
      countOffset as int + 2 <= rowsStart as int && pageSize as int <= length <= 2147483647
  }

  ghost predicate Reclaimable(bytes: seq<Byte>, pageSize: U32,
      countOffset: U32, rowsStart: U32, rowIndex: U32) {
    Layout(|bytes|, pageSize, countOffset, rowsStart) &&
      (var rows := Word(bytes, countOffset as int);
       0 < rows <= 255 && rowIndex < rows &&
       rowsStart as int + rows as int * 2 <= pageSize as int &&
       rowsStart as int + rows as int * 2 <= Slot(bytes, rowsStart, rowIndex as int) as int < pageSize as int &&
       forall index | 0 <= index < rows as int ::
         Slot(bytes, rowsStart, index) == 0 ||
           rowsStart as int + rows as int * 2 <= Slot(bytes, rowsStart, index) as int)
  }

  // Zero rows is the sole refusal result. Admission scans the same acquired
  // directory once, selecting both the first payload and next distinct bound.
  method PrepareReclaim(page: array<Byte>, pageSize: U32, countOffset: U32,
      rowsStart: U32, rowIndex: U32) returns (rows: U32, first: U32, start: U32, end: U32)
    requires page.Length <= 2147483647
    ensures rows != 0 <==> Reclaimable(page[..], pageSize, countOffset, rowsStart, rowIndex)
    ensures rows != 0 ==>
      rows == Word(page[..], countOffset as int) && rows <= 255 && rowIndex < rows &&
      4 <= rowsStart && rowsStart as int + rows as int * 2 <= first as int && first <= start < end <= pageSize <= 8191 &&
      start == Slot(page[..], rowsStart, rowIndex as int) &&
      (forall index | 0 <= index < rows as int ::
        Slot(page[..], rowsStart, index) == 0 || first <= Slot(page[..], rowsStart, index)) &&
      (exists index | 0 <= index < rows as int :: first == Slot(page[..], rowsStart, index)) &&
      (forall index | 0 <= index < rows as int && start < Slot(page[..], rowsStart, index) ::
        end <= Slot(page[..], rowsStart, index)) &&
      (end < pageSize ==> exists index | 0 <= index < rows as int :: end == Slot(page[..], rowsStart, index))
    ensures rows == 0 ==> first == 0 && start == 0 && end == 0
  {
    rows, first, start, end := 0, 0, 0, 0;
    if pageSize > 8191 || pageSize > page.Length as U32 || countOffset < 4 ||
        countOffset >= rowsStart || rowsStart > pageSize || rowsStart - countOffset < 2 {
      return;
    }
    var count := ReadWord(page, countOffset);
    if count == 0 || count > 255 || rowIndex >= count || count > (pageSize - rowsStart) / 2 {
      return;
    }
    var selected := ReadWord(page, rowsStart + rowIndex * 2) % 8192;
    assert selected == Slot(page[..], rowsStart, rowIndex as int);
    var directoryEnd := rowsStart + count * 2;
    if selected < directoryEnd || selected >= pageSize { return; }
    var minimum := pageSize;
    var next := pageSize;
    var index: U32 := 0;
    while index < count
      invariant index <= count <= 255
      invariant directoryEnd <= selected < pageSize <= 8191
      invariant 0 < minimum <= pageSize && selected < next <= pageSize
      invariant forall i | 0 <= i < index as int ::
        Slot(page[..], rowsStart, i) == 0 || minimum <= Slot(page[..], rowsStart, i)
      invariant minimum < pageSize ==> exists i | 0 <= i < index as int ::
        minimum == Slot(page[..], rowsStart, i)
      invariant forall i | 0 <= i < index as int && selected < Slot(page[..], rowsStart, i) ::
        next <= Slot(page[..], rowsStart, i)
      invariant next < pageSize ==> exists i | 0 <= i < index as int ::
        next == Slot(page[..], rowsStart, i)
      invariant forall i | 0 <= i < index as int ::
        Slot(page[..], rowsStart, i) == 0 || directoryEnd <= Slot(page[..], rowsStart, i)
      decreases count - index
    {
      var offset := ReadWord(page, rowsStart + index * 2) % 8192;
      assert offset == Slot(page[..], rowsStart, index as int);
      if offset > 0 && offset < directoryEnd { return; }
      if offset > 0 && offset < minimum { minimum := offset; }
      if offset > selected && offset < next { next := offset; }
      index := index + 1;
    }
    assert minimum <= selected;
    rows, first, start, end := count, minimum, selected, next;
  }

  ghost function ReclaimedWord(raw: U32, selected: bool, first: U32, start: U32, end: U32): U32
    requires raw < 65536 && first <= start < end <= 8191
    ensures ReclaimedWord(raw, selected, first, start, end) < 65536
  {
    if selected then 49152 + end
    else if first <= raw % 8192 <= start then raw / 8192 * 8192 + (raw % 8192 + end - start)
    else raw
  }

  lemma ReclaimedWordPreservesSurvivingFlags(raw: U32, first: U32, start: U32, end: U32)
    requires raw < 65536 && first <= start < end <= 8191
    ensures ReclaimedWord(raw, false, first, start, end) / 8192 == raw / 8192
    ensures ReclaimedWord(raw, false, first, start, end) % 8192 ==
      (if first <= raw % 8192 <= start then raw % 8192 + end - start else raw % 8192)
  { }

  // Native word masks retain the exact arithmetic slot/flag representation.
  lemma NativeWordMasks(raw: U32, offset: U32)
    requires raw < 65536 && offset < 8192
    ensures (raw as bv16 & 8191) as U32 == raw % 8192
    ensures ((raw as bv16 & 57344) | offset as bv16) as U32 == raw / 8192 * 8192 + offset
  { }

  lemma WordBytesInverse(word: U32)
    requires word < 65536
    ensures word % 256 + 256 * (word / 256 % 256) == word
  { }

  ghost predicate DirectoryRewrite(before: seq<Byte>, after: seq<Byte>,
      rowsStart: U32, rowIndex: U32, rows: U32, first: U32, start: U32, end: U32)
    requires 4 <= rowsStart && 0 < rows <= 255 && rowIndex < rows
    requires rowsStart as int + rows as int * 2 <= first as int && first <= start < end <= 8191
    requires end as int <= |before|
  {
    |after| == |before| && Word(after, 2) == first + end - start - rowsStart - rows * 2 &&
      (forall index | 0 <= index < rows as int ::
        Word(after, rowsStart as int + index * 2) ==
          ReclaimedWord(Word(before, rowsStart as int + index * 2), index == rowIndex as int, first, start, end)) &&
      (forall position | 0 <= position < |before| && position != 2 && position != 3 &&
        !(rowsStart as int <= position < rowsStart as int + rows as int * 2) :: after[position] == before[position])
  }

  // This complete generated loop writes only selected/shifted directory words
  // and the free header, retaining the bulk effects' moved payload and suffix.
  method RewriteReclaim(page: array<Byte>, rowsStart: U32, rowIndex: U32,
      rows: U32, first: U32, start: U32, end: U32)
    requires page.Length <= 2147483647
    requires 4 <= rowsStart && 0 < rows <= 255 && rowIndex < rows
    requires rowsStart as int + rows as int * 2 <= first as int && first <= start < end <= 8191
    requires end as int <= page.Length
    modifies page
    ensures DirectoryRewrite(old(page[..]), page[..], rowsStart, rowIndex, rows, first, start, end)
    ensures Word(page[..], 2) == first + end - start - rowsStart - rows * 2
    ensures forall index | 0 <= index < rows as int ::
      Word(page[..], rowsStart as int + index * 2) ==
        ReclaimedWord(Word(old(page[..]), rowsStart as int + index * 2), index == rowIndex as int, first, start, end)
    ensures forall position | 0 <= position < page.Length && position != 2 && position != 3 &&
        !(rowsStart as int <= position < rowsStart as int + rows as int * 2) :: page[position] == old(page[position])
  {
    hide ReclaimedWord;
    ghost var before := page[..];
    var width := end - start;
    var index: U32 := 0;
    while index < rows
      invariant index <= rows
      invariant forall position | 0 <= position < rowsStart as int :: page[position] == before[position]
      invariant forall position | rowsStart as int + index as int * 2 <= position < page.Length ::
        page[position] == before[position]
      invariant forall i | 0 <= i < index as int ::
        Word(page[..], rowsStart as int + i * 2) ==
          ReclaimedWord(Word(before, rowsStart as int + i * 2), i == rowIndex as int, first, start, end)
      decreases rows - index
    {
      var position := rowsStart + index * 2;
      ghost var iteration := page[..];
      if index == rowIndex {
        var updated := 49152 + end;
        assert {:isolate} updated == ReclaimedWord(Word(iteration, position as int), true, first, start, end) by {
          reveal ReclaimedWord;
        }
        page[position] := (updated % 256) as Byte;
        page[position + 1] := (updated / 256 % 256) as Byte;
        assert {:isolate} Word(page[..], position as int) == updated by { WordBytesInverse(updated); }
      } else {
        var raw := ReadWord(page, position);
        var offset := (raw as bv16 & 8191) as U32;
        assert {:isolate} offset == raw % 8192 by { NativeWordMasks(raw, 0); }
        if first <= offset <= start {
          var shifted := offset + width;
          var updated := ((raw as bv16 & 57344) | shifted as bv16) as U32;
          assert {:isolate} updated == ReclaimedWord(raw, false, first, start, end) by {
            reveal ReclaimedWord;
            NativeWordMasks(raw, shifted);
          }
          page[position] := (updated % 256) as Byte;
          page[position + 1] := (updated / 256 % 256) as Byte;
          assert {:isolate} Word(page[..], position as int) == updated by { WordBytesInverse(updated); }
        } else {
          assert {:isolate} ReclaimedWord(raw, false, first, start, end) == raw by {
            reveal ReclaimedWord;
          }
        }
      }
      assert forall untouched | 0 <= untouched < page.Length &&
        untouched != position as int && untouched != position as int + 1 :: page[untouched] == iteration[untouched];
      index := index + 1;
    }
    var free := first + end - start - rowsStart - rows * 2;
    page[2 as U32] := (free % 256) as Byte;
    page[3 as U32] := (free / 256) as Byte;
  }

  ghost function MovedPayload(before: seq<Byte>, first: U32, start: U32,
      end: U32, erase: bool): seq<Byte>
    requires first <= start < end && end as int <= |before|
  {
    seq(|before|, position =>
      if first as int + end as int - start as int <= position < end as int
        then before[position - (end as int - start as int)]
      else if erase && first as int <= position < first as int + end as int - start as int then 0 as Byte
      else if 0 <= position < |before| then before[position] else 0 as Byte)
  }

  // Exact contracts of overlapping BlockCopy followed by optional Clear.
  // This is a theorem about byte images, not an external declaration about CLR.
  lemma BulkEffectsPreserveSurvivingPayloads(before: seq<Byte>, first: U32,
      start: U32, end: U32, erase: bool)
    requires first <= start < end && end as int <= |before|
    ensures forall position | first as int <= position < start as int ::
      MovedPayload(before, first, start, end, erase)[position + end as int - start as int] == before[position]
    ensures forall position | end as int <= position < |before| ::
      MovedPayload(before, first, start, end, erase)[position] == before[position]
    ensures erase ==> forall position | first as int <= position < first as int + end as int - start as int ::
      MovedPayload(before, first, start, end, erase)[position] == 0
    ensures forall position | 0 <= position < first as int ::
      MovedPayload(before, first, start, end, erase)[position] == before[position]
  { }

  // Compose the exact CLR-effect image with the generated rewrite contract.
  // Every surviving physical interval wholly below the selected row moves by
  // its removed width; intervals above it stay byte-identical. Duplicate slots
  // at the removed start retain the existing tombstone/alias policy and are
  // deliberately not asserted to retain the removed payload.
  lemma ReclaimEffectsCompose(before: seq<Byte>, after: seq<Byte>, pageSize: U32,
      countOffset: U32, rowsStart: U32, rowIndex: U32, rows: U32,
      first: U32, start: U32, end: U32, erase: bool)
    requires Reclaimable(before, pageSize, countOffset, rowsStart, rowIndex)
    requires rows == Word(before, countOffset as int)
    requires rowsStart as int + rows as int * 2 <= first as int && first <= start < end <= pageSize
    requires DirectoryRewrite(MovedPayload(before, first, start, end, erase), after,
      rowsStart, rowIndex, rows, first, start, end)
    ensures |after| == |before|
    ensures Word(after, countOffset as int) == rows
    ensures Word(after, 2) == first + end - start - rowsStart - rows * 2
    ensures forall index | 0 <= index < rows as int ::
      Word(after, rowsStart as int + index * 2) ==
        ReclaimedWord(Word(before, rowsStart as int + index * 2), index == rowIndex as int, first, start, end)
    ensures forall index | 0 <= index < rows as int && index != rowIndex as int ::
      Word(after, rowsStart as int + index * 2) / 8192 == Word(before, rowsStart as int + index * 2) / 8192
    ensures forall position | first as int <= position < start as int ::
      after[position + end as int - start as int] == before[position]
    ensures forall position | end as int <= position < |before| :: after[position] == before[position]
    ensures erase ==> forall position | first as int <= position < first as int + end as int - start as int ::
      after[position] == 0
    ensures forall position | 0 <= position < first as int && position != 2 && position != 3 &&
        !(rowsStart as int <= position < rowsStart as int + rows as int * 2) :: after[position] == before[position]
  {
    BulkEffectsPreserveSurvivingPayloads(before, first, start, end, erase);
    var moved := MovedPayload(before, first, start, end, erase);
    assert moved[..first as int] == before[..first as int];
    forall index | 0 <= index < rows as int && index != rowIndex as int
      ensures Word(after, rowsStart as int + index * 2) / 8192 == Word(before, rowsStart as int + index * 2) / 8192
    {
      ReclaimedWordPreservesSurvivingFlags(Word(before, rowsStart as int + index * 2), first, start, end);
    }
  }

  lemma SupportedNonemptyReclaimIsReachable()
    ensures Reclaimable([0, 0, 0, 0, 1, 0, 8, 128, 7, 9], 10, 4, 6, 0)
    ensures !Reclaimable([0, 0, 0, 0, 1, 0, 5, 128, 7, 9], 10, 4, 6, 0)
  { }

  function ColumnMask(bit: U32): U32 {
    if bit == 0 then 1 else if bit == 1 then 2 else if bit == 2 then 4 else if bit == 3 then 8
    else if bit == 4 then 16 else if bit == 5 then 32 else if bit == 6 then 64 else 128
  }

  opaque function MergedColumnBit(before: Byte, source: Byte, bit: U32): Byte {
    var mask := ColumnMask(bit);
    (before as U32 / (mask * 2) * (mask * 2) +
      ((source as U32 / mask) % 2) * mask + before as U32 % mask) as Byte
  }

  lemma ColumnBitMergePreservesOtherBits(before: Byte, source: Byte, bit: U32)
    ensures (MergedColumnBit(before, source, bit) as U32 / ColumnMask(bit)) % 2 ==
      (source as U32 / ColumnMask(bit)) % 2
    ensures MergedColumnBit(before, source, bit) as U32 % ColumnMask(bit) == before as U32 % ColumnMask(bit)
    ensures MergedColumnBit(before, source, bit) as U32 / (ColumnMask(bit) * 2) == before as U32 / (ColumnMask(bit) * 2)
  {
    reveal MergedColumnBit();
  }

  function HasColumnPatchRange(targetLength: U32, sourceLength: U32,
      targetOffset: U32, sourceOffset: U32, count: U32,
      targetDataEnd: U32, sourceDataEnd: U32, targetMask: U32,
      sourceMask: U32, bit: U32): bool {
    targetMask < targetLength && sourceMask < sourceLength &&
      targetDataEnd <= targetMask && sourceDataEnd <= sourceMask &&
      targetOffset <= targetDataEnd && count <= targetDataEnd - targetOffset &&
      sourceOffset <= sourceDataEnd && count <= sourceDataEnd - sourceOffset && bit < 8
  }

  // A field payload cannot overlap trailer metadata. A Boolean field uses
  // count zero and still updates exactly one selected mask bit atomically.
  method PatchColumn(row: array<Byte>, source: array<Byte>,
      targetOffset: U32, sourceOffset: U32, count: U32,
      targetDataEnd: U32, sourceDataEnd: U32, targetMask: U32,
      sourceMask: U32, bit: U32) returns (applied: bool)
    requires row.Length <= 2147483647 && source.Length <= 2147483647
    modifies row
    ensures applied <==> row != source && HasColumnPatchRange(row.Length as U32,
      source.Length as U32, targetOffset, sourceOffset, count, targetDataEnd,
      sourceDataEnd, targetMask, sourceMask, bit)
    ensures !applied ==> row[..] == old(row[..])
    ensures applied ==> (row[targetOffset as int..targetOffset as int + count as int] ==
      old(source[sourceOffset as int..sourceOffset as int + count as int]))
    ensures applied ==> row[targetMask] == MergedColumnBit(old(row[targetMask]), old(source[sourceMask]), bit)
    ensures applied ==> row[targetMask] as U32 % ColumnMask(bit) == old(row[targetMask]) as U32 % ColumnMask(bit)
    ensures applied ==> row[targetMask] as U32 / (ColumnMask(bit) * 2) == old(row[targetMask]) as U32 / (ColumnMask(bit) * 2)
    ensures applied ==> (row[targetMask] as U32 / ColumnMask(bit)) % 2 == (old(source[sourceMask]) as U32 / ColumnMask(bit)) % 2
    ensures applied ==> forall position | 0 <= position < row.Length &&
      !(targetOffset as int <= position < targetOffset as int + count as int) && position != targetMask as int ::
        row[position] == old(row[position])
  {
    applied := false;
    if row == source || !HasColumnPatchRange(row.Length as U32, source.Length as U32,
        targetOffset, sourceOffset, count, targetDataEnd, sourceDataEnd,
        targetMask, sourceMask, bit) {
      return;
    }
    ghost var before := row[..];
    var oldMask := row[targetMask];
    var nextMask := source[sourceMask];
    var cursor: U32 := 0;
    while cursor < count
      invariant cursor <= count
      invariant row[targetMask] == oldMask
      invariant row[targetOffset as int..targetOffset as int + cursor as int] ==
        source[sourceOffset as int..sourceOffset as int + cursor as int]
      invariant forall position | 0 <= position < row.Length &&
        !(targetOffset as int <= position < targetOffset as int + cursor as int) :: row[position] == before[position]
      decreases count - cursor
    {
      row[targetOffset + cursor] := source[sourceOffset + cursor];
      cursor := cursor + 1;
    }
    ColumnBitMergePreservesOtherBits(oldMask, nextMask, bit);
    row[targetMask] := MergedColumnBit(oldMask, nextMask, bit);
    applied := true;
  }

  // Same-width publication retains the complete native slot directory and
  // every byte outside the one independently owned live physical row.
  method ReplaceRow(page: array<Byte>, pageSize: U32, countOffset: U32,
      rowsStart: U32, rowIndex: U32, expectedStart: U32,
      replacement: array<Byte>) returns (applied: bool)
    requires page.Length <= 2147483647 && replacement.Length <= 2147483647
    modifies page
    ensures !applied ==> page[..] == old(page[..])
    ensures applied ==> expectedStart as int + replacement.Length <= page.Length
    ensures applied ==> page[expectedStart as int..expectedStart as int + replacement.Length] == old(replacement[..])
    ensures applied ==> forall position | 0 <= position < page.Length &&
      !(expectedStart as int <= position < expectedStart as int + replacement.Length) ::
        page[position] == old(page[position])
    ensures applied ==> Reclaimable(old(page[..]), pageSize, countOffset, rowsStart, rowIndex)
    ensures applied ==> Word(old(page[..]), rowsStart as int + rowIndex as int * 2) == expectedStart
    ensures applied ==> forall index | 0 <= index < Word(old(page[..]), countOffset as int) as int && index != rowIndex as int ::
      Slot(old(page[..]), rowsStart, index) != expectedStart
  {
    applied := false;
    if page == replacement { return; }
    ghost var original := page[..];
    var rows, first, start, end := PrepareReclaim(page, pageSize, countOffset, rowsStart, rowIndex);
    if rows == 0 || start != expectedStart || end - start != replacement.Length as U32 ||
        ReadWord(page, rowsStart + rowIndex * 2) != start {
      return;
    }
    var index: U32 := 0;
    while index < rows
      invariant index <= rows
      invariant page[..] == original
      invariant Reclaimable(original, pageSize, countOffset, rowsStart, rowIndex)
      invariant rows == Word(original, countOffset as int)
      invariant rowsStart as int + rows as int * 2 <= start as int && start < end <= pageSize && pageSize as int <= page.Length
      invariant start == expectedStart && end - start == replacement.Length as U32
      invariant forall checked | 0 <= checked < index as int && checked != rowIndex as int ::
        Slot(original, rowsStart, checked) != start
      decreases rows - index
    {
      if index != rowIndex && ReadWord(page, rowsStart + index * 2) % 8192 == start {
        return;
      }
      index := index + 1;
    }
    var cursor: U32 := 0;
    while cursor < replacement.Length as U32
      invariant cursor <= replacement.Length as U32
      invariant start == expectedStart && start as int + replacement.Length <= page.Length
      invariant page[start as int..start as int + cursor as int] == replacement[..cursor as int]
      invariant forall position | 0 <= position < page.Length &&
        !(start as int <= position < start as int + cursor as int) :: page[position] == original[position]
      decreases replacement.Length as U32 - cursor
    {
      page[start + cursor] := replacement[cursor];
      cursor := cursor + 1;
    }
    applied := true;
  }
}
