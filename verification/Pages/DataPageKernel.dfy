// Complete physical row-reclamation admission and directory rewriting.
// CLR overlapping bulk copy/clear are explicit byte-effect adapters; their
// implementation, page ownership and surrounding statement rollback are not proved.
module DataPageNativeTypes {
  newtype {:nativeType "uint"} U32 = x: int | 0 <= x < 4294967296
  newtype {:nativeType "byte"} Byte = x: int | 0 <= x < 256
}

module DataPageKernel {
  import opened DataPageNativeTypes

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

  function ReclaimedWord(raw: U32, selected: bool, first: U32, start: U32, end: U32): U32
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

  // This complete generated loop writes exactly the directory words and free
  // header. It neither scans nor touches the moved payload or pooled suffix.
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
    ghost var before := page[..];
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
      var raw := ReadWord(page, position);
      var updated := ReclaimedWord(raw, index == rowIndex, first, start, end);
      page[position] := (updated % 256) as Byte;
      page[position + 1] := (updated / 256) as Byte;
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
}
