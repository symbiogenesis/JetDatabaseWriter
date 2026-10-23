// Executable recovery decisions. Generated C# calls these total native-width
// functions; ghost lemmas connect byte authorization to UndoJournal.Allowed.
// Record-byte decoding, hashes, evidence identity, I/O and loop correspondence remain
// outside this kernel. No external C# contract is trusted by these proofs.
module NativeRecoveryTypes {
  newtype {:nativeType "ulong"} U64 = x: int | 0 <= x < 18446744073709551616
  newtype {:nativeType "uint"} U32 = x: int | 0 <= x < 4294967296
  newtype {:nativeType "byte"} Byte = x: int | 0 <= x < 256
}

module RecoveryKernel {
  import opened NativeRecoveryTypes
  import Undo = UndoJournal

  function IsPageSize(pageSize: U32): bool {
    pageSize == 2048 || pageSize == 4096
  }

  function IsDatabaseLength(length: U64): bool {
    length <= 2147483648
  }

  // Zero is an invalid-input sentinel; even an empty database has a complete
  // 128-byte header. The manifest stores one 32-byte hash per rounded-up page.
  function JournalRecordStart(pageSize: U32, originalLength: U64): U64 {
    if !IsPageSize(pageSize) || !IsDatabaseLength(originalLength) then 0
    else 128 + ((originalLength + (pageSize as U64) - 1) / (pageSize as U64)) * 32
  }

  // A trailing fragment supplies no complete record. No hashing or record-byte
  // decoding is performed here; this is only the bounded framing arithmetic.
  function CompleteJournalEnd(pageSize: U32, originalLength: U64, journalLength: U64): U64 {
    var start := JournalRecordStart(pageSize, originalLength);
    if start == 0 || journalLength < start || journalLength > 17179869184 then 0
    else journalLength - ((journalLength - start) % ((pageSize as U64) + 40))
  }

  lemma JournalManifestIsExactBoundedPageRounding(pageSize: U32, originalLength: U64)
    ensures JournalRecordStart(pageSize, originalLength) == 0 <==>
      !IsPageSize(pageSize) || !IsDatabaseLength(originalLength)
    ensures JournalRecordStart(pageSize, originalLength) != 0 ==>
      128 <= JournalRecordStart(pageSize, originalLength) <= 33554560
    ensures JournalRecordStart(pageSize, originalLength) != 0 ==>
      (JournalRecordStart(pageSize, originalLength) as int - 128) % 32 == 0
    ensures JournalRecordStart(pageSize, originalLength) != 0 ==>
      ((JournalRecordStart(pageSize, originalLength) as int - 128) / 32) * pageSize as int >= originalLength as int
    ensures JournalRecordStart(pageSize, originalLength) != 0 && originalLength > 0 ==>
      (((JournalRecordStart(pageSize, originalLength) as int - 128) / 32) - 1) * pageSize as int < originalLength as int
  {
    if IsPageSize(pageSize) && IsDatabaseLength(originalLength) {
      if pageSize == 2048 { } else { assert pageSize == 4096; }
    }
  }

  lemma JournalFramingSelectsExactCompletePrefix(pageSize: U32, originalLength: U64, journalLength: U64)
    ensures CompleteJournalEnd(pageSize, originalLength, journalLength) == 0 <==>
      JournalRecordStart(pageSize, originalLength) == 0 ||
      journalLength < JournalRecordStart(pageSize, originalLength) || journalLength > 17179869184
    ensures CompleteJournalEnd(pageSize, originalLength, journalLength) != 0 ==>
      128 <= JournalRecordStart(pageSize, originalLength) <=
      CompleteJournalEnd(pageSize, originalLength, journalLength) <= journalLength <= 17179869184
    ensures CompleteJournalEnd(pageSize, originalLength, journalLength) != 0 ==>
      CompleteJournalEnd(pageSize, originalLength, journalLength) as int -
        JournalRecordStart(pageSize, originalLength) as int ==
      ((journalLength as int - JournalRecordStart(pageSize, originalLength) as int) /
        (pageSize as int + 40)) * (pageSize as int + 40)
    ensures CompleteJournalEnd(pageSize, originalLength, journalLength) != 0 ==>
      0 <= journalLength as int - CompleteJournalEnd(pageSize, originalLength, journalLength) as int < pageSize as int + 40
  {
    JournalManifestIsExactBoundedPageRounding(pageSize, originalLength);
    var start := JournalRecordStart(pageSize, originalLength) as int;
    if start != 0 && start <= journalLength as int <= 17179869184 {
      var size := pageSize as int + 40;
      var length := journalLength as int - start;
      assert length == (length / size) * size + length % size;
    }
  }

  lemma JournalCompletePrefixIsMaximal(pageSize: U32, originalLength: U64,
      journalLength: U64, candidate: int)
    requires CompleteJournalEnd(pageSize, originalLength, journalLength) != 0
    requires JournalRecordStart(pageSize, originalLength) as int <= candidate <= journalLength as int
    requires (candidate - JournalRecordStart(pageSize, originalLength) as int) % (pageSize as int + 40) == 0
    ensures candidate <= CompleteJournalEnd(pageSize, originalLength, journalLength) as int
  {
    JournalFramingSelectsExactCompletePrefix(pageSize, originalLength, journalLength);
    var start := JournalRecordStart(pageSize, originalLength) as int;
    var size := pageSize as int + 40;
    var length := journalLength as int - start;
    assert (candidate - start) / size <= length / size;
    assert candidate - start == ((candidate - start) / size) * size;
  }

  function IsPageOffset(offset: U64, pageSize: U32): bool {
    IsPageSize(pageSize) &&
    offset <= 2147483648 - (pageSize as U64) &&
    offset % (pageSize as U64) == 0
  }

  // Zero refuses an invalid original page. A valid partial final page copies
  // only its original prefix, never the serialized record's zero padding.
  function OriginalPageLength(pageSize: U32, originalLength: U64, offset: U64): U32 {
    if !IsPageOffset(offset, pageSize) || !IsDatabaseLength(originalLength) || offset >= originalLength then 0
    else if originalLength - offset >= pageSize as U64 then pageSize
    else (originalLength - offset) as U32
  }

  // The selected hash is wholly inside the prepared manifest. Zero is outside
  // that manifest and is reserved for invalid-input refusal.
  function OriginalPageHashOffset(pageSize: U32, originalLength: U64, offset: U64): U64 {
    if OriginalPageLength(pageSize, originalLength, offset) == 0 then 0
    else 128 + (offset / (pageSize as U64)) * 32
  }

  // Select only the payload of a complete aligned record. A trailing fragment
  // cannot supply payload bytes, even when it contains a complete tag.
  function JournalRecordPayloadOffset(pageSize: U32, originalLength: U64,
      journalLength: U64, position: U64): U64 {
    var start := JournalRecordStart(pageSize, originalLength);
    var end := CompleteJournalEnd(pageSize, originalLength, journalLength);
    var size := (pageSize as U64) + 40;
    if end == 0 || position < start || position > end || end - position < size ||
        (position - start) % size != 0 then 0
    else position + 8
  }

  lemma OriginalPageSliceIsExact(pageSize: U32, originalLength: U64, offset: U64)
    ensures OriginalPageLength(pageSize, originalLength, offset) == 0 <==>
      !IsPageOffset(offset, pageSize) || !IsDatabaseLength(originalLength) || offset >= originalLength
    ensures OriginalPageLength(pageSize, originalLength, offset) != 0 ==>
      0 < OriginalPageLength(pageSize, originalLength, offset) <= pageSize
    ensures OriginalPageLength(pageSize, originalLength, offset) != 0 ==>
      offset as int + OriginalPageLength(pageSize, originalLength, offset) as int ==
        (if offset as int + pageSize as int <= originalLength as int
          then offset as int + pageSize as int else originalLength as int)
  { }

  lemma OriginalPageHashSliceIsBounded(pageSize: U32, originalLength: U64, offset: U64)
    ensures OriginalPageHashOffset(pageSize, originalLength, offset) == 0 <==>
      OriginalPageLength(pageSize, originalLength, offset) == 0
    ensures OriginalPageHashOffset(pageSize, originalLength, offset) != 0 ==>
      128 <= OriginalPageHashOffset(pageSize, originalLength, offset) as int &&
      OriginalPageHashOffset(pageSize, originalLength, offset) as int + 32 <=
        JournalRecordStart(pageSize, originalLength) as int
    ensures OriginalPageHashOffset(pageSize, originalLength, offset) != 0 ==>
      (OriginalPageHashOffset(pageSize, originalLength, offset) as int - 128) / 32 ==
        offset as int / pageSize as int
  {
    OriginalPageSliceIsExact(pageSize, originalLength, offset);
    JournalManifestIsExactBoundedPageRounding(pageSize, originalLength);
    if OriginalPageLength(pageSize, originalLength, offset) != 0 {
      if pageSize == 2048 { } else { assert pageSize == 4096; }
    }
  }

  lemma JournalPayloadSliceIsExactAndBounded(pageSize: U32, originalLength: U64,
      journalLength: U64, position: U64)
    ensures JournalRecordPayloadOffset(pageSize, originalLength, journalLength, position) != 0 <==>
      CompleteJournalEnd(pageSize, originalLength, journalLength) != 0 &&
      JournalRecordStart(pageSize, originalLength) as int <= position as int &&
      position as int + pageSize as int + 40 <= CompleteJournalEnd(pageSize, originalLength, journalLength) as int &&
      (position as int - JournalRecordStart(pageSize, originalLength) as int) % (pageSize as int + 40) == 0
    ensures JournalRecordPayloadOffset(pageSize, originalLength, journalLength, position) != 0 ==>
      JournalRecordPayloadOffset(pageSize, originalLength, journalLength, position) as int == position as int + 8
    ensures JournalRecordPayloadOffset(pageSize, originalLength, journalLength, position) != 0 ==>
      136 <= JournalRecordPayloadOffset(pageSize, originalLength, journalLength, position) as int &&
      JournalRecordPayloadOffset(pageSize, originalLength, journalLength, position) as int + pageSize as int + 32 <=
        CompleteJournalEnd(pageSize, originalLength, journalLength) as int <= journalLength as int <= 17179869184
  {
    JournalFramingSelectsExactCompletePrefix(pageSize, originalLength, journalLength);
  }

  // The parser's validated before-image map supplies the association between
  // this page and its complete record. Under the existing exact-payload model,
  // the generated count extracts exactly original bytes and excludes padding.
  lemma OriginalPayloadExtractionRefinesCapturedPage(original: seq<Undo.Byte>,
      pageSize: U32, offset: U64, page: Undo.PageCapture)
    requires IsPageSize(pageSize) && |original| <= 2147483648
    requires page.pageOffset == offset as int
    requires Undo.ExactPagePayload(original, pageSize as int, page)
    ensures 0 < OriginalPageLength(pageSize, |original| as U64, offset) <= pageSize
    ensures OriginalPageLength(pageSize, |original| as U64, offset) as int <= |page.pageBytes|
    ensures offset as int + OriginalPageLength(pageSize, |original| as U64, offset) as int <= |original|
    ensures page.pageBytes[..OriginalPageLength(pageSize, |original| as U64, offset) as int] ==
      original[offset as int..offset as int + OriginalPageLength(pageSize, |original| as U64, offset) as int]
  {
    reveal Undo.ExactPagePayload();
    if pageSize == 2048 { } else { assert pageSize == 4096; }
    assert IsPageOffset(offset, pageSize);
    OriginalPageSliceIsExact(pageSize, |original| as U64, offset);
    var count := OriginalPageLength(pageSize, |original| as U64, offset) as int;
    assert forall i: int | 0 <= i < count :: page.pageBytes[i] == original[offset as int + i];
  }

  function IsTruncateLength(length: U64, pageSize: U32, maximumLength: U64): bool {
    IsPageSize(pageSize) && length <= maximumLength &&
    length % (pageSize as U64) == 0
  }

  function IsBeforeImageRecord(tag: U64, pageSize: U32, originalLength: U64): bool {
    IsPageSize(pageSize) && IsDatabaseLength(originalLength) &&
    tag >= 9223372036854775808 && tag <= 9223372039002259456 &&
    tag - 9223372036854775808 < originalLength &&
    (tag - 9223372036854775808) % (pageSize as U64) == 0
  }

  // The tag is the exact unsigned interpretation of the persisted 64 bits.
  // Native byte results: 0 invalid, 1 commit, 2 replay, 3 truncate,
  // 4 original before-image, 5 intended after-image. Complete records have
  // already passed framing and checksum validation. finalRecord means no
  // complete or incomplete bytes follow this record. removedTail means a
  // previously validated truncate shortened the original image.
  function ClassifyJournalRecord(tag: U64, pageSize: U32, originalLength: U64,
      recovering: bool, removedTail: bool, finalRecord: bool): Byte {
    if !IsPageSize(pageSize) || !IsDatabaseLength(originalLength) then 0
    else if tag == 18446744073709551615 then (if finalRecord then 1 else 0)
    else if tag == 18446744073709551613 then (if removedTail then 2 else 0)
    else if recovering then 0
    else if tag == 18446744073709551614 then 3
    else if IsBeforeImageRecord(tag, pageSize, originalLength) then 4
    else if IsPageOffset(tag, pageSize) then 5
    else 0
  }

  lemma BeforeImageRecordsAuthorizeOnlyOriginalAlignedPages(
      tag: U64, pageSize: U32, originalLength: U64)
    requires IsBeforeImageRecord(tag, pageSize, originalLength)
    ensures pageSize == 2048 || pageSize == 4096
    ensures 9223372036854775808 <= tag
    ensures 0 <= tag as int - 9223372036854775808 < originalLength as int <= 2147483648
    ensures (tag as int - 9223372036854775808) % (pageSize as int) == 0
  { }

  lemma JournalRecordClassificationIsExact(tag: U64, pageSize: U32, originalLength: U64,
      recovering: bool, removedTail: bool, finalRecord: bool)
    ensures ClassifyJournalRecord(tag, pageSize, originalLength, recovering, removedTail, finalRecord) <= 5
    ensures ClassifyJournalRecord(tag, pageSize, originalLength, recovering, removedTail, finalRecord) == 1 <==>
      IsPageSize(pageSize) && IsDatabaseLength(originalLength) &&
      tag == 18446744073709551615 && finalRecord
    ensures ClassifyJournalRecord(tag, pageSize, originalLength, recovering, removedTail, finalRecord) == 2 <==>
      IsPageSize(pageSize) && IsDatabaseLength(originalLength) &&
      tag == 18446744073709551613 && removedTail
    ensures ClassifyJournalRecord(tag, pageSize, originalLength, recovering, removedTail, finalRecord) == 3 <==>
      IsPageSize(pageSize) && IsDatabaseLength(originalLength) &&
      tag == 18446744073709551614 && !recovering
    ensures ClassifyJournalRecord(tag, pageSize, originalLength, recovering, removedTail, finalRecord) == 4 <==>
      !recovering && IsBeforeImageRecord(tag, pageSize, originalLength)
    ensures ClassifyJournalRecord(tag, pageSize, originalLength, recovering, removedTail, finalRecord) == 5 <==>
      IsDatabaseLength(originalLength) && !recovering && IsPageOffset(tag, pageSize)
  { }

  lemma ReplayStateRefusesFurtherMutations(tag: U64, pageSize: U32, originalLength: U64,
      removedTail: bool, finalRecord: bool)
    ensures ClassifyJournalRecord(tag, pageSize, originalLength, true, removedTail, finalRecord) <= 2
    ensures !removedTail ==>
      ClassifyJournalRecord(tag, pageSize, originalLength, true, removedTail, finalRecord) != 2
  { }

  function OriginalOrZeroByte(recovering: bool, offset: U64,
      minimumLength: U64, current: Byte, original: Byte): bool {
    current == original || (recovering && offset >= minimumLength && current == 0)
  }

  function IncludeAfterImageByte(allowed: bool, current: Byte, after: Byte): bool {
    allowed || current == after
  }

  lemma PageOffsetAuthorizesOnlyCompleteBoundedPages(offset: U64, pageSize: U32)
    requires IsPageOffset(offset, pageSize)
    ensures pageSize == 2048 || pageSize == 4096
    ensures offset as int + pageSize as int <= 2147483648
    ensures (offset as int) % (pageSize as int) == 0
  { }

  lemma TruncationAuthorizesOnlyAlignedPrefixes(length: U64, pageSize: U32, maximumLength: U64)
    requires IsTruncateLength(length, pageSize, maximumLength)
    ensures length <= maximumLength
    ensures pageSize == 2048 || pageSize == 4096
    ensures (length as int) % (pageSize as int) == 0
  { }

  ghost function FoldAfterImages(allowed: bool, current: Byte, images: seq<Byte>): bool
    decreases |images|
  {
    if |images| == 0 then allowed
    else IncludeAfterImageByte(
      FoldAfterImages(allowed, current, images[..|images| - 1]), current, images[|images| - 1])
  }

  lemma FoldAfterImagesIsExact(allowed: bool, current: Byte, images: seq<Byte>)
    ensures FoldAfterImages(allowed, current, images) == (allowed || current in images)
    decreases |images|
  {
    if |images| > 0 {
      FoldAfterImagesIsExact(allowed, current, images[..|images| - 1]);
      assert images == images[..|images| - 1] + [images[|images| - 1]];
    }
  }

  // A production page's original buffer contains its exact original bytes,
  // padded with zero beyond the original length. Its matching validated
  // write records supply exactly the abstract intent values at this offset.
  // Under that explicit correspondence, repeated native byte decisions are
  // equivalent to the existing unbounded model's Allowed predicate.
  lemma ByteAuthorizationRefinesUndo(
      original: seq<Undo.Byte>, intents: map<nat, set<Undo.Byte>>,
      minimumLength: U64, recovering: bool, offset: U64, current: Byte,
      originalByte: Byte, images: seq<Byte>)
    requires if (offset as int) < |original|
      then originalByte as int == original[offset as int] else originalByte == 0
    requires (offset as int) in intents ==>
      intents[offset as int] == set i: int | 0 <= i < |images| :: images[i] as int
    requires (offset as int) !in intents ==> |images| == 0
    ensures FoldAfterImages(
      OriginalOrZeroByte(recovering, offset, minimumLength, current, originalByte), current, images) ==
      Undo.Allowed(original, intents, minimumLength as int, recovering, offset as int, current as int)
  {
    FoldAfterImagesIsExact(
      OriginalOrZeroByte(recovering, offset, minimumLength, current, originalByte), current, images);
    if (offset as int) in intents {
      assert (current as int) in intents[offset as int] <==> current in images;
    }
  }

  lemma RecoveryMarkerCannotAuthorizeRetainedPrefix(
      offset: U64, minimumLength: U64, current: Byte, original: Byte)
    requires offset < minimumLength
    ensures OriginalOrZeroByte(true, offset, minimumLength, current, original) ==
      OriginalOrZeroByte(false, offset, minimumLength, current, original)
  { }
}
