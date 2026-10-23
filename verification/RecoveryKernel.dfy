// Executable recovery decisions. Generated C# calls these total native-width
// functions; ghost lemmas connect byte authorization to UndoJournal.Allowed.
// Remaining record-byte decoding, hashes, evidence identity, I/O and full loop correspondence remain
// outside this kernel. No external C# contract is trusted by these proofs.
module NativeRecoveryTypes {
  newtype {:nativeType "ulong"} U64 = x: int | 0 <= x < 18446744073709551616
  newtype {:nativeType "uint"} U32 = x: int | 0 <= x < 4294967296
  newtype {:nativeType "byte"} Byte = x: int | 0 <= x < 256
}

module RecoveryKernel {
  import opened NativeRecoveryTypes
  import Undo = UndoJournal
  import Replacement = ReplacementProtocol

  // Decode the on-disk signed-length field as raw unsigned bits. Admission
  // below rejects every negative encoding before production narrows to long.
  function DecodeReplacementLength(b0: Byte, b1: Byte, b2: Byte, b3: Byte,
      b4: Byte, b5: Byte, b6: Byte, b7: Byte): U64 {
    (b0 as U64) + 256 * (b1 as U64) + 65536 * (b2 as U64) + 16777216 * (b3 as U64) +
      4294967296 * (b4 as U64) + 1099511627776 * (b5 as U64) +
      281474976710656 * (b6 as U64) + 72057594037927936 * (b7 as U64)
  }

  function IsReplacementIntentLength(length: U64): bool {
    256 <= length <= 352
  }

  function IsReplacementHeader(headerFieldsValid: bool, oldLength: U64, newLength: U64): bool {
    headerFieldsValid && IsDatabaseLength(oldLength) && IsDatabaseLength(newLength)
  }

  // Native byte results: 0 refuses, 1 selects Prepared, 2 selects Committed.
  // Field/digest checks describe the exact acquired record, not ownership.
  // Even a purportedly valid decision cannot commit an incomplete append.
  function ReplacementAdmission(length: U64, headerFieldsValid: bool,
      oldLength: U64, newLength: U64, decisionValid: bool): Byte {
    if !IsReplacementIntentLength(length) || !IsReplacementHeader(headerFieldsValid, oldLength, newLength) then 0
    else if length < 352 then 1
    else if decisionValid then 2
    else 0
  }

  lemma ReplacementLengthDecodingMatchesSignedField(bytes: seq<Replacement.RecordByte>)
    requires |bytes| == 8
    ensures DecodeReplacementLength(bytes[0] as Byte, bytes[1] as Byte, bytes[2] as Byte,
      bytes[3] as Byte, bytes[4] as Byte, bytes[5] as Byte, bytes[6] as Byte, bytes[7] as Byte) as int ==
      Replacement.LittleEndian64(bytes)
    ensures IsDatabaseLength(DecodeReplacementLength(bytes[0] as Byte, bytes[1] as Byte,
      bytes[2] as Byte, bytes[3] as Byte, bytes[4] as Byte, bytes[5] as Byte, bytes[6] as Byte, bytes[7] as Byte)) <==>
      0 <= Replacement.SignedLittleEndian64(bytes) <= 2147483648
  {
    Replacement.LengthEncodingMatchesSignedBounds(bytes);
  }

  // All byte/field correspondence is explicit. Exact .NET acquisition,
  // GUID/path/digest semantics and generation authority are not inferred.
  lemma ReplacementAdmissionRefinesRecordBytes(bytes: seq<Replacement.RecordByte>,
      pathIdentity: seq<Replacement.RecordByte>,
      digest: seq<Replacement.RecordByte> -> seq<Replacement.RecordByte>,
      headerFieldsValid: bool, oldLength: U64, newLength: U64, decisionValid: bool)
    requires |bytes| < 18446744073709551616
    requires headerFieldsValid == (|bytes| >= 256 &&
      Replacement.ValidHeaderFields(bytes[..256], pathIdentity, digest))
    requires |bytes| >= 256 ==>
      oldLength as int == Replacement.LittleEndian64(bytes[64..72]) &&
      newLength as int == Replacement.LittleEndian64(bytes[72..80])
    requires decisionValid == (|bytes| == 352 &&
      Replacement.ValidDecisionBytes(bytes[..256], bytes[256..], digest))
    ensures ReplacementAdmission(|bytes| as U64, headerFieldsValid, oldLength, newLength, decisionValid) == 0 <==>
      Replacement.ReadRecordBytes(bytes, pathIdentity, digest) == Replacement.Refuse
    ensures ReplacementAdmission(|bytes| as U64, headerFieldsValid, oldLength, newLength, decisionValid) == 1 <==>
      Replacement.ReadRecordBytes(bytes, pathIdentity, digest) == Replacement.Admit(Replacement.Prepared)
    ensures ReplacementAdmission(|bytes| as U64, headerFieldsValid, oldLength, newLength, decisionValid) == 2 <==>
      Replacement.ReadRecordBytes(bytes, pathIdentity, digest) == Replacement.Admit(Replacement.Committed)
  { }

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

  // A torn after-image can persist any nonempty prefix of its bounded page.
  // This grants only a possible length; original-tail coverage and admitted
  // content are separate mandatory checks in the production parser.
  function IsAfterImageLength(length: U64, offset: U64, pageSize: U32): bool {
    IsPageOffset(offset, pageSize) && offset < length &&
    length <= offset + (pageSize as U64)
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

  // Zero refuses acquisition outside the complete aligned prefix. Every valid
  // cursor moves by exactly one record and decreases the remaining-record rank.
  function NextJournalRecord(pageSize: U32, originalLength: U64,
      journalLength: U64, position: U64): U64 {
    if JournalRecordPayloadOffset(pageSize, originalLength, journalLength, position) == 0 then 0
    else position + (pageSize as U64) + 40
  }

  // Unlike the complete prefix, finality includes every persisted trailing byte.
  // A commit followed by an incomplete fragment cannot be admitted as terminal.
  function IsFinalJournalRecord(pageSize: U32, originalLength: U64,
      journalLength: U64, position: U64): bool {
    var next := NextJournalRecord(pageSize, originalLength, journalLength, position);
    next != 0 && next == journalLength
  }

  // The tag must be acquired from this complete checksum-validated record.
  // Bind its exact original page before extracting any replay/content bytes;
  // a previously populated dictionary alone does not grant payload identity.
  function OriginalPageRecordPayloadOffset(pageSize: U32, originalLength: U64,
      journalLength: U64, position: U64, tag: U64, offset: U64): U64 {
    if OriginalPageLength(pageSize, originalLength, offset) == 0 ||
        !IsBeforeImageRecord(tag, pageSize, originalLength) ||
        tag - 9223372036854775808 != offset then 0
    else JournalRecordPayloadOffset(pageSize, originalLength, journalLength, position)
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

  lemma JournalRecordCursorMakesBoundedProgress(pageSize: U32, originalLength: U64,
      journalLength: U64, position: U64)
    ensures NextJournalRecord(pageSize, originalLength, journalLength, position) != 0 <==>
      JournalRecordPayloadOffset(pageSize, originalLength, journalLength, position) != 0
    ensures NextJournalRecord(pageSize, originalLength, journalLength, position) != 0 ==>
      position as int < NextJournalRecord(pageSize, originalLength, journalLength, position) as int <=
        CompleteJournalEnd(pageSize, originalLength, journalLength) as int <= journalLength as int
    ensures NextJournalRecord(pageSize, originalLength, journalLength, position) != 0 ==>
      NextJournalRecord(pageSize, originalLength, journalLength, position) as int ==
        position as int + pageSize as int + 40
    ensures NextJournalRecord(pageSize, originalLength, journalLength, position) != 0 ==>
      (CompleteJournalEnd(pageSize, originalLength, journalLength) as int - position as int) /
        (pageSize as int + 40) ==
      1 + (CompleteJournalEnd(pageSize, originalLength, journalLength) as int -
        NextJournalRecord(pageSize, originalLength, journalLength, position) as int) / (pageSize as int + 40)
    ensures (NextJournalRecord(pageSize, originalLength, journalLength, position) != 0 &&
      NextJournalRecord(pageSize, originalLength, journalLength, position) < CompleteJournalEnd(pageSize, originalLength, journalLength)) ==>
      JournalRecordPayloadOffset(pageSize, originalLength, journalLength,
        NextJournalRecord(pageSize, originalLength, journalLength, position)) != 0
    ensures IsFinalJournalRecord(pageSize, originalLength, journalLength, position) <==>
      JournalRecordPayloadOffset(pageSize, originalLength, journalLength, position) != 0 &&
      position as int + pageSize as int + 40 == journalLength as int
  {
    JournalPayloadSliceIsExactAndBounded(pageSize, originalLength, journalLength, position);
    JournalFramingSelectsExactCompletePrefix(pageSize, originalLength, journalLength);
    if NextJournalRecord(pageSize, originalLength, journalLength, position) != 0 {
      var start := JournalRecordStart(pageSize, originalLength) as int;
      var end := CompleteJournalEnd(pageSize, originalLength, journalLength) as int;
      var size := pageSize as int + 40;
      var next := NextJournalRecord(pageSize, originalLength, journalLength, position);
      assert (next as int - start) % size == 0;
      assert (end - next as int) % size == 0;
      if next < CompleteJournalEnd(pageSize, originalLength, journalLength) {
        assert next as int + size <= end;
        JournalPayloadSliceIsExactAndBounded(pageSize, originalLength, journalLength, next);
      }
    }
  }

  lemma OriginalRecordPayloadBindsExactPage(pageSize: U32, originalLength: U64,
      journalLength: U64, position: U64, tag: U64, offset: U64)
    ensures OriginalPageRecordPayloadOffset(pageSize, originalLength, journalLength, position, tag, offset) != 0 <==>
      JournalRecordPayloadOffset(pageSize, originalLength, journalLength, position) != 0 &&
      OriginalPageLength(pageSize, originalLength, offset) != 0 &&
      IsBeforeImageRecord(tag, pageSize, originalLength) && tag as int - 9223372036854775808 == offset as int
    ensures OriginalPageRecordPayloadOffset(pageSize, originalLength, journalLength, position, tag, offset) != 0 ==>
      OriginalPageRecordPayloadOffset(pageSize, originalLength, journalLength, position, tag, offset) ==
        JournalRecordPayloadOffset(pageSize, originalLength, journalLength, position)
    ensures OriginalPageRecordPayloadOffset(pageSize, originalLength, journalLength, position, tag, offset) != 0 ==>
      tag as int - 9223372036854775808 == offset as int &&
      offset as int < originalLength as int && (offset as int) % (pageSize as int) == 0
  {
    OriginalPageSliceIsExact(pageSize, originalLength, offset);
    if IsBeforeImageRecord(tag, pageSize, originalLength) {
      BeforeImageRecordsAuthorizeOnlyOriginalAlignedPages(tag, pageSize, originalLength);
    }
  }

  // Under explicit exact-payload correspondence, the generated count extracts
  // exactly original bytes and excludes padding. The acquired-record lemma
  // below additionally binds the record tag to this original page offset.
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

  // The acquired record supplies its own tag-to-payload association. Exact
  // logical payload identity remains an explicit integrity/model premise;
  // tag equality alone does not prove the stored bytes equal the old image.
  lemma AcquiredOriginalPayloadRefinesCapturedPage(original: seq<Undo.Byte>,
      pageSize: U32, journalLength: U64, position: U64, tag: U64, offset: U64,
      page: Undo.PageCapture)
    requires |original| <= 2147483648
    requires OriginalPageRecordPayloadOffset(pageSize, |original| as U64, journalLength, position, tag, offset) != 0
    requires page.pageOffset == tag as int - 9223372036854775808
    requires Undo.ExactPagePayload(original, pageSize as int, page)
    ensures page.pageOffset == offset as int
    ensures 0 < OriginalPageLength(pageSize, |original| as U64, offset) <= pageSize
    ensures OriginalPageLength(pageSize, |original| as U64, offset) as int <= |page.pageBytes|
    ensures offset as int + OriginalPageLength(pageSize, |original| as U64, offset) as int <= |original|
    ensures page.pageBytes[..OriginalPageLength(pageSize, |original| as U64, offset) as int] ==
      original[offset as int..offset as int + OriginalPageLength(pageSize, |original| as U64, offset) as int]
  {
    OriginalRecordPayloadBindsExactPage(pageSize, |original| as U64, journalLength, position, tag, offset);
    OriginalPayloadExtractionRefinesCapturedPage(original, pageSize, offset, page);
  }

  function IsTruncateLength(length: U64, pageSize: U32, maximumLength: U64): bool {
    IsPageSize(pageSize) && length <= maximumLength &&
    length % (pageSize as U64) == 0
  }

  // A truncate intent must account for every original page intersecting its
  // removed tail, including a partial final page. Invalid values select none;
  // the caller separately requires valid truncate bounds before this schedule.
  function TruncateOriginalPageCount(pageSize: U32, originalLength: U64, length: U64): U32 {
    if !IsPageSize(pageSize) || !IsDatabaseLength(originalLength) ||
        !IsDatabaseLength(length) || length % (pageSize as U64) != 0 || length >= originalLength then 0
    else (((originalLength - length - 1) / (pageSize as U64)) + 1) as U32
  }

  // The Access size limit cannot identify an original page and is the refusal
  // sentinel. This selector admits only an index within the exact tail count.
  function TruncateOriginalPageOffset(pageSize: U32, originalLength: U64,
      length: U64, index: U32): U64 {
    if index >= TruncateOriginalPageCount(pageSize, originalLength, length) then 2147483648
    else length + (index as U64) * (pageSize as U64)
  }

  lemma TruncateCoverageCountIsExact(pageSize: U32, originalLength: U64, length: U64)
    ensures TruncateOriginalPageCount(pageSize, originalLength, length) <= 1048576
    ensures TruncateOriginalPageCount(pageSize, originalLength, length) == 0 <==>
      !IsPageSize(pageSize) || !IsDatabaseLength(originalLength) || !IsDatabaseLength(length) ||
      (IsPageSize(pageSize) && length as int % pageSize as int != 0) || length >= originalLength
    ensures TruncateOriginalPageCount(pageSize, originalLength, length) != 0 ==>
      (TruncateOriginalPageCount(pageSize, originalLength, length) as int - 1) * pageSize as int <
        originalLength as int - length as int <=
      TruncateOriginalPageCount(pageSize, originalLength, length) as int * pageSize as int
  {
    if IsPageSize(pageSize) {
      if pageSize == 2048 { } else { assert pageSize == 4096; }
    }
  }

  lemma TruncateCoveragePageIsExact(pageSize: U32, originalLength: U64,
      length: U64, index: U32)
    ensures TruncateOriginalPageOffset(pageSize, originalLength, length, index) != 2147483648 <==>
      index < TruncateOriginalPageCount(pageSize, originalLength, length)
    ensures index < TruncateOriginalPageCount(pageSize, originalLength, length) ==>
      TruncateOriginalPageOffset(pageSize, originalLength, length, index) as int ==
        length as int + index as int * pageSize as int
    ensures index < TruncateOriginalPageCount(pageSize, originalLength, length) ==>
      IsPageOffset(TruncateOriginalPageOffset(pageSize, originalLength, length, index), pageSize) &&
      length <= TruncateOriginalPageOffset(pageSize, originalLength, length, index) < originalLength &&
      OriginalPageLength(pageSize, originalLength,
        TruncateOriginalPageOffset(pageSize, originalLength, length, index)) > 0
  {
    TruncateCoverageCountIsExact(pageSize, originalLength, length);
    if index < TruncateOriginalPageCount(pageSize, originalLength, length) {
      if pageSize == 2048 { } else { assert pageSize == 4096; }
      var offset := TruncateOriginalPageOffset(pageSize, originalLength, length, index);
      assert offset as int % pageSize as int == 0;
      OriginalPageSliceIsExact(pageSize, originalLength, offset);
    }
  }

  lemma TruncateCoveragePagesAreDistinctAndRanked(pageSize: U32, originalLength: U64,
      length: U64, left: U32, right: U32)
    requires left < right < TruncateOriginalPageCount(pageSize, originalLength, length)
    ensures TruncateOriginalPageOffset(pageSize, originalLength, length, left) <
      TruncateOriginalPageOffset(pageSize, originalLength, length, right)
    ensures 0 < TruncateOriginalPageCount(pageSize, originalLength, length) as int - right as int <
      TruncateOriginalPageCount(pageSize, originalLength, length) as int - left as int
  {
    TruncateCoveragePageIsExact(pageSize, originalLength, length, left);
    TruncateCoveragePageIsExact(pageSize, originalLength, length, right);
  }
  lemma TruncateCoverageSelectsEveryRemovedByte(pageSize: U32, originalLength: U64,
      length: U64, offset: U64)
    requires IsPageSize(pageSize) && IsDatabaseLength(originalLength) && IsDatabaseLength(length)
    requires length as int % pageSize as int == 0
    requires length <= offset < originalLength
    ensures (offset as int - length as int) / pageSize as int <
      TruncateOriginalPageCount(pageSize, originalLength, length) as int
    ensures var index := ((offset - length) / (pageSize as U64)) as U32;
      var page := TruncateOriginalPageOffset(pageSize, originalLength, length, index);
      page <= offset && offset as int < page as int + OriginalPageLength(pageSize, originalLength, page) as int
  {
    TruncateCoverageCountIsExact(pageSize, originalLength, length);
    if pageSize == 2048 { } else { assert pageSize == 4096; }
    var index := ((offset - length) / (pageSize as U64)) as U32;
    TruncateCoveragePageIsExact(pageSize, originalLength, length, index);
    var page := TruncateOriginalPageOffset(pageSize, originalLength, length, index);
    assert offset as int - length as int ==
      index as int * pageSize as int + (offset as int - length as int) % pageSize as int;
    OriginalPageSliceIsExact(pageSize, originalLength, page);
  }

  // A production map entry must stand for a checksum/hash-validated exact page
  // expanded into its original-byte records. Under that explicit map-to-Saved
  // contract, checking all generated selections establishes complete coverage;
  // a dictionary key or the arithmetic schedule alone grants no byte identity.
  lemma TruncateCoverageAdmissionRefinesSaved(original: seq<Undo.Byte>, records: seq<Undo.JournalRecord>,
      pageSize: U32, length: U64, capturedPages: set<nat>)
    requires |original| <= 2147483648 && IsPageSize(pageSize) && IsDatabaseLength(length)
    requires length as int % pageSize as int == 0
    requires Undo.ExactRecords(original, records)
    requires forall page: nat | page in capturedPages ::
      Undo.Covered(records, |original|, page, page + pageSize as int)
    requires forall index: U32 {:trigger TruncateOriginalPageOffset(pageSize, |original| as U64, length, index)} | index < TruncateOriginalPageCount(pageSize, |original| as U64, length) ::
      TruncateOriginalPageOffset(pageSize, |original| as U64, length, index) as int in capturedPages
    ensures Undo.ValidUndo(original, Undo.Saved(records))
    ensures Undo.Covered(records, |original|, length as int, |original|)
  {
    Undo.SavedRecordsAreExact(original, records);
    forall offset: nat | length as int <= offset < |original|
      ensures offset in Undo.Saved(records)
    {
      TruncateCoverageSelectsEveryRemovedByte(pageSize, |original| as U64, length, offset as U64);
      var index := ((offset - length as int) / pageSize as int) as U32;
      var page := TruncateOriginalPageOffset(pageSize, |original| as U64, length, index);
      assert page as int in capturedPages;
      assert Undo.Covered(records, |original|, page as int, page as int + pageSize as int);
    }
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

  lemma AfterImageLengthAuthorizesExactBoundedInterval(length: U64, offset: U64, pageSize: U32)
    ensures IsAfterImageLength(length, offset, pageSize) <==>
      IsPageOffset(offset, pageSize) && offset as int < length as int <= offset as int + pageSize as int
    ensures IsAfterImageLength(length, offset, pageSize) ==>
      IsDatabaseLength(length) && 0 < length as int - offset as int <= pageSize as int
    ensures IsAfterImageLength(length, offset, pageSize) ==>
      offset as int + pageSize as int <= 2147483648
  {
    if IsPageOffset(offset, pageSize) {
      PageOffsetAuthorizesOnlyCompleteBoundedPages(offset, pageSize);
    }
  }

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
