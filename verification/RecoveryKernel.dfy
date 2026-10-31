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

  // Shared parser slices use the signed managed offset domain. Subtraction
  // follows offset admission, so even hostile native-width values are total.
  function IsByteRange(length: U64, offset: U64, count: U64): bool {
    length <= 9223372036854775807 && offset <= length && count <= length - offset
  }

  lemma ByteRangeIsExactAndOverflowSafe(length: U64, offset: U64, count: U64)
    ensures IsByteRange(length, offset, count) <==>
      0 <= offset as int <= offset as int + count as int <= length as int <= 9223372036854775807
    ensures IsByteRange(length, offset, count) ==>
      offset + count <= 9223372036854775807
  { }

  lemma NestedByteSlicesCompose(length: U64, offset: U64, count: U64,
      innerOffset: U64, innerCount: U64)
    requires IsByteRange(length, offset, count)
    requires IsByteRange(count, innerOffset, innerCount)
    ensures IsByteRange(length, offset + innerOffset, innerCount)
    ensures offset + innerOffset + innerCount <= offset + count
  { }

  // uint.MaxValue is outside every managed buffer. A successful positive
  // read consumes exactly its count and strictly decreases remaining bytes.
  function BufferReadAdvance(length: U32, offset: U32, count: U32): U32 {
    if length > 2147483647 || offset >= length || count == 0 || count > length - offset
    then 4294967295 else offset + count
  }

  lemma BufferReadMakesExactProgress(length: U32, offset: U32, count: U32)
    ensures BufferReadAdvance(length, offset, count) != 4294967295 <==>
      length <= 2147483647 && offset < length && 0 < count <= length - offset
    ensures BufferReadAdvance(length, offset, count) != 4294967295 ==>
      offset < BufferReadAdvance(length, offset, count) <= length &&
      BufferReadAdvance(length, offset, count) == offset + count &&
      length - BufferReadAdvance(length, offset, count) < length - offset
    ensures BufferReadAdvance(length, offset, count) != 4294967295 ==>
      IsByteRange(length as U64, offset as U64, count as U64)
  { }

  function ReadMinimumSatisfied(length: U32, minimum: U32, consumed: U32): bool {
    length <= 2147483647 && minimum <= consumed <= length
  }

  lemma ExactReadIsExactlyFull(length: U32, consumed: U32)
    ensures ReadMinimumSatisfied(length, length, consumed) <==>
      length <= 2147483647 && consumed == length
  { }

  ghost predicate SuccessfulReadTrace(length: U32, offset: U32, counts: seq<U32>)
    decreases |counts|
  {
    length <= 2147483647 && offset <= length &&
      (|counts| == 0 ||
        (BufferReadAdvance(length, offset, counts[0]) != 4294967295 &&
          SuccessfulReadTrace(length, BufferReadAdvance(length, offset, counts[0]), counts[1..])))
  }

  ghost function ReadTraceEnd(length: U32, offset: U32, counts: seq<U32>): U32
    requires SuccessfulReadTrace(length, offset, counts)
    ensures offset <= ReadTraceEnd(length, offset, counts) <= length
    decreases |counts|
  {
    if |counts| == 0 then offset
    else ReadTraceEnd(length, BufferReadAdvance(length, offset, counts[0]), counts[1..])
  }

  lemma ReadTraceConsumesBoundedPrefix(length: U32, offset: U32, counts: seq<U32>)
    requires SuccessfulReadTrace(length, offset, counts)
    ensures offset <= ReadTraceEnd(length, offset, counts) <= length
    ensures |counts| <= ReadTraceEnd(length, offset, counts) as int - offset as int
    ensures |counts| <= length as int - offset as int
    decreases |counts|
  {
    if |counts| > 0 {
      BufferReadMakesExactProgress(length, offset, counts[0]);
      ReadTraceConsumesBoundedPrefix(length, BufferReadAdvance(length, offset, counts[0]), counts[1..]);
    }
  }

  // This models Stream's successful read effect, not a trusted declaration
  // about .NET: each acquired chunk is the next exact source prefix.
  lemma ExactReadChunkPreservesSourcePrefix(source: seq<Byte>, prefix: seq<Byte>,
      chunk: seq<Byte>, length: U32, offset: U32, count: U32)
    requires SuccessfulReadTrace(length, offset, [count])
    requires length as int <= |source|
    requires prefix == source[..offset as int]
    requires chunk == source[offset as int..(offset as int + count as int)]
    ensures prefix + chunk == source[..BufferReadAdvance(length, offset, count) as int]
    ensures BufferReadAdvance(length, offset, count) == length ==>
      prefix + chunk == source[..length as int]
  {
    BufferReadMakesExactProgress(length, offset, count);
  }
  ghost function ConcatenateReadChunks(chunks: seq<seq<Byte>>): seq<Byte>
    decreases |chunks|
  {
    if |chunks| == 0 then [] else chunks[0] + ConcatenateReadChunks(chunks[1..])
  }

  ghost predicate ReadEffectsMatchSource(source: seq<Byte>, length: U32,
      offset: U32, counts: seq<U32>, chunks: seq<seq<Byte>>)
    requires SuccessfulReadTrace(length, offset, counts)
    requires length as int <= |source|
    decreases |counts|
  {
    |counts| == |chunks| && (|counts| == 0 ||
      (chunks[0] == source[offset as int..(offset as int + counts[0] as int)] &&
        ReadEffectsMatchSource(source, length, BufferReadAdvance(length, offset, counts[0]),
          counts[1..], chunks[1..])))
  }

  // Arbitrary finite successful reads compose to the exact requested prefix.
  // The named effect premise excludes malicious streams inventing bytes;
  // cursor progress alone cannot establish that premise or I/O termination.
  lemma ExactReadTraceAcquiresRequestedPrefix(source: seq<Byte>, prefix: seq<Byte>,
      length: U32, offset: U32, counts: seq<U32>, chunks: seq<seq<Byte>>)
    requires SuccessfulReadTrace(length, offset, counts)
    requires length as int <= |source|
    requires prefix == source[..offset as int]
    requires ReadEffectsMatchSource(source, length, offset, counts, chunks)
    ensures prefix + ConcatenateReadChunks(chunks) ==
      source[..ReadTraceEnd(length, offset, counts) as int]
    ensures ReadTraceEnd(length, offset, counts) == length ==>
      prefix + ConcatenateReadChunks(chunks) == source[..length as int]
    decreases |counts|
  {
    if |counts| > 0 {
      ExactReadChunkPreservesSourcePrefix(source, prefix, chunks[0], length, offset, counts[0]);
      ExactReadTraceAcquiresRequestedPrefix(source, prefix + chunks[0], length,
        BufferReadAdvance(length, offset, counts[0]), counts[1..], chunks[1..]);
    }
  }
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

  // Production-order replay control after complete evidence/content admission.
  // phase 0 restores pages; phase 1 follows resize; phase 2 follows flush
  // as advanced by the caller only AFTER the preceding effect succeeds:
  // phase 1 therefore selects flush, and phase 2 selects the decision.
  // Results: 0 refuse; 1 durable marker; 2 next captured page; 3 resize;
  // 4 durable database flush; 5 completion decision. No I/O is trusted here.
  function JournalReplayAction(pageSize: U32, originalLength: U64, minimumLength: U64,
      recovering: bool, remainingPages: U32, phase: Byte): Byte {
    var start := JournalRecordStart(pageSize, originalLength);
    var marker := minimumLength < originalLength && !recovering;
    if start == 0 || minimumLength > originalLength || phase > 2 ||
        remainingPages as U64 > (start - 128) / 32 ||
        (phase > 0 && (remainingPages > 0 || marker)) then 0
    else if phase == 0 then (if marker then 1 else if remainingPages > 0 then 2 else 3)
    else if phase == 1 then 4 else 5
  }

  lemma JournalReplayActionRefinesProductionOrder(pageSize: U32, originalLength: U64,
      minimumLength: U64, recovering: bool, remainingPages: U32, phase: Byte)
    requires IsPageSize(pageSize) && IsDatabaseLength(originalLength)
    requires remainingPages as int <= (JournalRecordStart(pageSize, originalLength) as int - 128) / 32
    ensures JournalReplayAction(pageSize, originalLength, minimumLength, recovering, remainingPages, phase) as int ==
      Undo.PageReplayAction(originalLength as int,
        Undo.PageReplayControl(minimumLength as int, recovering, remainingPages as int, phase as int))
  {
    JournalManifestIsExactBoundedPageRounding(pageSize, originalLength);
  }

  lemma JournalReplayActionRequiresSafeOrder(pageSize: U32, originalLength: U64,
      minimumLength: U64, recovering: bool, remainingPages: U32, phase: Byte)
    ensures JournalReplayAction(pageSize, originalLength, minimumLength, recovering, remainingPages, phase) <= 5
    ensures JournalReplayAction(pageSize, originalLength, minimumLength, recovering, remainingPages, phase) != 0 ==>
      IsPageSize(pageSize) && minimumLength <= originalLength <= 2147483648 && remainingPages <= 1048576
    ensures JournalReplayAction(pageSize, originalLength, minimumLength, recovering, remainingPages, phase) == 1 ==>
      minimumLength < originalLength && !recovering && phase == 0
    ensures JournalReplayAction(pageSize, originalLength, minimumLength, recovering, remainingPages, phase) >= 2 ==>
      minimumLength == originalLength || recovering
    ensures JournalReplayAction(pageSize, originalLength, minimumLength, recovering, remainingPages, phase) == 2 ==>
      phase == 0 && remainingPages > 0
    ensures JournalReplayAction(pageSize, originalLength, minimumLength, recovering, remainingPages, phase) >= 3 ==>
      remainingPages == 0
    ensures JournalReplayAction(pageSize, originalLength, minimumLength, recovering, remainingPages, phase) == 3 ==> phase == 0
    ensures JournalReplayAction(pageSize, originalLength, minimumLength, recovering, remainingPages, phase) == 4 ==> phase == 1
    ensures JournalReplayAction(pageSize, originalLength, minimumLength, recovering, remainingPages, phase) == 5 ==> phase == 2
  {
    JournalManifestIsExactBoundedPageRounding(pageSize, originalLength);
  }

  // A replay page is selected from the serialized complete-record cursor,
  // never from dictionary enumeration. A map lookup supplies this record's
  // exact admitted source position. The Access bound is a refusal sentinel.
  function JournalReplayPageOffset(pageSize: U32, originalLength: U64,
      journalLength: U64, position: U64, tag: U64, mappedPosition: U64,
      remainingPages: U32): U64 {
    if !IsBeforeImageRecord(tag, pageSize, originalLength) then 2147483648
    else
      var offset := tag - 9223372036854775808;
      if remainingPages == 0 || mappedPosition != position ||
          remainingPages as U64 > (JournalRecordStart(pageSize, originalLength) - 128) / 32 ||
          OriginalPageRecordPayloadOffset(pageSize, originalLength, journalLength, position, tag, offset) == 0
      then 2147483648 else offset
  }

  lemma ReplaySelectionBindsExactSerializedInventoryEntry(pageSize: U32, originalLength: U64,
      journalLength: U64, position: U64, tag: U64, mappedPosition: U64,
      remainingPages: U32)
    ensures JournalReplayPageOffset(pageSize, originalLength, journalLength, position, tag, mappedPosition, remainingPages) != 2147483648 <==>
      IsBeforeImageRecord(tag, pageSize, originalLength) && remainingPages > 0 &&
      mappedPosition == position &&
      remainingPages as int <= (JournalRecordStart(pageSize, originalLength) as int - 128) / 32 &&
      JournalRecordPayloadOffset(pageSize, originalLength, journalLength, position) != 0
    ensures JournalReplayPageOffset(pageSize, originalLength, journalLength, position, tag, mappedPosition, remainingPages) != 2147483648 ==>
      var offset := JournalReplayPageOffset(pageSize, originalLength, journalLength, position, tag, mappedPosition, remainingPages);
      offset < originalLength && offset as int % pageSize as int == 0 &&
      mappedPosition == position && tag as int == 9223372036854775808 + offset as int &&
      OriginalPageRecordPayloadOffset(pageSize, originalLength, journalLength, position, tag, offset) == position + 8
  {
    if IsBeforeImageRecord(tag, pageSize, originalLength) {
      BeforeImageRecordsAuthorizeOnlyOriginalAlignedPages(tag, pageSize, originalLength);
      JournalManifestIsExactBoundedPageRounding(pageSize, originalLength);
      var offset := tag - 9223372036854775808;
      OriginalRecordPayloadBindsExactPage(pageSize, originalLength, journalLength, position, tag, offset);
      JournalPayloadSliceIsExactAndBounded(pageSize, originalLength, journalLength, position);
    }
  }

  lemma ReplaySelectionRefinesExactInventoryPayload(original: seq<Undo.Byte>, pageSize: U32,
      pages: seq<Undo.SerializedPage>, journalLength: U64, index: nat,
      tag: U64, remainingPages: U32)
    requires |original| <= 2147483648 && IsPageSize(pageSize)
    requires Undo.ExactSerializedPages(original, pageSize as int, pages)
    requires index < |pages|
    requires |pages| <= (JournalRecordStart(pageSize, |original| as U64) as int - 128) / 32
    requires remainingPages as int == |pages| - index
    requires pages[index].recordPosition < 18446744073709551616
    requires tag as int == 9223372036854775808 + pages[index].page.pageOffset
    requires JournalRecordPayloadOffset(pageSize, |original| as U64, journalLength,
      pages[index].recordPosition as U64) != 0
    ensures pages[index].page.pageOffset < |original|
    ensures pages[index].page.pageOffset in Undo.SerializedPageInventory(pages)
    ensures Undo.SerializedPageInventory(pages)[pages[index].page.pageOffset] == pages[index].recordPosition
    ensures 0 < OriginalPageLength(pageSize, |original| as U64, pages[index].page.pageOffset as U64) as int <= |pages[index].page.pageBytes|
    ensures pages[index].page.pageOffset + OriginalPageLength(pageSize, |original| as U64,
      pages[index].page.pageOffset as U64) as int <= |original|
    ensures var position := pages[index].recordPosition as U64;
      var offset := pages[index].page.pageOffset as U64;
      JournalReplayPageOffset(pageSize, |original| as U64, journalLength, position, tag,
        Undo.SerializedPageInventory(pages)[offset as int] as U64, remainingPages) == offset
    ensures pages[index].page.pageBytes[..OriginalPageLength(pageSize, |original| as U64,
      pages[index].page.pageOffset as U64) as int] ==
      original[pages[index].page.pageOffset..pages[index].page.pageOffset +
        OriginalPageLength(pageSize, |original| as U64, pages[index].page.pageOffset as U64) as int]
  {
    Undo.ExactSerializedInventoryHasExactSources(original, pageSize as int, pages);
    reveal Undo.ExactReplayPagePayload();
    var position := pages[index].recordPosition as U64;
    var offset := pages[index].page.pageOffset as U64;
    assert IsBeforeImageRecord(tag, pageSize, |original| as U64);
    OriginalRecordPayloadBindsExactPage(pageSize, |original| as U64, journalLength, position, tag, offset);
    ReplaySelectionBindsExactSerializedInventoryEntry(pageSize, |original| as U64, journalLength,
      position, tag, position, remainingPages);
    OriginalPageSliceIsExact(pageSize, |original| as U64, offset);
    var count := OriginalPageLength(pageSize, |original| as U64, offset) as int;
    var payload := pages[index].page.pageBytes[..count];
    var expected := original[offset as int..offset as int + count];
    assert |payload| == count && |expected| == count;
    forall i | 0 <= i < count
      ensures payload[i] == expected[i]
    {
      assert pages[index].page.pageBytes[i] == original[offset as int + i];
      assert payload[i] == pages[index].page.pageBytes[i];
      assert expected[i] == original[offset as int + i];
    }
    assert payload == expected;
  }

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

  // Replacement evidence is acquired in exactly bounded records. Successful
  // reads strictly decrease remaining bytes; zero/oversized results refuse.
  function ReplacementReadAdvance(length: U32, offset: U32, count: U32): U32 {
    if (length != 256 && length != 96) || offset >= length || count == 0 || count > length - offset
    then 353 else BufferReadAdvance(length, offset, count)
  }

  lemma ReplacementReadMakesExactProgress(length: U32, offset: U32, count: U32)
    ensures ReplacementReadAdvance(length, offset, count) != 353 <==>
      (length == 256 || length == 96) && offset < length && 0 < count <= length - offset
    ensures ReplacementReadAdvance(length, offset, count) != 353 ==>
      offset < ReplacementReadAdvance(length, offset, count) <= length &&
      ReplacementReadAdvance(length, offset, count) == offset + count &&
      length - ReplacementReadAdvance(length, offset, count) < length - offset
  {}

  function ReplacementHeaderFixedByte(index: U32): Byte {
    if index == 0 then 74 else if index == 1 then 68 else if index == 2 then 87
    else if index == 3 then 82 else if index == 4 then 69 else if index == 5 then 80
    else if index == 6 then 76 else if index == 7 then 49
    else if index == 8 || index == 13 then 1 else 0
  }

  function ReplacementDecisionFixedByte(index: U32): Byte {
    if index == 0 then 74 else if index == 1 then 68 else if index == 2 then 87
    else if index == 3 then 67 else if index == 4 then 77 else if index == 5 then 84
    else if index == 6 then 48 else if index == 7 then 49 else 0
  }

  // The caller supplies actual path/digest bytes, not a validation boolean.
  // Checksum-only reserved bytes and length fields retain their existing role.
  function ReplacementHeaderByteValid(index: U32, value: Byte, pathByte: Byte, digestByte: Byte): bool {
    index < 256 &&
      (if index < 16 then value == ReplacementHeaderFixedByte(index)
       else if 32 <= index < 64 then value == pathByte
       else if 224 <= index then value == digestByte else true)
  }

  function ReplacementGenerationByte(nonempty: bool, index: U32, value: Byte): bool {
    nonempty || (16 <= index < 32 && value != 0)
  }

  function ReplacementDecisionByteValid(index: U32, value: Byte,
      generationByte: Byte, imageByte: Byte, digestByte: Byte): bool {
    index < 96 &&
      (if index < 8 then value == ReplacementDecisionFixedByte(index)
       else if index < 24 then value == generationByte
       else if index < 56 then value == imageByte
       else if 64 <= index then value == digestByte else true)
  }

  ghost function ReplacementRecordBytes(bytes: seq<Byte>): seq<Replacement.RecordByte> {
    seq(|bytes|, i requires 0 <= i < |bytes| => bytes[i] as int)
  }

  // These loops, their bounds and their exact fold correspondence compile to
  // native byte[] code. Only the actual reads and SHA/path primitives remain
  // in the managed adapter; no handwritten validation fold is trusted.
  method CheckReplacementHeader(header: array<Byte>, pathIdentity: array<Byte>, checksum: array<Byte>)
      returns (valid: bool)
    requires header.Length <= 2147483647 && pathIdentity.Length <= 2147483647 && checksum.Length <= 2147483647
    ensures header.Length != 256 || pathIdentity.Length != 32 || checksum.Length != 32 ==> !valid
    ensures header.Length == 256 && pathIdentity.Length == 32 && checksum.Length == 32 ==>
      valid == (FoldReplacementHeader(ReplacementRecordBytes(header[..]),
        ReplacementRecordBytes(pathIdentity[..]), ReplacementRecordBytes(checksum[..]), 256) &&
        FoldReplacementGeneration(ReplacementRecordBytes(header[..]), 256))
  {
    if header.Length as U32 != 256 || pathIdentity.Length as U32 != 32 || checksum.Length as U32 != 32 {
      valid := false;
      return;
    }
    var index: U32 := 0;
    var fieldsValid := true;
    var generationPresent := false;
    while index < 256
      invariant index <= 256
      invariant fieldsValid == FoldReplacementHeader(ReplacementRecordBytes(header[..]),
        ReplacementRecordBytes(pathIdentity[..]), ReplacementRecordBytes(checksum[..]), index as int)
      invariant generationPresent == FoldReplacementGeneration(ReplacementRecordBytes(header[..]), index as int)
      decreases 256 - index
    {
      fieldsValid := fieldsValid && ReplacementHeaderByteValid(index, header[index],
        pathIdentity[if 32 <= index < 64 then index - 32 else 0], checksum[if 224 <= index < 256 then index - 224 else 0]);
      generationPresent := ReplacementGenerationByte(generationPresent, index, header[index]);
      index := index + 1;
    }
    valid := fieldsValid && generationPresent;
  }

  method CheckReplacementDecision(header: array<Byte>, decision: array<Byte>, checksum: array<Byte>)
      returns (valid: bool)
    requires header.Length <= 2147483647 && decision.Length <= 2147483647 && checksum.Length <= 2147483647
    ensures header.Length != 256 || decision.Length != 96 || checksum.Length != 32 ==> !valid
    ensures header.Length == 256 && decision.Length == 96 && checksum.Length == 32 ==>
      valid == FoldReplacementDecision(ReplacementRecordBytes(header[..]),
        ReplacementRecordBytes(decision[..]), ReplacementRecordBytes(checksum[..]), 96)
  {
    if header.Length as U32 != 256 || decision.Length as U32 != 96 || checksum.Length as U32 != 32 {
      valid := false;
      return;
    }
    var index: U32 := 0;
    valid := true;
    while index < 96
      invariant index <= 96
      invariant valid == FoldReplacementDecision(ReplacementRecordBytes(header[..]),
        ReplacementRecordBytes(decision[..]), ReplacementRecordBytes(checksum[..]), index as int)
      decreases 96 - index
    {
      valid := valid && ReplacementDecisionByteValid(index, decision[index],
        header[if 8 <= index < 24 then index + 8 else 16], header[if 24 <= index < 56 then index + 88 else 112], checksum[if 64 <= index < 96 then index - 64 else 0]);
      index := index + 1;
    }
  }

  ghost function FoldReplacementHeader(header: seq<Replacement.RecordByte>,
      pathIdentity: seq<Replacement.RecordByte>, checksum: seq<Replacement.RecordByte>, count: nat): bool
    requires |header| == 256 && |pathIdentity| == 32 && |checksum| == 32 && count <= 256
    decreases count
  {
    if count == 0 then true else
      FoldReplacementHeader(header, pathIdentity, checksum, count - 1) &&
      ReplacementHeaderByteValid((count - 1) as U32, header[count - 1] as Byte,
        pathIdentity[if 32 <= count - 1 < 64 then count - 33 else 0] as Byte, checksum[if 224 <= count - 1 < 256 then count - 225 else 0] as Byte)
  }

  ghost function FoldReplacementGeneration(header: seq<Replacement.RecordByte>, count: nat): bool
    requires |header| == 256 && count <= 256
    decreases count
  {
    if count == 0 then false else
      ReplacementGenerationByte(FoldReplacementGeneration(header, count - 1),
        (count - 1) as U32, header[count - 1] as Byte)
  }

  lemma ReplacementHeaderPrefixIsExact(header: seq<Replacement.RecordByte>,
      pathIdentity: seq<Replacement.RecordByte>, checksum: seq<Replacement.RecordByte>, count: nat)
    requires |header| == 256 && |pathIdentity| == 32 && |checksum| == 32 && count <= 256
    ensures FoldReplacementHeader(header, pathIdentity, checksum, count) <==>
      (forall i | 0 <= i < count :: ReplacementHeaderByteValid(i as U32,
        header[i] as Byte, pathIdentity[if 32 <= i < 64 then i - 32 else 0] as Byte, checksum[if 224 <= i < 256 then i - 224 else 0] as Byte))
    ensures FoldReplacementGeneration(header, count) <==>
      (exists i | 16 <= i < 32 && i < count :: header[i] != 0)
    decreases count
  {
    if count != 0 {
      ReplacementHeaderPrefixIsExact(header, pathIdentity, checksum, count - 1);
      if FoldReplacementHeader(header, pathIdentity, checksum, count) {
        forall i | 0 <= i < count
          ensures ReplacementHeaderByteValid(i as U32, header[i] as Byte,
            pathIdentity[if 32 <= i < 64 then i - 32 else 0] as Byte, checksum[if 224 <= i < 256 then i - 224 else 0] as Byte)
        {
          if i != count - 1 { assert i < count - 1; }
        }
      }
    }
  }

  // The exact expected SHA bytes are a parameter; no cryptographic property is
  // assumed. Every actual field comparison, including the nonempty GUID byte
  // representation, is covered by this fold instead of an unproved boolean.
  lemma ReplacementHeaderChecksRefineBytes(header: seq<Replacement.RecordByte>,
      pathIdentity: seq<Replacement.RecordByte>, digest: seq<Replacement.RecordByte> -> seq<Replacement.RecordByte>)
    requires |header| == 256 && |pathIdentity| == 32 && |digest(header[..224])| == 32
    ensures (FoldReplacementHeader(header, pathIdentity, digest(header[..224]), 256) &&
      FoldReplacementGeneration(header, 256)) <==> Replacement.ValidHeaderFields(header, pathIdentity, digest)
  {
    hide FoldReplacementHeader, FoldReplacementGeneration;
    var checksum := digest(header[..224]);
    ReplacementHeaderPrefixIsExact(header, pathIdentity, checksum, 256);
    var constants := Replacement.HeaderMagic() + [1, 0, 0, 0, 0, 1, 0, 0];
    assert forall i | 0 <= i < 16 :: ReplacementHeaderFixedByte(i as U32) as int == constants[i];
    assert constants[..8] == Replacement.HeaderMagic();
    assert constants[8..12] == [1, 0, 0, 0];
    assert constants[12..16] == [0, 1, 0, 0];
    if FoldReplacementHeader(header, pathIdentity, checksum, 256) {
      assert forall i | 0 <= i < 16 :: header[i] == constants[i];
      assert header[..16] == constants;
      forall i | 0 <= i < 32
        ensures header[32 + i] == pathIdentity[i] && header[224 + i] == checksum[i]
      {
        assert ReplacementHeaderByteValid((32 + i) as U32, header[32 + i] as Byte,
          pathIdentity[i] as Byte, checksum[0] as Byte);
        assert ReplacementHeaderByteValid((224 + i) as U32, header[224 + i] as Byte,
          pathIdentity[0] as Byte, checksum[i] as Byte);
      }
      assert header[32..64] == pathIdentity;
      assert header[224..256] == checksum;
    }
    if Replacement.ValidHeaderFields(header, pathIdentity, digest) {
      forall i | 0 <= i < 256
        ensures ReplacementHeaderByteValid(i as U32, header[i] as Byte,
          pathIdentity[if 32 <= i < 64 then i - 32 else 0] as Byte, checksum[if 224 <= i < 256 then i - 224 else 0] as Byte)
      {
        if i < 16 {
          if i < 8 { assert header[i] == Replacement.HeaderMagic()[i]; }
          else if i < 12 { assert header[i] == [1, 0, 0, 0][i - 8]; }
          else { assert header[i] == [0, 1, 0, 0][i - 12]; }
          assert header[i] == constants[i];
        }
        else if 32 <= i < 64 { assert header[i] == pathIdentity[i - 32]; }
        else if 224 <= i { assert header[i] == checksum[i - 224]; }
      }
    }
  }

  ghost function FoldReplacementDecision(header: seq<Replacement.RecordByte>,
      decision: seq<Replacement.RecordByte>, checksum: seq<Replacement.RecordByte>, count: nat): bool
    requires |header| == 256 && |decision| == 96 && |checksum| == 32 && count <= 96
    decreases count
  {
    if count == 0 then true else
      FoldReplacementDecision(header, decision, checksum, count - 1) &&
      ReplacementDecisionByteValid((count - 1) as U32, decision[count - 1] as Byte,
        header[if 8 <= count - 1 < 24 then count + 7 else 16] as Byte,
        header[if 24 <= count - 1 < 56 then count + 87 else 112] as Byte, checksum[if 64 <= count - 1 < 96 then count - 65 else 0] as Byte)
  }

  lemma ReplacementDecisionPrefixIsExact(header: seq<Replacement.RecordByte>,
      decision: seq<Replacement.RecordByte>, checksum: seq<Replacement.RecordByte>, count: nat)
    requires |header| == 256 && |decision| == 96 && |checksum| == 32 && count <= 96
    ensures FoldReplacementDecision(header, decision, checksum, count) <==>
      (forall i | 0 <= i < count :: ReplacementDecisionByteValid(i as U32, decision[i] as Byte,
        header[if 8 <= i < 24 then i + 8 else 16] as Byte, header[if 24 <= i < 56 then i + 88 else 112] as Byte, checksum[if 64 <= i < 96 then i - 64 else 0] as Byte))
    decreases count
  {
    if count != 0 {
      ReplacementDecisionPrefixIsExact(header, decision, checksum, count - 1);
      if FoldReplacementDecision(header, decision, checksum, count) {
        forall i | 0 <= i < count
          ensures ReplacementDecisionByteValid(i as U32, decision[i] as Byte,
            header[if 8 <= i < 24 then i + 8 else 16] as Byte, header[if 24 <= i < 56 then i + 88 else 112] as Byte, checksum[if 64 <= i < 96 then i - 64 else 0] as Byte)
        {
          if i != count - 1 { assert i < count - 1; }
        }
      }
    }
  }

  lemma ReplacementDecisionChecksRefineBytes(header: seq<Replacement.RecordByte>,
      decision: seq<Replacement.RecordByte>, digest: seq<Replacement.RecordByte> -> seq<Replacement.RecordByte>)
    requires |header| == 256 && |decision| == 96 && |digest(decision[..64])| == 32
    ensures FoldReplacementDecision(header, decision, digest(decision[..64]), 96) <==>
      Replacement.ValidDecisionBytes(header, decision, digest)
  {
    hide FoldReplacementDecision;
    var checksum := digest(decision[..64]);
    ReplacementDecisionPrefixIsExact(header, decision, checksum, 96);
    assert forall i | 0 <= i < 8 :: ReplacementDecisionFixedByte(i as U32) as int == Replacement.DecisionMagic()[i];
    if FoldReplacementDecision(header, decision, checksum, 96) {
      assert forall i | 0 <= i < 8 :: decision[i] == Replacement.DecisionMagic()[i];
      assert decision[..8] == Replacement.DecisionMagic();
      forall i {:trigger decision[8 + i]} | 0 <= i < 16
        ensures decision[8 + i] == header[16 + i]
      {
        assert ReplacementDecisionByteValid((8 + i) as U32, decision[8 + i] as Byte,
          header[16 + i] as Byte,
          header[112] as Byte, checksum[0] as Byte);
      }
      forall i | 0 <= i < 32
        ensures decision[24 + i] == header[112 + i] && decision[64 + i] == checksum[i]
      {
        assert ReplacementDecisionByteValid((24 + i) as U32, decision[24 + i] as Byte,
          header[16] as Byte,
          header[112 + i] as Byte, checksum[0] as Byte);
        assert ReplacementDecisionByteValid((64 + i) as U32, decision[64 + i] as Byte,
          header[16] as Byte,
          header[112] as Byte, checksum[i] as Byte);
      }
      assert decision[8..24] == header[16..32];
      assert decision[24..56] == header[112..144];
      assert decision[64..96] == checksum;
    }
    if Replacement.ValidDecisionBytes(header, decision, digest) {
      forall i | 0 <= i < 96
        ensures ReplacementDecisionByteValid(i as U32, decision[i] as Byte,
          header[if 8 <= i < 24 then i + 8 else 16] as Byte, header[if 24 <= i < 56 then i + 88 else 112] as Byte, checksum[if 64 <= i < 96 then i - 64 else 0] as Byte)
      {
        if i < 8 { assert decision[i] == Replacement.DecisionMagic()[i]; }
        else if i < 24 { assert decision[i] == header[16 + i - 8]; }
        else if i < 56 {
          assert decision[24..56][i - 24] == header[112..144][i - 24];
          assert decision[i] == header[112 + i - 24];
        }
        else if 64 <= i { assert decision[i] == checksum[i - 64]; }
      }
    }
  }

  ghost function ScannedReplacementAdmission(bytes: seq<Replacement.RecordByte>,
      pathIdentity: seq<Replacement.RecordByte>, digest: seq<Replacement.RecordByte> -> seq<Replacement.RecordByte>): Byte
    requires 256 <= |bytes| <= 352 && |pathIdentity| == 32
    requires |digest(bytes[..224])| == 32
    requires |bytes| == 352 ==> |digest(bytes[256..320])| == 32
  {
    var header := bytes[..256];
    var headerValid := FoldReplacementHeader(header, pathIdentity, digest(bytes[..224]), 256) &&
      FoldReplacementGeneration(header, 256);
    var decisionValid := |bytes| == 352 && FoldReplacementDecision(header, bytes[256..], digest(bytes[256..320]), 96);
    ReplacementAdmission(|bytes| as U64, headerValid, Replacement.LittleEndian64(header[64..72]) as U64,
      Replacement.LittleEndian64(header[72..80]) as U64, decisionValid)
  }

  // Composes the generated array-loop contracts with bounded record selection.
  // The old assumption that a handwritten validity boolean matched all record
  // fields is discharged by these scans; SHA/path/read primitives remain the
  // explicit acquisition boundary, with no assumed cryptographic theorem.
  lemma ReplacementScansRefineRecordAdmission(bytes: seq<Replacement.RecordByte>,
      pathIdentity: seq<Replacement.RecordByte>, digest: seq<Replacement.RecordByte> -> seq<Replacement.RecordByte>)
    requires 256 <= |bytes| <= 352 && |pathIdentity| == 32
    requires |digest(bytes[..224])| == 32
    requires |bytes| == 352 ==> |digest(bytes[256..320])| == 32
    ensures ScannedReplacementAdmission(bytes, pathIdentity, digest) == 0 <==>
      Replacement.ReadRecordBytes(bytes, pathIdentity, digest) == Replacement.Refuse
    ensures ScannedReplacementAdmission(bytes, pathIdentity, digest) == 1 <==>
      Replacement.ReadRecordBytes(bytes, pathIdentity, digest) == Replacement.Admit(Replacement.Prepared)
    ensures ScannedReplacementAdmission(bytes, pathIdentity, digest) == 2 <==>
      Replacement.ReadRecordBytes(bytes, pathIdentity, digest) == Replacement.Admit(Replacement.Committed)
  {
    var header := bytes[..256];
    assert header[..224] == bytes[..224];
    ReplacementHeaderChecksRefineBytes(header, pathIdentity, digest);
    if |bytes| == 352 {
      assert bytes[256..][..64] == bytes[256..320];
      ReplacementDecisionChecksRefineBytes(header, bytes[256..], digest);
    }
    var headerValid := FoldReplacementHeader(header, pathIdentity, digest(bytes[..224]), 256) &&
      FoldReplacementGeneration(header, 256);
    var decisionValid := |bytes| == 352 && FoldReplacementDecision(header, bytes[256..], digest(bytes[256..320]), 96);
    // Bind each scanned field to the original record before converting its
    // bounded natural value to the native-width admission argument.
    assert header[64..72] == bytes[64..72];
    assert header[72..80] == bytes[72..80];
    var oldLength := Replacement.LittleEndian64(header[64..72]);
    var newLength := Replacement.LittleEndian64(header[72..80]);
    assert (oldLength as U64) as int == oldLength;
    assert (newLength as U64) as int == newLength;
    ReplacementAdmissionRefinesRecordBytes(bytes, pathIdentity, digest, headerValid,
      oldLength as U64, newLength as U64, decisionValid);
  }

  // Index entry starts are bits relative to the payload, least-significant
  // bit first. Constant divisors retain native arithmetic without runtime support.
  function IndexBitmapBit(value: Byte, bit: U32): bool {
    if bit == 0 then value % 2 == 1
    else if bit == 1 then value / 2 % 2 == 1
    else if bit == 2 then value / 4 % 2 == 1
    else if bit == 3 then value / 8 % 2 == 1
    else if bit == 4 then value / 16 % 2 == 1
    else if bit == 5 then value / 32 % 2 == 1
    else if bit == 6 then value / 64 % 2 == 1
    else bit == 7 && value / 128 == 1
  }

  ghost predicate IndexBoundaryAt(bytes: seq<Byte>, bitmap: U32, first: U32, position: int) {
    bitmap < first && first as int <= |bytes| && first as int <= position < |bytes| &&
      bitmap as int + (position - first as int) / 8 < first as int &&
      IndexBitmapBit(bytes[bitmap as int + (position - first as int) / 8],
        ((position - first as int) % 8) as U32)
  }

  ghost predicate IndexSearchValid(length: int, bitmap: U32, first: U32, payloadEnd: U32, current: U32) {
    bitmap < first <= current < payloadEnd && payloadEnd as int <= length <= 2147483647
  }

  method FindIndexEntryStart(page: array<Byte>, bitmap: U32, first: U32,
      payloadEnd: U32, current: U32) returns (next: U32)
    requires page.Length <= 2147483647
    ensures !IndexSearchValid(page.Length, bitmap, first, payloadEnd, current) ==> next == 4294967295
    ensures next != 4294967295 ==>
      current < next < payloadEnd && IndexBoundaryAt(page[..], bitmap, first, next as int) &&
      (forall p | current as int < p < next as int :: !IndexBoundaryAt(page[..], bitmap, first, p))
    ensures IndexSearchValid(page.Length, bitmap, first, payloadEnd, current) ==>
      (next == 4294967295 <==>
        (forall p | current as int < p < payloadEnd as int :: !IndexBoundaryAt(page[..], bitmap, first, p)))
  {
    next := 4294967295;
    if bitmap >= first || first > current || current >= payloadEnd || payloadEnd > page.Length as U32 {
      return;
    }
    var bit: U32 := current - first + 1;
    var end: U32 := payloadEnd - first;
    while bit < end && bitmap + bit / 8 < first
      invariant current - first + 1 <= bit <= end
      invariant forall p | current as int < p < first as int + bit as int ::
        !IndexBoundaryAt(page[..], bitmap, first, p)
      decreases end - bit
    {
      if IndexBitmapBit(page[bitmap + bit / 8], bit % 8) {
        next := first + bit;
        return;
      }
      bit := bit + 1;
    }
    forall p | current as int < p < payloadEnd as int
      ensures !IndexBoundaryAt(page[..], bitmap, first, p)
    {
      if p >= first as int + bit as int {
        assert bitmap as int + (p - first as int) / 8 >= first as int;
      }
    }
  }

  function IndexLeafBitValid(bit: U64, payloadLength: U64, isSet: bool): bool {
    (bit != 0 || !isSet) && (bit <= payloadLength || !isSet) &&
      (bit != payloadLength || isSet == (payloadLength > 0))
  }

  ghost predicate IndexLeafBitmapExact(bytes: seq<Byte>, bitmap: U32, first: U32, payloadEnd: U32) {
    bitmap < first <= payloadEnd && payloadEnd as int <= |bytes| <= 2147483647 &&
      payloadEnd as int - first as int < (first as int - bitmap as int) * 8 &&
      (forall b | 0 <= b < (first as int - bitmap as int) * 8 ::
        IndexLeafBitValid(b as U64, (payloadEnd - first) as U64,
          IndexBitmapBit(bytes[bitmap as int + b / 8], (b % 8) as U32)))
  }

  method CheckIndexLeafBitmap(page: array<Byte>, bitmap: U32, first: U32,
      payloadEnd: U32) returns (valid: bool)
    requires page.Length <= 2147483647
    ensures valid == IndexLeafBitmapExact(page[..], bitmap, first, payloadEnd)
  {
    valid := false;
    if bitmap >= first || first > payloadEnd || payloadEnd > page.Length as U32 {
      return;
    }
    var payloadLength: U64 := (payloadEnd - first) as U64;
    var bitmapLength: U64 := (first - bitmap) as U64 * 8;
    if payloadLength >= bitmapLength { return; }
    var bit: U64 := 0;
    while bit < bitmapLength
      invariant bit <= bitmapLength
      invariant forall b | 0 <= b < bit as int ::
        IndexLeafBitValid(b as U64, payloadLength,
          IndexBitmapBit(page[bitmap as int + b / 8], (b % 8) as U32))
      decreases bitmapLength - bit
    {
      var offset: U64 := bitmap as U64 + bit / 8;
      if !IndexLeafBitValid(bit, payloadLength,
          IndexBitmapBit(page[offset], (bit % 8) as U32)) {
        return;
      }
      bit := bit + 1;
    }
    valid := true;
  }
}
