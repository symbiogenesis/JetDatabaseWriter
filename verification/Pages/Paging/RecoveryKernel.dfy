// Executable recovery decisions. Generated C# calls these total native-width
// functions; ghost lemmas connect byte authorization to UndoJournal.Allowed.
// Remaining record-byte decoding, hashes, evidence identity, I/O and full loop correspondence remain
// outside this kernel. No external C# contract is trusted by these proofs.
module NativeRecoveryTypes {
  newtype {:nativeType "ulong"} U64 = x: int | 0 <= x < 18446744073709551616
  newtype {:nativeType "uint"} U32 = x: int | 0 <= x < 4294967296
  newtype {:nativeType "byte"} Byte = x: int | 0 <= x < 256
  newtype {:nativeType "int"} I32 = x: int | -2147483648 <= x < 2147483648
}

// Proof-only prefix bookkeeping. Production retains its acquired record bytes,
// hash operations and sparse dictionary; none of these datatypes is an
// executable-kernel parameter or a shipping runtime dependency.
module JournalAdmissionModel {
  import opened NativeRecoveryTypes
  import Undo = UndoJournal

  datatype State = State(minimum: U64, maximum: U64, recovering: bool,
    authorized: bool, committed: bool, captures: set<U64>, writes: set<U64>,
    controls: seq<Undo.JournalRecord>)
  datatype Outcome = Outcome(tag: U64, length: U64, finalRecord: bool,
    capturePresent: bool, tailCovered: bool, payloadValid: bool)
}

module RecoveryKernel {
  import opened NativeRecoveryTypes
  import Undo = UndoJournal
  import Replacement = ReplacementProtocol
  import Admission = JournalAdmissionModel

  // General Date formatting rounds managed DateTime ticks to whole seconds.
  // Native OLE serials have a separate, earlier ties-up projection; this
  // controller preserves the managed strictly-greater-than-half rule and
  // refuses a carry beyond DateTime's final whole second without overflow.
  function RoundGeneralDateTicks(ticks: U64): U64
    ensures RoundGeneralDateTicks(ticks) <= 3155378975999999999
    ensures RoundGeneralDateTicks(ticks) % 10000000 == 0
    ensures ticks > 3155378975999999999 ==> RoundGeneralDateTicks(ticks) == 0
    ensures ticks <= 3155378975999999999 ==>
      RoundGeneralDateTicks(ticks) ==
        (if ticks % 10000000 > 5000000 && ticks - ticks % 10000000 <= 3155378975989999999
         then ticks - ticks % 10000000 + 10000000
         else ticks - ticks % 10000000)
  {
    if ticks > 3155378975999999999 then 0 else
      var whole := ticks - ticks % 10000000;
      if ticks % 10000000 > 5000000 && whole <= 3155378975989999999
      then whole + 10000000 else whole
  }

  lemma GeneralDateRoundingIsIdempotent(ticks: U64)
    ensures RoundGeneralDateTicks(RoundGeneralDateTicks(ticks)) == RoundGeneralDateTicks(ticks)
  { }

  lemma GeneralDateHalfSecondRoundsDown(ticks: U64)
    requires ticks <= 3155378975999999999
    requires ticks % 10000000 <= 5000000
    ensures RoundGeneralDateTicks(ticks) == ticks - ticks % 10000000
  { }

  // Ordinary variable values are admitted intact. Unicode Text has both a
  // logical UTF-16 width and a physical encoded width; compression never
  // grants permission to discard characters or to exceed the descriptor.
  function VariableValueAdmitted(units: U32, encodedBytes: U32,
      maximumBytes: U32, unicodeText: bool): bool
    ensures VariableValueAdmitted(units, encodedBytes, maximumBytes, unicodeText) <==>
      (maximumBytes == 0 || encodedBytes <= maximumBytes) &&
      (!unicodeText || maximumBytes == 0 || units as int * 2 <= maximumBytes as int)
  {
    (maximumBytes == 0 || encodedBytes <= maximumBytes) &&
      (!unicodeText || maximumBytes == 0 || units <= maximumBytes / 2)
  }

  lemma AdmittedUnicodeWidthRetainsEveryUnit(units: U32, encodedBytes: U32, maximumBytes: U32)
    requires maximumBytes > 0
    requires VariableValueAdmitted(units, encodedBytes, maximumBytes, true)
    ensures units as int * 2 <= maximumBytes as int
    ensures encodedBytes <= maximumBytes
  { }

  lemma OversizedVariableValuesRefuse(units: U32, encodedBytes: U32,
      maximumBytes: U32, unicodeText: bool)
    requires maximumBytes > 0
    requires encodedBytes > maximumBytes || (unicodeText && units as int * 2 > maximumBytes as int)
    ensures !VariableValueAdmitted(units, encodedBytes, maximumBytes, unicodeText)
  { }

  // Zero is a readable row, one is a strict refusal and two is an explicitly
  // lenient omission. Layout/overflow acquisition remains the caller's effect.
  function RowReadDisposition(readable: bool, strict: bool): U32
    ensures RowReadDisposition(readable, strict) == 0 <==> readable
    ensures RowReadDisposition(readable, strict) == 1 <==> !readable && strict
    ensures RowReadDisposition(readable, strict) == 2 <==> !readable && !strict
  {
    if readable then 0 else if strict then 1 else 2
  }

  lemma StrictRowReadsNeverOmit(readable: bool)
    ensures RowReadDisposition(readable, true) != 2
  { }

  function NextOmittedRowCount(count: U64): U64
    ensures NextOmittedRowCount(count) <= 9223372036854775807
    ensures count < 9223372036854775807 ==>
      NextOmittedRowCount(count) == count + 1
    ensures count >= 9223372036854775807 ==>
      NextOmittedRowCount(count) == 9223372036854775807
  {
    if count < 9223372036854775807 then count + 1 else 9223372036854775807
  }

  function RowNullMaskStart(rowSize: U32, headerWidth: U32, columns: U32): U32
    ensures RowNullMaskStart(rowSize, headerWidth, columns) != 0 ==>
      headerWidth <= RowNullMaskStart(rowSize, headerWidth, columns) < rowSize
    ensures RowNullMaskStart(rowSize, headerWidth, columns) != 0 ==>
      (RowNullMaskStart(rowSize, headerWidth, columns) as int) + ((columns as int) + 7) / 8 == (rowSize as int)
  {
    if (headerWidth == 1 || headerWidth == 2) && 0 < columns <= 65535 &&
       rowSize <= 2147483647 && (columns + 7) / 8 <= rowSize &&
       headerWidth <= rowSize - (columns + 7) / 8
    then rowSize - (columns + 7) / 8 else 0
  }

  function RowVariableCountStart(nullStart: U32, headerWidth: U32, countWidth: U32): U32
    ensures RowVariableCountStart(nullStart, headerWidth, countWidth) != 0 ==>
      headerWidth <= RowVariableCountStart(nullStart, headerWidth, countWidth) < nullStart
    ensures RowVariableCountStart(nullStart, headerWidth, countWidth) != 0 ==>
      (RowVariableCountStart(nullStart, headerWidth, countWidth) as int) + (countWidth as int) == (nullStart as int)
  {
    if (headerWidth == 1 || headerWidth == 2) && (countWidth == 1 || countWidth == 2) &&
       countWidth <= nullStart && headerWidth <= nullStart - countWidth
    then nullStart - countWidth else 0
  }

  function RowVariableDataEnd(countStart: U32, headerWidth: U32,
      variables: U32, entryWidth: U32, jumps: U32, endWidth: U32): U32
    ensures RowVariableDataEnd(countStart, headerWidth, variables, entryWidth, jumps, endWidth) != 0 ==>
      headerWidth <= RowVariableDataEnd(countStart, headerWidth, variables, entryWidth, jumps, endWidth) < countStart
    ensures RowVariableDataEnd(countStart, headerWidth, variables, entryWidth, jumps, endWidth) != 0 ==>
      (RowVariableDataEnd(countStart, headerWidth, variables, entryWidth, jumps, endWidth) as int) +
        (jumps as int) + (variables as int) * (entryWidth as int) + (endWidth as int) == (countStart as int)
  {
    if !(headerWidth == 1 || headerWidth == 2) || !(entryWidth == 1 || entryWidth == 2) ||
       !(endWidth == 1 || endWidth == 2) || variables > 65535 then 0
    else
      var metadata: U64 := (jumps as U64) + (variables as U64) * (entryWidth as U64) + (endWidth as U64);
      if metadata <= (countStart as U64) && (headerWidth as U64) <= (countStart as U64) - metadata
      then ((countStart as U64) - metadata) as U32 else 0
  }

  // The adapter compares the encoded bound with the original operand in the
  // predicate's lossless comparison domain. Keep equality exact; a range may
  // broaden only away from matching keys, retaining the original predicate.
  function IsOutwardSeekBound(op: U32, comparison: I32): bool {
    if op == 0 then comparison == 0
    else if op == 2 || op == 3 then comparison <= 0
    else if op == 4 || op == 5 then comparison >= 0
    else false
  }

  ghost predicate MatchesSeekComparison(op: U32, key: real, bound: real) {
    if op == 0 then key == bound
    else if op == 2 then key > bound
    else if op == 3 then key >= bound
    else if op == 4 then key < bound
    else if op == 5 then key <= bound
    else false
  }

  lemma OutwardSeekBoundRetainsEveryMatch(op: U32, comparison: I32,
      original: real, encoded: real, key: real)
    requires (comparison < 0 <==> encoded < original)
    requires (comparison == 0 <==> encoded == original)
    requires (comparison > 0 <==> encoded > original)
    requires IsOutwardSeekBound(op, comparison)
    ensures MatchesSeekComparison(op, key, original) ==>
      MatchesSeekComparison(op, key, encoded)
  { }

  lemma InwardStrictRoundingCanLoseAMatch()
    ensures MatchesSeekComparison(2, 2.0, 1.5)
    ensures !MatchesSeekComparison(2, 2.0, 2.0)
    ensures !IsOutwardSeekBound(2, 1)
    ensures MatchesSeekComparison(4, -2.0, -1.5)
    ensures !MatchesSeekComparison(4, -2.0, -2.0)
    ensures !IsOutwardSeekBound(4, -1)
  { }

  // IEEE encodings distinguish the two zeros even though CLR predicates do
  // not. Use the lower zero for an inclusive lower bound and the upper zero
  // for an inclusive upper bound; equality needs both and must scan instead.
  function IsFloatingZeroSeekBound(op: U32, negative: bool): bool {
    op == 2 || op == 4 || (op == 3 && negative) || (op == 5 && !negative)
  }

  lemma FloatingZeroBoundRetainsEveryMatch(op: U32, negative: bool, keyRank: int)
    requires IsFloatingZeroSeekBound(op, negative)
    ensures MatchesSeekComparison(op,
      if keyRank < 0 then -1.0 else if keyRank <= 1 then 0.0 else 1.0, 0.0) ==>
      MatchesSeekComparison(op, keyRank as real, if negative then 0.0 else 1.0)
  { }

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
  {
    var admitted := ReplacementAdmission(|bytes| as U64, headerFieldsValid, oldLength, newLength, decisionValid);
    var record := Replacement.ReadRecordBytes(bytes, pathIdentity, digest);
    if |bytes| < 256 || |bytes| > 352 {
      assert admitted == 0 && record == Replacement.Refuse;
    } else {
      var header := bytes[..256];
      assert header[64..72] == bytes[64..72];
      assert header[72..80] == bytes[72..80];
      assert IsReplacementHeader(headerFieldsValid, oldLength, newLength) <==>
        Replacement.ValidHeaderBytes(header, pathIdentity, digest);
      if !IsReplacementHeader(headerFieldsValid, oldLength, newLength) {
        assert admitted == 0 && record == Replacement.Refuse;
      } else if |bytes| < 352 {
        assert admitted == 1 && record == Replacement.Admit(Replacement.Prepared);
      } else if decisionValid {
        assert admitted == 2 && record == Replacement.Admit(Replacement.Committed);
      } else {
        assert admitted == 0 && record == Replacement.Refuse;
      }
    }
  }

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

  function IsJournalAdmissionState(originalLength: U64, currentLength: U64,
      minimumLength: U64, maximumLength: U64, recovering: bool): bool {
    IsDatabaseLength(originalLength) && IsDatabaseLength(currentLength) &&
      IsDatabaseLength(maximumLength) && minimumLength <= originalLength <= maximumLength &&
      (!recovering || minimumLength < originalLength)
  }

  // Complete-record checks and exact hash/map effects are supplied by the
  // acquisition adapter. Only accepted outcomes can change prefix authority.
  function JournalAdmissionKind(pageSize: U32, originalLength: U64, currentLength: U64,
      minimumLength: U64, maximumLength: U64, recovering: bool, tag: U64,
      truncateLength: U64, finalRecord: bool, capturePresent: bool,
      tailCovered: bool, payloadValid: bool): Byte {
    if !IsPageSize(pageSize) ||
      !IsJournalAdmissionState(originalLength, currentLength, minimumLength, maximumLength, recovering)
    then 0
    else
      var kind := ClassifyJournalRecord(tag, pageSize, originalLength, recovering,
        minimumLength < originalLength, finalRecord);
      if kind == 1 && !payloadValid then 0
      else if kind == 3 && (!IsTruncateLength(truncateLength, pageSize, maximumLength) || !tailCovered) then 0
      else if kind == 4 && (capturePresent || !payloadValid) then 0
      else if kind == 5 && tag < originalLength && !capturePresent then 0
      else kind
  }

  method AdvanceJournalAdmission(pageSize: U32, originalLength: U64, currentLength: U64,
      minimumLength: U64, maximumLength: U64, recovering: bool, authorizedLength: bool,
      tag: U64, truncateLength: U64, finalRecord: bool, capturePresent: bool,
      tailCovered: bool, payloadValid: bool)
      returns (kind: Byte, nextMinimum: U64, nextMaximum: U64,
        nextRecovering: bool, nextAuthorized: bool)
    ensures kind == JournalAdmissionKind(pageSize, originalLength, currentLength,
      minimumLength, maximumLength, recovering, tag, truncateLength, finalRecord,
      capturePresent, tailCovered, payloadValid)
    ensures nextMinimum == (if kind == 3 && truncateLength < minimumLength then truncateLength else minimumLength)
    ensures nextMaximum == (if kind == 5 && tag + pageSize as U64 > maximumLength then tag + pageSize as U64 else maximumLength)
    ensures nextRecovering == (recovering || kind == 2)
    ensures nextAuthorized == (authorizedLength ||
      (kind == 3 && currentLength == truncateLength) ||
      (kind == 5 && IsAfterImageLength(currentLength, tag, pageSize)))
    ensures kind == 0 ==> (nextMinimum == minimumLength && nextMaximum == maximumLength &&
      nextRecovering == recovering && nextAuthorized == authorizedLength)
    ensures kind != 0 ==> (IsPageSize(pageSize) &&
      IsJournalAdmissionState(originalLength, currentLength, nextMinimum, nextMaximum, nextRecovering))
    ensures kind == 1 ==> payloadValid && finalRecord
    ensures kind == 3 ==> !recovering && tailCovered && IsTruncateLength(truncateLength, pageSize, maximumLength)
    ensures kind == 4 ==> !recovering && !capturePresent && payloadValid
    ensures kind == 5 ==> !recovering && (tag >= originalLength || capturePresent)
    ensures kind != 0 ==> (forall state: Admission.State |
      !state.committed && state.minimum == minimumLength && state.maximum == maximumLength &&
        state.recovering == recovering && state.authorized == authorizedLength ::
      var next := ApplyJournalAdmission(pageSize, originalLength, currentLength, state,
        Admission.Outcome(tag, truncateLength, finalRecord, capturePresent, tailCovered, payloadValid));
      next.minimum == nextMinimum && next.maximum == nextMaximum &&
        next.recovering == nextRecovering && next.authorized == nextAuthorized &&
        next.committed == (kind == 1))
  {
    nextMinimum, nextMaximum := minimumLength, maximumLength;
    nextRecovering, nextAuthorized := recovering, authorizedLength;
    kind := JournalAdmissionKind(pageSize, originalLength, currentLength,
      minimumLength, maximumLength, recovering, tag, truncateLength, finalRecord,
      capturePresent, tailCovered, payloadValid);
    if kind == 2 {
      nextRecovering := true;
    } else if kind == 3 {
      if truncateLength < minimumLength { nextMinimum := truncateLength; }
      nextAuthorized := authorizedLength || currentLength == truncateLength;
    } else if kind == 5 {
      if tag + pageSize as U64 > maximumLength { nextMaximum := tag + pageSize as U64; }
      nextAuthorized := authorizedLength || IsAfterImageLength(currentLength, tag, pageSize);
    }
  }

  function CompleteJournalAdmission(originalLength: U64, currentLength: U64,
      minimumLength: U64, maximumLength: U64, recovering: bool,
      authorizedLength: bool, committed: bool): bool {
    IsJournalAdmissionState(originalLength, currentLength, minimumLength, maximumLength, recovering) &&
      (committed || (minimumLength <= currentLength <= maximumLength && (authorizedLength || recovering)))
  }

  ghost function InitialJournalAdmission(originalLength: U64, currentLength: U64): Admission.State {
    Admission.State(originalLength, originalLength, false, currentLength >= originalLength,
      false, {}, {}, [])
  }

  ghost function PrefixAdmissionKind(pageSize: U32, originalLength: U64, currentLength: U64,
      state: Admission.State, outcome: Admission.Outcome): Byte {
    if state.committed then 0 else
      JournalAdmissionKind(pageSize, originalLength, currentLength, state.minimum, state.maximum,
        state.recovering, outcome.tag, outcome.length, outcome.finalRecord,
        outcome.capturePresent, outcome.tailCovered, outcome.payloadValid)
  }

  ghost predicate AdmissionEffectsMatchPrefix(pageSize: U32, originalLength: U64,
      state: Admission.State, outcome: Admission.Outcome) {
    (IsBeforeImageRecord(outcome.tag, pageSize, originalLength) ==>
      outcome.capturePresent == (outcome.tag - 9223372036854775808 in state.captures)) &&
    (IsPageOffset(outcome.tag, pageSize) ==>
      outcome.capturePresent == (outcome.tag in state.captures)) &&
    (outcome.tag == 18446744073709551614 ==>
      outcome.tailCovered ==
        (forall offset: U64 | IsPageOffset(offset, pageSize) && outcome.length <= offset < originalLength ::
          offset in state.captures))
  }

  ghost function ApplyJournalAdmission(pageSize: U32, originalLength: U64, currentLength: U64,
      state: Admission.State, outcome: Admission.Outcome): Admission.State
    requires PrefixAdmissionKind(pageSize, originalLength, currentLength, state, outcome) != 0
  {
    var kind := PrefixAdmissionKind(pageSize, originalLength, currentLength, state, outcome);
    Admission.State(
      if kind == 3 && outcome.length < state.minimum then outcome.length else state.minimum,
      if kind == 5 && outcome.tag + pageSize as U64 > state.maximum then outcome.tag + pageSize as U64 else state.maximum,
      state.recovering || kind == 2,
      state.authorized || (kind == 3 && currentLength == outcome.length) ||
        (kind == 5 && IsAfterImageLength(currentLength, outcome.tag, pageSize)),
      kind == 1,
      if kind == 4 then state.captures + {outcome.tag - 9223372036854775808} else state.captures,
      if kind == 5 then state.writes + {outcome.tag} else state.writes,
      if kind == 3 then state.controls + [Undo.LengthIntent(outcome.length as int)]
      else if kind == 2 then state.controls + [Undo.RecoveryRecord] else state.controls)
  }

  ghost predicate JournalAdmissionInvariant(pageSize: U32, originalLength: U64,
      currentLength: U64, state: Admission.State) {
    IsPageSize(pageSize) &&
    IsJournalAdmissionState(originalLength, currentLength, state.minimum, state.maximum, state.recovering) &&
    state.minimum as int == Undo.RecordedMinimum(originalLength as int, state.controls) &&
    state.recovering == (Undo.RecoveryRecord in state.controls) &&
    (forall offset | offset in state.captures :: IsPageOffset(offset, pageSize) && offset < originalLength) &&
    (forall offset: U64 | IsPageOffset(offset, pageSize) && state.minimum <= offset < originalLength ::
      offset in state.captures) &&
    (forall offset | offset in state.writes :: IsPageOffset(offset, pageSize)) &&
    (forall offset | offset in state.writes :: offset + pageSize as U64 <= state.maximum) &&
    (state.maximum == originalLength ||
      (exists offset | offset in state.writes :: state.maximum == offset + pageSize as U64)) &&
    state.authorized == (currentLength >= originalLength ||
      (exists index | 0 <= index < |state.controls| ::
        state.controls[index].LengthIntent? && state.controls[index].length == currentLength as int) ||
      (exists offset | offset in state.writes :: IsAfterImageLength(currentLength, offset, pageSize)))
  }

  lemma InitialJournalAdmissionHasNoMutationAuthority(pageSize: U32, originalLength: U64, currentLength: U64)
    requires IsPageSize(pageSize) && IsDatabaseLength(originalLength) && IsDatabaseLength(currentLength)
    ensures JournalAdmissionInvariant(pageSize, originalLength, currentLength,
      InitialJournalAdmission(originalLength, currentLength))
  { }

  lemma JournalAdmissionStepPreservesPrefix(pageSize: U32, originalLength: U64, currentLength: U64,
      state: Admission.State, outcome: Admission.Outcome)
    requires JournalAdmissionInvariant(pageSize, originalLength, currentLength, state)
    requires AdmissionEffectsMatchPrefix(pageSize, originalLength, state, outcome)
    requires PrefixAdmissionKind(pageSize, originalLength, currentLength, state, outcome) != 0
    ensures JournalAdmissionInvariant(pageSize, originalLength, currentLength,
      ApplyJournalAdmission(pageSize, originalLength, currentLength, state, outcome))
    ensures state.recovering ==> PrefixAdmissionKind(pageSize, originalLength, currentLength, state, outcome) <= 2
    ensures PrefixAdmissionKind(pageSize, originalLength, currentLength, state, outcome) == 1 ==>
      outcome.finalRecord && outcome.payloadValid
    ensures PrefixAdmissionKind(pageSize, originalLength, currentLength, state, outcome) == 4 ==>
      !outcome.capturePresent && outcome.payloadValid
  {
    var kind := PrefixAdmissionKind(pageSize, originalLength, currentLength, state, outcome);
    var next := ApplyJournalAdmission(pageSize, originalLength, currentLength, state, outcome);
    if kind == 4 {
      BeforeImageRecordsAuthorizeOnlyOriginalAlignedPages(outcome.tag, pageSize, originalLength);
    }
    if kind == 3 {
      assert next.controls[..|next.controls|-1] == state.controls;
      assert next.controls[|next.controls|-1] == Undo.LengthIntent(outcome.length as int);
    } else if kind == 2 {
      assert next.controls[..|next.controls|-1] == state.controls;
      assert next.controls[|next.controls|-1] == Undo.RecoveryRecord;
    }
    forall offset: U64 | IsPageOffset(offset, pageSize) && next.minimum <= offset < originalLength
      ensures offset in next.captures
    {
      if kind == 3 && outcome.length < state.minimum {
        assert outcome.tailCovered;
      } else {
        assert state.minimum <= offset;
      }
    }
    if kind == 5 {
      PageOffsetAuthorizesOnlyCompleteBoundedPages(outcome.tag, pageSize);
      forall offset | offset in next.writes
        ensures offset + pageSize as U64 <= next.maximum
      {
        if offset != outcome.tag {
          assert offset in state.writes;
          assert offset + pageSize as U64 <= state.maximum <= next.maximum;
        }
      }
      if next.maximum != originalLength {
        if next.maximum == outcome.tag + pageSize as U64 {
          assert outcome.tag in next.writes;
        } else {
          assert next.maximum == state.maximum;
          var offset :| offset in state.writes && state.maximum == offset + pageSize as U64;
          assert offset in next.writes;
        }
      }
    }
    if kind == 1 { return {:isolate}; }
    if kind == 2 { return {:isolate}; }
    if kind == 3 { return {:isolate}; }
    if kind == 4 { return {:isolate}; }
    assert kind == 5;
  }

  ghost predicate JournalAdmissionPath(pageSize: U32, originalLength: U64, currentLength: U64,
      states: seq<Admission.State>, outcomes: seq<Admission.Outcome>) {
    |states| == |outcomes| + 1 &&
      (forall index | 0 <= index < |outcomes| ::
        AdmissionEffectsMatchPrefix(pageSize, originalLength, states[index], outcomes[index]) &&
        PrefixAdmissionKind(pageSize, originalLength, currentLength, states[index], outcomes[index]) != 0 &&
        states[index+1] == ApplyJournalAdmission(pageSize, originalLength, currentLength, states[index], outcomes[index]))
  }

  // The induction composes the executing native admission fold with exact
  // prefix-map/tail outcomes. It does not establish those outcomes from raw
  // record reads or prove cryptographic identity and physical I/O effects.
  lemma JournalAdmissionTracePreservesPrefix(pageSize: U32, originalLength: U64, currentLength: U64,
      states: seq<Admission.State>, outcomes: seq<Admission.Outcome>)
    requires JournalAdmissionPath(pageSize, originalLength, currentLength, states, outcomes)
    requires JournalAdmissionInvariant(pageSize, originalLength, currentLength, states[0])
    ensures JournalAdmissionInvariant(pageSize, originalLength, currentLength, states[|states|-1])
    ensures states[|states|-1].committed ==> (states[0].committed || (|outcomes| > 0 &&
      outcomes[|outcomes|-1].finalRecord && outcomes[|outcomes|-1].payloadValid))
    decreases |outcomes|
  {
    if |outcomes| > 0 {
      JournalAdmissionStepPreservesPrefix(pageSize, originalLength, currentLength, states[0], outcomes[0]);
      JournalAdmissionTracePreservesPrefix(pageSize, originalLength, currentLength, states[1..], outcomes[1..]);
      if |outcomes| == 1 {
        assert states[1].committed ==> PrefixAdmissionKind(pageSize, originalLength, currentLength, states[0], outcomes[0]) == 1;
      }
    }
  }

  lemma JournalAdmissionAcceptsUnchangedEmptyJournal(pageSize: U32, originalLength: U64)
    requires IsPageSize(pageSize) && IsDatabaseLength(originalLength)
    ensures JournalAdmissionInvariant(pageSize, originalLength, originalLength,
      InitialJournalAdmission(originalLength, originalLength))
    ensures CompleteJournalAdmission(originalLength, originalLength, originalLength, originalLength,
      false, true, false)
    ensures InitialJournalAdmission(originalLength, originalLength).captures == {}
  {
    InitialJournalAdmissionHasNoMutationAuthority(pageSize, originalLength, originalLength);
  }

  lemma CompleteJournalAdmissionHasRecordedLengthAuthority(pageSize: U32,
      originalLength: U64, currentLength: U64, state: Admission.State)
    requires JournalAdmissionInvariant(pageSize, originalLength, currentLength, state)
    requires CompleteJournalAdmission(originalLength, currentLength, state.minimum, state.maximum,
      state.recovering, state.authorized, state.committed)
    requires !state.committed
    ensures Undo.RecordedMinimum(originalLength as int, state.controls) <= currentLength as int <= state.maximum as int
    ensures currentLength >= originalLength || Undo.RecoveryRecord in state.controls ||
      (exists index | 0 <= index < |state.controls| ::
        state.controls[index].LengthIntent? && state.controls[index].length == currentLength as int) ||
      (exists offset | offset in state.writes :: IsAfterImageLength(currentLength, offset, pageSize))
    ensures forall offset: U64 | IsPageOffset(offset, pageSize) && state.minimum <= offset < originalLength ::
      offset in state.captures
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

  // The page-content controller keeps the same bounded scratch array across
  // records. Input byte arrays are stable during each call; only the admitted
  // prefix of authorization scratch changes. No allocation executes here.
  ghost predicate JournalAuthorizationBounds(currentLength: int, originalLength: int,
      validLength: int, count: U32, offset: U64) {
    count as int <= currentLength && count as int <= originalLength && count as int <= validLength &&
      (count == 0 || offset as int + count as int - 1 < 18446744073709551616)
  }

  method InitializeJournalAuthorization(current: array<Byte>, original: array<Byte>, valid: array<bool>,
      count: U32, recovering: bool, offset: U64, minimumLength: U64) returns (state: Byte)
    requires current.Length <= 2147483647 && original.Length <= 2147483647 && valid.Length <= 2147483647
    modifies valid
    ensures state <= 2
    ensures state == 0 <==> !JournalAuthorizationBounds(current.Length, original.Length, valid.Length, count, offset)
    ensures state == 0 ==> valid[..] == old(valid[..])
    ensures state != 0 ==> forall index: nat :: index < count as int ==>
      valid[index] == OriginalOrZeroByte(recovering, offset + index as U64, minimumLength, current[index], original[index])
    ensures state != 0 ==> (state == 2 <==> forall index: nat :: index < count as int ==> valid[index])
    ensures forall index: nat :: count as int <= index < valid.Length ==> valid[index] == old(valid[index])
  {
    state := 0;
    if count > current.Length as U32 || count > original.Length as U32 || count > valid.Length as U32 ||
        (count > 0 && (count - 1) as U64 > 18446744073709551615 - offset) { return; }
    var index: U32 := 0;
    var matches := true;
    while index < count
      invariant index <= count
      invariant forall prior: nat :: prior < index as int ==>
        valid[prior] == OriginalOrZeroByte(recovering, offset + prior as U64, minimumLength, current[prior], original[prior])
      invariant matches <==> forall prior: nat :: prior < index as int ==> valid[prior]
      invariant forall later: nat :: index as int <= later < valid.Length ==> valid[later] == old(valid[later])
      decreases count - index
    {
      valid[index] := OriginalOrZeroByte(recovering, offset + index as U64, minimumLength, current[index], original[index]);
      matches := matches && valid[index];
      index := index + 1;
    }
    state := if matches then 2 else 1;
  }

  method IncludeJournalAfterImage(current: array<Byte>, record: array<Byte>, valid: array<bool>, count: U32)
      returns (admitted: bool)
    requires current.Length <= 2147483647 && record.Length <= 2147483647 && valid.Length <= 2147483647
    modifies valid
    ensures admitted <==> count as int <= current.Length && count as int <= valid.Length && count as int + 8 <= record.Length
    ensures !admitted ==> valid[..] == old(valid[..])
    ensures admitted ==> forall index: nat :: index < count as int ==>
      valid[index] == IncludeAfterImageByte(old(valid[index]), current[index], record[8 + index])
    ensures forall index: nat :: count as int <= index < valid.Length ==> valid[index] == old(valid[index])
  {
    admitted := false;
    if count > current.Length as U32 || count > valid.Length as U32 || record.Length as U32 < 8 ||
        count > record.Length as U32 - 8 { return; }
    var index: U32 := 0;
    while index < count
      invariant index <= count
      invariant forall prior: nat :: prior < index as int ==>
        valid[prior] == IncludeAfterImageByte(old(valid[prior]), current[prior], record[8 + prior])
      invariant forall later: nat :: index as int <= later < valid.Length ==> valid[later] == old(valid[later])
      decreases count - index
    {
      valid[index] := IncludeAfterImageByte(valid[index], current[index], record[8 + index]);
      index := index + 1;
    }
    admitted := true;
  }

  method CheckJournalAuthorization(valid: array<bool>, count: U32) returns (admitted: bool)
    requires valid.Length <= 2147483647
    ensures admitted <==> count as int <= valid.Length && forall index: nat :: index < count as int ==> valid[index]
  {
    admitted := false;
    if count > valid.Length as U32 { return; }
    var index: U32 := 0;
    while index < count
      invariant index <= count
      invariant forall prior: nat :: prior < index as int ==> valid[prior]
      decreases count - index
    {
      if !valid[index] { return; }
      index := index + 1;
    }
    admitted := true;
  }

  method CheckJournalZeroExtension(current: array<Byte>, start: U32, count: U32) returns (admitted: bool)
    requires current.Length <= 2147483647
    ensures admitted <==> (start <= count && count as int <= current.Length &&
      forall index: nat :: start as int <= index < count as int ==> current[index] == 0)
  {
    admitted := false;
    if start > count || count > current.Length as U32 { return; }
    var index := start;
    while index < count
      invariant start <= index <= count
      invariant forall prior: nat :: start as int <= prior < index as int ==> current[prior] == 0
      decreases count - index
    {
      if current[index] != 0 { return; }
      index := index + 1;
    }
    admitted := true;
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
        assert current < next < payloadEnd <= 2147483647;
        assert IndexBoundaryAt(page[..], bitmap, first, next as int);
        assert !(forall p | current as int < p < payloadEnd as int ::
          !IndexBoundaryAt(page[..], bitmap, first, p));
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

  // A position is an absolute bit index. The refusal sentinel lies outside
  // every managed array, including pooled buffers with extra trailing bytes.
  function UsageBitmapPosition(length: U32, start: U32, count: U32, relative: U64): U64 {
    if length > 2147483647 || start > length || count > length - start ||
      relative >= (count as U64) * 8
    then 18446744073709551615
    else (start as U64) * 8 + relative
  }

  lemma UsageBitmapPositionIsExact(length: U32, start: U32, count: U32, relative: U64)
    ensures UsageBitmapPosition(length, start, count, relative) != 18446744073709551615 <==>
      length <= 2147483647 && start <= length && count <= length - start && relative as int < count as int * 8
    ensures UsageBitmapPosition(length, start, count, relative) != 18446744073709551615 ==>
      UsageBitmapPosition(length, start, count, relative) as int == start as int * 8 + relative as int &&
      start as int <= UsageBitmapPosition(length, start, count, relative) as int / 8 < start as int + count as int <= length as int
  { }

  ghost function InlineUsagePositionSpec(bytes: seq<Byte>, start: U32, size: U32, page: U64): U64 {
    if |bytes| > 2147483647 || start as int > |bytes| || size <= 5 || size as int > |bytes| - start as int
    then 18446744073709551615
    else
      var base := bytes[start + 1] as U64 + bytes[start + 2] as U64 * 256 +
        bytes[start + 3] as U64 * 65536 + bytes[start + 4] as U64 * 16777216;
      if base > 2147483647 || page < base then 18446744073709551615
      else UsageBitmapPosition(|bytes| as U32, start + 5, size - 5, page - base)
  }

  method InlineUsagePosition(bytes: array<Byte>, start: U32, size: U32, page: U64) returns (position: U64)
    requires bytes.Length <= 2147483647
    ensures position == InlineUsagePositionSpec(bytes[..], start, size, page)
    ensures position != 18446744073709551615 ==> start as int + 5 <= position as int / 8 < start as int + size as int <= bytes.Length
  {
    position := 18446744073709551615;
    var length := bytes.Length as U32;
    if start > length || size <= 5 || size > length - start { return; }
    var base := bytes[start + 1] as U64 + bytes[start + 2] as U64 * 256 +
      bytes[start + 3] as U64 * 65536 + bytes[start + 4] as U64 * 16777216;
    if base > 2147483647 || page < base { return; }
    UsageBitmapPositionIsExact(length, start + 5, size - 5, page - base);
    position := UsageBitmapPosition(length, start + 5, size - 5, page - base);
  }

  function ReferenceUsagePosition(length: U32, pageSize: U32, page: U64): U64 {
    if length > 2147483647 || pageSize <= 4 || pageSize > length
    then 18446744073709551615
    else UsageBitmapPosition(length, 4, pageSize - 4, page % ((pageSize as U64 - 4) * 8))
  }

  lemma ReferenceUsagePositionIsExact(length: U32, pageSize: U32, page: U64)
    ensures ReferenceUsagePosition(length, pageSize, page) != 18446744073709551615 <==>
      length <= 2147483647 && 4 < pageSize <= length
    ensures ReferenceUsagePosition(length, pageSize, page) != 18446744073709551615 ==>
      4 <= ReferenceUsagePosition(length, pageSize, page) as int / 8 < pageSize as int &&
      ReferenceUsagePosition(length, pageSize, page) as int == 32 + page as int % ((pageSize as int - 4) * 8)
  {
    if length <= 2147483647 && 4 < pageSize <= length {
      UsageBitmapPositionIsExact(length, 4, pageSize - 4, page % ((pageSize as U64 - 4) * 8));
    }
  }

  function UsageBitMask(bit: U32): U32 {
    if bit == 0 then 1 else if bit == 1 then 2 else if bit == 2 then 4 else if bit == 3 then 8
    else if bit == 4 then 16 else if bit == 5 then 32 else if bit == 6 then 64 else 128
  }

  function UsageByteMarked(value: Byte, bit: U32): bool {
    (value as U32 / UsageBitMask(bit)) % 2 == 1
  }

  function UpdatedUsageByte(value: Byte, bit: U32, marked: bool): Byte {
    var mask := UsageBitMask(bit);
    (value as U32 / (mask * 2) * (mask * 2) + (if marked then mask else 0) + value as U32 % mask) as Byte
  }

  lemma UpdatedUsageByteIsExact(value: Byte, bit: U32, marked: bool)
    ensures UsageByteMarked(UpdatedUsageByte(value, bit, marked), bit) == marked
    ensures UpdatedUsageByte(value, bit, marked) as U32 % UsageBitMask(bit) == value as U32 % UsageBitMask(bit)
    ensures UpdatedUsageByte(value, bit, marked) as U32 / (UsageBitMask(bit) * 2) == value as U32 / (UsageBitMask(bit) * 2)
  { }

  method ReadUsageBit(bytes: array<Byte>, position: U64) returns (state: Byte)
    requires bytes.Length <= 2147483647
    ensures state <= 2
    ensures state == 0 <==> position as int >= bytes.Length * 8
    ensures state != 0 ==> (state == 2 <==> UsageByteMarked(bytes[position / 8], (position % 8) as U32))
  {
    state := 0;
    if position >= (bytes.Length as U64) * 8 { return; }
    state := if UsageByteMarked(bytes[position / 8], (position % 8) as U32) then 2 else 1;
  }

  method WriteUsageBit(bytes: array<Byte>, position: U64, marked: bool) returns (valid: bool)
    requires bytes.Length <= 2147483647
    modifies bytes
    ensures valid <==> position as int < bytes.Length * 8
    ensures !valid ==> bytes[..] == old(bytes[..])
    ensures valid ==> UsageByteMarked(bytes[position / 8], (position % 8) as U32) == marked
    ensures forall index: nat :: index < bytes.Length && index != position as int / 8 ==> bytes[index] == old(bytes[index])
    ensures valid ==> (bytes[position / 8] as U32 % UsageBitMask((position % 8) as U32) ==
      old(bytes[position / 8]) as U32 % UsageBitMask((position % 8) as U32))
    ensures valid ==> (bytes[position / 8] as U32 / (UsageBitMask((position % 8) as U32) * 2) ==
      old(bytes[position / 8]) as U32 / (UsageBitMask((position % 8) as U32) * 2))
  {
    valid := false;
    if position >= (bytes.Length as U64) * 8 { return; }
    var index := (position / 8) as U32;
    var bit := (position % 8) as U32;
    UpdatedUsageByteIsExact(bytes[index], bit, marked);
    bytes[index] := UpdatedUsageByte(bytes[index], bit, marked);
    valid := true;
  }

  // Raw data-page slots include deleted overflow targets. The full logical
  // page must be acquired before interpreting its bounded directory.
  ghost predicate DataRowLayout(bytes: seq<Byte>, pageSize: U32, countOffset: U32, rowsStart: U32) {
    2 <= pageSize as int <= |bytes| <= 2147483647 && countOffset <= pageSize - 2 && rowsStart <= pageSize
  }

  ghost function DataRowUInt16(bytes: seq<Byte>, position: nat): nat
    requires position + 2 <= |bytes|
  {
    bytes[position] as int + 256 * bytes[position + 1] as int
  }

  ghost function DataRowCount(bytes: seq<Byte>, pageSize: U32, countOffset: U32, rowsStart: U32): nat
    requires DataRowLayout(bytes, pageSize, countOffset, rowsStart)
  {
    var declared := DataRowUInt16(bytes, countOffset as int);
    var capacity := (pageSize as int - rowsStart as int) / 2;
    if declared < capacity then declared else capacity
  }

  ghost function DataRowOffset(bytes: seq<Byte>, rowsStart: U32, index: nat): nat
    requires rowsStart as int + index * 2 + 2 <= |bytes|
  {
    DataRowUInt16(bytes, rowsStart as int + index * 2) % 8192
  }

  ghost predicate DataRowSlotAdmitted(bytes: seq<Byte>, pageSize: U32, countOffset: U32,
      rowsStart: U32, rowIndex: U32) {
    DataRowLayout(bytes, pageSize, countOffset, rowsStart) &&
    rowIndex as int < DataRowCount(bytes, pageSize, countOffset, rowsStart) &&
    rowsStart as int + 2 * DataRowCount(bytes, pageSize, countOffset, rowsStart) <=
      DataRowOffset(bytes, rowsStart, rowIndex as int) < pageSize as int
  }

  method ReadDataRowSlot(page: array<Byte>, pageSize: U32, countOffset: U32,
      rowsStart: U32, rowIndex: U32) returns (start: U32, end: U32)
    requires page.Length <= 2147483647
    ensures end != 0 <==> DataRowSlotAdmitted(page[..], pageSize, countOffset, rowsStart, rowIndex)
    ensures end == 0 ==> start == 0
    ensures end != 0 ==> (start as int == DataRowOffset(page[..], rowsStart, rowIndex as int) &&
      rowsStart as int + 2 * DataRowCount(page[..], pageSize, countOffset, rowsStart) <= start as int < end as int <= pageSize as int)
    ensures end != 0 ==> forall index: int | 0 <= index < DataRowCount(page[..], pageSize, countOffset, rowsStart) ::
      DataRowOffset(page[..], rowsStart, index) > start as int ==> end as int <= DataRowOffset(page[..], rowsStart, index)
    ensures end != 0 && end < pageSize ==> exists index: int ::
      0 <= index < DataRowCount(page[..], pageSize, countOffset, rowsStart) &&
      DataRowOffset(page[..], rowsStart, index) == end as int
  {
    start, end := 0, 0;
    if pageSize < 2 || pageSize > page.Length as U32 || countOffset > pageSize - 2 || rowsStart > pageSize {
      return;
    }
    var declared: U32 := page[countOffset] as U32 + 256 * page[countOffset + 1] as U32;
    var capacity: U32 := (pageSize - rowsStart) / 2;
    var count: U32 := if declared < capacity then declared else capacity;
    assert count as int == DataRowCount(page[..], pageSize, countOffset, rowsStart);
    if rowIndex >= count { return; }
    var position: U32 := rowsStart + rowIndex * 2;
    var offset: U32 := (page[position] as U32 + 256 * page[position + 1] as U32) % 8192;
    if offset < rowsStart + count * 2 || offset >= pageSize { return; }
    start, end := offset, pageSize;
    var index: U32 := 0;
    while index < count
      invariant index <= count
      invariant start as int == DataRowOffset(page[..], rowsStart, rowIndex as int)
      invariant rowsStart + count * 2 <= start < end <= pageSize
      invariant forall scanned: int | 0 <= scanned < index as int ::
        DataRowOffset(page[..], rowsStart, scanned) > start as int ==> end as int <= DataRowOffset(page[..], rowsStart, scanned)
      invariant end < pageSize ==> exists scanned: int ::
        0 <= scanned < index as int && DataRowOffset(page[..], rowsStart, scanned) == end as int
      decreases count - index
    {
      var slot: U32 := rowsStart + index * 2;
      var candidate: U32 := (page[slot] as U32 + 256 * page[slot + 1] as U32) % 8192;
      assert candidate as int == DataRowOffset(page[..], rowsStart, index as int);
      if candidate > start && candidate < end {
        end := candidate;
      }
      index := index + 1;
    }
  }

  // Intermediate-index descent keeps the platform's vectorized lexicographic
  // comparison as an explicit effect. These operations acquire one complete
  // compressed entry and consume its comparison sign, including exact child
  // bytes and the strictly increasing next-entry continuation.
  ghost predicate IndexChildLayout(bytes: seq<Byte>, bitmap: U32, first: U32,
      payloadEnd: U32, start: U32, prefix: U32, isFirst: bool) {
    bitmap < first <= start < payloadEnd && payloadEnd as int <= |bytes| <= 2147483647 &&
      prefix as int <= |bytes| - first as int && (isFirst ==> start == first)
  }

  function IsIndexChildSearch(length: U32, bitmap: U32, first: U32, payloadEnd: U32, prefix: U32): bool {
    length <= 2147483647 && bitmap < first < payloadEnd <= length && prefix <= length - first
  }

  lemma IndexChildSearchAdmission(bytes: seq<Byte>, bitmap: U32, first: U32, payloadEnd: U32, prefix: U32)
    requires |bytes| <= 2147483647
    ensures IsIndexChildSearch(|bytes| as U32, bitmap, first, payloadEnd, prefix) <==>
      IndexChildLayout(bytes, bitmap, first, payloadEnd, first, prefix, true)
  { }

  ghost function IndexChildBoundary(bytes: seq<Byte>, bitmap: U32, first: U32,
      payloadEnd: U32, candidate: nat): nat
    requires bitmap < first && first as int <= candidate <= payloadEnd as int <= |bytes| <= 2147483647
    ensures candidate <= IndexChildBoundary(bytes, bitmap, first, payloadEnd, candidate) <= payloadEnd as int
    decreases payloadEnd as int - candidate
  {
    if candidate == payloadEnd as int || IndexBoundaryAt(bytes, bitmap, first, candidate)
    then candidate
    else IndexChildBoundary(bytes, bitmap, first, payloadEnd, candidate + 1)
  }

  lemma IndexChildBoundaryExact(bytes: seq<Byte>, bitmap: U32, first: U32,
      payloadEnd: U32, candidate: nat)
    requires bitmap < first && first as int <= candidate <= payloadEnd as int <= |bytes| <= 2147483647
    ensures candidate <= IndexChildBoundary(bytes, bitmap, first, payloadEnd, candidate) <= payloadEnd as int
    ensures IndexChildBoundary(bytes, bitmap, first, payloadEnd, candidate) < payloadEnd as int ==>
      IndexBoundaryAt(bytes, bitmap, first, IndexChildBoundary(bytes, bitmap, first, payloadEnd, candidate))
    ensures forall p | candidate <= p < IndexChildBoundary(bytes, bitmap, first, payloadEnd, candidate) ::
      !IndexBoundaryAt(bytes, bitmap, first, p)
    decreases payloadEnd as int - candidate
  {
    if candidate < payloadEnd as int && !IndexBoundaryAt(bytes, bitmap, first, candidate) {
      IndexChildBoundaryExact(bytes, bitmap, first, payloadEnd, candidate + 1);
    }
  }

  ghost predicate IndexChildEntry(bytes: seq<Byte>, first: U32, payloadEnd: U32,
      start: U32, end: U32, prefix: U32, isFirst: bool) {
    first <= start < end <= payloadEnd && payloadEnd as int <= |bytes| <= 2147483647 &&
      prefix as int <= |bytes| - first as int &&
      end as int - start as int + (if isFirst then 0 else prefix as int) >= 8 &&
      (isFirst ==> start == first && prefix <= end - start)
  }

  ghost function IndexChildAcquisition(bytes: seq<Byte>, bitmap: U32, first: U32,
      payloadEnd: U32, start: U32, prefix: U32, isFirst: bool): U32
    requires IndexChildLayout(bytes, bitmap, first, payloadEnd, start, prefix, isFirst)
    ensures IndexChildAcquisition(bytes, bitmap, first, payloadEnd, start, prefix, isFirst) != 4294967295 ==>
      IndexChildEntry(bytes, first, payloadEnd, start,
        IndexChildAcquisition(bytes, bitmap, first, payloadEnd, start, prefix, isFirst), prefix, isFirst)
  {
    var boundary := IndexChildBoundary(bytes, bitmap, first, payloadEnd, start as int + 1) as U32;
    if IndexChildEntry(bytes, first, payloadEnd, start, boundary, prefix, isFirst) then boundary else 4294967295
  }

  method ReadIndexChildEntry(page: array<Byte>, bitmap: U32, first: U32,
      payloadEnd: U32, start: U32, prefix: U32, isFirst: bool) returns (end: U32)
    requires IndexChildLayout(page[..], bitmap, first, payloadEnd, start, prefix, isFirst)
    ensures end == IndexChildAcquisition(page[..], bitmap, first, payloadEnd, start, prefix, isFirst)
    ensures end != 4294967295 ==>
      IndexChildEntry(page[..], first, payloadEnd, start, end, prefix, isFirst)
    ensures (var boundary := IndexChildBoundary(page[..], bitmap, first, payloadEnd, start as int + 1);
       end == (if IndexChildEntry(page[..], first, payloadEnd, start, boundary as U32, prefix, isFirst)
         then boundary as U32 else 4294967295))
  {
    end := 4294967295;
    var next := FindIndexEntryStart(page, bitmap, first, payloadEnd, start);
    var boundary: U32 := if next == 4294967295 then payloadEnd else next;
    IndexChildBoundaryExact(page[..], bitmap, first, payloadEnd, start as int + 1);
    ghost var exact := IndexChildBoundary(page[..], bitmap, first, payloadEnd, start as int + 1);
    if boundary as int < exact {
      assert IndexBoundaryAt(page[..], bitmap, first, boundary as int);
    } else if exact < boundary as int {
      assert IndexBoundaryAt(page[..], bitmap, first, exact);
    }
    assert boundary as int == exact;
    var stored: U32 := boundary - start;
    if stored as U64 + (if isFirst then 0 else prefix as U64) < 8 || (isFirst && prefix > stored) {
      return;
    }
    end := boundary;
  }

  function IndexChildCanonicalByte(page: array<Byte>, first: U32, start: U32,
      ghost end: U32, prefix: U32, isFirst: bool, offset: U32): Byte
    requires IndexChildEntry(page[..], first, end, start, end, prefix, isFirst)
    requires offset as int < end as int - start as int + (if isFirst then 0 else prefix as int)
    reads page
  {
    if !isFirst && offset < prefix then page[first + offset]
    else page[start + (offset - (if isFirst then 0 else prefix))]
  }

  ghost function IndexChildPointer(page: array<Byte>, first: U32, start: U32,
      end: U32, prefix: U32, isFirst: bool): U64
    requires IndexChildEntry(page[..], first, end, start, end, prefix, isFirst)
    reads page
  {
    var keyLength: U32 := (end as int - start as int + (if isFirst then 0 else prefix as int) - 8) as U32;
    IndexChildCanonicalByte(page, first, start, end, prefix, isFirst, keyLength + 4) as U64 * 16777216 +
      IndexChildCanonicalByte(page, first, start, end, prefix, isFirst, keyLength + 5) as U64 * 65536 +
      IndexChildCanonicalByte(page, first, start, end, prefix, isFirst, keyLength + 6) as U64 * 256 +
      IndexChildCanonicalByte(page, first, start, end, prefix, isFirst, keyLength + 7) as U64
  }

  ghost function IndexChildAdvance(page: array<Byte>, first: U32, payloadEnd: U32,
      start: U32, end: U32, prefix: U32, isFirst: bool, comparison: I32): U64
    requires IndexChildEntry(page[..], first, payloadEnd, start, end, prefix, isFirst)
    reads page
  {
    if comparison <= 0 then IndexChildPointer(page, first, start, end, prefix, isFirst)
    else if end == payloadEnd then 18446744073709551615
    else 4294967296 + end as U64
  }

  method AdvanceIndexChildSearch(page: array<Byte>, first: U32, payloadEnd: U32,
      start: U32, end: U32, prefix: U32, isFirst: bool, comparison: I32) returns (decision: U64)
    requires IndexChildEntry(page[..], first, payloadEnd, start, end, prefix, isFirst)
    ensures decision == IndexChildAdvance(page, first, payloadEnd, start, end, prefix, isFirst, comparison)
    ensures comparison <= 0 ==> decision == IndexChildPointer(page, first, start, end, prefix, isFirst) && decision <= 4294967295
    ensures comparison > 0 && end == payloadEnd ==> decision == 18446744073709551615
    ensures comparison > 0 && end < payloadEnd ==> (decision == 4294967296 + end as U64 &&
      first <= end < payloadEnd && payloadEnd - end < payloadEnd - start)
    ensures decision <= 4294967295 <==> comparison <= 0
  {
    if comparison > 0 {
      decision := if end == payloadEnd then 18446744073709551615 else 4294967296 + end as U64;
      return;
    }
    var keyLength: U32 := (end as U64 - start as U64 + (if isFirst then 0 else prefix as U64) - 8) as U32;
    decision := IndexChildCanonicalByte(page, first, start, end, prefix, isFirst, keyLength + 4) as U64 * 16777216 +
      IndexChildCanonicalByte(page, first, start, end, prefix, isFirst, keyLength + 5) as U64 * 65536 +
      IndexChildCanonicalByte(page, first, start, end, prefix, isFirst, keyLength + 6) as U64 * 256 +
      IndexChildCanonicalByte(page, first, start, end, prefix, isFirst, keyLength + 7) as U64;
  }

  // The oracle reports the actual vectorized comparison sign at each admitted
  // entry start. This complete finite control trace composes both executable
  // operations: every skipped entry compares positive, selection is the first
  // nonpositive comparison, and malformed acquisition never selects a child.
  lemma IndexChildSearchTrace(page: array<Byte>, bitmap: U32, first: U32,
      payloadEnd: U32, prefix: U32, comparisons: seq<I32>)
      returns (decision: U64, visited: seq<U32>)
    requires IndexChildLayout(page[..], bitmap, first, payloadEnd, first, prefix, true)
    requires |comparisons| >= payloadEnd as int - first as int
    ensures decision == 18446744073709551615 || decision <= 4294967295
    ensures |visited| <= payloadEnd as int - first as int
    ensures forall i | 0 <= i < |visited| :: first <= visited[i] < payloadEnd
    ensures forall i | 0 <= i < |visited| - 1 :: comparisons[visited[i] as int - first as int] > 0
    ensures decision == 18446744073709551615 ==>
      forall i | 0 <= i < |visited| :: comparisons[visited[i] as int - first as int] > 0
    ensures decision <= 4294967295 ==> (|visited| > 0 &&
      comparisons[visited[|visited| - 1] as int - first as int] <= 0 &&
      (var start := visited[|visited| - 1];
       var end := IndexChildAcquisition(page[..], bitmap, first, payloadEnd, start, prefix, start == first);
       end != 4294967295 && decision == IndexChildPointer(page, first, start, end, prefix, start == first)))
  {
    decision, visited := 18446744073709551615, [];
    var start := first;
    var isFirst := true;
    while start < payloadEnd
      invariant first <= start < payloadEnd
      invariant isFirst == (start == first)
      invariant |visited| <= start as int - first as int
      invariant forall i | 0 <= i < |visited| :: first <= visited[i] < start
      invariant forall i | 0 <= i < |visited| :: comparisons[visited[i] as int - first as int] > 0
      decreases payloadEnd - start
    {
      var end := IndexChildAcquisition(page[..], bitmap, first, payloadEnd, start, prefix, isFirst);
      if end == 4294967295 { return; }
      var comparison := comparisons[start as int - first as int];
      var step := IndexChildAdvance(page, first, payloadEnd, start, end, prefix, isFirst, comparison);
      visited := visited + [start];
      if step <= 4294967295 {
        decision := step;
        return;
      }
      assert comparison > 0;
      if step == 18446744073709551615 { return; }
      start := (step - 4294967296) as U32;
      isFirst := false;
    }
  }

  // Jet3 jump-table controls. Native rows use at most seven entries, but the
  // sizing law is exact for every positive native-width input. Span access and
  // retention remain adapter effects; no copied row/offset arrays are needed.
  function Jet3JumpCount(lengthWithoutJumps: U32): U32 {
    if lengthWithoutJumps <= 256 then 0 else (lengthWithoutJumps - 2) / 255
  }

  lemma Jet3JumpCountIsSmallestFixedPoint(length: U32, smaller: U32)
    requires length > 0
    ensures (length as int + Jet3JumpCount(length) as int - 1) / 256 == Jet3JumpCount(length) as int
    ensures smaller < Jet3JumpCount(length) ==>
      (length as int + smaller as int - 1) / 256 > smaller as int
  {
    if length > 256 {
      var count := Jet3JumpCount(length) as int;
      assert 255 * count <= length as int - 2 < 255 * (count + 1);
    }
  }

  function Jet3JumpEntryCount(rowLength: U32): U32 {
    if rowLength == 0 then 0 else (rowLength - 1) / 256
  }

  function Jet3UsableJumpCount(entryCount: U32, eodPosition: U32): U32 {
    if entryCount > 0 && eodPosition / 256 < entryCount then entryCount - 1 else entryCount
  }

  lemma Jet3JumpCountsPreserveBoundaries(rowLength: U32, entries: U32, eod: U32)
    ensures rowLength > 0 ==> (256 * Jet3JumpEntryCount(rowLength) as int < rowLength as int <=
      256 * (Jet3JumpEntryCount(rowLength) as int + 1))
    ensures Jet3UsableJumpCount(entries, eod) <= entries
    ensures entries - Jet3UsableJumpCount(entries, eod) <= 1
    ensures Jet3UsableJumpCount(entries, eod) < entries <==> entries > 0 && eod / 256 < entries
  { }

  function Jet3JumpReached(entry: Byte, previous: Byte, index: U32): bool {
    previous <= entry && entry as U32 <= index
  }

  ghost predicate Jet3JumpPrefix(bytes: seq<Byte>, end: nat, reached: nat, index: U32)
    requires reached <= end <= |bytes|
    decreases reached
  {
    reached == 0 || (Jet3JumpPrefix(bytes, end, reached - 1, index) &&
      Jet3JumpReached(bytes[end - reached], if reached == 1 then 0 else bytes[end - reached + 1], index))
  }

  function Jet3JumpReadRange(length: U32, start: U32, end: U32, count: U32): bool {
    start <= length && end <= length - start && count <= end && count <= 8388607
  }

  method ReadJet3JumpHighPart(bytes: array<Byte>, start: U32, end: U32, count: U32, index: U32)
      returns (high: U32)
    requires bytes.Length <= 2147483647
    ensures high == 4294967295 <==> !Jet3JumpReadRange(bytes.Length as U32, start, end, count)
    ensures high != 4294967295 ==> (high <= 2147483392 && high % 256 == 0 &&
      high / 256 <= count &&
      Jet3JumpPrefix(bytes[..], (start + end) as int, (high / 256) as int, index) &&
      (high / 256 == count ||
       !Jet3JumpReached(bytes[start + end - 1 - high / 256],
         if high == 0 then 0 else bytes[start + end - high / 256], index)))
  {
    high := 4294967295;
    if !Jet3JumpReadRange(bytes.Length as U32, start, end, count) { return; }
    ghost var image := bytes[..];
    ghost var absoluteEnd: nat := (start + end) as int;
    var reached: U32 := 0;
    var previous: Byte := 0;
    while reached < count
      invariant reached <= count
      invariant Jet3JumpPrefix(image, absoluteEnd, reached as int, index)
      invariant previous == (if reached == 0 then 0 else bytes[start + end - reached])
      decreases count - reached
    {
      var entry := bytes[start + end - 1 - reached];
      if !Jet3JumpReached(entry, previous, index) {
        high := reached * 256;
        return;
      }
      assert Jet3JumpPrefix(image, absoluteEnd, reached as int + 1, index);
      previous := entry;
      reached := reached + 1;
    }
    high := reached * 256;
  }

  // 256 requests another acquired variable offset; 255 is the native dummy.
  // Zero-column rows that cross a boundary select the EOD's index zero.
  function Jet3JumpEntryStart(variableCount: U32, boundary: U32, eod: U32): U32 {
    if eod < boundary then 255 else if variableCount == 0 then 0 else 256
  }

  function Jet3JumpEntryAdvance(boundary: U32, index: U32, offset: U32): U32 {
    if offset >= boundary then index else 256
  }

  function Jet3DummyJumpCount(entries: U32, eod: U32): U32 {
    if eod / 256 < entries then entries - eod / 256 else 0
  }

  function Jet3JumpWritable(variableCount: U32, entries: U32, eod: U32): bool {
    variableCount != 255 || Jet3DummyJumpCount(entries, eod) <= 1
  }

  lemma Jet3JumpEncoderDecisionsAreExact(variableCount: U32, boundaryIndex: U32,
      eod: U32, index: U32, offset: U32, entries: U32)
    requires variableCount <= 255 && 0 < boundaryIndex <= 8388607 && index < variableCount
    ensures Jet3JumpEntryStart(variableCount, boundaryIndex * 256, eod) == 255 <==>
      eod as int < 256 * boundaryIndex as int
    ensures Jet3JumpEntryAdvance(boundaryIndex * 256, index, offset) == index <==>
      offset as int >= 256 * boundaryIndex as int
    ensures Jet3JumpEntryAdvance(boundaryIndex * 256, index, offset) == 256 <==>
      offset as int < 256 * boundaryIndex as int
    ensures Jet3DummyJumpCount(entries, eod) == 0 <==> entries <= eod / 256
    ensures Jet3DummyJumpCount(entries, eod) > 0 ==>
      Jet3DummyJumpCount(entries, eod) + eod / 256 == entries
    ensures boundaryIndex <= entries ==>
      (eod as int < 256 * boundaryIndex as int <==>
       boundaryIndex > entries - Jet3DummyJumpCount(entries, eod))
    ensures !Jet3JumpWritable(variableCount, entries, eod) <==>
      variableCount == 255 && Jet3DummyJumpCount(entries, eod) >= 2
  { }
}
