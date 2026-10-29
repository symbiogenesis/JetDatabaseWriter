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
