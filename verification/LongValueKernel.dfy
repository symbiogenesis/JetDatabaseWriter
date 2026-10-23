// Executable packing decisions for the JET long-value descriptor's first word.
// Physical byte acquisition, the remaining fields, payload validation and native
// format compatibility remain separate contracts. No external C# is trusted.
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
