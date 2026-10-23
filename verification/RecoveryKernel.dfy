// Executable recovery decisions. Generated C# calls these total native-width
// functions; ghost lemmas connect byte authorization to UndoJournal.Allowed.
// Parsing, hashes, before-image coverage, I/O and loop correspondence remain
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

  function IsPageOffset(offset: U64, pageSize: U32): bool {
    IsPageSize(pageSize) &&
    offset <= 2147483648 - (pageSize as U64) &&
    offset % (pageSize as U64) == 0
  }

  function IsTruncateLength(length: U64, pageSize: U32, maximumLength: U64): bool {
    IsPageSize(pageSize) && length <= maximumLength &&
    length % (pageSize as U64) == 0
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
