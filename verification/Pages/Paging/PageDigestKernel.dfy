// Bounded raw-page digest slots. Cryptographic identity and successful exact
// I/O are explicit adapter effects; invalidated ledgers grant no commit authority.
module PageDigestNativeTypes {
  newtype {:nativeType "uint"} U32 = x: int | 0 <= x < 4294967296
  newtype {:nativeType "ulong"} U64 = x: int | 0 <= x < 18446744073709551616
  newtype {:nativeType "byte"} Byte = x: int | 0 <= x < 256
}

module PageDigestKernel {
  import opened PageDigestNativeTypes

  function ValidShape(length: U64, pageSize: U32): bool {
    (pageSize == 2048 || pageSize == 4096) && length <= 2147483648
  }

  function DigestBytes(length: U64, pageSize: U32): U32
    ensures DigestBytes(length, pageSize) <= 33554432
    ensures ValidShape(length, pageSize) ==>
      DigestBytes(length, pageSize) as int == ((length as int + pageSize as int - 1) / pageSize as int) * 32
    ensures !ValidShape(length, pageSize) ==> DigestBytes(length, pageSize) == 0
  {
    if ValidShape(length, pageSize)
    then (((length + pageSize as U64 - 1) / pageSize as U64) * 32) as U32
    else 0
  }

  function CanTrackWrite(length: U64, offset: U64, count: U32, pageSize: U32): bool
    ensures CanTrackWrite(length, offset, count, pageSize) ==>
      (pageSize == 2048 || pageSize == 4096) && count == pageSize && offset as int + count as int <= 2147483648 &&
      offset <= length && offset % pageSize as U64 == 0 && length % pageSize as U64 == 0
  {
    ValidShape(length, pageSize) && count == pageSize && offset <= length && offset <= 2147483648 - pageSize as U64 &&
      offset % pageSize as U64 == 0 && length % pageSize as U64 == 0
  }

  function CanTrackResize(oldLength: U64, length: U64, pageSize: U32): bool {
    ValidShape(oldLength, pageSize) && ValidShape(length, pageSize) && length <= oldLength &&
      oldLength % pageSize as U64 == 0 && length % pageSize as U64 == 0
  }

  method ReplaceDigest(ledger: array<Byte>, index: U32, digest: array<Byte>) returns (accepted: bool)
    requires ledger.Length < 2147483648 && digest.Length < 2147483648
    modifies ledger
    ensures accepted <==> digest.Length == 32 && index as int * 32 + 32 <= ledger.Length
    ensures !accepted ==> ledger[..] == old(ledger[..])
    ensures accepted ==> forall i: int :: 0 <= i < 32 ==> ledger[index as int * 32 + i] == old(digest[i])
    ensures forall i: int :: (0 <= i < ledger.Length &&
      (i < index as int * 32 || index as int * 32 + 32 <= i)) ==> ledger[i] == old(ledger[i])
  {
    accepted := digest.Length as U32 == 32 && ledger.Length as U32 >= 32 &&
      index <= (ledger.Length as U32 - 32) / 32;
    if !accepted { return; }
    var start := index * 32;
    ghost var before := ledger[..];
    ghost var payload := digest[..];
    var cursor: U32 := 0;
    while cursor < 32
      invariant cursor <= 32
      invariant forall i: int :: 0 <= i < cursor as int ==> ledger[start as int + i] == payload[i]
      invariant forall i: int :: (0 <= i < ledger.Length &&
        (i < start as int || start as int + cursor as int <= i)) ==> ledger[i] == before[i]
      invariant ledger != digest || start == 0
      invariant forall i: int :: cursor as int <= i < 32 ==> digest[i] == payload[i]
      decreases 32 - cursor
    {
      ledger[start + cursor] := digest[cursor];
      cursor := cursor + 1;
    }
  }
}
