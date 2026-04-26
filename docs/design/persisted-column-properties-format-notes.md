# Format notes: `MSysObjects.LvProp` (`KKD\0` / `MR2\0`)

**Status:** Implemented

This document captures the on-disk layout of the property blob stored in `MSysObjects.LvProp`. The parser ([`ColumnPropertyBlock`](../../JetDatabaseWriter/Internal/ColumnPropertyBlock.cs)) and writer ([`ColumnPropertyBlockBuilder`](../../JetDatabaseWriter/Internal/ColumnPropertyBlockBuilder.cs)) follow the layout below; round-trip tests exercise both against Access-authored fixtures.

## 1. Sources

| Source | Confidence |
|---|---|
| mdbtools `src/libmdb/props.c` (`mdb_kkd_to_props`, `mdb_read_props_list`, `mdb_read_props`) | High — exercised against many real Access files in mdbtools' test corpus. |
| In-repo round-trip tests against Access-authored `.accdb` fixtures | High — covers Jet4/ACE name pool, property-block, and the four currently-surfaced text properties (`DefaultValue`, `ValidationRule`, `ValidationText`, `Description`). |

## 2. Verified layout (Jet4 / ACE — `MR2\0` magic)

All multi-byte integers are little-endian.

### 2.1 Blob header

```
0       4       magic       'M' 'R' '2' 0x00     (Jet4/ACE)
                            'K' 'K' 'D' 0x00     (Jet3)
4       ...     chunks      concatenated until end of blob
```

### 2.2 Chunk header (all chunk types)

```
0       4       chunkLen    uint32   total bytes including these 6
4       2       chunkType   uint16   0x80 = name pool, 0x00 = property block
                                     (mdbtools also accepts 0x01 and 0x02 — see §4)
6       ...     payload     chunkLen − 6 bytes
```

`pos += chunkLen` between chunks.

### 2.3 Name-pool chunk (type `0x80`)

The first chunk after the magic. Provides the dictionary of property names referenced by index from later property-block chunks.

Payload is a sequence of length-prefixed strings until the chunk payload is consumed:

```
0       2       nameLen     uint16   bytes of name (mdbtools uses uint16 for both Jet3 and Jet4)
2       ...     name        UTF-16LE bytes (Jet4) / database codepage (Jet3)
```

There is **no** count field. Walk the payload until exhausted.

### 2.4 Property-block chunk (type `0x00`)

One per target. The first property block in the blob describes the *table* itself; subsequent blocks describe individual columns. Block-to-target binding is by the embedded target name (column name, or table name for the table-level block).

Payload layout (`chunkLen − 6` bytes total):

```
0       4       innerRecLen   uint32   total bytes of this property-block payload (= chunkLen − 6).
                                       mdbtools reads only the lo-16 bits as int16 and skips the hi-16,
                                       which is equivalent to a uint32 read since these blocks never exceed 64 KiB.
4       2       targetNameLen uint16   bytes of UTF-16LE target name
6       ...     targetName    UTF-16LE bytes (Jet4) / codepage bytes (Jet3)
N       ...     entries       sequence of property entries until payload exhausted
```

Writer parity: emit `innerRecLen = chunkLen − 6` and the rest verbatim; reader treats the first 4 bytes as opaque on round-trip.

### 2.5 Property entry

```
0       2       entryLen        uint16   total bytes including these 2
2       1       ddlFlag         byte     hypothesis: 0x00 normally, 0x01 = "DDL-set"
                                         (mdbtools does not interpret this byte)
3       1       dataType        byte     Jet column-type code (see §3)
4       2       nameIndex       uint16   index into the name-pool
6       2       valueLen        uint16   bytes of value payload
8       ...     value           dataType-encoded bytes
```

Advance: `pos += entryLen`.

## 3. Known dataType codes (per mdbtools `MDB_*` constants)

| Code | Meaning | Notes |
|---|---|---|
| `0x01` | Boolean | 1 byte; nonzero = true |
| `0x02` | Byte | uint8 |
| `0x03` | Integer | int16 |
| `0x04` | Long | int32 |
| `0x05` | Money | currency, int64 / 10000 |
| `0x06` | Single | float32 |
| `0x07` | Double | float64 |
| `0x08` | DateTime | OLE date, float64 |
| `0x0A` | Text | UTF-16LE (Jet4); used for `DefaultValue`, `ValidationRule`, `ValidationText`, `Description`, `Format`, `Caption`, etc. |
| `0x0B` | OLE | opaque bytes |
| `0x0C` | Memo | UTF-16LE (Jet4) |
| `0x0F` | GUID | 16 bytes |

For the four properties we care about in this PR series, the dataType is always `0x0A` (Text) carrying a Jet expression string or free text.

## 4. Property-block subtype variation

mdbtools accepts chunk types `0x00`, `0x01`, and `0x02` as property blocks and treats all three identically. The subtype likely distinguishes table vs column vs index property blocks, but neither mdbtools nor Jackcess depends on the distinction.

This library:
- **Reads:** accepts all three chunk subtypes as property blocks; unknown chunk types are preserved as opaque bytes for round-trip.
- **Writes:** emits `0x00` exclusively. Round-trip tests against Access-authored fixtures pass without per-target-kind subtype emission.
