# Format notes: `MSysObjects.LvProp` (`KKD\0` / `MR2\0`)

**Status:** Implemented

This document captures the on-disk layout of the property blob stored in `MSysObjects.LvProp`. The parser ([`ColumnPropertyBlock`](../../JetDatabaseWriter/Schema/Models/ColumnPropertyBlock.cs)) and writer ([`ColumnPropertyBlockBuilder`](../../JetDatabaseWriter/Schema/ColumnPropertyBlockBuilder.cs)) follow the layout below; round-trip tests exercise both against Access-authored fixtures.

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
0       4       innerHeader   uint32   opaque to the reader. DAO programmatic table creation
                                       has been observed writing 4 + 2 + targetNameLen here
                                       (header through target name), not the full payload length.
                                       Earlier writer versions wrote chunkLen - 6; mdbtools ignores it.
4       2       targetNameLen uint16   bytes of UTF-16LE target name
6       ...     targetName    UTF-16LE bytes (Jet4) / codepage bytes (Jet3)
N       ...     entries       sequence of property entries until payload exhausted
```

Writer parity: treat the first 4 bytes as opaque on read and preserve existing values during round-trip. For new DAO-shaped column property blocks, prefer the DAO-observed `4 + 2 + targetNameLen` value.

### 2.5 Property entry

```
0       2       entryLen        uint16   total bytes including these 2
2       1       ddlFlag         byte     DAO writes 0x01 for Boolean properties created through DDL/DAO
                                         (`Required`, `AllowZeroLength`); 0x00 also appears in older blobs.
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

The 2026-05-11 DAO FK baseline found that programmatic DAO table creation emits column property blocks with subtype `0x01` for the simple Short Text / Long / AutoNumber schema under test. Subtype `0x00` remains readable, but `0x01` is the closer DAO shape for newly-authored column targets.

This library:
- **Reads:** accepts all three chunk subtypes as property blocks; unknown chunk types are preserved as opaque bytes for round-trip.
- **Writes:** should preserve parsed subtypes on round-trip and prefer `0x01` for newly-created column property targets when pursuing DAO Compact & Repair compatibility.

## 5. DAO-created column property facts (2026-05-11)

The FK Compact investigation compared writer-authored and DAO-authored tables with this schema: `Parent(ParentId AutoNumber PK, Label Text Required)` and `Child(ChildId AutoNumber PK, ParentId Long Required, Detail Text Nullable)`.

Observed DAO `MSysObjects.LvProp` facts:

- AutoNumber columns do not carry a `Required` property, even when DAO field/index objects are marked required. Their non-null behavior comes from the AutoNumber column flag and PK/index metadata.
- Text columns carry `AllowZeroLength = False` (`dataType = 0x01`, Boolean value `0x00`).
- Non-null text columns carry `Required = True`; nullable text columns can carry `Required = False`.
- Boolean property entries created by DAO use `ddlFlag = 0x01`.
- The name pool may contain both `Required` and `AllowZeroLength`; target property order in the observed blobs was `Required` then `AllowZeroLength` for non-null text, and `AllowZeroLength` then `Required` for nullable text. Access appears to tolerate either order when reading, but byte-for-byte DAO parity should preserve the observed order where practical.

These LvProp differences were real DAO deltas, but matching them was not sufficient by itself to make writer-created FK tables survive DAO Compact & Repair. The final compact fix also required system-table row placement, Type=8 relationship catalog objects, relationship ACE rows, shared table/index usage-map rows, and in-place single-leaf reuse. Treat the LvProp shape as a compatibility fact, not as the whole compact root cause.
