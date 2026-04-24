# Format notes: `MSysObjects.LvProp` (`KKD\0` / `MR2\0`)

**Status:** Phase 0 in progress
**Sibling:** [persisted-column-properties.md](./persisted-column-properties.md)

This document captures the verified on-disk layout of the property blob stored in `MSysObjects.LvProp`. It supersedes §3.2 of the parent design doc where the two disagree.

## 1. Sources

| Source | Confidence |
|---|---|
| mdbtools `src/libmdb/props.c` (`mdb_kkd_to_props`, `mdb_read_props_list`, `mdb_read_props`) | High — exercised against many real Access files in mdbtools' test corpus. |
| Microsoft Access hex-dump reconnaissance on Windows | **Pending** — author 3-column / 5-row `.accdb` and `.mdb` files with `DefaultValue`, `ValidationRule`, `ValidationText`, `Description` set, hex-dump the `MSysObjects.LvProp` payload, paste excerpts into §4 below. |

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
                                     (mdbtools also accepts 0x01 and 0x02 — see §5)
6       ...     payload     chunkLen − 6 bytes
```

`pos += chunkLen` between chunks.

> **Discrepancy with parent design doc §3.2:** the parent doc lists the chunk header as `uint16 chunkLen + byte chunkType`. mdbtools uses `uint32 chunkLen + uint16 chunkType`. Implementation follows mdbtools.

### 2.3 Name-pool chunk (type `0x80`)

The first chunk after the magic. Provides the dictionary of property names referenced by index from later property-block chunks.

Payload is a sequence of length-prefixed strings until the chunk payload is consumed:

```
0       2       nameLen     uint16   bytes of UTF-16LE name (Jet4)
                                     (Jet3 may use uint8 — TODO verify)
2       ...     name        UTF-16LE bytes (Jet4) / database codepage (Jet3)
```

There is **no** count field. Walk the payload until exhausted.

### 2.4 Property-block chunk (type `0x00`)

One per target. The first property block in the blob describes the *table* itself; subsequent blocks describe individual columns. Block-to-target binding is by the embedded target name (column name, or table name for the table-level block).

Payload layout (`chunkLen − 6` bytes total):

```
0       2       innerLen      uint16   purpose unclear; mdbtools reads then ignores it
2       2       reserved      uint16   purpose unclear; mdbtools reads then ignores
4       2       targetNameLen uint16   bytes of UTF-16LE target name
6       ...     targetName    UTF-16LE bytes (Jet4) / codepage bytes (Jet3)
N       ...     entries       sequence of property entries until payload exhausted
```

> mdbtools' read does `pos += 4; name_len = get_int16(pos); pos += 2`. The first 4 bytes are read as `record_len` then skipped — function unclear without further reconnaissance. Treat as opaque on read; preserve verbatim on round-trip.

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

## 4. Microsoft Access reconnaissance — TODO

For each of the following, hex-dump the `LvProp` cell value (raw bytes, not the cell's storage encoding) and paste a short excerpt under each subsection, plus a one-line interpretation.

### 4.1 `.accdb` (ACE) with `DefaultValue = 0` on a Long Integer column
- [ ] Hex dump
- [ ] Confirm chunk-type byte for property block (0x00 vs 0x01 vs 0x02)
- [ ] Confirm `ddlFlag` value Access writes
- [ ] Confirm `targetName` is the column name vs the table name

### 4.2 `.accdb` with `ValidationRule = ">=0"` and `ValidationText = "must be non-negative"`
- [ ] Hex dump
- [ ] Confirm both entries appear in the same property-block chunk for that column

### 4.3 `.accdb` with `Description` set on the table itself
- [ ] Hex dump
- [ ] Confirm whether table-level `Description` lives in (a) a separate property-block chunk targeted to the table name, or (b) the column-level chunks, or (c) `MSysObjects.LvProp` of the *table's catalog row* vs. a column block within the table's own blob — answers open question 3 in parent design doc

### 4.4 `.mdb` (Jet3) with `DefaultValue = 0`
- [ ] Hex dump
- [ ] Confirm name-pool string length prefix (uint8 vs uint16)
- [ ] Confirm string encoding (codepage vs UTF-16LE)

### 4.5 `MSysObjects` row for a user table — column inventory
- [ ] List all 15+ columns Access populates (`Owner`, `LvProp`, `LvExtra`, `Connect`, `RmtInfoLong`, `RmtInfoShort`, …)
- [ ] Confirm `Owner` value for a default-user-owned table — informs Phase 3 schema widening

## 5. Property-block subtype variation

mdbtools accepts chunk types `0x00`, `0x01`, and `0x02` as property blocks (treats all three identically). Hypothesis: subtype distinguishes table vs column vs index property blocks. **Verify in §4.**

This library should:
- **Read:** accept all three chunk subtypes as property blocks.
- **Write:** emit `0x00` exclusively unless reconnaissance shows Access requires a specific subtype per target kind.

## 6. Implementation status

- ✅ Phase 1 parser: implemented per §2 (mdbtools-aligned). Tolerant of unknown chunk types — preserved as opaque bytes.
- ⏳ Phase 0 manual verification: §4 unfilled. Parser is provisional until §4 is completed.
- ⏳ Parent design doc §3.2 needs an update PR to align with §2 above. Tracked as a follow-up.
