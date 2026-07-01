# Design notes: Index, primary-key, foreign-key, and relationship format

**Status:** All shipped phases listed in §7; outstanding work tracked in the same table.
**Empirical appendix:** [`format-probe-appendix-index.md`](../format-probe/format-probe-appendix-index.md) — annotated hex dumps of real index TDEFs from `NorthwindTraders.accdb` (Jet4/ACE), [`format-probe-appendix-jet3-index.md`](../format-probe/format-probe-appendix-jet3-index.md) — Jet3 index TDEFs + leaf pages from the Jackcess V1997 corpus (W17a). Regenerate them with `dotnet run --project JetDatabaseWriter.FormatProbe -- index,jet3-index`.
**Validation requirement:** any PR that lands writer disk-format bytes in this area MUST update the validation matrix and add the strongest feasible Access/DAO coverage: DAO CompactDatabase/OpenRecordset automation for automatable scenarios, with manual Access UI checks kept as supplemental evidence — see §8 and [dao-validation-strategy.md](dao-validation-strategy.md).

> ⚠️ Reverse-engineered notes. The mdbtools spec is partial; many fields are documented as `???`. The empirical appendix has been generated from `NorthwindTraders.accdb` and confirms the §3.1 / §3.2 layouts. DAO CompactDatabase now covers representative ACE/Jet4 advanced key encodings, bitmask/sentinel emission, and B-tree maintenance, but byte-for-byte parity for some sort-key encodings and individual surgical split/merge sub-phases remains a probe gap. Run the probe again against any new fixture before relying on a new offset.

---

## 1. Background

Row enumeration does **not** consume index information today; it remains a linear data-page scan via the per-table page-usage bitmap. Index *schema* metadata is observable via `IAccessReader.ListIndexesAsync` (R1), and exact equality seeks are exposed via `IAccessReader.SeekRowsAsync` (R3). Microsoft Access / DAO CompactDatabase remains the decisive compatibility validator for writer-emitted index bytes because it exercises the engine's own index and catalog walkers.

The two pinned tests that codify the original limitation, both still passing as of W16:

- `JetDatabaseWriter.Tests/Writer/LimitationsTests.SchemaEvolution_IAccessWriter_DoesNotExposeIndexOrConstraintApis` — the entry points added across W1–W14 are a `CreateTableAsync` overload (parameter type is `IndexDefinition`), `IsPrimaryKey` *properties* on `ColumnDefinition` / `IndexDefinition`, and the `Create/Drop/RenameRelationshipAsync` trio (which carry `Relationship` in their names but are deliberately not matched by the `Index`/`PrimaryKey`/`ForeignKey` substring filter the test uses).
- `JetDatabaseWriter.Tests/Writer/LimitationsTests.SchemaEvolution_FreshlyCreatedTable_HasNoUserDefinedIndexEntries` — asserts that no index/relationship catalog rows reference a freshly-created bare table. `MSysIndexes` is never written (it does not exist in any modern Access fixture — see §6); `MSysRelationships` is only written when the user calls `CreateRelationshipAsync`.

## 2. Reader-side prerequisite

Before *any* writer work, the reader needs to be able to enumerate indexes from a TDEF and (optionally) traverse leaf pages. This is the foundation that makes every writer change round-trippable in this repo's own test suite.

Recommended phasing:

1. **Phase R1 — TDEF index metadata.** ✅ **Implemented (2026-04-24).** Parses the logical-index block and index name list out of the existing TDEF byte buffer. Surfaced as `IAccessReader.ListIndexesAsync(tableName, CT)` returning `IReadOnlyList<IndexMetadata>` (with `IndexKind`, `IndexColumnReference`, real-idx sharing via `RealIndexNumber`, FK fields, and cascade flags). No B-tree page traversal. Tests in `JetDatabaseWriter.Tests/Indexes/IndexMetadataTests.cs`.
2. **Phase R2 — Leaf-page enumeration.**  Walk page-type `0x04` leafs from `first_dp` and decode entries (per the index entry layout in §4 below). Used internally for assertions in writer tests; not necessarily exposed publicly.
3. **Phase R3 — Index seeking.** ✅ **Implemented (2026-05-21).** `IndexCursor` descends `0x03` intermediate pages, probes `0x04` leafs, honours §4.2 entry-start bitmasks, §4.4 prefix compression, non-unique sibling leaf walks, and the §4.5 `tail_page` append fall-through. The public reader surface is `IAccessReader.SeekRowsAsync(tableName, indexName, keyValues, CT)`, which performs exact Jet4/ACE seeks and materialises matching rows through the same typed row path as `Rows(...)`. Range scans are intentionally not part of this API.

Phases R2/R3 require the encoded sort-key implementation that the writer also needs (§5), so doing R2 before W1 amortizes that cost.

## 3. TDEF layout (per [mdbtools HACKING.md](https://github.com/mdbtools/mdbtools/blob/master/HACKING.md) "TDEF (Table Definition) Pages")

The TDEF page chain (page type `0x02`, linked through `next_pg` at offset `4`) is concatenated by `AccessBase.ReadTDefBytesAsync` into a single byte array. Within that buffer, the layout (Jet4 / ACE) is:

| Section | Size | Notes |
|---|---|---|
| Page header | 8 bytes | `page_type=0x02`, `unknown=0x01`, `tdef_id`, `next_pg` |
| Jet4 TDEF block | 55 bytes | `tdef_len`, `unknown`, `num_rows`, `autonumber`, `autonum_flag`, `unknown[3]`, `ct_autonum`, `unknown[8]`, `table_type`, `max_cols`, `num_var_cols`, `num_cols`, **`num_idx`** at relative offset 39 (absolute 47 from page start), **`num_real_idx`** at absolute 51, `used_pages`, `free_pages` |
| Real-index entries | `num_real_idx × 12` (Jet4) / `× 8` (Jet3) | Already skipped by `AccessBase.ReadTableDefAsync`. Per mdbtools each Jet4 entry: `unknown(4) + num_idx_rows(4) + unknown(4)` |
| Column descriptors | `num_cols × 25` (Jet4) / `× 18` (Jet3) | Already parsed |
| Column names | `num_cols ×` length-prefixed | 2-byte len + UTF-16 (Jet4); 1-byte len + ANSI (Jet3) |
| **Real-index "physical" descriptors** | `num_real_idx × 52` (Jet4) / `× 39` (Jet3) | See §3.1 |
| **Logical-index entries** | `num_idx × 28` (Jet4) / `× 20` (Jet3) | See §3.2 |
| **Logical-index names** | `num_idx ×` length-prefixed | Same encoding as column names |
| Variable-length column trailing block | iterates while `col_num != 0xFFFF` | Per-var-col `used_pages` / `free_pages` pointers |

### 3.1 Real-index "physical" descriptor (Jet4: 52 bytes)

**Verified against `Companies` in `NorthwindTraders.accdb`** ([appendix](../format-probe/format-probe-appendix-index.md#companies--tdef-page-50)). Layout:

```text
4 bytes  unknown      // probe shows constant `83 07 00 00` across all real-idx entries in a TDEF;
                     // value differs across tables but is constant within one table — looks like a per-tdef cookie
30 bytes col_map      // 10 × { col_num(2), col_order(1) }; col_num=0xFFFF marks unused slot
4 bytes  used_pages   // FK to usage bitmap for this index
4 bytes  first_dp     // root index page (0x03 / 0x04 / sometimes 0x01 for tiny tables)
1 byte   flags        // index_flags table below
9 bytes  unknown      // first byte varies (probe sees 0x80, 0x81, 0x89); remaining 8 bytes appear zero
```

Index flags (`flags` byte; not exhaustive per mdbtools):

| Bit | Meaning |
|---|---|
| `0x01` | Unique |
| `0x02` | IgnoreNulls |
| `0x08` | Required |

> **Probe-confirmed (`NorthwindTraders.accdb` `Companies` PK, `MSysObjects` PK):** Access emits `flags = 0x00` on the real-idx physical descriptor of primary-key indexes. PK uniqueness is signalled by the logical-idx `index_type = 0x01` discriminator (§3.2), **not** by the `0x01` flag bit. `IAccessReader.ListIndexesAsync` surfaces `IndexMetadata.HasUniqueFlag` for the raw flag and `IndexMetadata.EnforcesUniqueness` for the semantic answer.

#### Jet3 real-idx physical descriptor (39 bytes) — W17a probe-confirmed

The mdbtools spec lists 39 bytes for Jet3 but does not enumerate the field map. The W17a probe ([`format-probe-appendix-jet3-index.md`](../format-probe/format-probe-appendix-jet3-index.md)) was run across 6 Jet3 fixtures (`indexTestV1997.mdb`, `compIndexTestV1997.mdb`, `testIndexCodesV1997.mdb`, `testV1997.mdb`, `mdbtools/nwind.mdb`, `Jet3Test.mdb`); for **every** real-idx slot the candidate at phys-desc bytes `34..37` resolved to a `page_type = 0x04` leaf or `0x03` intermediate, while the leading 8-byte skip entry slots either failed to resolve or pointed at data pages (`0x01`).

```text
4 bytes  unknown      // per-tdef cookie (Jet4 §3.1 parallel)
30 bytes col_map      // 10 × { col_num(2), col_order(1) }; col_num=0xFFFF marks unused slot
4 bytes  first_dp     // root index page (0x03 / 0x04, sometimes 0x01 for tiny tables)
1 byte   flags        // same bit semantics as Jet4 (0x01=Unique, 0x02=IgnoreNulls, 0x08=Required)
```

Key deltas vs. Jet4:

- The Jet4 `used_pages` slot at bytes 34..37 is **not** present on Jet3; per-table usage-bitmap pointers live elsewhere (almost certainly in the leading 8-byte skip-entry block at TDEF offset `_tdBlockEnd + i*8` — see the `Jet3 leading real-idx skip entries` section in the W17a appendix).
- Jet3 has no trailing 9-byte unknown block after `flags` — the descriptor terminates at byte 38.
- The same `index_type = 0x01` PK discriminator on the matching logical-idx (§3.2) signals uniqueness; `flags` byte 38 carries the same bit semantics as Jet4.

`col_order` per col-map slot: bit `0x01` is the ascending flag. The writer emits `0x01` for ascending and clears the bit (`0x00` in current output) for descending; DAO CompactDatabase preserves the mixed-direction descriptors covered by `AdvancedIndexKeysAndBTreeMaintenance_SurviveCompactAndRepair`.

### 3.2 Logical-index entry (Jet4: 28 bytes)

**Verified against `Companies` in `NorthwindTraders.accdb`** ([appendix](../format-probe/format-probe-appendix-index.md#companies--tdef-page-50)).

```text
4 bytes  unknown        // probe shows constant `59 06 00 00` across all logical-idx entries in a TDEF;
                        // varies across tables — same per-tdef cookie pattern as §3.1
4 bytes  index_num      // logical index number; NOT necessarily sequential
4 bytes  index_num2     // index into the real-index list (which physical idx backs this)
1 byte   rel_tbl_type   // FK role: 0x01 on parent-side FK logical entries,
                        // 0x02 on child-side FK logical entries; 0x00 for non-FK
4 bytes  rel_idx_num    // -1 (0xFFFFFFFF) when this is not a FK; otherwise the
                        // partner logical-index number, not the partner real-idx slot
4 bytes  rel_tbl_page   // page number of the other table in the FK
1 byte   cascade_ups    // FK cascade-updates: only bit 0x01 (CASCADE_UPDATES_FLAG)
                        // signals "cascade enabled". DAO/Access stamps placeholder
                        // 0x04 (CASCADE_SET_DEFAULT_FLAG) into this byte for every
                        // index — including PK and standalone — so a `!= 0` test
                        // produces false positives. Mask to 0x01 (Jackcess IndexImpl).
1 byte   cascade_dels   // FK cascade-deletes: same 0x01-mask rule as cascade_ups.
1 byte   index_type     // 0x01 = primary key, 0x02 = foreign key, otherwise normal
4 bytes  trailing       // probe sees zeros; HACKING.md does not document this tail
```

**Sharing confirmed empirically**: `Companies` (15 user columns) has 16 logical indexes but only 5 real indexes — multiple logical FK indexes share the same `index_num2` (real-idx slot).

#### Jet3 logical-idx entry (20 bytes) — W17a probe-confirmed

The mdbtools spec lists 20 bytes for Jet3 without enumerating the field map. The W17a probe verified `index_type = 0x01` at byte 19 on the `MSysAccessObjects.PrimaryKey` logical-idx in every fixture sampled. Removing the Jet4 leading 4-byte cookie and trailing 4-byte tail from the §3.2 layout produces the 20-byte slice with all per-field offsets shifted left by 4 bytes:

```text
4 bytes  index_num
4 bytes  index_num2
1 byte   rel_tbl_type
4 bytes  rel_idx_num   // -1 (0xFFFFFFFF) when this is not a FK
4 bytes  rel_tbl_page  // page number of the other table in the FK
1 byte   cascade_ups   // same 0x01-mask rule as Jet4 (see §3.2)
1 byte   cascade_dels  // same 0x01-mask rule as Jet4 (see §3.2)
1 byte   index_type    // 0x01 PK, 0x02 FK, otherwise normal
```

This matches the offsets `AccessReader.ParseIndexMetadata` uses for the Jet3 path (W17b 2026-04-26: the prior typo that read `index_num` / `index_num2` from Jet4 offsets even on Jet3 was fixed; both fields now read from the §3.2 shifted-left offsets).

### 3.3 Sharing rules

> "There can be more logical index infos than physical index infos (currently only seen for foreign-key indexes). In this situation, one or more of the logical indexes actually share the same underlying physical index (the `index_num2` indicates which physical index backs which logical index)." — HACKING.md

When the user enables "enforce referential integrity" in the Access GUI, both sides of the relationship gain an extra logical index (type `0x02`). If the FK columns are already covered by a user-created index, the new logical FK index reuses that physical index data instead of duplicating it.

### 3.4 DAO FK compatibility findings (updated 2026-05-12)

The `fk-dao-baseline` FormatProbe mode creates the same parent/child/enforced-FK schema through this writer and through DAO, then dumps `MSysObjects`, `MSysACEs`, `MSysRelationships`, TDEF FK logical entries, and CompactDatabase output. Current empirical facts:

- `MSysRelationships` rows can match DAO exactly and still not be sufficient for DAO Compact & Repair. The per-table TDEF FK logical entries, backing real-index allocation metadata, relationship catalog object, relationship ACE rows, and system-table data-page placement all matter.
- Parent-side FK logical entries use `rel_tbl_type = 0x01`; child-side FK logical entries use `rel_tbl_type = 0x02`.
- `rel_idx_num` cross-references the partner logical-index number (`index_num`), not the partner real-index physical slot (`index_num2`).
- DAO prepends the FK logical entry before the existing `PrimaryKey` logical entry. The matching logical-index name list follows the same order.
- DAO names the parent-side FK logical entry with a hidden `.rB`-style name in the simple baseline; the child-side FK logical entry uses the public relationship name.
- Jet4/ACE FK backing real-index descriptors need a non-zero `used_pages` pointer. DAO FK enforcement failed when rebuilt descriptors had `used_pages = 0`, even though `first_dp` pointed at a valid B-tree. After patching `used_pages`, DAO rejected orphan inserts with error `-2146825087`.
- DAO-authored simple FK tables place table used/free rows and real-index rows in one usage-map page (`row 0`, `row 1`, then `row >= 2`). The writer now follows that shape for created/rebuilt real indexes; appending a separate index usage-map page after rebuild was not compact-safe.
- DAO creates a Type=8 `MSysObjects` row for the relationship under the relationships pseudo-object (`ParentId = 0x0F000003`) and relationship ACE rows. Writer-created FK compact preservation now emits both on create. Rename/drop deliberately leave the Type=8 row alone; `MSysRelationships` is the canonical source, and DAO Compact & Repair normalizes stale relationship object rows from it.
- System-table rows inserted after bootstrap must land on existing mapped system-table data pages when possible. DAO Compact & Repair trusts the system-table usage maps and can prune valid rows placed on unmapped appended pages.

## 4. Index page layout (page types `0x03` and `0x04`)

`0x04` = leaf, one entry per row. `0x03` = intermediate. For very small tables, `first_dp` can point directly at a `0x01` data page.

### 4.1 Page header

```text
1 byte   page_type      // 0x03 or 0x04
1 byte   unknown        // 0x01
2 bytes  free_space
4 bytes  parent_page    // TDEF pg for this idx
4 bytes  unknown(0)     // Jet4 / ACE ONLY — not present in Jet3 (shifts every field below by +4)
4 bytes  prev_page      // sibling at this level
4 bytes  next_page      // sibling at this level
4 bytes  tail_page      // pointer to tail leaf (PK-style append optimization)
2 bytes  pref_len       // shared-prefix length (compression)
```

Per-format absolute offsets (matches Jackcess `JetFormat`):

| Field        | Jet3 (`.mdb` Access 97) | Jet4 / ACE (`.mdb` 2000–2003, `.accdb`) |
|--------------|------------------------:|------------------------------------------:|
| `parent_page`| `0x04` (4)              | `0x04` (4)                                |
| `prev_page`  | `0x08` (8)              | `0x0C` (12)                               |
| `next_page`  | `0x0C` (12)             | `0x10` (16)                               |
| `tail_page`  | `0x10` (16)             | `0x14` (20)                               |
| `pref_len`   | `0x14` (20)             | `0x18` (24)                               |
| bitmask start| `0x16` (22)             | `0x1B` (27)                               |
| first entry  | `0xF8` (248)            | `0x1E0` (480)                             |

These constants are exposed as `Constants.IndexLeafPage.{Jet3,Jet4}{PrevPage,NextPage,TailPage,PrefLen,Bitmask,FirstEntry}Offset` and bundled into `IndexPageLayout` (`IndexPageLayout.Jet3` / `IndexPageLayout.Jet4`) so writer + reader code paths pick the correct offsets per format.

### 4.2 Entry-start bitmask

A bitmask immediately following the header marks where each entry begins.

- Jet3: bitmask starts at `0x16`, first entry begins at `0xF8`.
- Jet4: bitmask starts at `0x1B`, first entry begins at `0x1E0`.

The bitmask encodes 1 bit per byte of payload (LSB-first within each byte). The first entry is implicit (no bit set). A bit at position N indicates an entry begins at offset `(first_entry_offset + N)`.

**Sentinel bit (2026-05-04):** Access/DAO also writes a "one-past-the-end" sentinel bit at the position immediately after the last entry (i.e. at `payloadEnd - first_entry_offset`, where `payloadEnd = page_size - free_space`). This sentinel does NOT correspond to an actual entry — it marks the boundary between used and free space. DAO validates this sentinel during Compact & Repair and rejects pages that omit it. The sentinel is present on every non-empty leaf page in NorthwindTraders.accdb (verified across all leaf pages). `IndexPageCodec.BuildLeafPage` writes the sentinel; `IndexPageCodec.DecodeLeafEntries` excludes it from the scan by stopping at `payloadEnd`.

> **Jet3 confirmed empirically by W17a** ([`format-probe-appendix-jet3-index.md`](../format-probe/format-probe-appendix-jet3-index.md), `MSysAccessObjects.PrimaryKey` leaf page 24 of `indexTestV1997.mdb`): bitmask at offset `0x16`, first entry at `0xF8`, 9-byte stride for `LongInteger` int keys (1B flag + 4B BE int + 3B page + 1B row), and the bitmask byte pattern `00 02 04 08 10 20 40 80 …` matches the LSB-first 9-byte stride exactly.
>
> **The §4.1 page-header layout is NOT identical between Jet3 and Jet4.** Earlier revisions of this document incorrectly claimed it was. Jet4 / ACE inserts an extra 4-byte `unknown(0)` field at offset `0x08`, shifting `prev_page`, `next_page`, `tail_page`, and `pref_len` by +4 bytes. See the per-format offset table above; verified against `JetDatabaseWriter.Tests/Databases/NorthwindTraders.accdb` (page 2790, `MSysObjects.ParentIdName` middle leaf) and Jackcess `com.healthmarketscience.jackcess.impl.JetFormat`.

### 4.3 Entry record

```text
1 byte   flags          // 0x7F = ascending non-null, 0x80 = descending non-null,
                        // 0x00 = ascending null, 0xFF = descending null
n bytes  encoded key    // per-column, in column-map order; null cols contribute only the flag
3 bytes  data page      // page containing the row (24-bit, big-endian per HACKING.md)
1 byte   data row       // row number on that page
4 bytes  child page     // ONLY on 0x03 intermediate pages; this entry is the LAST entry
                        // on the referenced child page
```

> "0x80 is the one's complement of 0x7F and all text data in the index would then need to be negated. The reason for this negation is descending order." — HACKING.md

### 4.4 Prefix compression

JET leaf and intermediate pages compress entries via a single **page-shared prefix** held in the header `pref_len` field. An earlier revision of these notes hypothesised a second per-entry incremental compression scheme; that hypothesis was **incorrect** (see §4.4.2) — the symptom that motivated it was a 4-byte header offset bug in our writer (§4.1).

#### 4.4.1 Page-shared prefix (`pref_len` header field)

If the page header `pref_len` (Jet3 offset `0x14`, Jet4 / ACE offset `0x18`, u16 LE) is non-zero, the first `pref_len` bytes of the *first* entry are implicitly prepended to every subsequent entry on that page. So a 4-byte-prefix page could cut every int-key entry from 9 bytes (`flag + int32 + 3-byte page + 1-byte row`) to 5 bytes. `IndexPageCodec.BuildLeafPage` (writer) computes the longest byte-wise prefix common to every entry and emits this on `enablePrefixCompression: true`; `IndexPageCodec.DecodeLeafEntries` (reader) re-prepends those bytes to entries beyond the first.

#### 4.4.2 Per-entry incremental compression — hypothesis WITHDRAWN (2026-05-02)

A prior diagnostic session (2026-05-01) hypothesised that Access-authored leaves with `pref_len = 0` use a per-entry incremental prefix scheme stripping leading bytes shared with the *previous* entry. **That hypothesis was wrong.** The corrupt-looking entries observed on `MSysObjects.ParentIdName` (NorthwindTraders.accdb page 2790) were the result of our writer reading `pref_len` from the wrong byte offset — we were reading offset `0x14` (correct for Jet3) on a Jet4 / ACE page where `pref_len` actually lives at `0x18`. The byte at `0x14` on a Jet4 page is the low half of `tail_page`, which happened to be `0x00 0x00` for the inspected leaf, so the writer mis-computed `pref_len = 0` instead of the true `pref_len = 1` (or higher), then re-encoded entries beyond the first as if they carried the canonical prefix.

Fix shipped: `Constants.IndexLeafPage` exposes per-format header offsets, `IndexPageLayout` carries them (`PrefLenOffset`, `PrevPageOffset`, `NextPageOffset`, `TailPageOffset`), and every writer + reader call site (`IndexBTreeBuilder`, `IndexCursor`, `IndexPageCodec`, `IndexBTreeEditor`, and the `IndexMaintainer` path) reads/writes through the layout. See §4.1 for the corrected offset table.

No evidence of any per-entry incremental compression scheme has been found in Jackcess (`com.healthmarketscience.jackcess.impl.IndexData` only models the §4.4.1 page-shared prefix). Treat any future "unaccountable per-entry stripping" symptom as a strong signal of a header-offset / decode bug rather than a missing format feature.

### 4.5 Tail-page optimization

PK-style indexes that mostly append (e.g. autoincrement) maintain a `tail_page` pointer on the rightmost leaf. New leaf pages chain through this pointer until full, *then* propagate up the tree. A reader cursor that walks the index tree must follow `tail_page` after exhausting the normal traversal, otherwise it misses recent appends.

## 5. Sort-key encoding ("alphabetic sort order")

**Status**: text/memo encoding is production-routed for General Legacy via the Jackcess-derived code tables; fixed-type encodings are implemented in W2 / W12 / W13. A General 1033v1 helper and V2010 long-row suffix tables exist for fixture/probe coverage: the suffix is byte-exact for the checked-in Access-authored `Table11` / `Table11_desc` contexts and for the DAO-derived 65-character contribution tables covering the plain, auxiliary, row10, row11, and row12 suffix contexts. Routing writer-maintained text/memo indexes through General 1033v1 table collation metadata, broader arbitrary-context V2010 suffix coverage, and binary long-key suffix coverage remain future compatibility expansions.

Per HACKING.md the **General Legacy** encoding (Access 2000–2007, locale 1033 version 0):

| Char range | Encoded bytes |
|---|---|
| `0–9` | `0x56–0x5F` |
| `A–Z` | `0x60–0x79` |
| `a–z` | `0x60–0x79` (case-insensitive) |

Text key terminator: `0x00` (or `0xFF` when negated for descending).

Access 2010 introduced a **General** encoding (locale 1033 version 1) that is *different* from General Legacy. HACKING.md does not document the new encoding. The helper implementation uses the Jackcess port (`com.healthmarketscience.jackcess.impl.GeneralIndexCodes` plus the `index_codes_gen.txt` / `index_codes_ext_gen.txt` resource tables) and a table-driven V2010 long-row suffix provider derived from DAO-authored contribution matrices; production writer routing still needs table-collation selection before this can be treated as the maintained text/memo index path.

For non-text fixed types the encoding is conceptually big-endian with a sign-flip on the high bit:

- `int32`: write as big-endian; XOR the top byte with `0x80`. Then a leading `0x7F` (or `0x00` for null), trailing `0x00`.
- `int16`, `int8`, `int64`: same pattern at appropriate widths.
- `double`/`single`: IEEE-754 → big-endian → if positive, flip sign bit; if negative, complement all bits. Then `0x7F` prefix / `0x00` suffix.
- `currency` / `money`: int64 internally, encode as int64.
- `datetime`: stored as IEEE double (OA date), use the double encoding.
- `guid`: 16-byte big-endian display order packed into 9-byte segments (8 data bytes + 1 length byte: `0x09` for intermediates, the actual valid count for the final segment). Per Jackcess `IndexData.writeGeneralBinaryEntry`. See W12 in §7 for the full byte layout.
- `binary(≤255)`: variable-length raw bytes packed into 9-byte segments using the same general-binary-entry framing as `guid` — ⌈len/8⌉ segments, intermediate length byte `0x09`, final length byte = remaining valid count. Empty input emits one zero-length segment so empty values are byte-comparable. On descending the data bytes and the FINAL length byte are bit-flipped; intermediate `0x09` length bytes are NOT flipped. See W19 in §7.

> ⚠️ The exact byte sequences above are conventional B-tree sort-key encodings and match what Jackcess emits. They are **NOT** independently verified against this codebase's test fixtures and need to be byte-compared with `NorthwindTraders.accdb` index leafs before use.

Because text data is **mangled** by sort-key encoding, "covered queries" (queries answerable from index alone) are not possible against text columns. This is a JET property, not a library limitation.

## 6. `MSysIndexes` / `MSysIndexColumns` / `MSysRelationships` catalog tables

**Probe-confirmed across the full Jackcess corpus:** **88 of 89** `.mdb` and `.accdb` fixtures under `JetDatabaseWriter.Tests/Databases/` opened cleanly (the one failure is the deliberately password-protected `AesEncrypted.accdb`). **Zero** of those 88 contain `MSysIndexes` or `MSysIndexColumns`. The corpus spans Access 97, 2000, 2003, 2007, 2010, and 2019, plus the upstream Jackcess test suite (which includes fixtures named `indexTest*`, `bigIndexTest*`, `compIndexTest*`, `testIndexProperties*`, `testIndexCodes*`, `binIdxTest*`, `indexCursorTest*`, `testRefGlobalV2000` — i.e. files specifically authored to exercise index, FK, and relationship behaviour). See [`format-probe-appendix-index.md`](../format-probe/format-probe-appendix-index.md) for the ACCDB scan and [`format-probe-appendix-mdb-catalogs.md`](../format-probe/format-probe-appendix-mdb-catalogs.md) for the full 89-file headline + per-fixture verdict table. **Index metadata in every JET format this library targets lives entirely in the per-table TDEF block** (sections §3.1 / §3.2 above). A writer that emits the TDEF blocks correctly does NOT need to populate any index catalog tables for ACCDB or `.mdb` output.

`MSysRelationships` *does* exist in every fresh ACCDB **and** in every `.mdb` fixture probed (Jet3 + Jet4) and is required for cross-table foreign keys. The ACCDB schema is in [`format-probe-appendix-index.md`](../format-probe/format-probe-appendix-index.md#msysrelationships--tdef-page-5); the `.mdb` per-fixture catalog rows are in [`format-probe-appendix-mdb-catalogs.md`](../format-probe/format-probe-appendix-mdb-catalogs.md) (W9 implementer should re-probe the `.mdb` `MSysRelationships` TDEF for column-level layout before emitting rows for `.mdb` output).

| Catalog table | Status | Required by writer? | Notes |
|---|---|---|---|
| `MSysObjects` | Already written | yes | `AccessWriter` already emits this. |
| `MSysIndexes` | **Not present** in any of 88 fixtures (Access 97 → 2019, Jet3 / Jet4 / ACE) | no | Confirmed across the full Jackcess test corpus including fixtures explicitly authored to exercise indexes, FKs, and relationship behaviour. mdbtools docs that imply this table exists are misleading for every JET format this library writes. |
| `MSysIndexColumns` | **Not present** in any of 88 fixtures | no | Same. |
| `MSysRelationships` | Exists in every probed fixture (ACCDB + every `.mdb`) | yes for FKs | ACCDB column layout in [`format-probe-appendix-index.md`](../format-probe/format-probe-appendix-index.md#msysrelationships--tdef-page-5). `.mdb` schema not yet decoded at the column level — re-probe before W9 emits rows for `.mdb` output. |

## 7. Implementation phases (writer)

This is the historical phase log for writer-side index support; every phase below has shipped (see the per-row ✅ status). Early rows record what a phase did *at the time* — e.g. W2 lists the types it initially left as `NotSupportedException` — and later phases (W7/W12/W13/W19) lift those limits, so read the table as a cumulative history rather than a current limitation list. The reader-side foundation (R1+R2) these phases build on landed long ago.

| Phase | Scope | Cost (rough) | Status |
|---|---|---|---|
| **W1** | Plumb `IndexDefinition` model + `BuildTDefPage` extension to emit `num_real_idx > 0` with the 52-byte real-idx descriptors and 28-byte logical-idx entries | medium | ✅ **Implemented (2026-04-24)**. Single-column, non-unique, ascending, Jet4/ACE only. Round-trips through `ListIndexesAsync`. Originally shipped with `first_dp = 0` (no leaf pages); W3 below has since wired in an empty leaf per index and patches `first_dp` to point at it. See §7.1 below. |
| **W2** | Sort-key encoders for fixed types (int*, double, datetime, guid, currency). Skip text. | medium | ✅ **Implemented (2026-04-24).** `JetDatabaseWriter/Indexes/IndexKeyEncoder.cs`. Supports `Byte`, `Integer`, `LongInteger`, `Money`, `Float`, `Double`, `DateTime`. **Not** supported: `Guid`, `Numeric`, `Boolean`, text — each throws `NotSupportedException`. Per-type encodings match the conventional B-tree sort-key recipe in §5 (BE + sign-bit flip for signed integers; BE + sign-flip-or-complement for IEEE-754; ascending-then-ones-complement for descending). Round-trip ordering verified for every supported type via 17 unit tests in `JetDatabaseWriter.Tests/Indexes/IndexKeyEncoderTests.cs`. **Byte sequences have NOT been compared against a real Access-authored leaf** — see §8. |
| **W3** | Leaf-page (`0x04`) emitter: bulk-build a single leaf for tables that fit on one page. | medium | ✅ **Implemented (2026-04-24).** `JetDatabaseWriter/Indexes/IndexPageCodec.cs` (Jet4/ACE only). Emits the §4.1 page header (`page_type=0x04`, `parent_page=<TDEF>`, `prev/next/tail=0`, `pref_len=0`), the §4.2 entry-start bitmask (1 bit per entry beyond the first, LSB-first within each byte at offset `0x1B`), and §4.3 entry records (encoded key + 24-bit BE data page + 1-byte data row). Wired into `AccessWriter.CreateTableAsync` so each `IndexDefinition` allocates one **empty** leaf page at table-creation time and the leaf's page number is patched into the matching real-idx `first_dp` field. **No prefix compression, no splits, no maintenance hooks** — the leaf is empty at create time and any subsequent insert/update/delete leaves the index stale until Access rebuilds it on Compact & Repair. 5 unit tests in `JetDatabaseWriter.Tests/Indexes/IndexPageCodecLeafPageTests.cs` plus 2 round-trip tests in `IndexWriterTests.cs` (`CreateTable_WithIndex_EmitsLeafPageWithMatchingParent`, `CreateTable_WithMultipleIndexes_EmitsOneLeafPagePerIndex`). |
| **W4** | B-tree split on overflow: emit intermediate `0x03` pages, maintain `tail_page` chain. | high | ✅ **Implemented (2026-04-25).** `JetDatabaseWriter/Indexes/IndexBTreeBuilder.cs`. Pure builder over a sorted leaf-entry list — packs entries into one or more chained leaf pages (prev/next sibling pointers per §4.1) and emits zero or more levels of intermediate (`0x03`) pages until a single root remains. Each intermediate entry mirrors the last entry of its child plus a 4-byte child-page pointer (§4.3). Returns `(IReadOnlyList<byte[]> Pages, long RootPageNumber)` so the caller can append the pages and patch the root into the real-idx `first_dp`. **Sub-phase A shipped (2026-04-25): prefix compression** — `IndexPageCodec.BuildLeafPage` accepts an `enablePrefixCompression` flag (default `false` for direct callers) and `IndexBTreeBuilder.Build` opts in for both leaf (`0x04`) and intermediate (`0x03`) pages. The longest byte-wise prefix common to every entry on a page is hoisted into the §4.4 `pref_len` header field and stripped from every entry beyond the first; the first entry is always written whole. Packing decisions still use uncompressed entry sizes, so leaf/intermediate count is unchanged from the pre-compression layout — sub-phase A is a strict on-disk size win without affecting tree shape. **Sub-phase D shipped (2026-04-26): multi-level rebuild from existing tree** — `TryMaintainIndexesIncrementalAsync` now handles trees rooted at an intermediate (`0x03`) page by descending through `IndexPageCodec.ReadFirstChildPointer` to the leftmost leaf, walking the leaf-sibling chain via `IndexPageCodec.ReadNextPage`, splicing the insert/delete diff into the collected entry list, and rebuilding via `IndexBTreeBuilder`. The bulk-path fall-through that fired for every multi-level tree is gone. Leaf splits and merges propagate correctly through any number of intermediate levels because the rebuild emits a fresh, well-formed tree when needed; when the rebuilt tree still fits on the existing single leaf, the writer now reuses that leaf in place for Compact & Repair compatibility. **Superseded by W4-C surgical phases (2026-04-27):** in-place mutation for non-append multi-page change-sets now lives in §7.12–§7.18; `tail_page` chain shipped in W18 (2026-04-26). Driven from W5 `MaintainIndexesAsync`. Microsoft Access compact-and-repair validation now passes for the covered create/table/FK flows. 9 unit tests in `JetDatabaseWriter.Tests/Indexes/IndexBTreeBuilderTests.cs` plus 26 in `IndexPageCodecAndEntrySplicerTests.cs` and 10 round-trip tests in `IndexMaintenanceTests.cs` (3 of which exercise the new multi-level path). |
| **W5** | Index maintenance hooks in `InsertRowDataAsync`, `UpdateRowsAsync`, `DeleteRowsAsync`, and the copy-and-swap path used by `AddColumnAsync` / `DropColumnAsync` / `RenameColumnAsync`. | high | ✅ **Implemented.** Bulk-rebuild strategy: `MaintainIndexesAsync` snapshots all live rows, encodes & sorts keys via `IndexKeyEncoder`, rebuilds a B-tree via `IndexBTreeBuilder`, and patches the root into the real-idx `first_dp`. When a rebuilt tree still fits on the existing single leaf, that leaf is rewritten in place; larger rebuilds emit fresh pages, update the real-index usage-map row, then deallocate replaced old index pages through the global free-list allocator for ordinary user tables. Access system tables and generated complex flat child tables stay conservative because DAO CompactDatabase currently relies on their exact complex/system artifacts after schema rewrites. As of W19 (2026-04-26) every scalar JET column type Microsoft Access itself permits to index participates in the bulk rebuild: `Byte`, `Integer`, `LongInteger`, `Money`, `Float`, `Double`, `DateTime`, `Numeric` (W13), `Guid` (W12), `Binary` (W19), and `Text` / `Memo` across the entire BMP via the W7 General Legacy port. (`OLE Object`, `Attachment`, and `Multi-Value (Complex)` columns are not in this list because Microsoft Access itself does not permit indexes over them — `ResolveIndexes` rejects such `IndexDefinition`s with `NotSupportedException` per W18.) **Prefix compression** in emitted leaves and intermediates landed in W4 sub-phase A (2026-04-25). 7 round-trip tests in `IndexMaintenanceTests`. |
| **W6** | `MSysIndexes` / `MSysIndexColumns` catalog rows in `BuildEmptyDatabase`, gated on `DatabaseFormat`. | medium | ✅ **Closed as not-needed.** See §6 — zero of 88 fixtures across Access 97 → 2019 contain these tables; no work item. |
| **W7** | Text sort-key encoder (General Legacy first; General 1033v1 if a clean spec emerges). | high; defer if possible | ✅ **Implemented (2026-04-26).** Full Jackcess-port `GeneralLegacyTextIndexEncoder` replaces the ASCII-only stop-gap. Routes `Text (0x0A)` and `Memo (0x0C)` through a state machine that emits inline bytes + the EXTRA / UNPRINTABLE / CRAZY auxiliary streams with `END_TEXT (0x01)` / `END_EXTRA_TEXT (0x00)` framing exactly as Jackcess does, using the per-codepoint resource tables `IndexCodeTables/index_codes_genleg.txt.gz` (BMP `U+0000`–`U+00FF`) and `IndexCodeTables/index_codes_ext_genleg.txt.gz` (`U+0100`–`U+FFFF`) embedded as gzipped resources. Trailing-space stripping and the 127-char (`MAX_TEXT_INDEX_CHAR_LENGTH = 255 / 2`) prefix truncation match Jackcess. Descending support inverts the payload + END_EXTRA_TEXT in place and appends an unflipped trailing `0x00`; the leading flag is never flipped. Surrogate pairs fall back to the documented `0x3F` extra byte. Single- and multi-column text/memo keys participate in the W5 bulk B-tree rebuild and W11 multi-column composite key concatenation, including descending text columns (the previous fall-through path is gone). **Remaining:** General 1033v1 production routing and secondary collation weights beyond what the General Legacy resource table encodes. See §7.6. Apache 2.0 attribution recorded in the top-level `THIRD-PARTY-NOTICES.md`. |
| **W8** | Primary-key API (`new ColumnDefinition(...) { IsPrimaryKey = true }` plus a multi-column variant). PK is a unique non-null logical index with `index_type = 0x01`. | small (depends on W1) | ✅ **Implemented.** Two equivalent declaration paths: the `ColumnDefinition.IsPrimaryKey` shortcut (synthesizes a single PK `IndexDefinition` named `"PrimaryKey"`) and an explicit `new IndexDefinition(name, columns) { IsPrimaryKey = true }`. Logical-idx `index_type = 0x01`; per §3.1 the real-idx `flags` byte stays at `0x00` (uniqueness lives in the discriminator). PK key columns are forced non-nullable. Mixing the column-flag shortcut with an explicit PK `IndexDefinition`, or declaring more than one PK, throws `ArgumentException`. **Both single- and multi-column PKs participate in W5 / W11 live B-tree leaf maintenance** — the W11 composite-key concatenation path covers PK uniqueness via the `MaintainIndexesAsync` logical-idx-walk that promotes any real-idx referenced by an `index_type == 0x01` entry to unique. The W8 "multi-column PKs ship the schema only" caveat was lifted by W26 (2026-04-27); see §7.16. 16 round-trip tests in `IndexPrimaryKeyWriterTests`. |
| **W9** | Foreign-key / relationship API. Adds the 28-byte logical-idx entry with `rel_idx_num` / `rel_tbl_page` populated, plus the `MSysRelationships` row. | medium | ✅ **W9a + W9b shipped** — `MSysRelationships` row emission (W9a) plus per-TDEF FK logical-idx entries on both PK-side and FK-side TDEFs (W9b, Jet4/ACE only). See §7.2. |
| **W10** | Cascade flags (`cascade_ups`, `cascade_dels`) and FK enforcement on insert/update/delete. The library has no SQL engine, so enforcement is a runtime check inside `AccessWriter`. | medium | ✅ **Implemented.** Runtime FK enforcement on `InsertRowAsync` / `InsertRowsAsync` (object\[\] and generic), `UpdateRowsAsync`, `DeleteRowsAsync`. Inserts validate FK-side parent presence (null components are allowed); updates re-validate FK-side and either cascade PK changes to dependents or throw based on `CascadeUpdates`; deletes either cascade or throw based on `CascadeDeletes`. Cascade depth capped at 64. Parent snapshots cached per public call (`FkContext`). Relationships with `EnforceReferentialIntegrity = false` are silently excluded. 11 tests in `ForeignKeyEnforcementTests`. |
| **W11** | Lift the W1-era restrictions on `IndexDefinition`: multi-column non-PK indexes, `IsUnique` (real-idx `flags & 0x01`), and `DescendingColumns` (per-column ascending flag cleared in `col_order`). Live W5 maintenance handles composite keys and per-column direction; unique violations throw `InvalidOperationException` after the bulk rebuild. | medium | ✅ **Implemented (2026-04-25).** Public surface adds `IndexDefinition.IsUnique` + `IndexDefinition.DescendingColumns`. `ResolveIndexes` accepts multi-column non-PK declarations. `BuildTDefPageWithIndexOffsets` writes `col_order = 0x01` for ascending slots and clears the ascending bit (`0x00`) for descending slots, then sets the real-idx `flags` byte to `0x01` for non-PK unique indexes (PK uniqueness still uses the §3.1 logical-idx `index_type` discriminator with `flags = 0x00`). `MaintainIndexesAsync` was rewritten to read every populated col_map slot, encode each column with the slot's direction, concatenate to a composite key, and (when the real-idx flag bit OR the logical-idx PK discriminator marks the index unique) detect adjacent-duplicate keys after the sort. `RewriteTableAsync` projection forwards `HasUniqueFlag` + `DescendingColumns` for both the rename-column path and the add/drop-column copy-and-swap path. Pre-write unique enforcement is superseded by W15; the previous "descending text indexes go stale" note is **resolved** as of W7 (2026-04-26) — descending text/memo keys now round-trip through the full Jackcess encoder. DAO CompactDatabase now covers representative mixed-direction composite text/integer indexes in `AdvancedIndexKeysAndBTreeMaintenance_SurviveCompactAndRepair`; byte-for-byte comparison against an Access-authored descending leaf remains a separate probe gap. 15 round-trip tests in `IndexWriterAdvancedTests`. See §7.6. |
| **W12** | Extend `IndexKeyEncoder` to support `Guid (0x0F)` so single- or multi-column indexes whose key column is a GUID participate in the W5 live B-tree rebuild instead of falling through to the stale-leaf path. | small | ✅ **Implemented (2026-04-25).** `IndexKeyEncoder.EncodeGuidEntry` emits a 19-byte block: leading flag (`0x7F` asc / `0x80` desc) + two 9-byte segments (8 GUID display-order data bytes + length byte). On descending the data bytes and the FINAL length byte are bit-flipped but the intermediate `0x09` length byte is **NOT** flipped, matching Jackcess `IndexData.writeGeneralBinaryEntry`. GUID display order is `byte 3,2,1,0,5,4,7,6,8..15` of the in-row storage layout (which is `Guid.ToByteArray()`-shaped). 7 unit tests in `IndexKeyEncoderTests` plus 2 round-trip tests in `IndexWriterAdvancedTests` (`GuidIndex_BulkInsert_RebuildsLeafWithExpectedEntryCount`, `UniqueGuidIndex_DuplicateInsert_Throws`). DAO CompactDatabase now covers a representative GUID-keyed index in `AdvancedIndexKeysAndBTreeMaintenance_SurviveCompactAndRepair`; byte-for-byte comparison against an Access-authored GUID-keyed leaf remains a probe gap. |
| **W13** | Extend `IndexKeyEncoder` to support `Numeric (0x10)` (Decimal columns) so Decimal-keyed indexes participate in the W5 live B-tree rebuild instead of falling through to the stale-leaf path. | medium | ✅ **Implemented (2026-04-25; descriptor-scale path corrected by W23).** `IndexKeyEncoder.EncodeNumericEntry` produces a 1-byte flag + 17-byte payload (1 sign byte + 16-byte big-endian unsigned mantissa) using the Jackcess `FixedPointColumnDescriptor` (ACCDB / ACE) and `LegacyFixedPointColumnDescriptor` (Jet4 `.mdb`) byte-twiddling rules — see §7.4. Current rebuild and incremental paths call `EncodeNumericEntryAtDeclaredScale`, so the canonical sort-key scale is the column descriptor's declared `NumericScale`, matching Access row storage; values with more fractional digits are rounded half-to-even before encoding. Mantissas that exceed 16 bytes after rescale throw `NotSupportedException`, which the W5 catch swallows to fall through to the stale-leaf path. Cross-sign descending order under the legacy form preserves the documented Jackcess MS KB 837148 quirk (negatives sort before positives in lex regardless of magnitude). DAO CompactDatabase now covers a representative NUMERIC-keyed index and descriptor-scale row storage in `AdvancedIndexKeysAndBTreeMaintenance_SurviveCompactAndRepair`; byte-for-byte comparison against an Access-authored NUMERIC-keyed leaf remains a probe gap. 12 unit tests in `IndexKeyEncoderTests` plus numeric round-trip/incremental coverage in `IndexWriterAdvancedTests` and `IndexNumericKeyIncrementalTests`. |
| **W14** | `DropRelationshipAsync` and `RenameRelationshipAsync` — reverse of W9a/W9b for catalog rows; FK logical-idx entries on both side TDEFs are also removed by Drop. | small | ✅ **Implemented (2026-04-25, extended by W26 2026-04-27, wide-TDEF mutation 2026-05-21, and DAO compact hardening 2026-05-23).** `DropRelationshipAsync(string)` rewrites `MSysRelationships` as the remaining live rows, excluding every matching `szRelationship`, and, for Jet4/ACE, removes the matching FK logical-idx entry from each side's TDEF (matched by `index_type == 0x02` + `rel_tbl_page` + exact backing real-idx `col_map`). Rewriting the system table live-only avoids relationship tombstones that DAO relationship enumeration and CompactDatabase reject. **W26 (2026-04-27):** trailing real-idx physical-descriptor slots that the removal left unreferenced are reclaimed via `TryReclaimTrailingRealIdxAsync`; mid-array orphans are still left for Compact & Repair (renumbering cross-tdef `rel_idx_num` references is out of scope). **Wide-TDEF mutation (2026-05-21):** drop/reclaim now operate on stitched logical TDEF buffers, so wide endpoint TDEF chains are handled; if a shrink no longer needs every old continuation page, the unreachable continuation pages are deallocated through the global free-list allocator after the rewritten chain is flushed. `RenameRelationshipAsync(old, new)` rejects collisions case-insensitively (via `ReadExistingRelationshipNamesAsync`), rewrites `MSysRelationships` as live rows with `szRelationship` replaced on every match, and short-circuits the no-op same-name path. The per-TDEF logical-idx name cookies on both sides are rewritten through the same logical-buffer layer so `IndexMetadata.Name` reflects the new name immediately on re-open, including when the rename grows into a continuation page. Type=8 relationship `MSysObjects` rows are emitted on create but are not manually renamed or deleted on mutation; DAO Compact & Repair normalizes them from `MSysRelationships`. 12 round-trip test methods in `RelationshipMutationTests`; DAO CompactDatabase coverage for shortened-chain reclamation and rename on a multi-page child TDEF lives in `DaoStorageMaintenanceTests`. |
| **W15** | Pre-write unique-index enforcement: hoist the W11 post-write duplicate-key check ahead of the row encode so the offending row never hits disk. Single insert, batch insert (object\[\] + generic), and update paths all participate. | small | ✅ **Implemented (2026-04-25).** Helpers `LoadUniqueIndexDescriptorsAsync`, `EncodeCompositeKeyForUniqueCheck`, `CheckUniqueIndexesPreInsertAsync`, `CheckUniqueIndexesPreUpdateAsync`, `CheckUniqueIndexesCore` on `UniqueIndexChecker` and called from `AccessWriter`. The pre-write check parses the TDEF for unique real-idx slots (real-idx `flags & 0x01` OR an associated logical-idx with `index_type = 0x01`), encodes a composite key per effective row using the same `IndexKeyEncoder` path the W5 bulk rebuild uses (including descriptor-scale NUMERIC encoding from W23), and throws `InvalidOperationException` on the first existing-vs-pending or pending-vs-pending collision. `InsertRowsAsync` materialises the batch up front so intra-batch duplicates are detected before the first `InsertRowDataLocAsync` call. `UpdateRowsAsync` reuses its already-loaded snapshot and substitutes the post-update payloads at the matching row indices before encoding. The W11 post-write check inside `MaintainIndexesAsync` is retained as defense-in-depth (it still fires for indexes whose encoder rejects the pre-write encode — e.g. text outside General Legacy). 7 round-trip tests in `IndexPreWriteUniqueEnforcementTests`. See §7.5. |
| **W16** | Replace the W10 O(N) parent-snapshot scan in `EnforceFkOnInsertAsync` with an O(log N) B-tree seek over the parent's PK / FK index. Insert path only; cascade-update and cascade-delete on the parent (W10 child-table scan) still load a snapshot. | small | ✅ **Implemented (2026-04-26).** `IndexCursor` walks the parent's real-idx `first_dp` through any number of `0x03` intermediate pages down to the matching `0x04` leaf, honouring the §4.2 entry-start bitmask and §4.4 prefix compression (re-prepends the leaf's `pref_len` bytes to every entry beyond the first; intermediate descent picks the first child whose summary key is ≥ the search key). `AccessWriter.EnforceFkOnInsertAsync` resolves a per-relationship `ParentSeekIndex` (real-idx slot + per-column ascending flags + foreign-table column indexes), encodes the FK row's composite key via the existing `IndexKeyEncoder`, and seeks. Resolution is cached on `FkContext.SeekIndexes` so a bulk insert hits the TDEF lookup once per relationship per call. `CreateRelationshipAsync` now calls `MaintainIndexesAsync` on both sides after `EmitFkPerTdefEntriesAsync` so pre-existing parent rows are visible to the cursor (the W9b empty leaf is replaced by a populated B-tree). Self-referential bulk inserts are still served correctly: `EnforceFkOnInsertAsync` first checks the per-call pending-key set populated by `AugmentParentSetsAfterInsert` so a row that satisfies its own FK against an earlier row in the same batch succeeds without waiting for the next index rebuild. Falls back to the W10 HashSet path on Jet3 (no index emission), when no covering parent real-idx exists, when `first_dp == 0`, when the TDEF spans multiple pages, or when the descriptor-aware seek encoder rejects a key component. Descriptor-scale `Numeric` relationship keys were routed into this seek path on 2026-05-22. **Cascade-side seek superseded by W22 (2026-04-27)** — `EnforceFkOnPrimaryUpdateAsync` / `EnforceFkOnPrimaryDeleteAsync` now also use index seek via the FK-side child real-idx; see §7.11. 3 new round-trip tests in `ForeignKeyEnforcementTests` (`Insert_LargeParentTable_SeekPathFindsExistingKey`, `Insert_TextKeyFk_SeekPathHonoursGeneralLegacyEncoding`, `Insert_BulkChildren_SeekPathReusesParentIndexAcrossRows`); the existing 11 W10 tests pass unchanged. **Validation gap:** the cursor reads bytes this library wrote; it has not yet been pointed at an Access-authored leaf (no fixture in the repo carries an FK index over a small enough column count to compare cheaply). See §8. |
| **W22** | Replace the W10 / W16 child-table O(N) snapshot scan inside `EnforceFkOnPrimaryUpdateAsync` and `EnforceFkOnPrimaryDeleteAsync` with an O(log N + K) B-tree seek over the child-side FK real-idx (the same per-TDEF entry W9b emits on every relationship). | small | ✅ **Implemented (2026-04-27; descriptor-scale NUMERIC seek/row decoding added 2026-05-22).** `IndexCursor.FindRowLocationsAsync` adds row-pointer enumeration on top of the W16 exists-only descent — collects every `(dataPage, rowIndex)` pair whose canonical key matches, including the sibling-leaf walk for non-unique keys. `AccessWriter` adds `ChildSeekIndex` / `ChildSeekKeyColumn` records, `ResolveChildSeekIndexAsync` (mirror of `ResolveParentSeekIndexAsync` against the child TDEF), `TryEncodeChildSeekKey` (encodes a typed parent-PK tuple via the descriptor-aware `IndexKeyEncoder` path; `Numeric` uses `EncodeNumericEntryAtDeclaredScale`; returns null on encoder rejection), and `TryReadColumnValuesTypedAsync` (single-row decoder over `EnumerateLiveRowBounds`-resolved bounds; returns null if any in-scope column is `Memo` / `Ole` / `Complex` / `Attachment` because those require LVAL/complex traversal). `EnforceFkOnPrimaryDeleteAsync` signature changed from `(string, IReadOnlyList<string?>, ...)` to `(string, TableDef primaryDef, List<object?[]>, ...)` so the seek path can encode typed parent PKs; `EnforceFkOnPrimaryUpdateAsync` extended its `changes` tuple to carry `OldFullRow` so the OLD typed PK is available for child-side seek. Both call `TryProcessCascadeXxxWithSeekAsync` BEFORE the snapshot fallback and return `bool` so the snapshot path runs only when seek cannot complete. Resolution cached on `FkContext.ChildSeekIndexes` (parallel to W16's `FkContext.SeekIndexes`). Falls back to the legacy O(N) child snapshot on Jet3 (no FK-side real-idx emission), when no covering child real-idx exists, when the FK key encoder rejects any component, or when the child row contains LVAL/complex columns the single-row reader cannot decode (cascade-delete recursion needs the typed full row to resolve grandchildren). **CRITICAL implementation note (regression seen during dev):** the snapshot-fallback `deletedSet` keys MUST be built with `BuildCompositeKey(pk, identity)` (where `identity[i] = i`), not an ad-hoc helper — `BuildCompositeKey` applies `'S:UPPER'` for case-insensitive text, `'N:decimal'` for cross-width numerics, `'D:UTC-ticks'` for `DateTime`, `'G:HEX'` for `Guid`, `'B:base64'` for `byte[]`, and `'?:0/1'` for booleans, all separated by `'|'`. Reinventing the normalisation produces non-matching keys and silently misses cascade victims. 2 new round-trip tests in `ForeignKeyEnforcementTests` (`Delete_PkSide_WithCascade_BulkSeeksChildIndex`, `Update_PkSide_WithCascade_BulkSeeksChildIndex`); the existing 14 W10/W16 tests pass unchanged. See §7.11. |
| **W19** | Extend `IndexKeyEncoder` to support `Binary (0x09)` so single- or multi-column indexes whose key column is a variable-length binary (≤ 255 bytes) participate in the W5 live B-tree rebuild and the W16 parent-PK seek path. | small | ✅ **Implemented (2026-04-26).** `IndexKeyEncoder.EncodeBinaryEntry` accepts `byte[]` (and `ArraySegment<byte>` / `ReadOnlyMemory<byte>` / `Memory<byte>`) and routes through a shared `EncodeGeneralBinaryEntry(ReadOnlySpan<byte>, bool ascending)` packer factored out of the W12 GUID path — both produce byte-identical output for 16-byte input, so the GUID byte sequence and existing GUID tests are unchanged. Variable-length: ⌈len/8⌉ 9-byte segments (8 zero-padded data bytes + 1 length byte: `0x09` for intermediate segments, the actual valid-byte count for the final segment). Empty input emits one zero-length final segment so empty values round-trip and sort below any non-empty value. Descending bit-flips data bytes and the FINAL length byte; intermediate `0x09` length bytes are NOT flipped (matches Jackcess `IndexData.writeGeneralBinaryEntry`). `Binary` was added to `IndexKeyEncoder.IsColumnTypeSeekable` so W16 parent-PK seeks engage on binary-keyed FKs (Jet4 / ACE; Jet3 still routes via the HashSet fallback). 9 unit tests in `IndexKeyEncoderTests` (null, empty, 3 bytes, exactly 8, 9-byte multi-segment, descending flip rules, ascending + descending lex ordering, non-`byte[]` rejection) plus 4 round-trip tests in [`IndexBinaryKeyTests`](../../JetDatabaseWriter.Tests/Indexes/IndexBinaryKeyTests.cs) (bulk insert, unique-violation detection, multi-column composite Text+Binary, descending direction). DAO CompactDatabase now covers a representative BINARY-keyed index in `AdvancedIndexKeysAndBTreeMaintenance_SurviveCompactAndRepair`; byte-for-byte comparison against an Access-authored BINARY-keyed leaf remains a probe gap. See §7.10 below. |
| **W17** | Lift the "Jet3 (`.mdb` Access 97) rejects `IndexDefinition` entirely" limitation. Pin the Jet3 real-idx (39 B), logical-idx (20 B), and leaf-page (`0x04`) layouts empirically; emit them from `BuildTDefPageWithIndexOffsets`; route Jet3 leaf builds through `IndexPageCodec` / `IndexBTreeBuilder` (page size 2048, bitmask at `0x16`, first entry at `0xF8`); thread Jet3 through `MaintainIndexesAsync` and the W15 unique pre-check. | medium | ✅ **W17a + W17b + W17c + W17d shipped.** **W17a (probe + spec) shipped (2026-04-26):** [`JetDatabaseWriter.FormatProbe`](../../JetDatabaseWriter.FormatProbe/Program.cs) `WriteJet3IndexAppendixAsync` dumps the TDEF + first leaf page for every user table with `numIdx > 0` across 6 Jet3 fixtures (`indexTestV1997.mdb`, `compIndexTestV1997.mdb`, `testIndexCodesV1997.mdb`, `testV1997.mdb`, `mdbtools/nwind.mdb`, `Jet3Test.mdb`) into [`format-probe-appendix-jet3-index.md`](../format-probe/format-probe-appendix-jet3-index.md). Output confirmed (i) **`first_dp` lives at phys-desc bytes 34..37 on Jet3** — *not* in the leading 8-byte skip-entry block as the mdbtools spec implies for Jet4-style `used_pages`; (ii) `index_type = 0x01` PK discriminator at logical-idx byte 19; (iii) §4.2 leaf bitmask at `0x16` / first entry at `0xF8` with the 9-byte stride for `LongInteger` keys matching the bitmask LSB-first pattern exactly; (iv) §4.1 page header is identical between Jet3 and Jet4. Spec corrections landed in §3.1 ("Jet3 real-idx physical descriptor"), §3.2 ("Jet3 logical-idx entry"), and §4.2 ("Jet3 confirmed empirically"). **W17b (TDEF + schema-only leaf emission) shipped (2026-04-26):** the `Jet3Mdb` rejection in `AccessWriter.CreateTableAsync` is gone. `BuildTDefPageWithIndexOffsets` now branches on format and emits the §3.1 39-byte real-idx (no `used_pages` slot, `first_dp` at phys+34, flags at phys+38) and the §3.2 20-byte logical-idx (no leading cookie / trailing tail; `index_num`/`index_num2`/`rel_idx_num`/`index_type` at the shifted-left offsets). `IndexPageCodec.BuildLeafPage` emits a single empty leaf per index — page size 2048, header identical to Jet4, bitmask at `0x16` (no bits set on an empty page), first-entry slot at `0xF8`, `free_space = pageSize - 0xF8`. `CreateTableInternalAsync` patches `first_dp` exactly the same way as Jet4. At the W17b checkpoint the Jet3 leaf was schema-only and stayed empty until Access rebuilt it on Compact & Repair; W17c below superseded that interim state by adding live Jet3 maintenance. Reader bugfix: `AccessReader.ParseIndexMetadata` was reading `index_num` / `index_num2` from Jet4 offsets even on Jet3; the offsets are now shifted left by 4 bytes per §3.2, restoring round-trip through `ListIndexesAsync`. 5 round-trip tests in [`IndexWriterTests`](../../JetDatabaseWriter.Tests/Indexes/IndexWriterTests.cs) cover single index, primary key (via `ColumnDefinition.IsPrimaryKey` shortcut), three-index emission, leaf-page byte-pattern (page-type `0x04` count + `free_space = 2048 - 0xF8`), and the no-indexes regression path. **W17d shipped (2026-04-26):** `TryMaintainIndexesIncrementalAsync` and the helper `DescendToLeftmostLeafAsync` now branch on `_format == DatabaseFormat.Jet3Mdb` and select `IndexPageLayout.Jet3` for `IndexBTreeBuilder.Build`, `IndexPageCodec.DecodeLeafEntries`, `IndexPageCodec.TryBuildLeafPage`, and `IndexPageCodec.ReadFirstChildPointer`. The §3.1 39-byte real-idx physical-descriptor walk uses `realIdxPhysSz = 39` and `physFirstDpOff = 34` instead of the Jet4 52 / 38. The W15 pre-write unique-index check (`LoadUniqueIndexDescriptorsAsync`) was extended in the same commit so the W11 post-write duplicate detection — previously the only safety net on Jet3, and skipped by the incremental path — is no longer relied on; PK / unique violations are caught before disk mutation via the §3.2 20-byte logical-idx layout (`logIndexNum2Off = 4`, `logIndexTypeOff = 19`). 2 new round-trip tests in [`IndexMaintenanceTests`](../../JetDatabaseWriter.Tests/Indexes/IndexMaintenanceTests.cs) (`IncrementalFastPath_SplicesSingleLeaf_OnInsertAndDelete`, `IncrementalFastPath_RebuildsMultiLevelTree`) — the latter forces an intermediate (`0x03`) root by bulk-loading 400 INT rows then splicing one more, and asserts the post-rebuild file contains at least one `0x03` page and the reader sees all 401 rows. **W17c (live leaf maintenance) shipped (2026-04-26):** `IndexPageLayout` (Jet3 / Jet4 static instances) parameterises `IndexPageCodec`, `IndexBTreeBuilder.Build`, `IndexPageCodec.DecodeLeafEntries / IndexPageCodec.TryBuildLeafPage / IndexPageCodec.ReadFirstChildPointer`, and the writer's create-time + bulk-maintain paths. `MaintainIndexesAsync` no longer short-circuits on `Jet3Mdb` — it reads the §3.1 39-byte real-idx + §3.2 20-byte logical-idx with per-format sizes (`realIdxPhysSz`, `logIdxEntrySz`) and offsets (`physFirstDpOff = 34`, `physFlagsOff = 38`, `logIndexNum2Off = 4`, `logIndexTypeOff = 19`) selected via a `bool jet3` switch, then drives `IndexBTreeBuilder.Build` with `IndexPageLayout.Jet3`. The W15 pre-write unique pre-check participates because the same TDEF-walk helper now reads PK discriminators from the Jet3 logical-idx at the shifted-left offsets. 7 round-trip tests in [`IndexMaintenanceTests`](../../JetDatabaseWriter.Tests/Indexes/IndexMaintenanceTests.cs) cover insert (single + bulk), update, delete (latest-leaf entry-count check), PK duplicate enforcement, text-key (General Legacy through Jet3), and AddColumn round-trip. The `LimitationsTests.SchemaEvolution_*` pinned tests still pass. The README no longer lists a Jet3 index-maintenance limitation. See §7.7 below. |

### 7.1 W1 — what shipped (2026-04-24)

Kept as the foundational reference for every later writer phase.

Public surface added on `IAccessWriter` / `AccessWriter`:

- `IndexDefinition(string name, string columnName)` — single-column constructor; `Columns` is fixed at length 1.
- `CreateTableAsync(string tableName, IReadOnlyList<ColumnDefinition> columns, IReadOnlyList<IndexDefinition> indexes, CancellationToken ct)` — new overload. The original 3-arg overload is now a thin wrapper that passes `Array.Empty<IndexDefinition>()`.

What the writer emits into the new TDEF page (Jet4/ACE):

- `num_idx` (4 bytes at `_tdNumCols + 2`) = `indexes.Count`.
- `num_real_idx` (4 bytes at `_tdNumRealIdx`) = `indexes.Count` — W1 issues **one real-idx per logical-idx**, no sharing (§3.3 sharing is a W9-era optimization).
- Leading real-idx entry block (`numRealIdx × 12` bytes Jet4) — left zeroed; mdbtools labels every field there `unknown`.
- Per index, the 52-byte real-idx physical descriptor (§3.1) with:
  - `col_map` slot 0 = `(col_num, 0x01)` (ascending), slots 1–9 = `(0xFFFF, 0x00)`.
  - `used_pages = 0`, `first_dp` patched by W3 to point at the empty leaf page allocated per index in `CreateTableAsync`.
  - `flags = 0x00`, trailing 9 bytes zeroed.
- Per index, the 28-byte logical-idx entry (§3.2) with `index_num = i`, `index_num2 = i`, `rel_idx_num = 0xFFFFFFFF`, `index_type = 0x00` (Normal), cascade flags 0.
- Logical-idx name list (UTF-16 LE length-prefixed, same encoding as column names).
- `tdef_len` (offset 8) updated to span the full block including the new index sections.

W1 explicit constraints (enforced by `ResolveIndexes`):

- Single column per index. `IndexDefinition.Columns.Count != 1` throws `NotSupportedException`. (Multi-column indexes are accepted from W8 PKs and W9b FKs; the W1 `IndexDefinition` public constructor itself is single-column.)
- Unknown column name throws `ArgumentException`.
- Duplicate index names (case-insensitive) throw `ArgumentException`.
- Historical W1 constraint: Jet3 (`.mdb` Access 97) with non-empty `indexes` originally threw `NotSupportedException`. W17 later lifted this by adding the Jet3 real-idx, logical-idx, and leaf-page layouts; keep this subsection as the original W1 reference, not as current behavior.

Validation: round-trip tests in `IndexWriterTests`. This W1 reference predates the broader DAO compact matrix; use [writer-disk-format-validation-matrix.md](writer-disk-format-validation-matrix.md) for the current strongest validation level.

### 7.2 W2–W10 — additional notes

The per-phase summaries in the W-table above carry the public-surface delta and the explicit caller constraints. The implementation details (helper names, internal layout, test counts) live in the C# XML-doc summaries on the helpers themselves — grep for the helper name in `JetDatabaseWriter/AccessWriter.cs` and its domain modules (`Indexes/`, `Schema/`, `Catalog/`, `ValueEncoding/`, `Transactions/`, `Relationships/`). Notes that don't fit naturally into either:

- **W9a `grbit` bit values** (`0x00000002 = NO_REFERENTIAL_INTEGRITY`, `0x00000100 = CASCADE_UPDATES`, `0x00001000 = CASCADE_DELETES`) are taken from Jackcess `RelationshipImpl` and have not been independently re-verified against an Access-authored fixture. The format-probe appendix does not yet decode `grbit` semantics.
- **W9b** mutates two endpoint TDEFs per call. The per-TDEF emission is gated on `_format != Jet3Mdb`; on Jet3, only the W9a `MSysRelationships` rows ship. Jet4 / ACE endpoints may now be single-page or multi-page TDEF chains. `RelationshipManager` reads the full chain into the same stitched logical-buffer shape as `ReadTDefBytesAsync`, applies FK logical-idx append/remove/rename/reclaim shifts in logical offsets, then materialises the resized buffer back into a physical page chain. Growth appends continuation pages as needed; shrink deallocates old continuation pages that are no longer reachable after the rewritten chain is flushed. `RelationshipWriterTests.CreateRelationshipAsync_MultiPageEndpointTDef_EmitsFkLogicalIdxEntries` covers parent-wide and child-wide create paths. Real-idx sharing per §3.3 reuses any existing slot whose `col_map` matches the relationship's column list exactly.
- **W9b FK-entry forwarding through schema evolution.** `AddColumnAsync` / `DropColumnAsync` / `RenameColumnAsync` use the copy-and-swap path that only forwards user-declared `IndexDefinition` records; W9b FK logical-idx entries are dropped from rewritten TDEFs. The `MSysRelationships` rows survive, and Access regenerates the per-TDEF entries on the next Compact & Repair pass.
- **W9b multi-page implementation notes.** The create path still preserves the W9b ordering guarantees: plan both sides from an original snapshot, allocate any new leaf pages before patching `first_dp`, emit both cross-referenced logical entries, then rebuild affected real indexes after TDEF materialisation where the existing index-maintenance path can handle the endpoint. Drop and rename use the same logical-buffer layer; `RelationshipMutationTests.DropRelationshipAsync_MultiPageEndpointTDef_RemovesFkLogicalIdxEntries` and `RenameRelationshipAsync_MultiPageEndpointTDef_UpdatesTDefLogicalIdxNameCookies` cover both parent-wide and child-wide endpoints.
- **W10 composite-key normalisation** (`AccessWriter.BuildCompositeKey`): strings are upper-cased via `ToUpperInvariant`; `Guid` is N-format hex; `byte[]` is Base64; `DateTime` is `ToUniversalTime().Ticks`; `IConvertible` numerics route through `decimal` so cross-width values compare equal. A null component makes the whole composite key null and the row is allowed without a parent lookup. As of W16 (2026-04-26) this normalisation is only consulted by the HashSet fallback path (Jet3, missing parent index, encoder-rejected key types) and by the per-call pending-key set that handles self-referential bulk inserts; the primary insert-side path encodes via `IndexKeyEncoder.EncodeEntry` and seeks the parent B-tree directly.
- **W10 caveats:** insert-side enforcement is an O(log N) parent-index seek (W16) on Jet4/ACE; cascade-update / cascade-delete on the parent are an O(log N + K) child-side index seek (W22) on Jet4/ACE. Both fall back to an O(N) snapshot on Jet3, when no covering real-idx exists, when the encoder rejects a key component, or — for cascade — when child rows contain LVAL columns the single-row reader cannot decode. No SET NULL / SET DEFAULT cascade actions; no transaction or rollback; no FK enforcement on `RewriteTableAsync`.
- **W11 unique enforcement.** Originally W11 only ran a post-write duplicate-key check inside `MaintainIndexesAsync` — the offending row hit disk before the throw. **W15 (2026-04-25)** added pre-write enforcement so the conflict is detected before any row is encoded; the post-write check is retained as defense-in-depth and only fires for indexes whose encoder rejects the pre-write encode (text outside General Legacy, etc.). See §7.5.
- **W11 descending `col_order`** clears Jackcess' `ASCENDING_COLUMN_FLAG` bit (`0x01`). The writer emits `0x01` for ascending and `0x00` for descending; DAO CompactDatabase preserves the representative mixed-direction indexes covered by `AdvancedIndexKeysAndBTreeMaintenance_SurviveCompactAndRepair`. Byte-for-byte comparison against an Access-authored descending leaf remains a probe gap.
- **W11 silently falls through** the W5 stale-leaf path whenever any column in a composite key is unsupported by `IndexKeyEncoder` — same model as the single-column W5 path. As of W19 (2026-04-26) this only applies to indexes whose key column is `Numeric` and whose mantissa exceeds 16 bytes after rescale, or text outside the General Legacy resource table's coverage; every other Access-permitted scalar key type now encodes successfully. (Composite keys including `OLE Object`, `Attachment`, or `Multi-Value (Complex)` columns do not reach this fall-through path: `ResolveIndexes` rejects them with `NotSupportedException` per W18, matching Microsoft Access semantics.)

### 7.3 W11 — what shipped (2026-04-25)

Kept as a brief reference because W11 lifted the W1 single-column-non-PK restriction, which is widely cited.

Public surface added on `IndexDefinition`:

- `IsUnique` (init-only `bool`, default `false`) — emits real-idx `flags` bit `0x01` (§3.1) on non-PK indexes. PK indexes still carry `flags = 0x00`; the `IsUnique` setter is silently subsumed because PK uniqueness is signalled by the logical-idx `index_type = 0x01` discriminator.
- `DescendingColumns` (init-only `IReadOnlyList<string>`, default empty) — case-insensitive subset of `Columns`. Each listed entry's col_map slot clears the ascending flag and is emitted as `col_order = 0x00`; entries that don't match any name in `Columns` throw `ArgumentException` at `CreateTableAsync` time.
- The W1-era restriction "non-PK indexes must be single column" is lifted. `IndexDefinition` now accepts up to 10 columns regardless of `IsPrimaryKey`.

Validation: 15 round-trip tests in `IndexWriterAdvancedTests` plus representative DAO CompactDatabase coverage in `AdvancedIndexKeysAndBTreeMaintenance_SurviveCompactAndRepair`. W11 expands the on-disk byte surface in two places: the `flags` byte at real-idx `phys + 42` and the `col_order` byte in each col_map slot.

### 7.4 W13 — what shipped (2026-04-25)

Per-value layout (after the entry flag byte):

```
[sign byte (1)] [unsigned mantissa, big-endian (16)] = 17 bytes
```

Construction sequence:

1. Decompose the .NET `decimal` into sign, natural scale, and 96-bit unsigned mantissa.
2. Round to the column descriptor's declared scale when necessary, then validate `targetScale ≥ naturalScale`; rescale via `BigInteger.Pow(10, targetScale − naturalScale)`. Mantissa exceeding 16 bytes after rescale throws `NotSupportedException` (W5 catches and falls through to the stale-leaf path).
3. Pack the rescaled magnitude into 16 BE bytes; prepend `0x80` if negative, `0x00` if positive.
4. Apply the format-specific twiddling rules from Jackcess `IndexData`:

| | Legacy (`LegacyFixedPointColumnDescriptor`, Jet4 `.mdb`) | New-style (`FixedPointColumnDescriptor`, ACCDB / ACE) |
|---|---|---|
| asc & pos | no flip; `byte[0] = 0xFF` | `byte[0] = 0xFF`; no flip |
| asc & neg | flip all 17 bytes; `byte[0] = 0x00` | `byte[0] = 0xFF`; flip all 17 bytes |
| desc & pos | flip all 17 bytes; `byte[0] = 0xFF` | `byte[0] = 0xFF`; flip all 17 bytes |
| desc & neg | no flip; `byte[0] = 0x00` | `byte[0] = 0xFF`; no flip |

5. Prepend the entry flag byte unflipped (`0x7F` asc / `0x80` desc; `0x00` / `0xFF` for null entries).

The original W13 implementation computed `targetScale` from each rebuild snapshot. W23 superseded that: `MaintainIndexesAsync`, incremental maintenance, and unique checks now use the column descriptor's declared `NumericScale`. This matches Access row storage, where every normal `Numeric` fixed slot is 17 bytes (`[sign][16-byte magnitude]`) and the scale lives in the descriptor, not in each row cell.

W13 caveats:

- **Legacy descending indexes preserve the documented Jackcess MS KB 837148 quirk:** under `LegacyFixedPointColumnDescriptor` the sign byte for descending negatives stays at `0x00` and for descending positives stays at `0xFF`, so negatives always sort before positives in lex order regardless of magnitude. Within a single sign the descending ordering is correct. The `FixedPointColumnDescriptor` form (ACCDB) does not carry this quirk.
- **Declared canonical scale.** W23+ numeric columns persist `NumericPrecision` / `NumericScale` in the TDEF descriptor. Older files whose descriptor lacks precision metadata can still fall back to the older snapshot-driven scale path.
- **Validation gap:** the format-probe corpus does not contain a NUMERIC-keyed index leaf. DAO CompactDatabase now covers representative NUMERIC row storage and index maintenance, but byte-for-byte comparison against an Access-authored NUMERIC-keyed leaf remains pending. See §8.

### 7.5 W15 — what shipped (2026-04-25)

W15 hoists the W11 unique-index duplicate-key check from the post-write `MaintainIndexesAsync` rebuild to a pre-write pass that runs before any row is encoded onto disk. The on-disk byte format is unchanged — W15 is purely an in-memory validation. The post-write check is retained as defense-in-depth (it still fires for indexes whose encoder rejects the pre-write encode).

Helpers now live on `UniqueIndexChecker` and are called from `AccessWriter`:

- `LoadUniqueIndexDescriptorsAsync(tdefPage, tableDef, ct)` — parses the TDEF for unique real-idx slots (real-idx `flags & 0x01` OR an associated logical-idx with `index_type = 0x01` PK discriminator) and returns one `UniqueIndexDescriptor` per slot, including a best-effort logical-idx name for the error message.
- `EncodeCompositeKeyForUniqueCheck(descriptor, row, numericTargetScales)` — encodes one row's composite key using the same `IndexKeyEncoder` path the W5 rebuild uses, with NUMERIC scales sourced from the column descriptor. Returns `null` on `NotSupportedException` so the caller can skip the check for that index.
- `CheckUniqueIndexesPreInsertAsync(tdefPage, tableDef, tableName, pendingRows, ct)` — loads the table snapshot once, then for each unique index encodes existing rows + pending rows into a `HashSet<byte[]>` keyed by composite key bytes. First collision throws `InvalidOperationException` with the message *"Unique index violation on table 'X': duplicate key for index 'Y'. The conflict was detected before any row was written; the table is unchanged."*
- `CheckUniqueIndexesPreUpdateAsync(tdefPage, tableDef, tableName, snapshot, updates, ct)` — reuses the caller-loaded snapshot and substitutes the post-update payloads at their original snapshot indices before encoding.

Wiring:

- `InsertRowAsync` (object\[\] + generic `T`) calls the pre-insert check via `InsertRowCoreAsync`.
- `InsertRowsAsync` (object\[\] + generic `T`) materialises the batch up front, applies `ApplyConstraintsAsync` to every row first (so auto-increment values are resolved), then runs the pre-insert check once with the full batch — intra-batch duplicates are caught here.
- `UpdateRowsAsync` runs the pre-update check after FK enforcement and before the per-row mutate-and-reinsert loop.

W15 caveats:

- **Indexes whose encoder rejects the pre-write encode (text outside General Legacy `0-9 / A-Z / a-z`, etc.) are silently skipped by W15.** A duplicate on such an index still surfaces from the post-write `MaintainIndexesAsync` check, but the offending row hits disk first — same model as before W15. The `RollbackInsertedRowsAsync` path already restores the row count and auto-increment counters when the post-write check throws, so the table state is still recoverable in that case.
- **Cost is one full snapshot scan per call** (insert or update) on top of the existing post-write rebuild scan. Acceptable for v1; W17 (index-seek RI enforcement) and W19 (incremental B-tree maintenance) will eventually retire both passes.
- **`AddAttachmentAsync` / `AddMultiValueItemAsync`** route through `InsertRowDataLocAsync` directly, bypassing `InsertRowCoreAsync` and therefore the W15 pre-check; the flat child tables ship a unique PK on a writer-managed autoincrement scalar (C7), so duplicate-key collisions there are not user-reachable.
- **Validation:** 7 round-trip tests in `IndexPreWriteUniqueEnforcementTests`. W15 does not touch the byte-level format, so the §8 Microsoft Access compact-and-repair validation gap does not apply.

### 7.6 W7 — what shipped (2026-04-26)

W7 replaces the W2-era ASCII-only stop-gap (`0–9 / A–Z / a–z`, `NotSupportedException` on anything else) with a full Jackcess-port "General Legacy" text-index encoder that covers the entire BMP. This unblocks:

- Single-column text/memo indexes containing spaces, punctuation, non-ASCII letters, accented Latin, CJK, etc. — they now participate in the W5 bulk B-tree rebuild instead of falling through to the stale-leaf path.
- The W11 multi-column composite-key path for any composite that includes a text or memo column.
- Descending text/memo keys (W11 descending `col_order` clears the ascending flag) — the previous fall-through path is gone.

New file: [JetDatabaseWriter/Indexes/Collation/GeneralLegacyTextIndexEncoder.cs](../../JetDatabaseWriter/Indexes/Collation/GeneralLegacyTextIndexEncoder.cs). Direct port of `com.healthmarketscience.jackcess.impl.GeneralLegacyIndexCodes` (Apache 2.0).

Embedded resources (gzipped to keep the assembly small):

- [JetDatabaseWriter/Indexes/CodeTables/index_codes_genleg.txt.gz](../../JetDatabaseWriter/Indexes/CodeTables/index_codes_genleg.txt.gz) — per-codepoint handler table for `U+0000..U+00FF` (256 entries).
- [JetDatabaseWriter/Indexes/CodeTables/index_codes_ext_genleg.txt.gz](../../JetDatabaseWriter/Indexes/CodeTables/index_codes_ext_genleg.txt.gz) — sparse handler table for `U+0100..U+FFFF`.

Each line in the resource files declares one of six handler kinds:

| Kind | Letter | Description |
|---|---|---|
| Ignored | `X` | Codepoint contributes nothing to any stream (e.g. control chars). |
| Simple | `S` | Inline bytes only — the common case for ASCII letters/digits. |
| International | `I` | Inline bytes + extra-codes stream bytes (for accent/case weights). |
| Unprintable | `U` | Unprintable-codes stream bytes only (no inline contribution). |
| UnprintableExt | `P` | Single unprintable-codes "ext-modifier" byte placed via the offset/midfix scheme. |
| InternationalExt | `Z` | Inline + extra + crazy-flag bit set on the running crazy-codes mask. |
| Significant | `G` | Inline bytes that participate in the post-END_TEXT extra stream (rare). |

`CharHandler` subclasses (`SimpleCharHandler`, `InternationalCharHandler`, `UnprintableCharHandler`, `UnprintableExtCharHandler`, `InternationalExtCharHandler`, `SignificantCharHandler`, plus a sentinel `IgnoredCharHandler`) are tagged with a `CharHandlerType` enum and dispatched by the inner loop in `Encode`.

State-machine layout of the emitted entry block:

```
┌────────┬──────────────────┬─────────┬───────────────────┬─────────┐
│ flag   │ inline payload   │ END_TXT │ extra/unprint/    │ END_EXT │
│ (1 B)  │ (per-char S/I/Z) │ (0x01)  │ crazy aux streams │ (0x00)  │
└────────┴──────────────────┴─────────┴───────────────────┴─────────┘
```

Aux-stream serialization order after `END_TEXT`: trimmed extra-codes stream → unprintable codes (length-prefixed and offset-encoded with the `0x06` midfix) → crazy codes (terminated with the documented `FF 02 80 FF 80` suffix). `TrimExtraCodes` strips the placeholder fill the international codes wrote — the trimming is the reason `END_TEXT` and `END_EXTRA_TEXT` are distinct bytes.

Descending pass: the entire payload region (flag-byte exclusive, trailing `END_EXTRA_TEXT` inclusive) is one's-complemented in place; a fresh unflipped `0x00` is then appended so the descending entry still ends in `0x00` and can be parsed by the same scanner. The leading flag (`0x80` for descending non-null) is never flipped — it is the descending signal itself.

Truncation + padding rules (matching Jackcess `toIndexCharSequence`):

- Inputs longer than `MaxTextIndexCharLength = 127` chars (Jet4/ACE: `TEXT_FIELD_MAX_LENGTH (255 bytes) / TEXT_FIELD_UNIT_SIZE (2 B/char)`) are sliced to 127 chars before encoding.
- Trailing ASCII spaces (`U+0020`) are stripped before encoding so `"AB"` and `"AB   "` produce identical entries.
- Surrogate pairs (lone or paired) collapse to the documented `0x3F` extra-byte fallback rather than throwing.

Routing in `IndexKeyEncoder.EncodeEntry`:

```csharp
if (columnType == Text || columnType == Memo)
{
    return GeneralLegacyTextIndexEncoder.Encode(ToText(value!), ascending);
}
```

Memo (`Memo`, `0x0C`) is routed through the same encoder; the indexed prefix is bounded by the same 127-char cap, so memo-keyed indexes only exercise the leading 127 chars of each value (matching Access). The reader-side LVAL chain is not re-fetched during indexing — only the inline preview / loaded string value is encoded.

Wiring + subsystems updated:

- `IndexKeyEncoder.EncodeEntry` (text + memo branch).
- `MaintainIndexesAsync` (W5) — text/memo keys no longer take the `NotSupportedException` swallow-and-skip path.
- `MaintainIndexesAsync` composite path (W11) — text/memo columns compose into multi-column keys; per-column descending direction is honored via the encoder's descending pass.
- `LoadUniqueIndexDescriptorsAsync` + `EncodeCompositeKeyForUniqueCheck` (W15) — text/memo unique indexes are now caught pre-write instead of falling through to the post-write check.

Validation:

- `JetDatabaseWriter.Tests/Indexes/IndexKeyEncoderTests.cs` — empty-string framing (asc + desc), digit codes, case-insensitive letter codes, trailing-space stripping, 127-char truncation, ascending case-insensitive ordering, plus the legacy unsupported-type tests now collapsed to numeric only.
- `JetDatabaseWriter.Tests/Indexes/IndexWriterAdvancedTests.cs` — round-trip MaintainIndexes coverage for unicode text and memo keys (single-column + composite + descending).

W7 caveats / not done:

- **General 1033v1** (the Access 2010+ default sort) is partially implemented as a helper/probe path, not yet as the writer-maintained text/memo index path. `GeneralTextIndexEncoder` ports the Jackcess `GeneralIndexCodes` tables and the V2010 suffix tables cover the checked-in fixture plus sampled DAO contexts, but `IndexKeyEncoder` still needs table-collation selection before it can route production index maintenance through that encoder. Arbitrary V2010 suffix contexts and broader byte-level fixture coverage remain open.
- **Validation gap (§8):** the byte sequences are inherited from Jackcess. DAO CompactDatabase now covers representative mixed-direction composite text/integer leaves, but the format-probe corpus still does not contain non-ASCII text-keyed leaves to byte-compare against.
- **W7 supersedes the partial encoder shipped with W2** — the previous "fall-through to stale leaf for any non-`[0-9A-Za-z]` char" behavior no longer applies for the General Legacy sort.

### 7.7 W16 — what shipped (2026-04-26)

Replaces the W10 O(N) parent-table snapshot inside `EnforceFkOnInsertAsync` with an O(log N) B-tree seek over the parent's PK / FK index. Cascade-update / cascade-delete on the parent (`EnforceFkOnPrimaryUpdateAsync`, `EnforceFkOnPrimaryDeleteAsync`) shipped under W22 (2026-04-27); see §7.11.

New helper:

- `IndexCursor.ContainsKeyAsync(rootPage, searchKey, ct)` — read-only descent through any number of `0x03` intermediate pages down to the matching `0x04` leaf. Honours:
  - **§4.2 entry-start bitmask** at offset `0x1B` (LSB-first within each byte; first entry implicit).
  - **§4.4 prefix compression** — `pref_len` (u16 at offset 20) is read once per page; the canonical key for entries beyond the first is reconstructed by prepending the leaf's first-entry prefix bytes.
  - **Sibling-leaf walk** — when the search key equals the last entry on a leaf and `next_page` is non-zero (non-unique key spans siblings), the cursor continues onto the next leaf. Pure equality probe; no range or multi-row enumeration.
  - **Intermediate descent** — picks the first entry whose summary key is ≥ the search key and follows its 4-byte LE child pointer (§4.3).
  - **§4.5 `tail_page` is consulted on intermediate overshoot** — W18 taught `IndexCursor.ContainsKeyAsync` and `FindRowLocationsAsync` to follow a non-zero `tail_page` when every summary key is less than the search key. Single-leaf roots still keep `tail_page = 0`; intermediate pages emitted by `IndexBTreeBuilder` stamp it with the rightmost leaf.

Writer-side wiring (in `AccessWriter`):

- `FkContext.SeekIndexes : Dictionary<string, ParentSeekIndex?>` — per-call cache. Resolution attempts the lookup once per relationship; a `null` entry is a "do not retry" sentinel that pins this relationship to the W10 HashSet fallback for the rest of the call.
- `ResolveParentSeekIndexAsync(rel, ctx, ct)` — locates the parent TDEF, finds the real-idx whose `col_map` exactly covers `rel.PrimaryColumns` (in declaration order), captures the per-column ascending flag (`(col_order & 0x01) != 0`), and the FK-side row indexes used to extract values from the inserter's `object[]`.
- `TryFindCoveringRealIdxAsync(tdefPage, targetColNums, ct)` — TDEF walk (re-uses the same `LocateRealIdxDescStart` helper that W9b uses to navigate past the column-name section). Returns `null` when the TDEF spans multiple pages, `first_dp == 0`, or no slot's `col_map` matches.
- `TryEncodeSeekKey(idx, values)` — concatenates per-column `IndexKeyEncoder.EncodeEntry` blocks in col_map order using the same direction flag the leaf was emitted with. Returns `null` when the encoder rejects any value (`NotSupportedException` / `ArgumentException` / `OverflowException`), which falls the caller through to the HashSet path.
- `IndexKeyEncoder.IsColumnTypeSeekable(byte)` — gates resolution (excludes `Boolean`; descriptor-scale `Numeric` participates because `ParentSeekKeyColumn` / `ChildSeekKeyColumn` carry `NumericScale` and the Jet4-vs-ACE numeric twiddle flag from TDEF resolution).
- `CreateRelationshipAsync` now calls `MaintainIndexesAsync` on both endpoint TDEFs after `EmitFkPerTdefEntriesAsync` so the W9b empty leaf is replaced by a populated B-tree before the very next child INSERT runs the seek probe. Without this, inserting a child immediately after declaring a relationship over a parent that already had rows would always fall through to the HashSet path on the first call.

Self-referential bulk inserts: `EnforceFkOnInsertAsync` checks `FkContext.ParentKeySets[rel.Name]` (populated by `AugmentParentSetsAfterInsert` after each successful insert) **before** attempting the seek, so a row that satisfies its own FK against an earlier row in the same `InsertRowsAsync` batch succeeds without re-running the index rebuild.

W16 fallback triggers (drop back to the W10 HashSet path):

- `_format == DatabaseFormat.Jet3Mdb` (Jet3 user/PK index emission exists, but relationship seek enforcement remains gated off for Jet3 and falls back to snapshot validation).
- Parent table missing from the catalog or has a multi-page TDEF.
- No real-idx slot covers `rel.PrimaryColumns` exactly (sharing per §3.3 is honoured — a non-FK user index over the same columns is acceptable).
- `first_dp == 0` (the W3 placeholder before the first MaintainIndexes pass).
- Any key column type is not seekable (currently `Boolean`) or its descriptor-aware encoder rejects the concrete value.
- `IndexKeyEncoder.EncodeEntry` throws on any FK-side value.

Validation: 3 new round-trip tests in `JetDatabaseWriter.Tests/Relationships/ForeignKeyEnforcementTests.cs` — large parent (5 000 rows, deep-key probe), text key (General Legacy round-trip on the seek path), bulk children (per-call cache reuse). The existing 11 W10 tests pass unchanged. **Validation gap (§8):** the cursor only reads bytes this library wrote. Pointing it at an Access-authored leaf is still pending — the format-probe corpus does not yet single out an FK index over a small enough column count to make a focused byte-level diff cheap.

### 7.8 W17a — what shipped (2026-04-26)

**W17a is the empirical-probe sub-phase only.** No `AccessWriter` or `AccessReader` behaviour changes; the `_format == DatabaseFormat.Jet3Mdb` rejection in `CreateTableAsync` stays in place until W17b lands.

What landed:

- New top-level probe `WriteJet3IndexAppendixAsync` in [`JetDatabaseWriter.FormatProbe/Program.cs`](../../JetDatabaseWriter.FormatProbe/Program.cs) — iterates a curated short-list of Jet3 fixtures (`Jackcess/V1997/indexTestV1997.mdb`, `compIndexTestV1997.mdb`, `testIndexCodesV1997.mdb`, `testV1997.mdb`, `mdbtools/nwind.mdb`, `Jet3Test.mdb`) and dumps the TDEF + the first leaf page reachable from each real-idx for up to 4 user tables per fixture.
- Jet3-specific decoded annotation blocks added to `EmitTDefAsync` (real-idx phys descriptor, leading 8-byte skip entry, logical-idx entry).
- New helper `EmitJet3LeafPagesAsync` runs a candidate-resolution table (`skip entry [0..3]`, `skip entry [4..7]`, `phys desc [34..37]`) per real-idx and dumps the first page that resolves to `page_type ∈ {0x03, 0x04}`. Output: [`format-probe-appendix-jet3-index.md`](../format-probe/format-probe-appendix-jet3-index.md).
- New internal accessor `AccessReader.GetRawPageBytesAsync(long, CancellationToken)` so the probe can read individual index leaf / intermediate pages by absolute page number (previously only TDEF chains were exposed).

What W17a confirmed (carried into §3.1, §3.2, §4.2):

- **`first_dp` lives at phys-desc bytes `34..37` on Jet3** — confirmed across every real-idx slot in every probed fixture. The mdbtools Jet4 layout (`used_pages` at 34..37, `first_dp` at 38..41) does **not** apply: Jet3 has no `used_pages` slot inside the 39-byte phys descriptor; per-table page-usage pointers must live in the leading 8-byte skip-entry block instead (probe shows the second 4-byte slot of that block resolves to data pages `0x01`, consistent with `used_pages`).
- **Jet3 logical-idx entry has no leading 4-byte cookie and no trailing 4-byte tail** — every Jet4 §3.2 field is present at offsets shifted left by 4 bytes; `index_type = 0x01` at byte 19 was verified on every probed PK entry.
- **§4.2 Jet3 leaf bitmask** at offset `0x16` and first entry at `0xF8` confirmed: a `LongInteger` PK leaf's first entries occupy bytes `0xF8`, `0x101`, `0x10A`, … with 9-byte stride (1B flag + 4B BE int + 3B page + 1B row), and the bitmask LSB-first pattern `00 02 04 08 10 20 40 80 …` matches that stride exactly.
- **§4.1 page header is identical between Jet3 and Jet4** (page_type, unknown, free_space, parent_page, prev_page, next_page, tail_page, pref_len at canonical offsets).

What W17a deliberately did **not** confirm (open questions for the W17b–c implementer):

- Exact field map of the 8-byte leading real-idx skip entry on Jet3. The probe shows `[0..3]` is sometimes 0 / sometimes a small-cardinality value, and `[4..7]` resolves to a data page when nonzero — likely `unknown(4) + used_pages(4)` but not byte-confirmed.
- Whether the Jet3 `flags` byte at phys-desc offset `38` carries the same `0x01 = Unique`, `0x02 = IgnoreNulls`, `0x08 = Required` semantics as Jet4. PK entries observed in the probe corpus carry `flags = 0x00` (matching Jet4's empirical `flags = 0x00 + index_type = 0x01` PK convention from §3.1), but no non-PK unique index was observed in any fixture.
- Whether Jet3 descending indexes use the same cleared ascending-flag convention as Jet4/ACE. No fixture in the V1997 corpus carries one.

W17a validation gap: the probe is read-only on real Access-authored bytes, so there is no Microsoft Access compact-and-repair concern at this phase. The §8 standing gap applies again starting at W17b.

### 7.9 W4-C-1 + W4-C-2 — what shipped (2026-04-26)

W4-C-1 (single-leaf incremental INSERT) and W4-C-2 (single-leaf incremental DELETE) lift the W5 "every mutation rebuilds the whole B-tree from a snapshot" cost when the index B-tree is rooted on a single leaf page (`page_type = 0x04`, no sibling pointers, no tail-page chain) AND the post-mutation entry list still fits on that one page. Multi-level trees and overflowing single-leaf trees fall back to `MaintainIndexesAsync` exactly as before. (Numeric-keyed indexes also fell back to bulk in this sub-phase pending the W13 per-rebuild canonical-scale pre-pass; **superseded by W23 (2026-04-27)** — the canonical scale now comes from the column's declared scale persisted in the TDEF descriptor, so numeric keys participate in the fast path; see §7.13.)

Current helper ownership, after the original W4-C helper facade was deleted on 2026-05-31:

- `IndexPageCodec.IsSingleRootLeaf(layout, page)` — guard that returns `true` only for `page_type = 0x04` with `prev_page == next_page == tail_page == 0`.
- `IndexPageCodec.DecodeLeafEntries(layout, page, pageSize)` — walks the §4.2 bitmask to recover every entry as `(canonical-key, data_page, data_row)`, re-prepending the §4.4 `pref_len` bytes to entries beyond the first.
- `IndexEntrySplicer.Splice(existing, adds, removes)` — removes entries by `(data_page, data_row)` pointer (returns `null` if any remove target is missing — defensive against a hint that doesn't match the leaf state), appends adds, and stable-sorts by encoded key bytes. Adjacent equal-key tie-breaking on `(page, row)` keeps non-unique-index ordering deterministic.
- `IndexPageCodec.TryBuildLeafPage(layout, pageSize, parentTdef, entries)` — thin wrapper that re-emits through `IndexPageCodec.BuildLeafPage(... enablePrefixCompression: true)` and returns `null` on overflow.

New writer-side helper, now owned by `IndexMaintainer`: `TryMaintainIndexesIncrementalAsync(tdefPage, tableDef, insertedRows?, deletedRows?, ct)`. Returns `true` when every supported real-idx was maintained incrementally; the caller MUST then NOT call `MaintainIndexesAsync`. Returns `false` on any fall-back trigger:

- Historical W4-C-1/W4-C-2 limitation: Jet3 fell back before W17 shipped Jet3 index emission and maintenance.
- Multi-page TDEF chain. `CreateTableAsync` now emits multi-page TDEF chains
  for wide schemas (>32 col / >16 idx on Jet3, ≫50 col on Jet4 / ACE), so this
  fall-back trigger is no longer purely defensive — it engages whenever
  `MaintainIndexesIncrementalAsync` is called against one of those tables.
  Lifting the restriction is tracked alongside the W9b multi-page in-place
  TDEF mutation gap (see §7.2).
- Any key column is `Numeric` AND the column was written by a pre-W23 build (descriptor `NumericPrecision == 0`) — fall back to the W13 snapshot-driven canonical-scale pre-pass. W23+ NUMERIC columns engage the fast path using the declared scale.
- The index root is not a single leaf (`IndexPageCodec.IsSingleRootLeaf` returns false).
- The encoder rejects any value (text outside General Legacy, etc.).
- The spliced entry list overflows the leaf payload area (`IndexPageCodec.TryBuildLeafPage` returns `null`).

Per-call hint shape: each public mutation builds `IReadOnlyList<(RowLocation Loc, object[] Row)>` for inserted and/or deleted rows and passes them as the hint. The hint is consumed by `EncodeHintEntries` (per real-idx, per row, encoded in col_map order).

Wiring in `AccessWriter`:

- `InsertRowCoreAsync` (single insert) — hint = `[(loc, values)]`, deletes none.
- `InsertRowsAsync(object[])` and `InsertRowsAsync<T>` (batch) — hint accumulates `(loc, mappedRow)` per inserted row.
- `UpdateRowsAsync` — switched to `InsertRowDataLocAsync` so the new loc is captured; hint = inserts of `(newLoc, newRow)` + deletes of `(oldLoc, oldRow)`.
- `DeleteRowsAsync` — hint = deletes of `(loc, oldRow)` for every matching row.

Pre-write unique-index enforcement (W15) still runs before the data-page mutation, so the fast path does not re-check uniqueness post-write. The bulk path's post-write check (defense-in-depth for encoder-rejected indexes) is unchanged.

Page reuse: when the fast path succeeds, the existing single leaf is rewritten in place and `first_dp` remains stable. This keeps the table's real-index usage-map row valid and avoids creating orphaned leaves during ordinary single-leaf insert/update/delete maintenance.

Validation:

- 26 unit tests in [`JetDatabaseWriter.Tests/Indexes/IndexPageCodecAndEntrySplicerTests.cs`](../../JetDatabaseWriter.Tests/Indexes/IndexPageCodecAndEntrySplicerTests.cs) cover decode round-trip, single-root-leaf detection, splice insert/delete/combined, missing-remove rejection, overflow rejection, sibling-pointer reads, intermediate child pointers, and prefix-compressed round-trip.
- 7 round-trip tests in [`JetDatabaseWriter.Tests/Indexes/IndexIncrementalMaintenanceTests.cs`](../../JetDatabaseWriter.Tests/Indexes/IndexIncrementalMaintenanceTests.cs) cover single-row insert (leaf count stable, entry count updated), repeated single inserts (one leaf reused with all rows), single delete (same leaf, reduced count), update (delete+insert hint produces correct per-row read-back), bulk-fall-back on leaf overflow (800-row insert spills to multi-level, then a follow-up insert succeeds via bulk), text-keyed insert via the General Legacy encoder, and the W15 unique pre-check still firing on the fast path.
- All 263 tests in `AccessWriterTests` + `IndexBulkInsertStressTests` pass unchanged. Full repo test count: 2193 passed / 0 failed.

W4-C-1/W4-C-2 caveats:

- **Update path now uses `InsertRowDataLocAsync` everywhere.** Previously `UpdateRowsAsync` called `InsertRowDataAsync` (no-loc variant). The change is internal and the public surface is identical — the new loc is only used to populate the incremental hint.
- **The fast path is silently skipped per-index when its key column is `Numeric` AND the file was written before W23.** Once the column is rewritten by a CREATE TABLE under W23+ the declared scale is persisted in the descriptor and the fast path engages. See §7.13.
- **No batched leaf-page allocation.** Successful single-leaf fast-path calls rewrite one existing leaf page per mutated index. A 5-row batch insert against a 3-index table rewrites 3 leaves holding the post-batch state, not 15 leaves and not a fresh page range.
- **Access validation:** Microsoft Access compact-and-repair validation now passes for the covered FK compact round-trip cases that exercise this single-leaf reuse behavior.

### 7.10 W19 — what shipped (2026-04-26)

W19 closes the last remaining `IndexKeyEncoder` gap on the supported-by-Access list: variable-length `Binary (0x09)` columns can now back a live, B-tree-maintained index. Prior to W19 an `IndexDefinition` over a binary column would throw `NotSupportedException` from the encoder on the first row insert that triggered `MaintainIndexesAsync`.

Encoding (`IndexKeyEncoder.EncodeBinaryEntry`):

- Accepts `byte[]`, `ArraySegment<byte>`, `ReadOnlyMemory<byte>`, and `Memory<byte>`. Other CLR types throw `ArgumentException`.
- Layout: `flag(1) + ⌈len/8⌉ × 9-byte segments`. Each segment is `8 zero-padded data bytes + 1 length byte`. Intermediate length byte = `0x09`; final length byte = remaining valid count (`0..8`).
- Empty input still emits one final segment of `0x00 × 8 + length 0x00` so empty values are representable and lex-comparable; an empty value sorts strictly below any non-empty value.
- Descending: data bytes and the FINAL length byte are bit-flipped. Intermediate `0x09` length bytes are NOT flipped — matches Jackcess `IndexData.writeGeneralBinaryEntry`.

Implementation note — refactor of W12 GUID path:

- The W12 `EncodeGuidEntry` previously inlined the 19-byte two-segment layout. W19 hoists the per-segment loop into a private `EncodeGeneralBinaryEntry(ReadOnlySpan<byte> data, bool ascending)` helper that handles arbitrary lengths. `EncodeGuidEntry` now reorders the GUID into 16-byte big-endian display order and delegates to the helper. Output is byte-identical to the pre-refactor path for 16-byte input — every existing W12 GUID test passes unchanged.

W16 parent-PK seek participation:

- `Binary` is added to `IndexKeyEncoder.IsColumnTypeSeekable`. FK relationships whose primary-key column is `byte[]`-backed (`MaxLength` ∈ [1, 255]) now route through the O(log N) `IndexCursor.ContainsKeyAsync` path on Jet4 / ACE instead of the O(N) HashSet snapshot fall-back. Jet3 still routes via the HashSet path because no parent index is emitted there.

Validation surface:

- 9 unit tests in [`IndexKeyEncoderTests`](../../JetDatabaseWriter.Tests/Indexes/IndexKeyEncoderTests.cs) (prefix `Binary_`): null, empty, 3 bytes, exactly 8, 9-byte multi-segment, descending flip rules, ascending lex ordering, descending lex ordering, non-`byte[]` rejection.
- 4 round-trip tests in [`IndexBinaryKeyTests`](../../JetDatabaseWriter.Tests/Indexes/IndexBinaryKeyTests.cs): bulk-insert + reader round-trip, unique-violation detection, multi-column composite index over (`Text`, `Binary`), descending direction.
- All 2236 tests in the full suite pass after W19.

W19 caveats:

- **Validation gap (§8):** the format-probe corpus does not contain a `Binary`-keyed index leaf; the byte sequences come from Jackcess. DAO CompactDatabase now covers a representative writer-emitted binary-keyed index, but Access-authored byte comparison remains pending.
- **`Binary` columns whose `MaxLength > 255` map to `Ole (0x0B)`**, which Microsoft Access does not permit indexes over and which W18 rejects up-front in `ResolveIndexes`. The W19 encoder is therefore only reachable for declarations that produce `Binary` (i.e. `byte[]` with `MaxLength ∈ [1, 255]`).

### 7.11 W22 — what shipped (2026-04-27)

W22 closes the W16 follow-up — `EnforceFkOnPrimaryUpdateAsync` and `EnforceFkOnPrimaryDeleteAsync` no longer load an O(N) child snapshot per relationship per call. They now use an O(log N + K) seek over the FK-side child real-idx that W9b already emits on every relationship.

New cursor surface:

- `IndexCursor.FindRowLocationsAsync(rootPageNumber, searchKey, ct)` — same descent as W16's `ContainsKeyAsync`, but instead of returning `bool` it walks the sibling-leaf chain accumulating every `(dataPage, rowIndex)` whose canonical key equals the search key. Stops when the last canonical key on a leaf sorts strictly less than the search key. Honours intermediate `tail_page` fall-through (W18) the same way `ContainsKeyAsync` does.

`AccessWriter` additions:

- `ChildSeekIndex` record + `ChildSeekKeyColumn` readonly record struct — per-relationship descriptor cached on `FkContext.ChildSeekIndexes` (parallel to W16's `FkContext.SeekIndexes`).
- `ResolveChildSeekIndexAsync(rel, ctx, ct)` — mirror of `ResolveParentSeekIndexAsync` against the child TDEF; bails on Jet3, looks for a covering child real-idx whose `col_map` matches the FK column list, returns null on miss.
- `TryEncodeChildSeekKey(ChildSeekIndex, object?[] parentPkValues)` — encodes via the descriptor-aware `IndexKeyEncoder` path; `Numeric` uses the resolved `NumericScale` and Jet4/ACE numeric form. Returns null on encoder rejection (numeric overflow, text outside General Legacy, etc.).
- `TryReadColumnValuesTypedAsync(RowLocation, TableDef, int[] columnOrdinals, ct)` — single-row typed reader. Returns null if any in-scope column is `Memo` / `Ole` / `Complex` / `Attachment` because the writer does not currently reassemble LVAL chains or complex child rows for inline access; cascade-delete recursion needs the typed full row to resolve grandchildren. Descriptor-scale `Numeric` is decoded inline through `JetTypeInfo.ReadFixedTyped`.
- `TryProcessCascadeDeleteWithSeekAsync(rel, childEntry, childDef, childSeek, parentPkRows, ctx, depth, ct)` — seeks each parent PK, dedupes via `(page << 16) | rowIndex`, resolves full `RowLocation` via per-page `EnumerateLiveRowBounds`, reads typed full row for grandchild recursion, runs `CascadeDeleteComplexChildrenAsync` + `MarkRowDeletedAsync` + `AdjustTDefRowCountAsync` + `MaintainIndexesAsync`.
- `TryProcessCascadeUpdateWithSeekAsync(rel, childEntry, childDef, childSeek, movingChanges, fkIdx, ct)` — seeks each old PK, splices the FK columns to the new PK, rewrites via `MarkRowDeletedAsync` + `InsertRowDataAsync`, then `MaintainIndexesAsync`.

Signature changes:

- `EnforceFkOnPrimaryDeleteAsync` was `(string primaryTable, IReadOnlyList<string?> deletedKeys, ...)` and is now `(string primaryTable, TableDef primaryDef, List<object?[]> deletedParentRows, ...)`. The seek path needs the typed parent-PK tuples to call `TryEncodeChildSeekKey`.
- `EnforceFkOnPrimaryUpdateAsync` extended its `changes` tuple to carry `OldFullRow object?[]` so the OLD typed PK is available for child-side seek.

Both cascade enforcers call `TryProcessCascadeXxxWithSeekAsync` BEFORE the snapshot fallback and return `bool` so the snapshot path runs only when the seek path cannot complete.

CRITICAL implementation note (regression seen during dev):

- The snapshot-fallback `deletedSet` keys MUST be built with `BuildCompositeKey(pk, identity)` (where `identity` is an `int[]` with `identity[i] = i`), not an ad-hoc helper. `BuildCompositeKey` applies `'S:UPPER'` for case-insensitive text, `'N:decimal'` for cross-width numerics, `'D:UTC-ticks'` for `DateTime`, `'G:HEX'` for `Guid`, `'B:base64'` for `byte[]`, and `'?:0/1'` for booleans, all separated by `'|'`. Reinventing the normalisation produces non-matching keys and silently misses cascade victims (this exact bug surfaced and was caught by `ComplexColumnsCascadeDeleteTests.FkCascadeDelete_AlsoCascadesComplexChildrenOnTheChildTable`).

Falls back to the legacy O(N) child snapshot when:

- `_format == DatabaseFormat.Jet3Mdb` — no FK-side real-idx is emitted.
- No covering child real-idx exists (e.g. relationship over a non-PK column where W9b's slot was reclaimed).
- The encoder rejects any FK key component (Numeric overflow, etc.).
- A child row contains LVAL columns the single-row reader cannot decode.

Validation:

- 2 new round-trip tests in [`ForeignKeyEnforcementTests`](../../JetDatabaseWriter.Tests/Relationships/ForeignKeyEnforcementTests.cs): `Delete_PkSide_WithCascade_BulkSeeksChildIndex`, `Update_PkSide_WithCascade_BulkSeeksChildIndex` (functional verification at scale — 50 parents × 200 children, asserts the post-state under cascade-delete and cascade-update are correct).
- The 14 existing W10 / W16 cascade tests pass unchanged.
- Full suite: 2302 / 2302 passing.

W22 caveats:

- **Validation gap (§8):** the cursor's `FindRowLocationsAsync` reads bytes this library wrote; it has not been pointed at an Access-authored child-side index leaf. Same caveat as W16.
- **No batched seek.** Each parent PK seeks independently. A future optimisation could sort the parent PKs in canonical key order and walk the leaf chain once, but the current per-key descent is already O(log N) per parent and the cascade fan-out (K) tends to dominate.

### 7.12 W4-C-3 + W4-C-4 — what shipped (2026-04-27)

W4-C-3 (surgical multi-level descent + leaf-local mutate) and W4-C-4 (surgical leaf split + propagate up) close the longest-standing W4 follow-up: non-append mutations against a multi-level tree no longer always re-emit the whole index on a fresh page-number range and orphan every old page. When every change in the batch lands on the SAME leaf, the affected pages are now mutated in place at their existing page numbers — zero orphans on the W4-C-3 path, exactly one new appended page on the W4-C-4 split path.

Current helpers:

- `DecodedIntermediateEntry` + `IndexPageCodec.DecodeIntermediateEntries(layout, page, pageSize)` — decodes every entry on an intermediate (`0x03`) page back into its canonical `(Key, DataPage, DataRow, ChildPage)` record, re-prepending the §4.4 shared prefix to entries beyond the first.
- `IndexPageCodec.TryBuildLeafPage(layout, pageSize, parentTdef, entries, prev, next, tail)` — overload that preserves a non-root leaf's sibling-chain header fields. Returns `null` on overflow.
- `IndexBTreeBuilder.TryBuildIntermediatePage(layout, pageSize, parentTdef, entries, prev, next, tail)` — re-emits a single `0x03` page from an arbitrary list of `(summaryKey, dataPage, dataRow, childPage)` tuples, preserving sibling-chain headers. Returns `null` on overflow.

Writer-side code now lives in `IndexBTreeEditor.TrySurgicalMultiLevelMaintainAsync(layout, tdefPage, firstDp, addEntries, removeEntries, ct)` — called by `IndexMaintainer.TryMaintainIndexesIncrementalAsync` before the W4-D rebuild branch, after the W18 tail-append fast path. Pipeline:

1. **Path-capturing descent** with the FIRST change-set key (`DescendCapturingAsync`) — records `(pageNumber, pageBytes, decodedSummaryEntries, takenChildIndex)` at every intermediate level down to the target leaf. Bails on overshoot (search key `>` every summary on a page; the cursor would normally follow `tail_page`, but the surgical path needs a clean (page, child-index) pair at every level for an in-place ancestor rewrite).
2. **Same-leaf verification** — for every other key in the change-set (`ConfirmKeyTargetsSamePath`), walk the captured path and assert each level picks the same child entry. Multi-leaf change-sets bail to W4-D.
3. **W4-C-3 in-place leaf rewrite** — try `IndexPageCodec.TryBuildLeafPage` with the original leaf's `prev_page` / `next_page` / `tail_page`. On success, compare the new max key to the old max key:
   - **Unchanged max** → just `WritePageAsync(targetLeafPage, rebuilt)`. Done.
   - **Changed max** → call `PrepareAncestorReplaceWrites` to build the in-place rewrite list for the parent intermediate (replace its summary entry for this leaf with the new max). When the changed entry was the LAST on its parent, the parent's own max key changes too, so the pipeline walks up replacing each ancestor's last-entry summary until either a non-last position is reached or the root is rewritten. Bail to W4-D if any intermediate would overflow on rebuild (no recursive intermediate split this phase).
4. **W4-C-4 leaf split** — when the spliced entry list overflows one page, `TryGreedySplitInTwo` greedy-fills a left half until overflow, then puts the remainder on the right; bails on 3+ pages. Allocates one new page at end of file for the right half, builds both halves with proper sibling pointers, calls `PrepareAncestorSplitWrites` to insert one new summary entry into the parent intermediate in place. When the parent's affected entry was its last, the right summary becomes the parent's new max and is propagated up via `PrepareAncestorReplaceWrites`. Bails on parent overflow or if the next-sibling leaf cannot be patched. Commit order: append right page → patch `leafNext.prev_page` → rewrite original leaf as left half → rewrite ancestors.

Bail triggers (caller falls through to W4-D bulk rebuild):

- Multi-leaf change-set (different keys land on different leaves).
- Leaf becomes empty after splice (W4-C-5 underflow scope).
- 3+ page split needed.
- Parent intermediate overflow on summary update or new entry insertion (recursive intermediate split is a future W4 sub-phase).
- Descent overshoot (search key sorts above every summary on some intermediate; the W18 `tail_page` chain is reachable but the surgical path needs a deterministic (page, child-index) pair at every level).
- Encoder rejects any change-set key (text outside General Legacy, etc.) — the existing W4-D pre-checks catch this before the surgical path is invoked.

Side effect: `EnforceFkOnInsertAsync` and friends already passed `removeEntries` (with row pointers), but the surgical path also needs the encoded delete keys for path verification. `TryMaintainIndexesIncrementalAsync` now calls `EncodeHintEntries(deletedRows, keyColInfos)` and derives `removePtrs` from the result; existing W4-C-1/W4-C-2 and W4-D paths are unaffected (they consume only the pointer list).

Validation:

- 7 round-trip tests in [`IndexSurgicalSingleLeafMutationTests`](../../JetDatabaseWriter.Tests/Indexes/IndexSurgicalSingleLeafMutationTests.cs): in-place update on middle leaf, in-place insert between existing keys, in-place delete from middle leaf, in-place insert that propagates a max-key change to the parent intermediate, 2-way leaf split (asserts ≤ 2 new index pages, vs. 6+ for W4-D), bail-to-W4-D for multi-leaf change-sets, bail-to-W4-D for empty-leaf underflow. Each in-place test asserts the absolute count of `0x03` + `0x04` pages in the file is unchanged before vs. after the mutation — surgical paths rewrite at existing page numbers (zero orphans), W4-D appends a fresh tree (count strictly increases).
- All 2309 tests in the full suite pass (was 2302 before W4-C-3+W4-C-4).

W4-C-3 / W4-C-4 caveats:

- **No recursive intermediate split.** When a 2-way leaf split would push the parent intermediate past one-page capacity, the surgical path bails to W4-D. Splitting an intermediate means inserting a new summary into THE grandparent (which may itself overflow → grandparent split → … → new root + `first_dp` update), and the W4-C-3 / W4-C-4 commit order would have to thread a transactional rollback through several levels of in-place writes. Out of scope this phase; tracked as part of any future W4-C-5+ work.
- **Multi-leaf change-sets always bail.** A common case where this hits is updates that change an indexed column's value such that the new key sorts into a different leaf than the old key. The surgical path requires every key in the batch to descend to the same leaf — when two keys differ at any intermediate, it bails to W4-D. Splitting the batch into per-leaf sub-batches and running surgical mutation per group is a possible future optimisation.
- **Empty-leaf underflow always bails.** Deleting every entry from a leaf would orphan it (and leave its summary in the parent stale). W4-C-5 (merge / redistribute with siblings on delete underflow) is the planned follow-up.
- **Validation gap (§8):** ACE/Jet4 DAO CompactDatabase coverage now includes representative advanced-index maintenance in `AdvancedIndexKeysAndBTreeMaintenance_SurviveCompactAndRepair`. The remaining surgical-path gap is byte-level parity for individual sub-phases, not lack of compact automation.

### 7.13 W23 — what shipped (2026-04-27)

W23 drops the `Numeric` bail from every incremental fast path (W4-C-1, W4-C-2, W4-C-3, W4-C-4, W18, W4-D). Before W23 every NUMERIC-keyed mutation fell through to the bulk rebuild because the canonical sort-key scale required a snapshot of every live row to compute `max(natural-scale)` per index. After W23 the canonical scale is the column's DECLARED scale, persisted in the TDEF column descriptor and read on load — the same scheme Microsoft Access itself uses.

What's persisted:

- `ColumnDefinition.NumericPrecision` (`byte`, default 18) and `NumericScale` (`byte`, default 0) — the Access "Number → Decimal" UI defaults. Validation: precision must be 1–28, scale must be ≤ precision and ≤ 28.
- `AccessWriter.BuildTableDefinition` writes precision at TDEF column-descriptor offset 11 and scale at offset 12 for every `Numeric (0x10)` column on Jet4 / ACE (Jet3 has no NUMERIC). These are the same bytes Jackcess' `FixedPointColumnDescriptor` reads on parse, and the same bytes Access-authored `fixedNumericTest.accdb` carries (verified via `docs/design/format-probe-appendix-complex.md` — descriptor bytes `12 00 00 00` = precision 0x12 = 18, scale 0).
- `AccessBase.LoadColumnInfos` parses both bytes back into `ColumnInfo.NumericPrecision` / `NumericScale`. `ColumnMetadata` exposes them on the public reader API.

What changed in encoding: new `IndexKeyEncoder.EncodeNumericEntryAtDeclaredScale(value, ascending, declaredScale, legacy)` rounds the input value to the declared scale via `decimal.Round(d, declaredScale, MidpointRounding.ToEven)` before delegating to the existing `EncodeNumericEntry`. This matches Access's "round half to even" (banker's rounding) on store. Half-even is `MidpointRounding.ToEven`'s default behaviour and is what Access has used since at least Jet 4.

Where the new path engages:

- `EncodeHintEntries` (used by every fast path's encoded-key derivation) — calls `EncodeNumericEntryAtDeclaredScale(value, ascending, col.NumericScale, legacyNumeric)` for `Numeric` columns; non-numeric columns unchanged. Method changed from `private static` to `private` to access `_format`.
- `EncodeCompositeKeyForUniqueCheck` (W15 pre-insert duplicate check) — same call site update.
- `MaintainIndexesAsync` bulk rebuild — the per-index canonical scale is the column's declared scale.
- `CheckUniqueIndexesCore` (W15 bulk path) — same declared-scale rewrite.
- `TryMaintainIndexesIncrementalAsync` (W4-C entry point) — the prior `Numeric` bail is gone; numeric keys now participate in the incremental fast paths.

Backward compatibility:

- `BuildColumnDefinitionFromInfo` round-trips `NumericPrecision` / `NumericScale` from the on-disk descriptor so AddColumn / DropColumn / RenameColumn preserve the declared metadata.

Validation:

- 9 round-trip tests in [`IndexNumericKeyIncrementalTests`](../../JetDatabaseWriter.Tests/Indexes/IndexNumericKeyIncrementalTests.cs): metadata round-trip (declared and default), single-leaf splice on numeric key, tail append on multi-level numeric tree, incremental delete, unique pre-check at declared scale, half-even rounding collision at declared scale 0, precision and scale validation.
- 1 existing test in `IndexWriterAdvancedTests` (`UniqueDecimalIndex_DuplicateInsert_Throws`) updated to declare scale=2 explicitly so the duplicate detection point under the new declared-scale semantics matches Access behaviour.
- `LimitationsTests.SchemaEvolution_ColumnDefinition_ExposesConstraintProperties` updated to include the two new properties in the alphabetical expected list.
- Full suite: 2318 / 2318 passing.

W23 caveats:

- **Validation gap (§8):** descriptor-scale numeric row storage now has DAO CompactDatabase coverage through `AdvancedIndexKeysAndBTreeMaintenance_SurviveCompactAndRepair`. Access-authored byte comparison remains limited to the existing `fixedNumericTest.accdb` fixture.
- **Relationship seek over `Numeric`.** ✅ **Shipped (2026-05-22).** Parent/child seek-key descriptors now carry `NumericScale` plus the Jet4-vs-ACE numeric encoding flag, `TryEncodeSeekKey` / `TryEncodeChildSeekKey` route `Numeric` through `EncodeNumericEntryAtDeclaredScale`, and the W22 single-row cascade reader decodes descriptor-scale numeric columns inline. Focused coverage lives in `IndexSeekKeyEncodingTests` and `ForeignKeyEnforcementTests`.
- **No persisted-property scale.** Access also stores UI-facing scale and decimal-place properties in the persisted column-property block; the writer only writes the TDEF descriptor bytes. DAO compact validation shows the engine canonicalises against the descriptor, so this is a Design View metadata polish gap rather than a row-storage compatibility blocker.

### 7.14 W4-C-6-v2 — what shipped (2026-04-27)

W4-C-6-v2 closes the rightmost-leaf-merge bail in `AccessWriter.TrySurgicalCrossLeafMaintainAsync`. Before this phase, when the dead leaf was the parent intermediate's last child (`mergeParent.TakenIndex == mergeParent.Entries.Count - 1`) the cross-leaf path bailed because removing that child would shrink the parent's `tail_page` header (§4.5) and the surgical commit had no way to recompute it. After this phase the merge engages on the rightmost-child case as well, and the new `tail_page` value cascades up every captured ancestor whose own rightmost child is the parent we just modified.

What changed in `TryStageIntermediateRewrites` (the deepest-first intermediate-rewrite loop):

- A new per-page `intermediateTailOverrides` `Dictionary<long, long>` is staged as we process pages bottom-up. For a parent-of-leaf intermediate the new tail is always `newEntries[^1].ChildPage` (the surviving rightmost leaf in this subtree). For a higher intermediate the new tail is inherited from the override recorded on a deeper iteration when the rightmost child intermediate's tail moved.
- The `IndexBTreeBuilder.TryBuildIntermediatePage` call now passes `newTail` instead of `origTail`. The override is recorded only when `newTail != origTail`, and only when `origTail != 0` — single-leaf-root state stays untouched.
- The bail at `mergeParent.TakenIndex == mergeParent.Entries.Count - 1` is gone.

What did NOT change:

- Page commit order. Sibling-pointer patches → leaf rewrites → intermediate rewrites → optional `first_dp` patch — same as W4-C-3 / W4-C-4 / W4-C-5.
- Disposal model. The orphaned dead-leaf page is reclaimed by Access on the next Compact & Repair pass (same as the W4-D bulk path).
- The `mergeParent.Entries.Count < 2` bail (single-child parent → cascade-collapse the parent → out of scope; tracked as W4-C-8+).
- The "neighbour leaf is being mutated by another group in this batch" bail.

Validation:

- Two existing tests in [`IndexSurgicalLeafMergeTests`](../../JetDatabaseWriter.Tests/Indexes/IndexSurgicalLeafMergeTests.cs) flipped from "asserts W4-D bail (page count strictly increases)" to "asserts surgical merge engaged (page count unchanged)":
  - `DeleteAllInTailLeaf_MergesAndPropagatesTailPage_AppendsZeroIndexPages` — 3-leaf tree, deleting the rightmost leaf's contents propagates the new tail through the captured root; `tail_page` on the (now 2-entry) root points at the surviving (former middle) leaf.
  - `DeleteAllInRightmostLeaf_MergesAndShrinksRoot_AppendsZeroIndexPages` — 2-leaf tree → 1-leaf single-entry root with `tail_page` pointing at the surviving leaf. Verifies single-entry intermediates are still navigable by the cursor.
- Both tests use a unique sparse INT index keyed on `Id` plus a non-indexed `Region` predicate column, so the per-key descent groups deterministically into one leaf (non-unique keys can spill across a leaf boundary, which would route deletes into multiple groups and trigger the "leafPrev/leafNext is in groups" bail before the merge can engage).
- Full suite: 2332 / 2332 passing (was 2332 before — the two flipped tests were already counted).

W4-C-6-v2 caveats:

- **Single-child parent collapse still bails.** When the surviving leaf would be the parent's only entry AFTER the dead leaf is removed (i.e. the parent had exactly 2 entries before) the parent's `Entries.Count < 2` check now refers to the PRE-mutation count, so this code path engages and produces a single-entry root. Genuine "parent had only one child to begin with" inputs are rejected up-front by the `mergeParent.Entries.Count < 2` bail and fall through to W4-D (which itself also leaves the now-empty subtree as orphan pages).
- **Recursive ancestor empties.** ✅ **Shipped (W4-C-8+, 2026-04-27).** Multi-group delete batches that empty a parent intermediate entirely now cascade a `Remove` op upward to the grandparent rather than bailing to W4-D. See §7.18.
- **Validation gap (§8):** the byte sequences emitted by the merge path are produced by `IndexBTreeBuilder.TryBuildIntermediatePage` with the new `tail_page` value spliced in via the same path it was already taking for the W4-C-7-v1 root-split case. Representative DAO CompactDatabase coverage now exercises advanced-index maintenance in `AdvancedIndexKeysAndBTreeMaintenance_SurviveCompactAndRepair`; remaining W4-C risk is byte-level parity for individual sub-phases.

### 7.15 W4-C-7-v2 — what shipped (2026-04-27)

`TryStageIntermediateRewrites` was renamed `TryStageIntermediateRewritesAsync` and converted to async so the higher-level intermediate split case (children are themselves intermediates, not leaves) can read the rightmost child's effective `tail_page` on demand. The previous v1 bail (`if (!parentOfLeaf.Contains(deepest)) return false;`) is removed; mid-level and root-level intermediate overflows now split in place identically to the parent-of-leaf case shipped in W4-C-7 v1.

The new `GetEffectiveTailPageAsync(intermediatePage, overrides, rewrites, ct)` helper returns the rightmost-leaf page reachable through the supplied intermediate's subtree, consulting (in order) the per-page `intermediateTailOverrides` dictionary populated as deeper intermediates are processed in this batch, the staged `existingPageRewrites` byte map, and finally the cache-backed live page. After splitting any intermediate, both halves' tail values are recorded in `intermediateTailOverrides` so a shallower split that subsequently looks up either page picks up the post-split value rather than the now-stale live header.

Mutable plumbing: the `ref nextAllocatedPageNumber` + `out long? newRootPage` parameters could not survive the conversion to async, so the new `IntermediateStagingState` class wraps both and is passed by reference through `TrySurgicalCrossLeafMaintainAsync` → `TryStageIntermediateRewritesAsync`.

Tests: 3 new round-trip tests in [`IndexSurgicalRecursiveIntermediateSplitTests`](../../JetDatabaseWriter.Tests/Indexes/IndexSurgicalRecursiveIntermediateSplitTests.cs) exercise multi-level (composite 255-byte TEXT key) trees through bulk insert + cross-leaf delete, drip-feed inserts that cascade splits up through mid-level intermediates, and delete-and-reinsert cycles. Representative ACE/Jet4 DAO CompactDatabase coverage for advanced key encodings and post-insert update/delete/insert B-tree maintenance now lives in `AdvancedIndexKeysAndBTreeMaintenance_SurviveCompactAndRepair`; byte-for-byte Access-authored parity remains tracked in §8.

W4-C-7-v2 caveats:

- **3+ page splits at any captured level still bail.** ✅ **Shipped (W4-C-8, 2026-04-27).** Both `TryGreedySplitLeafInN` and `TryGreedySplitIntermediateInN` produce N-way splits without an artificial cap; see §7.17.
- **Recursive intermediate collapse on cascading underflow remains W4-C-8+.** A multi-group delete batch that empties a higher-level intermediate still falls to W4-D.

### 7.16 W26 — what shipped (2026-04-27)

Closes the two W14 follow-ups plus a stale README claim about multi-column primary keys.

**A. `DropRelationshipAsync` trailing real-idx reclaim.** `TryRemoveFkLogicalIdxEntryAsync` now returns the real-idx slot number it severed (instead of `bool`). After both PK-side and FK-side removals succeed, `DropRelationshipAsync` calls `TryReclaimTrailingRealIdxAsync(tdefPage, ct)` once per affected TDEF (skipping the second call when self-referential). The reclaim helper:

1. Walks the post-removal logical-idx entries to build a `bool[numRealIdx] referenced` map (each logical-idx points at its real-idx via `index_num2` at offset +8).
2. Counts contiguous trailing unreferenced slots.
3. Removes the corresponding leading real-idx skip block entries (12 bytes each on Jet4) AND trailing real-idx physical descriptors (52 bytes each on Jet4) via two left-shifts.
4. Decrements `num_real_idx` and updates `tdef_len`.

The "trailing only" restriction sidesteps the cross-TDEF renumbering problem: if we removed a non-trailing slot, every other TDEF whose logical-idx entries store `rel_idx_num > R` referencing this TDEF would need its `rel_idx_num` decremented. Trailing-only reclaim leaves every still-referenced slot's index unchanged, so no other TDEF needs to be touched. In the common case (create + drop), the dropped FK got the last slot and is reclaimed cleanly; multi-drop in any order still reclaims every now-trailing orphan.

**B. `RenameRelationshipAsync` TDEF logical-idx name cookie update.** After `MSysRelationships` is rewritten as live rows with the new `szRelationship`, the rename method groups matches by (PK table, FK table) pair (mirroring `DropRelationshipAsync`), resolves both side TDEFs and column numbers, and calls `TryRenameFkLogicalIdxNameAsync(tdefPage, columnNumbers, otherTdefPage, newName, ct)` on each side. The helper:

1. Locates the matching FK logical-idx entry the same way drop does (`index_type == 0x02` + `rel_tbl_page` + `RealIdxColMapMatches`).
2. Walks the names section to find the entry's variable-length name byte range.
3. Computes `delta = newNameRecordSize - oldNameRecordSize` and shifts the trailing variable-column block (rest of names + var-col block) by `delta` via `Buffer.BlockCopy` (overlap-safe).
4. Writes the new length-prefixed UTF-16 name and updates `tdef_len`.

The cookie name reproduces `CreateRelationshipAsync`'s convention: PK side uses `newName`; FK side uses `newName + "_FK"` for self-referential relationships. Disambiguation against existing names on the same TDEF runs through the new `PickUniqueLogicalIdxNameAsync` helper (reuses `MakeUniqueLogicalIdxName`). Growth is handled by the logical TDEF-chain writer, so a rename can spill into a continuation page instead of leaving the cookie stale.

**C. Multi-column primary keys participate in live B-tree leaf maintenance.** This was effectively shipped as a side effect of W11 (composite-key concatenation) + W23 (canonical NUMERIC scale from descriptor); the README and §7 W8 row both still claimed multi-column PKs "ship the schema only". The W11 `MaintainIndexesAsync` already iterates per real-idx, encodes each column with its slot direction, concatenates to a composite key, and promotes any real-idx referenced by a `index_type == 0x01` logical-idx entry to unique. PK uniqueness is handled by the same logical-idx-walk that promotes non-PK unique-flag indexes; multi-column PK leaf maintenance has worked since W11 (2026-04-25) without being claimed.

Tests: 3 new round-trip tests in [`IndexPrimaryKeyWriterTests`](../../JetDatabaseWriter.Tests/Indexes/IndexPrimaryKeyWriterTests.cs) (`CompositePrimaryKey_OnInsert_ParticipatesInBulkRebuild`, `CompositePrimaryKey_OnUpdateAndDelete_LeafReflectsLatestState`, `CompositePrimaryKey_SurvivesAddColumn_LeafRebuilt`) confirm the PK leaf is maintained on bulk insert, update + delete, and `AddColumnAsync` rewrite. Relationship mutation coverage in [`RelationshipMutationTests`](../../JetDatabaseWriter.Tests/Relationships/RelationshipMutationTests.cs) confirms rename/drop via `IndexMetadata.Name` + `IndexMetadata.RealIndexNumber` round-trips, including multi-page endpoint TDEFs. Current DAO compact coverage for shortened-chain reclamation and relationship rename lives in `DaoStorageMaintenanceTests`, while mid-array drop orphan byte parity remains a residual gap.

W26 caveat:

- **Mid-array drop orphans still leak.** Dropping the older of two relationships on the same TDEF leaves a non-trailing real-idx slot orphaned (same disk impact as before). Closing this would require cross-TDEF `rel_idx_num` renumbering — out of scope for in-place mutation.

Shortened logical TDEF chains no longer leave unreachable continuation pages behind; the old pages are deallocated through the global free-list allocator after the rewritten chain is flushed.

### 7.17 W4-C-8 — what shipped (2026-04-27)

Closes the last remaining incremental B-tree bail. Both surgical paths (single-leaf `TrySurgicalMultiLevelMaintainAsync` and cross-leaf `TrySurgicalCrossLeafMaintainAsync`) now handle leaf and intermediate splits of arbitrary width — the previous "3+ pages → fall through to W4-D bulk rebuild" floor is gone.

What changed:

- **`TryGreedySplitInTwo` → `TryGreedySplitLeafInN`** ([`AccessWriter.cs`](../../JetDatabaseWriter/AccessWriter.cs)). Greedy left-fill of a spliced leaf entry list; opens a new page each time the next entry would overflow the per-page payload area. Returns the full ordered page list (`Count >= 2` on success). Bails only when a single entry's encoded key + 4-byte slot offset exceeds the entire page payload area (degenerate input — same case the old 2-way splitter rejected). No artificial N-cap — Access itself splits as needed and we match.
- **`TryGreedySplitIntermediateInTwo` → `TryGreedySplitIntermediateInN`**. Same greedy left-fill pattern, but each candidate page is validated by `IndexBTreeBuilder.TryBuildIntermediatePage` so the §4.4 prefix-compression budget the simpler leaf splitter cannot model is respected exactly. Linear extension probe (O(N²) total page-build calls per intermediate) — fine because N is bounded by the page's pre-overflow entry count.
- **`PrepareAncestorSplitWrites`** generalised from `(leftSummary, rightSummary)` to `(leftSummary, rightSummaries[])`. Inserts N-1 new summary entries at `path[^1].TakenIndex + 1`. When the original entry was the last on the parent, the right-most new summary is propagated upward via `PrepareAncestorReplaceWrites`.
- **`IntermediateOp` aggregator** in `TrySurgicalCrossLeafMaintainAsync` emits one `Replace` + (N-1) `InsertAfter` ops at the same `OriginalIndex`. `ApplyIntermediateOps`'s existing declaration-order tie-break keeps the new summaries left-to-right.
- **Sibling chain stitching** for both leaf and intermediate N-way splits: `pages[0]` reuses the original page number; `pages[1..N-1]` get freshly allocated page numbers. Page `i`'s `prev_page` = `pages[i-1]`, `next_page` = `pages[i+1]` (or the original `next_page` for the last page). The far-side neighbour's `prev_page` is patched to point at the LAST new page.
- **`tail_page` cascade (§4.5)** — when the splitting page is the rightmost in any captured ancestor's subtree, the new rightmost is the LAST appended page. The existing `intermediateTailOverrides` plumbing picks this up automatically because `lastChildPage = newEntries[newEntries.Count - 1].ChildPage` now resolves to the LAST `InsertAfter` op's `NewChildPage`.
- **Recursive intermediate split through any level** for the intermediate-side N-way too: when an N-way intermediate split has a grandparent, it emits a `Replace` + (N-1) `InsertAfter` ops on the grandparent and re-enqueues it; when there is no grandparent (the split was at the root), the new root intermediate carries N summary entries (one per split page) with `tail_page` = the LAST split page's tail.

Tests: 5 new round-trip tests in [`IndexSurgicalNWaySplitTests`](../../JetDatabaseWriter.Tests/Indexes/IndexSurgicalNWaySplitTests.cs) cover (1) a 40-row bulk insert whose splice spans 3 leaf pages on a fresh table, (2) a 200-row bulk insert spanning ~10 leaf pages, (3) a small seed + 60-row append batch onto a single rightmost leaf (exercises `tail_page` cascade), (4) a deeply seeded tree + drip-fed inserts + final 50-row clustered batch that triggers an N-way leaf split feeding multiple summaries into a near-capacity parent intermediate (W4-C-8 + W4-C-7-v2 interaction), and (5) post-N-way uniqueness enforcement on every leaf page produced by the split. The full 2345-test suite passes; representative DAO compact coverage for advanced-index maintenance now lives in `AdvancedIndexKeysAndBTreeMaintenance_SurviveCompactAndRepair`, with per-sub-phase byte parity still tracked in §8.

W4-C-8 caveats:

- **Recursive intermediate collapse on cascading delete underflow.** ✅ **Shipped (W4-C-8+, 2026-04-27).** See §7.18.
- **Validation gap (§8):** representative DAO CompactDatabase coverage now exercises advanced-index maintenance after insert/update/delete, but the splice → multi-page write paths still lack Access-authored byte-level parity for each individual sub-phase.

### 7.18 W4-C-8+ — what shipped (2026-04-27)

Closes the trailing surgical-bail listed under §7.17 caveats: a multi-group delete batch that empties an entire intermediate page subtree no longer falls through to the W4-D bulk rebuild. The previous bail in `TryStageIntermediateRewritesAsync`

```csharp
if (newEntries.Count == 0) return false;  // pre-W4-C-8+
```

…has been replaced with a recursive collapse that orphans the now-empty intermediate, emits a `Remove` op against the grandparent at the captured `IndexInParent`, and re-enqueues the grandparent into `pending` so the same machinery can chain another collapse if the grandparent itself empties out. Only when the empty page has no captured grandparent (true root collapse) does the path bail; that case is rare and W4-D handles it cleanly by allocating a fresh single-leaf root.

What changed:

- **Cascade-collapse branch in `TryStageIntermediateRewritesAsync`** ([`AccessWriter.cs`](../../JetDatabaseWriter/AccessWriter.cs)). When `newEntries.Count == 0`, look up `intermediateGrandparent[deepest]`, stage a `Remove` op on the grandparent at the captured `IndexInParent`, push the grandparent onto the `pending` worklist, and `continue` — the dead intermediate is left for Compact & Repair to reclaim (same disposal model as W4-C-6 dead-leaf orphans).
- **`tail_page` lookup via `GetEffectiveTailPageAsync`** for the non-parent-of-leaf branch. When recomputing an intermediate's `tail_page` after a cascade-induced child rewrite, the previous `intermediateTailOverrides.TryGetValue(...)` lookup only found pages already staged THIS pass; deeper-up cascades read through to the live page header. The new helper consults staged overrides first, then falls back to the on-disk header — matching the W4-C-7-v2 plumbing already used on the split side.
- **`emptyingLeaves` pre-pass in `TrySurgicalCrossLeafMaintainAsync`**. Before the per-group T8 neighbour-bail check, speculatively splice each group's leaf to discover which leaves will end up empty after the batch. A leaf whose neighbour is also marked emptying is no longer a bail trigger — neighbours that disappear together don't need cross-rewrite coordination.
- **Run-boundary stitching** for contiguous emptying-leaves runs. A new pre-Phase-C pass walks each run rightward and leftward to compute the surviving outer neighbours, then patches `leafPrevPointerPatches[deadPrev] = surv` / `leafNextPointerPatches[deadNext] = survLeft` with idempotent collision handling so the surviving non-unique-key spillover chain stays consistent across the orphaned run.

Tests: 3 new round-trip tests in [`IndexSurgicalCascadingIntermediateCollapseTests`](../../JetDatabaseWriter.Tests/Indexes/IndexSurgicalCascadingIntermediateCollapseTests.cs):

- **`DeepTree_DeleteEmptiesEntireMidIntermediateSubtree_DataRoundTrips`** — 600-row composite-key build (3-level tree), delete an entire mid-tree subtree (60 rows tagged 9), assert the remaining 540 rows round-trip with key-pair fidelity.
- **`DeepTree_BulkDeleteAcrossEntireSubtree_DataRoundTrips`** — 1500-row build, bulk-delete ~1475 rows leaving 25 scattered survivors. Exercises every shape simultaneously: multi-group delete, contiguous emptying-leaves runs, and partial-content boundary leaves; surviving 25 rows must round-trip with their 60-stride key pattern intact.
- **`DeepTree_DeleteThenReinsertAcrossCollapsedSubtree_DataRoundTrips`** — 600-row build, delete then re-insert across the now-empty subtree, assert the re-inserted rows are visible AND no resurrection of the deleted rows.

Running the full 2348-test suite passes. Validation gap §8 still applies.

W4-C-8+ caveats:

- **The cascade-collapse branch is dormant defense-in-depth on current CRUD workloads.** The two `TrySurgicalCrossLeafMaintainAsync` bails still upstream (T8 neighbour-in-groups for partial-content boundary leaves, plus several pre-flight invariant checks) divert most cascading-underflow workloads to the W4-D bulk rebuild before reaching `TryStageIntermediateRewritesAsync`. The new code is exercised by the round-trip tests above (which validate end-to-end correctness regardless of which path runs) and is in place for any future surgical-path improvements that lift those upstream bails.
- **Validation gap (§8):** ACE/Jet4 DAO CompactDatabase coverage now includes a representative advanced-index maintenance path in `AdvancedIndexKeysAndBTreeMaintenance_SurviveCompactAndRepair`; remaining W4-C risk is Access-authored byte-level parity for individual sub-phases rather than lack of compact automation.

## 8. Validation strategy

The current cross-feature coverage matrix lives in [writer-disk-format-validation-matrix.md](writer-disk-format-validation-matrix.md). General DAO validation rules live in [dao-validation-strategy.md](dao-validation-strategy.md). Update the matrix whenever a writer disk-format feature gains stronger coverage.

Every new writer phase landing on disk format should be validated against:

1. **Round-trip in this repo:** writer -> reader (using the relevant parser) -> assert structural equivalence.
2. **DAO on Windows:** when possible, mutate an Access-authored fixture such as Northwind, run `DAO.DBEngine.120.CompactDatabase`, reopen the compacted file, and verify the feature that motivated the test.
3. **Cross-tool sanity:** open with [Jackcess](https://jackcess.sourceforge.io) or [mdbtools](https://github.com/mdbtools/mdbtools) (`mdb-schema -E`) and confirm the index metadata matches.
4. **Manual Access UI verification:** keep manual Compact and Repair notes as supplemental evidence for UI-only workflows, not as the only signal when DAO can automate the same mutation.

Automated DAO coverage now exists for the matrix rows covering the highest-risk PK/FK compact paths, multi-table index/FK stress, storage-maintenance index-page reuse and TDEF-chain reclamation, encrypted compact, fresh ACCDB bootstrap, a representative Northwind-hosted complex-column/LVAL compact path, representative ACE/Jet4 advanced index key encodings plus B-tree maintenance, and conditional Jet3 writer-emitted primary/normal index maintenance in an Access-authored `.mdb` host when the installed DAO engine can open Access 97 files. Treat remaining §7 validation-gap notes as byte-level parity or per-format residual gaps, not as blanket statements that DAO compact coverage is absent. Promote any remaining high-risk residual gap into [DaoValidationTests.cs](../../JetDatabaseWriter.Tests/RoundTrip/DaoValidationTests.cs), [DaoStorageMaintenanceTests.cs](../../JetDatabaseWriter.Tests/RoundTrip/DaoStorageMaintenanceTests.cs), or [AccessRoundTripTests.cs](../../JetDatabaseWriter.Tests/RoundTrip/AccessRoundTripTests.cs).

## 9. Residual work and compatibility gaps

The W-table in §7 records both historical shipped phases and current behavior. All W4 B-tree maintenance follow-ups listed in earlier revisions are now shipped: tail-page append handling, single-leaf mutation, same-leaf and cross-leaf surgical mutation, leaf merge, N-way leaf/intermediate split, and recursive intermediate collapse. Keep the detailed phase notes in §7 as implementation history; treat the items below as the remaining live follow-ups.

- **W7 General 1033v1 (Access 2010+).** The Jackcess-derived `GeneralTextIndexEncoder` and DAO-derived V2010 suffix tables are in place for fixture/probe coverage, but writer-maintained text/memo indexes still need table-collation selection before General 1033v1 becomes the production path. Remaining work: route the encoder from `IndexKeyEncoder`, extend the V2010 suffix tables beyond sampled DAO contexts, and broaden byte-level fixture coverage.
- **W14 `DropRelationshipAsync` mid-array real-idx compaction.** Trailing real-idx physical-descriptor slots that are unreferenced by any logical-idx after a drop are reclaimed in place (`TryReclaimTrailingRealIdxAsync`; see §7.16). Mid-array orphans, such as dropping the older of two relationships on the same TDEF, are still left for Access to reclaim because compaction would require renumbering `index_num2` / cross-TDEF `rel_idx_num` references on every other table that points at the slot.
- **Hosted Access validation.** The matrix records DAO tests that pass on Access-equipped hosts, but ordinary CI may still skip them when Access/DAO is unavailable. A Windows runner with Access installed would promote the guarded DAO tests into CI; manual UI checks remain supplemental evidence only.

## 10. References

- [mdbtools HACKING.md §"TDEF (Table Definition) Pages"](https://github.com/mdbtools/mdbtools/blob/master/HACKING.md) — index block layout
- [mdbtools HACKING.md §"Indices"](https://github.com/mdbtools/mdbtools/blob/master/HACKING.md) — index page format and sort-key encoding
- mdbtools source: [`src/libmdb/index.c`](https://github.com/mdbtools/mdbtools/blob/master/src/libmdb/index.c) — read-only index walker
- Jackcess source: [`com.healthmarketscience.jackcess.impl.IndexImpl`](https://github.com/jahlborn/jackcess/tree/master/src/main/java/com/healthmarketscience/jackcess/impl) and `IndexData` — only known open-source *write* implementation
