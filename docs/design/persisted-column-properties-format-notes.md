# Format notes: `MSysObjects.LvProp` (`KKD\0` / `MR2\0`)

**Status:** Parser and writer implemented for covered layouts; complete schema preservation and native mutation acceptance remain incomplete.

This document captures the on-disk layout of the property blob stored in `MSysObjects.LvProp`. The parser ([`ColumnPropertyBlock`](../../src/JetDatabaseWriter/Schema/Models/ColumnPropertyBlock.cs)) and writer ([`ColumnPropertyBlockBuilder`](../../src/JetDatabaseWriter/Schema/ColumnPropertyBlockBuilder.cs)) follow the layout below; round-trip tests exercise both against Access-authored fixtures.

Property names, target names and Text/Memo values use the database header's ANSI code page on Jet3 and UTF-16LE on Jet4/ACE. Jet3 writes use strict encoding: an unrepresentable character refuses the operation rather than becoming a best-fit character or question mark. Header code page zero defaults to Windows-1252; an unsupported nonzero code page is refused, never repaired by assuming UTF-8. The cp1251 property tests exercise names, values and linked ODBC NameMap entries; Microsoft-produced non-1252 fixtures remain part of the interoperability corpus work.

Stored defaults and column rules must evaluate successfully before a row can be written. Null is a successful default result, not an evaluation failure. Required comes from persisted properties; the old library-private TDEF 0x08 marker is not interpreted as Required. GUID AutoNumbers use the GUID-specific 0x40 flag; flags on other column types retain their own meanings.

The Microsoft-authored `TestV2003`/`TestV2010` Table4 and
`TestIndexCodesV2000`/`TestIndexCodesV2010` Table13 GUID fields have a
16-byte fixed slot, flags `0x43` and a `GenGUID()` default. DAO120 reports
type 15 and attributes 1, and allows explicit GUID updates. Nullable Table13
fields preserve explicitly assigned Null; the Table4 primary key refuses it.
Omission uses the default. GUID generation is distinct from the numeric
`dbAutoIncrField` attribute and does not imply a read-only or non-null field.

Writer-generated GUID columns persist `GenGUID()` as a Memo (`0x0C`)
property with DDL flag 1 alongside the `0x40` marker, and preserve
nullable/Required semantics through schema rewrites. The Microsoft-authored
fixtures also contain valid Text (`0x0A`)/DDL-1 defaults, which are preserved.
A DDL-0 default can be visible through DAO metadata without being executed
for an omitted native insert; matching the expression string alone does not
establish native generation.
DAO creation with `dbSystemField` and `GenGUID()` produces another native
variant, flags `0x13`; its system bit is preserved independently of GUID
generation. A plain GUID with a `GenGUID()` default has flags `0x03`.
Removing or replacing the stored default can retain flags `0x43`; omission
honors that actual default or stores Null when it is absent. Schema edits do
not recreate a removed default. The descriptor marker alone does not request
GUID generation.
`ReplicationIdDescriptorTests` and `DaoReplicationIdTests` keep fixture,
DAO-created and writer-created evidence separate. Non-nullable CLR POCO
`Guid.Empty` omission is a library mapping convention; explicit row values
can store that GUID.

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
DAO3.6 (Jet3) and DAO120 (Jet4/ACE) accept extra empty `0x80` chunks after
an existing populated dictionary. Empty chunks preserve the current name
binding. The reader retains them, and the builder leaves them empty while
appending new names to the active populated pool. If the source has only empty
pools, a newly needed populated dictionary precedes them.
`ColumnPropertyBlockTests` checks exact preservation and new-target/name placement.
`DaoPropertyMutationTests` verifies metadata, valid native insertion/deletion,
rule enforcement and compact after property/schema edits.
`NativePropertyVariantsTests` independently checks the Jet3 empty-chunk control.

Both native engines reject the tested interleaved populated dictionaries and
adjacent globally indexed split dictionaries before writer edits. This does
not establish that every possible multiple-populated-pool layout is invalid.
Replacement binding for successive populated pools still lacks a native-accepted
specimen; that requirement remains in S4.

Additional corpus checks read complete `MSysObjects.LvProp` values, including
external chains and application/system objects, without executing stored code.
Every inspected value in these author-distributed samples has one dictionary:

| Sample | SHA-256 of extracted database |
|---|---|
| [ViewLvFields v1.2](https://www.isladogs.co.uk/lv-fields/files/ViewLvFields_v1.2.zip) | `8399C609AD6A4039A6F2E3A5E8387974402AD3CAC4ECD91792F84278BC211554` |
| [Version Control v5.0.1 ACCDA](https://github.com/joyfullservice/msaccess-vcs-addin/releases/download/v5.0.1/Version_Control_v5.0.1.zip) | `7C844E43AC11A91AB6BD3AE88F6790EC143CCD0E4C9B5CB6A0C035355F1DECF0` |

DAO120-created Jet4 and ACE controls with 16 text columns and distinct
61-character custom property names reached a 65,383-byte `LvProp` value with
447 dictionary names. The next append failed with native "Property value is too
large" rather than producing a second dictionary. This characterizes those
controls, without establishing a universal native property-size limit.

### 2.4 Property-block chunk (type `0x00`)

One per target. Each column with properties has a block named after the column. The *table* itself has a block with an **empty** target name and chunk type `0x00`, which can be anywhere in the blob: usually first in the Jet3 and Jet4 fixtures (but last of four in compIndexTestV2000's `Table1`), and usually last in the ACCDB fixtures (block #11 of 12 in NorthwindTraders.accdb's `OrderDetails`, but the third of 18 in calcFieldTestV2010's `Table1`). Block-to-target binding is by the embedded target name; the table-level block is never named after the table ([`ColumnPropertyBlock.FindTableTarget`](../../src/JetDatabaseWriter/Schema/Models/ColumnPropertyBlock.cs) finds the first empty-named block). Access-authored column blocks use chunk type `0x01`.

Payload layout (`chunkLen − 6` bytes total):

```
0       4       innerHeader   uint32   opaque to the reader. DAO programmatic table creation
                                       has been observed writing 4 + 2 + targetNameLen here
                                       (header through target name), not the full payload length.
4       2       targetNameLen uint16   bytes of UTF-16LE target name
6       ...     targetName    UTF-16LE bytes (Jet4) / codepage bytes (Jet3)
N       ...     entries       sequence of property entries until payload exhausted
```

Writer parity: preserve unrecognized inner-header values verbatim. When a parsed header matches `4 + 2 + targetNameLen`, treat it as the native name-length field and recompute it if the target name changes. New targets use that DAO-observed value. The encoding, opaque headers, entry padding, raw name-pool bytes (including unused and duplicate names), original entry indices and source chunk ordering survive property edits and Add/Drop/Rename rewrites. New names append to the target's existing pool; new targets use the final existing pool. A new pool is inserted only when no source pool can precede a target that needs names. Removing the last target retains the original name pool, and a parsed signature-only blob remains present.

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
| `0x09` | Binary | opaque bytes; linked-table property caches use this for `GUID` properties carrying 16-byte GUID payloads |
| `0x0A` | Text | UTF-16LE (Jet4); used for `DefaultValue`, `ValidationRule`, `ValidationText`, `Description`, `Format`, `Caption`, etc. |
| `0x0B` | OLE | opaque bytes |
| `0x0C` | Memo | UTF-16LE (Jet4) |
| `0x0F` | GUID | 16 bytes |

Expression and descriptive properties can use Text (`0x0A`) or Memo (`0x0C`); edits preserve an existing text property's type. A new table-target `ValidationRule` uses Memo and `ValidationText` uses Text, both with DDL flag 1. DAO120-created table rules and a synthetically migrated Text-typed rule remain readable, preserve validation text through setters and column rename, and retain native enforcement through Compact & Repair.

## 4. Property-block subtype variation

The current [mdbtools notes](https://github.com/mdbtools/mdbtools/blob/dev/HACKING.md#properties) and [Jackcess model](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/PropertyMaps.java) identify `0x00` as the object/table, `0x01` as a column and `0x02` as an index. Jackcess distinguishes maps by both name and subtype because an index can share a column name. DAO3.6 Jet3 and DAO120 Jet4/ACE controls independently create a column and an index both named Score with distinct Description/SharedCustom values. Native serialization emits separate `0x01` and `0x02` targets in one shared dictionary, retains their separate values through native edits and compact, and corroborates that subtype distinction. Library lookup, constraint hydration and projection bind column properties only to subtype `0x01`, table properties to empty-name subtype `0x00`, and retain subtype `0x02` index maps independently. `PropertyTargetIdentityTests` checks index-first source ordering, and `NativePropertyTargetIdentityTests` checks the distinct native values through Add/Drop/Rename and native mutation/compact in all writer modes. Index maps are removed only when their logical index is dropped. Arbitrary synthetic subtype substitutions have no established native acceptance.

The 2026-05-11 DAO FK baseline found that programmatic DAO table creation emits column property blocks with subtype `0x01` for the simple Short Text / Long / AutoNumber schema under test. Subtype `0x00` remains parseable as an object/table map; column lookup and hydration bind only subtype `0x01`.

This library:
- **Reads:** accepts all three chunk subtypes as property blocks; unknown chunk types are preserved as opaque bytes for round-trip.
- **Writes:** preserves parsed subtypes on round-trip and uses `0x01` for newly-created column property targets.

## 5. DAO-created column property facts (2026-05-11)

The FK Compact investigation compared writer-authored and DAO-authored tables with this schema: `Parent(ParentId AutoNumber PK, Label Text Required)` and `Child(ChildId AutoNumber PK, ParentId Long Required, Detail Text Nullable)`.

Observed DAO `MSysObjects.LvProp` facts:

- The integral AutoNumber columns in this baseline do not carry a `Required` property, even when DAO field/index objects are marked required. Their non-null behavior comes from the integral AutoNumber flag and PK/index metadata. Generated GUID fields retain independent `Required` and index semantics; the `0x40` GUID marker does not make a field non-null.
- Text columns carry `AllowZeroLength = False` (`dataType = 0x01`, Boolean value `0x00`).
- Non-null text columns carry `Required = True`; nullable text columns can carry `Required = False`.
- Boolean property entries created by DAO use `ddlFlag = 0x01`.
- The name pool may contain both `Required` and `AllowZeroLength`; target property order in the observed blobs was `Required` then `AllowZeroLength` for non-null text, and `AllowZeroLength` then `Required` for nullable text. Access appears to tolerate either order when reading, but byte-for-byte DAO parity should preserve the observed order where practical.

These LvProp differences were real DAO deltas, but matching them was not sufficient by itself to make writer-created FK tables survive DAO Compact & Repair. The final compact fix also required system-table row placement, Type=8 relationship catalog objects, relationship ACE rows, shared table/index usage-map rows, and in-place single-leaf reuse. Treat the LvProp shape as a compatibility fact, not as the whole compact root cause.

## 6. Linked-table property-cache facts (2026-05-24)

`linked-odbc-lvprop` compares the Jackcess V2007 ODBC linked-table fixture with the Access-linked-table fixture and writes [format-probe-linked-odbc-lvprop.md](../format-probe/format-probe-linked-odbc-lvprop.md).

Observed linked-table facts:

- The ODBC Type 4 fixture row stores a 6,465-byte `MR2\0` `LvProp` payload with 21 parsed targets, no unknown chunks, and a table-level `NameMap` property. The parser/builder pair retains opaque inner headers, raw name-pool bytes and indices, entry padding and source chunk positions. Guarded DAO120 checks accept edited single-pool Jet4/ACE output with nonzero entry padding and duplicate/unused names, including rule enforcement and Compact & Repair. These added bytes are synthetic variants of DAO-created sources. Native empty-pool and repeated-target controls are described in sections 2.3 and 8. Multiple populated pools remain unverified.
- The linked Access Type 6 comparison fixture also stores a real `MR2\0` cache in this older Access-authored Jackcess corpus, but DAO-authored Access-file and text links are separately observed to tolerate null `Lv`/`LvProp`/`LvModule`/`LvExtra` cache columns.
- `NameMap` is `dataType = 0x0B` and remains partly opaque. The probe extracts recognizable UTF-16 string runs for human inspection, while the writer now synthesizes a conservative generated `NameMap` with table/column GUID records and source table/column names. The generated payload is parseable and structurally useful, but it is not expected to be byte-identical to Access-authored ODBC caches.
- Linked-cache `GUID` properties appear as `dataType = 0x09` Binary with 16-byte GUID payloads, not `dataType = 0x0F`.

Upstream check: mdbtools and Jackcess both parse/preserve generic property maps, but neither project appears to generate Type 4 ODBC linked-table schema-cache `LvProp`/`NameMap` payloads from a connection string and remote table name. This codebase now generates the property-block envelope and a best-effort ODBC `NameMap` from caller-supplied source columns; exact live-source DAO behavior remains DSN/source-dependent until more ODBC fixtures are available.

## 7. Locating a table's blob

A table's blob is the `LvProp` column of the `MSysObjects` row whose `Id` equals the table's TDEF page. The match is on the whole Id, never on its low 24 bits: Access gives forms, reports, modules and some queries Ids with the high bit set (`0x80000000 | n`), and the containers Ids of the form `0x0F0000nn`, and their low 24 bits can equal a table's TDEF page. In NorthwindTraders.accdb the module `modDAO` has Id `0x80000032` and the table `Companies` TDEF page `0x32`, and the module's row comes first in the catalog; eight Access-authored fixture tables collide this way (`LvPropReadTests`).

The blob is the column's stored bytes, inline, on one LVAL row or in an LVAL chain, handed to the parser unchanged ([`ColumnPropertyReader`](../../src/JetDatabaseWriter/Catalog/ColumnPropertyReader.cs) reads it through a typed `MSysObjects` scan that decodes only `Id` and `LvProp`). Nothing may sniff its content as if it were an OLE object: a `KKD\0` blob holds code-page text, so a description that starts "BMI" or "%PDF" begins with a file signature.

## 8. Through schema rewrites

Column add, rename and drop preserve stored `Filter` and `OrderBy` entries
unchanged, including references to renamed or removed columns. These are
display properties, not enforced table rules. DAO3.6 retains their exact text
through related and unrelated column edits, reopen and compact in Jet3 and
Jet4. The writer follows that behavior without parsing or executing the text;
opaque and repeated entries also retain their raw values and metadata.
`NativeDisplayPropertyTests` compares independently native-edited controls and
writer-edited subjects, including native insert/update/delete after each edit.
The local engine was DAO3.6, `dao360.dll` 10.0.26100.5074. ACE has managed
preservation coverage and separate DAO120-guarded controls; those native checks
remain an evidence gap when DAO120 is unavailable. Access UI Name AutoCorrect
remains a separate requirement and is not applied by this API.

Column rename/drop and table drop preserve surviving lookup `RowSource` and
`RowSourceType` entries unchanged. DAO3.6 retains direct table names, SQL and
saved-query SQL through the tested referenced-column edits and table deletion,
then compacts the database. Preservation does not rebind stale names or promise
that a lookup or saved query can still execute after its referenced object is
renamed or removed. Schema edits do not execute those sources. Table validation
rules and calculated expressions retain their separate dependency checks.

`SchemaRewritePropertyPreservationTests` checks raw lookup metadata preservation
across Jet3/Jet4/ACE and direct/automatic/explicit modes, including repeated and
opaque entries. `NativeLookupPropertyTests` compares native and writer edits
for direct sources, SQL and saved-query indirection through reopen and compact.
The native controls also cover parameterized SELECTs, UNION, crosstab and stored
append/update/delete/make-table definitions. They compare each native QueryDef's
type and SQL after both native and managed dependency edits and compact, without
executing a saved definition. Frozen DAO3.6-authored Jet3/Jet4 specimens extend
hosted coverage to every write mode: complete query rows, catalog field values,
query permissions and lookup properties remain unchanged through column
rename/drop and referenced-table deletion. Fixture hashes and production details
are in `Databases/QueryDependencies/README.md` under the test project.
The native engine is DAO3.6 for Jet3/Jet4; fresh ACE DAO120 confirmation,
remaining query/application forms and Access UI Name AutoCorrect remain separate
evidence gaps.

`NativeSqlSpecificQueryPreservationTests` extends the Jet3/Jet4 controls to
stored data-definition queries (native Type 96), row-returning pass-through
queries (112) and action pass-through queries (144). Pinned DAO3.6-authored
specimens preserve complete query/catalog values and each query's permissions
through rename, case-only rename, column drop and referenced-table deletion in
every write mode. Independent native edits, native compact and writer-output
compact retain exact SQL, Connect, ReturnsRecords, ODBCTimeout, MaxRecords and
long Description properties. A deliberate stored DSN change must alter the
metadata snapshot. The inert connection names are never opened and stored
definitions are never executed. Provenance and hashes are in
`Databases/SqlSpecificQueries/README.md` under the test project. These controls
do not establish pass-through execution or ACE/UI behavior.

AddColumn, DropColumn and RenameColumn rebuild a table and write its blob again. [`PersistedPropertyProjector`](../../src/JetDatabaseWriter/Schema/PersistedPropertyProjector.cs) builds the new blob from the stored one and changes only what the rewrite changes:

- The table-level block keeps its position and every entry except `NameMap`, which is dropped.
- A dropped column's block is removed, and a renamed column's block takes the new name, keeping its chunk type and entries.
- Other subtype maps retain their name, entries and position independently of column names. Index maps follow the survival of their actual logical index. An orphan column map is retained unless an added column takes its name.
- A surviving column keeps every entry byte for byte, except a property the writer models (one [`JetExpressionConverter.ApplyColumn`](../../src/JetDatabaseWriter/Schema/JetExpressionConverter.cs) emits: `DefaultValue`, `ValidationRule`, `ValidationText`, `Description`, `Required`, `AllowZeroLength`, `Expression`, `ResultType`) whose value the rewrite changes, such as an expression that names a renamed column. That entry takes the new value in place, keeping its data type when both are text (Access stores `DefaultValue`, `ValidationRule` and `Expression` as Memo, `0x0C`), DDL flag, padding, original name index and position. Table-rule replacement updates every equivalent repeated rule/text entry across all table targets, retaining each entry's metadata; removal clears every matching rule/text entry. Contradictory original repeats refuse before writing. Table metadata binds only to the first empty-name target, matching DAO3.6 and DAO120: a rule or message stored only in a later table target is inactive. Column properties currently resolve per property across repeated targets after consistency checks. Schema projection changes every matching entry while retaining target and source-chunk structure. Contradictory calculated Expression/ResultType repeats also refuse mutation. The guarded `NativePropertyVariantsTests` verify original Jet3 property edits and equivalent repeated rule/text entries and same-subtype table targets on DAO3.6 Jet3 and DAO120 Jet4/ACE sources, with native read/write, enforcement and compact after Set/Rename/Add/Drop. Its split table-target control verifies that native metadata ignores later-only text; managed table-rule reads retain that binding. The repeated layouts are synthetic variants accepted by native engines, not Microsoft-authored repeated-layout fixtures.
- An added column gets the entries `ApplyColumn` emits for its definition.
- Unknown chunks retain their source positions relative to surviving chunks. Existing name pools retain their raw content and placement; new names append without renumbering prior entries.

Jet4/ACE descriptors retain the native identity at bytes 9–10 independently of the row-layout column number at bytes 5–6. Added columns receive unused identities above surviving identities when possible. The Microsoft-authored TestV2010 Table4 identities and native compact/readback are covered by `NativeSchemaPreservationTests` and `NativeSchemaDaoTests`; raw TDEF preservation is exercised separately by `TDefRewriteBytePreservationTests`, `NativeTDefChainPreservationTests` and `IndexDescriptorPreservationTests`.

Replacement definitions resolve through the catalog after index descriptor edits, retaining calculated `ResultType` hydration independently of the physical descriptor type. Every schema rewrite publishes private replacement storage through the original TDEF chain and updates that original catalog row with the projected blob. `TDefRewriteComposer` retains source header bytes, the complete declared sentinel/trailer suffix, surviving column descriptors and sort metadata; it overlays the rebuilt structural counts, counters and storage references and projects long-value map records by column identity. The original chain owns growth/shrink, retaining surviving continuation headers and original physical padding beyond the changed metadata. The composed physical-chain payload, including padding, must fit the configured definition and schema traversal byte limits before its output buffer is allocated or old storage is reclaimed. The complete private replacement chain is reclaimed. Table identity, ownership and permission rows remain attached.

## Table-level validation

Table rules bind to the empty-name table property target and evaluate complete
candidate rows, including unchanged columns. Null results pass; false results
and unsupported expressions refuse mutation. Column rename rewrites rule
references, and dropping a referenced column is refused.

The guarded `DaoTextCollationTests` table-rule cases compare writer output with
DAO120. They cover unrelated updates, Null-result rows, cascade acceptance and
refusal, rollback of cascades and rule/text changes, and a native `Partition(...)`
rule that the writer evaluates before an unrelated update. The Partition case
also checks selected native formatting boundaries, DAO updates and Compact &
Repair. `PartitionTableValidationRuleTests` separately verify reopened writers
and byte-identical rejected inserts/updates across Jet3, Jet4 and ACE; those
fixtures establish library behavior. The
property-migration case changes a DAO source rule to Text synthetically; it
establishes native acceptance of the edited output, not native provenance of
that encoding. Access97-engine coverage remains part of the corpus requirements.
