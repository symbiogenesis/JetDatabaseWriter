# Design: Persisted column properties (`DefaultValue`, `ValidationRule`, `ValidationText`, `Description`)

**Status:** Proposed — multi-PR effort
**Owner:** TBD
**Validation requirement:** every PR landing on disk-format-changing code must round-trip through Microsoft Access on Windows before merge.
**Scope-limit guardrail:** no PR in this series may ship without (a) round-trip tests in this repo *and* (b) a manual Access verification note in the PR description.

---

## 1. Background

`README.md` currently marks two `ColumnDefinition` constraints as `❌ client-side only`:

| Constraint | Today |
|---|---|
| `DefaultValue` | Substituted for `DBNull.Value` at insert time. Lives only on the writer instance. |
| `ValidationRule` | A CLR `Func<>` cannot be serialized. Lives only on the writer instance. |

Both are *persistable* in the JET format, but not in the place initially assumed. Per [mdbtools `HACKING.md` §"Properties"](https://github.com/mdbtools/mdbtools/blob/master/HACKING.md):

> Design View table definitions are stored in **`LvProp` column of `MSysObjects`** as OLE fields. They contain default values, description, format, required... They start with a 32-bit header: `'KKD\0'` in Jet3 and `'MR2\0'` in Jet4.

Reference implementation: `mdbtools/src/libmdb/props.c` (read path only — mdbtools does not write properties).

This library currently writes a slimmed 9-column `MSysObjects` (`Id, ParentId, Name, Type, DateCreate, DateUpdate, Flags, ForeignName, Database`). Real Access writes ≥15 columns including `Owner`, **`LvProp`** (LongBinary), **`LvExtra`** (LongBinary), `Connect`, `RmtInfoLong`, `RmtInfoShort`. **The column where defaults are supposed to live is not in the bootstrap.**

## 2. Goals / non-goals

### Goals
- Persist `DefaultValue` (any literal CLR value convertible to a Jet expression string) per column, round-trippable through this library and Microsoft Access.
- Persist `ValidationRule` (string Jet expression) and `ValidationText` (string) per column.
- Persist `Description` (string) per column and per table — same blob, "for free."
- Surface persisted values via `ColumnMetadata` on read.
- Preserve persisted values across `AddColumnAsync` / `DropColumnAsync` / `RenameColumnAsync` (which today rebuild the TDEF).
- New `ColumnDefinition` API: `DefaultValueExpression`, `ValidationRuleExpression`, `ValidationText`, `Description`. The existing `object? DefaultValue` and `Func<> ValidationRule` remain as client-side conveniences.

### Non-goals (for this series)
- `Format`, `DecimalPlaces`, `InputMask`, `Caption`, `Required` (deducible from `IsNullable`), `IndexedNoDup`, lookup metadata. These ride the same blob and can be added later.
- Calculated columns (Access 2010+ expression columns).
- Editing properties on tables created by *other* tools that already have a populated `LvProp` blob with chunks this library doesn't recognize. (Read+preserve-unknown-chunks is required; *editing* unknown chunks is not.)
- Encrypted-database write (already unsupported).

## 3. On-disk format

### 3.1 `MSysObjects.LvProp` (LongBinary / OLE)

A single OLE blob per `MSysObjects` row. Empty / null when no properties are set.

### 3.2 Blob layout (Jet4 / ACE — `MR2\0`)

```
Offset  Size    Field
0       4       'M' 'R' '2' 0x00     header magic (Jet4); 'K' 'K' 'D' 0x00 for Jet3
4       ...     Chunks (concatenated until end of blob)
```

Each chunk:

```
0       2       chunkLen        uint16, little-endian, total bytes including these 2
2       1       chunkType       0x80 = name pool, 0x01 = property block
3       ...     payload (chunkLen − 3 bytes)
```

#### 3.2.1 Name-pool chunk (`0x80`)

Exactly one per blob, must come first. Provides the name dictionary referenced by index from property blocks.

```
0       2       nameCount       uint16
2       ...     names           sequence of length-prefixed strings
                                  Jet4: uint16 byteLen + UTF-16LE bytes
                                  Jet3: uint8  byteLen + Jet3 codepage bytes
```

Name-pool indices (0-based) are referenced by property entries. Names of *table-level* properties (e.g. `Description`) and *column-level* properties (e.g. `DefaultValue`, `ValidationRule`, `ValidationText`, `Description`, `Format`, `DecimalPlaces`, `InputMask`) are mixed in this single pool.

#### 3.2.2 Property-block chunk (`0x01`)

Zero or one per *target* (one for the table itself, one per named column). Order: table block first (named after the table), then one per column (named after the column).

```
0       2       blockLen           uint16, total bytes including these 2
2       1       blockType          0x00 (per mdbtools props.c)
3       2       targetNameLen      uint16  Jet4 (uint8 in Jet3)
5       ...     targetName         UTF-16LE bytes (Jet4) / codepage bytes (Jet3)
                                   (For column blocks this is the column name. For the
                                    table block it is the table name.)
N       ...     entries            sequence of property entries until blockLen consumed
```

Each property entry:

```
0       2       entryLen        uint16, total bytes including these 2
2       1       ddlFlag         0x00 normally; 0x01 flags "DDL-set" (writable by user)
3       1       dataType        Jet column-type code:
                                  0x0A = Text     (varchar / Unicode string)
                                  0x0C = Memo     (long text)
                                  0x03 = Integer  (Int16)
                                  0x04 = Long     (Int32)
                                  0x06 = Single
                                  0x07 = Double
                                  0x08 = DateTime
                                  0x01 = Boolean
                                  ...
4       2       nameIndex       uint16 — index into the name-pool chunk
6       2       valueLen        uint16 — bytes of value payload
8       ...     value           dataType-encoded value
```

For **`DefaultValue`** the entry is conventionally `dataType = 0x0A` (Text) carrying a Jet expression string (`"0"`, `"\"hi\""`, `"#2026-04-24#"`, `"True"`, `"=Now()"`).
For **`ValidationRule`** and **`ValidationText`**, `dataType = 0x0A` (Text).
For **`Description`**, `dataType = 0x0A` (Text).

> **Spec gap.** mdbtools `props.c` is the only authoritative reference for the blockType byte and ddlFlag semantics. Phase 1 must include reading several Access-authored `.accdb` files with hex dumps to ratify the encoding before writing. Capture findings in `docs/design/persisted-column-properties-format-notes.md` (created in Phase 1).

### 3.3 CLR-to-Jet-expression conversion

Conversion from a CLR literal to the string a Jet expression engine would parse:

| CLR | Jet expression |
|---|---|
| `null` / `DBNull.Value` | property entry omitted |
| `string s` | `"\"" + s.Replace("\"", "\"\"") + "\""` |
| `bool b` | `"True"` / `"False"` |
| `byte`, `short`, `int`, `long` | `Convert.ToString(v, CultureInfo.InvariantCulture)` |
| `float`, `double`, `decimal` | invariant-culture `R`-format |
| `DateTime d` | `"#" + d.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) + "#"` |
| `Guid g` | `"{guid " + g.ToString("D") + "}"` |
| `byte[]` | not supported — throw `NotSupportedException` |

`DefaultValueExpression` (string) bypasses the converter and is written verbatim — escape hatch for `=Now()`, `=Date()`, `Environ("USERNAME")`, etc.

## 4. API changes

### 4.1 `ColumnDefinition` (additive)

```csharp
public sealed record ColumnDefinition
{
    // ... existing members unchanged ...

    /// <summary>
    /// Persisted Jet expression string used as the column default at the database engine
    /// level. Takes precedence over <see cref="DefaultValue"/> when both are set.
    /// Persisted in MSysObjects.LvProp.
    /// </summary>
    public string? DefaultValueExpression { get; init; }

    /// <summary>
    /// Persisted Jet expression evaluated by the database engine on insert/update.
    /// Example: ">=0 And <=100". Persisted in MSysObjects.LvProp.
    /// </summary>
    public string? ValidationRuleExpression { get; init; }

    /// <summary>
    /// User-facing message shown by Access when <see cref="ValidationRuleExpression"/>
    /// fails. Persisted in MSysObjects.LvProp.
    /// </summary>
    public string? ValidationText { get; init; }

    /// <summary>
    /// Free-text column description. Persisted in MSysObjects.LvProp.
    /// </summary>
    public string? Description { get; init; }
}
```

Existing `object? DefaultValue` and `Func<object?, bool>? ValidationRule` are retained as client-side-only conveniences. When *both* a CLR `DefaultValue` and `DefaultValueExpression` are set, `DefaultValueExpression` wins for persistence and the CLR value continues to drive the client-side `DBNull` substitution. (Document this precedence explicitly in XML docs.)

### 4.2 `ColumnMetadata` (additive read surface)

```csharp
public string? DefaultValueExpression { get; init; }
public string? ValidationRuleExpression { get; init; }
public string? ValidationText { get; init; }
public string? Description { get; init; }
```

### 4.3 `IAccessWriter` — no new methods

`CreateTableAsync`, `AddColumnAsync`, and `RenameColumnAsync` accept `ColumnDefinition` already. `DropColumnAsync` takes a column name. No surface changes needed beyond record fields.

## 5. Implementation phases (proposed PR breakdown)

Each phase is intended as a self-contained PR. Phases 1–3 are prerequisites for any user-facing work; phases 4–6 are the user-visible deliverables.

### Phase 0 — Format reconnaissance (no code)
- Author 3-column / 5-row `.accdb` and `.mdb` files in Microsoft Access with `DefaultValue`, `ValidationRule`, `ValidationText`, and `Description` set.
- Hex-dump `MSysObjects.LvProp` for the user-table row.
- Verify chunk-type bytes, length-prefix endianness, name-pool encoding, dataType byte, ddlFlag byte against this design doc.
- Capture findings in `docs/design/persisted-column-properties-format-notes.md`.
- Output: design doc updated with verified byte layouts; no code change.

### Phase 1 — Read path: `MR2`/`KKD` parser + `ColumnMetadata` surface
- New `JetDatabaseWriter/Internal/ColumnPropertyBlock.cs`: parser for the blob layout in §3.2.
- New `JetDatabaseWriter/Internal/ColumnPropertyChunkType.cs` enum.
- `AccessReader` reads `MSysObjects.LvProp` per user table when `GetColumnMetadataAsync` is called; populates new `ColumnMetadata` fields.
- Tests: load a Phase-0 Access-authored `.accdb`, assert `ColumnMetadata.DefaultValueExpression == "0"` etc.
- **Forward-compat guarantee:** parser must return `null` (not throw) for unknown chunk types or magic, and must preserve unknown chunks as opaque `byte[]` for later round-tripping.

### Phase 2 — `LvProp` round-trip preservation through schema mutations
*Prerequisite: Phase 1 read path.*
- `AddColumnAsync`, `DropColumnAsync`, `RenameColumnAsync`, `DropTableAsync` (already TDEF-rebuilding) must read the original `LvProp` blob and re-emit it with the column block re-targeted to the new name (rename) / removed (drop) / preserved (add).
- This phase ships a *no-op* writer codec (round-trip only) — does not yet *create* property entries.
- Tests: open a Phase-0 Access-authored `.accdb`, rename a column, close, reopen in Access (manual), confirm properties still bind to the renamed column. Automated test: same flow asserting via this library's reader.

### Phase 3 — Schema widening: bootstrap `MSysObjects` with `LvProp` + `LvExtra`
*Prerequisite: Phase 1 read path is name-driven and tolerant of missing columns (verify, don't assume).*
- Extend `BuildMSysObjectsTDef` (`AccessWriter.cs:1516`) to emit the full Access-shape `MSysObjects` schema. **This changes the on-disk layout of every newly created database.** Gate behind a `AccessWriterOptions.WriteFullCatalogSchema` boolean defaulting to `true` for new files; false leaves the slim 9-column form for diff-minimisation in tests/benchmarks that hash whole-file output.
- Verify the following round-trip scenarios in Microsoft Access:
  - Library creates DB → Access opens it → Access creates a table → library reopens it.
  - Library creates DB → Access opens it → Access edits an existing library-created table → library reopens it.
- Tests: existing test database round-trips remain green; a new test asserts that a freshly-created library DB shows ≥15 columns in `MSysObjects` when opened by the library's own reader.

### Phase 4 — Write path: `MR2` codec + `CreateTableAsync` integration
*Prerequisite: Phases 1–3.*
- New `JetDatabaseWriter/Internal/ColumnPropertyBlockWriter.cs`: serializer mirroring Phase 1's parser.
- `CreateTableAsync` builds the property blob from `ColumnDefinition.{DefaultValueExpression, ValidationRuleExpression, ValidationText, Description}`, writes it to the new table's `MSysObjects.LvProp` row.
- CLR-to-Jet-expression converter (§3.3) — internal static helper.
- Tests: round-trip every supported CLR-default type through the library; manually verify in Access that defaults appear in Design View.

### Phase 5 — `Jet3` (`KKD`) parity
*Prerequisite: Phase 4.*
- Add Jet3 string encoding (length-prefix `uint8`, payload in database codepage) to both parser and writer.
- Tests: same round-trip suite against an `.mdb` (Jet3) file.
- Note: Access 97 is the lowest-priority target; if Phase-0 reconnaissance shows Access 97's encoding diverges materially, this phase may slip out of scope.

### Phase 6 — README + Limitations updates
*Prerequisite: Phase 5 (or Phase 4 if 5 slips).*
- Flip the two `❌` rows in the column-constraints table to `✅` with a footnote about Jet expression strings vs CLR delegates.
- Add the new four `ColumnDefinition` fields to the constraints table.
- Remove from `Limitations` any text contradicting the shipped feature.
- Add a `ColumnMetadata` example showing `DefaultValueExpression` etc.

## 6. Testing strategy

Per phase, in addition to the phase-specific tests above:

1. **Unit**: codec round-trip for every property type and every dataType byte.
2. **Integration**: full DB create → write properties → reopen → assert via `ColumnMetadata`.
3. **Compatibility (manual, gated by checkbox in PR)**: open library-written DB in Microsoft Access; open Access-written DB in library; mutate; close; reopen in the other tool. Capture screenshots of Design View showing the property values.
4. **Forward-compat**: take an Access-authored DB that uses `Format` / `DecimalPlaces` (chunks this library doesn't emit), open with library, mutate an unrelated column, save, reopen in Access — `Format` / `DecimalPlaces` must still be intact.
5. **Negative**: `byte[]` default → `NotSupportedException`; malformed `LvProp` (Phase 1) → `null` metadata, no throw; chunk-length overrun → `InvalidDataException`.

## 7. Risks & mitigations

| Risk | Likelihood | Mitigation |
|---|---|---|
| Phase 0 reveals the format differs from mdbtools `props.c`. | Medium | Treat §3.2 as a hypothesis; phase 0 has explicit budget to update this doc before any code lands. |
| Phase 3 schema widening breaks an existing test that hash-compares whole-file output. | High | `WriteFullCatalogSchema` opt-out flag, defaulted true for new files but false-able for legacy hashes. |
| Access rejects the library's `MR2` blob for an under-specified reason. | Medium | Phase 4 manual verification is gating; if Access rejects, fall back to "library round-trip only" with `❌` retained in README. |
| `Func<>` `ValidationRule` and string `ValidationRuleExpression` confuse users. | Low | XML doc on both makes precedence explicit; consider `[Obsolete]` on the `Func<>` form in a later major version. |
| Forward-compat: emitting `MR2` blob on a previously-empty `LvProp` triggers Access "needs to repair" prompt. | Medium | Phase 4 verification covers this; if it fires, audit `Owner` / `Flags` for missing required columns. |
| Phase 2 ships preservation but Phase 4 doesn't land — readers see properties they can't edit through this API. | Low | Acceptable — preservation is read-only-friendly. |

## 8. Open questions for the implementer

1. Should the slim 9-column `MSysObjects` schema upgrade be opportunistic (rewrite on first mutation of an old library DB) or only apply to new files? **Default proposal:** new files only; document that old library DBs need a one-time `AccessWriter.UpgradeCatalogSchemaAsync()` call to gain property support.
2. Does Access require a matching `Owner` column populated with the default user SID, or does an empty value suffice? **Verify in Phase 0.**
3. Where does table-level `Description` go — same blob, table-named block? Or is the table description in `MSysObjects.LvProp` of the *table's catalog row* vs. a column block within the table's own blob? **Verify in Phase 0** (mdbtools is ambiguous).
4. What is the maximum reasonable `LvProp` blob size before we should split it across LVAL pages vs. inline? Standard OLE LVAL handling already in `AccessWriter` should cover this — confirm by seeding a 100-column table with long descriptions in Phase 4.

## 9. Success criteria

- Round-trip: declare `new ColumnDefinition("X", typeof(int)) { DefaultValueExpression = "0", ValidationRuleExpression = ">=0", ValidationText = "Must be non-negative", Description = "X coordinate" }`, create table, close, reopen in Microsoft Access, observe all four properties in Design View, type a row violating the rule, see Access reject it with the expected message.
- Reverse: open an Access-authored DB with the same properties via this library, observe identical values via `ColumnMetadata`.
- README's column-constraints table flips both `❌` to `✅` with accurate notes.
- No regression in the existing test suite.
- Forward-compat test passes: opening a DB with `Format`/`DecimalPlaces`/etc. and mutating an unrelated column preserves the unknown property entries byte-for-byte.
