# Calculated columns — format notes & implementation gameplan

This document captures everything we know about Access 2010+ calculated columns
(also called "expression columns") and the multi-phase plan for full read/write
support in `JetDatabaseWriter`. The reference implementation throughout is
[Jackcess](https://github.com/jahlborn/jackcess) (Java, Apache 2.0). Specific
files we translate from:

- [`CalculatedColumnUtil.java`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/CalculatedColumnUtil.java) — the wrapper format and read/write helpers.
- [`ColumnImpl.java`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/ColumnImpl.java) — descriptor parsing, column-flag plumbing, fixed-vs-variable handling for calc columns, and `getCalculationContext()` integration with the expression evaluator.
- [`JetFormat.java`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/JetFormat.java) — `CALC_FIXED_FIELD_LEN`, `CALCULATED_EXT_FLAG_MASK`, etc.
- The whole `com.healthmarketscience.jackcess.impl.expr` package — the
  expression lexer/parser/evaluator (Phase 2/3).

## On-disk format

Calculated columns are an ACCDB-only (ACE) feature. The Jet3 MDB
descriptor has no slot for the extra-flags byte, so calc columns cannot exist in
those files. The writer rejects calculated columns for Jet3/Jet4 `.mdb` output.

### 1. Extra-flags byte (column descriptor)

Each ACE column descriptor is 25 bytes. The byte at **offset 16** is the
"extra flags" byte. A column is calculated when the **high two bits** are set:

| Constant | Value | Source |
| --- | --- | --- |
| `CALCULATED_EXT_FLAG_MASK` | `0xC0` | `JetFormat.CALCULATED_EXT_FLAG_MASK` |

Mirrored in this codebase as `Constants.CalculatedColumn.ExtFlagMask`.
`AccessBase.LoadColumnInfos` reads it into `ColumnInfo.ExtraFlags` and exposes
`ColumnInfo.IsCalculated`.

### 2. Persisted expression & result type (LvProp)

Two `MSysObjects.LvProp` entries on the column carry the expression and the
declared result type:

| Property name | Type | Meaning |
| --- | --- | --- |
| `Expression` | `Memo`/`Text` | The Access/VBA expression text (e.g. `[FirstName] & " " & [LastName]`). |
| `ResultType` | `Byte` | The JET data-type code the expression evaluates to. |

Names are pinned in `Constants.ColumnPropertyNames.Expression` /
`.ResultType`. `AccessReader.GetColumnMetadataAsync` populates
`ColumnMetadata.CalculationExpression` and `.CalculatedResultType` from them.

### 3. Stored value wrapper (data pages)

Even though the value is calculated by the engine, Access also **persists the
last evaluated result** on the row, prefixed by a 23-byte header:

| Constant | Value | Meaning |
| --- | --- | --- |
| `CALC_EXTRA_DATA_LEN` | `23` | Header length prepended to every stored value. |
| `CALC_DATA_LEN_OFFSET` | `16` | Offset within the header where the payload length (Int32 LE) lives. |
| `CALC_DATA_OFFSET` | `20` | Offset within the header where the payload begins. |
| `CALC_FIXED_FIELD_LEN` | `39` | The fixed-portion column length used for *all* fixed-width calc columns (largest fixed payload `16` + the `23`-byte header). |

For variable-width source types the on-disk `col_len` becomes
`originalLen + CALC_EXTRA_DATA_LEN`. For fixed-width source types it is forced
to `CALC_FIXED_FIELD_LEN` regardless of the underlying type. Long-value result
types (`MEMO` / `OLE`) keep the normal LVAL row header in the row; the bytes
inside the LVAL payload are wrapped.

Two result types have Access-specific payload encodings inside the wrapper:

- `T_BOOL`: one byte, `0xFF` for true and `0x00` for false. Calculated booleans
  are not stored in the row null mask.
- `T_NUMERIC`: 16 bytes: a 2-byte payload-length prefix, scale byte, sign byte
  (`0x80` for negative), then the 96-bit decimal mantissa in Access's calculated
  numeric byte order. This is not the normal 17-byte `T_NUMERIC` fixed slot.

Helpers in this codebase: `CalculatedColumnUtil.Wrap` / `.Unwrap` (round-trip
verified by `CalculatedColumnUtilTests`).

## Phased implementation plan

### Phase 1A — Read-side metadata + foundation **(DONE)**

Goal: surface calc-column metadata to clients and recognise the format on disk.

Delivered:

- `Constants.CalculatedColumn` constants (mask, header layout, fixed length).
- `Constants.ColumnPropertyNames.Expression` / `.ResultType`.
- `CalculatedColumnUtil.Wrap` / `.Unwrap` (round-trip + truncation tests).
- `ColumnInfo.ExtraFlags` + `ColumnInfo.IsCalculated`.
- `AccessBase.LoadColumnInfos` reads byte at descriptor offset 16 (ACE only;
  Jet3 hard-coded to `0`).
- `ColumnDefinition` / `ColumnMetadata` `IsCalculated`, `CalculationExpression`,
  `CalculatedResultType` properties.
- `AccessReader.GetColumnMetadataAsync` extracts `Expression` / `ResultType`
  from LvProp.
- Tests: `JetDatabaseWriter.Tests/Schema/CalculatedColumnUtilTests.cs`,
  expanded metadata fixture coverage.

### Phase 1B — Write & round-trip the persisted value **(DONE)**

Goal: be able to create a calc column, store an evaluated value, and have both
ourselves and Access read it back correctly. Still **no client-side
evaluation**: the caller supplies the literal value to persist, plus the
expression text; Access will recompute on next open.

Jackcess sources to translate:

- `CalculatedColumnUtil.create*Handler` factory methods — they wrap an existing
  `ColumnImpl` to override `read` / `write` / `getType` / `isVariableLength`.
- The `ColumnImpl` constructor branch that detects `extraFlags & 0xC0`,
  rewrites `_columnLength`, and forces the column into the variable-length
  bucket so it can store the wrapper.
- `ColumnImpl.writeRealCodecHandler` calls into the wrapper helpers.

Delivered:

- `Schema/TDefPageBuilder` emits the `0xC0` extra-flags byte, adjusts `col_len`,
  and treats every calculated column as variable-area storage.
- `Schema/JetExpressionConverter.ApplyColumn` emits `Expression` as Memo and
  `ResultType` as Byte in `MSysObjects.LvProp`.
- `AccessWriter.CreateTableAsync` accepts calculated columns for ACCDB, validates
  expression/result-type constraints, and rejects unsupported Jet3/Jet4 MDB
  targets.
- `ValueEncoding/RowEncoder` wraps cached values by result type, including
  calculated booleans, calculated numeric payloads, and calculated MEMO values
  whose wrapped payload spills to LVAL pages.
- `AccessReader` unwraps calculated cached values on the string, typed
  `DataTable`, and POCO paths; the compiled direct POCO decoder falls back to
  the unwrap-aware path for any bound calculated column.
- Tests: `JetDatabaseWriter.Tests/Writer/CalculatedColumnWriteTests.cs` plus
  updated Access-authored fixture coverage in
  `JetDatabaseWriter.Tests/Schema/CalculatedColumnFixtureTests.cs`.

### Phase 2 — Subset expression evaluator

Goal: on `INSERT` / `UPDATE`, recompute the value ourselves so callers do not
have to supply it, and so updates to dependent columns refresh the persisted
value the same way Access does.

Translate the most common subset of Jackcess `expr`:

- Lexer + Pratt-style parser for VBA expression syntax.
- Operators: arithmetic (`+ - * / \ ^ Mod`), string concat (`& +`), comparison,
  `And Or Not Xor Eqv Imp`, `Is Null`, `Like` (with `?*#[]` patterns),
  `Between..And`, `In(...)`.
- Built-ins: `IIf`, `Nz`, `IsNull`, `IsNumeric`, `IsDate`, `Len`, `Left`,
  `Right`, `Mid`, `InStr`, `InStrRev`, `Replace`, `UCase`, `LCase`, `Trim`,
  `LTrim`, `RTrim`, `Space`, `String`, `StrConv`, `Format` (numeric +
  date subset), `Year`, `Month`, `Day`, `Hour`, `Minute`, `Second`,
  `Weekday`, `DateAdd`, `DateDiff`, `DatePart`, `Now`, `Date`, `Time`,
  `CInt`, `CLng`, `CDbl`, `CSng`, `CCur`, `CDec`, `CStr`, `CDate`, `CBool`,
  `CByte`, `Abs`, `Sgn`, `Int`, `Fix`, `Round`, `Sqr`.
- Column reference resolution against the in-flight row (`[ColName]`,
  `ColName`, `[Table].[Col]` — only the row-local form matters because calc
  columns cannot reference other tables).

#### Formula parsing library recommendation

For parsing and validating calculated column expressions (which use Access/Excel-style formula syntax), we recommend using [ClosedXML.Parser](https://github.com/ClosedXML/ClosedXML.Parser). This .NET library is actively maintained, supports modern Excel formula syntax, and is suitable for parsing and validating expressions before storing them in the Access/ACE on-disk format. Note: You must still implement all Access/ACE-specific binary writing logic yourself; ClosedXML.Parser only handles formula parsing, not database serialization.

Tests: golden expressions evaluated against an Access oracle.

### Phase 3 — Full VBA expression library + cross-table lookups

- The remaining VBA functions (`DLookup`, `DCount`, `DSum`, `DAvg`, `DMin`,
  `DMax`, `Switch`, `Choose`, `Partition`, full `Format` grammar, financial
  functions, etc.).
- Cross-record / cross-table evaluation context (only relevant if Microsoft
  ever extends calc columns beyond the row-local restriction; today this is
  effectively dead code but worth noting because Jackcess models it).

## Why phased

Each phase is independently shippable and independently testable against a
real Microsoft Access oracle:

1. **1A** lets clients *detect* calc columns and decide whether to error.
2. **1B** unblocks anyone who computes the value themselves (e.g. ETL tools).
3. **2** covers the >95% of real-world Access expressions.
4. **3** is for parity with the long tail.

This avoids a multi-week mega-PR and keeps the scope of each Jackcess
translation bounded.
