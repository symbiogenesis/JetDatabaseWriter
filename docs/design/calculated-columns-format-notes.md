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

Calculated columns are an ACCDB-only (Jet 4 / ACE) feature. The Jet3 MDB
descriptor has no slot for the extra-flags byte, so calc columns cannot exist in
those files.

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
to `CALC_FIXED_FIELD_LEN` regardless of the underlying type.

Helpers in this codebase: `CalculatedColumnUtil.Wrap` / `.Unwrap` (round-trip
verified by `CalculatedColumnUtilTests`).

## Phased implementation plan

### Phase 1A — Read-side metadata + foundation **(DONE)**

Goal: surface calc-column metadata to clients, recognise the format on disk,
and reject unsupported writes with a clear error so callers can detect partial
support today.

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
- `AccessWriter.CreateTableAsync` throws `NotSupportedException` when any
  `ColumnDefinition.IsCalculated == true` (message points at this doc and
  mentions Phase 1B).
- Tests: `JetDatabaseWriter.Tests/Internal/CalculatedColumnUtilTests.cs`,
  expanded `LimitationsTests`.

Phase 1A does **not** unwrap the persisted value when reading rows; the
underlying typed-value parser still sees the original payload and would
mis-decode a stored calc value. Real read-side row support lands with Phase 1B
because we need the descriptor-length adjustments and `RowMapper` plumbing in
the same change set.

### Phase 1B — Write & round-trip the persisted value

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

Required code changes here:

- `Internal/Builders/ColumnDescriptorBuilder` (or wherever the 25-byte
  descriptor is emitted) — write the `0xC0` extra-flags byte and adjust
  `col_len` (`CALC_FIXED_FIELD_LEN` for fixed, `original + 23` for variable).
- `Internal/Builders/TableDefinitionBuilder` (`BuildTableDefinition`,
  `BuildTDefPageWithIndexOffsets`) — treat all calc columns as variable-length
  for slot allocation; ensure variable-column count, NULL bitmap, and offset
  table line up.
- `Internal/JetExpressionConverter.ApplyColumn` — when `IsCalculated`, emit
  the `Expression` (Memo) and `ResultType` (Byte) LvProp entries on the
  column.
- `Internal/Helpers/FixedSize` (or the equivalent fixed-payload sizer) — return
  `CALC_FIXED_FIELD_LEN` for any calc column regardless of its `JetType`.
- `Internal/TypedValueParser.TryEncodeFixedValue` /
  `EncodeVariableValue` — wrap the encoded bytes through
  `CalculatedColumnUtil.Wrap` before they reach the row payload.
- Reader: `RowMapper.ReadFixed` / `ReadVarAsync` (or whichever methods materialise
  per-column bytes) — call `CalculatedColumnUtil.Unwrap` for calc columns
  before handing the bytes to `TypedValueParser`.
- `AccessWriter.CreateTableAsync` — drop the Phase 1B guard once the above is
  in place.

Tests: round-trip create/insert/select using Access-generated oracle ACCDB
files; cross-check against `JetDatabaseWriter.FormatProbe` dumps.

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
