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
those files. The writer rejects calculated columns for Jet3/Jet4 `.mdb` output
with `NotSupportedException` ("calculated columns are only supported in ACCDB
databases"). `CreateTableAsync` and `AddColumnAsync` make that check before they
parse the expression, so an `.mdb` reports it even for an expression the parser
would reject, and `AddColumnAsync` makes it before it reads the table.

### 1. Extra-flags byte (column descriptor)

Each ACE column descriptor is 25 bytes. The byte at **offset 16** is the
"extra flags" byte. A column is calculated when the **high two bits** are set:

| Constant | Value | Source |
| --- | --- | --- |
| `CALCULATED_EXT_FLAG_MASK` | `0xC0` | `JetFormat.CALCULATED_EXT_FLAG_MASK` |

Mirrored in this codebase as `Constants.CalculatedColumn.ExtFlagMask`.
`DatabaseFile.ReadTableDefAsync` reads it into `ColumnInfo.ExtraFlags` and exposes
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

#### Descriptor `col_type` vs `ResultType`

The descriptor `col_type` controls where the wrapped value is placed in the row
(the column is always in the variable area), but the `ResultType` LvProp
controls how the value is encoded, including whether the row slot holds an
inline wrapper or a long-value header. Access-authored columns can have a
descriptor type that differs from their result type. In
`calcFieldTestV2010.accdb` `Table1`:

| Column | Descriptor `col_type` | `ResultType` |
| --- | --- | --- |
| `AllNames` | Text (`col_len` 0) | Memo |
| `MonthlySalary` | Double | Currency |
| `WeeklySalary` | Double | Decimal |
| `IsRich`, `BoolTest` | Integer | Boolean |
| `FloatTest` | Decimal | Single |

`AllNames`' row slots are 12-byte LVAL headers, as for any Memo: three rows
point at single-page LVAL rows (byte 3 = `0x40`) and the short `John Doe` value
is inline (`0x80`). The LVAL payload is the 23-byte wrapper around the
uncompressed UCS-2 text. Jackcess does the same: `ColumnImpl.create` replaces
the column type with `ResultType` for calculated columns.

So the reader (`RowDecodePlan`) and the writer (`RowEncoder`,
`LongValueEncoder`) both pick the payload codec with
`JetTypeInfo.ResolveValueType`, which is the hydrated `ResultType` for a
calculated column. The writer's `TableCatalog` hydrates `ResultType` through
`ColumnPropertyReader` when it resolves a table, once per table until the next
`Invalidate`. A schema rewrite (`AddColumnAsync`, `DropColumnAsync`,
`RenameColumnAsync`) projects each calculated column from its result type, so
the rebuilt descriptor carries the result type, as the writer's own tables do.
Rows that earlier builds of this library inserted into or updated in such
tables were encoded by the descriptor type and are not repaired.

`RenameColumnAsync` also rewrites the renamed column's references in every
`Expression`, `ValidationRule` and `DefaultValue` of the table, before anything
is written. `ExpressionFieldReferences` scans the stored text without parsing
it, so an expression the engine cannot parse (a stored `%`) is renamed too. It
skips string, `#date#` and `{guid ...}` literals, numbers and radix literals,
and rewrites `[Old]`, a bare `Old` (not a keyword, a `vb` constant, a function
call or a `$` name) and `[Table].[Old]` / `Table.Old` qualified by the table's
own name as `[New]`. Other qualified names (`[Other].[Old]`, `Forms![F]![Old]`)
are left alone. Every Access-authored expression in the fixtures uses
`[Field]` brackets. A rename whose new name contains `]` throws
`ArgumentException` when an expression names the column, since a bracketed
name cannot hold `]`. `DropColumnAsync` uses the same scan to refuse, with
`InvalidOperationException` and before anything is written, to drop a column
that another column's `Expression`, `ValidationRule` or `DefaultValue` names; a
mention in a string literal, or in the dropped column's own rule, does not
block it. What Access itself does when a field that a calculated column uses
is renamed or deleted is not verified.

Two result types have Access-specific payload encodings inside the wrapper:

- `Boolean`: one byte, `0xFF` for true and `0x00` for false. Calculated booleans
  are not stored in the row null mask.
- `Numeric`: 16 bytes: a 2-byte payload-length prefix, scale byte, sign byte
  (`0x80` for negative), then the 96-bit decimal mantissa in Access's calculated
  numeric byte order. This is not the normal 17-byte `Numeric` fixed slot.

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
- `DatabaseFile.ReadTableDefAsync` reads byte at descriptor offset 16 (ACE only;
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
- Tests: `JetDatabaseWriter.Tests/Writer/CalculatedColumnWriteTests.cs`,
  updated Access-authored fixture coverage in
  `JetDatabaseWriter.Tests/Schema/CalculatedColumnFixtureTests.cs`, and
  byte-level cached-payload assertions in
  `JetDatabaseWriter.Tests/Schema/CalculatedColumnPayloadTests.cs` (including
  DAO-authored `IIf` / `Switch` calculated fields on Access-equipped hosts).

### Phase 2 — Subset expression evaluator **(DONE)**

Goal: on `INSERT` / `UPDATE`, recompute the value ourselves so callers do not
have to supply it, and so updates to dependent columns refresh the persisted
value the same way Access does.

Delivered:

- Added [ClosedXML.Parser](https://github.com/ClosedXML/ClosedXML.Parser) for
  formula parsing and an internal AST factory that maps parser nodes into the
  row-local calculated-column evaluator. ClosedXML.Parser handles expression
  parsing only; all Access/ACE storage and type coercion remains in this
  library.
- `ConstraintRegistry` evaluates calculated columns during inserts when the row
  omits the cached value or supplies `NULL`/`DBNull`, and recomputes calculated
  columns during updates after source values are applied.
- Caller-supplied cached values still work on insert. This preserves Phase 1B
  behavior and lets unsupported expressions be persisted when the caller has
  already computed the value.
- Expressions are normalized for common Access syntax: leading `=` is ignored,
  bracketed column references such as `[Column Name]` resolve against the
  in-flight row, `#date literal#` becomes `DATEVALUE("date literal")`,
  single-quoted text (`'it''s'`, where `''` is a quote) becomes the
  double-quoted literal, `&H`/`&O` radix literals become decimal numbers, and
  Access word operators are lowered into evaluator functions before the
  ClosedXML.Parser pass. The expression text persisted in the file is never
  rewritten.
- Radix literals follow VBA: `&H` takes hex digits, `&O` (or a bare `&`
  before an octal digit, which OLE Automation also accepts) octal digits, up
  to 32 bits. A value up to `&HFFFF` is a signed Integer, so `&HFFFF` is -1
  and `&H8000` is -32768; a larger value is a signed Long (`&HFFFFFFFF` is
  -1); a trailing `&` makes it a Long (`&HFFFF&` is 65535). `&` starts a
  literal only where an operand is expected; after an operand it joins text,
  so `[A]&10` is concatenation. A literal wider than 32 bits, or `&H`/`&O`
  without digits, throws `ArgumentException` naming the expression.
- Every expression is parsed with Access (VBA) operator precedence by
  `CalculatedExpressionNormalizer` before ClosedXML.Parser sees it; the
  normalizer emits a formula parenthesized wherever Excel's grammar would
  group differently, so Excel precedence never decides the result. From
  tightest to loosest: `^` (left-associative, and tighter than unary minus,
  so `-2^2` is -4), unary `-`/`+`, `*` `/`, `\`, `Mod`, `+` `-`, `&`,
  comparisons and `Like`/`Is`/`Between`/`In`, `Not`, `And`, `Or`, `Xor`,
  `Eqv`, `Imp`. Syntax the Access grammar rejects throws `ArgumentException`
  naming the expression instead of falling back to the spreadsheet grammar.
- Excel's postfix `%` is not an Access operator. `CreateTableAsync` and
  `AddColumnAsync` reject a calculated column whose expression uses it
  (outside string literals and `[field]` names), or whose expression the
  parser cannot read, with an `ArgumentException` naming the column and the
  expression; its `ParamName` is `columns` for `CreateTableAsync` and `column`
  for `AddColumnAsync`. The checks run in this order, all before any catalog
  I/O: the table name, then whether the format can hold each column (ACCDB
  only, a result type and no AutoNumber, Attachment, multi-value or Hyperlink
  flag; `NotSupportedException`), then the expression syntax, then that the
  column declares no `DefaultValue` or `DefaultValueExpression`, which a
  calculated column cannot have (`ArgumentException`). The "already
  exists" check comes last. Columns carried over by a table rewrite are not
  re-checked, so schema edits such as adding a plain column still work on a
  table that already stores such an expression. Its rows still read, and
  an insert that supplies the calculated value keeps it, but an insert that
  leaves the column Null and every `UpdateRowsAsync` (which re-evaluates all
  calculated columns) throw the same `ArgumentException`, much as a stored
  expression that calls an unsupported function makes them throw
  `NotSupportedException`.
- Each referenced field's value is converted to that field's declared type
  before evaluation, with the same `Convert` call the row encoder stores it
  with. The writer accepts `"10"` for an Integer field and `10` for a Text
  field, so without this the text rules below would follow the CLR type the
  caller passed instead of the field type.
- Values follow Access rather than Excel: `True` is -1 and `False` is 0 in
  arithmetic, comparisons, numeric result columns and the `CInt`, `CLng`,
  `CDbl`, `CSng`, `CCur` and `CDec` conversions, and `"-1"` / `"0"` as text
  (`&`, `CStr` and Text result columns); `+` concatenates when both operands
  are text; and two text operands compare as text even when they look
  numeric (`"10" < "9"`).
- A date becomes text (`&`, `CStr`, `Like`, `Len`, `Format` with no format or
  `"General Date"`, `FormatDateTime(d, vbGeneralDate)`, a Text result column
  or a Text column's default) in VBA's General Date form for en-US, as OLE
  Automation's `VarBstrFromDate` gives it (measured with oleaut32 on Windows,
  LCID 1033), whatever the current culture: `1/31/2020 6:00:00 AM`, with no
  zero padding and a 12-hour clock. A date at midnight has no time part
  (`1/31/2020`), and a time on day 0 (1899-12-30) has no date part
  (`6:00:00 AM`; day 0 at midnight is `12:00:00 AM`). More than half a second
  rounds up to the next second. Access itself formats with the Windows
  locale, so a non-US Access installation writes other text; this library
  always uses the en-US form. The other named formats (`Short Date`,
  `Long Time` and so on) still use .NET's invariant patterns.
- Conversions follow OLE Automation's `VariantChangeType`, which VBA uses
  (measured with oleaut32 on Windows; Access itself was not checked): a
  Boolean is 255 or 0 in a Byte result column and in `CByte`, and a number or
  Boolean is a date serial (days since 1899-12-30, so `True` is 1899-12-29) in
  a Date result column, `CDate` and the date functions. `CByte(-1)` and
  `CByte(256)` overflow. `IsDate` is True only for a date or text that parses
  as one, so `IsDate(5)` is False as in VBA.
- Arithmetic follows the OLE Automation Variant rules (`VarAdd`, `VarSub`,
  `VarIdiv`, `VarMod`; `AccessVariantOperators`). A date plus or minus a
  number, a Boolean, numeric text or another date is a date (`#2020-01-31# + 1`
  is 2020-02-01, `[D] + 0.5` adds twelve hours, `-[D]` is a date); a date minus
  a date is a `Double` number of days; `*`, `/` and `^` with a date give a
  `Double`. A date plus or minus a Decimal is also a date, because the engine
  holds Currency and Decimal fields alike as `decimal` and VBA gives a date for
  Currency (OLE Automation gives Decimal for a true Decimal). A result outside
  years 100-9999 throws `OverflowException`, and non-numeric text throws
  `InvalidCastException` ("Type mismatch"). A date compared with a number or
  date text compares as dates. `\` and `Mod` round both operands half to even
  to a Long before dividing (`7.6 \ 2` is 4, `7 Mod 2.5` is 1), throw
  `DivideByZeroException` for a zero divisor after rounding, and return a Long.
  These rules apply to persisted `DefaultValue` and `ValidationRule`
  expressions too, so `=Date()+7` and `>=Date()-30` are applied instead of
  being skipped as unsupported.
- `And`, `Or`, `Not`, `Xor`, `Eqv` and `Imp` follow VBA (OLE Automation
  `VarAnd` and friends; `AccessVariantOperators`): logical on two Booleans and
  bitwise on numbers (`12 And 10` is 8, `Not 5` is -6, `True And 12` is 12).
  Two Bytes give a Byte, any mix of Boolean, Byte and Integer an Integer, and
  other numbers, dates and numeric text are rounded half to even to a Long
  (`2.5 And 3` is 2); a value beyond the Long range throws
  `OverflowException` and non-numeric text throws `InvalidCastException`.
  Null is three-valued: `Null And False` is False, `Null And 0` is 0,
  `Null Or True` is True, `Null Or 12` is 12, and every other combination
  with Null, including `Not Null`, is Null; `Eqv` is `Not (a Xor b)` and
  `Imp` is `(Not a) Or b`, so `False Imp Null` is True. A calculated Boolean
  or numeric column can therefore store Null. Jackcess keeps these operators
  logical; this follows OLE Automation, measured on Windows, and Access itself
  was not checked. `ValidationRule` terms are still joined by the rule's own
  `And`/`Or` logic, which only combines Boolean terms.
- An evaluation failure keeps its exception type (`OverflowException`,
  `InvalidCastException`, `DivideByZeroException` and so on) but its message
  names the table, the calculated column and its expression, plus the value
  and result type when the result could not be stored (`[A] * 100` in a Byte
  column with `A = 5` "evaluated to 500, which a Byte result cannot hold").
  The original exception is the `InnerException`. A column whose expression
  references another calculated column reports the inner column's failure
  once.
- Calculated columns may reference earlier or later calculated columns in the
  same row; dependency evaluation is lazy and circular references are rejected.

Supported subset:

- Operators: arithmetic (`+`, `-`, `*`, `/`, `\`, `^`, `Mod`, including date arithmetic), string
  concatenation (`&`), comparisons (`=`, `<>`, `>`, `>=`, `<`, `<=`), logical
  and bitwise word operators (`Not`, `And`, `Or`, `Xor`, `Eqv`, `Imp`), and Access special
  comparisons (`Is [Not] Null`, `[Not] Like`, `[Not] Between`, `[Not] In`).
- Literals: numbers, double- and single-quoted text, `#date#`, and `&H`/`&O`
  radix literals.
- Constants and nulls: `True`/`False`, `Yes`/`No`, `On`/`Off`, common `vb*`
  constants, blank/null nodes, and `DBNull` values from the in-flight row.
- Built-ins: `IIf`/`IF`, `Nz`, `IsNull`/`IsBlank`, `IsNumeric`/`IsNumber`,
  `IsDate`, `Len`, `Left`, `Right`, `Mid`, `UCase`/`Upper`, `LCase`/`Lower`,
  `Trim`, `LTrim`, `RTrim`, `Replace`, `InStr`, `InStrRev`, `Space`,
  `StrComp`, `StrConv`, `String`, `StrReverse`, `Asc`/`AscW`, `Chr`/`ChrW`,
  `Str`, string-returning `$` aliases such as `Left$`/`UCase$`, `Format*`
  helpers, `Abs`, `Round`, `Int`, `Fix`, `Rnd`, `Sgn`, `Sqr`, `Sin`, `Cos`,
  `Tan`, `Atn`/`Atan`, `Exp`, `Log`, `Date`/`Today`, `Now`, `Time`, `DateValue`,
  `DateSerial`, `DateAdd`, `DateDiff`, `DatePart`, `Year`, `Month`, `Day`,
  `Hour`, `Minute`, `Second`, `TimeValue`, `TimeSerial`, `Timer`, `MonthName`,
  `Weekday`, `WeekdayName`, `CInt`, `CLng`, `CDbl`, `CSng`, `CCur`/`CDec`,
  `CStr`, `CDate`/`CVDate`, `CBool`, `CByte`, `CVar`, `VarType`, `TypeName`,
  `Hex`, `Oct`, `Val`, common financial helpers (`FV`, `PV`, `Pmt`, `NPer`,
  `IPmt`, `PPmt`, `DDB`, `SLN`, `SYD`, `Rate`), `Choose`, and `Switch`.

Still intentionally out of scope: domain aggregate functions (`DLookup`,
`DCount`, `DSum`, `DAvg`, `DMin`, `DMax`) because DAO/Access rejects them in
table calculated columns with "cannot be used in a calculated column" even
though they are valid in other Access expression contexts; SQL/query evaluation,
cross-record or cross-table lookups; and spreadsheet-only parser constructs such
as cell, sheet, external workbook, array, range, and structured references.

Tests: focused insert/update/POCO coverage in
`JetDatabaseWriter.Tests/Writer/CalculatedColumnWriteTests.cs`, precedence and
value-semantics cases in
`JetDatabaseWriter.Tests/Schema/CalculatedExpressionAccessSemanticsTests.cs`,
plus the Phase 1B Access-authored fixture coverage, which also re-evaluates
every Access-authored fixture expression against the values Access cached.

### Phase 3 — Non-row-local expression contexts **(DONE)**

- Domain aggregate functions (`DLookup`, `DCount`, `DSum`, `DAvg`, `DMin`,
  `DMax`) remain intentionally rejected for table calculated columns; DAO/Access
  rejects each with "cannot be used in a calculated column". SQL/query
  evaluation and cross-record / cross-table lookup context remain outside the
  row-local evaluator.
- `Partition` and the long tail of highly specialized VBA functions can be
  added if real Access-authored calculated-column fixtures show they are valid
  in this context.

## Why phased

Each phase is independently shippable and independently testable against a
real Microsoft Access oracle:

1. **1A** lets clients *detect* calc columns and decide whether to error.
2. **1B** unblocks anyone who computes the value themselves (e.g. ETL tools).
3. **2** covers the >95% of real-world Access expressions.
4. **3** is for parity with the long tail.

This avoids a multi-week mega-PR and keeps the scope of each Jackcess
translation bounded.
