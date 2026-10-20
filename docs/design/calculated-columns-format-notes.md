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

## Qualified expression references

Native DAO120 accepts `[T].[Price]`, `T.Price` and `[T]![Price]` in an ACE
calculated field. A DAO-created field using each spelling evaluates Price 4
to 8 for multiplication by 2. The same engine rejects these spellings when
assigned to table validation rules, field validation rules or defaults.

Calculated plans retain qualifier syntax separately from the column name.
An adjacent bare qualifier binds to the current row, even when its name is
another existing table: DAO120 evaluates `Other.Price` and `Other![Price]`
from `T.Price`, leaving the separate `Other` table unchanged. A bracketed
qualifier requires a matching current-table context. Plans require an explicit
row/table context; multi-segment object references are refused, and no
embedded code or cross-table lookup is executed. Defaults and validation
rules reject every qualified reference before mutation. Column rename and
drop dependency checks recognize row-local bare aliases while preserving
the original qualifier spelling. A qualified second segment is a field even
when it spells a builtin constant or keyword (such as `vbTrue`, `Null` or
`And`); unqualified builtin constants keep their usual meanings.
`QualifiedReservedFieldTests` checks dependency refusal, rename and native
recalculation/compact for these names. DAO120 rejects all nine tested whitespace
variants of `[T].[Price]`, `T.Price` and `[T]![Price]`; the evaluator rejects
them too. Each native refusal leaves the field collection unchanged, and
the adjacent spelling succeeds in the same database.

DAO120 table renames retain the original calculated-expression text. After
renaming `T` to `Renamed T`, bare qualifiers `T.Price` and `T![Price]`
continue evaluating new and updated rows after reopen. The writer follows
that row-local binding through insert, update and field rename; DAO then
updates, compacts and reads the resulting expressions and values.
`TableRenamedQualifiedExpressionTests` also uses the bare `Other` qualifier
with a real separate table, and verifies that table's data is untouched.
Bracketed qualifiers `[T].[Price]`, `[T]![Price]` and `[T].Price` instead
return Null after native table rename, and compact refuses them with DAO
error 3790. The writer still refuses these stale bracketed qualifiers before
changing bytes. Their native Null semantics remain open; allowing arbitrary
bracketed foreign qualifiers would erase the syntax distinction.

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
`TableDefReader.ReadTableDefAsync` reads it into `ColumnInfo.ExtraFlags` and exposes
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
uncompressed UCS-2 text. Long-value preparation and direct inline encoding both retain that plain payload; after envelope removal, readers do not reinterpret a literal leading U+FEFF as an ordinary Memo compression marker. Jackcess does the same: `ColumnImpl.create` replaces
the column type with `ResultType` for calculated columns.

So the reader (`RowDecodePlan`) and the writer (`RowEncoder`,
`LongValueEncoder`) both pick the payload codec with
`JetTypeInfo.ResolveValueType`, which is the hydrated `ResultType` for a
calculated column. Both readers and writers resolve a `TableSchema` through
`TableCatalog`; calculated columns load persisted properties through
`ColumnPropertyReader`, cached by structural image and catalog generation.
Each caller-owned layout projects `ResultType` without changing the cached
image. Catalog invalidation or a changed structural image forces resolution
again. A schema rewrite (`AddColumnAsync`, `DropColumnAsync`,
`RenameColumnAsync`) preserves each surviving native descriptor and its result
type property; the cached payload codec continues to follow `ResultType`.
Malformed cached values are decoded under the selected fault policy; writes
do not reinterpret or repair payloads encoded with the wrong result type.

Access gives each external long-value column independent owned/free maps. In
`Table1`, `AllNames` uses rows 2/3 of map page 0x57, both listing LVAL page
0x59; row 4 belongs to the index. The writer follows each native TDEF trailer
pointer and creates independent column rows for new and rewritten tables.
Single-page values use the free map; native chains keep it empty even when
the final page has spare bytes. Row deletion releases external values and
updates their maps. Table drops and schema rewrites release reachable values
and only map/index storage with proven ownership across surviving references.
See [usage-map maintenance](usage-map-maintenance.md).

A drop deletes the table's `MSysObjects` row, which holds `ResultType`, before
it frees the pages. So `TableSchemaEditor` first reads the table definition
through `TableCatalog.ReadTableDefAsync`. With the descriptor type alone, the
free skipped `AllNames`' headers and left the writer's LVAL rows allocated.

`RenameColumnAsync` also rewrites the renamed column's references in every
`Expression`, `ValidationRule` and `DefaultValue` of the table, before anything
is written. `ExpressionFieldReferences` scans the stored text without parsing
it, so an expression the engine cannot parse (a stored `%`) is renamed too. It
skips string, `#date#` and `{guid ...}` literals, numbers and radix literals,
and rewrites `[Old]` and a bare `Old` (not a keyword, a `vb` constant, a
function call or a `$` name) as `[New]`. A reference qualified by the table's
own name or an adjacent bare alias keeps the qualifier: `[Table].[Old]` becomes
`[Table].[New]`, `Alias.Old` becomes `Alias.[New]`, and `Table.Old` becomes
`Table.[New]`. Same-table qualified references evaluate
before and after column rename. Other qualified names (`[Other].[Old]`,
`Forms![F]![Old]`) are left alone. The text scanner also groups spaced name
chains conservatively, so unsupported stored syntax such as `[Other] . [Old]`
cannot be mistaken for a local field and rewritten. Every Access-authored
expression in the original fixtures uses
`[Field]` brackets. A bracketed name cannot hold `]`, and the Access naming
rules that `RenameColumnAsync` checks the new name against exclude it, so a
rename to such a name throws `ArgumentException` before the table is read.
`DropColumnAsync` uses the same scan to refuse, with
`InvalidOperationException` and before anything is written, to drop a column
that another column's `Expression`, `ValidationRule` or `DefaultValue` names; a
mention in a string literal, or in the dropped column's own rule, does not
block it. What Access itself does when a field that a calculated column uses
is renamed or deleted is not verified: `DaoCalculatedColumnRenameTests` checks
both through DAO, and that DAO evaluates and compacts a table the writer
renamed a field of, but it runs only on a host with Microsoft Access.

Two result types have Access-specific payload encodings inside the wrapper:

- `Boolean`: one byte, `0xFF` for true and `0x00` for false. Calculated booleans
  are not stored in the row null mask.
- `Numeric`: 16 bytes: a 2-byte payload-length prefix, scale byte, sign byte
  (`0x80` for negative), then the 96-bit decimal mantissa in Access's calculated
  numeric byte order. This is not the normal 17-byte `Numeric` fixed slot.

The committed Access fixture `calcFieldTestV2010.accdb`, `Table1.WeeklySalary`,
uses a 16-byte Numeric payload with stored length 14. Its zero payload is
`0E000000000000000000000000000000`: scale/sign zero and a full twelve-byte
zero mantissa. `CalculatedNumericDecodePolicyTests` checks these native cached
values and preserves both zero and an empty-envelope null result.

A claimed, four-byte-aligned mantissa length cannot exceed the bytes available
after the four-byte Numeric header. Contradictions raise a contextual
`MalformedValue` failure on strict reads; lenient reads return their documented
missing values with a diagnostic. An unrelated-column update refuses the same
unreadable value without changing database bytes. The decoder zero-extends
accepted shorter mantissas into its decimal buffer. Four/eight-byte controls
pin existing decoder behavior only; native compact-width, nonpositive-prefix
and empty-mantissa semantics remain unverified.

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
- `TableDefReader.ReadTableDefAsync` reads byte at descriptor offset 16 (ACE only;
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

- `CalculatedExpressionParser` parses Access syntax directly into the
  row-local evaluator's expression tree. Field nodes carry their original
  names and qualifiers; bare names such as `A1` are ordinary field names.
  Parsing never generates or reparses an intermediate formula.
  Column validation rules reuse bounded ranges of the same token stream and
  implicit field nodes, with separate short-circuit, three-valued Boolean logic.
- `ConstraintRegistry` evaluates calculated columns during inserts when the row
  omits the cached value or supplies `NULL`, `DBNull` or `DbDefault.Value`, and
  recomputes calculated columns during updates after source values are applied.
- Caller-supplied cached values still work on insert. This preserves Phase 1B
  behavior and lets unsupported expressions be persisted when the caller has
  already computed the value.
- The parser accepts common Access syntax: leading `=` is ignored,
  bracketed column references such as `[Column Name]` resolve against the
  in-flight row, and `#date literal#` uses the CDate evaluator to preserve its time,
  while DateValue removes the time. DateSerial rounds each argument half to even
  to a VBA Integer; Now and Time use whole seconds, while Timer retains fractional seconds.
  Single- and double-quoted text becomes literal nodes, doubled quotes are
  unescaped, and `&H`/`&O` radix literals become typed Integer/Long values.
  Access word operators produce evaluator function nodes. The expression
  text persisted in the file is never rewritten.
- Radix literals follow VBA: `&H` takes hex digits, `&O` (or a bare `&`
  before an octal digit, which OLE Automation also accepts) octal digits, up
  to 32 bits. A value up to `&HFFFF` is a signed Integer, so `&HFFFF` is -1
  and `&H8000` is -32768; a larger value is a signed Long (`&HFFFFFFFF` is
  -1); a trailing `&` makes it a Long (`&HFFFF&` is 65535). `&` starts a
  literal only where an operand is expected; after an operand it joins text,
  so `[A]&10` is concatenation. A literal wider than 32 bits, or `&H`/`&O`
  without digits, throws `ArgumentException` naming the expression.
  The typed literal nodes preserve `TypeName`/`VarType` and bitwise operand
  subtypes. Unary plus keeps Integer/Long; negating the minimum Integer promotes
  to Long, and negating the minimum Long promotes to Double, as measured with
  native `oleaut32!VarNeg`. Literal typing follows Microsoft's
  [MS-VBAL number tokens](https://learn.microsoft.com/en-us/openspecs/microsoft_general_purpose_programming_languages/ms-vbal/685ad840-accb-4bdb-8bfd-f3d88498547a).
  Access Eval refused `TypeName(&H10)` syntax in the local probe; native Access
  acceptance of these literals remains unverified. Binary arithmetic and
  subtype-sensitive functions still have separate Variant subtype gaps.
- Every expression is parsed with Access (VBA) operator precedence by
  `CalculatedExpressionParser`. From
  tightest to loosest: `^` (left-associative, and tighter than unary minus,
  so `-2^2` is -4), unary `-`/`+`, `*` `/`, `\`, `Mod`, `+` `-`, `&`,
  comparisons and `Like`/`Is`/`Between`/`In`, `Not`, `And`, `Or`, `Xor`,
  `Eqv`, `Imp`. Syntax the Access grammar rejects throws `ArgumentException`
  naming the expression. The 4,096-character source limit, 128-level nesting
  limit and 256-argument limit apply before evaluation; recursive unary
  expressions are also bounded, while flat left-associative chains do not
  consume parser nesting depth. An `In` list counts its compared operand
  toward the argument limit.
- Excel's postfix `%` is not an Access operator. `CreateTableAsync` and
  `AddColumnAsync` reject a calculated column whose expression uses it
  (outside string literals and `[field]` names), or whose expression the
  parser cannot read, with an `ArgumentException` naming the column and the
  expression; its `ParamName` is `columns` for `CreateTableAsync` and `column`
  for `AddColumnAsync`. The checks run in this order, all before any catalog
  I/O: the table name, then whether the format can hold each column (ACCDB
  only, a result type and no AutoNumber, Attachment, multi-value or Hyperlink
  flag; `JetNotSupportedException` with `FeatureNotSupported`), then the
  expression syntax, then that the
  column declares no `DefaultValue` or `DefaultValueExpression`, which a
  calculated column cannot have (`ArgumentException`). The "already
  exists" check comes last. Columns carried over by a table rewrite are not
  re-checked, so schema edits such as adding a plain column still work on a
  table that already stores such an expression. Its rows still read, and
  an insert that supplies the calculated value keeps it, but an insert that
  leaves the column Null and every `UpdateRowsAsync` (which re-evaluates all
  calculated columns) throw `JetOperationException` with `InvalidExpression`
  and table/column context. A stored expression that calls an unsupported
  function throws `JetNotSupportedException` with `UnsupportedExpression`.
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
- Custom `Format` date strings use VBA's Gregorian tokens: `m`/`mm` are
  months unless the preceding date/time token is `h`/`hh`; `n`/`nn` are
  minutes. Quoted and backslash-escaped text stays literal. `w`, `ww`, `q`,
  `y`, `AM/PM`, `A/P`, `ddddd`, `dddddd` and `ttttt` have VBA meanings;
  optional first-day and first-week arguments control weekday/week numbers.
  Custom short/long date and time tokens use en-US patterns, consistent with
  General Date. These cases were checked against Windows `oleaut32!VarFormat`;
  in particular `mm:ss` is month and seconds, not minutes and seconds.
  See Microsoft's [VBA Format reference](https://learn.microsoft.com/en-us/office/vba/language/reference/user-interface-help/format-function-visual-basic-for-applications)
  and [VarFormat API](https://learn.microsoft.com/en-us/windows/win32/api/oleauto/nf-oleauto-varformat).
- A time with no date is on day 0 (1899-12-30), as in VBA. This was measured
  with VBScript, whose date conversions are VBA's, under LCID 1033. It covers
  time-only text (`CDate("6:00 PM")`), `#6:00#` literals, `Time()`,
  `TimeValue` and `TimeSerial`. So a day-0 date turned into General Date text
  reads back as day 0, and a `=Time()` default stores no date.
  - `TimeSerial` rounds each argument half to even to an Integer. An argument
    outside -32,768 to 32,767 throws `OverflowException`.
  - It then adds the arguments up as an OLE date serial. So
    `TimeSerial(25, 0, 0)` is 12/31/1899 1:00 AM, and `TimeSerial(-1, 0, 0)`
    is -0.0417, which OLE reads as 1:00 AM on day 0.
- Defaults and validation rules use OLE Automation conversion semantics:
  `CByte(True)` yields 255 and False yields 0. A Boolean calculated result
  instead becomes -1 or 0 before coercion to Byte. True overflows; the writer
  refuses that result before changing bytes, as it refuses other result
  overflows. DAO120 caches an error for True and for numeric Byte results
  outside 0-255: a 39-byte envelope with first UInt32 6, zero payload length
  and all remaining bytes zero. Ordinary Null is a 23-byte zero envelope;
  valid Byte results carry a one-byte payload. DAO exposes errors as COM null,
  whereas ordinary Null is `DBNull`; `IsNull` and `IsError` also return COM null
  for these errors. Public reads still conflate empty error and Null payloads,
  and native error-envelope writes remain open under R4. The COM distinction is pinned by
  `CalculatedConversionDaoTests`; the envelope layout was measured in native
  byte probes, distinct from
  the writer's byte-preserving refusal regressions.
  A number or Boolean is a date serial (days since 1899-12-30, so True is
  1899-12-29) in a Date result or the shared rule/default interpreter.
  `CByte(-1)` and `CByte(256)` overflow. `IsDate` is True only for a date or
  text that parses as one, so `IsDate(5)` is False as in VBA.
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
- An evaluation failure carries a structured code: result overflow uses
  `JetLimitationException` with `NumericOverflow`, a generated text result
  exceeding its safety limit uses `JetLimitationException` with `ValueTooLarge`,
  unsupported evaluation uses
  `JetNotSupportedException` with `UnsupportedExpression`, and other evaluation
  failures use `JetOperationException` with `InvalidExpression`. Its context
  names the table and calculated column, and its message
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
  `IPmt`, `PPmt`, `DDB`, `SLN`, `SYD`, `Rate`), `Choose` and `Switch`. `Partition` is supported for stored rules and defaults; table calculated columns refuse it before mutation, matching DAO120.

The list above describes the shared row-local interpreter for defaults and
validation rules. Native table calculated columns additionally refuse
`CBool`, `CByte`, `CInt`, `CLng`, `CSng`, `CCur`, `CDec`, `CStr`, `CDate`,
`TimeValue` and `DateValue`, including nested calls and normalized `$` aliases.
`CalculatedExpressionParser` applies this restriction only in calculated-column
context, before create/add/recalculation changes bytes. Existing cached values
remain readable. `CalculatedConversionContextTests` separates those refusals
from supported defaults/rules and literal text. `CalculatedConversionDaoTests`
checks DAO120's refusal set and accepted `CDbl`, `DateSerial`, `TimeSerial`
and date literals, then verifies writer updates and native compact/readback.
This evidence does not establish all other functions' native admissibility.

Still intentionally out of scope: domain aggregate functions (`DLookup`,
`DCount`, `DSum`, `DAvg`, `DMin`, `DMax`) because DAO/Access rejects them in
table calculated columns with "cannot be used in a calculated column" even
though they are valid in other Access expression contexts; SQL/query evaluation,
cross-record or cross-table lookups; and spreadsheet-only parser constructs such
as sheet, external workbook, array, range, and structured references. Names
that resemble spreadsheet cells resolve as Access field names;
`CalculatedExpressionParserDaoTests` covers native authoring, writer
recalculation and DAO compact for the bare `A1` field name.

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
- `Partition(number, start, stop, interval)` is interpreted with bounded Long
  coercion and constant work. It propagates Null, rounds numeric arguments half
  to even, rejects invalid ranges/intervals, clips the final interval at `stop`,
  and pads each side to the width of `stop + 1` and `start - 1`. Below/above-range
  labels have one blank side, including when the interval is one. The guarded
  `DaoTextCollationTests` case verifies a native table validation rule, selected
  formatting boundaries and DAO write/compact after a writer update. Library
  regressions cover persisted rules on Jet3/Jet4/ACE, int32 bounds, coercion and
  byte-preserving rejected writes. DAO120 refuses direct, case-insensitive and
  nested `Partition` calls when appending a table calculated field, with error
  3859 and no added field; an adjacent supported calculation still succeeds.
  `PartitionCalculatedColumnTests` matches that refusal for create/add and
  for recalculating a synthetically persisted expression, preserving bytes.
  Field names and string literals containing `Partition` remain valid.
  Specialized functions remain separate work.

## Why phased

Each phase is independently shippable and independently testable against a
real Microsoft Access oracle:

1. **1A** lets clients *detect* calc columns and decide whether to error.
2. **1B** unblocks anyone who computes the value themselves (e.g. ETL tools).
3. **2** covers the >95% of real-world Access expressions.
4. **3** is for parity with the long tail.

This avoids a multi-week mega-PR and keeps the scope of each Jackcess
translation bounded.

## Native row counts and schema preservation

DAO120 on 2026-10-06 enumerated four live rows in `calcFieldTestV2010.accdb`
Table1 (Bruce, Bart, John, Test), while its table recordset reported
`RecordCount = 3`. The table's stored statistic is stale; scans must retain
all four rows. `WriterRenamedCalculatedColumn_OpensEvaluatesAndCompactsInDao`
pins that distinction before changing the fixture.

Schema edits retain original descriptor storage types, sizes, flags,
miscellaneous fields and unknown bytes. Only column numbers, variable-slot
indexes and fixed offsets change with the rebuilt row layout. Calculated
result types remain separate from their physical descriptor type; a native
variable Numeric field must not become fixed during a rename.
Native calculated Memo values use the same 64-byte inline long-value limit as
ordinary Memo and OLE, including the calculated envelope. DAO-authored cached
values with 20 UTF-16 characters occupy 63 bytes and stay inline; 21 characters
occupy 65 bytes and spill. Replacing only the oversized inline AllNames slots
with native external LVAL slots made the rewritten calculated table readable
through DAO without changing its rewritten descriptors or other row fields.

### Native text comparisons

Expression comparisons use the database collation and ignore trailing U+0020
spaces, as the native DAO field-rule oracle demonstrates for both directions
of `a` versus `a `. Stored strings and concatenation retain those spaces.
Persisted expressions may have one terminal NUL in their property encoding;
runtime compilation removes that terminator while preserving the raw property
bytes and any NUL inside a string literal.
