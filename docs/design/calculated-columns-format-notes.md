# Calculated columns — storage and evaluation

This document captures everything we know about Access 2010+ calculated columns
(also called "expression columns") and their read/write evaluation in
`JetDatabaseWriter`. The reference implementation throughout is
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
qualifier that differs from the explicit current table yields Null for a known
local field. Missing table context and unknown fields refuse. Plans require an explicit
row/table context; multi-segment object references are refused, and no
embedded code or cross-table lookup is executed. Defaults and validation
rules reject every qualified reference before mutation. Column rename and
drop dependency checks recognize row-local bare aliases while preserving
the original qualifier spelling. A qualified second segment is a field even
when it spells a builtin constant or keyword (such as `vbTrue`, `Null` or
`And`). In a table calculation, bare `vb` constant spellings also bind to fields;
defaults and validation rules retain their builtin constants.
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
error 3790. The writer preserves that expression text and stores Null when
inserting or updating a row with a known local field and a stale bracketed
qualifier. `CreateTableAsync` and `AddColumnAsync` reject new bracketed foreign
qualifiers before mutation, following native declaration refusal; persisted
schema rewrites do not apply that declaration check. Bare qualifiers retain
their separate row-local semantics. The existing DAO controls establish the
native syntax distinction. Extended checks of writer-produced Null caches
through native read and update remain DAO120-guarded and were not executed on
the current host; the native stale-expression compact refusal remains expected.
Resolution when the local field is absent remains unverified and refuses.

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
last evaluated result** on the row, with 23 bytes of envelope overhead:

| Constant | Value | Meaning |
| --- | --- | --- |
| `CALC_EXTRA_DATA_LEN` | `23` | Total envelope overhead: 20 prefix bytes and three trailing bytes. |
| `CALC_DATA_LEN_OFFSET` | `16` | Offset within the header where the payload length (Int32 LE) lives. |
| `CALC_DATA_OFFSET` | `20` | Offset within the header where the payload begins. |
| `CALC_FIXED_FIELD_LEN` | `39` | The fixed-portion column length used for *all* fixed-width calc columns (largest fixed payload `16` + the `23`-byte envelope overhead). |

Calculated-value validation requires the complete 23-byte envelope overhead: a 20-byte prefix followed by the declared payload and three trailing bytes. A truncated envelope or a payload length that consumes those trailing bytes is corruption. Strict public reads report contextual corruption; lenient reads diagnose a missing value, and writes refuse to erase unreadable calculated content. A complete zero-length envelope remains valid framing, but an empty payload alone does not establish ordinary Null. The measured calculated Byte error form below refuses before value conversion.

For variable-width source types the on-disk `col_len` becomes
`originalLen + CALC_EXTRA_DATA_LEN`. For fixed-width source types it is forced
to `CALC_FIXED_FIELD_LEN` regardless of the underlying type. Long-value result
types (`MEMO` / `OLE`, and calculated Binary) keep the normal LVAL row header in the row; the bytes
inside the LVAL payload are wrapped.

Native calculated Text uses a 486-byte maximum payload (243 UTF-16 units) and
a 509-byte descriptor width including the envelope. Caller `MaxLength` hints do
not change this width. `CalculatedTextSchemaTests` compares newly declared fields
with native metadata and verifies the writer's caches after native recalculation
and compact.

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
- `Numeric`: normally 16 bytes: a reserved two-byte word, scale byte, sign byte
  (`0x80` for negative), then little-endian Hi32, Lo32 and Mid32 coefficient
  words. This differs from the ordinary 17-byte Numeric fixed slot.

Independent native source and compacted controls establish that the leading
word is not a payload length: native direct references use zero and arithmetic
results commonly use 14. Decoding ignores that reserved word, validates scale
and sign, and reads coefficient words at fixed offsets. Payload widths from
four through sixteen bytes zero-fill omitted trailing bytes; a four-byte header
is Decimal zero. An empty calculated envelope is ordinary Null. Short headers,
invalid scale/sign and oversized payloads report contextual corruption.
`CalculatedNumericNativeLayoutTests` checks native ordinary/reference/arithmetic
payloads, native compaction and public writer/native recalculation.
Helpers in this codebase: `CalculatedColumnUtil.Wrap` / `.Unwrap` (round-trip
verified by `CalculatedColumnUtilTests`).

## Implementation

### Metadata

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
- Tests: `tests/JetDatabaseWriter.Tests/Schema/CalculatedColumnUtilTests.cs`,
  expanded metadata fixture coverage.

### Cached value storage

The writer creates calculated columns, computes their expressions and stores
the resulting cached values for both the library and Access to read.

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
- Tests: `tests/JetDatabaseWriter.Tests/Writer/CalculatedColumnWriteTests.cs`,
  updated Access-authored fixture coverage in
  `tests/JetDatabaseWriter.Tests/Schema/CalculatedColumnFixtureTests.cs`, and
  byte-level cached-payload assertions in
  `tests/JetDatabaseWriter.Tests/Schema/CalculatedColumnPayloadTests.cs` (including
  DAO-authored `IIf` calculated fields and native `Switch`/domain-aggregate
  declaration refusals on Access-equipped hosts).

### Row-local expression evaluation

Declarations bind every syntactic source, including unselected branches, before
mutation. Missing fields, self/circular dependencies and Binary/OLE/complex source
fields are refused, matching native errors 3799, 3866 and 3885. Forward references
to other declared calculated fields are valid. Persisted write plans validate the
reachable dependency graph before evaluation. `CalculatedExpressionBindingTests`
compares native declarations and verifies atomic create/add refusals, rename/drop
binding and native update/compact behavior.

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
- `ConstraintRegistry` recomputes calculated columns for every insert and update
  after source values are applied. Supplied calculated values never override
  expressions, including after reopen and when a dependent calculated column
  appears later in the schema. A blank or unsupported stored expression refuses
  the mutation atomically. Validation consumes the completed row, including Null
  and volatile calculated results, without evaluating them a second time.
- The shared parser accepts common Access syntax: leading `=` is ignored
  outside table calculated fields,
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
  table that already stores such an expression. Its rows still read; every
  insert and `UpdateRowsAsync` (which re-evaluates all calculated columns)
  throws `JetOperationException` with `InvalidExpression`
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
  instead becomes -1 or 0 before coercion to Byte. DAO120 and the writer cache
  an error for True and numeric Byte results
  outside 0-255: a 39-byte envelope with first UInt32 6, zero payload length
  and all remaining bytes zero. Ordinary Null is a 23-byte zero envelope;
  valid Byte results carry a one-byte payload. DAO exposes errors as COM null,
  whereas ordinary Null is `DBNull`; `IsNull` and `IsError` also return COM null
  for these errors. Public typed rows and object POCO properties retain a
  `CalculatedError` with its signed `NativeCode`; string rows expose `#Error(code)`.
  Scalar POCO properties refuse incompatible error values with column/property
  context. Calculated DataTable columns use `object` to retain scalar values,
  errors and ordinary Null. Malformed framing retains its strict/lenient policy.
  `NativeCalculatedErrorTests` uses independently authored source/compacted
  fixtures and fresh native read/update/compact controls for observed statuses
  5, 6, 11, 13, 94, 440 and -3030. Dependencies propagate the measured -3030
  status with IIf/And/Or masking; validation and Text/Memo error results refuse
  statements as Access does. Native compact can turn a dependency error into
  ordinary Null while retaining its source error; the fixtures distinguish both.
  `CalculatedByteErrorReadTests` constructs that independently described layout
  in a writer-created row; it is a native-layout regression control, not a
  Microsoft-authored fixture. Its controls retain ordinary Null, Byte values
  and malformed-envelope classification. `CalculatedConversionDaoTests`
  confirms the native error/Null distinction and writer error emission.
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
  radix literals in the shared interpreter; native `{guid {...}}` literals retain
  Guid identity in table and shared contexts. A Guid converted to calculated Text
  uses uppercase hexadecimal with braces. `CalculatedGuidLiteralTests` verifies
  typed/text caches, writer updates and fresh DAO read/update/compact.
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
validation rules. Table calculated fields use the smaller native ACE language
enforced by `CalculatedColumnExpressionPolicy`. Fresh Access Runtime
16.0.20527.20112 declarations establish the positive/refusal controls in
`CalculatedNativeAdmissionTests`, including nested functions, exact `$` aliases,
operators, literals and argument counts. Native table fields reject `Nz`,
`Switch`, domain aggregates, formatting/clock functions, most explicit conversions
and the interpreter's spreadsheet/VBA aliases. Mid and InStr require their
explicit third argument. String-returning intrinsic `$` spellings retain their
native runtime distinction; MonthName/WeekdayName do not have `$` aliases.
Table expressions reject a leading `=`, radix literals, Between, Mod, integer
division, Xor, Eqv and Imp. Shared defaults/rules retain their separate language.

Declaration checks run before create/add/recalculation changes bytes. Function
names are case-insensitive; field names and literal text containing function
names remain valid. `CalculatedFunctionContextTests` checks complete-image
preservation on refused declarations. Existing cached values remain readable.
The conversion DAO controls additionally verify accepted CDbl, DateSerial,
TimeSerial and date literals through writer updates and native compact/readback.

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
`tests/JetDatabaseWriter.Tests/Writer/CalculatedColumnWriteTests.cs`, precedence and
value-semantics cases in
`tests/JetDatabaseWriter.Tests/Schema/CalculatedExpressionAccessSemanticsTests.cs`,
plus the Access-authored cached-value fixture coverage, which also re-evaluates
every Access-authored fixture expression against the values Access cached.

### Expression contexts

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

### Native calculated Text markers

Calculated Text follows Access's cached conversion for leading U+FEFF. Native
mixed-mode Unicode compression accepts a tie with plain UTF-16; a zero low byte
forces plain storage. Short or ineligible plain marker-prefixed results can
therefore decode to a different value, exactly as native calculated caches do.
An empty decoded calculated Text result is ordinary Null. Dependent calculated
expressions see the converted cached value, including that Null, while functions
inside the original expression can still return an empty string.

`CalculatedTextLeadingMarkerDaoTests` independently authors native source and
compacted controls, compares exact original payloads with writer output, checks
dependent lengths, and exercises writer/native updates and native compact.
`NativeTextMarkerTests` keeps the corresponding frozen native read expectations.
These calculated conversions are separate from ordinary Text/Memo preservation.
Calculated references first apply the physical character limit, so dependent
values agree with the encoded source and cached columns. Ordinary compressed
Text also counts UTF-16 units before compression. Plain UTF-16 storage preserves
unpaired code units without encoding replacement; fresh native surrogate
controls qualify both source and calculated values.

## Floating expression values

### Stored date identity

Writer snapshots retain the exact OLE date serial separately from the public
`DateTime` projection. Native -1/24 and +1/24 both display as 01:00 on day zero,
but compare differently; submillisecond serial bits can also share one public
DateTime value. Updates, schema rewrites and index encoding preserve those stored
serials, and validation callbacks still receive DateTime values.
`NativeDateSerialTests` uses independently authored source/compacted fixtures,
exact raw serials, index reads, criteria and fresh DAO update/compact controls.


The shared row-local interpreter retains Single/Double intermediates for
addition, subtraction, multiplication, division, unary signs and Abs/Int/Fix.
Single results promote to Double on overflow; Long or Double operands select
Double, while verified narrow/Boolean operands can retain Single. Decimal
operands retain their separate existing decimal path. Integral division selects
Double in the measured DAO3.6 Jet4 query context; this evidence is distinct from
the documented VBA Variant division matrix.

Numeric text uses 15 significant digits for Double and 7 for Single, with the
measured complete fractional-width rule for fixed versus scientific notation
and a normalized negative zero. `CStr(1/3)` is `0.333333333333333`.
Nonfinite numeric conversions, arithmetic results and division operands refuse;
public mutation regressions retain identical database bytes on refusal. Native
numeric-text overflow can return Null where the writer conservatively refuses;
this does not establish native error-envelope parity.

`CalculatedExpressionFloatingSubtypeTests` compares fixed native controls with
the interpreter, public stored defaults and ACE calculated Double insert/update.
Its guarded native query controls use x86 DAO3.6, `dao360.dll` 10.0.26100.5074,
including formatting boundaries and invalid text divisors. These checks do not
establish fresh ACE native behavior, all locale formats or calculated-field
admissibility of every function. Remaining division, Mod, bitwise and missing-result rules,
mixed Date/Decimal arithmetic and other subtype-sensitive functions
remain R4 work. Expression evaluation remains handwritten and tested.

### Numeric literals and Decimal-to-floating expression coercion

The shared interpreter selects Long for exactly integral literals within Int32,
Decimal for other exactly representable Decimal literals, and finite Double
otherwise. Decimal admission checks source digits and scale so parsing cannot
silently round an overprecise literal or underflow it to zero. Native literals
discard scale-only zeros before conversion. A direct `-2147483648` literal is
Long; `-(2147483648)` remains Decimal. Exponentiation and other unary grouping
retain their grammar. Measured Long addition/subtraction/multiplication with
integral operands retain Long in range and promote to Double on overflow.
Abs/Int/Fix preserve the measured Long behavior; Abs of Int32.MinValue is Double.
Byte, Boolean and Integer arithmetic uses signed Integer results, promotes to
Long when needed, and retains an explicit Long operand's wider subtype.
Unary signs convert Byte and Boolean to Integer. Abs, Int and Fix retain
Integer; Abs of Int16.MinValue promotes to Long. `CalculatedExpressionIntegralSubtypeTests`
freezes independently measured DAO3.6 Jet4 values, subtypes and mixed Single
result bits, repeats the native controls, and checks public calculated
insert/update/reopen. These controls do not establish every missing-result
boundary or fresh ACE behavior.
Decimal expression text uses full fixed decimal digits without trailing zeros.

Field references carry native expression subtypes rather than CLR storage types:
Byte fields project as Long and Yes/No fields as signed Integer (-1 or 0),
including calculated sources. An ordinary nullable Yes/No input projects the
stored false value. `CalculatedFieldSubtypeTests` verifies mixed-Single result
bits and Binary result widths through native read, recalculation and compact.

Decimal-to-Double conversion explicitly follows OLE's low-64/high-32 limb
conversion, addition and scale division. Decimal-to-Single uses that Double
intermediate. This retains measured native bits across the .NET 11 conversion
change, including a Single midpoint where directly correct rounding would
produce a different result. Stored Decimal coefficient/scale remains intact:
literal `99.9999999999999900` normalizes and CDbl returns `100`, while the same
mathematical value in a native Decimal field retaining scale 16 converts to the
lower Double. CDbl/CSng and floating result coercion use this expression policy;
caller-input and storage conversions retain separate qualification obligations.

`CalculatedExpressionNumericLiteralTests` freezes types, text and exact floating
bits, including range, precision, underflow, signed-minimum and scale controls.
Fresh DAO3.6 Jet4 query and stored-default controls use `dao360.dll`
10.0.26100.5074, then accept writer default insertion, native read/update/compact
and managed compacted readback. Independent Decimal-field controls preserve
source scale and compare native CDbl/CSng result bits. Public ACE cached
insert/update/reopen remains library evidence; fresh ACE-native contexts,
additional locales and remaining subtype-sensitive functions
remain R4/E2 work. The OLE-derived routines retain their MIT notice and add no
shipping dependency or formal expression proof.

### Currency

Currency field references retain a distinct internal fixed-point Variant subtype;
public row values remain `decimal`. `CCur` rounds half to even to four places and
checks the signed 64-bit scaled range. Currency addition/subtraction and products
with integral/Currency operands retain Currency; floating/text products and
non-Decimal division produce Double. Decimal mixing retains Decimal precision.
Abs, Int, Fix, Round and unary signs retain Currency. Floating addition/subtraction
performs the native floating operation before rounding; numeric text first converts
to Currency.

`CalculatedExpressionCurrencyTests` freezes independent native values and subtypes,
repeats fresh ACE queries including Decimal fields, and exercises DAO-authored
calculated Currency caches through writer/native updates and native compact.
Text uses exact current ACE formatting at Currency boundaries. DAO3.6's older
extreme Currency-to-text rounding differs and remains separate engine evidence.

Abs, Sgn, Int and Fix preserve ordinary Null. Sgn returns a signed Integer and
accepts finite Double values outside Decimal range and Date serials. Abs, Int
and Fix preserve Date results, applying their operations to the OLE serial.
`CalculatedExpressionNumericFunctionTests` compares fresh ACE queries and calculated
cached values through writer updates, native reads and compact. Other numeric
functions have distinct native Null/error behavior and do not inherit this rule.

### Round

`Round` retains the measured native Single/Double/Integer/Long/Date subtype;
Byte and Boolean produce Integer, numeric text produces Double, and Decimal
fields retain Decimal. Precision is coerced half-even to Long. Negative precision
refuses, including a Null input; Null and integral values otherwise retain their
value for positive precision. Floating/date precision accepts 0 through 22;
Decimal precision of 28 or more retains the input without a narrower .NET limit.
Floating rounding scales the binary value directly before half-even rounding and
division. It preserves native midpoints such as `Round(CDbl(1.225),2)=1.23` and
valid large Double values; overflowing scale products retain the original value.

`CalculatedExpressionRoundTests` freezes native query controls and exact source,
stored and result Double bit patterns around precision/magnitude boundaries.
Fresh DAO3.6 Jet4 Double and Decimal field controls distinguish function behavior
from literal conversion, separately covered by
`CalculatedExpressionNumericLiteralTests`. For example, native
`CDbl(99.99999999999999)` already yields `100` before rounding; exact field bits
avoid treating that literal conversion as a Round result. Decimal controls use
native TypeName/CStr before DAO marshals a computed numeric result as Double.
Public ACE calculated
insert/update/reopen and byte-preserving invalid-precision refusal supply library
regressions. These controls use `dao360.dll` 10.0.26100.5074; fresh ACE native
contexts, additional locales and Currency identity remain separate R4/E2 gaps.

### Floating-to-Decimal expression coercion

Numeric source references first project through the ordinary field encoder's
precision/scale normalization, so calculated caches use the coefficient and scale
actually stored on the row. Calculated Numeric results preserve their native
coefficient/scale rather than applying the ordinary field descriptor's scale.
Decimal division removes trailing quotient zeroes. `CalculatedNumericReferenceTests`
compares source bytes, dependent results and native recalculation/compact.

Expression `ToDecimal` and `TryConvertDecimal` use the measured OLE numeric
policy: Single and Double retain 7 and 15 significant digits through binary
scaling followed by half-even integer-mantissa rounding. Fixed literal powers
through 28 preserve the native operation order at decimal midpoint and underflow
boundaries. Decimal construction uses integer limbs and scale; runtime floating
casts do not determine this policy. This avoids the changed floating/Decimal
[conversion behavior in .NET 11](https://learn.microsoft.com/en-us/dotnet/core/compatibility/core-libraries/11/decimal-biginteger-floating-point-conversions).

`CalculatedExpressionDecimalCoercionTests` pins exact Single/Double source bits,
native Decimal values and boundary refusals. DAO3.6 Jet4 controls independently
compare Decimal-zero plus floating fields, native result TypeName/CStr before
DAO marshaling, and complete stored source bits. Public calculated mutations
retain native decimal rounding through reopen and byte-identical refusal.
The scaling policy adapts the .NET 10 OLE-derived implementation; its MIT notice
is retained in the packaged third-party notices.

The expression coercion refuses nonfinite values and magnitudes at 2^96; its
try-conversion reports false. Mixed Decimal/Single/Double addition, subtraction,
multiplication and division instead use Double when the finite floating operand
cannot convert to Decimal. Promotion precedes arithmetic, including a zero
product; it preserves signed zero. Convertible operands retain Decimal
arithmetic, whose result overflow still refuses rather than promoting.

`CalculatedExpressionDecimalPromotionTests` compares both operand orders and
positive/negative Single/Double 2^96 with native DAO3.6 Jet4 TypeName and exact
result bits. Decimal 0/1 controls and public ACE calculated insert/update/reopen
exercise the shared interpreter; nonfinite update refusal preserves bytes.
Native in-range Decimal product overflow exposes a missing result, while the
writer conservatively refuses. Native missing/nonfinite mapping, fresh ACE and
locale contexts remain R4/E2 work. Caller-field normalization and storage retain
their shared conversion contract; remaining native/runtime qualification and
caller-field/storage conversions and broader native context/representation qualification remain separate obligations; the expression reverse conversion policy and measured scope are described above.

## Literal Unicode markers in Text


Ordinary variable Text with Unicode compression disabled stores plain UTF-16, including literal FF FE at the beginning. Row, string, POCO and write-back decoding use that column storage rule. Calculated Text still permits compressed Unicode inside its envelope. Its dedicated decoder accepts the verified terminal low-byte form; ordinary Text and Memo retain separate decoding rules pending their own native controls. Native controls preserve leading U+FEFF in a cached Latin-1 string using an escaped compressed payload; other controlled native cached values lose or reinterpret that marker. A source field's original text is not authority to invent a different cached result. Raw compact assembly preserves each observed envelope without recalculation. Writers reproduce the native calculated cache conversion and project dependent expressions through the stored value. Ordinary compressed variable Text supports an independently verified 6 through 255-unit escape: an optional contiguous nonsurrogate wide prefix with nonzero low bytes precedes at least four nonzero Latin-1 units; without a wide prefix at least five Latin-1 units follow the marker. The complete value fits the declared character limit. Short values/tails, NUL, low-byte-zero wide units, surrogates and wide units after the Latin-1 tail remain refused. Native DAO3.6 Jet4 read/update/compact and frozen native-row fixtures establish that ordinary subset; fresh DAO120 confirmation remains separate.
## Financial functions

FV, PV, Pmt, NPer, IPmt, PPmt, DDB, SLN, SYD and Rate use the measured ACE
parameter domains and arithmetic order. Payment timing rounds half to even to
a signed Integer before selecting beginning/end timing. Invalid cash-flow
combinations and nonconvergent Rate calculations report evaluation failures;
Rate uses a bounded forty-step secant search. Native financial Infinity and NaN
results remain Double values rather than ordinary Null or numeric overflow.

`CalculatedExpressionFinancialTests` freezes independent native normal,
boundary and nonfinite results and invalid domains, repeats fresh ACE queries,
and verifies native calculated caches through writer recomputation, native
updates and compact. The bounded Rate algorithm adapts the MIT-licensed .NET
implementation named in THIRD-PARTY-NOTICES.md; other operations preserve the
independently measured Access order where it differs from .NET.

## Native calculated conditions and Boolean results

Table calculated fields distinguish condition truth from Boolean result conversion.
`IIf` treats every non-null string, including `""`, `"0"` and `"false"`, as true;
floating conditions retain every nonzero value. Its unused branch is not evaluated.
Boolean result conversion instead rounds numeric input half to even to a signed
16-bit Integer before testing zero. Long/Double overflow caches status 6, while
Decimal overflow caches status 13. Ordinary Null remains Null.

`Choose` evaluates all arguments before selecting a branch and truncates its
one-based index. A Null index caches status 13; an error in an unselected argument
still propagates. `IsEmpty` evaluates its argument and returns false for the
available row values, including Null. Native bitwise operators treat nonnumeric
strings, including an empty string, as true; numeric strings follow integer
conversion. These rules are specific to table calculations; shared rule/default
contexts retain their independently qualified behavior.

`CalculatedNativeLogicalTests` compares independent ACE-authored source and native
compacted fixtures with writer-produced caches, native recalculation and compact.
The provenance, exact input manifests and hashes are retained in
`Databases/NativeCalculatedLogic/README.md`.

## Native calculated text functions

Slice lengths and positions use native rounded integer conversion with checked
ranges. Plain string functions preserve Null; their dollar aliases retain the
native invalid-Null status. Trim removes U+0020 only. Asc and numeric String
characters use the measured CP1252 mapping. `String$` distinguishes a Null
count (status 13) from a Null character (status 94); `Str$` Null is status 94.
Str omits the leading zero before a
fraction and prefixes nonnegative values with a space.

UCase/LCase use pinned native mappings, independent of the host runtime's Unicode
version. Independent source and compacted fixtures cover every BMP code unit,
all supplementary scalars and unpaired surrogates; supplementary case changes
are the measured Deseret ranges. Full enumeration and fixture provenance are
recorded in `Databases/CalculatedTextCase/README.md`.

InStr uses the supported database collation for textual comparison, including
native ligature behavior. Its bounded search permits 16,777,216 comparison-work
units and refuses excessive work rather than reporting a false absence.
`CalculatedTextFunctionTests`, `CalculatedTextFunctionDaoTests` and
`CalculatedTextCaseTests` cover Null/error ordering, native source and compacted
fixtures, and writer caches through native recalculation/compact.

## Native calculated date functions

Date components round the exact stored OLE serial to the nearest whole second,
including day carry, before returning a signed Integer. This preserves boundaries
that a preliminary millisecond DateTime conversion would change. Null component
inputs remain Null. DateSerial and TimeSerial round arguments to native Integer
ranges and preserve their raw native serial results, including negative time-only
values. Native argument/domain failures retain statuses 5, 6 and 13.

Two-digit DateSerial years follow the operating system Gregorian calendar window;
the measured host uses 2049 as its upper endpoint. The independently measured
English General profile uses Sunday for system-default week starts and English
month/day names. `CalculatedNativeDateFunctionTests` and its DAO controls retain
native subtypes through Binary payloads and verify writer/native recalculation
and compact. Database/thread locale probes are separate from changing operating
system regional settings; they did not alter the measured naming profile.

## Native calculated Binary and OLE results

Binary result type 9 and OLE result type 11 both store their wrapped cache through
long-value descriptors, regardless of the physical descriptor type. Ordinary
Binary remains an inline byte field. Variant result bytes preserve native scalar
widths, little-endian floating/date bits, scaled Currency, UTF-16 text and GUID
identity. Decimal results use their native text conversion. Empty bytes are Null;
GUID literals and references retain their 16-byte tag for Binary serialization,
but use native string semantics in addition and string metadata. Their text form
uses uppercase hexadecimal within braces. `CalculatedGuidExpressionTests`
compares native expression and Binary results without discarding this identity.
calculated errors use the independently measured 27-byte error envelope inside
the long value, including status 11 for division by zero.

The shared storage decision covers decoding, snapshots, row encoding, allocation,
usage-map reclamation and raw compact assembly. `CalculatedBinaryResultTests`
compares independent native bytes, writer/native updates, rollback and compact,
including Binary and OLE error repair. Long caches remain intact in managed
reads/writes. The measured ACE version itself loses large chained calculated
Binary/OLE caches during native CompactDatabase, reproduced without any library
writes; shorter external controls survive. Native compaction is not authority to
replace such independently readable source caches with Null in managed storage.
