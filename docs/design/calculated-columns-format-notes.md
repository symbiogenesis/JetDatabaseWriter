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
from `T.Price`, leaving the separate `Other` table unchanged. A stale bracketed
qualifier makes its calculated plan retain the stored cache, as described below.
Missing table context and unknown fields refuse. Plans require an explicit
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
Bracketed qualifiers `[T].[Price]`, `[T]![Price]` and `[T].Price` retain existing
cached bytes after a native table rename. Native UPDATE leaves every expression
containing a stale qualifier untouched, including unused branches; dependent
fields compute from the retained cache. New rows have no cached slot for those
fields, and their dependents cache status -5617. Runtime DAO Value access reports
a COM error rather than exposing the stored cache. Native compact refuses with
3790. Public library reads expose stored values; an absent cache is CalculatedError
with NativeCode null and display `#Error(Uncached)`, distinct from ordinary Null.

The writer preserves these states, ignores supplied INSERT caches, validates
field bindings before preserving a stale plan, and applies native Required/field/
table-rule behavior. Unknown local fields remain a malformed-schema mutation
refusal; ordinary native rename/drop operations cannot author the tested dangling
field metadata. `CalculatedStaleCacheTests` pins independent native before/after
and constraint specimens and compares all public read surfaces and writer edits.
New bracketed foreign qualifiers refuse declaration before mutation. Bare aliases
retain their current-row binding.

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

Calculated-value validation requires the complete 23-byte envelope overhead: a 20-byte prefix followed by the declared payload and three trailing bytes. A truncated envelope or a payload length that consumes those trailing bytes is corruption. Strict public reads report contextual corruption; lenient reads diagnose a missing value, and writes refuse to erase unreadable calculated content. A complete zero-length envelope can hold ordinary Null or a measured calculated-error status; the status is recognized before value conversion.

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

## Declaration, evaluation and public values

ACCDB calculated fields retain their original expression, physical descriptor and
hydrated result type. New declarations accept the native scalar, Text, Memo,
Binary and OLE result types; AutoNumber, defaults, attachments, multi-value,
Hyperlink, Large Number and DateTime Extended combinations refuse before mutation.
MDB formats refuse calculated declarations before parsing their expression.

Native calculated fields cannot be index, primary-key or relationship keys.
DAO can append index metadata to an empty table, but subsequent inserts and
populated-index creation refuse with 3251; relationship declarations refuse with
3888. The writer refuses new calculated-key declarations and actual row mutations
through existing calculated indexes before effects. Empty metadata remains
readable and survives metadata-only schema rewrites; no-op updates/deletes retain
their native zero-row result. `CalculatedKeyRestrictionTests` qualifies these
boundaries independently, including ordinary/unique/composite/primary declarations.

Declarations bind every syntactic source, including unused branches, before
mutation. Unknown fields, self/circular dependencies and Binary/OLE/complex source
fields refuse, matching native errors 3799, 3866 and 3885. Forward references to
other calculated fields are valid. Persisted write plans validate their reachable
dependency graph. `CalculatedExpressionBindingTests` compares native declarations,
atomic create/add refusals, rename/drop binding and native update/compact.

Untyped Null result declarations follow the native static-type rule: literal,
unary and arithmetic Null, two Null IIf branches and Null Choose alternatives do
not declare a result type. Typed nullable expressions remain valid. DateSerial
also has a native source-token rule for a bare year literal, separate from runtime
Integer conversion; parenthesized, unary-plus and computed year expressions use
runtime conversion. `CalculatedDeclarationAdmissionTests` compares every native
result type and independent decimal/scientific year-token controls.

Insert and update compute active calculated caches from the stored source
values. Caller-supplied caches do not override expressions. Dependencies evaluate
once per statement in dependency order; constraints consume the completed row,
including calculated Null, without evaluating expressions again. Source and cached
Text limits, Numeric coefficient/scale, Currency and Date identity participate in
that projection. Blank or unsupported persisted expressions refuse atomically.
`CalculatedColumnWriteTests` covers direct, auto-committed and explicit transaction
writes, reopen, schema edits and original Access-authored result-type variants.

Typed rows and object POCO properties retain immutable `CalculatedError` values
with signed native status codes, or null NativeCode for an absent cache.
String rows render `#Error(code)` or `#Error(Uncached)`. Calculated
DataTable columns use object so valid scalars, errors and ordinary Null survive.
Incompatible scalar POCO projections fail with property/column context. Error
operands propagate dependency status -3030, or -5617 for an absent source cache,
with native branch and
logical masking. Required/rule constraints and calculated Text/Memo error results
refuse statements as Access does. Native compaction can clear a dependency cache
while retaining the source error; independent fixtures preserve that distinction.
`NativeCalculatedErrorTests` and `CalculatedByteErrorReadTests` also retain ordinary
Null/value controls, exact native error widths and malformed-frame diagnostics.

An ordinary Null uses a complete 23-byte zero envelope. Measured scalar errors
normally use 39 bytes and calculated Binary/OLE errors use 27 bytes inside LVAL
storage; the first signed Int32 is the status and the remaining bytes are zero.
Known error values are recognized only after complete framing validation. Corrupt
framing retains strict contextual failure and documented lenient diagnostics;
write-back refuses to erase unreadable assigned content. These are handwritten,
native-tested parser contracts, without a whole-expression formal proof.

### Native table expression language

`CalculatedExpressionParser` builds a bounded row-local tree directly from Access
syntax. Source text is retained. Limits are 4,096 source units, 128 nested levels,
256 function arguments and 32,768 generated text units. Flat operator chains do
not consume parser nesting depth. References bind the in-flight row; no VBA,
macro, saved query, external command or arbitrary CLR code is executed.

The table language includes numeric/text/date/GUID/Boolean/Null literals,
arithmetic +, -, *, / and ^, concatenation &, comparisons, And/Or/Not,
Is [Not] Null, [Not] Like and [Not] In. Precedence is Access precedence; power is
left-associative and binds more tightly than unary signs. Native And/Or/Not use
Boolean operand conversion, described below.

The native intrinsic functions are Abs, Asc, Atn, CDbl, Choose, Cos, DateSerial,
Day, DDB, Exp, Fix, FV, Hour, IIf, InStr, Int, IPmt, IsEmpty, IsNull, LCase,
Left, Len, Log, Mid, Minute, Month, MonthName, NPer, Pmt, PPmt, PV, Rate,
Right, Round, Second, Sgn, Sin, SLN, Space, Sqr, Str, String, SYD, Tan,
TimeSerial, Trim, UCase, Weekday, WeekdayName and Year. Intrinsic dollar aliases
are Left$, Right$, Mid$, LCase$, UCase$, Trim$, Space$, Str$ and String$.
Mid and InStr require an explicit third argument. Function names ignore case;
field names and quoted text retain their own binding and spelling.

`CalculatedColumnExpressionPolicy` rejects native-prohibited declarations,
including a leading equals sign, radix literals, Between, Mod, integer division,
Xor/Eqv/Imp, Nz, Switch, domain aggregates, formatting/clock functions, most
explicit conversions and spreadsheet/VBA aliases. Field names or string literals
containing those spellings remain valid. `CalculatedNativeAdmissionTests` and
`CalculatedFunctionContextTests` retain independent positive/refusal controls and
whole-image preservation on refusal. `PartitionCalculatedColumnTests` separately
qualifies native Partition refusal even when nested, and persisted-write refusal.

### Shared defaults and validation expressions

Defaults and validation rules use their separate, broader interpreter language.
It includes VBA Variant bitwise operators, radix Integer/Long literals, formatting
and clock functions, explicit conversions, date helpers and Partition. Radix
literals retain signed 16/32-bit identity and optional Long suffixes; a wider or
empty radix literal refuses. An ampersand after an operand concatenates text.

The shared formatting profile is invariant/en-US: General Date omits midnight
time or day-zero date, custom date tokens follow the measured Gregorian VBA
semantics, and numeric text preserves the qualified native notation boundaries.
DateSerial uses rounded Integer arguments; TimeSerial retains day-zero OLE date
behavior. These contexts retain their own Variant subtype, error and native-version
qualification in R4. Their larger function set does not expand table calculated
field admission. Domain aggregates and Eval remain open work outside table fields;
no embedded code or cross-table expression execution is enabled.

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

Numeric comparisons retain the native Variant operation: Double values compare
without first rounding through Decimal, Single mixtures apply native Single
precision, Currency mixtures round to four places and Decimal mixtures use their
measured OLE conversion policy. NaN has the measured native comparison result
of equality. Text against numbers compares their native text forms. Date Variants
retain their separate raw-serial comparison. `CalculatedNumericComparisonTests`
compares all six operators against independent native mixed-subtype controls.

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
Calculated arithmetic retains its native status/error-value rules described below;
valid financial nonfinite values remain distinct from arithmetic errors. Shared
default/rule conversion failures retain their independently qualified refusals.

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
Native calculated Decimal overflow retains its measured calculated status.
Additional shared expression and locale contexts remain R4/E2 work. Caller-field normalization and storage retain
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
available row values, including Null. Table calculated And/Or/Not coerce operands to Boolean rather than applying
VBA bitwise operations. Numeric magnitudes at or below one half are false; larger
magnitudes, Infinity and NaN are true. Nonnumeric strings, including empty strings,
are true; numeric strings follow the same Boolean conversion. Null/error masking
retains native three-valued behavior. `CalculatedLogicalOperandTests` compares
independent native controls and writer/native recomputation. These rules are specific to table calculations; shared rule/default
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

Evaluation retains the raw native Date Variant through references, dependent
calculated Date caches, unary signs, addition/subtraction, Abs/Int/Fix/Round,
numeric conversion and Binary serialization. Date comparisons use the raw serial,
including the reversed fractional ordering of negative OLE dates. Native General
Date text rounds the raw fractional serial to whole seconds, avoiding intermediate
DateTime millisecond loss. `CalculatedNativeDateExpressionTests` and its DAO
pipeline compare exact native source bits, dependencies and expression results
through writer updates, native recalculation and both compactions.

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
uses uppercase hexadecimal within braces. `CalculatedGuidReferenceTests`
compares native expression and Binary results without discarding this identity.
Calculated errors use the independently measured 27-byte error envelope inside
the long value, including status 11 for division by zero.

The shared storage decision covers decoding, snapshots, row encoding, allocation,
usage-map reclamation and raw compact assembly. `CalculatedBinaryResultTests`
compares independent native bytes, writer/native updates, rollback and compact,
including Binary and OLE error repair. Long caches remain intact in managed
reads/writes. The measured ACE version itself loses large chained calculated
Binary/OLE caches during native CompactDatabase, reproduced without any library
writes; shorter external controls survive. Native compaction is not authority to
replace such independently readable source caches with Null in managed storage.

## Native LIKE patterns

LIKE uses a bounded native matcher with pinned character mappings. ANSI-89
wildcards and their measured fullwidth forms are active; percent and underscore
are literal characters. Ranges compare native lowercase UTF-16 units, separate
from linguistic index order. The measured digraphs ae/oe/ss/th expand from their
single-character forms. A question mark consumes an original UTF-16 boundary,
while literals and classes may consume expansion parts.

Invalid patterns retain native status 93. Evaluation preserves native lazy
validation before a reached star and validates the remaining pattern thereafter.
Both operands evaluate before Null propagation: Null beside an invalid pattern
string yields Null, while a calculated-error operand still propagates its error.
The matcher permits 1,048,576 combined UTF-16 units and 16,777,216 matching or
validation steps; exceeding either limit refuses with ValueTooLarge.

`CalculatedLikeTests` freezes 355 independent General Legacy/General cases with
source and native compacted fixtures and repeats writer/native recalculation and
compact. Their hashes and reproducible manifest are retained in
`Databases/CalculatedLike/README.md`.

## Native numeric domains and calculated statuses

Math functions retain native argument domains and cached failures: negative Sqr,
nonpositive Log and infinite Sin/Cos/Tan use status 5; finite Exp/power overflow
uses 6; invalid real powers use 440. Division distinguishes zero/zero (6) from a
nonzero numerator divided by zero (11). Nonfinite numeric names in text produce
status 13, while numeric exponent overflow produces 6.

Valid financial Infinity/NaN values remain values through the measured unary and
math functions. NaN signs are normalized only where native output establishes a
specific bit pattern. Subsequent arithmetic retains its operation-specific errors;
Infinity numerator division and NaN operands do not share a blanket missing-value
rule. The measured ACE version can cache either 11 or 6 for compound Infinity/
Infinity depending on native declaration/evaluation context; independent controls
retain that variation rather than interpreting it as ordinary Null.

`CalculatedMathDomainTests` compares independent native domains, exact floating
results and error statuses through writer/native recalculation and compact.
