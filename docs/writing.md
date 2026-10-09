# Writing data and changing schemas

[Back to the README](../README.md) · [Reading guide](reading.md) · [Operations guide](operations.md)

Start with the [create-and-insert example](../README.md#writing-data), then use the recipes below for existing databases, constraints, schema changes and relationships. Examples use types from `JetDatabaseWriter`, `JetDatabaseWriter.Models` and `JetDatabaseWriter.Enums`.

Keep other writers away from the file while your writer is open. For grouped changes and failure handling, see [transactions](operations.md#transactions) and [concurrent access](operations.md#thread-safety-and-concurrent-access).

## Contents

- [Creating tables](#create--drop-tables)
- [Column constraints](#column-constraints)
- [Inserting named values](#insert-rows--named-columns-rowvalues)
- [Defaults and NULL](#defaults-and-explicit-null)
- [Updating and deleting](#update--delete)
- [Storage maintenance](#storage-maintenance-and-secure-erase)
- [Changing columns](#add-drop-and-rename-columns)
- [Linked tables](#linked-tables)
- [Relationships](#foreign-key-relationships)

## Writing Data

> Supports Jet3, Jet4, and ACE formats — `.mdb` (Access 97+) or `.accdb`.
>
> Jet3 stores text and object names in the database's code page. New Jet3 files use Windows-1252, as Access 97 does. .NET would write a character outside the code page as its closest match or `?` (`Łódź` as `Lódz`, `中文` as `??`), so the writer refuses it before writing anything: a name throws `ArgumentException`, and a Text or Memo value in an insert or update throws `JetLimitationException`. Jet4 and ACE store text as UTF-16, which holds any character.

Path-based `CreateDatabaseAsync` initializes and flushes an adjacent private staging file before publishing the complete database. Publication refuses an existing destination and retains the staging file's [private-file permissions](operations.md#encryption-options). A failure reopening the published file leaves the completed database in place; a crash before publication may leave an unused staging file. Caller-supplied streams keep their separate in-process failure contract.

Strings supplied to Binary columns use the database's code page on every format. A character that cannot be represented exactly throws `EncoderFallbackException` before that row is written; supply a `byte[]` to store arbitrary bytes.

```csharp
await using var writer = await AccessWriter.OpenAsync("database.mdb");
```

### Create & drop tables

```csharp
await writer.CreateTableAsync("Contacts", new[]
{
    new ColumnDefinition("ContactID", typeof(int)),
    new ColumnDefinition("Name",      typeof(string), maxLength: 100),
    new ColumnDefinition("Email",     typeof(string), maxLength: 255),
    new ColumnDefinition("Score",     typeof(decimal)) { NumericPrecision = 5, NumericScale = 1 },
});

await writer.DropTableAsync("Contacts");
```

`DropTableAsync` refuses with `JetOperationException` (`TableInRelationship`) before mutation when an enforced relationship connects the table to another table; drop that relationship first. Unenforced and self-referencing relationships are removed with the table. Targeted DAO `TableDefs.Delete` checks verify these cases (see [Foreign-key relationships](#foreign-key-relationships)).

New table, column, index, relationship and linked-table names follow the Access naming rules: 1 to 64 characters, not only white space, no leading space, and none of `.` `!` `` ` `` `[` `]` or a control character (U+0000–U+001F and U+007F). Spaces inside or at the end of a name, quotes, `#`, `=` and non-ASCII letters are fine. `CreateTableAsync`, `AddColumnAsync`, `RenameColumnAsync` (the new name), `CreateRelationshipAsync`, `RenameRelationshipAsync` (the new name) and the `CreateLinked*TableAsync` methods (the local name) throw `ArgumentException` for any other name before writing anything, and `CreateTableAsync` also rejects two columns whose names differ only by case, since Access compares names ignoring case. A linked table's foreign name, such as `dbo.Orders` or `orders.csv`, is not checked against these rules. On a Jet3 database every new name, and a linked table's foreign name, path and connect string, must also be in the database's code page (see above). Names that an existing database already holds are only looked up, so a table or column written with such a name by another tool can still be read, written, altered and dropped, and a column can be renamed to a valid name.

#### Decimal and Currency columns

A `decimal` column is an Access Decimal with the declared `NumericPrecision` and `NumericScale`, and a value is rounded half to even to that scale when it is stored. Without them it is Access's default Decimal(18,0), which holds whole numbers only, so `95.5` would be stored as `96`. Set `IsCurrency` for an Access Currency column instead, on any format. Currency keeps four decimal places, rounding half to even past them, and holds -922,337,203,685,477.5808 to 922,337,203,685,477.5807; a value outside that range throws `OverflowException`. `ColumnMetadata.IsCurrency` reports Currency columns, so copying a column's metadata into a `ColumnDefinition` creates the same type.

```csharp
new ColumnDefinition("UnitPrice", typeof(decimal)) { IsCurrency = true },
new ColumnDefinition("Weight",    typeof(decimal)) { NumericPrecision = 10, NumericScale = 3 },
```

Jet3 (Access 97) databases have no Decimal type, so on a Jet3 `.mdb` a `decimal` column is created as Currency. Its values then keep four decimal places whatever `NumericScale` says, and the precision and scale are neither stored nor applied to values. Currency's largest value, 922,337,203,685,477.5807, leaves room for every value with up to 14 digits before the decimal point, so a declared `NumericScale` above 4, or a `NumericPrecision` more than 14 above `NumericScale`, throws `NotSupportedException` before anything is written. That includes Decimal(15,0) and the default Decimal(18,0): on Jet3, declare a precision and scale that fit, such as Decimal(18,4) or Decimal(10,2), or set `IsCurrency` for Currency's full range.

#### Column constraints

`ColumnDefinition` accepts four optional constraints in addition to `Name`/`ClrType`/`MaxLength`:

```csharp
await writer.CreateTableAsync("Contacts", new[]
{
    new ColumnDefinition("ContactID", typeof(int)) { IsAutoIncrement = true, IsNullable = false },
    new ColumnDefinition("Name",      typeof(string), maxLength: 100) { IsNullable = false },
    new ColumnDefinition("Score",     typeof(int))
    {
        DefaultValue             = 0,
        ValidationRule           = v => v is int i and >= 0 and <= 100,
        ValidationRuleExpression = ">=0 And <=100",
        ValidationText           = "Score must be between 0 and 100.",
        Description              = "Test score (0-100).",
    },
});
```

| Constraint | Persisted in the file? | Notes |
|---|---|---|
| `IsNullable` | ✅ `MSysObjects.LvProp` (`Required`) | An insert whose value is still null after defaults and AutoNumber values are applied, and an update that sets the column to null, throw `InvalidOperationException`. An explicit `null` on insert is never replaced by a default, so it is rejected even when the column has one, as in Access. Restored on reopen; surfaced to readers via `ColumnMetadata.IsNullable`. |
| `IsAutoIncrement` | ✅ TDEF flag bit `FLAG_AUTO_LONG 0x04` | Supported for `short` and `int`; `byte` and `long` throw `NotSupportedException`, but an AutoNumber `Byte` or `Large Number` column another tool wrote is filled in the same way. An insert generates the next value for `null`, `DBNull.Value`, `DbDefault.Value`, an omitted column, or a non-nullable POCO property left at 0. Seeded on first use from `max(TDEF AutoNumber counter, existing) + 1`, where the existing maximum comes from the rightmost key of an ascending index that starts with the column when there is one, and otherwise from a scan of that column alone; the counter only rises, so deleted top values are not reused in a later session. An update may assign an explicit value, which raises the stored high-water as an insert does, but setting the column to null throws `InvalidOperationException`. |
| `IsPrimaryKey` | ✅ TDEF logical-index entry with `index_type = 0x01` | Shortcut for synthesizing a PK `IndexDefinition` named `"PrimaryKey"` from one or more columns (in declaration order). Forces the PK key columns to `IsNullable = false` on the emitted TDEF. Mixing this with an explicit PK `IndexDefinition` in the same call throws `ArgumentException`. Single- and multi-column PKs both participate in live B-tree leaf maintenance (composite-key path). |
| `DefaultValue` | ✅ as a literal `DefaultValue` expression in `MSysObjects.LvProp` | CLR object stored when an insert leaves the column out (a `RowValues` row that does not name it, a POCO with no property for it, or `DbDefault.Value`); an explicit `null` / `DBNull.Value` stores null. Unless `DefaultValueExpression` is set, it is also persisted as a literal expression (`7`, `"text"`, `True`, `#2024-02-29 08:30:00#`), so a later `AccessWriter` and Microsoft Access apply the same default. Text, Boolean, numeric, `DateTime` and `Guid` values persist; other types such as `byte[]` throw `NotSupportedException` at table creation. Every writer evaluates the literal and converts it to the column's type: a `DateTime` default to the whole second, and a number of any CLR type as its literal reads back in the column's type. A number the column's type cannot hold (1e39 on a Single column, 300 on a Byte column), NaN and infinities throw `ArgumentException`. `DBNull.Value` means no default. Updates never apply defaults. Not allowed on AutoNumber, calculated, Attachment or multi-value columns (`CreateTableAsync` and `AddColumnAsync` throw `ArgumentException`); a `DefaultValue` property another tool stored on one is kept but never applied. |
| `ValidationRule` | ⚠️ this writer only | Checked against every non-null value an insert supplies or an update assigns; a rejection throws `ArgumentException`. A CLR `Func<>` cannot be serialized into the file, so a writer that opens the database later does not enforce it. For a rule every writer and Microsoft Access enforce, set `ValidationRuleExpression`. |
| `DefaultValueExpression` | ✅ `MSysObjects.LvProp` (`DefaultValue`) | Jet expression string (e.g. `"0"`, `"\"hi\""`, `"=Now()"`, `"Date()"`). Every `AccessWriter` evaluates it when an insert leaves the column out or passes `DbDefault.Value`; an explicit `null` stores null. Blank text is treated as absent. Non-blank text wins over `DefaultValue` for persistence; the declaring writer still uses a CLR `DefaultValue` when both are set. Not allowed on AutoNumber, calculated, Attachment or multi-value columns, as for `DefaultValue`. Surfaced to readers via `ColumnMetadata.DefaultValueExpression`. |
| `ValidationRuleExpression` | ✅ `MSysObjects.LvProp` (`ValidationRule`) | Access rule with the column as implicit left operand (e.g. `">=0 And <=100"`, `"Is Not Null"`, `"Between 1 And 10"`, `"In (1,2,3)"`, `"Like \"A*\""`, `"0 Or >100"`). Every `AccessWriter` checks it against each value an insert stores and each value an update assigns; a rejection throws `ArgumentException`. Like Access, a rule that does not test for Null accepts Null. Surfaced via `ColumnMetadata.ValidationRuleExpression`. |
| `ValidationText` | ✅ `MSysObjects.LvProp` (`ValidationText`) | User-facing message Access shows when `ValidationRuleExpression` rejects a value; the writer appends it to the `ArgumentException` message. Surfaced via `ColumnMetadata.ValidationText`. |
| `Description` | ✅ `MSysObjects.LvProp` (`Description`) | Free-text column description shown in Access Design View. Surfaced via `ColumnMetadata.Description`. Preserved across `AddColumnAsync` / `DropColumnAsync` / `RenameColumnAsync`. |

Column defaults and validation rules that Microsoft Access wrote into an existing database are applied the same way. For example, older versions of Access gave every Number column a default of `0`, so an insert that leaves such a column out stores `0`; an explicit `null` still stores null, as in Access SQL. The writer evaluates default and rule expressions with its calculated-column expression engine, which does date arithmetic as Access does, so a default of `=Date()+7` stores the date a week from today and a rule of `>=Date()-30` rejects older dates. It also reads single-quoted text (a default of `'N/A'`) and VBA `&H`/`&O` literals (a rule of `<=&HFF`). An expression that uses unsupported syntax or functions (such as `DLookUp`), or cannot produce a value of the required type, refuses the write before mutation. `GenGUID()` generates a GUID and `CurrentUser()` returns `Admin` for the supported session without workgroup authentication. Table-level validation rules are enforced against complete candidate rows, including unchanged fields on updates. Null rule results are accepted; false or unevaluable rules refuse the write.

### Insert rows — generic POCO

```csharp
public class Contact
{
    public int ContactID { get; set; }
    public string Name { get; set; }
    public string Email { get; set; }
    public decimal Score { get; set; }
}

await writer.InsertRowAsync("Contacts", new Contact { ContactID = 1, Name = "Alice", Email = "alice@example.com", Score = 95.5m });

await writer.InsertRowsAsync("Contacts", new[]
{
    new Contact { ContactID = 2, Name = "Bob",   Email = "bob@example.com",   Score = 88.0m },
    new Contact { ContactID = 3, Name = "Carol", Email = "carol@example.com", Score = 92.3m },
});
```

### Insert rows — object array

```csharp
await writer.InsertRowAsync("Contacts", new object[] { 4, "Dave", "dave@example.com", 77.1m });

await writer.InsertRowsAsync("Contacts", new[]
{
    new object[] { 5, "Eve",   "eve@example.com",   91.0m },
    new object[] { 6, "Frank", "frank@example.com", 85.4m },
});
```

### Insert rows — named columns (`RowValues`)

Positional object arrays match columns by position, so a reordered array silently
corrupts data. `RowValues` matches by **column name** (case-insensitive) instead.
Omitted columns take the column's default value, or database null when it has none
(an AutoNumber column still generates its next value), so the order you assign
columns is irrelevant:

```csharp
await writer.InsertRowAsync("Contacts", new RowValues
{
    ["Email"] = "grace@example.com",
    ["Name"]  = "Grace",
    ["ContactID"] = 7,
    // Score omitted -> its default value, or null when it has none
});

// Fluent form and bulk insert
await writer.InsertRowsAsync("Contacts", new[]
{
    RowValues.Create().Set("ContactID", 8).Set("Name", "Heidi"),
    RowValues.Create().Set("ContactID", 9).Set("Name", "Ivan"),
});
```

#### Defaults and explicit NULL

Inserts follow Access SQL: a value you supply is stored as given, so `null` and `DBNull.Value` store NULL even in a column that has a default, and a NOT NULL column rejects them. A column the insert leaves out gets its default: leave it out of a `RowValues` row or a POCO, or pass `DbDefault.Value` in any insert.

```csharp
await writer.CreateTableAsync("Scores", new[]
{
    new ColumnDefinition("Id",    typeof(int)) { IsAutoIncrement = true },
    new ColumnDefinition("Score", typeof(int)) { DefaultValue = 0 },
});

await writer.InsertRowAsync("Scores", new object?[] { null, null });            // Id 1, Score NULL
await writer.InsertRowAsync("Scores", new object?[] { null, DbDefault.Value }); // Id 2, Score 0
await writer.InsertRowAsync("Scores", new RowValues { ["Score"] = 5 });        // Id 3, Score 5
await writer.InsertRowAsync("Scores", new RowValues { ["Id"] = null });        // Id 4, Score 0
```

An AutoNumber column generates its next value for `null`, `DBNull.Value` and `DbDefault.Value` alike, and a calculated column is computed when the insert leaves it out or passes any of the three; a non-null value you pass for a calculated column is stored as given. A POCO property whose value is `null` stores NULL; to take the default instead, leave the property off the type or mark it `[NotMapped]`. A non-nullable numeric property mapped to an AutoNumber column, such as `int Id`, counts as not supplied while it holds 0, so the column generates its next value, as Entity Framework treats a generated key; any other value is stored as given, and an `int?` property generates for `null`. To store 0 itself, insert the row as `RowValues` or `object[]`. Updates never apply defaults, and `UpdateRowsAsync` rejects `DbDefault.Value`.

### Update & delete

Single-column convenience overloads cover the common case:

```csharp
int updated = await writer.UpdateRowsAsync("Contacts", "ContactID", 1,
    new Dictionary<string, object?> { ["Score"] = 99.9m });

int deleted = await writer.DeleteRowsAsync("Contacts", "ContactID", 3);
```

For multi-column, range, and set filters, build a `RowCriteria` from one or more
`ColumnPredicate` conditions combined with logical AND. The new values are supplied
as a `RowValues`:

```csharp
// WHERE Region = 'West' AND Score > 80
int promoted = await writer.UpdateRowsAsync(
    "Contacts",
    RowCriteria.Where("Region", "West").And(ColumnPredicate.GreaterThan("Score", 80m)),
    new RowValues { ["Tier"] = "Gold" });

// DELETE WHERE Score BETWEEN 50 AND 90
await writer.DeleteRowsAsync("Contacts",
    RowCriteria.Where(ColumnPredicate.Between("Score", 50m, 90m)));

// DELETE WHERE ContactID IN (1, 3, 5) AND Region IS NULL
await writer.DeleteRowsAsync("Contacts", new RowCriteria
{
    ColumnPredicate.In("ContactID", 1, 3, 5),
    ColumnPredicate.IsNull("Region"),
});
```

`ColumnPredicate` supports `EqualTo`, `NotEqualTo`, `GreaterThan`,
`GreaterThanOrEqual`, `LessThan`, `LessThanOrEqual`, `Between`, `In`, `IsNull`, and
`IsNotNull`. Ordered comparisons coerce the operand to the column's runtime type, so
passing an `int` operand against a `decimal` column compares correctly.

> By default, update and delete are logical row mutations, not secure erase operations. Old row payload bytes and external LVAL payload pages can remain in the file unless secure erase is enabled.

### Storage maintenance and secure erase

```csharp
await using var writer = await AccessWriter.OpenAsync(
    "database.accdb",
    new AccessWriterOptions
    {
        SecureEraseMode = SecureEraseMode.DeletedRowsAndFreedPages,
    });

await writer.DeleteRowsAsync("Contacts", "ContactID", 3);
int scrubbed = await writer.ScrubFreePagesAsync();
long truncated = await writer.ShrinkDatabaseAsync();
```

`SecureEraseMode.DeletedRowsAndFreedPages` overwrites deleted row bodies and the deleted rows' MEMO/OLE LVAL data. Access can pack several long values onto one LVAL page, so an LVAL page returns to the Access global free list, overwritten, only when no other live value is left on it; otherwise only the deleted value's rows on it are overwritten and marked deleted. `ScrubFreePagesAsync` overwrites pages already on the free list. `ShrinkDatabaseAsync` truncates free pages from the physical end of the file; it does not renumber live pages or perform a full Access Compact & Repair rebuild. File-backed shrinking keeps its raw snapshot and truncation intent in the adjacent recovery journal until its result is committed. After a process crash, preserve that journal and reopen with `AccessWriter` to finish recovery.

### Add, drop, and rename columns

```csharp
// Append a new column. Existing rows receive DBNull for the new column.
await writer.AddColumnAsync("Contacts", new ColumnDefinition("Phone", typeof(string), maxLength: 32));

// Rename an existing column. Row data is preserved.
await writer.RenameColumnAsync("Contacts", "Score", "Rating");

// Drop a column. Its data is permanently lost.
await writer.DropColumnAsync("Contacts", "Phone");
```

> These operations rewrite the whole table (copy rows to a new schema, then swap the catalog entry). Cost scales with row count. A rename may change only the letter case of a name (`Score` to `SCORE`), and is then handled like any other rename, as described below. A new name that another column of the table has, in any case, throws `InvalidOperationException`, and renaming a column to the name it already has, spelled as stored, changes nothing and does not rewrite the table. Access is believed to allow the case-only rename and to treat the unchanged name as no change, but neither has been checked against it; see the remaining schema requirements in [docs/todo.md](todo.md#s--hostile-file-parsing-and-schema-preservation). Every column keeps its type, size, precision and scale, Unicode compression and calculated expression, and indexes and foreign-key relationships follow the rebuilt table: its relationship index entries are re-created and the related tables are re-linked to it, and renaming a key column updates `MSysRelationships`. The table's persisted `MSysObjects.LvProp` properties are kept as stored, including those the writer does not model: a column's `Caption`, `Format`, `AllowZeroLength` or `AppendOnly`, and the table-level `Description`, `Filter`, `OrderBy`, `ValidationRule` and `SubdatasheetName`. A dropped column's properties go with it, a renamed column's move to its new name, an added column gets the properties its `ColumnDefinition` declares, and the table-level `NameMap` is dropped. Renaming a column also renames it in every calculated expression, validation rule and default value expression of the table that names it: `[Score]` or a bare `Score` becomes `[Rating]`, so they keep evaluating. A reference qualified by the table's own name keeps the qualifier: `[Contacts].[Score]` becomes `[Contacts].[Rating]`, and `Contacts.Score` becomes `Contacts.[Rating]`. Supported same-table qualifiers in calculated fields are evaluated before and after rename. Table-level `ValidationRule` references are also rewritten on rename, and dropping a field that the rule names is refused. Rename and drop throw `JetOperationException` with `FeatureNotSupported` before writing when the table has a nonempty `Filter` or `OrderBy`, including when the changed column appears unrelated; adds preserve these properties. Native dependency rewriting, lookup and saved-query handling remain open under S9 in [the TODO](todo.md#s--hostile-file-parsing-and-schema-preservation). Dropping a column that a relationship uses as a key column throws `InvalidOperationException`; drop the relationship first. Dropping a column that another column's calculated expression, validation rule or default value expression names throws `InvalidOperationException` too, naming that column and its expression; change or drop that column first. Dropping a column also drops the indexes it is a key column of; every other index is carried over with its settings. A table with an index the writer cannot maintain, such as one that names a column the table does not have or lies past the end of a damaged table definition, makes all three operations throw `JetLimitationException` before anything is written, rather than lose that index.

### Linked tables

Linked tables are catalog-only entries that point at data living in another source. The library can create and enumerate Access, ODBC, and text linked-table entries. Managed reads follow Access-file links and supported delimited text/CSV links through the linked-source path policy; ODBC links are metadata-only. Text links currently materialize delimited fields as string columns and support `HDR=YES/NO`, `FMT=Delimited`, `FMT=CSVDelimited`, `FMT=TabDelimited`, and `FMT=Delimited(<char>)`. Ragged linked-text rows are normalized to the resolved column set: missing fields become empty strings and extra fields are ignored by row materialization. Header names are normalized; linked-text values follow DAO text-driver trimming by removing leading spaces outside quoted fields and trailing spaces after CSV unescaping. ODBC links write a parseable `MSysObjects.LvProp` property block; supply remote source columns when you want a generated linked-schema cache, or supply an Access/DAO-authored `LvProp` payload when you need byte-for-byte engine-authored metadata. Access/DAO-authored payloads are the source-of-truth fixture bytes; generated writer payloads are subjects under test, not oracles.
Table aliases and catalog identities must be unique. Access/text link IDs and
negative ODBC IDs identify catalog objects, not physical TDEF pages. Access/text
links require a source path and foreign table/file name: strict reads reject
incomplete entries, and lenient reads omit them. Native ODBC entries can expose
cached schema without connection metadata. Missing required catalog columns are
`CorruptCatalog` errors in every mode, and storage I/O failures propagate.

Linked reads interpret stored Windows path separators before authorization and reject paths that escape the approved source directory or cross a symlink/reparse point. Supply canonical filesystem paths for trusted host and allowlist roots; for example, macOS callers whose temporary directory is reached through `/var` should use its resolved `/private/var` path. A path callback does not permit symlink crossings. For text links, the callback must approve both the source directory and the final text file. `LinkedTextMaxSourceFileBytes` checks the opened file length and limits consumption even if the file grows; one excess byte may be read to detect the limit. Authorization is still path-based: opened-handle identity, hard links and path-swap races remain open requirements under S6.

```csharp
// Linked Access table (MSysObjects type 6) — references a table in another .mdb / .accdb file.
await writer.CreateLinkedTableAsync(
    linkedTableName:    "RemoteOrders",
    sourceDatabasePath: @"C:\Data\Backend.accdb",
    foreignTableName:   "Orders");

// Linked ODBC table (MSysObjects type 4) — references a table over an ODBC connection.
// The "ODBC;" prefix is added automatically when omitted.
await writer.CreateLinkedOdbcTableAsync(
    linkedTableName:  "LinkedSalesOrders",
    connectionString: "ODBC;DRIVER={SQL Server};SERVER=db.example.com;DATABASE=Sales;Trusted_Connection=Yes",
    foreignTableName: "dbo.Orders");

// Generated ODBC schema cache: provide the source table shape so the writer can
// create table and column property targets in MSysObjects.LvProp.
await writer.CreateLinkedOdbcTableAsync(
    linkedTableName:  "LinkedSalesOrdersWithCache",
    connectionString: "ODBC;DRIVER={SQL Server};SERVER=db.example.com;DATABASE=Sales;Trusted_Connection=Yes",
    foreignTableName: "dbo.Orders",
    sourceColumns:
    [
        new ColumnDefinition("OrderId", typeof(int)) { IsPrimaryKey = true, IsNullable = false },
        new ColumnDefinition("CustomerName", typeof(string), maxLength: 100),
        new ColumnDefinition("Total", typeof(decimal)) { NumericPrecision = 18, NumericScale = 2 },
    ]);

// Advanced ODBC path: supply an Access/DAO-authored cached-schema LvProp payload
// when you need Access/DAO-compatible catalog metadata for the linked source.
await writer.CreateLinkedOdbcTableAsync(
    linkedTableName:     "LinkedSalesOrdersCached",
    connectionString:    "ODBC;DRIVER={SQL Server};SERVER=db.example.com;DATABASE=Sales;Trusted_Connection=Yes",
    foreignTableName:    "dbo.Orders",
    cachedSchemaLvProp:  cachedSchemaLvPropBytes);

// Linked CSV table (MSysObjects type 6) — reads rows from the text file on demand.
await writer.CreateLinkedTextTableAsync(
    linkedTableName:      "LinkedOrdersCsv",
    sourceDirectoryPath:  @"C:\Data\Exports",
    foreignFileName:      "orders.csv",
    connectString:        "Text;HDR=YES;FMT=Delimited");

DataTable csvRows = await reader.ReadTableAsync("LinkedOrdersCsv", cancellationToken: cancellationToken);
```

> ODBC links remain metadata-only. Fixed-width text links and schema.ini type inference are not part of the managed text reader. Use `ListLinkedTablesAsync()` to enumerate linked entries and inspect their `Kind`, `ConnectString`, `SourcePath`, and `SourceObjectName` metadata.

### Foreign-key relationships

Declare a relationship between two existing tables. The library appends one row per FK column to the `MSysRelationships` catalog (which Microsoft Access reads to populate the Relationships designer) and emits the matching per-TDEF foreign-key logical-index entries on both sides so the relationship is visible to readers immediately. On Jet3 (Access 97) files the entries take the layout Access 97 writes.

`DropRelationshipAsync` and `RenameRelationshipAsync` rewrite `MSysRelationships` as live rows, update or remove the TDEF entries on every format, and leave Type=8 relationship rows in `MSysObjects` for Microsoft Access Compact & Repair to normalize from the canonical relationship rows.

A table in an enforced relationship with another table cannot be dropped: `DropTableAsync` refuses the operation with `JetOperationException` before mutation. Unenforced relationships and self relationships are removed atomically with the table, matching DAO. Drop enforced relationships first; `AccessReader.ListRelationshipsAsync` lists them:

```csharp
foreach (RelationshipMetadata rel in await reader.ListRelationshipsAsync())
{
    if (rel.PrimaryTable == "Orders" || rel.ForeignTable == "Orders")
    {
        await writer.DropRelationshipAsync(rel.Name);
    }
}

await writer.DropTableAsync("Orders");
```

**Runtime referential integrity is enforced on `InsertRowAsync` / `UpdateRowsAsync` / `DeleteRowsAsync`** for any relationship created with `EnforceReferentialIntegrity = true` (the default); `CascadeUpdates` and `CascadeDeletes` honour the cascade flags. See the Limitations section for caveats.

An insert must give each non-null foreign key a value the primary table has; a null foreign key is never checked. An update is checked, as in Access, only on the rows whose foreign key it changes: assigning other columns, or assigning a key the value it already has, does not check the key, so such an update succeeds even on a row whose key has no parent. Changing a key to a value with no parent row throws `InvalidOperationException` before any row is written.

When other rows reference a key, deleting its row deletes them if the relationship cascades deletes, and changing the key updates them if it cascades updates; otherwise the delete or update throws `InvalidOperationException`. A delete finds and checks every dependent row, through every level of cascades, before it deletes any row, so a refused delete changes nothing. A key update likewise finds the dependent rows of every relationship whose key it changes, checks that every row it rewrites still fits on a data page, and checks the table's own unique indexes, before it writes any row, so a refused key update changes nothing, even when another relationship would cascade the change; a dependent row that several relationships reach is rewritten once, with every new key. A cascaded key that would duplicate a key in a unique index of the child table, which a child row left without a parent row can cause, is still refused only after the child rows are prepared, and a delete or key update that cascades into a table whose indexes the writer cannot maintain throws `JetLimitationException` (see [Error Handling](operations.md#error-handling)). These failures roll back the entire call, including earlier child changes; an explicit transaction keeps its earlier successful calls.

An enforced relationship whose table or key column cannot be found, which Access never leaves behind but another tool or a damaged file can, is not skipped: every write it would have to check throws `InvalidOperationException` naming the relationship and what is missing, before any row is written, even when another relationship would cascade first. Writes it does not constrain still succeed: an update that leaves the key alone, a delete that matches no row, and an insert with a null foreign key, unless the missing object is the foreign-key column itself, which makes every insert into the table throw. `DropRelationshipAsync` removes such a relationship.

```csharp
// Single-column FK
await writer.CreateRelationshipAsync(new RelationshipDefinition(
    name:           "FK_Orders_Customers",
    primaryTable:   "Customers",   // PK side  — szReferencedObject
    primaryColumn:  "CustomerID",
    foreignTable:   "Orders",      // FK side  — szObject
    foreignColumn:  "CustomerID")
{
    EnforceReferentialIntegrity = true,   // default
    CascadeUpdates              = false,
    CascadeDeletes              = false,
});

// Multi-column FK
await writer.CreateRelationshipAsync(new RelationshipDefinition(
    name:           "FK_OrderItems_Orders",
    primaryTable:   "Orders",
    primaryColumns: new[] { "OrderID", "Region" },
    foreignTable:   "OrderItems",
    foreignColumns: new[] { "OrderID", "Region" }));
```

> Requires a database that contains the `MSysRelationships` catalog table. ACCDB databases created by `AccessWriter.CreateDatabaseAsync` include it. `CreateRelationshipAsync` reports `SystemTableMissing` when the table is absent. Access-authored and DAO-authored databases provide the interoperability evidence; library round trips validate internal consistency.
