# JetDatabaseWriter

[![NuGet](https://img.shields.io/nuget/v/JetDatabaseWriter.svg)](https://www.nuget.org/packages/JetDatabaseWriter/)
[![Downloads](https://img.shields.io/nuget/dt/JetDatabaseWriter.svg)](https://www.nuget.org/packages/JetDatabaseWriter/)
[![Targets](https://img.shields.io/badge/targets-net10.0%20%7C%20netstandard2.1-blue)](#nuget-target-compatibility)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Fully managed .NET library for reading and writing Microsoft Access (JET/ACE) databases — no OleDB, ODBC, or ACE/Jet driver installation required.

Use JetDatabaseWriter when you need to query, migrate, or generate `.mdb` and `.accdb` files from .NET without relying on native Access drivers or a local Access installation.

## Contents

- [Installation](#installation)
- [Features](#features)
- [Quick Start](#quick-start)
- [Reading Data](#reading-data)
- [Writing Data](#writing-data)
- [Encryption Support](#encryption-support)
- [Limitations](#limitations)

## At a Glance

- Best fit for .NET applications and tools that need direct file-level access to Access databases.
- Not a fit if you need a SQL engine, an ODBC driver, or full Access application features like forms, reports, macros, or VBA.
- Access compatibility is ensured by extensive automated coverage, including DAO validation.

## Features

| | |
|---|---|
| ✅ **Pure managed .NET** | No OleDB, ODBC, or ACE/Jet driver — runs anywhere .NET runs |
| ✅ **All Access versions** | Jet3 / Jet4 / ACE — Access 97 through Microsoft 365 (`.mdb` / `.accdb`) |
| ✅ **Read & write** | Create databases and tables; insert/update/delete rows; add/drop/rename columns |
| ✅ **Typed values** | `int`, `DateTime`, `decimal`, `Guid`, MEMO, OLE, Hyperlink — not just strings |
| ✅ **POCO + LINQ** | `Rows<T>("...", o => …)` auto-infers an index; `FromIndex<T>(...)` to override; async LINQ (`Where`/`Take`/`FirstOrDefaultAsync`/…) over `IAsyncEnumerable<T>` |
| ✅ **IQueryable** | `Query<T>(...)` is an `IQueryable<T>` with `Where`/`OrderBy`/`Skip`/`Take`/`Include` (relationship-inferred eager load) and async terminals (`ToListAsync`/`CountAsync`/…) |
| ✅ **Async-first** | `ValueTask<T>` API, `OpenAsync(...)`, `await using` (`IAsyncDisposable`), `IProgress<T>` callbacks |
| ✅ **Stream-based I/O** | Open from any seekable `Stream` (files, byte arrays, blobs, embedded resources) |
| ✅ **Encryption** | Jet3 XOR, Jet4 RC4, ACCDB legacy / AES-128 / Standard (Office 2007) / Agile (Office 2010+) — all read/write |
| ✅ **Schema features** | Indexes, primary & foreign keys with referential integrity (cascade update/delete), linked tables (Access-file read-through plus ODBC/text catalog entries) |
| ✅ **Complex columns** | Read/write attachments and multi-value columns (ACCDB) |
| ✅ **Calculated columns** | ACCDB expression-column metadata, cached values, and a row-local expression evaluator |
| ✅ **Concurrency** | `.ldb` / `.laccdb` lockfile + BCL page-level byte-range locks where supported |
| ✅ **Transactions** | `BeginTransactionAsync()` page-buffered `CommitAsync` / `RollbackAsync` via in-memory journal |
| ✅ **Storage maintenance** | Access-style free-page reuse, free-page scrubbing, opt-in secure erase, and tail shrinking |
| ✅ **Performance** | Configurable LRU page cache, default parallel read-ahead for eligible page scans, streams millions of rows without loading the file |

---

### Security

> **✅ All 36 known relevant CVEs have been addressed.** Every identified attack surface — CFB/OLE compound file parsing, JET/ACE MDB/ACCDB format, and Office Agile Encryption — is mitigated in code and covered by regression tests. See [docs/cve-vulnerability-analysis.md](docs/cve-vulnerability-analysis.md) for the full threat model and test inventory.

---

### Correctness

The test suite draws from and extends the coverage of [Jackcess](https://jackcess.sourceforge.io/), [mdbtools](https://github.com/mdbtools/mdbtools), [OpenMcdf](https://github.com/ironfede/openmcdf), and Microsoft's [Extensible Storage Engine](https://github.com/microsoft/Extensible-Storage-Engine) for analogous storage-engine risk categories, with additional coverage for corner cases, corruption resilience, and format variants.

For a compact map of writer-created disk-format surfaces and their strongest DAO OpenRecordset / CompactDatabase validation signals, see the [writer disk-format validation matrix](docs/design/writer-disk-format-validation-matrix.md).

Beyond functional tests, the codebase is validated by:

- **Strict compiler settings** — nullable reference types, warnings-as-errors, `WarningLevel 9999`, `AnalysisLevel latest-all`, and arithmetic overflow checking enabled globally
- **Static analysis** — Roslyn .NET analyzers, Roslynator, StyleCop, and `dotnet format` enforced in CI
- **Reproducible builds** — deterministic compilation via [DotNet.ReproducibleBuilds](https://github.com/dotnet/reproducible-builds); identical source always produces identical binaries
- **Access Compact & Repair round-trips** — writer-created tables, indexes, relationships, password-protected ACCDB output, and Northwind-hosted attachment/multi-value complex columns with chained-LVAL payloads are validated on Access-equipped Windows hosts.
- **Index key fixture parity** — long text/MEMO index keys with embedded line breaks are validated against Access-authored fixtures: Jet4 (V2000 / V2003 / V2007) is byte-exact, and V2010 ACE is byte-exact for the checked-in Access-authored `Table11` / `Table11_desc` long rows. The V2010 encoder also includes the DAO-derived 65-character contribution tables for the plain, auxiliary, row10, row11, and row12 long-row suffix contexts, with probe validation showing zero mismatches across the exported matrices and observed double-space sweeps. See [GeneralLegacyEncoderFixtureTests.cs](JetDatabaseWriter.Tests/Indexes/Collation/GeneralLegacyEncoderFixtureTests.cs), [GeneralEncoderFixtureTests.cs](JetDatabaseWriter.Tests/Indexes/Collation/GeneralEncoderFixtureTests.cs), and [format-probe notes](docs/format-probe/format-probe-long-row-index-encoding.md).
- **Fuzz testing** — random byte mutations and truncation matrices at every page boundary
- **Memory safety analysis** — control-flow and resource-leak detection via [InferSharp](https://github.com/microsoft/infersharp)

---

## Quick Start

### Installation

```bash
dotnet add package JetDatabaseWriter
```

```powershell
Install-Package JetDatabaseWriter
```

### Usage

```csharp
using JetDatabaseWriter;
using System.Threading;

public class Order
{
    public int OrderID { get; set; }
    public DateTime OrderDate { get; set; }
    public decimal Freight { get; set; }
}

await using var reader = await AccessReader.OpenAsync("database.mdb");

IReadOnlyList<string> tables = await reader.ListTablesAsync();
Console.WriteLine($"Found {tables.Count} tables: {string.Join(", ", tables)}");

IReadOnlyList<Order> orders = await reader.ReadTableAsync<Order>("Orders", maxRows: 100);
foreach (Order o in orders)
    Console.WriteLine($"#{o.OrderID}  {o.OrderDate:yyyy-MM-dd}  {o.Freight:C}");
```

---

## Opening a Reader or Writer

### From a file path

```csharp
await using var reader = await AccessReader.OpenAsync("database.mdb", cancellationToken: cts.Token);
await using var writer = await AccessWriter.OpenAsync("database.mdb");
```

### From a Stream

Both `AccessReader` and `AccessWriter` accept any seekable `Stream` — useful for byte arrays, Azure Blob Storage, embedded resources, or HTTP downloads.

```csharp
byte[] bytes = await File.ReadAllBytesAsync("database.mdb");
var ms = new MemoryStream(bytes);
await using var reader = await AccessReader.OpenAsync(ms);
```

By default the stream is disposed with the reader/writer. Pass `leaveOpen: true` to retain ownership:

```csharp
var ms = new MemoryStream(File.ReadAllBytes("template.mdb"));
await using (var writer = await AccessWriter.OpenAsync(ms, leaveOpen: true))
{
    await writer.InsertRowAsync("Orders", new object[] { 1, "Widget", 9.99m });
}

byte[] modified = ms.ToArray();
```

> The stream must be readable and seekable. For `AccessWriter`, it must also be writable.

---

## Reading Data

### Generic POCO mapping — recommended

```csharp
public class Product
{
    public int ProductID { get; set; }
    public string ProductName { get; set; }
    public decimal UnitPrice { get; set; }
    public bool Discontinued { get; set; }
}

IReadOnlyList<Product> products = await reader.ReadTableAsync<Product>("Products", maxRows: 100, cancellationToken);
decimal total = products.Where(p => !p.Discontinued).Sum(p => p.UnitPrice);
```

Property names are matched to column headers **case-insensitively**. Unmatched properties keep their default value. The type `T` must be a class with a parameterless constructor.

### Typed DataTable

```csharp
DataTable dt = await reader.ReadTableAsync("Products", cancellationToken: cancellationToken);
// dt.Columns["ProductID"].DataType    == typeof(int)
// dt.Columns["UnitPrice"].DataType    == typeof(decimal)
// dt.Columns["Discontinued"].DataType == typeof(bool)
```

`ReadTableAsync`, `ReadAllTablesAsync`, and the string-typed DataTable APIs fully materialize their results. They are convenient for data binding, previews, exports, and compatibility code; for bulk processing or large-table scans, prefer `Rows(...)` or `Rows<T>(...)` so rows stream lazily and can short-circuit through async LINQ. `ReadDataTableAsync` remains available as a compatibility alias for `ReadTableAsync`.

### Column metadata

```csharp
IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync("Products", cancellationToken);
foreach (ColumnMetadata col in meta)
{
    Type   clrType = col.ClrType;         // e.g. typeof(int), typeof(string)
    string display = col.Size.ToString(); // e.g. "4 bytes", "255 chars", "LVAL"
    Console.WriteLine($"{col.Name}: {clrType.Name} ({col.Size})");
}
```

### Date/Time Extended

Access 2019+ Date/Time Extended columns decode to `DateTime` with `DateTimeKind.Unspecified`, preserving the 100 ns tick precision stored in the 42-byte Access payload. Metadata reports `TypeName == "Date/Time Extended"` and `ClrType == typeof(DateTime)`; `Rows(...)`, `Rows<T>(...)`, and `ReadTableAsync` surface row values as `DateTime`.

Classic `Date/Time` remains the default for `new ColumnDefinition("When", typeof(DateTime))`. To author a Date/Time Extended column in an ACCDB database, opt in explicitly:

```csharp
await writer.CreateTableAsync("Events",
[
    new ColumnDefinition("Id", typeof(int)),
    new ColumnDefinition("ObservedAt", typeof(DateTime)) { IsDateTimeExtended = true },
]);
```

### Index metadata

`ListIndexesAsync` returns the logical indexes declared on a table — primary keys, foreign-key indexes, and ordinary user indexes — parsed directly from the TDEF page chain.

```csharp
IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("Companies", cancellationToken);
foreach (IndexMetadata idx in indexes)
{
    string keys = string.Join(", ", idx.Columns.Select(c => c.Name));
    Console.WriteLine($"{idx.Name}: {idx.Kind} on ({keys})  unique={idx.EnforcesUniqueness}  rawUniqueFlag={idx.HasUniqueFlag}  fk={idx.IsForeignKey}");
}
```

Multiple logical indexes can share the same physical index — consult `IndexMetadata.RealIndexNumber` to detect that sharing. The `IndexKind` enum distinguishes `Normal`, `PrimaryKey`, and `ForeignKey`. Use `IndexMetadata.EnforcesUniqueness` for semantic uniqueness and `IndexMetadata.HasUniqueFlag` when you need the raw real-index `flags & 0x01` bit. Access does not always set that flag on primary keys because uniqueness is implied by `Kind == PrimaryKey`.

### Index-backed reads

Pass a predicate to `Rows<T>(...)` and the reader **infers the index automatically**: a leading-key equality (optionally terminated by one range) is satisfied by a Jet4/ACE index seek, and anything else transparently falls back to a table scan. Inference never changes the result set — only how the rows are found — so you write the obvious query and let the reader optimize it:

```csharp
await foreach (Order order in reader.Rows<Order>("Orders", o => o.OrderDate >= start && o.OrderDate < end))
    Console.WriteLine(order.OrderId);
```

Only conjuncts combined with `&&` over direct column members (`o.Column == value`, `o.Column > value`, …) drive inference; method calls, `||`, computed members such as `o.When.Year`, and column-to-column comparisons are evaluated client-side. Jet3 `.mdb` files always scan.

`FromIndex(...)` is the explicit override — reach for it to **force a specific index**, guarantee **index-ordered** streaming, or seek shapes the inferrer does not model. With no predicate it streams rows in index order; `WhereEquals(...)` performs a complete-key equality seek; `WhereKeyPrefix(...)` filters by leading composite-key columns; `WhereBetween(...)` / `WhereRange(...)` walk bounded key ranges. The typed overload maps rows through the same POCO mapper as `Rows<T>(...)`.

```csharp
await foreach (Company company in reader
    .FromIndex<Company>("Companies", "IX_CompanyName")
    .WhereEquals("Contoso")
    .ToRowsAsync(cancellationToken))
{
    Console.WriteLine(company.Name);
}

await foreach (Order order in reader
    .FromIndex<Order>("Orders", "IX_OrderDate")
    .WhereBetween(startDate, endDate, lowerInclusive: true, upperInclusive: false)
    .ToRowsAsync(cancellationToken))
{
    Console.WriteLine(order.OrderId);
}
```

`SeekRowsAsync` remains the exact-key compatibility surface by table name, index name, and full key tuple, returning the same typed `object[]` row shape as `Rows(...)`. It supports unique and non-unique indexes, including composite keys; use `FromIndex(...)` for range or leading-key-prefix scans.

```csharp
await foreach (object[] row in reader.SeekRowsAsync("Companies", "IX_CompanyName", ["Contoso"], cancellationToken))
{
    Console.WriteLine(row[0]);
}
```

### Relationships and eager loading (`Include`)

`ListRelationshipsAsync` returns every foreign-key relationship from the database's `MSysRelationships` catalog. Each entry links a child (`ForeignTable`) to a parent (`PrimaryTable`) with matching key columns, plus the integrity and cascade flags.

```csharp
foreach (RelationshipMetadata rel in await reader.ListRelationshipsAsync(cancellationToken))
    Console.WriteLine($"{rel.Name}: {rel.ForeignTable}({string.Join(",", rel.ForeignColumns)}) -> {rel.PrimaryTable}({string.Join(",", rel.PrimaryColumns)})");
```

`Query<T>(...)` returns an `IQueryable<T>` and **infers relationships automatically**: pass a navigation property to `Include(...)` and the related rows are eagerly loaded and stitched onto each entity, with the relationship resolved from the catalog — no mapping configuration. Compose the standard LINQ operators (`Where`, `OrderBy`/`ThenBy`, `Skip`, `Take`) and execute with the async terminals (`ToListAsync`, `FirstOrDefaultAsync`, `SingleOrDefaultAsync`, `CountAsync`, `AnyAsync`) or `AsAsyncEnumerable()` for `await foreach`. `Where` drives the same index inference as `Rows<T>(table, predicate)`; ordering and paging run in memory after the filtered set is read.

```csharp
// Parent with its children (collection navigation), filtered, ordered and paged
List<Customer> customers = await reader.Query<Customer>("Customers")
    .Where(c => c.Region == "West")
    .OrderBy(c => c.Name)
    .Skip(20)
    .Take(10)
    .Include(c => c.Orders)
    .ToListAsync(cancellationToken);

// Child with its parent (reference navigation)
Order? order = await reader.Query<Order>("Orders")
    .Where(o => o.OrderId == 10248)
    .Include(o => o.Customer)
    .FirstOrDefaultAsync(cancellationToken);
```

The navigation's target type is matched to the related table by name, so the entity classes the [scaffolder](#scaffolding--generate-c-models-from-a-database) generates work as-is. When the join columns are indexed (a primary key or foreign-key index, inferred automatically) each `Include` loads the related rows with one index seek per distinct key; otherwise it scans the related table once and joins in memory. Operators outside the supported set throw `NotSupportedException` — materialize with `ToListAsync(...)` and use LINQ-to-Objects for anything further. Relationships are read from `MSysRelationships`, so this requires a full-catalog database (Jet3 `.mdb` files have no relationship catalog).

### Complex (Attachment / Multi-value) column metadata

`GetComplexColumnsAsync` joins the parent TDEF column descriptors with `MSysComplexColumns` to expose the per-column `ComplexID`, the parent table object/TDEF id, the hidden flat child-table name, and the column subtype (Attachment, MultiValue, or VersionHistory).

```csharp
IReadOnlyList<ComplexColumnInfo> complex = await reader.GetComplexColumnsAsync("Documents", cancellationToken);
foreach (ComplexColumnInfo c in complex)
{
    Console.WriteLine($"{c.ColumnName}: {c.Kind}  flat={c.FlatTableName}  template={c.ComplexTypeName}");
}
```

Returns an empty list for tables without complex columns and for older Jet3 / Jet4 (`.mdb`) files.

#### Reading and writing complex column rows

For ACE `.accdb` files, attachments and multi-value items can be inserted into an existing parent row and read back via spec-compliant APIs:

```csharp
// Insert an attachment into the row whose Id = 1
await writer.AddAttachmentAsync(
    "Documents",
    "Files",
    new Dictionary<string, object?> { ["Id"] = 1 },
    new AttachmentInput("notes.txt", File.ReadAllBytes("notes.txt")),
    cancellationToken);

// Insert a multi-value tag item
await writer.AddMultiValueItemAsync("Tags", "Items", new Dictionary<string, object?> { ["Id"] = 1 }, "red", cancellationToken);

// Read back
IReadOnlyList<AttachmentRecord> attachments = await reader.GetAttachmentsAsync("Documents", "Files", cancellationToken);
IReadOnlyList<MultiValueItem> tags = await reader.GetMultiValueItemsAsync("Tags", "Items", cancellationToken);
```

The parent-row predicate must match exactly one row (zero or multiple matches throw `InvalidOperationException`). Attachment payloads are wrapper-encoded on disk (4-byte typeFlag + dataLen + extension + payload, with raw-deflate compression skipped for already-compressed extensions). Payloads larger than the 256-byte inline-OLE cap are pushed onto freshly allocated Access-style LVAL pages (single-page or chained form with the `LVAL` page signature) and reassembled by the reader; the upper limit is the 24-bit on-disk LVAL length field (~16 MB per file).

### Hyperlink columns

Hyperlink columns are MEMO columns whose TDEF flag byte has the `HYPERLINK_FLAG_MASK = 0x80` bit set. Microsoft Access stores the value as a single `#`-delimited string (`displaytext#address#subaddress#screentip`); the library round-trips that structure as a strongly-typed [`Hyperlink`](JetDatabaseWriter/Models/Hyperlink.cs) record.

```csharp
await writer.CreateTableAsync("Bookmarks",
[
    new("Id", typeof(int)) { IsAutoIncrement = true, IsPrimaryKey = true },
    new("Link", typeof(Hyperlink)),                 // shorthand
    // equivalent: new("Link", typeof(string)) { IsHyperlink = true }
]);

await writer.InsertRowAsync("Bookmarks", new object[]
{
    DBNull.Value,
    new Hyperlink("Docs site", "https://example.com/docs", subAddress: "intro"),
});

await using var reader = await AccessReader.OpenAsync(path);
await foreach (object[] row in reader.Rows("Bookmarks"))
{
    var link = (Hyperlink)row[1];
    Console.WriteLine($"{link.DisplayText} → {link.Address}#{link.SubAddress}");
}

ColumnMetadata col = (await reader.GetColumnMetadataAsync("Bookmarks"))[1];
// col.IsHyperlink == true
// col.ClrType     == typeof(Hyperlink)
// col.TypeName    == "Hyperlink"
```

POCO mapping accepts either a `Hyperlink` property or a plain `string` property — the conversion runs both ways. Compatibility surfaces (`RowsAsStrings`, `ReadTableAsStringsAsync`) continue to yield the raw `#`-delimited form. See [docs/design/hyperlink-format-notes.md](docs/design/hyperlink-format-notes.md) for the on-disk layout, escape semantics, and round-trip rules.

### String DataTable — compatibility

```csharp
// All values as strings
DataTable preview = await reader.ReadTableAsStringsAsync("Products", maxRows: 20, cancellationToken: cancellationToken);

// String row access
string firstCell = preview.Rows[0][0].ToString();
```

---

## Streaming Large Tables

### Generic streaming — recommended

```csharp
var progress = new Progress<long>(n => Console.Write($"\r{n:N0} rows"));

await foreach (Product p in reader.Rows<Product>("Products", progress))
    Console.WriteLine($"{p.ProductName}: {p.UnitPrice:C}");
```

### Typed object array streaming

```csharp
await foreach (object[] row in reader.Rows("BigTable"))
{
    int     id  = (int)row[0];
    decimal val = row[2] == DBNull.Value ? 0m : (decimal)row[2];
}
```

### String streaming — compatibility

```csharp
await foreach (string[] row in reader.RowsAsStrings("BigTable"))
    Console.WriteLine(string.Join(", ", row));
```

Null values in typed `object[]` rows surface as `DBNull.Value`.

---

## Querying with async LINQ

The reader exposes each table as an `IAsyncEnumerable<T>` via `Rows`, `Rows<T>`, and `RowsAsStrings`. Compose with the standard async LINQ operators (`Where`, `Take`, `Select`, `ToListAsync`, `FirstOrDefaultAsync`, `CountAsync`, …) — there is no separate query type and no terminal `Execute` call.

```csharp
// Generic POCO result — first match
Order? first = await reader.Rows<Order>("Orders")
    .Where(o => o.OrderDate.Year == 2024)
    .Take(10)
    .FirstOrDefaultAsync(ct);

// Materialize to a list
List<Order> recent = await reader.Rows<Order>("Orders")
    .Where(o => o.OrderDate.Year == 2024)
    .ToListAsync(ct);

// Object-array chain (no POCO)
int count = await reader.Rows("OrderDetails")
    .Where(row => row[3] is decimal p && p > 100m)
    .CountAsync(ct);

// String chain — useful for compatibility / CSV-style consumers
await foreach (string[] row in reader.RowsAsStrings("Orders")
    .Where(r => r[2].StartsWith("2024"))
    .Take(50)
    .WithCancellation(ct))
{
    Console.WriteLine(string.Join(", ", row));
}
```

Filtering and projection run client-side per row and require a table scan unless enumeration short-circuits — there is no SQL engine underneath. To let the reader pick an index for you, pass the predicate directly to `Rows<T>(table, predicate)` (see [Index-backed reads](#index-backed-reads)); chained `.Where(...)` over `Rows(...)` stays client-side because it receives a compiled delegate the engine can't inspect. Use `FromIndex(...)` to force a specific index or index ordering, or `SeekRowsAsync` for the older full-key exact-match object-array API.

---

## Reading All Tables

`ReadAllTablesAsync` materializes every user table in one call and accepts a `Progress<TableProgress>` callback that fires once per table:

For large databases, enumerate `ListTablesAsync()` and stream each table with `Rows(...)` or `Rows<T>(...)` unless you specifically need `DataTable` instances for every table. For string-typed `DataTable` compatibility across all tables, enumerate `ListTablesAsync()` and call `ReadTableAsStringsAsync(...)` per table.

```csharp
IReadOnlyDictionary<string, DataTable> all = await reader.ReadAllTablesAsync(
    new Progress<TableProgress>(p => Console.WriteLine($"Reading {p.TableName} ({p.TableIndex + 1}/{p.TableCount})...")),
    cancellationToken);
```

---

## Writing Data

> Supports Jet3, Jet4, and ACE formats — `.mdb` (Access 97+) or `.accdb`.

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
    new ColumnDefinition("Score",     typeof(decimal)),
});

await writer.DropTableAsync("Contacts");
```

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
| `IsNullable` | ✅ TDEF flag bit `FLAG_NULL_ALLOWED 0x02` | Restored on reopen; surfaced to readers via `ColumnMetadata.IsNullable`. |
| `IsAutoIncrement` | ✅ TDEF flag bit `FLAG_AUTO_LONG 0x04` | Supported for `byte`/`short`/`int`/`long`. Seeded from `max(existing) + 1` on first use. |
| `IsPrimaryKey` | ✅ TDEF logical-index entry with `index_type = 0x01` | Shortcut for synthesizing a PK `IndexDefinition` named `"PrimaryKey"` from one or more columns (in declaration order). Forces the PK key columns to `IsNullable = false` on the emitted TDEF. Mixing this with an explicit PK `IndexDefinition` in the same call throws `ArgumentException`. Single- and multi-column PKs both participate in live B-tree leaf maintenance (composite-key path). |
| `DefaultValue` | ⚠️ client-side only | CLR object substituted for `DBNull.Value` at insert time on the `AccessWriter` instance that declared it. For an engine-level default that Microsoft Access also honours, set `DefaultValueExpression` (it is auto-derived from `DefaultValue` when omitted). |
| `ValidationRule` | ⚠️ client-side only | A CLR `Func<>` cannot be serialized into the file. For an engine-level rule Microsoft Access also enforces, set `ValidationRuleExpression`. |
| `DefaultValueExpression` | ✅ `MSysObjects.LvProp` (`DefaultValue`) | Jet expression string (e.g. `"0"`, `"\"hi\""`, `"=Now()"`). Surfaced to readers via `ColumnMetadata.DefaultValueExpression`. Wins over `DefaultValue` for persistence. |
| `ValidationRuleExpression` | ✅ `MSysObjects.LvProp` (`ValidationRule`) | Jet expression string (e.g. `">=0 And <=100"`). Surfaced via `ColumnMetadata.ValidationRuleExpression`. |
| `ValidationText` | ✅ `MSysObjects.LvProp` (`ValidationText`) | User-facing message Access shows when `ValidationRuleExpression` rejects a value. Surfaced via `ColumnMetadata.ValidationText`. |
| `Description` | ✅ `MSysObjects.LvProp` (`Description`) | Free-text column description shown in Access Design View. Surfaced via `ColumnMetadata.Description`. Preserved across `AddColumnAsync` / `DropColumnAsync` / `RenameColumnAsync`. |

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
Omitted columns default to database null (an AutoNumber column still generates its
next value), so the order you assign columns is irrelevant:

```csharp
await writer.InsertRowAsync("Contacts", new RowValues
{
    ["Email"] = "grace@example.com",
    ["Name"]  = "Grace",
    ["ContactID"] = 7,
    // Score omitted -> stored as null
});

// Fluent form and bulk insert
await writer.InsertRowsAsync("Contacts", new[]
{
    RowValues.Create().Set("ContactID", 8).Set("Name", "Heidi"),
    RowValues.Create().Set("ContactID", 9).Set("Name", "Ivan"),
});
```

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

`SecureEraseMode.DeletedRowsAndFreedPages` overwrites deleted row bodies and old MEMO/OLE LVAL pages before returning those LVAL pages to the Access global free list. `ScrubFreePagesAsync` overwrites pages already on the free list. `ShrinkDatabaseAsync` truncates free pages from the physical end of the file; it does not renumber live pages or perform a full Access Compact & Repair rebuild.

### Add, drop, and rename columns

```csharp
// Append a new column. Existing rows receive DBNull for the new column.
await writer.AddColumnAsync("Contacts", new ColumnDefinition("Phone", typeof(string), maxLength: 32));

// Rename an existing column. Row data is preserved.
await writer.RenameColumnAsync("Contacts", "Score", "Rating");

// Drop a column. Its data is permanently lost.
await writer.DropColumnAsync("Contacts", "Phone");
```

> These operations rewrite the whole table (copy rows to a new schema, then swap the catalog entry). Cost scales with row count.

### Linked tables

Linked tables are catalog-only entries that point at data living in another source. The library can create and enumerate Access, ODBC, and text linked-table entries. Managed reads follow Access-file links and supported delimited text/CSV links through the linked-source path policy; ODBC links are metadata-only. Text links currently materialize delimited fields as string columns and support `HDR=YES/NO`, `FMT=Delimited`, `FMT=CSVDelimited`, `FMT=TabDelimited`, and `FMT=Delimited(<char>)`. Ragged linked-text rows are normalized to the resolved column set: missing fields become empty strings and extra fields are ignored by row materialization. Header names are normalized; linked-text values follow DAO text-driver trimming by removing leading spaces outside quoted fields and trailing spaces after CSV unescaping. ODBC links write a parseable `MSysObjects.LvProp` property block; supply remote source columns when you want a generated linked-schema cache, or supply an Access/DAO-authored `LvProp` payload when you need byte-for-byte engine-authored metadata. Access/DAO-authored payloads are the source-of-truth fixture bytes; generated writer payloads are subjects under test, not oracles.

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

Declare a relationship between two existing tables. The library appends one row per FK column to the `MSysRelationships` catalog (which Microsoft Access reads to populate the Relationships designer) and, on Jet4 / ACE databases, emits the matching per-TDEF foreign-key logical-index entries on both sides so the relationship is visible to readers immediately. Jet3 `.mdb` files get only the catalog rows.

`DropRelationshipAsync` and `RenameRelationshipAsync` rewrite `MSysRelationships` as live rows, update or remove the Jet4 / ACE TDEF entries, and leave Type=8 relationship rows in `MSysObjects` for Microsoft Access Compact & Repair to normalize from the canonical relationship rows.

**Runtime referential integrity is enforced on `InsertRowAsync` / `UpdateRowsAsync` / `DeleteRowsAsync`** for any relationship created with `EnforceReferentialIntegrity = true` (the default); `CascadeUpdates` and `CascadeDeletes` honour the cascade flags. See the Limitations section for caveats.

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

> Requires a database that already contains the `MSysRelationships` catalog table. Full-catalog ACCDB databases created by `AccessWriter.CreateDatabaseAsync` include it; Access-authored `.mdb` / `.accdb` files do as well. Jet/MDB writer-created outputs and slim-catalog ACCDB outputs may not, and `CreateRelationshipAsync` throws `NotSupportedException` when the table is absent. For validation, treat writer-created full-catalog databases as supported writer outputs under test; use Access-authored or DAO-authored databases as the fixture source of truth for DAO/Access compatibility.

---

## Transactions

`AccessWriter` supports explicit page-buffered transactions for multi-row/page operations. All page mutations are buffered in memory until committed or rolled back.

```csharp
await using var tx = await writer.BeginTransactionAsync();
await writer.InsertRowAsync("Contacts", new object[] { 7, "Grace", "grace@example.com", 90.0m });
await writer.UpdateRowsAsync("Contacts", "ContactID", 2, new Dictionary<string, object?> { ["Score"] = 93.5m });
await tx.CommitAsync(); // Replays all buffered pages and flushes the stream
```

If the transaction is disposed without a `CommitAsync` call (for example, because an exception unwound the scope), all buffered changes are discarded automatically. Only one transaction may be active per `AccessWriter` instance.

`CommitAsync` is not a durable write-ahead log. Once commit replay starts, pages are written directly to the target stream in page-number order, then the page-0 commit-lock byte is bumped and the stream is flushed. If the process, stream, device, or cancellation token fails after replay begins, pages already written are left in place and no recovery pass is attempted; the transaction object is marked rolled back and the exception is surfaced. WAL-style crash recovery is out of scope for the current file-format writer.

---

## Statistics & Metadata

```csharp
// Table-level stats (single catalog scan)
foreach (TableStat ts in await reader.GetTableStatsAsync(cancellationToken))
    Console.WriteLine($"{ts.Name}: {ts.RowCount:N0} rows, {ts.ColumnCount} cols");

// First table preview as a string-typed DataTable
DataTable first = await reader.ReadFirstTableAsStringsAsync(maxRows: 20, cancellationToken);
Console.WriteLine($"First table: {first.TableName}, {first.Rows.Count} rows");

DatabaseStatistics s = await reader.GetStatisticsAsync(cancellationToken);
Console.WriteLine($"Version:   {s.Version}");
Console.WriteLine($"Size:      {s.DatabaseSizeBytes / 1024 / 1024} MB");
Console.WriteLine($"Tables:    {s.TableCount}  Rows: {s.TotalRows:N0}");
Console.WriteLine($"Cache hit: {s.PageCacheHitRate}%");
```

---

## Scaffolding — Generate C# Models from a Database

The **JetDatabaseWriter.Scaffold** CLI tool reads the schema of every user table in a JET database and emits one C# entity-model source file per table.

### Usage

```bash
# Positional argument
dotnet run --project JetDatabaseWriter.Scaffold -- Northwind.mdb

# Named options
dotnet run --project JetDatabaseWriter.Scaffold -- --database Northwind.mdb --output ./Entities --namespace MyApp.Models

# Emit records with nullable reference types
dotnet run --project JetDatabaseWriter.Scaffold -- Northwind.mdb --records --nullable

# Password-protected database
dotnet run --project JetDatabaseWriter.Scaffold -- Secure.accdb --password secret
```

### Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--database` | `-d` | *(positional)* | Path to the `.mdb` or `.accdb` file |
| `--output` | `-o` | `./Models` | Output directory for generated files |
| `--namespace` | `-n` | `GeneratedModels` | Namespace for generated classes |
| `--password` | `-p` | — | Database password (for encrypted files) |
| `--records` | | `false` | Emit C# `record` types instead of `class` |
| `--nullable` | | `true` | Emit nullable reference types (`#nullable enable`) |

### Example Output

Given an `Orders` table with columns `OrderID (int)`, `OrderDate (DateTime)`, and `Freight (decimal)`, the tool generates:

```csharp
// <auto-generated>
namespace GeneratedModels;

using System;

public sealed class Orders
{
    /// <summary>Column: OrderID (Long Integer, 4 bytes).</summary>
    public int OrderID { get; set; }

    /// <summary>Column: OrderDate (DateTime, 8 bytes).</summary>
    public DateTime OrderDate { get; set; }

    /// <summary>Column: Freight (Currency, 8 bytes).</summary>
    public decimal Freight { get; set; }
}
```

Table and column names are automatically converted to PascalCase C# identifiers — spaces, hyphens, and special characters are cleaned, and C# keywords are escaped.

When the database declares foreign-key relationships, each generated entity also gets **navigation properties** inferred from `MSysRelationships`: a reference to the parent (named after the foreign-key column, EF-style — `CustomerID` → `Customer`) and a collection of children (named after the child table). These pair directly with `reader.Query<T>(...).Include(...)`:

```csharp
public sealed class Customers
{
    public int CustomerID { get; set; }
    /// <summary>Navigation: related Orders children.</summary>
    public ICollection<Orders> Orders { get; set; } = new List<Orders>();
}

public sealed class Orders
{
    public int OrderID { get; set; }
    public int CustomerID { get; set; }
    /// <summary>Navigation: the related Customers (parent).</summary>
    public Customers? Customer { get; set; }
}
```

---

## Configuration

```csharp
var options = new AccessReaderOptions("secretPassword")
{
    PageCacheSize            = 512,    // pages in LRU cache (default: 256)
    PageReadOptimizationMode = PageReadOptimizationMode.Auto, // random-access/read-ahead policy (default)
    DiagnosticsEnabled       = false,  // verbose logging (default: false)
    ValidateOnOpen           = true,   // format check on open (default: true)
    FileAccess               = FileAccess.Read,        // default
    FileShare                = FileShare.ReadWrite,    // default: tolerate Access also having the file open
    // FileShare             = FileShare.Read,         // tighten to read-only sharing if you don't need that
    UseLockFile              = true,   // create .ldb/.laccdb lockfile (default: true)
    LinkedSourcePathAllowlist = new[] { @"C:\TrustedLinkedDatabases" },
    LinkedSourcePathValidator = (link, fullPath) => link.Kind == LinkedTableKind.Access,
};
await using var reader = await AccessReader.OpenAsync("database.mdb", options);

var writerOptions = new AccessWriterOptions("secretPassword")
{
    UseLockFile = true,              // create .ldb/.laccdb lockfile (default: true)
    RespectExistingLockFile = true,  // throw IOException if lockfile already exists (default: true)
};
await using var writer = await AccessWriter.OpenAsync("database.mdb", writerOptions);
```

---

## Error Handling

```csharp
try { var dt = await reader.ReadTableAsync("Orders"); }
catch (FileNotFoundException)   { /* file missing */ }
catch (UnauthorizedAccessException) { /* no password provided, or wrong password */ }
catch (InvalidDataException)    { /* corrupt or non-JET file */ }
catch (JetLimitationException)  { /* deleted-column gap, numeric overflow */ }
catch (NotSupportedException)   { /* write: CLR type not mappable to a Jet column, or table definition too large for one TDEF page */ }
catch (ObjectDisposedException) { /* reader already disposed */ }
```

---

## Encryption Support

All password-protected formats produced by Microsoft Access from Access 97 through Microsoft 365 are fully **read- and write-supported**. Supply the password via [`AccessReaderOptions.Password`](JetDatabaseWriter/AccessReaderOptions.cs) or [`AccessWriterOptions.Password`](JetDatabaseWriter/AccessWriterOptions.cs); the format is auto-detected from the file header. For new encrypted ACCDB output, omit `targetFormat` or choose `AccessEncryptionFormat.AccdbAgile`; Access-native Agile is the default and recommended writer target.

- **In-place mutation.** Flat Access formats (Jet3 XOR, Jet4 RC4, ACCDB legacy `;pwd=`, AES-128 page encryption, and Access-native Agile) re-encrypt modified pages on flush. Office Crypto / CFB containers that the reader opens are re-emitted as CFB v4 on `DisposeAsync` when mutated; `AccessEncryptionFormat.AccdbAgile` writes the modern Access-native flat Agile layout, while `AccessEncryptionFormat.AccdbAgileCfb` writes an Office Crypto Agile wrapper.
- **Encryption mutation APIs.** `AccessWriter.EncryptAsync(path, password, targetFormat: null, …)`, `AccessWriter.DecryptAsync(path, password, …)`, and `AccessWriter.ChangePasswordAsync(path, oldPassword, newPassword, …)` add, remove, or rotate encryption (and switch formats) on an existing file. When `targetFormat` is omitted or `null`, `EncryptAsync` chooses the best supported target for the file kind: Jet4 RC4 for Jet4 `.mdb`, and Access-native Agile for `.accdb`. These APIs accept `ReadOnlyMemory<char>`; pass a mutable `char[]` / `Memory<char>` when the caller needs to erase its own password buffer after the awaited operation completes. Use `AccessWriter.DetectEncryptionFormatAsync(path)` to discover the current format.

| Format | Versions | Detection | Key derivation | Page / payload cipher |
|---|---|---|---|---|
| Jet3 page XOR | Access 97 (`.mdb`) | header byte `0x62` bit `0x01` | static 128-byte mask | XOR (no password required) |
| Jet4 RC4 | Access 2000–2003 (`.mdb`) | header byte `0x62` value `0x02` / `0x03` | password XOR-verified at `0x42`; `dbKey` at `0x3E` | per-page RC4 with `MD5(dbKey ‖ pageNumber)` |
| ACCDB legacy password | Access 2007+ (`.accdb`, `;pwd=...`) | header byte `0x62` value `0x07` | password XOR-verified at `0x42` | none (password only) |
| ACCDB AES-128 (CFB-wrapped) | Access 2007+ (`.accdb`) | CFB magic `D0 CF 11 E0` + Jet4-style header password | SHA-256(password) → 16 bytes | per-page AES-128-ECB |
| ACCDB Standard (Office 2007) | Access 2007 (`.accdb`) | CFB compound document with `EncryptionInfo` version (3,2) or (4,2), AlgID `0x6601` | MS-OFFCRYPTO §2.3.6 PBKDF: SHA-1 + 50 000 iterations + 16-byte salt | AES-128-CBC with zero IV over whole `EncryptedPackage` stream |
| ACCDB Agile (Access-native, default for new ACCDB encryption) | Access 2010 SP1+, Microsoft 365 (`.accdb`) | Flat Agile header: masked encoding key at page-0 `0x3E`, `EncryptionInfo` length at `0x299`, descriptor at `0x29B`; written by `AccessEncryptionFormat.AccdbAgile` | ECMA-376 §2.3.4.11 PBKDF: SHA-512 + `spinCount` iterations + spec block keys (`0xfea7d2763b4b9e79`, `0xd7aa0f6d3061344e`, `0x146e0be7abacd0d6`) | AES-256-CBC per data page; IV `SHA-512(keyDataSalt || (uint32_le(pageNumber) XOR encodingKey))[:16]` |
| ACCDB Agile (Office Crypto CFB) | Access 2010 SP1+, Office Crypto wrappers (`.accdb`) | CFB v4 compound document with Agile `EncryptionInfo` version (4,4) and `EncryptedPackage`; readable from existing files and written by `AccessEncryptionFormat.AccdbAgileCfb` | ECMA-376 §2.3.4.11 PBKDF: SHA-512 + `spinCount` iterations + spec block keys | AES-256-CBC over 4096-byte `EncryptedPackage` segments |

---

## Limitations

The items below are either **not yet implemented** or are important behavioral caveats, and are the most likely places to hit a wall.

### Thread safety and concurrent access
- **Do not treat a single `AccessReader` / `AccessWriter` instance as a parallel worker.** Low-level page I/O is funneled through one internal gate, so overlapping calls on the same instance block behind each other rather than running in parallel; `AccessWriter` also allows only one active explicit transaction per instance. **Concurrent writers against the same file will corrupt it.** Open with `UseLockFile = true` and `RespectExistingLockFile = true` (both defaults) to fail fast when another process already holds the database. Page byte-range locks use `FileStream.Lock` where .NET supports it, such as Windows, Linux, and Android; on unsupported platforms such as iOS, macOS, and tvOS, the option is a no-op and lockfiles or external coordination are the authoritative protection.

### Transaction durability
- **No WAL or crash recovery.** Transactions provide in-memory rollback before commit replay begins. They do not provide ESE-style redo/undo recovery after process loss, storage failure, or cancellation once `CommitAsync` has started writing pages to the target stream.

### Compact & Repair
- **`ShrinkDatabaseAsync` is a tail shrinker, not a full Compact & Repair.** It truncates free pages from the physical end of the file but does not move live pages, renumber page references, rebuild all tables into a new file, or scrub every unused byte gap inside otherwise-live pages.

### Forms, reports, macros, queries, VBA
- Out of scope. The library targets the JET storage layer only. `MSysObjects` entries of type Form, Report, Macro, Module, or Query are preserved on disk but are neither parsed nor editable.

### SQL and ODBC
- **No SQL parser, query engine, or ODBC driver.** This library is a managed reader/writer over the JET on-disk format, not a database engine. Filter, project, and join through LINQ over `Rows(...)` / `Rows<T>(...)` instead.

---

## How It Works

The library parses JET pages directly, based on the [mdbtools format specification](https://github.com/mdbtools/mdbtools/blob/master/HACKING.md):

1. **Page 0** — header: Jet3/Jet4 detection, code page, encryption flag
2. **Page 2** — `MSysObjects` catalog: table names → TDEF page numbers
3. **TDEF pages** — table definition chains: column descriptors + names
4. **Data pages** — row slot arrays → null mask + fixed/variable fields
5. **LVAL pages** — long-value chains for MEMO, OLE, and attachment payloads

---

## Contributing

Issues and pull requests are welcome. Please open an issue to discuss larger changes before submitting a PR.

## License

MIT — see [LICENSE](LICENSE) for details.
