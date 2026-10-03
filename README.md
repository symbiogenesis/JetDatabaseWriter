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
| ✅ **IQueryable** | `Query<T>(...)` is an `IQueryable<T>` with `Where`/`OrderBy`/`Skip`/`Take`/`Select`/`Include`+`ThenInclude` (relationship-inferred eager load) and sync/async terminals (`ToListAsync`/`CountAsync`/`FirstAsync`/…) |
| ✅ **Async-first** | `ValueTask<T>` API, `OpenAsync(...)`, `await using` (`IAsyncDisposable`), `IProgress<T>` callbacks |
| ✅ **Stream-based I/O** | Open from any seekable `Stream` (files, byte arrays, blobs, embedded resources) |
| ✅ **Encryption** | Jet3 XOR, Jet4 RC4, ACCDB legacy / AES-128 / Standard (Office 2007) / Agile (Office 2010+) — all readable; all but Access-native flat Agile writable in place |
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
- **Static analysis** — Roslyn .NET analyzers, Roslynator, StyleCop, and the `.editorconfig` code-style rules, all errors in the Release build of the library and the tests that CI runs on every push to main and every pull request
- **Continuous integration** — [GitHub Actions](.github/workflows/ci.yml) builds the solution in Release and runs the test suite on .NET 10 and on .NET 8 (the `netstandard2.1` build) on Windows, on every push to main and every pull request; a release tag runs the same checks and publishes the package that run built only after it passes
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

### NuGet target compatibility

The package ships two builds. Apps on .NET 10 or later get the `net10.0` build; apps on .NET Core 3.x and .NET 5 through 9 get the `netstandard2.1` build, which brings in `System.Linq.Async`, `System.ComponentModel.Annotations` and `System.Text.Encoding.CodePages` for the APIs .NET 10 has built in. The test suite runs against both: once on .NET 10 and once on .NET 8, where it loads the `netstandard2.1` build. The two builds read and write databases the same way, except that only the `net10.0` build reads pages from a file path with `RandomAccess`; the `netstandard2.1` build reads them through the stream.

### Usage

```csharp
using JetDatabaseWriter;

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

Use the standard `System.ComponentModel.DataAnnotations.Schema` attributes when a property name can't match its column. `[Column("Last Name")]` binds a property to a differently named column (for example one with a space), and `[NotMapped]` excludes a property. One mapping model applies these everywhere a `T` is used: typed inserts (`InsertRowAsync<T>` / `InsertRowsAsync<T>`), `Rows<T>`, `ReadTableAsync<T>`, `FromIndex<T>`, and `Query<T>` (`Where` index inference, index-ordered `OrderBy`, and `Include` join keys). `[Table("...")]` names the table an `Include` navigation's target type maps to. Two properties that name the same column with `[Column]` throw `InvalidOperationException`; an explicit `[Column("X")]` wins over a property that is merely named `X`.

```csharp
public class Person
{
    [Column("Person ID")] public int PersonId { get; set; }
    [Column("Last Name")] public string? LastName { get; set; }
    [NotMapped] public string? DisplayLabel { get; set; }
}

int smiths = await reader.Query<Person>("People").Where(p => p.LastName == "Smith").CountAsync(cancellationToken);
```

### Typed DataTable

```csharp
DataTable dt = await reader.ReadTableAsync("Products", cancellationToken: cancellationToken);
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

### Relationships and eager loading (`Include` / `ThenInclude`)

`ListRelationshipsAsync` returns every foreign-key relationship from the database's `MSysRelationships` catalog. Each entry links a child (`ForeignTable`) to a parent (`PrimaryTable`) with matching key columns, plus the integrity and cascade flags.

```csharp
foreach (RelationshipMetadata rel in await reader.ListRelationshipsAsync(cancellationToken))
    Console.WriteLine($"{rel.Name}: {rel.ForeignTable}({string.Join(",", rel.ForeignColumns)}) -> {rel.PrimaryTable}({string.Join(",", rel.PrimaryColumns)})");
```

`Query<T>(...)` returns an `IQueryable<T>` and **infers relationships automatically**: pass a navigation property to `Include(...)` and the related rows are eagerly loaded and stitched onto each entity, with the relationship resolved from the catalog — no mapping configuration. Chain `ThenInclude(...)` to eager-load a nested navigation off the included entity, any number of levels deep. A collection navigation can be **filtered, ordered, and paged** inline — `Include(c => c.Orders.Where(o => o.Amount >= 100).OrderBy(o => o.Amount).Take(5))` — and a following `ThenInclude` then descends only into the kept rows. Compose the standard LINQ operators — `Where`, `OrderBy`/`OrderByDescending`/`ThenBy`/`ThenByDescending`, `Skip`, `Take`, `Select` — and execute with the sync or async terminals (`ToListAsync`/`ToArrayAsync`/`ToDictionaryAsync`, `FirstAsync`/`FirstOrDefaultAsync`, `SingleAsync`/`SingleOrDefaultAsync`, `CountAsync`/`LongCountAsync`, `AnyAsync`, `SumAsync`/`AverageAsync`/`MinAsync`/`MaxAsync`), or `AsAsyncEnumerable()` for `await foreach`. `Where` drives the same index inference as `Rows<T>(table, predicate)`; an unfiltered `OrderBy` over a column backed by a covering unique integer-key index streams straight from that index (so a following `Take` bounds the work), and other ordering and paging run in memory after the filtered set is read.

```csharp
// Parent with its children (collection navigation), filtered, ordered and paged
List<Customer> customers = await reader.Query<Customer>("Customers")
    .Where(c => c.Region == "West")
    .OrderBy(c => c.Name)
    .Skip(20)
    .Take(10)
    .Include(c => c.Orders)
    .ToListAsync(cancellationToken);

// Filtered / ordered / paged collection include
List<Customer> withTopOrders = await reader.Query<Customer>("Customers")
    .Include(c => c.Orders.Where(o => o.Freight >= 100m).OrderByDescending(o => o.Freight).Take(5))
    .ToListAsync(cancellationToken);

// Nested eager loading: order -> customer -> region (Include then ThenInclude)
List<Order> orders = await reader.Query<Order>("Orders")
    .Include(o => o.Customer)
    .ThenInclude(c => c.Region)
    .ToListAsync(cancellationToken);

// Child with its parent (reference navigation)
Order? order = await reader.Query<Order>("Orders")
    .Where(o => o.OrderId == 10248)
    .Include(o => o.Customer)
    .FirstOrDefaultAsync(cancellationToken);
```

The navigation's target type is matched to the related table by name (ignoring case and non-alphanumeric separators), so the entity classes the [scaffolder](#scaffolding--generate-c-models-from-a-database) generates work as-is; annotate a type with `[Table("ActualName")]` (`System.ComponentModel.DataAnnotations.Schema.TableAttribute`) to bind a POCO whose name doesn't match its table. When the join columns are indexed (a primary key or foreign-key index, inferred automatically) each `Include` / `ThenInclude` loads the related rows with one index seek per distinct key; otherwise it scans the related table once and joins in memory, and a shared include prefix (two `ThenInclude` chains off the same `Include`) loads only once. Inline collection filters apply per parent in memory and may only reference the child being filtered. Operators the engine doesn't translate natively — `Select` projections, `GroupBy`, and the like — run in memory over the materialized rows, so the broader LINQ surface still composes. Relationships are read from `MSysRelationships`, so this requires a full-catalog database (Jet3 `.mdb` files have no relationship catalog).

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

`GetColumnMetadataAsync` names each complex column by subtype in `TypeName`: `"Attachment"`, `"Version History"`, or `"Multi-value "` plus the element type (`"Multi-value Text"`, `"Multi-value Long Integer"`), and `"Complex"` when `MSysComplexColumns` cannot resolve it. `ClrType` is `byte[]` for all of them (see the cells below).

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

The parent-row predicate must match exactly one row (zero or multiple matches throw `InvalidOperationException`). As in Access, every row inserted into a table with complex columns gets one per-row complex reference, shared by all its complex columns, from the table's complex AutoNumber counter in its TDEF; the reference of a deleted row is never handed out again, and rows added to an Access-authored table never take a reference an Access row owns. Attachment payloads are stored in Access's `FileData` wrapper (4-byte typeFlag + uncompressed length + a header with the lowercased extension + the file): files Access stores raw (jpg, jpeg, gif, png, zip, cab, docx, xlsx, xlsb, pptx) are stored as is, and everything else as a zlib stream, as Access does; the reader also reads the raw-deflate form earlier builds of this library wrote. Payloads larger than the 256-byte inline-OLE cap are pushed onto freshly allocated Access-style LVAL pages (single-page or chained form with the `LVAL` page signature) and reassembled by the reader; the upper limit is the 24-bit on-disk LVAL length field (~16 MB per file).

Row reads carry the same items. Complex columns report `ClrType = byte[]`, so `Rows(...)`, `ReadTableAsync(...)`, and `Rows<T>(...)` put every attachment or value of a parent row into one `byte[]` cell (`DBNull` when the row has none); decode it with `ComplexCellValue`:

```csharp
await foreach (object[] row in reader.Rows("Documents", cancellationToken: cancellationToken))
{
    if (row[1] is byte[] cell)
    {
        foreach (AttachmentRecord file in ComplexCellValue.ReadAttachments(cell))
            Console.WriteLine($"{file.FileName}: {file.FileData.Length} bytes");
    }
}

// Multi-value and version-history columns: ComplexCellValue.ReadMultiValueItems(cell)
```

For a version-history column (an append-only Memo column's history), `ReadMultiValueItems` and `GetMultiValueItemsAsync` return one item per version, with its text in `Value` and the time Access recorded it in `Modified`; `Modified` is `null` for multi-value items.

`RowsAsStrings(...)` and `ReadTableAsStringsAsync(...)` return the same cell as a `data:application/octet-stream;base64,...` URI (empty string when the row has none). The cell layout is documented on `ComplexCellValue`.

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
DataTable preview = await reader.ReadTableAsStringsAsync("Products", maxRows: 20, cancellationToken: cancellationToken);
```

---

## Streaming Large Tables

`Rows<T>(...)`, `Rows(...)`, and `RowsAsStrings(...)` stream rows lazily over `IAsyncEnumerable<T>`, so enumeration can short-circuit without materializing the whole table. Each takes an optional `IProgress<long>` row-count callback.

```csharp
var progress = new Progress<long>(n => Console.Write($"\r{n:N0} rows"));

await foreach (Product p in reader.Rows<Product>("Products", progress))
    Console.WriteLine($"{p.ProductName}: {p.UnitPrice:C}");
```

`Rows(...)` yields `object[]` rows (nulls surface as `DBNull.Value`); `RowsAsStrings(...)` yields `string[]` rows for compatibility and CSV-style consumers.

---

## Querying with async LINQ

Because those `Rows` / `Rows<T>` / `RowsAsStrings` streams are `IAsyncEnumerable<T>`, they compose with the standard async LINQ operators (`Where`, `Take`, `Select`, `ToListAsync`, `FirstOrDefaultAsync`, `CountAsync`, …) — there is no separate query type and no terminal `Execute` call.

```csharp
// POCO chain — first match (use ToListAsync() to materialize instead)
Order? first = await reader.Rows<Order>("Orders")
    .Where(o => o.OrderDate.Year == 2024)
    .Take(10)
    .FirstOrDefaultAsync(ct);

// Object-array chain (no POCO)
int count = await reader.Rows("OrderDetails")
    .Where(row => row[3] is decimal p && p > 100m)
    .CountAsync(ct);
```

Filtering and projection run client-side per row and require a table scan unless enumeration short-circuits — there is no SQL engine underneath. To let the reader pick an index for you, pass the predicate directly to `Rows<T>(table, predicate)` (see [Index-backed reads](#index-backed-reads)); chained `.Where(...)` over `Rows(...)` stays client-side because it receives a compiled delegate the engine can't inspect. Use `FromIndex(...)` to force a specific index or index ordering, or `SeekRowsAsync` for the older full-key exact-match object-array API.

---

## Reading All Tables

`ReadAllTablesAsync` materializes every user table into `DataTable` instances in one call, with a `Progress<TableProgress>` callback that fires once per table:

```csharp
IReadOnlyDictionary<string, DataTable> all = await reader.ReadAllTablesAsync(
    new Progress<TableProgress>(p => Console.WriteLine($"Reading {p.TableName} ({p.TableIndex + 1}/{p.TableCount})...")),
    cancellationToken);
```

For large databases, prefer streaming each table from `ListTablesAsync()` with `Rows(...)` / `Rows<T>(...)` (or `ReadTableAsStringsAsync(...)` for string-typed `DataTable`s) rather than materializing everything at once.

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
| `IsNullable` | ✅ `MSysObjects.LvProp` (`Required`) | Inserts that leave the column null and updates that set it to null throw `InvalidOperationException`. Restored on reopen; surfaced to readers via `ColumnMetadata.IsNullable`. |
| `IsAutoIncrement` | ✅ TDEF flag bit `FLAG_AUTO_LONG 0x04` | Supported for `byte`/`short`/`int`/`long`. Seeded on first use from `max(TDEF AutoNumber counter, existing) + 1`; the counter only rises, so deleted top values are not reused in a later session. An update may assign an explicit value, which raises the stored high-water as an insert does, but setting the column to null throws `InvalidOperationException`. |
| `IsPrimaryKey` | ✅ TDEF logical-index entry with `index_type = 0x01` | Shortcut for synthesizing a PK `IndexDefinition` named `"PrimaryKey"` from one or more columns (in declaration order). Forces the PK key columns to `IsNullable = false` on the emitted TDEF. Mixing this with an explicit PK `IndexDefinition` in the same call throws `ArgumentException`. Single- and multi-column PKs both participate in live B-tree leaf maintenance (composite-key path). |
| `DefaultValue` | ✅ as a literal `DefaultValue` expression in `MSysObjects.LvProp` | CLR object substituted for `null` / `DBNull.Value` at insert time. Unless `DefaultValueExpression` is set, it is also persisted as a literal expression (`7`, `"text"`, `True`, `#2024-02-29 08:30:00#`), so a later `AccessWriter` and Microsoft Access apply the same default. Text, Boolean, numeric, `DateTime` and `Guid` values persist; other types such as `byte[]` throw `NotSupportedException` at table creation. Updates never apply defaults. |
| `ValidationRule` | ⚠️ this writer only | Checked against every non-null value an insert supplies or an update assigns; a rejection throws `ArgumentException`. A CLR `Func<>` cannot be serialized into the file, so a writer that opens the database later does not enforce it. For a rule every writer and Microsoft Access enforce, set `ValidationRuleExpression`. |
| `DefaultValueExpression` | ✅ `MSysObjects.LvProp` (`DefaultValue`) | Jet expression string (e.g. `"0"`, `"\"hi\""`, `"=Now()"`, `"Date()"`). Every `AccessWriter` evaluates it when an insert leaves the column `null` / `DBNull.Value`. Wins over `DefaultValue` for persistence; the declaring writer still uses a CLR `DefaultValue` when both are set. Surfaced to readers via `ColumnMetadata.DefaultValueExpression`. |
| `ValidationRuleExpression` | ✅ `MSysObjects.LvProp` (`ValidationRule`) | Access rule with the column as implicit left operand (e.g. `">=0 And <=100"`, `"Is Not Null"`, `"Between 1 And 10"`, `"In (1,2,3)"`, `"Like \"A*\""`, `"0 Or >100"`). Every `AccessWriter` checks it against each value an insert stores and each value an update assigns; a rejection throws `ArgumentException`. Like Access, a rule that does not test for Null accepts Null. Surfaced via `ColumnMetadata.ValidationRuleExpression`. |
| `ValidationText` | ✅ `MSysObjects.LvProp` (`ValidationText`) | User-facing message Access shows when `ValidationRuleExpression` rejects a value; the writer appends it to the `ArgumentException` message. Surfaced via `ColumnMetadata.ValidationText`. |
| `Description` | ✅ `MSysObjects.LvProp` (`Description`) | Free-text column description shown in Access Design View. Surfaced via `ColumnMetadata.Description`. Preserved across `AddColumnAsync` / `DropColumnAsync` / `RenameColumnAsync`. |

Column defaults and validation rules that Microsoft Access wrote into an existing database are applied the same way. For example, older versions of Access gave every Number column a default of `0`, so an insert that leaves such a column null stores `0`. The writer evaluates default and rule expressions with its calculated-column expression engine, which does date arithmetic as Access does, so a default of `=Date()+7` stores the date a week from today and a rule of `>=Date()-30` rejects older dates. It also reads single-quoted text (a default of `'N/A'`) and VBA `&H`/`&O` literals (a rule of `<=&HFF`). An expression that uses syntax or a function the engine does not support (such as `GenGUID()`, `CurrentUser()` or `DLookUp`) is skipped by the writer rather than blocking every write to the table; Microsoft Access still applies it. Table-level (record) validation rules are not enforced.

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

`SecureEraseMode.DeletedRowsAndFreedPages` overwrites deleted row bodies and the deleted rows' MEMO/OLE LVAL data. Access can pack several long values onto one LVAL page, so an LVAL page returns to the Access global free list, overwritten, only when no other live value is left on it; otherwise only the deleted value's rows on it are overwritten and marked deleted. `ScrubFreePagesAsync` overwrites pages already on the free list. `ShrinkDatabaseAsync` truncates free pages from the physical end of the file; it does not renumber live pages or perform a full Access Compact & Repair rebuild.

### Add, drop, and rename columns

```csharp
// Append a new column. Existing rows receive DBNull for the new column.
await writer.AddColumnAsync("Contacts", new ColumnDefinition("Phone", typeof(string), maxLength: 32));

// Rename an existing column. Row data is preserved.
await writer.RenameColumnAsync("Contacts", "Score", "Rating");

// Drop a column. Its data is permanently lost.
await writer.DropColumnAsync("Contacts", "Phone");
```

> These operations rewrite the whole table (copy rows to a new schema, then swap the catalog entry). Cost scales with row count. Every column keeps its properties (type, size, precision and scale, Unicode compression, calculated expression, persisted properties), and indexes and foreign-key relationships follow the rebuilt table: its relationship index entries are re-created and the related tables are re-linked to it, and renaming a key column updates `MSysRelationships`. Dropping a column that a relationship uses as a key column throws `InvalidOperationException`; drop the relationship first.

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

Declare a relationship between two existing tables. The library appends one row per FK column to the `MSysRelationships` catalog (which Microsoft Access reads to populate the Relationships designer) and emits the matching per-TDEF foreign-key logical-index entries on both sides so the relationship is visible to readers immediately. On Jet3 (Access 97) files the entries take the layout Access 97 writes.

`DropRelationshipAsync` and `RenameRelationshipAsync` rewrite `MSysRelationships` as live rows, update or remove the TDEF entries on every format, and leave Type=8 relationship rows in `MSysObjects` for Microsoft Access Compact & Repair to normalize from the canonical relationship rows.

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

If the transaction is disposed without a `CommitAsync` call (for example, because an exception unwound the scope), all buffered changes are discarded automatically. Rolling back also returns the writer's cached table list and column constraints to their state when the transaction began, so tables, columns and AutoNumber values from the rolled-back work are gone. Only one transaction may be active per `AccessWriter` instance.

`CommitAsync` is not atomic and is not a durable write-ahead log. The journal holds the new contents of each changed page; commit writes them directly over the target stream in page-number order, then flushes it. The transaction ends whether or not `CommitAsync` succeeds:

- If it fails before the first page write starts (cancellation, or a byte-range commit-lock timeout), the file is unchanged and `IsRolledBack` is `true`.
- Once the first page write starts, cancellation is ignored and the commit runs to completion.
- If anything fails after that (the process, stream or device, or a per-page byte-range lock timeout), pages already written are left in place and no recovery pass is attempted. `CommitAsync` surfaces the exception with both `IsCommitted` and `IsRolledBack` `false`; treat the file as damaged and restore it from a copy.

WAL-style crash recovery is out of scope for the current file-format writer.

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

Table and column names are automatically converted to PascalCase C# identifiers — spaces, hyphens, and special characters are cleaned, and C# keywords are escaped. When the identifier differs from the Access name by more than case, the generated code carries `[Table("Order Details")]` on the class or `[Column("Unit Price")]` on the property, so the properties read, write and query the original columns, and `Include` navigations to the entity resolve the original table.

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
catch (JetLimitationException)  { /* deleted-column gap, numeric overflow, or write: a table whose indexes cannot be enforced or maintained, or a Jet3 table over 255 columns */ }
catch (NotSupportedException)   { /* write: CLR type not mappable to a Jet column, or table definition too large for one TDEF page */ }
catch (ObjectDisposedException) { /* reader already disposed */ }
```

---

## Encryption Support

All password-protected formats produced by Microsoft Access from Access 97 through Microsoft 365 are **read-supported**, and all of them except Access-native flat Agile can be opened by `AccessWriter` and edited in place (see below). Supply the password via [`AccessReaderOptions.Password`](JetDatabaseWriter/AccessReaderOptions.cs) or [`AccessWriterOptions.Password`](JetDatabaseWriter/AccessWriterOptions.cs); the format is auto-detected from the file header. For new encrypted ACCDB output, omit `targetFormat` or choose `AccessEncryptionFormat.AccdbAgile`; Access-native Agile is the default `EncryptAsync` target, but `AccessWriter` cannot open it afterwards (see below).

- **In-place mutation.** Flat Access formats (Jet3 XOR, Jet4 RC4, ACCDB legacy `;pwd=`, and AES-128 page encryption) re-encrypt modified pages on flush. Office Crypto / CFB containers that the reader opens are re-emitted as CFB v4 on `DisposeAsync` when mutated; `AccessEncryptionFormat.AccdbAgile` writes the modern Access-native flat Agile layout, while `AccessEncryptionFormat.AccdbAgileCfb` writes an Office Crypto Agile wrapper.
- **Access-native flat Agile is read-only.** `AccessWriter.OpenAsync` (path and stream overloads) throws `NotSupportedException` for an `AccessEncryptionFormat.AccdbAgile` file before writing anything, because the writer has no AES-CBC page cipher for that layout. Read it with `AccessReader`; to change its data, call `AccessWriter.DecryptAsync`, edit the decrypted file, then `AccessWriter.EncryptAsync` again, or re-encrypt it as `AccessEncryptionFormat.AccdbAgileCfb`, which the writer can open. `EncryptAsync` still defaults to flat Agile for `.accdb`, so pass `AccdbAgileCfb` when the file must stay writable.
- **Open cost.** Opening a reader or writer reads page 0 to detect flat Agile; only a flat Agile file is then read and decrypted in full (into memory). The CFB Office Crypto formats (Standard, Agile CFB) are also decrypted in full on open.
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
- **No WAL or crash recovery; commit is not atomic.** Transactions provide in-memory rollback before commit replay begins. Commit writes the journaled pages in place, so process loss or a storage failure once `CommitAsync` has started writing pages leaves part of the transaction in the file, with no ESE-style redo/undo recovery. Cancellation cannot tear a commit: it is honoured only before the first page write.

### Encryption
- **`AccessWriter` cannot open Access-native flat Agile (`AccessEncryptionFormat.AccdbAgile`) files.** `OpenAsync` throws `NotSupportedException` and leaves the file untouched. This is the format `EncryptAsync` picks by default for `.accdb`. Decrypt, edit, and re-encrypt, or use `AccessEncryptionFormat.AccdbAgileCfb` for files the writer must open. See [Encryption Support](#encryption-support).

### Column defaults and validation rules
- **Persisted `DefaultValue` and `ValidationRule` expressions run on the library's own expression engine.** An expression that uses syntax or a function the engine does not support (for example `GenGUID()`, `CurrentUser()` or `DLookUp`) is skipped by the writer, not enforced; Microsoft Access still applies it. Table-level (record) validation rules are not enforced, and a CLR `ValidationRule` delegate binds only the writer that created the table.

### Table width
- **A Jet3 (Access 97) table holds at most 255 columns**, Access's field limit; a Jet3 row stores its column count in one byte. `CreateTableAsync` and `AddColumnAsync` throw `JetLimitationException` for a 256th column before writing anything.

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
4. **Data pages** — row slot arrays (an overflow row's slot points to the slot Access moved the row to) → null mask + fixed/variable fields
5. **LVAL pages** — long-value chains for MEMO, OLE, and attachment payloads

---

## Contributing

Issues and pull requests are welcome. Please open an issue to discuss larger changes before submitting a PR.

CI runs the same commands you can run locally; a PR needs them to pass:

```bash
dotnet build JetDatabaseWriter.slnx -c Release
dotnet test --project JetDatabaseWriter.Tests -c Release --no-build
```

Releases are published from version tags; [PUBLISH.md](PUBLISH.md) describes the steps.

## License

MIT — see [LICENSE](LICENSE) for details.
