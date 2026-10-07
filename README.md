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
- Access compatibility is checked with Microsoft-authored fixtures and DAO tests. Coverage is incomplete; see the [validation matrix](docs/design/writer-disk-format-validation-matrix.md) and [open requirements](docs/todo.md).

## Features

| | |
|---|---|
| ✅ **Pure managed .NET** | No OleDB, ODBC, or ACE/Jet driver — runs anywhere .NET runs |
| ✅ **All Access versions** | Jet3 / Jet4 / ACE — Access 97 through Microsoft 365 (`.mdb` / `.accdb`) |
| ✅ **Read & write** | Create databases and tables; insert/update/delete rows; add/drop/rename columns |
| ✅ **Typed values** | `int`, `DateTime`, `decimal`, `Guid`, MEMO, OLE, Hyperlink — not just strings |
| ✅ **POCO + LINQ** | `Rows<T>("...", o => …)` auto-infers an index; `FromIndex<T>(...)` to override; async LINQ (`Where`/`Take`/`FirstOrDefaultAsync`/…) over `IAsyncEnumerable<T>` |
| ✅ **IQueryable** | `Query<T>(...)` is an `IQueryable<T>` with `Where`/`OrderBy`/`Skip`/`Take`/`Select`/`Include`+`ThenInclude` (relationship-inferred eager load) and async terminals (`ToListAsync`/`CountAsync`/`FirstAsync`/…); results are async-only |
| ✅ **Async-first** | `ValueTask<T>` API, `OpenAsync(...)`, `await using` (`IAsyncDisposable`), `IProgress<T>` callbacks |
| ✅ **Stream-based I/O** | Open from any seekable `Stream` (files, byte arrays, blobs, embedded resources) |
| **Encryption** | Native JET4 encrypted/password-only fixture coverage; ACE provider coverage remains incomplete. See [Encryption Support](#encryption-support). |
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

A reader opened from a path reads each page on the calling thread when that thread is a thread-pool thread with no synchronization context, which is where most `await` continuations run in console, ASP.NET Core and worker apps; in our measurements that made each page read about twice as fast as handing it to another thread. Elsewhere, such as on a UI thread, page reads stay off the calling thread. Cancellation is checked before every page read, so a cancelled scan stops at its next page, but a page read that has started runs to completion.

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

If you open a `FileStream` yourself for `AccessReader`, open it without `FileOptions.Asynchronous`. The reader reads every page through the stream, and on Windows, in our measurements, a page read on an overlapped handle took about twice as long as one on a synchronous handle, even when the page was already in the OS cache. `AccessReader.OpenAsync(path)` opens its file that way.

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

A value converts to the type of its property. A number converts to any numeric property whose range holds it, rounded to the property's precision: a fraction maps to an integral property as the nearest integer, halves to even as Access's `CInt` rounds them (2.5 maps to 2, 3.5 to 4 and 3.7 to 4), and a Double maps to a `float` property as the nearest `float`. To keep the stored value, map the column to a property of its own type. An enum property maps from an integral column by value, as a C# cast does, so a value with no named member still maps, or from a text column by member name, ignoring case. A `Guid` property maps from a ReplicationID column, from text holding a GUID (with or without braces), or from a 16-byte Binary column. A `Hyperlink` property maps from text, and a `string` property from a hyperlink. A value outside the property's range, such as 300 or 255.5 in a `byte` property, or text that isn't a number in an `int` property, throws `InvalidCastException` naming the column and the property. That applies to every typed read: `Rows<T>`, `ReadTableAsync<T>`, `FromIndex<T>`, `Query<T>` and the entities `Include` loads.

Use the standard `System.ComponentModel.DataAnnotations.Schema` attributes when a property name can't match its column. `[Column("Last Name")]` binds a property to a differently named column (for example one with a space), and `[NotMapped]` excludes a property. One mapping model applies these everywhere a `T` is used: typed inserts (`InsertRowAsync<T>` / `InsertRowsAsync<T>`), `Rows<T>`, `ReadTableAsync<T>`, `FromIndex<T>`, and `Query<T>` (`Where` index inference, index-ordered `OrderBy`, and `Include` join keys). `[Table("...")]` names the table an `Include` navigation's target type maps to. Two properties that name the same column with `[Column]` throw `InvalidOperationException`; an explicit `[Column("X")]` wins over a property that is merely named `X`.

```csharp
using System.ComponentModel.DataAnnotations.Schema;
using JetDatabaseWriter.Linq; // CountAsync, ToListAsync, Include, ... on Query<T>

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

Only conjuncts combined with `&&` over direct column members (`o.Column == value`, `o.Column > value`, …) drive inference; method calls, `||`, computed members such as `o.When.Year`, and column-to-column comparisons are evaluated client-side. So is a comparison an index can't answer exactly: one on a property that holds a converted value, such as an enum or `Guid` property bound to a Text column, whose index holds the text; one whose bound the column can't hold, such as `o.Id > 1.5` or a `long` beyond Long Integer's range on a Long Integer column; and an upper bound on a Date/Time column finer than a millisecond, such as `o.When < DateTime.Now`, since the index keeps whole milliseconds. Jet3 `.mdb` files always scan.

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

`Query<T>(...)` returns an `IQueryable<T>` and **infers relationships automatically**: pass a navigation property to `Include(...)` and the related rows are eagerly loaded and stitched onto each entity, with the relationship resolved from the catalog — no mapping configuration. Chain `ThenInclude(...)` to eager-load a nested navigation off the included entity, any number of levels deep. A collection navigation can be **filtered, ordered, and paged** inline — `Include(c => c.Orders.Where(o => o.Amount >= 100).OrderBy(o => o.Amount).Take(5))` — and a following `ThenInclude` then descends only into the kept rows. Compose the standard LINQ operators — `Where`, `OrderBy`/`OrderByDescending`/`ThenBy`/`ThenByDescending`, `Skip`, `Take`, `Select` — and execute with the async terminals (`ToListAsync`/`ToArrayAsync`/`ToDictionaryAsync`, `FirstAsync`/`FirstOrDefaultAsync`, `SingleAsync`/`SingleOrDefaultAsync`, `LastAsync`/`LastOrDefaultAsync`, `CountAsync`/`LongCountAsync`, `AnyAsync`/`AllAsync`, `ContainsAsync`, `SumAsync`/`AverageAsync`/`MinAsync`/`MaxAsync`), or `AsAsyncEnumerable()` for `await foreach`. The aggregates take expression selectors, as `Queryable`'s do, and fold the rows as the read returns them, without buffering the result set, by LINQ to Objects' rules: integer and decimal sums are checked, float sums accumulate in `double`, nulls are skipped, and over no values a sum is 0 while an average, minimum or maximum is `null` when its result type admits null and throws `InvalidOperationException` when it does not. Results are async-only: `foreach`, `ToList()`, `Count()`, `First()` and the other synchronous LINQ terminals throw `NotSupportedException` naming `ToListAsync`, and the matching async terminal when there is one, before anything is read, so no thread blocks on the async read stack. `Include`, `ThenInclude`, the async terminals and `AsAsyncEnumerable()` are extension methods in the `JetDatabaseWriter.Linq` namespace (`AccessQueryExtensions`), and `Include` returns an `IAccessIncludableQueryable<TEntity, TProperty>`. EF Core declares extension methods with the same names, so keeping them out of the root `JetDatabaseWriter` namespace lets an EF Core file use `AccessReader` without ambiguity errors. `Where` drives the same index inference as `Rows<T>(table, predicate)`; an unfiltered `OrderBy` over a column backed by a covering unique integer-key index streams straight from that index (so a following `Take` bounds the work), and other ordering and paging run in memory after the filtered set is read.

```csharp
using JetDatabaseWriter.Linq;

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

The navigation's target type is matched to the related table by name (ignoring case and non-alphanumeric separators), so the entity classes the [scaffolder](#scaffolding--generate-c-models-from-a-database) generates work as-is; annotate a type with `[Table("ActualName")]` (`System.ComponentModel.DataAnnotations.Schema.TableAttribute`) to bind a POCO whose name doesn't match its table. When the join columns are indexed (a primary key or foreign-key index, inferred automatically) each `Include` / `ThenInclude` loads the related rows with one index seek per distinct key; otherwise it scans the related table once and joins in memory, and a shared include prefix (two `ThenInclude` chains off the same `Include`) loads only once. Inline collection filters apply per parent in memory and may only reference the child being filtered. Supported operators the engine doesn't translate natively run in memory over the streamed rows: a `Select` projection and, after it, `Where`, the orderings (with or without a comparer, and `Order`/`OrderDescending`), `Skip`/`Take` with their `While` and `Last` forms, `Distinct`, `Concat`/`Union`/`Intersect`/`Except` (whose second sequence may be another `Query<T>`), `Reverse`, `DefaultIfEmpty`, `Append`/`Prepend`, `Cast` and `OfType`. Streaming operators such as `Select` and `Where` let a following `Take` or `FirstAsync` stop the read early; orderings, `Reverse` and `TakeLast` consume their whole input first, and `Intersect`/`Except` buffer the second sequence. `GroupBy`, `SelectMany`, the joins, `Zip`, `Chunk`, the `DistinctBy`-style set operators and the overloads whose lambdas take an element index throw `NotSupportedException` naming the operator and `AsAsyncEnumerable()`: run them over `query.AsAsyncEnumerable()` with the async LINQ operators instead. Relationships are read from `MSysRelationships`, which Access-authored Jet3 files can contain. Writer-created Jet3, Jet4 and slim-catalog ACCDB files lack that table and return no relationships.

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

Multi-value string columns store Text with a length of 1-255 characters. A `MaxLength` of 0 selects Text(255); invalid declarations and overlong items are rejected before the database changes.

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

The parent-row predicate must match exactly one row (zero or multiple matches throw `InvalidOperationException`). As in Access, every row inserted into a table with complex columns gets one per-row complex reference, shared by all its complex columns, from the table's complex AutoNumber counter in its TDEF; the reference of a deleted row is never handed out again, and rows added to an Access-authored table never take a reference an Access row owns. A complex column that `AddColumnAsync` adds takes each row's reference, and `UpdateRowsAsync` refuses to assign a complex column (`ArgumentException`), so a row keeps its reference and its items. Attachment payloads are stored in Access's `FileData` wrapper (4-byte typeFlag + uncompressed length + a header with the lowercased extension + the file): files Access stores raw (jpg, jpeg, gif, png, zip, cab, docx, xlsx, xlsb, pptx) are stored as is, and everything else as a zlib stream, as Access does; compressed payloads must have a valid native zlib wrapper, declared uncompressed length and Adler-32 checksum. Payloads larger than the 256-byte inline-OLE cap are pushed onto freshly allocated Access-style LVAL pages (single-page or chained form with the `LVAL` page signature) and reassembled by the reader; the upper limit is the 24-bit on-disk LVAL length field (~16 MB per file).

A multi-value column of `decimal` items stores them with the definition's `NumericPrecision` and `NumericScale`, which default to Access's Decimal(18,0), whole numbers; declare them to keep fractions, for example `new ColumnDefinition("Amounts", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(decimal), NumericPrecision = 10, NumericScale = 2 }`.

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

### OLE Object columns

An OLE Object column reads as `byte[]` holding the value's stored bytes, from every read API: `Rows`, `ReadTableAsync`, `ReadDataTableAsync`, `Rows<T>`, `ReadTableAsync<T>`, `Query<T>`, `FromIndex` and `SeekRowsAsync`. That is what DAO, ADO and Jackcess return. A value your code wrote comes back byte for byte. An object Microsoft Access inserted (a file dropped into the field, a Word document, a linked file) keeps Access's OLE header and the OLE object stream around it; `OleObjectValue` unwraps it on request:

```csharp
await foreach (Employee e in reader.Rows<Employee>("Employees"))
{
    OleObjectContent photo = OleObjectValue.Parse(e.Photo);
    switch (photo.Kind)
    {
        case OleObjectKind.EmbeddedFile:   // an OLE Package: photo.FileName, photo.Content (the file)
        case OleObjectKind.EmbeddedObject: // a Word, Excel, ... object: photo.ClassName, photo.Content (its native data)
            await File.WriteAllBytesAsync(photo.FileName ?? $"{e.Id}.bin", photo.Content);
            break;
        case OleObjectKind.LinkedFile:     // a link: photo.SourcePath, no content
        case OleObjectKind.NotWrapped:     // stored as is: photo.Content is the stored bytes
        case OleObjectKind.Unknown:        // a header that does not parse: photo.Content is the stored bytes
            break;
    }
}

byte[] content = OleObjectValue.GetContent(storedBytes);       // Parse(...).Content
string? mediaType = OleObjectValue.DetectMediaType(content);   // "image/jpeg", "application/pdf", ... or null
```

`Parse` never throws for any content: it checks every length against the bytes that are there, and returns the stored bytes as `Content`, with `Kind` `NotWrapped` or `Unknown`, when it cannot follow them. `MediaType` and `DetectMediaType` look for a file signature at the first byte only. The string APIs (`RowsAsStrings`, `ReadTableAsStringsAsync`, `ReadFirstTableAsStringsAsync`) render an OLE value as a `data:` URI of its stored bytes, with the media type of a signature at the first byte or `application/octet-stream`.

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
>
> Jet3 stores text and object names in the database's code page. New Jet3 files use Windows-1252, as Access 97 does. .NET would write a character outside the code page as its closest match or `?` (`Łódź` as `Lódz`, `中文` as `??`), so the writer refuses it before writing anything: a name throws `ArgumentException`, and a Text or Memo value in an insert or update throws `JetLimitationException`. Jet4 and ACE store text as UTF-16, which holds any character.

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

`DropTableAsync` refuses, with `InvalidOperationException` and before it writes anything, to drop a table that a relationship names, as Microsoft Access does; drop the relationships first (see [Foreign-key relationships](#foreign-key-relationships)).

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

Column defaults and validation rules that Microsoft Access wrote into an existing database are applied the same way. For example, older versions of Access gave every Number column a default of `0`, so an insert that leaves such a column out stores `0`; an explicit `null` still stores null, as in Access SQL. The writer evaluates default and rule expressions with its calculated-column expression engine, which does date arithmetic as Access does, so a default of `=Date()+7` stores the date a week from today and a rule of `>=Date()-30` rejects older dates. It also reads single-quoted text (a default of `'N/A'`) and VBA `&H`/`&O` literals (a rule of `<=&HFF`). An expression that uses syntax or a function the engine does not support (such as `GenGUID()`, `CurrentUser()` or `DLookUp`) is skipped by the writer rather than blocking every write to the table; Microsoft Access still applies it. Table-level (record) validation rules are not enforced.

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

> These operations rewrite the whole table (copy rows to a new schema, then swap the catalog entry). Cost scales with row count. A rename may change only the letter case of a name (`Score` to `SCORE`), and is then handled like any other rename, as described below. A new name that another column of the table has, in any case, throws `InvalidOperationException`, and renaming a column to the name it already has, spelled as stored, changes nothing and does not rewrite the table. Access is believed to allow the case-only rename and to treat the unchanged name as no change, but neither has been checked against it; see "Schema edits" under "Unchecked against Access" in [docs/todo.md](docs/todo.md#f--real-formats-and-encryption). Every column keeps its type, size, precision and scale, Unicode compression and calculated expression, and indexes and foreign-key relationships follow the rebuilt table: its relationship index entries are re-created and the related tables are re-linked to it, and renaming a key column updates `MSysRelationships`. The table's persisted `MSysObjects.LvProp` properties are kept as stored, including those the writer does not model: a column's `Caption`, `Format`, `AllowZeroLength` or `AppendOnly`, and the table-level `Description`, `Filter`, `OrderBy`, `ValidationRule` and `SubdatasheetName`. A dropped column's properties go with it, a renamed column's move to its new name, an added column gets the properties its `ColumnDefinition` declares, and the table-level `NameMap` is dropped. Renaming a column also renames it in every calculated expression, validation rule and default value expression of the table that names it: `[Score]` or a bare `Score` becomes `[Rating]`, so they keep evaluating. A reference qualified by the table's own name keeps the qualifier: `[Contacts].[Score]` becomes `[Contacts].[Rating]`, and `Contacts.Score` becomes `Contacts.[Rating]`. The writer's expression engine does not evaluate table-qualified references yet, before or after a rename. The table-level `Filter`, `OrderBy` and `ValidationRule` are not rewritten: one that names the renamed column keeps the old name. Dropping a column that a relationship uses as a key column throws `InvalidOperationException`; drop the relationship first. Dropping a column that another column's calculated expression, validation rule or default value expression names throws `InvalidOperationException` too, naming that column and its expression; change or drop that column first. Dropping a column also drops the indexes it is a key column of; every other index is carried over with its settings. A table with an index the writer cannot maintain, such as one that names a column the table does not have or lies past the end of a damaged table definition, makes all three operations throw `JetLimitationException` before anything is written, rather than lose that index.

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

When other rows reference a key, deleting its row deletes them if the relationship cascades deletes, and changing the key updates them if it cascades updates; otherwise the delete or update throws `InvalidOperationException`. A delete finds and checks every dependent row, through every level of cascades, before it deletes any row, so a refused delete changes nothing. A key update likewise finds the dependent rows of every relationship whose key it changes, checks that every row it rewrites still fits on a data page, and checks the table's own unique indexes, before it writes any row, so a refused key update changes nothing, even when another relationship would cascade the change; a dependent row that several relationships reach is rewritten once, with every new key. A cascaded key that would duplicate a key in a unique index of the child table, which a child row left without a parent row can cause, is still refused only after the child rows are prepared, and a delete or key update that cascades into a table whose indexes the writer cannot maintain throws `JetLimitationException` (see [Error Handling](#error-handling)). These failures roll back the entire call, including earlier child changes; an explicit transaction keeps its earlier successful calls.

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

> Requires a database that already contains the `MSysRelationships` catalog table. Full-catalog ACCDB databases created by `AccessWriter.CreateDatabaseAsync` include it; Access-authored `.mdb` / `.accdb` files do as well. Jet/MDB writer-created outputs and slim-catalog ACCDB outputs may not, and `CreateRelationshipAsync` throws `NotSupportedException` when the table is absent. For validation, treat writer-created full-catalog databases as supported writer outputs under test; use Access-authored or DAO-authored databases as the fixture source of truth for DAO/Access compatibility.

---

## Transactions

Row, table-schema, relationship and complex-item mutations are statement-atomic by default. Each call buffers a bounded batch of changed pages and spills it when it reaches `max(64, PageCacheSize / 2)` pages. Undo retains each page's original stored bytes and the original file length, so a work-phase failure, cancellation or write-back failure restores the call. On file-backed stores that support in-place page writes, larger undo logs use a temporary file deleted on close; encrypted pages remain encrypted in that log. Other stores retain undo in memory. `UseTransactionalWrites = true` additionally requests a durable device flush after each successful call. Explicit transactions group several calls into one durable commit and remain subject to `MaxTransactionPageBudget`. Database creation, physical tail shrinking and encrypted-container rewrapping have separate lifecycles.

`AccessWriter` supports explicit page-buffered transactions for multi-row/page operations. All page mutations are buffered in memory until committed or rolled back.

```csharp
await using var tx = await writer.BeginTransactionAsync();
await writer.InsertRowAsync("Contacts", new object[] { 7, "Grace", "grace@example.com", 90.0m });
await writer.UpdateRowsAsync("Contacts", "ContactID", 2, new Dictionary<string, object?> { ["Score"] = 93.5m });
await tx.CommitAsync(); // Replays all buffered pages and flushes the stream
```

If the transaction is disposed without a `CommitAsync` call (for example, because an exception unwound the scope), all buffered changes are discarded automatically. Rolling back also returns the writer's cached table list and column constraints to their state when the transaction began, so tables, columns and AutoNumber values from the rolled-back work are gone. Only one transaction may be active per `AccessWriter` instance.

Each mutation inside an explicit transaction has an internal savepoint. If the call fails, including cancellation or exceeding `MaxTransactionPageBudget`, its page changes, allocated pages, table metadata and AutoNumber counters are restored to their state before the call. Earlier successful calls stay in the transaction, which remains usable. Savepoints are internal; nested transactions are not supported. Mutations, commit, rollback and disposal serialize on each writer. A callback that tries to mutate the same writer throws with `ReentrantWriterCall`.

`CommitAsync` captures the original bytes of pages it will overwrite, then writes the new pages in page-number order and flushes the stream. The transaction ends whether or not `CommitAsync` succeeds:

- If it fails before the first page write starts, the file is unchanged and `IsRolledBack` is `true`.
- Once the first page write starts, cancellation is ignored through replay and recovery.
- If a stream write or flush fails, commit restores the original page bytes and file length. Successful recovery sets `IsRolledBack` to `true`, restores the writer's cached state and AutoNumber counters, and surfaces the original failure. The writer remains usable.
- If recovery also fails, both `IsCommitted` and `IsRolledBack` remain `false`. Further mutations, transaction starts and maintenance throw with `WriterFaulted`; disposal releases resources without flushing or rewriting the file. Restore the database from a known-good copy before reopening it.

Recovery uses in-process before-images. It cannot recover from a process crash or power loss; there is no persistent rollback journal.

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
// <auto-generated />
#nullable enable

using System;

namespace GeneratedModels;

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

Table and column names are converted to PascalCase C# identifiers: spaces, hyphens and other special characters are dropped, a leading digit gets a `_` prefix, and a name with no letters or digits becomes `Unknown`. A property named like its class or like a member every object or record has (`ToString`, `Equals`, `GetHashCode`, `GetType`, `MemberwiseClone`, `ReferenceEquals`, `EqualityContract`, `PrintMembers`, `Clone`) gets a `Value` suffix (`ToStringValue`), a navigation named that way a `Navigation` suffix, and a name another property already has a numeric suffix (`Name2`). The using directives come before the namespace declaration, so a namespace such as `MyApp.System` works. When the identifier differs from the Access name by more than case, the generated code carries `[Table("Order Details")]` on the class or `[Column("Unit Price")]` on the property, so the properties read, write and query the original columns, and `Include` navigations to the entity resolve the original table.

Every table gets a class, and a file, of its own. A table whose cleaned name is its own name, ignoring case, keeps that name first. A class name that the generated code itself uses gets an `Entity` suffix: `System`, `JetDatabaseWriter`, `ColumnAttribute`, `TableAttribute`, the property types (`DateTime`, `DateTimeOffset`, `TimeSpan`, `DateOnly`, `TimeOnly`, `Guid`, `Hyperlink`, and the type of any other column) and the member names above, so a `Column Attribute` table becomes `ColumnAttributeEntity` and a `DateTime` table `DateTimeEntity`. A name another table already has, ignoring case, gets a numeric suffix: with tables `OrderDetails` and `Order Details`, the second becomes `OrderDetails2`. Navigations are emitted only between tables that are generated. A `--namespace` that is not a valid C# namespace stops the tool with exit code 1 before it writes anything. So does one with a segment named like a type the generated code names: `ColumnAttribute`, `TableAttribute`, or a property type, as in `MyApp.DateTime` or `Guid.Models`. C# looks a type name up in the namespace and in each enclosing namespace before it consults the usings above them, so such a segment would hide the type. For the same reason, a type or namespace of your own with one of those names in an enclosing namespace, such as a `MyApp.Hyperlink` class when you scaffold into `MyApp.Data`, takes the place of the type the generated code means. The tool cannot see your project, so rename that type or namespace, or scaffold into a namespace outside the one that holds it.

When the database declares foreign-key relationships, each generated entity also gets **navigation properties** inferred from `MSysRelationships`: a reference to the parent (named after the foreign-key column, EF-style — `CustomerID` → `Customer`) and a collection of children (named after the child table). These pair directly with `reader.Query<T>(...).Include(...)` (with `using JetDatabaseWriter.Linq;`):

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
    PageCacheSize = 256,             // writer page frames; 0 disables caching
    MaxTransactionPageBudget = 16_384, // pages one transaction may hold in memory (default: 16384); OpenAsync and
                                       // CreateDatabaseAsync throw ArgumentOutOfRangeException for 0 or less
};
await using var writer = await AccessWriter.OpenAsync("database.mdb", writerOptions);
```

---

## Error Handling

Constraint, lookup, schema, lock and transaction-state refusals implement `IJetException` in `JetDatabaseWriter.Exceptions`. Use `ErrorCode` for programmatic handling and `ErrorInfo` for available table, column, index, relationship and page context. These exceptions retain their previous BCL base types and messages. Other failure paths are being converted separately; callers should still handle ordinary I/O and argument errors.

```csharp
try { var dt = await reader.ReadTableAsync("Orders"); }
catch (FileNotFoundException)   { /* file missing */ }
catch (ArgumentException)       { /* write: an object name Access does not allow, or an invalid definition */ }
catch (UnauthorizedAccessException) { /* no password provided, or wrong password */ }
catch (InvalidDataException)    { /* corrupt or non-JET file */ }
catch (JetLimitationException)  { /* deleted-column gap, numeric overflow, or write: a table whose indexes cannot be enforced or maintained (an insert, update, delete or column change of that table is refused before it changes anything; a failed attachment or multi-value insertion also rolls back its parent-reference changes), a row larger than one data page, a table over 255 columns, or Jet3 text outside the database's code page */ }
catch (NotSupportedException)   { /* write: CLR type not mappable to a Jet column, or table definition too large for one TDEF page */ }
catch (ObjectDisposedException) { /* reader already disposed */ }
```

---

## Encryption Support

Native JET4 encrypted and password-only files are covered by Microsoft DAO-created fixtures. The reader verifies the creation-date-masked header password independently of the encoding key. Encrypted pages use RC4 with the unmasked four-byte encoding key XOR the little-endian page number. The writer can update these files while preserving their original key and password. Creating, removing or changing JET4 password protection is currently refused before mutation: those operations also require native system-security metadata that the library does not yet update.

Pass passwords through `AccessReaderOptions.Password` or `AccessWriterOptions.Password`. Linked databases receive credentials only from the explicit `LinkedSourcePasswordResolver`; the host database's password is not forwarded.

ACE encryption remains incomplete. The Office Standard and Agile cryptographic implementations and CFB container tests do not establish compatibility with every Access encryption provider or generation. Library-generated fixtures are not Microsoft interoperability evidence. Native flat ACE and Office compound-container behavior, including write and durable-commit limitations, remain tracked under F3–F6 in [the TODO](docs/todo.md#f--real-formats-and-encryption).

Untrusted encrypted descriptors and compound streams are bounded before allocation or expensive key derivation. `MaxEncryptionSpinCount`, `MaxEncryptionInfoBytes` and `MaxEncryptionContainerBytes` configure resource ceilings; a limit refusal does not mean the file is corrupt. Malformed descriptors and sector chains fail deliberately.

---
## Limitations

The items below are either **not yet implemented** or are important behavioral caveats, and are the most likely places to hit a wall.

### Thread safety and concurrent access
- **Do not treat a single `AccessReader` / `AccessWriter` instance as a parallel worker.** Low-level page I/O is funneled through one internal gate, so overlapping calls on the same instance block behind each other rather than running in parallel; `AccessWriter` also allows only one active explicit transaction per instance. **Concurrent writers against the same file will corrupt it.** An `AccessWriter` assumes no other process writes the file while it is open: it keeps the table list, AutoNumber counters and enforced relationships it has read for the rest of the session. Opened by path, it holds the file so other processes can only read it; opened on a stream you supply, keeping other writers away is up to you. Open with `UseLockFile = true` and `RespectExistingLockFile = true` (both defaults) to fail fast when another process already holds the database. Page byte-range locks use `FileStream.Lock` where .NET supports it, such as Windows, Linux, and Android; on unsupported platforms such as iOS, macOS, and tvOS, the option is a no-op and lockfiles or external coordination are the authoritative protection.
- **An `AccessReader` does not see changes made to a table after it has read it.** It assumes the file does not change while it is open, and keeps the table list, each table's definition and owned data pages, and up to `PageCacheSize` recently read pages until it is disposed. The default `FileShare.ReadWrite` lets Access or another process have the file open for writing at the same time, but the reader keeps a table's columns, row count, indexes and pages as it first read them, and a later read may combine pages cached before a change with pages read after it. Open a new reader to read the file as it is now.

### Transaction durability

- **There is no crash recovery.** Undo is private to the open writer; its temporary files are not recoverable journals. A process crash or power loss during an early spill or final write-back can leave a partial database; a second I/O failure while restoring a failed write-back faults the writer. See [Transactions](#transactions) for the recovery and disposal contract.
- **Statement page buffers are bounded; undo for memory and container stores grows with the call.** File-backed stores that support in-place page writes spill larger undo logs to temporary files. `MaxTransactionPageBudget` limits explicit transactions only.
- **Cancellation is honoured between spill batches and before final write-back.** Each started physical batch and any recovery finish without cancellation. A cancelled row or schema call restores earlier spills; inside an explicit transaction, it rolls back to its internal savepoint and leaves the transaction usable.
- **Physical shrinking, initial database creation and encrypted-container rewrapping use separate lifecycles.** The statement rollback contract above does not make those operations crash-safe or make container replacement atomic.

### Encryption
- **`AccessWriter` cannot open Access-native flat Agile (`AccessEncryptionFormat.AccdbAgile`) files.** `OpenAsync` throws `NotSupportedException` and leaves the file untouched. This is the format `EncryptAsync` picks by default for `.accdb`. Decrypt, edit, and re-encrypt, or use `AccessEncryptionFormat.AccdbAgileCfb` for files the writer must open. See [Encryption Support](#encryption-support).

### Column defaults and validation rules
- **Persisted `DefaultValue` and `ValidationRule` expressions run on the library's own expression engine.** An expression that uses syntax or a function the engine does not support (for example `DLookUp`) refuses the write before mutation. `GenGUID()` generates a GUID; `CurrentUser()` returns `Admin` for the currently supported session without workgroup authentication. Table-level (record) validation rules are not enforced, and a CLR `ValidationRule` delegate binds only the writer that created the table.

### Table and row size
- **A row must fit on one data page: 2,036 bytes on Jet3, 4,080 on Jet4 and ACE.** MEMO values over 1,024 bytes and OLE values over 256 bytes are stored on separate long-value pages and take 12 bytes in the row; shorter ones stay in the row while it fits. Microsoft Access also moves long values out of a row too long for a page. The writer moves the largest MEMO and byte-array OLE values first, until the row fits, so three 1,000-character MEMO values on Jet3, or five on Jet4 and ACE, are written; whether Access picks the same values is unchecked. An OLE value given as a string stays in the row. A row still too long with every such value moved, because of its other columns, its OLE strings or the 12-byte headers of many long values, throws `JetLimitationException` before anything is written, long-value pages included. An update that grows a row too far, one of its own rows or a dependent row its cascade rewrites, throws the same exception before it changes any row.
- **A table holds at most 255 columns in every format**, matching Access's field limit. `CreateTableAsync` and `AddColumnAsync` throw `JetLimitationException` for a 256th column before writing anything.

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
