# JetDatabaseWriter

[![NuGet](https://img.shields.io/nuget/v/JetDatabaseWriter.svg)](https://www.nuget.org/packages/JetDatabaseWriter/)
[![Downloads](https://img.shields.io/nuget/dt/JetDatabaseWriter.svg)](https://www.nuget.org/packages/JetDatabaseWriter/)
[![.NET Standard](https://img.shields.io/badge/.NET%20Standard-2.1-blue)](https://docs.microsoft.com/en-us/dotnet/standard/net-standard)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Pure-managed .NET library for reading and writing Microsoft Access JET databases — no OleDB, ODBC, or ACE/Jet driver installation required.

> See [CHANGELOG.md](CHANGELOG.md) and the [migration guide](#migration-from-v1) for breaking changes from v1.

---

## Features

| | |
|---|---|
| ✅ **No native dependencies** | Pure C# — runs anywhere .NET runs |
| ✅ **Jet3 & Jet4 / ACE** | Access 97 through Access 2021 and Microsoft 365 (`.mdb` / `.accdb`) |
| ✅ **Typed by default** | `int`, `DateTime`, `decimal`, `Guid` — not just strings |
| ✅ **All column types** | Text, Integer, Currency, Date/Time, GUID, MEMO, OLE Object, Decimal |
| ✅ **Streaming API** | Process millions of rows without loading the whole file |
| ✅ **Async support** | Async-first `ValueTask<T>` API, `OpenAsync(...)` + `await using` (`IAsyncDisposable`) |
| ✅ **Stream support** | Open from any seekable `Stream` (byte arrays, blobs, embedded resources) |
| ✅ **Page cache** | 256-page LRU cache (~1 MB, configurable) |
| ✅ **Generic POCO mapping** | `ReadTable<T>()`, `Rows<T>()`, `InsertRow<T>()` — no manual casting |
| ✅ **LINQ-friendly streaming** | `reader.Rows<T>("...").Where(...).Take(...).ToListAsync(ct)` — standard async LINQ over `IAsyncEnumerable<T>` |
| ✅ **Progress reporting** | `IProgress<int>` callbacks on all long operations |
| ✅ **Non-Western text** | Code page auto-detected from the database header |
| ✅ **OLE Objects** | Detects embedded JPEG, PNG, PDF, ZIP, DOC, RTF |
| ✅ **Write support** | Create databases, Create/drop tables, insert/update/delete rows (Jet3/Jet4/ACE) |
| ✅ **Encryption & passwords** | Jet3 page-level XOR, Jet4 `.mdb` RC4, legacy password-only `.accdb` (`;pwd=`), AES-128 page-encrypted Access 2007+ `.accdb` (CFB-wrapped), and Office Crypto API ECMA-376 "Agile" (SHA-512 PBKDF + AES-CBC) used by Access 2010 SP1+ / Microsoft 365 |
| ✅ **Linked tables** | Read source paths / foreign names via `ListLinkedTablesAsync()`; create Access (type 4) and ODBC (type 6) links via `CreateLinkedTableAsync` / `CreateLinkedOdbcTableAsync` |
| ✅ **Foreign-key relationships** | Read existing relationships via `ListIndexesAsync` (`Kind = ForeignKey`); create via `CreateRelationshipAsync` — `MSysRelationships` catalog rows (W9a) + per-TDEF FK logical-index entries on both sides with real-idx sharing (W9b, Jet4/ACE only); **runtime referential-integrity enforcement** on `InsertRowAsync` / `UpdateRowsAsync` / `DeleteRowsAsync`, including cascade-update and cascade-delete (W10) |
| ✅ **Complex fields** | Attachment and multi-value columns resolved via `MSysComplexColumns` FK lookup |
| ✅ **Lockfile support** | Creates `.ldb` / `.laccdb` lockfile on open, deletes on disposal (opt-out) |

---

## Installation

```bash
dotnet add package JetDatabaseWriter
```

```powershell
Install-Package JetDatabaseWriter
```

### NuGet target compatibility

`JetDatabaseWriter` targets **`netstandard2.1`**, which is consumed by the following .NET surfaces:

| Consumer | Minimum version |
|----------|----------------|
| .NET Core | 3.0 |
| .NET | 5 / 6 / 7 / 8 / 9 |
| Mono | 6.4 |
| Xamarin.iOS | 12.16 |
| Xamarin.Android | 10.0 |
| Unity | 2021.2+ |

---

## Quick Start

```csharp
using JetDatabaseWriter;
using System.Threading;

public class Order
{
    public int OrderID { get; set; }
    public DateTime OrderDate { get; set; }
    public decimal Freight { get; set; }
}

using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
await using var reader = await AccessReader.OpenAsync("database.mdb", cancellationToken: cts.Token);

List<string> tables = await reader.ListTablesAsync(cts.Token);
Console.WriteLine($"Found {tables.Count} tables: {string.Join(", ", tables)}");

List<Order> orders = await reader.ReadTableAsync<Order>("Orders", maxRows: 100, cts.Token);
foreach (Order o in orders)
    Console.WriteLine($"#{o.OrderID}  {o.OrderDate:yyyy-MM-dd}  {o.Freight:C}");
```

`OpenAsync(...)` + `await using` is the recommended pattern.

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

List<Product> products = await reader.ReadTableAsync<Product>("Products", maxRows: 100, cancellationToken);
decimal total = products.Where(p => !p.Discontinued).Sum(p => p.UnitPrice);
```

Property names are matched to column headers **case-insensitively**. Unmatched properties keep their default value. The type `T` must be a class with a parameterless constructor.

### Typed DataTable

```csharp
DataTable? dt = await reader.ReadDataTableAsync("Products", cancellationToken: cancellationToken);
// dt.Columns["ProductID"].DataType    == typeof(int)
// dt.Columns["UnitPrice"].DataType    == typeof(decimal)
// dt.Columns["Discontinued"].DataType == typeof(bool)
```

### Column metadata

```csharp
List<ColumnMetadata> meta = await reader.GetColumnMetadataAsync("Products", cancellationToken);
foreach (ColumnMetadata col in meta)
{
    Type   clrType = col.ClrType;         // e.g. typeof(int), typeof(string)
    string display = col.Size.ToString(); // e.g. "4 bytes", "255 chars", "LVAL"
    Console.WriteLine($"{col.Name}: {clrType.Name} ({col.Size})");
}
```

### Index metadata

`ListIndexesAsync` returns the logical indexes declared on a table — primary keys, foreign-key indexes, and ordinary user indexes — parsed directly from the TDEF page chain. Only schema metadata is surfaced; the index B-tree leaf pages are not traversed.

```csharp
IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("Companies", cancellationToken);
foreach (IndexMetadata idx in indexes)
{
    string keys = string.Join(", ", idx.Columns.Select(c => c.Name));
    Console.WriteLine($"{idx.Name}: {idx.Kind} on ({keys})  unique={idx.IsUnique}  fk={idx.IsForeignKey}");
}
```

Multiple logical indexes can share the same physical index — consult `IndexMetadata.RealIndexNumber` to detect that sharing. The `IndexKind` enum distinguishes `Normal`, `PrimaryKey`, and `ForeignKey`. Note: Access does not always set the `IsUnique` flag bit on primary keys (uniqueness is implied by `Kind == PrimaryKey`).

### Complex (Attachment / Multi-value) column metadata

`GetComplexColumnsAsync` joins the parent TDEF column descriptors with `MSysComplexColumns` to expose the per-column `ComplexID`, the hidden flat child-table name, and the column subtype (Attachment, MultiValue, or VersionHistory).

```csharp
IReadOnlyList<ComplexColumnInfo> complex = await reader.GetComplexColumnsAsync("Documents", cancellationToken);
foreach (ComplexColumnInfo c in complex)
{
    Console.WriteLine($"{c.ColumnName}: {c.Kind}  flat={c.FlatTableName}  template={c.ComplexTypeName}");
}
```

Returns an empty list for tables without complex columns and for older Jet3 / Jet4 (`.mdb`) files.

#### Reading and writing complex column rows (Phase C4)

For ACE `.accdb` files, attachments and multi-value items can be inserted into an existing parent row and read back via spec-compliant APIs:

```csharp
// Insert an attachment into the row whose Id = 1
await writer.AddAttachmentAsync(
    "Documents",
    "Files",
    new Dictionary<string, object> { ["Id"] = 1 },
    new AttachmentInput("notes.txt", File.ReadAllBytes("notes.txt")),
    cancellationToken);

// Insert a multi-value tag item
await writer.AddMultiValueItemAsync("Tags", "Items", new Dictionary<string, object> { ["Id"] = 1 }, "red", cancellationToken);

// Read back
IReadOnlyList<AttachmentRecord> attachments = await reader.GetAttachmentsAsync("Documents", "Files", cancellationToken);
IReadOnlyList<(int ConceptualTableId, object? Value)> tags = await reader.GetMultiValueItemsAsync("Tags", "Items", cancellationToken);
```

The parent-row predicate must match exactly one row (zero or multiple matches throw `InvalidOperationException`). Attachment payloads are wrapped per MS-ACCDB §3.1 (4-byte typeFlag + dataLen + extension + payload, with raw-deflate compression skipped for already-compressed extensions). Payload size is capped at ~256 bytes per file by the writer's inline-OLE limitation.

### String DataTable — compatibility

```csharp
// All values as strings
DataTable preview = await reader.ReadTableAsStringsAsync("Products", maxRows: 20, cancellationToken);

// String row access
string firstCell = preview.Rows[0][0].ToString();
```

---

## Streaming Large Tables

### Generic streaming — recommended

```csharp
var progress = new Progress<int>(n => Console.Write($"\r{n:N0} rows"));

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

Filtering and projection run client-side per row — there is no SQL engine underneath — but enumeration is fully lazy and `Take`/`First` short-circuit the underlying page reader.

---

## Async Operations

```csharp
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
await using var reader = await AccessReader.OpenAsync("database.mdb", cancellationToken: cts.Token);

List<Order>                   orders = await reader.ReadTableAsync<Order>("Orders", 50, cts.Token);
List<string>                  tables = await reader.ListTablesAsync(cts.Token);
DataTable?                    dt     = await reader.ReadDataTableAsync("Orders", cancellationToken: cts.Token);
DataTable                     str    = await reader.ReadTableAsStringsAsync("Orders", 50, cts.Token);
DatabaseStatistics            stats  = await reader.GetStatisticsAsync(cts.Token);
Dictionary<string, DataTable> all    = await reader.ReadAllTablesAsync(cancellationToken: cts.Token);
Dictionary<string, DataTable> allStr = await reader.ReadAllTablesAsStringsAsync(cancellationToken: cts.Token);
```

All async APIs return `ValueTask<T>` and can be awaited directly. Reader/writer instances also implement `IAsyncDisposable`, so prefer `await using`.

---

## Bulk Operations

```csharp
// Typed columns
Dictionary<string, DataTable> all = await reader.ReadAllTablesAsync(
    new Progress<TableProgress>(p => Console.WriteLine($"Reading {p.TableName} ({p.TableIndex + 1}/{p.TableCount})...")),
    cancellationToken);

// String columns (compatibility)
Dictionary<string, DataTable> allStr = await reader.ReadAllTablesAsStringsAsync(
    cancellationToken: cancellationToken);
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
        DefaultValueExpression   = "0",
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
| `IsPrimaryKey` | ✅ TDEF logical-index entry with `index_type = 0x01` | Shortcut for synthesizing a PK `IndexDefinition` named `"PrimaryKey"` from one or more columns (in declaration order). Forces the PK key columns to `IsNullable = false` on the emitted TDEF. Mixing this with an explicit PK `IndexDefinition` in the same call throws `ArgumentException`. Multi-column PKs ship the schema only — the B-tree leaf is not maintained on subsequent mutations (single-column PKs are). |
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

### Update & delete

```csharp
int updated = await writer.UpdateRowsAsync("Contacts", "ContactID", 1,
    new Dictionary<string, object> { ["Score"] = 99.9m });

int deleted = await writer.DeleteRowsAsync("Contacts", "ContactID", 3);
```

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

Linked tables are catalog-only entries that point at data living in another database. The library writes the `MSysObjects` row; readers (this library, Microsoft Access, etc.) follow the entry to fetch the data on demand.

```csharp
// Linked Access table (MSysObjects type 4) — references a table in another .mdb / .accdb file.
await writer.CreateLinkedTableAsync(
    linkedTableName:    "RemoteOrders",
    sourceDatabasePath: @"C:\Data\Backend.accdb",
    foreignTableName:   "Orders");

// Linked ODBC table (MSysObjects type 6) — references a table over an ODBC connection.
// The "ODBC;" prefix is added automatically when omitted.
await writer.CreateLinkedOdbcTableAsync(
    linkedTableName:  "LinkedSalesOrders",
    connectionString: "ODBC;DRIVER={SQL Server};SERVER=db.example.com;DATABASE=Sales;Trusted_Connection=Yes",
    foreignTableName: "dbo.Orders");
```

> The library only writes the catalog metadata. It does not open the ODBC source itself — reading an ODBC-linked table from this library is not supported. Use `ListLinkedTablesAsync()` to enumerate linked entries.

### Foreign-key relationships

Declare a relationship between two existing tables. The library appends one row per FK column to the `MSysRelationships` system table (the source Access reads to populate the Relationships designer) **and** emits a per-table FK logical-index entry on both the PK-side and FK-side TDEFs (Jet4/ACE only — Jet3 `.mdb` files get only the catalog rows). Existing real-idx slots are reused when their `col_map` already covers the FK columns; otherwise a new real-idx slot plus an empty leaf page are appended on each side. **Runtime referential integrity is enforced on `InsertRowAsync` / `UpdateRowsAsync` / `DeleteRowsAsync`** for any relationship created with `EnforceReferentialIntegrity = true` (the default); `CascadeUpdates` / `CascadeDeletes` honour the cascade flags. See the Limitations section for the full contract.

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

> Requires a database that already contains the `MSysRelationships` catalog table — every Access-authored `.mdb` / `.accdb` does, but databases freshly created by `AccessWriter.CreateDatabaseAsync` do not. Open or copy an Access-authored fixture first, or `CreateRelationshipAsync` throws `NotSupportedException`.

---

## Statistics & Metadata

```csharp
foreach (ColumnMetadata col in await reader.GetColumnMetadataAsync("Orders", cancellationToken))
    Console.WriteLine($"{col.Ordinal}. {col.Name} — {col.TypeName} ({col.ClrType.Name})");

// Table-level stats (single catalog scan)
foreach (TableStat ts in await reader.GetTableStatsAsync(cancellationToken))
    Console.WriteLine($"{ts.Name}: {ts.RowCount:N0} rows, {ts.ColumnCount} cols");

// First table preview as DataTable
DataTable first = await reader.ReadFirstTableAsync(maxRows: 20, cancellationToken);
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

---

## Configuration

```csharp
var options = new AccessReaderOptions("secretPassword")
{
    PageCacheSize            = 512,    // pages in LRU cache (default: 256)
    ParallelPageReadsEnabled = true,   // parallel I/O (default: false)
    DiagnosticsEnabled       = false,  // verbose logging (default: false)
    ValidateOnOpen           = true,   // format check on open (default: true)
    FileAccess               = FileAccess.Read,   // default
    FileShare                = FileShare.Read,    // default: others may read, writes blocked
    // FileShare             = FileShare.ReadWrite // use when Access has the file open
    UseLockFile              = true,   // create .ldb/.laccdb lockfile (default: true)
    LinkedSourcePathAllowlist = new[] { @"C:\TrustedLinkedDatabases" },
    LinkedSourcePathValidator = (link, fullPath) => !link.IsOdbc,
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

All password-protected formats produced by Microsoft Access from Access 97 through Microsoft 365 are fully **read- and write-supported**. Supply the password via [`AccessReaderOptions.Password`](JetDatabaseWriter/Core/AccessReaderOptions.cs) or [`AccessWriterOptions.Password`](JetDatabaseWriter/Core/AccessWriterOptions.cs); the format is auto-detected from the file header.

- **In-place mutation.** All formats (Jet3 XOR, Jet4 RC4, ACCDB legacy `;pwd=`, AES-128 CFB-wrapped, and Office Crypto API "Agile") are writable in place — modified pages are re-encrypted on flush, and Agile containers are re-emitted on `DisposeAsync`.
- **Encryption mutation APIs.** `AccessWriter.EncryptAsync(path, password, AccessEncryptionFormat, …)`, `AccessWriter.DecryptAsync(path, password, …)`, and `AccessWriter.ChangePasswordAsync(path, oldPassword, newPassword, …)` add, remove, or rotate encryption (and switch formats) on an existing file. Use `AccessWriter.DetectEncryptionFormatAsync(path)` to discover the current format.

| Format | Versions | Detection | Key derivation | Page / payload cipher |
|---|---|---|---|---|
| Jet3 page XOR | Access 97 (`.mdb`) | header byte `0x62` bit `0x01` | static 128-byte mask | XOR (no password required) |
| Jet4 RC4 | Access 2000–2003 (`.mdb`) | header byte `0x62` value `0x02` / `0x03` | password XOR-verified at `0x42`; `dbKey` at `0x3E` | per-page RC4 with `MD5(dbKey ‖ pageNumber)` |
| ACCDB legacy password | Access 2007+ (`.accdb`, `;pwd=...`) | header byte `0x62` value `0x07` | password XOR-verified at `0x42` | none (password only) |
| ACCDB AES-128 (CFB-wrapped) | Access 2007+ (`.accdb`) | CFB magic `D0 CF 11 E0` + Jet4-style header password | SHA-256(password) → 16 bytes | per-page AES-128-ECB |
| ACCDB Agile (Office Crypto API) | Access 2010 SP1+, Microsoft 365 (`.accdb`) | CFB compound document with `EncryptionInfo` (version 4.4, flag `0x40`) and `EncryptedPackage` streams | ECMA-376 §2.3.4.11 PBKDF: SHA-512 + `spinCount` iterations + spec block keys (`0xfea7d2763b4b9e79`, `0xd7aa0f6d3061344e`, `0x146e0be7abacd0d6`) | AES-256-CBC over 4096-byte segments with per-segment IV `SHA-512(keyDataSalt ‖ uint32_le(segmentIndex))[:16]` |

Wrong or missing passwords throw `UnauthorizedAccessException`. Corrupt or non-JET data throws `InvalidDataException`.

---

## Limitations

The writer covers the common create / insert / update / delete path. The items below are **not yet implemented** and are the most likely places to hit a wall.

### Indexes
- **Live B-tree maintenance covers single- and multi-column indexes, ascending or descending, optionally unique.** Jet3 (`.mdb` Access 97) rejects `IndexDefinition` entirely.
- **Indexable key types are limited.** Live leaf maintenance is supported for `Byte`, `Integer`, `Long Integer`, `Currency`, `Single`, `Double`, `Date/Time`, `GUID`, and `Text` containing only ASCII letters/digits. Other types (`Decimal`, `OLE`, `MEMO`, attachment, complex) and text with spaces/punctuation/non-ASCII round-trip as schema only — Access rebuilds the leaf on Compact & Repair. If *any* column in a multi-column index is unsupported, the whole index falls through to the schema-only path.
- **No incremental B-tree maintenance.** Each insert/update/delete rebuilds the entire B-tree (no prefix compression, no `tail_page` chain). Cost scales with row count.
- **Unique enforcement is post-write.** A duplicate row is persisted to disk before the bulk-rebuild detects the violation and throws `InvalidOperationException`; the caller must delete one of the offending rows manually before continuing. The index B-tree is left stale until the duplicate is removed.

### Primary & foreign keys
- **TDEF must fit on one page** after FK entries are appended, otherwise `NotSupportedException`.
- **`DropRelationshipAsync` leaves the orphaned real-idx slot in place.** Catalog rows and FK logical-idx entries are removed from both side TDEFs, so the relationship disappears from `ListIndexesAsync` and the writer immediately. Microsoft Access reclaims the unused real-idx slot during the next Compact & Repair pass.
- **`RenameRelationshipAsync` does not update TDEF logical-idx name cookies.** Catalog rows are rewritten with the new name; per-TDEF FK logical-idx names stay at the original value. Access regenerates them from the catalog row on the next Compact & Repair pass.
- **RI enforcement uses an O(N) parent scan** (no index seek). Parent-key sets are cached per `InsertRowsAsync` call.
- **Not yet validated end-to-end through Microsoft Access.** Files produced with `IndexDefinition` lists or `CreateRelationshipAsync` have not been round-tripped through a Compact & Repair pass on Windows.

### Specialized column kinds
- **Attachment / multi-value (complex) columns — partial.** Schema creation (with the per-flat-table primary-key, FK back-reference, and (attachment-only) composite secondary index Access expects), row-level inserts, spec-compliant reads, cascade-on-delete from the parent row, and `DropTableAsync` cascade for the hidden flat child tables work for ACE `.accdb`. Still missing:
  - LVAL chain emission for attachment payloads — current cap is ~256 bytes per file (inline-OLE limit).
  - `MSysComplexType_*` template tables (`ComplexTypeObjectID` is written as `0`).
  - `AddColumnAsync` / `DropColumnAsync` / `RenameColumnAsync` on tables that already contain attachment columns.
- **No calculated columns** (Access 2010+ expression columns).
- **No hyperlink semantics.** Hyperlink fields round-trip as plain MEMO text; the `#display#address#subaddress#` structure is not parsed or emitted.

### Forms, reports, macros, queries, VBA
- Out of scope. The library targets the JET storage layer only. `MSysObjects` entries of type Form, Report, Macro, Module, or Query are preserved on disk but are neither parsed nor editable.

### Concurrency
- **No byte-range locking and no populated `.ldb` slots.** Microsoft Access uses page-level byte-range locks via `LockFileEx` plus 64-byte machine-name / SID entries in the lockfile; this library implements neither. Concurrent writers against the same file (including Access opening it while a writer is active) will corrupt it. Keep `RespectExistingLockFile = true` (default) and `FileShare.Read` / `FileShare.None` on the writer to let the OS block other openers.
- **No transactions or rollback.** A crashed write leaves the file in whatever partially-flushed state the page cache had reached.

---

## How It Works

Based on the [mdbtools format specification](https://github.com/mdbtools/mdbtools/blob/master/HACKING.md). The library parses JET pages directly:

1. **Page 0** — header: Jet3/Jet4 detection, code page, encryption flag
2. **Page 2** — `MSysObjects` catalog: table names → TDEF page numbers
3. **TDEF pages** — table definition chains: column descriptors + names
4. **Data pages** — row slot arrays → null mask + fixed/variable fields
5. **LVAL pages** — long-value chains for MEMO and OLE fields

---

## Contributing

Issues and pull requests welcome at [github.com/diegoripera/JetDatabaseWriter](https://github.com/diegoripera/JetDatabaseWriter).

## License

MIT — see [LICENSE](LICENSE) for details.
