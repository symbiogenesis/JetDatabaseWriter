# Reading and querying data

[Back to the README](../README.md) · [Writing guide](writing.md) · [Operations guide](operations.md)

Choose the API that fits your task:

- `Rows<T>` streams rows into your C# model; use it for large tables.
- `ReadTableAsync<T>` loads a list, while `ReadTableAsync` loads a `DataTable`.
- `Query<T>` supports query composition and relationship loading.
- `FromIndex<T>` gives explicit control over an index scan.

Examples assume an open `AccessReader` named `reader`, your own table models, and the relevant `JetDatabaseWriter`, `JetDatabaseWriter.Models` and `JetDatabaseWriter.Enums` namespaces. See [opening a reader](operations.md#opening-a-reader-or-writer).

## Contents

- [Mapping rows to C# models](#generic-poco-mapping--recommended)
- [Index-backed reads](#index-backed-reads)
- [Relationships and eager loading](#relationships-and-eager-loading-include--theninclude)
- [Attachments and multi-value columns](#complex-attachment--multi-value-column-metadata)
- [Streaming large tables](#streaming-large-tables)
- [Async LINQ](#querying-with-async-linq)
- [Statistics and metadata](#statistics--metadata)

## Reading Data

Reads that name a missing table throw `JetObjectNotFoundException` with
`TableNotFound`, including metadata, row, index and complex-item reads. Use
`TryLookupTableAsync` to check whether a local or linked table name exists;
this check reads the catalog without opening a linked source and returns
`false` only for an absent name. An unusable catalog entry reports
`CorruptCatalog`; an existing empty table still returns an empty result.

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

A narrow `Rows<T>` or `ReadTableAsync<T>` projection reads only its bound columns. When ordinary scalar auto-properties are combined with OLE `byte[]` properties, the reader assigns scalars directly and resolves the exact stored OLE bytes in the same scan before returning each object. Unbound long-value columns are not decoded. Each returned object owns its complete bound payloads. Unsupported mappings continue through the projection-aware mapper; see the [typed scan details and measurements](design/read-performance-bottlenecks.md#mixed-scalar-and-ole-typed-scans-2026-10-07).

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

`ReadTableAsync`, `ReadAllTablesAsync`, and the string-typed DataTable APIs fully materialize their results. They are convenient for data binding, previews, exports, and compatibility code; for bulk processing or large-table scans, prefer `Rows(...)` or `Rows<T>(...)` so rows stream lazily and can short-circuit through async LINQ.

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

The navigation's target type is matched to the related table by name (ignoring case and non-alphanumeric separators), so the entity classes the [scaffolder](scaffolding.md#scaffolding--generate-c-models-from-a-database) generates work as-is; annotate a type with `[Table("ActualName")]` (`System.ComponentModel.DataAnnotations.Schema.TableAttribute`) to bind a POCO whose name doesn't match its table. When the join columns are indexed (a primary key or foreign-key index, inferred automatically) each `Include` / `ThenInclude` loads the related rows with one index seek per distinct key; otherwise it scans the related table once and joins in memory, and a shared include prefix (two `ThenInclude` chains off the same `Include`) loads only once. Inline collection filters apply per parent in memory and may only reference the child being filtered. Supported operators the engine doesn't translate natively run in memory over the streamed rows: a `Select` projection and, after it, `Where`, the orderings (with or without a comparer, and `Order`/`OrderDescending`), `Skip`/`Take` with their `While` and `Last` forms, `Distinct`, `Concat`/`Union`/`Intersect`/`Except` (whose second sequence may be another `Query<T>`), `Reverse`, `DefaultIfEmpty`, `Append`/`Prepend`, `Cast` and `OfType`. Streaming operators such as `Select` and `Where` let a following `Take` or `FirstAsync` stop the read early; orderings, `Reverse` and `TakeLast` consume their whole input first, and `Intersect`/`Except` buffer the second sequence. `GroupBy`, `SelectMany`, the joins, `Zip`, `Chunk`, the `DistinctBy`-style set operators and the overloads whose lambdas take an element index throw `NotSupportedException` naming the operator and `AsAsyncEnumerable()`: run them over `query.AsAsyncEnumerable()` with the async LINQ operators instead. Relationships are read from `MSysRelationships`, which Access-authored Jet3 files can contain. Databases without that catalog table return no relationships.

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

The parent-row predicate must match exactly one row (zero or multiple matches throw `InvalidOperationException`). As in Access, every row inserted into a table with complex columns gets one per-row complex reference, shared by all its complex columns, from the table's complex AutoNumber counter in its TDEF; the reference of a deleted row is never handed out again, and rows added to an Access-authored table never take a reference an Access row owns. A complex column that `AddColumnAsync` adds takes each row's reference, and `UpdateRowsAsync` refuses to assign a complex column (`ArgumentException`), so a row keeps its reference and its items. Attachment payloads are stored in Access's `FileData` wrapper (4-byte typeFlag + uncompressed length + a header with the lowercased extension + the file): files Access stores raw (jpg, jpeg, gif, png, zip, cab, docx, xlsx, xlsb, pptx) are stored as is, and everything else as a zlib stream, as Access does; compressed payloads must have a valid native zlib wrapper, declared uncompressed length and Adler-32 checksum. Payloads larger than the 64-byte inline limit are pushed onto freshly allocated Access-style LVAL pages (single-page or chained form with the `LVAL` page signature) and reassembled by the reader; the writer currently limits each payload to 16,777,215 bytes. Native LVAL lengths occupy 30 bits. Readers default to the same bounded size and can opt into a larger MaxLongValueBytes budget; large-value interoperability beyond the default remains unverified.

A multi-value column of `decimal` items stores them with the definition's `NumericPrecision` and `NumericScale`, which default to Access's Decimal(18,0), whole numbers; declare them to keep fractions, for example `new ColumnDefinition("Amounts", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(decimal), NumericPrecision = 10, NumericScale = 2 }`.

Typed inserts accept a positive integral per-row reference for a complex property, or null/default to allocate the row's reference. Supplied references across the row must agree. Attachment objects, collections and encoded complex cell byte arrays are not typed-insert payloads: invalid supplied values throw `ArgumentException` naming `item` before mutation. Use the attachment and multi-value APIs to add items.

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

Hyperlink columns are MEMO columns whose TDEF flag byte has the `HYPERLINK_FLAG_MASK = 0x80` bit set. Microsoft Access stores the value as a single `#`-delimited string (`displaytext#address#subaddress#screentip`); the library round-trips that structure as a strongly-typed [`Hyperlink`](../JetDatabaseWriter/Models/Hyperlink.cs) record.

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

POCO mapping accepts either a `Hyperlink` property or a plain `string` property — the conversion runs both ways. Compatibility surfaces (`RowsAsStrings`, `ReadTableAsStringsAsync`) continue to yield the raw `#`-delimited form. See [docs/design/hyperlink-format-notes.md](design/hyperlink-format-notes.md) for the on-disk layout, escape semantics, and round-trip rules.

### OLE Object columns

An OLE Object column reads as `byte[]` holding the value's stored bytes, from every read API: `Rows`, `ReadTableAsync`, `Rows<T>`, `ReadTableAsync<T>`, `Query<T>`, `FromIndex` and `SeekRowsAsync`. That is what DAO, ADO and Jackcess return. A value your code wrote comes back byte for byte. An object Microsoft Access inserted (a file dropped into the field, a Word document, a linked file) keeps Access's OLE header and the OLE object stream around it; `OleObjectValue` unwraps it on request:

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
