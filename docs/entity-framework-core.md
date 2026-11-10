# Entity Framework Core

`JetDatabaseWriter.EntityFrameworkCore` maps EF Core models directly to Microsoft
Access databases through the managed reader and writer. It requires no Access,
OleDB or ODBC installation. The provider targets .NET 10 and .NET 11 and uses EF
Core 10. Install the provider package; it brings in the core library:

```sh
dotnet add package JetDatabaseWriter.EntityFrameworkCore
```

## Create, save and query

Import `JetDatabaseWriter.EntityFrameworkCore.Extensions` for
`UseJetDatabaseWriter` and `Microsoft.EntityFrameworkCore` for the ordinary EF
query and model extensions:

```csharp
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore;

var options = new DbContextOptionsBuilder<ContactsContext>()
    .UseJetDatabaseWriter("contacts.accdb")
    .Options;

await using var context = new ContactsContext(options);
await context.Database.EnsureCreatedAsync();

context.Contacts.Add(new Contact { Name = "Alice" });
await context.SaveChangesAsync();

var names = await context.Contacts
    .AsNoTracking()
    .Where(contact => contact.Id > 0)
    .OrderBy(contact => contact.Id)
    .Take(20)
    .Select(contact => contact.Name)
    .ToListAsync();

public sealed class ContactsContext(DbContextOptions<ContactsContext> options)
    : DbContext(options)
{
    public DbSet<Contact> Contacts => Set<Contact>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Contact>().ToTable("Contacts");
        modelBuilder.Entity<Contact>().Property(contact => contact.Name)
            .HasMaxLength(100);
    }
}

public sealed class Contact
{
    public int Id { get; set; }

    public string Name { get; set; } = string.Empty;
}
```

An integer generated key maps to AutoNumber. Explicit keys and custom sentinels
follow EF metadata. Configure `ValueGeneratedNever()` when the application
supplies every key. Normal tracking, optimistic concurrency tokens, owned
entities, complex properties, TPH inheritance, relationships, `Include` and
`ThenInclude` are supported. Use `HasConversion` and EF value/key comparers for
model types needing custom storage or equality.

## Files, streams and options

ACE `.accdb` is the default creation format. Select `DatabaseFormat.Jet4Mdb` or
`DatabaseFormat.Jet3Mdb` for `.mdb` creation. Existing files retain their format.
Jet3 has no native Decimal column; use representable Currency precision and
scale, such as `HasPrecision(18, 4)`. See [writing values](writing.md#writing-data)
for format-specific limits.

```csharp
using JetDatabaseWriter;
using JetDatabaseWriter.Enums;

var options = new DbContextOptionsBuilder<ContactsContext>()
    .UseJetDatabaseWriter(
        "contacts.mdb",
        DatabaseFormat.Jet4Mdb,
        jet => jet.WithPassword("database-password")
            .WithReaderOptions(new AccessReaderOptions
            {
                MaxLongValueBytes = 16 * 1024 * 1024,
            }))
    .Options;
```

`WithWriterOptions` accepts an `AccessWriterOptions` template. Options are
snapshotted; later caller edits do not change the context's configuration.
Without an explicit writer template, the provider enables transactional writes.
The configured provider password applies to both reads and writes.

For caller-owned images, pass a readable, writable, seekable stream:

```csharp
await using var image = new MemoryStream();
var options = new DbContextOptionsBuilder<ContactsContext>()
    .UseJetDatabaseWriter(image, leaveOpen: true)
    .Options;
```

There is also a `Func<Stream>` overload. Its default is `leaveOpen: false`;
the direct-stream overload defaults to `true`. Contexts sharing a stream
coordinate reads and writer ownership. A live enumeration owns a read lease;
dispose it before starting a write or another operation on that stream.
File peers coordinate using the captured absolute path spelling; use the same
spelling for contexts that access the same database. As with EF generally, await
one operation before starting another on the same context.

## Transactions and bulk writes

Persistence, schema operations and transaction operations require asynchronous
APIs. `SaveChanges`, synchronous transaction methods and synchronous
`EnsureCreated`/`Migrate` throw with guidance to use their async equivalents.
Synchronous query APIs work by waiting on the asynchronous query pipeline;
prefer asynchronous queries for application I/O.

```csharp
await using var transaction = await context.Database.BeginTransactionAsync();
context.Contacts.Add(new Contact { Name = "Bob" });
await context.SaveChangesAsync();
await transaction.CreateSavepointAsync("after-insert");
await context.Contacts.Where(contact => contact.Name == "Bob")
    .ExecuteUpdateAsync(setters => setters.SetProperty(contact => contact.Name, "Robert"));
await transaction.CommitAsync();
```

`SaveChangesAsync` protects multiple row operations with a nested transaction.
Access supports at most five active transaction levels; a save or schema change
that cannot open its protective scope refuses before changing rows. Bulk
`ExecuteUpdateAsync` and `ExecuteDeleteAsync` use atomic writer batches and leave
already tracked entities unchanged, so reload or clear tracking before relying
on their values. See [transaction and recovery guarantees](operations.md#transactions).

## Schema creation, additive migration and seeding

`EnsureCreatedAsync` creates the modeled schema in a missing file or empty
stream. It preserves and refuses a nonempty unreadable destination, including
one opened with the wrong password.

The provider's `Database.MigrateAsync()` reconciles the current model by adding
missing tables, columns, indexes, foreign keys and seed rows. Existing rows and
objects remain in place. Conflicting existing primary keys, indexes and
relationships refuse transactionally. Existing columns are not altered; this
operation does not establish full column-facet parity with the model.

This API does not run versioned EF migration classes, maintain migration history
or generate SQL scripts. Renames, drops and changes to existing columns require
an explicit schema plan using the [core schema APIs](writing.md#schema-operations-and-preservation).

`HasData` supplies model seed rows, including inherited and owned values. Seed
rows need explicit stable keys, including shadow ownership keys for owned types.
Additive migration inserts missing rows; it does not overwrite existing seed
rows when their model values change. `UseAsyncSeeding` runs after schema work,
including when the schema is already current, and can implement application
initialization that needs queries or updates. Configure `UseAsyncSeeding` for
this provider because schema entry points are asynchronous. A seeding callback
failure rolls back its writes when no caller transaction owns them; schema
creation has already committed before the callback runs.

## Query performance and diagnostics

Prefer scalar projections and apply filters and paging before materializing
results. Supported single-table queries stream buffers, and eligible orderings
stream from native indexes. Scalar aggregates can visit a reusable buffer.
`AsNoTracking` avoids retaining returned entities in the context; use
`AsNoTrackingWithIdentityResolution` when a result graph needs shared identity.

Blocking LINQ operators, complete navigation graphs and orderings whose native
collation differs from CLR comparison can still buffer or sort. An index on a
text or GUID column does not guarantee that CLR `OrderBy` can use its native
order. The provider retains the correct comparison and paging semantics.

Enable EF query-category Debug logging to see `JetEventId.QueryExecution`,
`TableRead` and `TableReadCompleted`. These report the execution strategy,
admitted index or scan, required sorting and completed enumeration counts.
Payloads omit query text, parameters and row values, including with sensitive
logging enabled.

See [execution details](design/ef-query-execution.md) and
[hosted performance evidence](design/ef-query-performance.md) for optimized
shapes, remaining blocking paths and the scope of measured workloads.
