namespace JetDatabaseWriter.Tests.Schema;

using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Checks cached structure against real mutations, commits and rollbacks.</summary>
public sealed class SchemaCacheCoherenceTests
{
    public static TheoryData<DatabaseFormat, WriteMode, bool> FormatsAndModes
    {
        get
        {
            var data = new TheoryData<DatabaseFormat, WriteMode, bool>();
            foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
            {
                data.Add(format, WriteMode.Direct, false);
                data.Add(format, WriteMode.AutoCommit, false);
                data.Add(format, WriteMode.ExplicitCommit, false);
                data.Add(format, WriteMode.ExplicitCommit, true);
            }

            return data;
        }
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task CachedSchema_SeesDdlAndRestoresAfterRollback(DatabaseFormat format, WriteMode mode, bool rollback)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int))], ct);
        }

        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, WriteModes.WriterOptions(mode), cancellationToken: ct);
        harness.Database.TableDefs.VerifyOnHit = true;
        ResolvedTable original = await harness.Services.Catalog.ResolveRequiredTableAsync("T", ct);
        ResolvedTable repeated = await harness.Services.Catalog.ResolveRequiredTableAsync("T", ct);
        Assert.Same(original.Schema, repeated.Schema);
        Assert.Same(original.Definition, repeated.Definition);
        JetTransaction? tx = mode == WriteMode.ExplicitCommit ? await harness.BeginTransactionAsync(ct) : null;
        try
        {
            await harness.Services.Transactions.RunAutoCommitAsync(
                token => harness.Services.Schema.RenameColumnAsync("T", "Id", "Renamed", token), ct);
            ResolvedTable changed = await harness.Services.Catalog.ResolveRequiredTableAsync("T", ct);
            Assert.Equal("Renamed", Assert.Single(changed.Definition.Columns).Name);
            Assert.Equal("Id", Assert.Single(original.Definition.Columns).Name);
            Assert.NotSame(original.Schema, changed.Schema);
            if (tx is not null)
            {
                if (rollback)
                {
                    await tx.RollbackAsync(ct);
                }
                else
                {
                    await tx.CommitAsync(ct);
                }
            }

            ResolvedTable settled = await harness.Services.Catalog.ResolveRequiredTableAsync("T", ct);
            Assert.Equal(rollback ? "Id" : "Renamed", Assert.Single(settled.Definition.Columns).Name);
            Assert.NotNull(await harness.Services.Catalog.GetSchemaAsync(settled.Entry.TDefPage, cancellationToken: ct));
        }
        finally
        {
            if (tx is not null)
            {
                await tx.DisposeAsync();
            }
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CounterWrites_KeepStructuralLayoutAndReadLiveCounters(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int))], ct);
        }

        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, WriteModes.WriterOptions(WriteMode.Direct), cancellationToken: ct);
        harness.Database.TableDefs.VerifyOnHit = true;
        ResolvedTable original = await harness.Services.Catalog.ResolveRequiredTableAsync("T", ct);
        await harness.InsertRowAsync("T", [1], ct);
        ResolvedTable after = await harness.Services.Catalog.ResolveRequiredTableAsync("T", ct);
        Assert.Same(original.Schema.Image, after.Schema.Image);
        Assert.Same(original.Definition, after.Definition);
        TableCounters? counters = await harness.Database.TableDefs.ReadTableCountersAsync(after.Entry.TDefPage, ct);
        Assert.NotNull(counters);
        Assert.Equal(1u, counters.Value.RowCount);
    }
}
