namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

#pragma warning disable EF1001 // This regression controls the provider connection's raw buffer scan and writer lifetime directly.
public sealed class JetConnectionStreamingTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OrderedFirstRow_DoesNotDecodeALaterIndexedRow(bool transaction)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using ReadFaultStream stream = new();
        long faultOffset;
        await using (AccessWriter created = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: ct))
        {
            List<ColumnDefinition> columns = [new ColumnDefinition("Id", typeof(int))];
            for (int i = 1; i <= 7; i++)
            {
                columns.Add(new ColumnDefinition($"Padding{i}", typeof(string), 255));
            }

            await created.CreateTableAsync("Items", columns, [new IndexDefinition("PrimaryKey", "Id") { IsPrimaryKey = true }], ct);
            object?[][] inserted = [BuildRow(2), BuildRow(1)];
            await created.InsertRowsAsync("Items", inserted, ct);
            ResolvedTable table = await created.Services.Catalog.ResolveRequiredTableAsync("Items", ct);
            List<LocatedRow> stored = await created.Services.Snapshots.ReadRowsAsync(table.Entry.TDefPage, ct);
            Assert.Equal(2, stored.Count);
            Assert.NotEqual(stored[0].Location.DataPageNumber, stored[1].Location.DataPageNumber);
            faultOffset = checked(stored[0].Location.DataPageNumber * created.Database.Format.PageSize);
        }

        DbContextOptions<BufferContext> options = new DbContextOptionsBuilder<BufferContext>()
            .UseJetDatabaseWriter(stream, leaveOpen: true, jetOptionsAction: jet => jet
                .WithWriterOptions(new AccessWriterOptions { PageCacheSize = 0 })
                .WithReaderOptions(new AccessReaderOptions { PageCacheSize = 0 })).Options;
        await using BufferContext context = new(options);
        await using IDbContextTransaction? scope = transaction ? await context.Database.BeginTransactionAsync(ct) : null;
        if (transaction)
        {
            IJetClientConnection connection = context.GetService<IJetClientConnection>();
            (AccessWriter writer, bool owned) = await connection.RentWriterAsync(ct);
            writer.Pager.InvalidateAll();
            await connection.ReleaseWriterAsync(writer, owned);
        }

        stream.FaultOffset = faultOffset;
        try
        {
            await using IAsyncEnumerator<int> rows = context.Set<BufferItem>().AsNoTracking().OrderBy(item => item.Id).Select(item => item.Id)
                .AsAsyncEnumerable().GetAsyncEnumerator(ct);
            Assert.True(await rows.MoveNextAsync());
            Assert.Equal(1, rows.Current);
        }
        finally
        {
            stream.FaultOffset = -1;
        }

        Assert.Equal(0, stream.FaultReads);
        context.Add(new BufferItem { Id = 3 });
        Assert.Equal(1, await context.SaveChangesAsync(ct));

        static object?[] BuildRow(int id)
            => [id, new string('\u4e00', 255), new string('\u4e00', 255), new string('\u4e00', 255), new string('\u4e00', 255), new string('\u4e00', 255), new string('\u4e00', 255), new string('\u4e00', 255)];
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Disposing_A_Writer_Scan_Does_Not_Decode_An_Unrequested_Row(bool cancelAfterFirstRow)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using ReadFaultStream stream = new();
        long faultOffset;
        await using (AccessWriter created = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: ct))
        {
            List<ColumnDefinition> columns = [new ColumnDefinition("Id", typeof(int))];
            for (int i = 1; i <= 7; i++)
            {
                columns.Add(new ColumnDefinition($"Padding{i}", typeof(string), 255));
            }

            await created.CreateTableAsync("Items", columns, ct);
            object?[][] inserted = [BuildRow(1), BuildRow(2)];
            await created.InsertRowsAsync("Items", inserted, ct);
            ResolvedTable table = await created.Services.Catalog.ResolveRequiredTableAsync("Items", ct);
            List<LocatedRow> stored = await created.Services.Snapshots.ReadRowsAsync(table.Entry.TDefPage, ct);
            Assert.Equal(2, stored.Count);
            Assert.NotEqual(stored[0].Location.DataPageNumber, stored[1].Location.DataPageNumber);
            faultOffset = checked(stored[1].Location.DataPageNumber * created.Database.Format.PageSize);
        }

        DbContextOptions<BufferContext> options = new DbContextOptionsBuilder<BufferContext>().UseJetDatabaseWriter(stream, leaveOpen: true, jetOptionsAction: jet => jet.WithWriterOptions(new AccessWriterOptions { PageCacheSize = 0 })).Options;
        await using BufferContext modelContext = new(options);
        await using JetClientConnection connection = new(options);
        (AccessWriter writer, bool owned) = await connection.RentWriterAsync(ct);
        writer.Pager.InvalidateAll();
        await connection.ReleaseWriterAsync(writer, owned);

        using var canceled = CancellationTokenSource.CreateLinkedTokenSource(ct);
        stream.FaultOffset = faultOffset;
        try
        {
            await using IAsyncEnumerator<ValueBuffer> rows = connection.StreamTableValueBuffersAsync(
                modelContext.Model.FindEntityType(typeof(BufferItem))!,
                seekConditions: null,
                parameters: new Dictionary<string, object?>(),
                requiredProperties: null,
                maxRows: 1,
                orderedProperties: null,
                rowFilter: null,
                canceled.Token).GetAsyncEnumerator(canceled.Token);
            Assert.True(await rows.MoveNextAsync());
            Assert.Equal(1, rows.Current[0]);
            if (cancelAfterFirstRow)
            {
                await canceled.CancelAsync();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await rows.MoveNextAsync());
            }
        }
        finally
        {
            stream.FaultOffset = -1;
        }

        Assert.Equal(0, stream.FaultReads);

        static object?[] BuildRow(int id)
            => [id, new string('\u4e00', 255), new string('\u4e00', 255), new string('\u4e00', 255), new string('\u4e00', 255), new string('\u4e00', 255), new string('\u4e00', 255), new string('\u4e00', 255)];
    }

    private sealed class BufferContext(DbContextOptions<BufferContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<BufferItem>().Property(item => item.Id).ValueGeneratedNever();
    }

#pragma warning disable CA1812 // The test consumes raw buffers and deliberately never constructs the mapped CLR entity.
    [Table("Items")]
    private sealed class BufferItem
    {
        public int Id { get; set; }
    }
#pragma warning restore CA1812

    private sealed class ReadFaultStream : MemoryStream
    {
        /// <summary>Allows the initial ownership-validation read before row decoding begins.</summary>
        private int allowedOwnershipReads = 1;

        public long FaultOffset { get; set; } = -1;

        public int FaultReads { get; private set; }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (this.Position == this.FaultOffset && this.allowedOwnershipReads-- <= 0)
            {
                this.FaultReads++;
                return ValueTask.FromException<int>(new IOException("The scan read an unrequested row's page."));
            }

            return base.ReadAsync(buffer, cancellationToken);
        }
    }
}
#pragma warning restore EF1001
