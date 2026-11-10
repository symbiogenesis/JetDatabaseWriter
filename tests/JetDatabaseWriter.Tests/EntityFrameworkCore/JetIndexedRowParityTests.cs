namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.EntityFrameworkCore.Query.Internal;
using JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

/// <summary>Indexed reader and transaction queries interpret the same physical row bookmarks.</summary>
/// <param name="seeds">The reusable private database images.</param>
public sealed class JetIndexedRowParityTests(GeneratedDatabaseCache seeds) : IClassFixture<GeneratedDatabaseCache>
{
    private static readonly string Memo = new('m', 5000);
    private static readonly byte[] Payload = Enumerable.Range(0, 3000).Select(static i => (byte)(i & 0xFF)).ToArray();

    /// <summary>Both paths follow moved rows and retain projected long values.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="layout">The valid overflow chain.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, OverflowRowLayout.CrossPage)]
    [InlineData(DatabaseFormat.Jet4Mdb, OverflowRowLayout.CrossPage)]
    [InlineData(DatabaseFormat.AceAccdb, OverflowRowLayout.CrossPage)]
    [InlineData(DatabaseFormat.Jet3Mdb, OverflowRowLayout.TwoHop)]
    [InlineData(DatabaseFormat.Jet4Mdb, OverflowRowLayout.TwoHop)]
    [InlineData(DatabaseFormat.AceAccdb, OverflowRowLayout.TwoHop)]
    public async Task MovedRows_PreserveValuesAcrossReaderAndTransaction(DatabaseFormat format, OverflowRowLayout layout)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] image = await this.CreateImageAsync(format);
        _ = await SyntheticOverflowRows.MoveRowAsync(image, "Items", layout, ct);
        await using (AccessReader reader = await AccessReader.OpenAsync(new MemoryStream(image, writable: false), leaveOpen: false, cancellationToken: ct))
        {
            IndexMetadata primary = (await reader.ListIndexesAsync("Items", ct)).Single(static index => index.Kind == IndexKind.PrimaryKey);
            object[] row = Assert.Single(await reader.SeekRowsAsync("Items", primary.Name, [1], ct).ToListAsync(ct));
            Assert.Equal(Memo, row[2]);
            Assert.Equal(Payload, Assert.IsType<byte[]>(row[3]));
        }

        await using MemoryStream stream = new(image);
        await using RowContext context = new(CreateOptions(stream, format));
        await using IDbContextTransaction transaction = await context.Database.BeginTransactionAsync(ct);
        object?[] indexedRow = Assert.Single(await (await ReadActiveIndexRowsAsync(context, ct)).ToListAsync(ct));
        Assert.Equal(Memo, indexedRow[2]);
        Assert.Equal(Payload, Assert.IsType<byte[]>(indexedRow[3]));
        var actual = await context.Set<Row>().AsNoTracking().Where(static row => row.Id == 1)
            .Select(static row => new { row.Id, row.Memo, row.Payload }).SingleAsync(ct);
        Assert.Equal(1, actual.Id);
        Assert.Equal(Memo, actual.Memo);
        Assert.Equal(Payload, actual.Payload);
    }

    /// <summary>Corrupt index bookmarks refuse strict reads and remain observable in lenient reads.</summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CorruptRows_PreserveStrictFailureAndLenientAccounting(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] image = await this.CreateImageAsync(format);
        SyntheticOverflowRow moved = await SyntheticOverflowRows.MoveRowAsync(image, "Items", OverflowRowLayout.PointerPastEndOfFile, ct);
        await using (AccessReader reader = await AccessReader.OpenAsync(new MemoryStream(image, writable: false), leaveOpen: false, cancellationToken: ct))
        {
            IndexMetadata primary = (await reader.ListIndexesAsync("Items", ct)).Single(static index => index.Kind == IndexKind.PrimaryKey);
            JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                await reader.SeekRowsAsync("Items", primary.Name, [1], ct).ToListAsync(ct));
            Assert.Equal(JetErrorCode.MalformedValue, failure.ErrorCode);
            Assert.Equal(moved.HeaderPage, failure.ErrorInfo.PageNumber);
            Assert.Equal(0, reader.OmittedRowCount);
        }

        await using (AccessReader reader = await AccessReader.OpenAsync(
            new MemoryStream(image, writable: false), new AccessReaderOptions { StrictParsing = false }, leaveOpen: false, cancellationToken: ct))
        {
            IndexMetadata primary = (await reader.ListIndexesAsync("Items", ct)).Single(static index => index.Kind == IndexKind.PrimaryKey);
            Assert.Empty(await reader.SeekRowsAsync("Items", primary.Name, [1], ct).ToListAsync(ct));
            Assert.Equal(1, reader.OmittedRowCount);
        }

        await using MemoryStream stream = new(image);
        await using RowContext context = new(CreateOptions(stream, format));
        await using IDbContextTransaction transaction = await context.Database.BeginTransactionAsync(ct);
        JetCorruptDataException indexedFailure = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await (await ReadActiveIndexRowsAsync(context, ct)).ToListAsync(ct));
        Assert.Equal(JetErrorCode.MalformedValue, indexedFailure.ErrorCode);
        Assert.Equal(moved.HeaderPage, indexedFailure.ErrorInfo.PageNumber);
        JetCorruptDataException transactionFailure = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await context.Set<Row>().AsNoTracking().Where(static row => row.Id == 1).ToArrayAsync(ct));
        Assert.Equal(JetErrorCode.MalformedValue, transactionFailure.ErrorCode);
        Assert.Equal(moved.HeaderPage, transactionFailure.ErrorInfo.PageNumber);
    }

    private static DbContextOptions<RowContext> CreateOptions(Stream stream, DatabaseFormat format)
        => new DbContextOptionsBuilder<RowContext>().UseJetDatabaseWriter(stream, format, leaveOpen: true).Options;

#pragma warning disable EF1001 // Require an admitted transaction index read so a scan fallback cannot hide decoding regressions.
    private static async ValueTask<IAsyncEnumerable<object?[]>> ReadActiveIndexRowsAsync(RowContext context, CancellationToken ct)
    {
        AccessWriter? writer = context.GetService<IJetClientConnection>().ActiveWriter;
        Assert.NotNull(writer);
        ResolvedTable resolved = await writer.Services.Catalog.ResolveRequiredTableAsync("Items", ct);
        Expression<Func<Row, bool>> predicate = static row => row.Id == 1;
        List<JetIndexSeekCondition> conditions = JetIndexSeekPlanner.ExtractSeekConditions(context.Model.FindEntityType(typeof(Row))!, predicate);
        (IAsyncEnumerable<object?[]> Rows, bool Ordered)? plan = await JetIndexSeekPlanner.TryBuildActiveWriterIndexQueryAsync(
            writer, resolved, conditions, orderedProperties: null, new Dictionary<string, object?>(), [true, false, true, true], ct);
        Assert.NotNull(plan);
        return plan.Value.Rows;
    }
#pragma warning restore EF1001

    private ValueTask<byte[]> CreateImageAsync(DatabaseFormat format)
        => seeds.CopyToArrayAsync(
            $"indexed-row-parity/{format}",
            () => SyntheticOverflowRows.CreateTableAsync(
                format,
                "Items",
                100,
                primaryKey: true,
                [new ColumnDefinition("Memo", typeof(string)), new ColumnDefinition("Payload", typeof(byte[]))],
                static id => id == 1 ? [Memo, Payload] : ["short", Array.Empty<byte>()],
                CancellationToken.None).AsTask());

    private sealed class RowContext(DbContextOptions<RowContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => _ = modelBuilder.Entity<Row>(entity =>
            {
                _ = entity.ToTable("Items");
                _ = entity.Property(static row => row.Id).ValueGeneratedNever();
            });
    }

#pragma warning disable CA1812 // EF creates these entities through its materializer.
    private sealed class Row
    {
        public int Id { get; set; }

        public string Memo { get; set; } = string.Empty;

        public byte[] Payload { get; set; } = [];
    }
#pragma warning restore CA1812
}
