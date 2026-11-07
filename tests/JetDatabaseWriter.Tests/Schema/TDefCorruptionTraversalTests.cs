namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Fuzz;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Regression tests for hostile table-definition (TDEF) name records, duplicate
/// column identities, overflowed logical lengths, and unique-index preflight cancellation.
/// </summary>
public sealed class TDefCorruptionTraversalTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, 0)]
    [InlineData(DatabaseFormat.Jet4Mdb, 0)]
    [InlineData(DatabaseFormat.Jet4Mdb, 1)]
    [InlineData(DatabaseFormat.AceAccdb, 0)]
    [InlineData(DatabaseFormat.AceAccdb, 1)]
    public async Task CorruptIndexName_ZeroOrOddLength_RefusesWithCorruptTableDefinition(DatabaseFormat kind, int corruptLength)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            kind,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            Ct))
        {
            await writer.CreateTableAsync(
                "Items",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("Code", typeof(int)),
                ],
                [new IndexDefinition("IX_Code", "Code")],
                Ct);
            await writer.InsertRowAsync("Items", [1, 42], Ct);
        }

        byte[] bytes = stream.ToArray();
        var format = JetFormat.ForNewDatabase(kind);
        long tdefPage = await FindTableTDefPageAsync(bytes, "Items", Ct);
        int pageOffset = checked((int)(tdefPage * format.PageSize));
        byte[] tdefSlice = bytes.AsSpan(pageOffset, format.PageSize).ToArray();
        TDefCounts counts = TDefCodec.ReadCounts(format, tdefSlice);
        int realIdxStart = TDefCodec.LocateRealIndexDescriptors(format, tdefSlice, counts.ColumnCount, counts.RealIndexCount);
        Assert.True(realIdxStart > 0);
        IndexSectionAnchors anchors = format.Index.GetIndexSection(realIdxStart, counts.RealIndexCount, counts.LogicalIndexCount);
        int indexNameOffset = pageOffset + anchors.LogIdxNamesStart;
        if (format.IsJet3)
        {
            bytes[indexNameOffset] = checked((byte)corruptLength);
        }
        else
        {
            BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(indexNameOffset, 2), checked((ushort)corruptLength));
        }

        await using var corruptedStream = new MemoryStream(bytes, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(
            corruptedStream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            Ct);

        JetCorruptDataException listEx = await Assert.ThrowsAsync<JetCorruptDataException>(
            async () => await reader.ListIndexesAsync("Items", Ct));
        Assert.Equal(JetErrorCode.CorruptTableDefinition, listEx.ErrorCode);
        Assert.Equal(tdefPage, listEx.ErrorInfo.PageNumber);

        JetCorruptDataException fromIndexEx = await Assert.ThrowsAsync<JetCorruptDataException>(
            async () => await reader.FromIndex("Items", "IX_Code").ToRowsAsync(Ct).ToListAsync(Ct));
        Assert.Equal(JetErrorCode.CorruptTableDefinition, fromIndexEx.ErrorCode);
        Assert.Equal(tdefPage, fromIndexEx.ErrorInfo.PageNumber);

        JetCorruptDataException seekEx = await Assert.ThrowsAsync<JetCorruptDataException>(
            async () => await reader.SeekRowsAsync("Items", "IX_Code", [42], Ct).ToListAsync(Ct));
        Assert.Equal(JetErrorCode.CorruptTableDefinition, seekEx.ErrorCode);
        Assert.Equal(tdefPage, seekEx.ErrorInfo.PageNumber);

        JetCorruptDataException readEx = await Assert.ThrowsAsync<JetCorruptDataException>(
            async () => await reader.ReadTableAsync("Items", cancellationToken: Ct));
        Assert.Equal(JetErrorCode.CorruptTableDefinition, readEx.ErrorCode);
        Assert.Equal(tdefPage, readEx.ErrorInfo.PageNumber);

        await AccessReaderFuzzExercise.RunAsync(bytes, _ => { }, Ct);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, "zero")]
    [InlineData(DatabaseFormat.Jet4Mdb, "zero")]
    [InlineData(DatabaseFormat.Jet4Mdb, "odd")]
    [InlineData(DatabaseFormat.AceAccdb, "zero")]
    [InlineData(DatabaseFormat.AceAccdb, "odd")]
    [InlineData(DatabaseFormat.Jet3Mdb, "duplicate-name")]
    [InlineData(DatabaseFormat.Jet4Mdb, "duplicate-name")]
    [InlineData(DatabaseFormat.AceAccdb, "duplicate-name")]
    [InlineData(DatabaseFormat.Jet3Mdb, "duplicate-colnum")]
    [InlineData(DatabaseFormat.Jet4Mdb, "duplicate-colnum")]
    [InlineData(DatabaseFormat.AceAccdb, "duplicate-colnum")]
    public async Task CorruptColumnMetadata_ZeroOddLengthOrDuplicateIdentity_RefusesWithCorruptTableDefinition(DatabaseFormat kind, string corruption)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            kind,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            Ct))
        {
            await writer.CreateTableAsync(
                "Items",
                [
                    new ColumnDefinition("AA", typeof(int)),
                    new ColumnDefinition("BB", typeof(int)),
                ],
                Ct);
            await writer.InsertRowAsync("Items", [1, 2], Ct);
        }

        byte[] bytes = stream.ToArray();
        var format = JetFormat.ForNewDatabase(kind);
        long tdefPage = await FindTableTDefPageAsync(bytes, "Items", Ct);
        int pageOffset = checked((int)(tdefPage * format.PageSize));
        byte[] tdefSlice = bytes.AsSpan(pageOffset, format.PageSize).ToArray();
        TDefCounts counts = TDefCodec.ReadCounts(format, tdefSlice);
        int colDescStart = format.TDef.BlockEnd + (counts.RealIndexCount * format.TDef.RealIdxEntrySz);
        int colNamesStart = colDescStart + (counts.ColumnCount * format.ColumnDescriptor.Size);

        switch (corruption)
        {
            case "zero":
                if (format.IsJet3)
                {
                    bytes[pageOffset + colNamesStart] = 0;
                }
                else
                {
                    BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(pageOffset + colNamesStart, 2), 0);
                }

                break;
            case "odd":
                BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(pageOffset + colNamesStart, 2), 1);
                break;
            case "duplicate-name":
                byte[] duplicateRecord = format.EncodeTDefNameRecord("aa");
                int secondNameOffset = colNamesStart + duplicateRecord.Length;
                duplicateRecord.CopyTo(bytes.AsSpan(pageOffset + secondNameOffset, duplicateRecord.Length));
                break;
            case "duplicate-colnum":
                int secondColDesc = pageOffset + colDescStart + format.ColumnDescriptor.Size;
                Wu16(bytes, secondColDesc + format.ColumnDescriptor.NumOff, 0);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(corruption));
        }

        await using var readerStream = new MemoryStream(bytes, writable: false);
        await using (AccessReader reader = await AccessReader.OpenAsync(
            readerStream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            Ct))
        {
            JetCorruptDataException tableEx = await Assert.ThrowsAsync<JetCorruptDataException>(
                async () => await reader.ReadTableAsync("Items", cancellationToken: Ct));
            Assert.Equal(JetErrorCode.CorruptTableDefinition, tableEx.ErrorCode);
            Assert.Equal(tdefPage, tableEx.ErrorInfo.PageNumber);

            JetCorruptDataException stringsEx = await Assert.ThrowsAsync<JetCorruptDataException>(
                async () => await reader.ReadTableAsStringsAsync("Items", cancellationToken: Ct));
            Assert.Equal(JetErrorCode.CorruptTableDefinition, stringsEx.ErrorCode);
            Assert.Equal(tdefPage, stringsEx.ErrorInfo.PageNumber);

            JetCorruptDataException metaEx = await Assert.ThrowsAsync<JetCorruptDataException>(
                async () => await reader.GetColumnMetadataAsync("Items", Ct));
            Assert.Equal(JetErrorCode.CorruptTableDefinition, metaEx.ErrorCode);
            Assert.Equal(tdefPage, metaEx.ErrorInfo.PageNumber);
        }

        await using var writerStream = new MemoryStream((byte[])bytes.Clone(), writable: true);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            writerStream,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            Ct))
        {
            JetCorruptDataException writeEx = await Assert.ThrowsAsync<JetCorruptDataException>(
                async () => await writer.AddColumnAsync("Items", new ColumnDefinition("CC", typeof(int)), Ct));
            Assert.Equal(JetErrorCode.CorruptTableDefinition, writeEx.ErrorCode);
            Assert.Equal(tdefPage, writeEx.ErrorInfo.PageNumber);
        }

        await AccessReaderFuzzExercise.RunAsync(bytes, _ => { }, Ct);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CorruptTDefLogicalLength_IntMaxValueOnRelationshipTarget_RefusesWithCorruptTableDefinition(DatabaseFormat kind)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            kind,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            Ct))
        {
            await writer.CreateTableAsync(
                "Parent",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("Notes", typeof(string)) { ColumnTypeOverride = ColumnType.MemoType },
                ],
                Ct);
            await writer.CreateTableAsync(
                "Child",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("ParentId", typeof(int)),
                ],
                Ct);
        }

        byte[] bytes = stream.ToArray();
        var format = JetFormat.ForNewDatabase(kind);
        long parentPage = await FindTableTDefPageAsync(bytes, "Parent", Ct);
        int parentOffset = checked((int)(parentPage * format.PageSize));
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(parentOffset + 8, 4), int.MaxValue);

        await using var corruptedStream = new MemoryStream(bytes, writable: true);
        await using AccessWriter corruptedWriter = await AccessWriter.OpenAsync(
            corruptedStream,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            Ct);

        JetCorruptDataException ex = await Assert.ThrowsAsync<JetCorruptDataException>(
            async () => await corruptedWriter.CreateRelationshipAsync(
                new RelationshipDefinition("FK_Child_Parent", "Parent", "Id", "Child", "ParentId"),
                Ct));
        Assert.Equal(JetErrorCode.CorruptTableDefinition, ex.ErrorCode);
        Assert.Equal(parentPage, ex.ErrorInfo.PageNumber);
    }

    [Fact]
    public async Task CorruptTDefLogicalLength_IntMaxValueOnAppendOnlyHistory_RefusesWithCorruptComplexColumn()
    {
        byte[] fixtureBytes = await File.ReadAllBytesAsync(TestDatabases.ComplexDataTestV2007, Ct);
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        long historyPage;
        await using (var inspectStream = new MemoryStream(fixtureBytes, writable: false))
        await using (AccessReader reader = await AccessReader.OpenAsync(
            inspectStream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            Ct))
        {
            ComplexColumnInfo history = Assert.Single(
                await reader.GetComplexColumnsAsync("Table1", Ct),
                column => column.Kind == ComplexColumnKind.VersionHistory);
            historyPage = await FindTableTDefPageAsync(fixtureBytes, history.FlatTableName, Ct);
        }

        int pageOffset = checked((int)(historyPage * format.PageSize));
        BinaryPrimitives.WriteInt32LittleEndian(fixtureBytes.AsSpan(pageOffset + 8, 4), int.MaxValue);

        await using var corruptedStream = new MemoryStream(fixtureBytes, writable: true);
        await using AccessWriter writer = await AccessWriter.OpenAsync(
            corruptedStream,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            Ct);

        JetCorruptDataException ex = await Assert.ThrowsAsync<JetCorruptDataException>(
            async () => await writer.RenameColumnAsync("Table1", "append-memo-data", "Renamed", Ct));
        Assert.Equal(JetErrorCode.CorruptComplexColumn, ex.ErrorCode);
    }

    [Fact]
    public async Task UniqueIndexChecker_CheckUniqueIndexesCore_ObservesCancellationToken()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            Ct))
        {
            await writer.CreateTableAsync(
                "Items",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("Code", typeof(int)),
                ],
                [new IndexDefinition("UX_Code", "Code") { IsUnique = true }],
                Ct);
            await writer.InsertRowsAsync("Items", [[1, 10], [2, 20]], Ct);
        }

        stream.Position = 0;
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        ResolvedTable resolved = await harness.Services.Catalog.ResolveRequiredTableAsync("Items", Ct);
        List<LocatedRow> existingRows = await harness.Services.Snapshots.ReadRowsAsync(resolved.Entry.TDefPage, Ct);
        using var cts = new CancellationTokenSource();
        var cancellingPages = new CancelOnPageReadSource(harness.Database.Pages, cts);
        using var tableDefs = new TableDefReader(cancellingPages, harness.Database.Format, cacheResults: false);
        var checker = new JetDatabaseWriter.Indexes.UniqueIndexChecker(
            harness.Database.Format,
            cancellingPages,
            tableDefs,
            harness.Services.Snapshots);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await checker.CheckUniqueIndexesPreUpdateAsync(
                resolved.Entry.TDefPage,
                resolved.Definition,
                "Items",
                existingRows,
                [(0, existingRows[0].Values, [1, 30])],
                cts.Token));
    }

    private static async Task<long> FindTableTDefPageAsync(byte[] bytes, string tableName, CancellationToken ct)
    {
        await using var stream = new MemoryStream(bytes, writable: false);
        await using AccessReader harness = await TestDatabase.OpenReaderAsync(stream, cancellationToken: ct);
        ResolvedTable? resolved = await harness.Services.Catalog.ResolveTableAsync(tableName, ct);
        Assert.NotNull(resolved);
        return resolved.Entry.TDefPage;
    }

    private sealed class CancelOnPageReadSource(
        JetDatabaseWriter.Pages.Paging.IPageSource inner,
        CancellationTokenSource cts) : JetDatabaseWriter.Pages.Paging.IPageSource
    {
        public int PageSize => inner.PageSize;

        public long PageCount => inner.PageCount;

        public bool IsDisposed => inner.IsDisposed;

        public void ThrowIfDisposed()
            => inner.ThrowIfDisposed();

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken)
            => inner.ThrowIfDisposedOrCancelled(cancellationToken);

        public async ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            byte[] page = await inner.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            await cts.CancelAsync().ConfigureAwait(false);
            return page;
        }
    }
}
