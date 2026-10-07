namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using JetDatabaseWriter.ValueDecoding;
using Xunit;
using static JetDatabaseWriter.Enums.ColumnType;

#pragma warning disable CA1812 // The typed row mapper creates these projection targets.

/// <summary>Mixed scalar/OLE scans retain complete playlist definitions in one enumeration.</summary>
public sealed class MixedOleTypedScanTests
{
    [Fact]
    public void HybridPlan_BindsStoredOleBytesAndScalars_ButRejectsUnsupportedBindings()
    {
        string[] headers = ["Id", "Filter", "SortOrder", "Notes"];
        ColumnInfo[] columns =
        [
            new() { Name = "Id", Type = LongIntegerType, Flags = Constants.ColumnDescriptorFlags.Fixed },
            new() { Name = "Filter", Type = OleType },
            new() { Name = "SortOrder", Type = OleType },
            new() { Name = "Notes", Type = MemoType },
        ];
        Type[] types = [typeof(int), typeof(byte[]), typeof(byte[]), typeof(string)];
        Assert.Null(DirectRowDecoderBuilder.TryBuild<PlaylistRow>(headers, columns, types));
        Assert.NotNull(DirectRowDecoderBuilder.TryBuildHybrid<PlaylistRow>(headers, columns, types));
        Assert.Null(DirectRowDecoderBuilder.TryBuildHybrid<UnsupportedPlaylistRow>(headers, columns, types));
        Assert.Null(DirectRowDecoderBuilder.TryBuildHybrid<ThrowingRow>(headers, columns, types));
    }

    public static TheoryData<DatabaseFormat, int> FormatsAndCaches => new()
    {
        { DatabaseFormat.Jet3Mdb, 0 },
        { DatabaseFormat.Jet3Mdb, 1 },
        { DatabaseFormat.Jet4Mdb, 0 },
        { DatabaseFormat.Jet4Mdb, 1 },
        { DatabaseFormat.AceAccdb, 0 },
        { DatabaseFormat.AceAccdb, 1 },
    };

    [Theory]
    [MemberData(nameof(FormatsAndCaches))]
    public async Task HybridScan_AllStorageFormsAndSortOnlyDefinitions_MatchFallback(DatabaseFormat format, int cacheSize)
    {
        await using MemoryStream original = await CreateAsync(format);
        await using var stream = new FaultingMemoryStream(original.ToArray());
        await using AccessReader reader = await OpenAsync(stream, cacheSize);
        List<PlaylistRow> expected = await CollectAsync(reader.ReadPrivateField<ReaderServices>("services").Tables.RowsWithDecoder<PlaylistRow>("Playlists", enableHybridOle: false, forceProjection: true, progress: null, cancellationToken: Ct));
        List<PlaylistRow> actual = await CollectAsync(reader.ReadPrivateField<ReaderServices>("services").Tables.RowsWithDecoder<PlaylistRow>("Playlists", enableHybridOle: true, forceProjection: false, progress: null, cancellationToken: Ct));
        Assert.Equal(7, actual.Count);
        for (int i = 0; i < expected.Count; i++)
        {
            Assert.Equal(expected[i].Id, actual[i].Id);
            Assert.Equal(expected[i].Name, actual[i].Name);
            Assert.Equal(expected[i].Filter, actual[i].Filter);
            Assert.Equal(expected[i].SortOrder, actual[i].SortOrder);
        }

        Assert.Null(actual[0].Filter);
        Assert.Null(actual[0].SortOrder);
        Assert.Empty(Assert.IsType<byte[]>(actual[1].Filter));
        Assert.Empty(Assert.IsType<byte[]>(actual[1].SortOrder));
        Assert.Null(actual[6].Filter);
        Assert.Equal(Payload(4096), actual[6].SortOrder);
        Assert.Equal(Payload(23), actual[2].Filter);
        Assert.Equal(Payload(600), actual[3].Filter);
        Assert.Equal(Payload(12_000), actual[4].Filter);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task HybridScan_AbandonedAndCancelledEnumeration_LeavesReaderReusable(DatabaseFormat format)
    {
        await using MemoryStream original = await CreateAsync(format);
        await using var stream = new FaultingMemoryStream(original.ToArray());
        await using AccessReader reader = await OpenAsync(stream, 0);
        await using (IAsyncEnumerator<PlaylistRow> enumeration = reader.ReadPrivateField<ReaderServices>("services").Tables.RowsWithDecoder<PlaylistRow>("Playlists", enableHybridOle: true, forceProjection: false, progress: null, cancellationToken: Ct).GetAsyncEnumerator(Ct))
        {
            Assert.True(await enumeration.MoveNextAsync());
            Assert.Equal(1, enumeration.Current.Id);
        }

        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        await using (IAsyncEnumerator<PlaylistRow> enumeration = reader.ReadPrivateField<ReaderServices>("services").Tables.RowsWithDecoder<PlaylistRow>("Playlists", enableHybridOle: true, forceProjection: false, progress: null, cancellationToken: cancellation.Token).GetAsyncEnumerator(cancellation.Token))
        {
            Assert.True(await enumeration.MoveNextAsync());
            await cancellation.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await enumeration.MoveNextAsync());
        }

        Assert.Equal(7, (await CollectAsync(reader.ReadPrivateField<ReaderServices>("services").Tables.RowsWithDecoder<PlaylistRow>("Playlists", enableHybridOle: true, forceProjection: false, progress: null, cancellationToken: Ct))).Count);
    }

    [Theory(DisableParallelization = true)]
    [InlineData(DatabaseFormat.Jet3Mdb, 0)]
    [InlineData(DatabaseFormat.Jet3Mdb, 1)]
    [InlineData(DatabaseFormat.Jet3Mdb, 2)]
    [InlineData(DatabaseFormat.Jet3Mdb, 3)]
    [InlineData(DatabaseFormat.Jet4Mdb, 0)]
    [InlineData(DatabaseFormat.Jet4Mdb, 1)]
    [InlineData(DatabaseFormat.Jet4Mdb, 2)]
    [InlineData(DatabaseFormat.Jet4Mdb, 3)]
    [InlineData(DatabaseFormat.AceAccdb, 0)]
    [InlineData(DatabaseFormat.AceAccdb, 1)]
    [InlineData(DatabaseFormat.AceAccdb, 2)]
    [InlineData(DatabaseFormat.AceAccdb, 3)]
    public async Task HybridScan_CorruptDefinition_MatchesStrictAndLenientFallback(DatabaseFormat format, int corruption)
    {
        await using MemoryStream original = await CreateAsync(format);
        byte[] bytes = original.ToArray();
        int length = corruption switch { 2 => 12_000, 3 => 23, _ => 600 };
        byte storageMode = corruption switch { 2 => 0, 3 => 0x80, _ => 0x40 };
        int offset = FindDescriptor(bytes, length, storageMode);
        Assert.True(LongValueDescriptor.TryRead(bytes.AsSpan(offset), out LongValueDescriptor descriptor));
        if (corruption == 0)
        {
            bytes[offset + 3] = Constants.LongValue.StorageModeMask;
        }
        else if (corruption == 1)
        {
            (descriptor with { Length = 8000 }).WriteTo(bytes.AsSpan(offset));
        }
        else if (corruption == 3)
        {
            (descriptor with { Length = 45 }).WriteTo(bytes.AsSpan(offset));
        }
        else
        {
            var profile = JetFormat.ForNewDatabase(format);
            int pageStart = checked((int)(descriptor.FirstDp >> 8) * profile.PageSize);
            int row = checked((int)(descriptor.FirstDp & 0xFF));
            int rowStart = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(pageStart + profile.DataPage.RowsStart + (row * 2))) & Constants.DataPage.RowOffsetMask;
            BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(pageStart + rowStart), descriptor.FirstDp);
        }

        await using var stream = new MemoryStream(bytes, writable: false);
        await using (AccessReader strict = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, PageCacheSize = 0 }, leaveOpen: true, cancellationToken: Ct))
        {
            InvalidDataException fallback = await Assert.ThrowsAsync<InvalidDataException>(async () => await CollectAsync(strict.ReadPrivateField<ReaderServices>("services").Tables.RowsWithDecoder<PlaylistRow>("Playlists", enableHybridOle: false, forceProjection: true, progress: null, cancellationToken: Ct)));
            InvalidDataException hybrid = await Assert.ThrowsAsync<InvalidDataException>(async () => await CollectAsync(strict.ReadPrivateField<ReaderServices>("services").Tables.RowsWithDecoder<PlaylistRow>("Playlists", enableHybridOle: true, forceProjection: false, progress: null, cancellationToken: Ct)));
            Assert.Equal(fallback.Message, hybrid.Message);
            Assert.Contains("Filter", hybrid.Message, StringComparison.Ordinal);
            Assert.NotNull(hybrid.InnerException);
        }

        stream.Position = 0;
        await using var messages = new StringWriter(System.Globalization.CultureInfo.InvariantCulture);
        using var listener = new TextWriterTraceListener(messages);
        Trace.Listeners.Add(listener);
        try
        {
            await using AccessReader lenient = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, PageCacheSize = 1, StrictParsing = false }, leaveOpen: true, cancellationToken: Ct);
            List<PlaylistRow> expected = await CollectAsync(lenient.ReadPrivateField<ReaderServices>("services").Tables.RowsWithDecoder<PlaylistRow>("Playlists", enableHybridOle: false, forceProjection: true, progress: null, cancellationToken: Ct));
            listener.Flush();
            string fallbackDiagnostic = messages.ToString();
            Assert.Contains("column 'Filter' is unreadable and was returned as a missing value", fallbackDiagnostic, StringComparison.Ordinal);
            messages.GetStringBuilder().Clear();
            List<PlaylistRow> actual = await CollectAsync(lenient.ReadPrivateField<ReaderServices>("services").Tables.RowsWithDecoder<PlaylistRow>("Playlists", enableHybridOle: true, forceProjection: false, progress: null, cancellationToken: Ct));
            Assert.Equal(expected.Count, actual.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i].Id, actual[i].Id);
                Assert.Equal(expected[i].Filter, actual[i].Filter);
                Assert.Equal(expected[i].SortOrder, actual[i].SortOrder);
            }

            int corruptRow = corruption switch { 2 => 4, 3 => 2, _ => 3 };
            Assert.Null(actual[corruptRow].Filter);
            listener.Flush();
            Assert.Contains("column 'Filter' is unreadable and was returned as a missing value", messages.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            Trace.Listeners.Remove(listener);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, OverflowRowLayout.CrossPage)]
    [InlineData(DatabaseFormat.Jet3Mdb, OverflowRowLayout.SamePage)]
    [InlineData(DatabaseFormat.Jet3Mdb, OverflowRowLayout.TwoHop)]
    [InlineData(DatabaseFormat.Jet4Mdb, OverflowRowLayout.CrossPage)]
    [InlineData(DatabaseFormat.Jet4Mdb, OverflowRowLayout.SamePage)]
    [InlineData(DatabaseFormat.Jet4Mdb, OverflowRowLayout.TwoHop)]
    [InlineData(DatabaseFormat.AceAccdb, OverflowRowLayout.CrossPage)]
    [InlineData(DatabaseFormat.AceAccdb, OverflowRowLayout.SamePage)]
    [InlineData(DatabaseFormat.AceAccdb, OverflowRowLayout.TwoHop)]
    public async Task HybridScan_OverflowRows_PreservePositionAndStoredBytes(DatabaseFormat format, OverflowRowLayout layout)
    {
        byte[] bytes = await SyntheticOverflowRows.CreateTableAsync(
            format,
            "Playlists",
            100,
            primaryKey: false,
            [new("Filter", typeof(byte[])), new("SortOrder", typeof(byte[])), new("Notes", typeof(string))],
            id => [Payload(id % 2 == 0 ? 600 : 23), Payload(id % 2 == 0 ? 9000 : 31), new string('n', 4096)],
            Ct);
        List<PlaylistRow> expected;
        await using (var original = new MemoryStream(bytes, writable: false))
        await using (AccessReader reader = await OpenAsync(original, 0))
        {
            expected = await CollectAsync(reader.ReadPrivateField<ReaderServices>("services").Tables.RowsWithDecoder<PlaylistRow>("Playlists", enableHybridOle: false, forceProjection: true, progress: null, cancellationToken: Ct));
        }

        _ = await SyntheticOverflowRows.MoveRowAsync(bytes, "Playlists", layout, Ct);
        await using var stream = new MemoryStream(bytes, writable: false);
        await using AccessReader moved = await OpenAsync(stream, 1);
        List<PlaylistRow> actual = await CollectAsync(moved.ReadPrivateField<ReaderServices>("services").Tables.RowsWithDecoder<PlaylistRow>("Playlists", enableHybridOle: true, forceProjection: false, progress: null, cancellationToken: Ct));
        Assert.Equal(expected.Count, actual.Count);
        for (int i = 0; i < expected.Count; i++)
        {
            Assert.Equal(expected[i].Id, actual[i].Id);
            Assert.Equal(expected[i].Name, actual[i].Name);
            Assert.Equal(expected[i].Filter, actual[i].Filter);
            Assert.Equal(expected[i].SortOrder, actual[i].SortOrder);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    public async Task HybridScan_PayloadIoFailure_PropagatesEvenWhenLenient(DatabaseFormat format, bool strict)
    {
        await using MemoryStream original = await CreateAsync(format);
        byte[] bytes = original.ToArray();
        int offset = FindDescriptor(bytes, 12_000, 0);
        Assert.True(LongValueDescriptor.TryRead(bytes.AsSpan(offset), out LongValueDescriptor descriptor));
        int pageSize = JetFormat.ForNewDatabase(format).PageSize;
        await using var stream = new FaultingMemoryStream(bytes);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, PageCacheSize = 0, PageReadOptimizationMode = PageReadOptimizationMode.Disabled, StrictParsing = strict }, leaveOpen: true, cancellationToken: Ct);
        stream.FailureOffset = checked((descriptor.FirstDp >> 8) * pageSize);
        IOException failure = await Assert.ThrowsAsync<IOException>(async () => await CollectAsync(reader.ReadPrivateField<ReaderServices>("services").Tables.RowsWithDecoder<PlaylistRow>("Playlists", enableHybridOle: true, forceProjection: false, progress: null, cancellationToken: Ct)));
        Assert.Same(stream.Failure, failure);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task HybridScan_BudgetRefusal_OnlyReadsBoundLongValuesAndAlwaysPropagates(bool strict)
    {
        await using MemoryStream stream = await CreateAsync(DatabaseFormat.AceAccdb);
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, MaxLongValueBytes = 1000, StrictParsing = strict }, leaveOpen: true, cancellationToken: Ct);
        await using IAsyncEnumerator<PlaylistRow> enumeration = reader.ReadPrivateField<ReaderServices>("services").Tables.RowsWithDecoder<PlaylistRow>("Playlists", enableHybridOle: true, forceProjection: false, progress: null, cancellationToken: Ct).GetAsyncEnumerator(Ct);
        for (int id = 1; id <= 4; id++)
        {
            Assert.True(await enumeration.MoveNextAsync());
            Assert.Equal(id, enumeration.Current.Id);
        }

        JetLimitationException failure = await Assert.ThrowsAsync<JetLimitationException>(async () => await enumeration.MoveNextAsync());
        Assert.Equal(JetErrorCode.ValueTooLarge, failure.ErrorCode);
    }

    [Theory]
    [InlineData(LongIntegerType)]
    [InlineData(MoneyType)]
    [InlineData(NumericType)]
    public void HybridPlan_UnsupportedScalarLayouts_UseProjectionFallback(ColumnType scalarType)
        => Assert.Null(DirectRowDecoderBuilder.TryBuildHybrid<DecimalPlaylistRow>(
            ["Id", "Filter"],
            [new() { Name = "Id", Type = scalarType, Flags = scalarType == NumericType ? Constants.ColumnDescriptorFlags.Fixed : (byte)0 }, new() { Name = "Filter", Type = OleType }],
            [scalarType == LongIntegerType ? typeof(int) : typeof(decimal), typeof(byte[])]));

    [Fact]
    public async Task HybridScan_UnsupportedBoundMemo_UsesCompleteProjectionFallback()
    {
        await using MemoryStream stream = await CreateAsync(DatabaseFormat.AceAccdb);
        await using AccessReader reader = await OpenAsync(stream, 0);
        List<NotesRow> expected = await CollectAsync(reader.ReadPrivateField<ReaderServices>("services").Tables.RowsWithDecoder<NotesRow>("Playlists", enableHybridOle: false, forceProjection: true, progress: null, cancellationToken: Ct));
        List<NotesRow> actual = await CollectAsync(reader.ReadPrivateField<ReaderServices>("services").Tables.RowsWithDecoder<NotesRow>("Playlists", enableHybridOle: true, forceProjection: false, progress: null, cancellationToken: Ct));
        Assert.Equal(expected.Count, actual.Count);
        for (int i = 0; i < expected.Count; i++)
        {
            Assert.Equal(expected[i].Id, actual[i].Id);
            Assert.Equal(expected[i].Notes, actual[i].Notes);
            Assert.Equal(new string('n', 4096), actual[i].Notes);
        }
    }

    [Fact]
    public async Task HybridScan_CancellationDuringAwaitedPayloadRead_DoesNotYieldIncompleteTarget()
    {
        await using MemoryStream original = await CreateAsync(DatabaseFormat.AceAccdb);
        byte[] bytes = original.ToArray();
        int offset = FindDescriptor(bytes, 600, 0x40);
        Assert.True(LongValueDescriptor.TryRead(bytes.AsSpan(offset), out LongValueDescriptor descriptor));
        await using var stream = new FaultingMemoryStream(bytes);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, PageCacheSize = 0, PageReadOptimizationMode = PageReadOptimizationMode.Disabled }, leaveOpen: true, cancellationToken: Ct);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        stream.FailureOffset = checked((long)(descriptor.FirstDp >> 8) * Constants.PageSizes.Jet4);
        stream.CancelOnRead = cancellation;
        await using (IAsyncEnumerator<PlaylistRow> enumeration = reader.ReadPrivateField<ReaderServices>("services").Tables.RowsWithDecoder<PlaylistRow>("Playlists", enableHybridOle: true, forceProjection: false, progress: null, cancellationToken: cancellation.Token).GetAsyncEnumerator(cancellation.Token))
        {
            for (int id = 1; id <= 3; id++)
            {
                Assert.True(await enumeration.MoveNextAsync());
                Assert.Equal(id, enumeration.Current.Id);
            }

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await enumeration.MoveNextAsync());
            Assert.True(cancellation.IsCancellationRequested);
        }

        stream.FailureOffset = -1;
        Assert.Equal(7, (await CollectAsync(reader.ReadPrivateField<ReaderServices>("services").Tables.RowsWithDecoder<PlaylistRow>("Playlists", enableHybridOle: true, forceProjection: false, progress: null, cancellationToken: Ct))).Count);
    }

    [Fact]
    public async Task HybridScan_UserSetterFailure_PropagatesLikeProjectionFallback()
    {
        await using MemoryStream stream = await CreateAsync(DatabaseFormat.AceAccdb);
        await using AccessReader reader = await OpenAsync(stream, 0);
        ArgumentException expected = await Assert.ThrowsAsync<ArgumentException>(async () => await CollectAsync(reader.ReadPrivateField<ReaderServices>("services").Tables.RowsWithDecoder<ThrowingRow>("Playlists", enableHybridOle: false, forceProjection: true, progress: null, cancellationToken: Ct)));
        ArgumentException actual = await Assert.ThrowsAsync<ArgumentException>(async () => await CollectAsync(reader.ReadPrivateField<ReaderServices>("services").Tables.RowsWithDecoder<ThrowingRow>("Playlists", enableHybridOle: true, forceProjection: false, progress: null, cancellationToken: Ct)));
        Assert.Equal(expected.Message, actual.Message);
    }

    [Fact]
    public async Task HybridScan_UserSetters_ObserveProjectionColumnAssignmentOrder()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync("Playlists", [new("Id", typeof(int)), new("Filter", typeof(byte[])), new("Name", typeof(string), 100)], Ct);
            await writer.InsertRowAsync("Playlists", [1, Payload(600), "Dynamic"], Ct);
        }

        await using AccessReader reader = await OpenAsync(stream, 0);
        SetterOrderRow expected = Assert.Single(await CollectAsync(reader.ReadPrivateField<ReaderServices>("services").Tables.RowsWithDecoder<SetterOrderRow>("Playlists", enableHybridOle: false, forceProjection: true, progress: null, cancellationToken: Ct)));
        SetterOrderRow actual = Assert.Single(await CollectAsync(reader.ReadPrivateField<ReaderServices>("services").Tables.RowsWithDecoder<SetterOrderRow>("Playlists", enableHybridOle: true, forceProjection: false, progress: null, cancellationToken: Ct)));
        Assert.Equal(expected.Id, actual.Id);
        Assert.Equal(expected.Filter, actual.Filter);
        Assert.Equal(expected.Name, actual.Name);
        Assert.Equal("Dynamic", actual.Name);
    }

    private static int FindDescriptor(byte[] bytes, int length, byte storageMode)
    {
        uint pattern = checked((uint)length) | ((uint)storageMode << 24);
        for (int offset = 0; offset <= bytes.Length - Constants.LongValue.HeaderSize; offset++)
        {
            if (BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(offset)) == pattern && (storageMode == 0x80 || BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(offset + 4)) != 0))
            {
                return offset;
            }
        }

        throw new InvalidDataException("The fixture's OLE descriptor was not found.");
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static async Task<MemoryStream> CreateAsync(DatabaseFormat format)
    {
        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync(
                "Playlists",
                [
                    new("Id", typeof(int)),
                    new("Name", typeof(string), 100),
                    new("Filter", typeof(byte[])),
                    new("SortOrder", typeof(byte[])),
                    new("Notes", typeof(string)),
                ],
                Ct);
            await writer.InsertRowsAsync(
                "Playlists",
                [
                    [1, "Static", DBNull.Value, DBNull.Value, new string('n', 4096)],
                    [2, "Empty", Array.Empty<byte>(), Array.Empty<byte>(), new string('n', 4096)],
                    [3, "Inline", Payload(23), Payload(31), new string('n', 4096)],
                    [4, "Single", Payload(600), Payload(700), new string('n', 4096)],
                    [5, "Chained", Payload(12_000), Payload(13_000), new string('n', 4096)],
                    [6, "Mixed", Payload(45), Payload(10_000), new string('n', 4096)],
                    [7, "Sort only", DBNull.Value, Payload(4096), new string('n', 4096)],
                    [8, "Deleted", Payload(700), Payload(900), new string('n', 4096)],
                ],
                Ct);
            Assert.Equal(1, await writer.DeleteRowsAsync("Playlists", "Id", 8, Ct));
        }

        return stream;
    }

    private static async Task<AccessReader> OpenAsync(MemoryStream stream, int cacheSize)
    {
        stream.Position = 0;
        return await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, PageCacheSize = cacheSize }, leaveOpen: true, cancellationToken: Ct);
    }

    private static byte[] Payload(int length)
    {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++)
        {
            bytes[i] = unchecked((byte)((i * 37) ^ (i >> 8)));
        }

        return bytes;
    }

    private static async Task<List<T>> CollectAsync<T>(IAsyncEnumerable<T> rows)
    {
        var result = new List<T>();
        await foreach (T row in rows)
        {
            result.Add(row);
        }

        return result;
    }

    [SuppressMessage("Performance", "CA1819:Properties should not return arrays", Justification = "Projection preserves stored OLE bytes.")]
    private sealed class PlaylistRow
    {
        public int Id { get; set; }

        public string? Name { get; set; }

        public byte[]? Filter { get; set; }

        public byte[]? SortOrder { get; set; }
    }

    private sealed class FaultingMemoryStream(byte[] bytes) : MemoryStream(bytes, writable: false)
    {
        public long FailureOffset { get; set; } = -1;

        public CancellationTokenSource? CancelOnRead { get; set; }

        public IOException Failure { get; } = new("Injected payload page I/O failure.");

        public override int Read(byte[] buffer, int offset, int count)
        {
            this.CheckFailure();
            return base.Read(buffer, offset, count);
        }

        public override int Read(Span<byte> buffer)
        {
            this.CheckFailure();
            return base.Read(buffer);
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            await Task.Yield();
            cancellationToken.ThrowIfCancellationRequested();
            this.CheckFailure();
            cancellationToken.ThrowIfCancellationRequested();
            return base.Read(buffer.AsSpan(offset, count));
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            cancellationToken.ThrowIfCancellationRequested();
            this.CheckFailure();
            cancellationToken.ThrowIfCancellationRequested();
            return base.Read(buffer.Span);
        }

        private void CheckFailure()
        {
            if (this.Position == this.FailureOffset)
            {
                if (this.CancelOnRead is { } source)
                {
                    source.Cancel();
                    return;
                }

                throw this.Failure;
            }
        }
    }

    [SuppressMessage("Performance", "CA1819:Properties should not return arrays", Justification = "Projection preserves stored OLE bytes.")]
    private sealed class DecimalPlaylistRow
    {
        public decimal Id { get; set; }

        public byte[]? Filter { get; set; }
    }

    private sealed class NotesRow
    {
        public int Id { get; set; }

        public string? Notes { get; set; }
    }

    [SuppressMessage("Performance", "CA1819:Properties should not return arrays", Justification = "Projection preserves stored OLE bytes.")]
    private sealed class ThrowingRow
    {
#pragma warning disable CA1822 // The mapper requires an instance setter whose deliberate exception must propagate.
        public int Id
        {
            get => 0;
            set => throw new ArgumentException("The projection setter rejected the value.");
        }
#pragma warning restore CA1822

        public byte[]? Filter { get; set; }
    }

    [SuppressMessage("Performance", "CA1819:Properties should not return arrays", Justification = "Projection preserves stored OLE bytes.")]
    private sealed class SetterOrderRow
    {
#pragma warning disable IDE0032 // This deliberately non-auto setter checks projection column assignment order.
        private string? name;
#pragma warning restore IDE0032

        public int Id { get; set; }

        public byte[]? Filter { get; set; }

        public string? Name
        {
            get => this.name;
            set
            {
                if (this.Filter is null)
                {
                    throw new InvalidOperationException("The OLE property must already be assigned.");
                }

                this.name = value;
            }
        }
    }

    private sealed class UnsupportedPlaylistRow
    {
        public object? Id { get; set; }
    }
}
