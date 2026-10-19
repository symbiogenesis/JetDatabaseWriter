namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using Xunit;

#pragma warning disable CA1812 // Public row materialization constructs the projection targets.

/// <summary>External values must not consume page headers or contradictory row directories as payload.</summary>
public sealed class LvalRowBoundsDecodePolicyTests
{
    public static TheoryData<DatabaseFormat, bool, bool, bool> Cases()
    {
        var data = new TheoryData<DatabaseFormat, bool, bool, bool>();
        foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
        {
            foreach (bool ole in new[] { false, true })
            {
                foreach (bool excessiveRows in new[] { false, true })
                {
                    foreach (bool strict in new[] { false, true })
                    {
                        data.Add(format, ole, excessiveRows, strict);
                    }
                }
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task MalformedLvalDirectory_PublicReadsAndWritesRefuseInventedPayload(DatabaseFormat format, bool ole, bool excessiveRows, bool strict)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var original = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(original, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct))
        {
            await writer.CreateTableAsync("Values", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Value", ole ? typeof(byte[]) : typeof(string))], ct);
            await writer.InsertRowAsync("Values", [1, ole ? Enumerable.Repeat((byte)'v', 300).ToArray() : new string('v', 300)], ct);
        }

        byte[] bytes = original.ToArray();
        (JetFormat profile, LongValueDescriptor descriptor) = await FindLvalAsync(original, ct);
        int lvalOffset = LongValueStore.PageNumber(descriptor.FirstDp) * profile.PageSize;
        int directoryOffset = excessiveRows
            ? profile.DataPage.NumRows
            : profile.DataPage.RowsStart + (LongValueStore.RowIndex(descriptor.FirstDp) * sizeof(ushort));
        ushort invalid = excessiveRows ? ushort.MaxValue : checked((ushort)profile.DataPage.RowsStart);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(lvalOffset + directoryOffset, sizeof(ushort)), invalid);
        await using var messages = new StringWriter(CultureInfo.InvariantCulture);
        using var listener = new TextWriterTraceListener(messages);
        var traceScope = new TestTraceScope(listener);
        try
        {
            await using var stream = new MemoryStream(bytes, writable: false);
            await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ct);
            if (strict)
            {
                InvalidDataException typed = await Assert.ThrowsAsync<InvalidDataException>(async () => await reader.Rows("Values", cancellationToken: ct).ToListAsync(ct));
                InvalidDataException strings = await Assert.ThrowsAsync<InvalidDataException>(async () => await reader.RowsAsStrings("Values", cancellationToken: ct).ToListAsync(ct));
                InvalidDataException poco = await Assert.ThrowsAsync<InvalidDataException>(async () => await reader.Rows<ValueRow>("Values", cancellationToken: ct).ToListAsync(ct));
                InvalidDataException natural = await Assert.ThrowsAsync<InvalidDataException>(async () => await ReadNaturalProjectionAsync(reader, ole, ct));
                InvalidDataException table = await Assert.ThrowsAsync<InvalidDataException>(async () => await reader.ReadTableAsync("Values", cancellationToken: ct));
                Assert.Contains("Value", typed.Message, StringComparison.Ordinal);
                Assert.Equal(typed.Message, strings.Message);
                Assert.Equal(typed.Message, poco.Message);
                Assert.Equal(typed.Message, natural.Message);
                Assert.Equal(typed.Message, table.Message);
            }
            else
            {
                Assert.Equal(DBNull.Value, Assert.Single(await reader.Rows("Values", cancellationToken: ct).ToListAsync(ct))[1]);
                Assert.Null(Assert.Single(await reader.RowsAsStrings("Values", cancellationToken: ct).ToListAsync(ct))[1]);
                Assert.Null(Assert.Single(await reader.Rows<ValueRow>("Values", cancellationToken: ct).ToListAsync(ct)).Value);
                Assert.Null(await ReadNaturalProjectionAsync(reader, ole, ct));
                Assert.Equal(DBNull.Value, Assert.Single((await reader.ReadTableAsync("Values", cancellationToken: ct)).Rows.Cast<System.Data.DataRow>())[1]);
                listener.Flush();
                Assert.Contains("column 'Value' is unreadable and was returned as a missing value", messages.ToString(), StringComparison.Ordinal);
            }
        }
        finally
        {
            traceScope.Dispose();
        }

        await using var mutationStream = new MemoryStream();
        await mutationStream.WriteAsync(bytes, ct);
        mutationStream.Position = 0;
        await using AccessWriter mutation = await AccessWriter.OpenAsync(mutationStream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct);
        await Assert.ThrowsAsync<InvalidDataException>(async () => await mutation.UpdateRowsAsync("Values", RowCriteria.All(), new RowValues { ["Id"] = 2 }, ct));
        Assert.Equal(bytes, mutationStream.ToArray());
    }

    [Theory]
    [InlineData(false, true)]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(true, false)]
    public async Task PayloadIoFailure_PublicReadsPropagateOriginalException(bool ole, bool strict)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var original = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(original, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct))
        {
            await writer.CreateTableAsync("Values", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Value", ole ? typeof(byte[]) : typeof(string))], ct);
            await writer.InsertRowAsync("Values", [1, ole ? Enumerable.Repeat((byte)'v', 300).ToArray() : new string('v', 300)], ct);
        }

        (JetFormat profile, LongValueDescriptor descriptor) = await FindLvalAsync(original, ct);
        await using var stream = new FaultingMemoryStream(original.ToArray());
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false, StrictParsing = strict, PageCacheSize = 0, PageReadOptimizationMode = PageReadOptimizationMode.Disabled },
            leaveOpen: true,
            cancellationToken: ct);
        stream.FailureOffset = LongValueStore.PageNumber(descriptor.FirstDp) * profile.PageSize;
        Assert.Same(stream.Failure, await Assert.ThrowsAsync<IOException>(async () => await reader.Rows("Values", cancellationToken: ct).ToListAsync(ct)));
        Assert.Same(stream.Failure, await Assert.ThrowsAsync<IOException>(async () => await reader.RowsAsStrings("Values", cancellationToken: ct).ToListAsync(ct)));
        Assert.Same(stream.Failure, await Assert.ThrowsAsync<IOException>(async () => await reader.Rows<ValueRow>("Values", cancellationToken: ct).ToListAsync(ct)));
        Assert.Same(stream.Failure, await Assert.ThrowsAsync<IOException>(async () => await ReadNaturalProjectionAsync(reader, ole, ct)));
        Assert.Same(stream.Failure, await Assert.ThrowsAsync<IOException>(async () => await reader.ReadTableAsync("Values", cancellationToken: ct)));
    }

    private static async Task<(JetFormat Format, LongValueDescriptor Descriptor)> FindLvalAsync(MemoryStream original, CancellationToken ct)
    {
        original.Position = 0;
        await using AccessReader harness = await TestDatabase.OpenReaderAsync(original, cancellationToken: ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync("Values", ct));
        TableDef definition = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(entry.TDefPage, ct));
        ColumnInfo column = definition.Columns[1];
        JetFormat profile = harness.Database.Format;
        for (long number = 1; number < harness.Database.Pages.PageCount; number++)
        {
            byte[] page = await harness.ReadPageCopyAsync(number, ct);
            if (page[0] != Constants.PageTypes.Data || JetTypeInfo.Ri32(page, profile.DataPage.TDefOff) != entry.TDefPage)
            {
                continue;
            }

            RowBound bound = Assert.Single(DataPageRows.EnumerateLiveRowBounds(profile, page));
            Assert.True(RowDecodePlan.TryParseRowLayout(profile.RowFields, page, bound.RowStart, bound.RowSize, definition.HasVarColumns, out RowLayout layout));
            ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(profile.RowFields, page, bound.RowStart, bound.RowSize, layout, column);
            Assert.True(LongValueDescriptor.TryRead(page.AsSpan(bound.RowStart + slice.DataStart, slice.DataLen), out LongValueDescriptor descriptor));
            Assert.True(descriptor.IsSinglePage);
            return (profile, descriptor);
        }

        throw new InvalidOperationException("The created database must contain the external long value.");
    }

    private static async Task<object?> ReadNaturalProjectionAsync(AccessReader reader, bool ole, CancellationToken ct)
        => ole
            ? Assert.Single(await reader.Rows<OleRow>("Values", cancellationToken: ct).ToListAsync(ct)).Value
            : Assert.Single(await reader.Rows<MemoRow>("Values", cancellationToken: ct).ToListAsync(ct)).Value;

    private sealed class ValueRow
    {
        public object? Value { get; set; }
    }

    private sealed class OleRow
    {
        public byte[]? Value { get; set; }
    }

    private sealed class MemoRow
    {
        public string? Value { get; set; }
    }

    private sealed class FaultingMemoryStream(byte[] bytes) : MemoryStream(bytes, writable: false)
    {
        internal long FailureOffset { get; set; } = -1;

        internal IOException Failure { get; } = new("payload page I/O failure");

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => this.Position == this.FailureOffset ? ValueTask.FromException<int>(this.Failure) : base.ReadAsync(buffer, cancellationToken);
    }
}
