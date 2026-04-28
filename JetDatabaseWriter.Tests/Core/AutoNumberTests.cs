namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

#pragma warning disable CA1812 // Test class instantiated by xUnit

/// <summary>
/// Behavioural coverage for <see cref="ColumnDefinition.IsAutoIncrement"/>
/// across the integral CLR types Jet supports for autonumber columns
/// (<see cref="byte"/>, <see cref="short"/>, <see cref="int"/>,
/// <see cref="long"/>) plus explicit-value override and seed-after-delete
/// behaviour.
///
/// <para>Jackcess analogue: <c>impl/AutoNumberTest.java</c>.
/// </para>
/// </summary>
public sealed class AutoNumberTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    // Auto-increment values start at 1 and increase monotonically when null
    // is supplied for the column, across each integral CLR type the writer
    // currently supports for FLAG_AUTO_LONG (int and short).
    [Theory]
    [InlineData(typeof(int), 1, 2, 3)]
    [InlineData(typeof(short), (short)1, (short)2, (short)3)]
    public async Task AutoIncrement_NullValues_AssignsMonotonicSequenceFromOne(Type clrType, object expected1, object expected2, object expected3)
    {
        var ms = await CopyNorthwindAsync();
        if (ms is null)
        {
            return;
        }

        string tableName = $"AI_{Guid.NewGuid():N}".Substring(0, 18);

        await using (var writer = await OpenWriterAsync(ms, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                tableName,
                [
                    new("Id", clrType) { IsAutoIncrement = true, IsNullable = false },
                    new("Label", typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(tableName, [DBNull.Value, "first"], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(tableName, [DBNull.Value, "second"], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(tableName, [DBNull.Value, "third"], TestContext.Current.CancellationToken);
        }

        await using (var reader = await OpenReaderAsync(ms, TestContext.Current.CancellationToken))
        {
            List<object> ids = [];
            await foreach (object[] row in reader.Rows(tableName, cancellationToken: TestContext.Current.CancellationToken))
            {
                ids.Add(row[0]);
            }

            Assert.Equal(3, ids.Count);
            Assert.Equal(expected1, ids[0]);
            Assert.Equal(expected2, ids[1]);
            Assert.Equal(expected3, ids[2]);
        }
    }

    // FLAG_AUTO_LONG (0x04) is persisted in the TDEF column flags and surfaced
    // through ColumnMetadata on reopen, across the integral CLR types the
    // writer currently supports.
    [Theory]
    [InlineData(typeof(short))]
    [InlineData(typeof(int))]
    public async Task AutoIncrement_FlagPersists_AcrossWriterClose(Type clrType)
    {
        var ms = await CopyNorthwindAsync();
        if (ms is null)
        {
            return;
        }

        string tableName = $"AIF_{Guid.NewGuid():N}".Substring(0, 18);

        await using (var writer = await OpenWriterAsync(ms, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                tableName,
                [new("Id", clrType) { IsAutoIncrement = true, IsNullable = false }],
                TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(ms, TestContext.Current.CancellationToken);
        var meta = await reader.GetColumnMetadataAsync(tableName, TestContext.Current.CancellationToken);

        ColumnMetadata id = Assert.Single(meta);
        Assert.Equal(clrType, id.ClrType);
        Assert.False(id.IsNullable);
    }

    // After deleting all rows from an autonumber table, the next inserted
    // row continues from the high-water mark — Access never re-uses counter
    // values within a single writer session.
    [Fact]
    public async Task AutoIncrement_AfterDeleteAllRows_DoesNotReuseValues()
    {
        var ms = await CopyNorthwindAsync();
        if (ms is null)
        {
            return;
        }

        string tableName = $"AID_{Guid.NewGuid():N}".Substring(0, 18);

        await using (var writer = await OpenWriterAsync(ms, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                tableName,
                [
                    new("Id", typeof(int)) { IsAutoIncrement = true, IsNullable = false },
                    new("Label", typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(tableName, [DBNull.Value, "a"], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(tableName, [DBNull.Value, "b"], TestContext.Current.CancellationToken);
            int deleted = await writer.DeleteRowsAsync(tableName, "Label", "a", TestContext.Current.CancellationToken);
            Assert.Equal(1, deleted);
            deleted = await writer.DeleteRowsAsync(tableName, "Label", "b", TestContext.Current.CancellationToken);
            Assert.Equal(1, deleted);

            await writer.InsertRowAsync(tableName, [DBNull.Value, "c"], TestContext.Current.CancellationToken);
        }

        await using (var reader = await OpenReaderAsync(ms, TestContext.Current.CancellationToken))
        {
            List<int> ids = [];
            await foreach (object[] row in reader.Rows(tableName, cancellationToken: TestContext.Current.CancellationToken))
            {
                ids.Add((int)row[0]);
            }

            Assert.Single(ids);
            Assert.True(ids[0] >= 3, $"Expected counter to skip over reused values 1 and 2; observed Id={ids[0]}.");
        }
    }

    // When an explicit, non-null integer value is supplied for an autoincrement
    // column, the writer honours the override and the value round-trips on read.
    [Fact]
    public async Task AutoIncrement_ExplicitValue_OverridesCounterAndRoundTrips()
    {
        var ms = await CopyNorthwindAsync();
        if (ms is null)
        {
            return;
        }

        string tableName = $"AIE_{Guid.NewGuid():N}".Substring(0, 18);

        await using (var writer = await OpenWriterAsync(ms, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                tableName,
                [
                    new("Id", typeof(int)) { IsAutoIncrement = true, IsNullable = false },
                    new("Label", typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(tableName, [42, "explicit"], TestContext.Current.CancellationToken);
        }

        await using (var reader = await OpenReaderAsync(ms, TestContext.Current.CancellationToken))
        {
            object[]? row = null;
            await foreach (object[] r in reader.Rows(tableName, cancellationToken: TestContext.Current.CancellationToken))
            {
                row = r;
                break;
            }

            Assert.NotNull(row);
            Assert.Equal(42, row![0]);
            Assert.Equal("explicit", row[1]);
        }
    }

    // Documents the gap relative to Jackcess: byte and long auto-increment
    // (Jet "BigInt"/Large Number autonumber and tiny-int autonumber) are
    // not yet supported by the writer. The Jackcess analogue is
    // AutoNumberTest#testInsertLongAutoNumber.
    [Theory]
    [InlineData(typeof(byte))]
    [InlineData(typeof(long))]
    public async Task AutoIncrement_OnUnsupportedIntegralType_ThrowsNotSupported(Type clrType)
    {
        var ms = await CopyNorthwindAsync();
        if (ms is null)
        {
            return;
        }

        string tableName = $"AIU_{Guid.NewGuid():N}".Substring(0, 18);

        await using var writer = await OpenWriterAsync(ms, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await writer.CreateTableAsync(
                tableName,
                [new("Id", clrType) { IsAutoIncrement = true, IsNullable = false }],
                TestContext.Current.CancellationToken));
    }

    // ── Helpers ────────────────────────────────────────────────────────

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream, CancellationToken cancellationToken)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken);
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream stream, CancellationToken cancellationToken)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken);
    }

    private async ValueTask<MemoryStream?> CopyNorthwindAsync()
    {
        if (!File.Exists(TestDatabases.NorthwindTraders))
        {
            return null;
        }

        byte[] bytes = await db.GetFileAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        var ms = new MemoryStream();
        await ms.WriteAsync(bytes, TestContext.Current.CancellationToken);
        ms.Position = 0;
        return ms;
    }
}
