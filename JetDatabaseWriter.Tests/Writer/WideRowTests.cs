namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Round-trip tests for tables with wide rows (multiple max-length text
/// columns). These exercise the row-pointer / data-page code path on
/// payloads close to but under the Jet4 single-page threshold; they do not
/// themselves assert overflow-page allocation. True LVAL/overflow paths
/// are covered by the LVAL and complex-column test suites.
/// </summary>
public sealed class WideRowTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task WideRows_RoundTrip_ReturnsAllRows(string path)
    {
        await using var ms = await db.CopyToStreamAsync(path, TestContext.Current.CancellationToken);
        if (!IsJet4(ms))
        {
            return; // wide-row layout under test only applies to Jet4/ACE
        }

        string tableName = $"Overflow_{Guid.NewGuid():N}"[..20];
        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("BigText1", typeof(string), maxLength: 255),
            new("BigText2", typeof(string), maxLength: 255),
            new("BigText3", typeof(string), maxLength: 255),
            new("BigText4", typeof(string), maxLength: 255),
        };

        // Each row: 4 bytes int + 4×510 bytes UCS-2 ≈ 2044 bytes.
        const int rowCount = 20;
        var rows = Enumerable.Range(1, rowCount).Select(i => new object[]
        {
            i,
            new string('A', 255),
            new string('B', 255),
            new string('C', 255),
            new string('D', 255),
        });

        await using (var writer = await OpenWriterAsync(ms, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken);
            await writer.InsertRowsAsync(tableName, rows, TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(ms, TestContext.Current.CancellationToken);
        DataTable dt = (await reader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken))!;

        Assert.Equal(rowCount, dt.Rows.Count);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task WideRows_GetRealRowCount_MatchesInsertCount(string path)
    {
        await using var ms = await db.CopyToStreamAsync(path, TestContext.Current.CancellationToken);
        if (!IsJet4(ms))
        {
            return;
        }

        string tableName = $"OvfCnt_{Guid.NewGuid():N}"[..18];
        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Payload", typeof(string), maxLength: 255),
        };

        const int rowCount = 50;
        var rows = Enumerable.Range(1, rowCount).Select(i => new object[]
        {
            i,
            new string('X', 255),
        });

        await using (var writer = await OpenWriterAsync(ms, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken);
            await writer.InsertRowsAsync(tableName, rows, TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(ms, TestContext.Current.CancellationToken);
        long count = await reader.GetRealRowCountAsync(tableName, TestContext.Current.CancellationToken);

        Assert.Equal(rowCount, count);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task WideRows_StreamRows_YieldsAllRows(string path)
    {
        await using var ms = await db.CopyToStreamAsync(path, TestContext.Current.CancellationToken);
        if (!IsJet4(ms))
        {
            return;
        }

        string tableName = $"OvfStr_{Guid.NewGuid():N}"[..18];
        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Data1", typeof(string), maxLength: 255),
            new("Data2", typeof(string), maxLength: 255),
        };

        const int rowCount = 30;
        var rows = Enumerable.Range(1, rowCount).Select(i => new object[]
        {
            i,
            new string((char)('A' + (i % 26)), 255),
            new string((char)('a' + (i % 26)), 255),
        });

        await using (var writer = await OpenWriterAsync(ms, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken);
            await writer.InsertRowsAsync(tableName, rows, TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(ms, TestContext.Current.CancellationToken);
        int streamed = await reader.Rows(tableName, cancellationToken: TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);

        Assert.Equal(rowCount, streamed);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task WideRows_CellValues_AreRoundTripped(string path)
    {
        // Verify that the actual cell values survive a wide-row round-trip.
        await using var ms = await db.CopyToStreamAsync(path, TestContext.Current.CancellationToken);
        if (!IsJet4(ms))
        {
            return;
        }

        string tableName = $"OvfVal_{Guid.NewGuid():N}"[..18];
        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Text1", typeof(string), maxLength: 255),
            new("Text2", typeof(string), maxLength: 255),
        };

        string expectedText1 = new('Z', 200);
        string expectedText2 = new('Q', 200);

        await using (var writer = await OpenWriterAsync(ms, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(tableName, [1, expectedText1, expectedText2], TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(ms, TestContext.Current.CancellationToken);
        DataTable dt = (await reader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken))!;

        Assert.Single(dt.Rows);
        Assert.Equal(1, dt.Rows[0]["Id"]);
        Assert.Equal(expectedText1, dt.Rows[0]["Text1"]);
        Assert.Equal(expectedText2, dt.Rows[0]["Text2"]);
    }

    /// <summary>
    /// Round-trip rows where the null-mask byte boundary falls exactly
    /// between columns. The null mask is one bit per column; for column
    /// counts of 8 / 9 / 16 / 17 / 24 / 25 the mask byte width changes
    /// by exactly one byte, which is where mdbtools historically had
    /// off-by-one bugs (a row with column 9 nullable would see the mask
    /// read as if only 8 columns existed, dropping the value).
    /// </summary>
    /// <param name="columnCount">Total number of nullable int columns to declare.</param>
    [Theory]
    [InlineData(8)]
    [InlineData(9)]
    [InlineData(16)]
    [InlineData(17)]
    [InlineData(24)]
    [InlineData(25)]
    [InlineData(32)]
    [InlineData(33)]
    public async Task NullMask_AcrossEightByteBoundaries_RoundTripsCorrectly(int columnCount)
    {
        // Use a fresh in-memory ACE database so we don't pollute one of
        // the cached fixtures with a wide schema.
        await using var ms = new MemoryStream();
        await using var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);
        string tableName = $"NM{columnCount}_{Guid.NewGuid():N}".Substring(0, 18);

        var columns = new List<ColumnDefinition>(columnCount);
        for (int i = 0; i < columnCount; i++)
        {
            columns.Add(new ColumnDefinition($"C{i:D2}", typeof(int)));
        }

        await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken);

        // Row A: every column populated. Verifies the writer doesn't
        // accidentally null out any column when the mask happens to
        // be all-ones.
        object[] allSet = Enumerable.Range(0, columnCount).Select(i => (object)(i + 1)).ToArray();

        // Row B: alternating null / value pattern. With column count
        // 8 / 16 / 24 / 32 this exactly fills mask byte boundaries;
        // 9 / 17 / 25 / 33 forces one extra byte of mask with a
        // single bit set, the off-by-one historic bug.
        object[] alternating = new object[columnCount];
        for (int i = 0; i < columnCount; i++)
        {
            alternating[i] = i % 2 == 0 ? i + 100 : DBNull.Value;
        }

        // Row C: every column null. Verifies the writer doesn't
        // truncate trailing-null columns past the mask byte width.
        object[] allNull = Enumerable.Range(0, columnCount).Select(_ => (object)DBNull.Value).ToArray();

        await writer.InsertRowAsync(tableName, allSet, TestContext.Current.CancellationToken);
        await writer.InsertRowAsync(tableName, alternating, TestContext.Current.CancellationToken);
        await writer.InsertRowAsync(tableName, allNull, TestContext.Current.CancellationToken);

        await using var reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
        DataTable dt = (await reader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken))!;

        Assert.Equal(3, dt.Rows.Count);

        // Row A: every column matches its seed value, no DBNull leaked.
        for (int i = 0; i < columnCount; i++)
        {
            object cell = dt.Rows[0][$"C{i:D2}"];
            Assert.IsNotType<DBNull>(cell);
            Assert.Equal(i + 1, Convert.ToInt32(cell, System.Globalization.CultureInfo.InvariantCulture));
        }

        // Row B: alternating — even columns hold (i+100), odd are null.
        for (int i = 0; i < columnCount; i++)
        {
            object cell = dt.Rows[1][$"C{i:D2}"];
            if (i % 2 == 0)
            {
                Assert.IsNotType<DBNull>(cell);
                Assert.Equal(i + 100, Convert.ToInt32(cell, System.Globalization.CultureInfo.InvariantCulture));
            }
            else
            {
                Assert.IsType<DBNull>(cell);
            }
        }

        // Row C: every column is DBNull.
        for (int i = 0; i < columnCount; i++)
        {
            Assert.IsType<DBNull>(dt.Rows[2][$"C{i:D2}"]);
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════════

    private static bool IsJet4(MemoryStream stream)
    {
        if (stream.Length < 0x20)
        {
            return false;
        }

        stream.Position = 0x14;
        return stream.ReadByte() >= 1;
    }

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
}
