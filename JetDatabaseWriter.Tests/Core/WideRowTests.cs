namespace JetDatabaseWriter.Tests;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

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
        await using var ms = await CopyToStreamAsync(path);
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
        await using var ms = await CopyToStreamAsync(path);
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
        await using var ms = await CopyToStreamAsync(path);
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
        await using var ms = await CopyToStreamAsync(path);
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

    private async ValueTask<MemoryStream> CopyToStreamAsync(string sourcePath)
    {
        byte[] bytes = await db.GetFileAsync(sourcePath, TestContext.Current.CancellationToken);
        var ms = new MemoryStream();
        await ms.WriteAsync(bytes, TestContext.Current.CancellationToken);
        ms.Position = 0;
        return ms;
    }
}
