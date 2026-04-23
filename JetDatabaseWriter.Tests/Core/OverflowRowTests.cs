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
/// Tests for overflow rows — rows whose payload exceeds the space available
/// on a single data page and must span multiple pages.
///
/// Overflow pointer rows (offset bit 0x4000) are followed to the target
/// page/row to read the actual data, rather than being skipped like
/// deleted rows (0x8000). A Jet4 data page is 4096 bytes; rows larger than
/// ~3800 bytes will trigger overflow or LVAL usage.
/// </summary>
public sealed class OverflowRowTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task OverflowRows_LargeRowData_IsReadableBack(string path)
    {
        // Arrange — create a table whose rows are large enough that the JET
        // engine may need to overflow them across pages.
        // A Jet4 data page is 4096 bytes.  With overhead, a row payload > ~3800 bytes
        // should force overflow or LVAL usage.
        await using var ms = await CopyToStreamAsync(path);
        if (!IsJet4(ms))
        {
            return; // overflow handling only applies to Jet4/ACE
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

        // Each row: 4 bytes int + 4×510 bytes UCS-2 ≈ 2044 bytes
        // Fill the page to capacity so later rows may overflow
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

        // Act — read back all rows
        await using var reader = await OpenReaderAsync(ms, TestContext.Current.CancellationToken);
        DataTable dt = (await reader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken))!;

        // Assert — every inserted row should be returned, including any that overflow
        Assert.Equal(rowCount, dt.Rows.Count);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task OverflowRows_GetRealRowCount_IncludesOverflowRows(string path)
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
    public async Task OverflowRows_StreamRows_YieldsAllRows(string path)
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
        int streamed = await reader.StreamRowsAsync(tableName, cancellationToken: TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);

        Assert.Equal(rowCount, streamed);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task OverflowRows_LargeRowContent_IsPreserved(string path)
    {
        // Verify that the actual cell values survive an overflow round-trip
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
