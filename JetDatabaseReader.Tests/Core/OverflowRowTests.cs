namespace JetDatabaseReader.Tests;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
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
public sealed class OverflowRowTests : IDisposable
{
    private readonly List<string> _tempFiles = [];

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void OverflowRows_LargeRowData_IsReadableBack(string path)
    {
        // Arrange — create a table whose rows are large enough that the JET
        // engine may need to overflow them across pages.
        // A Jet4 data page is 4096 bytes.  With overhead, a row payload > ~3800 bytes
        // should force overflow or LVAL usage.
        string temp = CopyToTemp(path);
        if (!IsJet4(temp))
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

        using (var writer = AccessWriter.Open(temp))
        {
            writer.CreateTable(tableName, columns);
            writer.InsertRows(tableName, rows);
        }

        // Act — read back all rows
        using var reader = AccessReader.Open(temp);
        DataTable dt = reader.ReadTable(tableName)!;

        // Assert — every inserted row should be returned, including any that overflow
        Assert.Equal(rowCount, dt.Rows.Count);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void OverflowRows_GetRealRowCount_IncludesOverflowRows(string path)
    {
        string temp = CopyToTemp(path);
        if (!IsJet4(temp))
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

        using (var writer = AccessWriter.Open(temp))
        {
            writer.CreateTable(tableName, columns);
            writer.InsertRows(tableName, rows);
        }

        using var reader = AccessReader.Open(temp);
        long count = reader.GetRealRowCount(tableName);

        Assert.Equal(rowCount, count);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void OverflowRows_StreamRows_YieldsAllRows(string path)
    {
        string temp = CopyToTemp(path);
        if (!IsJet4(temp))
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

        using (var writer = AccessWriter.Open(temp))
        {
            writer.CreateTable(tableName, columns);
            writer.InsertRows(tableName, rows);
        }

        using var reader = AccessReader.Open(temp);
        int streamed = reader.StreamRows(tableName).Count();

        Assert.Equal(rowCount, streamed);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void OverflowRows_LargeRowContent_IsPreserved(string path)
    {
        // Verify that the actual cell values survive an overflow round-trip
        string temp = CopyToTemp(path);
        if (!IsJet4(temp))
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

        using (var writer = AccessWriter.Open(temp))
        {
            writer.CreateTable(tableName, columns);
            writer.InsertRow(tableName, new object[] { 1, expectedText1, expectedText2 });
        }

        using var reader = AccessReader.Open(temp);
        DataTable dt = reader.ReadTable(tableName)!;

        Assert.Single(dt.Rows);
        Assert.Equal(1, dt.Rows[0]["Id"]);
        Assert.Equal(expectedText1, dt.Rows[0]["Text1"]);
        Assert.Equal(expectedText2, dt.Rows[0]["Text2"]);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════════

    public void Dispose()
    {
        foreach (string path in _tempFiles)
        {
            try
            {
                File.Delete(path);
            }
            catch (IOException)
            {
                /* best-effort cleanup */
            }
        }
    }

    private static bool IsJet4(string path)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        var hdr = new byte[0x20];
        _ = fs.Read(hdr, 0, hdr.Length);
        return hdr[0x14] >= 1;
    }

    private string CopyToTemp(string sourcePath)
    {
        string ext = Path.GetExtension(sourcePath);
        string temp = Path.Combine(Path.GetTempPath(), $"JetOvfTest_{Guid.NewGuid():N}{ext}");
        _tempFiles.Add(temp);
        File.Copy(sourcePath, temp, overwrite: true);
        return temp;
    }
}
