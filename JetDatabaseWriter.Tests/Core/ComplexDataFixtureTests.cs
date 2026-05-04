namespace JetDatabaseWriter.Tests.Core;

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Fixture-based read tests for the Jackcess <c>complexDataTestV2007.accdb</c>
/// fixture. Verifies that the reader correctly decodes complex column
/// metadata (attachment, multi-value, and version-history kinds) and
/// retrieves attachment payloads that were stored with deflate compression
/// (txt extension → typeFlag <c>0x01</c>).
/// Covers §2.2 of <c>docs/design/test-coverage-gaps.md</c>.
/// </summary>
public sealed class ComplexDataFixtureTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    /// <summary>
    /// The fixture has exactly one user table (<c>Table1</c>).
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.ComplexData), MemberType = typeof(TestDatabases))]
    public async Task ComplexDataFixture_ListTables_ReturnsSingleTable(string path)
    {
        AccessReader reader = await db.GetReaderAsync(
            path,
            TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.Single(tables);
        Assert.Equal("Table1", tables[0]);
    }

    /// <summary>
    /// <c>Table1</c> has at least one attachment-kind complex column
    /// (<c>attach-data</c>).
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.ComplexData), MemberType = typeof(TestDatabases))]
    public async Task Table1_HasAttachmentComplexColumn(string path)
    {
        AccessReader reader = await db.GetReaderAsync(
            path,
            TestContext.Current.CancellationToken);

        IReadOnlyList<ComplexColumnInfo> complex = await reader.GetComplexColumnsAsync(
            "Table1",
            TestContext.Current.CancellationToken);

        Assert.Contains(complex, c =>
            c.ColumnName == "attach-data" &&
            c.Kind == ComplexColumnKind.Attachment);
    }

    /// <summary>
    /// <c>Table1</c> has a multi-value complex column (<c>multi-value-data</c>).
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.ComplexData), MemberType = typeof(TestDatabases))]
    public async Task Table1_HasMultiValueComplexColumn(string path)
    {
        AccessReader reader = await db.GetReaderAsync(
            path,
            TestContext.Current.CancellationToken);

        IReadOnlyList<ComplexColumnInfo> complex = await reader.GetComplexColumnsAsync(
            "Table1",
            TestContext.Current.CancellationToken);

        Assert.Contains(complex, c =>
            c.ColumnName == "multi-value-data" &&
            c.Kind == ComplexColumnKind.MultiValue);
    }

    /// <summary>
    /// The attachment column (<c>attach-data</c>) contains 3 attachments
    /// with <c>.txt</c> file type. Because <c>txt</c> is not in the
    /// COMPRESSED_FORMATS skip-list, these payloads are stored with deflate
    /// compression (typeFlag <c>0x01</c>) and decoded correctly by the reader.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.ComplexData), MemberType = typeof(TestDatabases))]
    public async Task AttachData_ReturnsThreeDeflateCompressedTxtFiles(string path)
    {
        AccessReader reader = await db.GetReaderAsync(
            path,
            TestContext.Current.CancellationToken);

        IReadOnlyList<AttachmentRecord> attachments = await reader.GetAttachmentsAsync(
            "Table1",
            "attach-data",
            TestContext.Current.CancellationToken);

        Assert.Equal(3, attachments.Count);
        Assert.All(attachments, a =>
        {
            Assert.Equal("txt", a.FileType);
            Assert.True(a.FileData.Length > 0, "Decoded FileData must be non-empty.");
        });
    }

    /// <summary>
    /// The attachment file names match the known Jackcess test data:
    /// <c>test_data.txt</c> and <c>test_data2.txt</c> (×2).
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.ComplexData), MemberType = typeof(TestDatabases))]
    public async Task AttachData_HasExpectedFileNames(string path)
    {
        AccessReader reader = await db.GetReaderAsync(
            path,
            TestContext.Current.CancellationToken);

        IReadOnlyList<AttachmentRecord> attachments = await reader.GetAttachmentsAsync(
            "Table1",
            "attach-data",
            TestContext.Current.CancellationToken);

        string[] names = attachments.Select(a => a.FileName).OrderBy(n => n).ToArray();
        Assert.Equal("test_data.txt", names[0]);
        Assert.Equal("test_data2.txt", names[1]);
        Assert.Equal("test_data2.txt", names[2]);
    }

    /// <summary>
    /// The multi-value column (<c>multi-value-data</c>) contains at least 5
    /// items across all parent rows.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.ComplexData), MemberType = typeof(TestDatabases))]
    public async Task MultiValueData_ReturnsAtLeastFiveItems(string path)
    {
        AccessReader reader = await db.GetReaderAsync(
            path,
            TestContext.Current.CancellationToken);

        IReadOnlyList<(int ConceptualTableId, object? Value)> items =
            await reader.GetMultiValueItemsAsync(
                "Table1",
                "multi-value-data",
                TestContext.Current.CancellationToken);

        Assert.True(items.Count >= 5, $"Expected at least 5 multi-value items, got {items.Count}.");

        // Known values from the Jackcess test data include "value1"..."value4".
        string[] values = items.Select(i => i.Value?.ToString() ?? string.Empty).ToArray();
        Assert.Contains("value1", values);
        Assert.Contains("value4", values);
    }

    /// <summary>
    /// All 4 rows in <c>Table1</c> can be streamed without throwing,
    /// even though the table contains complex columns.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.ComplexData), MemberType = typeof(TestDatabases))]
    public async Task Table1_StreamsAllRows_WithoutThrowing(string path)
    {
        AccessReader reader = await db.GetReaderAsync(
            path,
            TestContext.Current.CancellationToken);

        int rowCount = 0;
        await foreach (object[] row in reader.Rows(
            "Table1",
            cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.NotNull(row);
            rowCount++;
        }

        Assert.Equal(4, rowCount);
    }
}
