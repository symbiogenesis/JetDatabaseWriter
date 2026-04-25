namespace JetDatabaseWriter.Tests;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Tests for Phase C7 — per-flat-table PK / FK indexes that the previous C3
/// MVP relied on Microsoft Access to rebuild on Compact &amp; Repair. The
/// attachment flat-table layout is verified against
/// <c>format-probe-appendix-complex.md</c>
/// (the <c>f_A3DF50CFC033433899AF0AC1A4CF4171_Attachments</c> probe of
/// <c>ComplexFields.accdb</c>); the multi-value layout mirrors that pattern
/// minus the composite secondary index because no real-Access fixture
/// exercises a multi-value flat table in the format-probe corpus.
/// </summary>
public sealed class ComplexColumnsFlatIndexesTests
{
    [Fact]
    public async Task Attachment_FlatTable_HasAutoincrementScalarPkColumn()
    {
        await using var reader = await CreateAndReadAttachmentFlat();
        IReadOnlyList<ComplexColumnInfo> info = await reader.GetComplexColumnsAsync("Documents", TestContext.Current.CancellationToken);
        ComplexColumnInfo att = Assert.Single(info);

        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(att.FlatTableName, TestContext.Current.CancellationToken);
        ColumnMetadata scalar = meta.Single(m => string.Equals(m.Name, "Documents_Files", StringComparison.Ordinal));
        Assert.Equal(typeof(int), scalar.ClrType);

        // The FK back-reference column to the parent's ConceptualTableID
        // remains in place as a plain LONG.
        ColumnMetadata fk = meta.Single(m => string.Equals(m.Name, "_Files", StringComparison.Ordinal));
        Assert.Equal(typeof(int), fk.ClrType);
    }

    [Fact]
    public async Task Attachment_FlatTable_EmitsThreeIndexes()
    {
        await using var reader = await CreateAndReadAttachmentFlat();
        IReadOnlyList<ComplexColumnInfo> info = await reader.GetComplexColumnsAsync("Documents", TestContext.Current.CancellationToken);
        ComplexColumnInfo att = Assert.Single(info);

        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(att.FlatTableName, TestContext.Current.CancellationToken);

        Assert.Equal(3, indexes.Count);

        IndexMetadata pk = indexes.Single(i => i.Kind == IndexKind.PrimaryKey);
        Assert.Equal("MSysComplexPKIndex", pk.Name);
        ColumnReferenceCheck(pk.Columns, "Documents_Files");

        IndexMetadata fk = indexes.Single(i => string.Equals(i.Name, "_Files", StringComparison.Ordinal));
        Assert.Equal(IndexKind.Normal, fk.Kind);
        ColumnReferenceCheck(fk.Columns, "_Files");

        IndexMetadata composite = indexes.Single(i => string.Equals(i.Name, "IdxFKPrimaryScalar", StringComparison.Ordinal));
        Assert.Equal(IndexKind.Normal, composite.Kind);
        ColumnReferenceCheck(composite.Columns, "_Files", "FileName");
    }

    [Fact]
    public async Task MultiValue_FlatTable_EmitsTwoIndexes()
    {
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Tags",
                new[]
                {
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Items", typeof(object), maxLength: 50) { IsMultiValue = true, MultiValueElementType = typeof(string) },
                },
                TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        IReadOnlyList<ComplexColumnInfo> info = await reader.GetComplexColumnsAsync("Tags", TestContext.Current.CancellationToken);
        ComplexColumnInfo mv = Assert.Single(info);

        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(mv.FlatTableName, TestContext.Current.CancellationToken);
        ColumnMetadata scalar = meta.Single(m => string.Equals(m.Name, "Tags_Items", StringComparison.Ordinal));
        Assert.Equal(typeof(int), scalar.ClrType);

        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(mv.FlatTableName, TestContext.Current.CancellationToken);
        Assert.Equal(2, indexes.Count);

        IndexMetadata pk = indexes.Single(i => i.Kind == IndexKind.PrimaryKey);
        Assert.Equal("MSysComplexPKIndex", pk.Name);
        ColumnReferenceCheck(pk.Columns, "Tags_Items");

        IndexMetadata fk = indexes.Single(i => string.Equals(i.Name, "_Items", StringComparison.Ordinal));
        Assert.Equal(IndexKind.Normal, fk.Kind);
    }

    [Fact]
    public async Task AddAttachment_FillsAutoincrementScalar_AndSurvivesRoundTrip()
    {
        // The C7 schema introduces a NOT-NULL autoincrement column. The
        // AddAttachmentAsync path must hydrate the constraint from the
        // FLAG_AUTO_LONG bit and seed the next value from existing rows so
        // multiple attachments per parent get distinct scalar PKs.
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Documents",
                new[]
                {
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true },
                },
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(
                "Documents",
                new object[] { 1, DBNull.Value },
                TestContext.Current.CancellationToken);

            await writer.AddAttachmentAsync(
                "Documents",
                "Files",
                new Dictionary<string, object> { ["Id"] = 1 },
                new AttachmentInput("a.txt", new byte[] { 1, 2, 3 }),
                TestContext.Current.CancellationToken);

            await writer.AddAttachmentAsync(
                "Documents",
                "Files",
                new Dictionary<string, object> { ["Id"] = 1 },
                new AttachmentInput("b.txt", new byte[] { 4, 5, 6 }),
                TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        IReadOnlyList<ComplexColumnInfo> info = await reader.GetComplexColumnsAsync("Documents", TestContext.Current.CancellationToken);
        ComplexColumnInfo att = Assert.Single(info);

        // Read the raw flat-table rows and assert the autoincrement scalar PK
        // column carries two distinct values.
        System.Data.DataTable dt = await reader.ReadDataTableAsync(att.FlatTableName, cancellationToken: TestContext.Current.CancellationToken)
            ?? throw new InvalidOperationException("Flat table not found.");
        Assert.Equal(2, dt.Rows.Count);

        var scalars = dt.Rows.Cast<System.Data.DataRow>()
            .Select(r => Convert.ToInt32(r["Documents_Files"], System.Globalization.CultureInfo.InvariantCulture))
            .OrderBy(v => v)
            .ToArray();
        Assert.Equal(2, scalars.Distinct().Count());
        Assert.True(scalars[1] > scalars[0], "Autoincrement scalar PK must be monotonically increasing.");

        // Spec-compliant attachment read still works.
        IReadOnlyList<AttachmentRecord> attachments = await reader.GetAttachmentsAsync("Documents", "Files", TestContext.Current.CancellationToken);
        Assert.Equal(2, attachments.Count);
    }

    private static async ValueTask<AccessReader> CreateAndReadAttachmentFlat()
    {
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Documents",
                new[]
                {
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true },
                },
                TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        return await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
    }

    private static void ColumnReferenceCheck(IReadOnlyList<IndexColumnReference> actual, params string[] expected)
    {
        Assert.Equal(expected.Length, actual.Count);
        for (int i = 0; i < expected.Length; i++)
        {
            Assert.Equal(expected[i], actual[i].Name);
        }
    }
}
