namespace JetDatabaseWriter.Tests;

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Tests for the Phase C1 + C2 complex-columns writer surface
/// (see <c>docs/design/complex-columns-format-notes.md</c>).
/// <list type="bullet">
///   <item>C1: <c>MSysComplexColumns</c> system table is scaffolded into every fresh ACCDB.</item>
///   <item>C2: <see cref="ColumnDefinition.IsAttachment"/> / <see cref="ColumnDefinition.IsMultiValue"/>
///         declarations are recognized; <c>CreateTableAsync</c> rejects them with
///         <see cref="NotSupportedException"/> until C3 lands the hidden flat-table
///         emission path.</item>
/// </list>
/// </summary>
public sealed class ComplexColumnsWriterTests
{
    // ── C1: MSysComplexColumns scaffold ────────────────────────────────────────

    [Fact]
    public async Task CreateDatabaseAsync_AceAccdb_FullCatalog_EmitsMSysComplexColumns()
    {
        var ms = new MemoryStream();
        await using (await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        var meta = await reader.GetColumnMetadataAsync("MSysComplexColumns", TestContext.Current.CancellationToken);
        var names = meta.Select(m => m.Name).ToArray();

        Assert.Contains("ColumnName", names);
        Assert.Contains("ComplexID", names);
        Assert.Contains("ConceptualTableID", names);
        Assert.Contains("FlatTableID", names);
        Assert.Contains("ComplexTypeObjectID", names);
    }

    [Fact]
    public async Task CreateDatabaseAsync_AceAccdb_MSysComplexColumns_IsHiddenFromUserTables()
    {
        var ms = new MemoryStream();
        await using (await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        var userTables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        // The scaffold must set the system flag (0x80000000) so MSysComplexColumns
        // does not appear in the user-table listing.
        Assert.DoesNotContain("MSysComplexColumns", userTables, StringComparer.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task CreateDatabaseAsync_Jet4Mdb_DoesNotEmitMSysComplexColumns()
    {
        // Complex columns are an Access 2007+ ACCDB feature — the system table
        // must not be added to .mdb scaffolds.
        var ms = new MemoryStream();
        await using (await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.Jet4Mdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        var meta = await reader.GetColumnMetadataAsync("MSysComplexColumns", TestContext.Current.CancellationToken);
        Assert.Empty(meta);
    }

    [Fact]
    public async Task CreateDatabaseAsync_AceAccdb_SlimCatalog_DoesNotEmitMSysComplexColumns()
    {
        var ms = new MemoryStream();
        var options = new AccessWriterOptions { WriteFullCatalogSchema = false };
        await using (await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.AceAccdb, options, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        var meta = await reader.GetColumnMetadataAsync("MSysComplexColumns", TestContext.Current.CancellationToken);
        Assert.Empty(meta);
    }

    // ── C2: ColumnDefinition declaration surface ───────────────────────────────

    [Fact]
    public void ColumnDefinition_Defaults_AreNonComplex()
    {
        var def = new ColumnDefinition("X", typeof(int));

        Assert.False(def.IsAttachment);
        Assert.False(def.IsMultiValue);
        Assert.Null(def.MultiValueElementType);
    }

    [Fact]
    public void ColumnDefinition_AsAttachment_FlagIsSet()
    {
        var def = new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true };

        Assert.True(def.IsAttachment);
        Assert.False(def.IsMultiValue);
    }

    [Fact]
    public void ColumnDefinition_AsMultiValue_CarriesElementType()
    {
        var def = new ColumnDefinition("Tags", typeof(object))
        {
            IsMultiValue = true,
            MultiValueElementType = typeof(string),
        };

        Assert.True(def.IsMultiValue);
        Assert.Equal(typeof(string), def.MultiValueElementType);
    }

    [Fact]
    public async Task CreateTableAsync_AttachmentColumn_C3_RoundTripsViaGetComplexColumns()
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
        await using var reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        var info = await reader.GetComplexColumnsAsync("Documents", TestContext.Current.CancellationToken);
        var attachment = Assert.Single(info);
        Assert.Equal("Files", attachment.ColumnName);
        Assert.True(attachment.ComplexId > 0);
        Assert.True(attachment.FlatTableId > 0);
        Assert.StartsWith("f_", attachment.FlatTableName, StringComparison.Ordinal);
        Assert.EndsWith("_Files", attachment.FlatTableName, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CreateTableAsync_MultiValueColumn_C3_RoundTripsViaGetComplexColumns()
    {
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Things",
                new[]
                {
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Tags", typeof(object))
                    {
                        IsMultiValue = true,
                        MultiValueElementType = typeof(string),
                    },
                },
                TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        var info = await reader.GetComplexColumnsAsync("Things", TestContext.Current.CancellationToken);
        var mv = Assert.Single(info);
        Assert.Equal("Tags", mv.ColumnName);
        Assert.True(mv.ComplexId > 0);
        Assert.True(mv.FlatTableId > 0);
    }

    [Fact]
    public async Task CreateTableAsync_AttachmentColumn_C3_RejectedOnJet4Mdb()
    {
        var ms = new MemoryStream();
        await using var writer = await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.Jet4Mdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        var ex = await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await writer.CreateTableAsync(
                "Documents",
                new[]
                {
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true },
                },
                TestContext.Current.CancellationToken));

        Assert.Contains(".accdb", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CreateTableAsync_MultiValueColumn_C3_RejectsMissingElementType()
    {
        var ms = new MemoryStream();
        await using var writer = await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.CreateTableAsync(
                "Bad",
                new[]
                {
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Tags", typeof(object)) { IsMultiValue = true },
                },
                TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task CreateTableAsync_AttachmentColumn_C3_HiddenFlatTablePresent()
    {
        // The hidden flat child table must carry MSysObjects.Flags = 0x800A0000 so
        // it is excluded from the user-table listing but reachable via direct lookup.
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
        await using var reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        var userTables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.Contains("Documents", userTables, StringComparer.OrdinalIgnoreCase);
        Assert.DoesNotContain(userTables, t => t.StartsWith("f_", StringComparison.Ordinal));

        // The flat table has the per-kind value columns from the design doc §2.4.1.
        var info = await reader.GetComplexColumnsAsync("Documents", TestContext.Current.CancellationToken);
        var attachment = Assert.Single(info);
        var flatMeta = await reader.GetColumnMetadataAsync(attachment.FlatTableName, TestContext.Current.CancellationToken);
        var names = flatMeta.Select(m => m.Name).ToArray();
        Assert.Contains("FileURL", names);
        Assert.Contains("FileName", names);
        Assert.Contains("FileType", names);
        Assert.Contains("FileFlags", names);
        Assert.Contains("FileTimeStamp", names);
        Assert.Contains("FileData", names);
    }

    [Fact]
    public async Task GetComplexColumns_OnFreshAccdb_ReturnsEmpty()
    {
        // Empty ACCDB has the MSysComplexColumns scaffold, but there are no
        // user-table complex columns yet.
        var ms = new MemoryStream();
        await using (await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        await using var writer = await AccessWriter.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        await writer.CreateTableAsync(
            "Plain",
            new[]
            {
                new ColumnDefinition("Id", typeof(int)),
            },
            TestContext.Current.CancellationToken);

        var info = await reader.GetComplexColumnsAsync("Plain", TestContext.Current.CancellationToken);
        Assert.Empty(info);
    }
}
