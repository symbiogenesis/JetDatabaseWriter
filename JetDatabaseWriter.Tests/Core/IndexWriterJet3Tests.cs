namespace JetDatabaseWriter.Tests.Core;

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Core.Interfaces;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Round-trip tests for the Jet3 empty-leaf limitation lift: <see cref="IAccessWriter.CreateTableAsync(string, IReadOnlyList{ColumnDefinition}, IReadOnlyList{IndexDefinition}, System.Threading.CancellationToken)"/>
/// emits Jet3 (<c>.mdb</c> Access 97) real-idx (39 B) + logical-idx (20 B)
/// descriptors and a schema-only empty Jet3 leaf page (page size 2048,
/// bitmask at <c>0x16</c>, first entry at <c>0xF8</c>) per
/// <c>docs/design/index-and-relationship-format-notes.md</c> §3.1 / §3.2 / §4.2
/// and <c>format-probe-appendix-jet3-index.md</c>. Live Jet3 leaf maintenance
/// is intentionally not covered — the leaf is empty after CreateTableAsync and
/// stays empty on subsequent inserts (Microsoft Access rebuilds it on the next
/// Compact &amp; Repair pass).
/// </summary>
public sealed class IndexWriterJet3Tests
{
    private static readonly string[] ExpectedIndexNames = ["IX_Name", "IX_Score", "IX_Id"];

    [Fact]
    public async Task Jet3_CreateTable_WithSingleIndex_RoundTripsThroughListIndexes()
    {
        await using var stream = await CreateFreshJet3StreamAsync();
        const string TableName = "Idx_Single";
        const string IndexName = "IX_Idx_Single_Name";

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Name", typeof(string), maxLength: 50),
                ],
                [new IndexDefinition(IndexName, "Name")],
                TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(TableName, TestContext.Current.CancellationToken);

        IndexMetadata idx = Assert.Single(indexes);
        Assert.Equal(IndexName, idx.Name);
        Assert.Equal(IndexKind.Normal, idx.Kind);
        Assert.False(idx.IsUnique);
        Assert.False(idx.IsForeignKey);

        IndexColumnReference col = Assert.Single(idx.Columns);
        Assert.Equal("Name", col.Name);
        Assert.True(col.IsAscending);
    }

    [Fact]
    public async Task Jet3_CreateTable_WithPrimaryKey_RoundTripsAsPkKind()
    {
        await using var stream = await CreateFreshJet3StreamAsync();

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "Pk_Table",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("Name", typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("Pk_Table", TestContext.Current.CancellationToken);

        IndexMetadata pk = Assert.Single(indexes);
        Assert.Equal("PrimaryKey", pk.Name);
        Assert.Equal(IndexKind.PrimaryKey, pk.Kind);
        Assert.Equal("Id", Assert.Single(pk.Columns).Name);
    }

    [Fact]
    public async Task Jet3_CreateTable_WithMultipleIndexes_RoundTripsAll()
    {
        await using var stream = await CreateFreshJet3StreamAsync();

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "Multi",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Name", typeof(string), maxLength: 50),
                    new ColumnDefinition("Score", typeof(int)),
                ],
                [
                    new IndexDefinition("IX_Name", "Name"),
                    new IndexDefinition("IX_Score", "Score"),
                    new IndexDefinition("IX_Id", "Id"),
                ],
                TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("Multi", TestContext.Current.CancellationToken);

        Assert.Equal(3, indexes.Count);
        Assert.Equal(ExpectedIndexNames, indexes.Select(i => i.Name).ToArray());
        Assert.All(indexes, i => Assert.Equal(IndexKind.Normal, i.Kind));
    }

    [Fact]
    public async Task Jet3_CreateTable_WithIndex_EmitsLeafPageWithJet3Layout()
    {
        // Confirms one page-type 0x04 leaf is emitted per index and that the
        // page size matches Jet3 (2048). Bitmask offset (§4.2) is implicit in
        // BuildJet3EmptyLeafPage: free_space == pageSize - 0xF8.
        await using var stream = await CreateFreshJet3StreamAsync();

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "Leaf_Probe",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Name", typeof(string), maxLength: 50),
                ],
                [new IndexDefinition("IX_Name", "Name")],
                TestContext.Current.CancellationToken);
        }

        byte[] bytes = stream.ToArray();
        int leafCount = 0;
        int leafFreeSpace = -1;
        for (int p = 0; p < bytes.Length / Constants.PageSizes.Jet3; p++)
        {
            int o = p * Constants.PageSizes.Jet3;
            if (bytes[o] == 0x04 && bytes[o + 1] == 0x01)
            {
                leafCount++;
                leafFreeSpace = bytes[o + 2] | (bytes[o + 3] << 8);
            }
        }

        Assert.Equal(1, leafCount);
        Assert.Equal(Constants.PageSizes.Jet3 - 0xF8, leafFreeSpace);
    }

    [Fact]
    public async Task Jet3_CreateTable_NoIndexes_StillSucceeds()
    {
        await using var stream = await CreateFreshJet3StreamAsync();

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "Bare",
                [new ColumnDefinition("Id", typeof(int))],
                TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("Bare", TestContext.Current.CancellationToken);
        Assert.Empty(indexes);
    }

    private static async ValueTask<MemoryStream> CreateFreshJet3StreamAsync()
    {
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.Jet3Mdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
            // Empty database; tests reopen and add tables.
        }

        ms.Position = 0;
        return ms;
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(
            stream,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
    }
}
