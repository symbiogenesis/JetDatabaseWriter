namespace JetDatabaseWriter.Tests.Indexes.Collation;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Linq;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.Tests.Relationships;
using Xunit;

/// <summary>Existing General and General 97 text primary keys remain unique and seekable after writes.</summary>
/// <param name="cache">Caches fixture bytes.</param>
public sealed class TextCollationIndexMaintenanceTests(DatabaseCache cache) : IClassFixture<DatabaseCache>
{
    public static TheoryData<string, WriteMode> Cases => new()
    {
        { TestDatabases.TestV1997, WriteMode.Direct },
        { TestDatabases.TestV1997, WriteMode.AutoCommit },
        { TestDatabases.TestV1997, WriteMode.ExplicitCommit },
        { TestDatabases.TestV2010, WriteMode.Direct },
        { TestDatabases.TestV2010, WriteMode.AutoCommit },
        { TestDatabases.TestV2010, WriteMode.ExplicitCommit },
    };

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task FixtureTextPrimaryKey_RejectsDuplicateAndSeeksStoredKey(string fixture, WriteMode mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await cache.CopyToStreamAsync(fixture, ct);
        var keys = new List<(string Table, IndexMetadata Index, string Column, object[] Row)>();
        stream.Position = 0;
        await using (AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            foreach (string table in await reader.ListTablesAsync(ct))
            {
                IReadOnlyList<ColumnMetadata> columns = await reader.GetColumnMetadataAsync(table, ct);
                foreach (IndexMetadata index in await reader.ListIndexesAsync(table, ct))
                {
                    if (index.Kind != IndexKind.PrimaryKey || index.Columns.Count != 1)
                    {
                        continue;
                    }

                    string column = index.Columns[0].Name;
                    if (!columns.Any(value => value.Name == column && value.ClrType == typeof(string)))
                    {
                        continue;
                    }

                    using DataTable rows = await reader.ReadDataTableAsync(table, cancellationToken: ct);
                    foreach (DataRow row in rows.Rows)
                    {
                        if (row[column] is string)
                        {
                            keys.Add((table, index, column, row.ItemArray.Cast<object>().ToArray()));
                        }
                    }
                }
            }
        }

        Assert.NotEmpty(keys);
        foreach ((string table, IndexMetadata index, string column, object[] row) in keys)
        {
            byte[] before = stream.ToArray();
            stream.Position = 0;
            await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, ct))
            {
                await WriteModes.RunAsync(writer, mode, async () =>
                {
                    InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.InsertRowAsync(table, row, ct));
                    Assert.Contains("unique", error.Message, StringComparison.OrdinalIgnoreCase);
                }, ct);
            }

            Assert.Equal(before, stream.ToArray());
            stream.Position = 0;
            await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
            IReadOnlyList<ColumnMetadata> columns = await reader.GetColumnMetadataAsync(table, ct);
            int ordinal = columns.ToList().FindIndex(value => value.Name == column);
            var found = new List<object[]>();
            await foreach (object[] result in reader.SeekRowsAsync(table, index.Name, [row[ordinal]], ct))
            {
                found.Add(result);
            }

            Assert.Equal(row, Assert.Single(found));
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task InsertUpdateDelete_TextKeysStaySeekableInHeaderCollation(string fixture, WriteMode mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await cache.CopyToStreamAsync(fixture, ct);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, ct))
        {
            await WriteModes.RunAsync(writer, mode, async () =>
            {
                await writer.CreateTableAsync(
                    "CollatedKeys",
                    [new ColumnDefinition("Code", typeof(string), 40) { IsPrimaryKey = true }, new ColumnDefinition("Value", typeof(int))],
                    ct);
                await writer.InsertRowsAsync("CollatedKeys", [["\u0152uvre", 1], ["caf\u00e9", 2], ["stra\u00dfe", 3]], ct);
                Assert.Equal(1, await writer.UpdateRowsAsync("CollatedKeys", "Code", "caf\u00e9", new RowValues { ["Code"] = "r\u00e9sum\u00e9" }, ct));
                Assert.Equal(1, await writer.DeleteRowsAsync("CollatedKeys", "Code", "stra\u00dfe", ct));
            }, ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        IndexMetadata primary = Assert.Single(await reader.ListIndexesAsync("CollatedKeys", ct), value => value.Kind == IndexKind.PrimaryKey);
        foreach (string key in new[] { "\u0152uvre", "r\u00e9sum\u00e9", "caf\u00e9", "stra\u00dfe" })
        {
            var rows = new List<object[]>();
            await foreach (object[] row in reader.SeekRowsAsync("CollatedKeys", primary.Name, [key], ct))
            {
                rows.Add(row);
            }

            if (key is "\u0152uvre" or "r\u00e9sum\u00e9")
            {
                Assert.Equal(key, Assert.Single(rows)[0]);
            }
            else
            {
                Assert.Empty(rows);
            }
        }

        using DataTable scanned = await reader.ReadDataTableAsync("CollatedKeys", cancellationToken: ct);
        Assert.Equal(2, scanned.Rows.Count);
        stream.Position = 0;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: ct);
        JetFormat format = harness.Database.Format;
        var cursor = new IndexCursor(format.IndexPage, harness.ReadPageCopyAsync, format.PageSize);
        foreach (string key in new[] { "\u0152uvre", "r\u00e9sum\u00e9" })
        {
            byte[] expected = format.IsJet3
                ? General97TextIndexEncoder.Encode(key, ascending: true)
                : format.DefaultTextSortOrder.Version == 1
                    ? GeneralTextIndexEncoder.Encode(key, ascending: true)
                    : GeneralLegacyTextIndexEncoder.Encode(key, ascending: true);
            Assert.True(await cursor.ContainsKeyAsync(primary.FirstDp, expected, ct), "The stored key must match the independently selected fixture-family encoder.");
        }
    }

    [Fact]
    public async Task UnencodableGeneralSeekBound_QueryAndIncludeFallBackToScan()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string longKey = new('\u00e9', 255);
        await using MemoryStream stream = await cache.CopyToStreamAsync(TestDatabases.TestV2010, ct);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("UnencodableParent", [new ColumnDefinition("Code", typeof(string), 255)], ct);
            await writer.CreateTableAsync(
                "UnencodableChild",
                [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("ParentCode", typeof(string), 255)],
                [new IndexDefinition("IX_ParentCode", "ParentCode")],
                ct);
            await writer.InsertRowAsync("UnencodableParent", [longKey], ct);
            var rows = new List<object[]>();
            for (int id = 1; id <= 100; id++)
            {
                rows.Add([id, FormattableString.Invariant($"short{id:D3}")]);
            }

            await writer.InsertRowsAsync("UnencodableChild", rows, ct);
        }

        // Catalog-only metadata lets Include test an unindexed parent key
        // without trying to create an index whose General key cannot fit.
        await ForeignKeyTestDatabase.PlantRelationshipAsync(stream, "UnencodableRel", "UnencodableChild", "ParentCode", "UnencodableParent", "Code");
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        List<UnencodableParent> parents = await reader.Query<UnencodableParent>("UnencodableParent").Include(row => row.Children).ToListAsync(ct);
        Assert.Equal(longKey, Assert.Single(parents).Code);
        Assert.Empty(parents[0].Children);
        List<UnencodableChild> children = await reader.Query<UnencodableChild>("UnencodableChild").Where(row => row.ParentCode == longKey).ToListAsync(ct);
        Assert.Empty(children);
    }

#pragma warning disable CA1812 // The query materializer constructs these test entities.
    internal sealed class UnencodableParent
    {
        public string Code { get; set; } = string.Empty;

        public List<UnencodableChild> Children { get; set; } = [];
    }

    internal sealed class UnencodableChild
    {
        public int Id { get; set; }

        public string ParentCode { get; set; } = string.Empty;
    }
#pragma warning restore CA1812
}
