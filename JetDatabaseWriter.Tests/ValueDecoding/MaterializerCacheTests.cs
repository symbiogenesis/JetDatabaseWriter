namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Mapping;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

#pragma warning disable CA1812 // RowMapper constructs these POCOs through compiled expressions.

/// <summary>Materializers reuse bounded shape identities across readers and schema edits.</summary>
public sealed class MaterializerCacheTests
{
    /// <summary>Verifies materializer reuse, identity or bounded cache behavior.</summary>
    [Fact]
    public void EquivalentDefinitions_ReuseMaterializers()
    {
        TableDef first = Definition("Id", 0);
        TableDef second = Definition("Id", 0);
        Assert.Equal(first.Shape, second.Shape);
        Assert.Same(RowMapper<Entity>.Build(["Id"], [typeof(int)]), RowMapper<Entity>.Build(["Id"], [typeof(int)]));
        Assert.Same(DirectRowDecoderBuilder.TryBuild<Entity>(["Id"], first.Columns, first.ClrTypes), DirectRowDecoderBuilder.TryBuild<Entity>(["Id"], second.Columns, second.ClrTypes));
    }

    /// <summary>Verifies materializer reuse, identity or bounded cache behavior.</summary>
    [Fact]
    public void PhysicalLayoutAndRenames_ChangeShape()
    {
        Assert.NotEqual(Definition("Id", 0).Shape, Definition("Id", 4).Shape);
        Assert.NotEqual(Definition("Id", 0).Shape, Definition("Renamed", 0).Shape);
        TableDef added = Definition("Id", 0);
        RowShape before = added.Shape;
        added = new TableDef { Columns = [.. added.Columns, new ColumnInfo { Name = "Extra", Type = ColumnType.LongIntegerType, ColNum = 1 }] };
        Assert.NotEqual(before, added.Shape);
    }

    /// <summary>Verifies materializer reuse, identity or bounded cache behavior.</summary>
    [Fact]
    public async Task ConcurrentFirstUse_PublishesOneDelegate()
    {
        Func<object?[], Entity>[] delegates = await Task.WhenAll(Enumerable.Range(0, 16).Select(_ => Task.Run(() => RowMapper<Entity>.Build(["ConcurrentId"], [typeof(int)]), TestContext.Current.CancellationToken)));
        Assert.All(delegates, item => Assert.Same(delegates[0], item));
    }

    /// <summary>Verifies materializer reuse, identity or bounded cache behavior.</summary>
    [Fact]
    public void Cache_EvictsOldestShape()
    {
        var cache = new MaterializerCache<object>();
        RowShape original = Definition("First", 0).Shape;
        object first = cache.Get(original, static () => new object());
        for (int i = 0; i < 64; i++)
        {
            _ = cache.Get(Definition("Column" + i, 0).Shape, static () => new object());
        }

        Assert.NotSame(first, cache.Get(original, static () => new object()));
    }

    /// <summary>Verifies materializer reuse, identity or bounded cache behavior.</summary>
    [Fact]
    public async Task ReopenedTable_AfterSchemaEditsUsesTheNewShape()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("Entities", [new ColumnDefinition("Id", typeof(int))], ct);
            await writer.InsertRowAsync("Entities", [7], ct);
        }

        for (int i = 0; i < 2; i++)
        {
            stream.Position = 0;
            await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
            await foreach (Entity entity in reader.Rows<Entity>("Entities", cancellationToken: ct))
            {
                Assert.Equal(7, entity.Id);
            }
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.RenameColumnAsync("Entities", "Id", "Other", ct);
            await writer.AddColumnAsync("Entities", new ColumnDefinition("Id", typeof(int)), ct);
            await writer.InsertRowAsync("Entities", [8, 9], ct);
        }

        stream.Position = 0;
        await using AccessReader changed = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        var ids = new List<int>();
        await foreach (Entity entity in changed.Rows<Entity>("Entities", cancellationToken: ct))
        {
            ids.Add(entity.Id);
        }

        Assert.Equal([0, 9], ids);
    }

    private static TableDef Definition(string name, int offset)
    {
        var definition = new TableDef { Columns = [new ColumnInfo { Name = name, Type = ColumnType.LongIntegerType, FixedOff = offset, Flags = 1 }] };
        return definition;
    }

    private sealed class Entity
    {
        public int Id { get; set; }
    }
}
