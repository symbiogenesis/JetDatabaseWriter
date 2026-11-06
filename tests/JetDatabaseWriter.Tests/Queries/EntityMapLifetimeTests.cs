namespace JetDatabaseWriter.Tests.Queries;

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.Loader;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Mapping;
using JetDatabaseWriter.Queries;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Process-wide mapping retention must not keep collectible plugin types alive.</summary>
public sealed class EntityMapLifetimeTests
{
    [Fact(DisableParallelization = true)]
    public void MappedCollectibleType_DoesNotPreventItsLoadContextFromUnloading()
    {
        (WeakReference context, WeakReference type) = MapCollectibleType();
        for (int attempt = 0; attempt < 10 && (context.IsAlive || type.IsAlive); attempt++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }

        Assert.False(type.IsAlive, "The mapping cache retained the collectible entity type.");
        Assert.False(context.IsAlive, "The mapping cache retained the collectible assembly load context.");
    }

    [Fact(DisableParallelization = true)]
    public void LiveType_RetainsOneMapAcrossCollections()
    {
        Type type = typeof(SnapshotPlaylistRow);
        var map = EntityMap.For(type);
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        Assert.Same(map, EntityMap.For(type));
        Assert.NotNull(map.FindByColumn(nameof(SnapshotPlaylistRow.PlaylistID)));
        Assert.InRange(EntityMap.CachedTypeCount, 1, 256);
    }

    [Fact(DisableParallelization = true)]
    public void MaterializerCache_BoundsShapesPerType_AndTypes()
    {
        Type type = typeof(SnapshotPlaylistRow);
        for (int i = 0; i < 80; i++)
        {
            IReadOnlyList<string> headers = [nameof(SnapshotPlaylistRow.PlaylistID), $"Col_{i}"];
            var shape = new RowShape(headers, [typeof(int), typeof(string)]);
            _ = MaterializerCache.GetOrCreate(type, shape, static _ => new object());
        }

        Assert.InRange(MaterializerCache.CachedTypeCount, 1, 256);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (WeakReference Context, WeakReference Type) MapCollectibleType()
    {
        var context = new AssemblyLoadContext("EntityMapLifetime-" + Guid.NewGuid().ToString("N"), isCollectible: true);
        try
        {
            Type type = context.LoadFromAssemblyPath(typeof(SnapshotPlaylistRow).Assembly.Location)
                .GetType("JetDatabaseWriter.TestSupport.SnapshotPlaylistRow", throwOnError: true)
                ?? throw new InvalidOperationException("The independent entity specimen is missing.");
            var map = EntityMap.For(type);
            Assert.Equal(nameof(SnapshotPlaylistRow), map.TableName);
            Assert.NotNull(map.FindByColumn(nameof(SnapshotPlaylistRow.PlaylistID)));
            Assert.Same(map, EntityMap.For(type));
            _ = RuntimeRowMapper.Build(type, [nameof(SnapshotPlaylistRow.PlaylistID), nameof(SnapshotPlaylistRow.Title)]);
            _ = RuntimeRowMapper.CreateList(type);
            var td = new TableDef
            {
                Columns =
                [
                    new ColumnInfo { Name = nameof(SnapshotPlaylistRow.PlaylistID), Type = ColumnType.LongIntegerType, ColNum = 0, FixedOff = 0, Size = 4 },
                    new ColumnInfo { Name = nameof(SnapshotPlaylistRow.Title), Type = ColumnType.TextType, ColNum = 1, VarIdx = 0, Size = 50 },
                ],
            };
            _ = td.GetShape([true, false]);
            return (new WeakReference(context), new WeakReference(type));
        }
        finally
        {
            context.Unload();
        }
    }
}
