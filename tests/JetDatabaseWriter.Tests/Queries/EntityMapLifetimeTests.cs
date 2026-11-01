namespace JetDatabaseWriter.Tests.Queries;

using System;
using System.Runtime.CompilerServices;
using System.Runtime.Loader;
using JetDatabaseWriter.Mapping;
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
            return (new WeakReference(context), new WeakReference(type));
        }
        finally
        {
            context.Unload();
        }
    }
}
