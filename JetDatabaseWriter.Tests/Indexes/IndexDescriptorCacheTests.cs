namespace JetDatabaseWriter.Tests.Indexes;

using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Parsed index descriptors are shared only while their immutable table image is current.</summary>
public sealed class IndexDescriptorCacheTests
{
    [Fact]
    public async Task WarmIndexMetadata_IsReusedAndCannotBeMutatedByCallers()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream file = await CreateDatabaseAsync(ct);
        await using var trace = new CountingStream(file);
        await using AccessReader reader = await AccessReader.OpenAsync(trace, new AccessReaderOptions { PageCacheSize = 0, UseLockFile = false }, leaveOpen: true, ct);
        reader.Database.TableDefs.VerifyOnHit = false;
        IReadOnlyList<IndexMetadata> first = await reader.ListIndexesAsync("T", ct);
        IndexMetadata index = Assert.Single(first);
        trace.Reset();

        IReadOnlyList<IndexMetadata> repeated = await reader.ListIndexesAsync("T", ct);

        Assert.Same(first, repeated);
        Assert.Equal(0, trace.BytesRead);
        Assert.True(Assert.IsAssignableFrom<IList<IndexMetadata>>(first).IsReadOnly);
        Assert.True(Assert.IsAssignableFrom<IList<IndexColumnReference>>(index.Columns).IsReadOnly);
    }

    [Fact]
    public async Task InvalidatedTableImage_RebuildsIndexDescriptors()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream file = await CreateDatabaseAsync(ct);
        await using AccessReader reader = await AccessReader.OpenAsync(file, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        IReadOnlyList<IndexMetadata> first = await reader.ListIndexesAsync("T", ct);
        reader.Database.TableDefs.OnInvalidateAll();

        IReadOnlyList<IndexMetadata> refreshed = await reader.ListIndexesAsync("T", ct);

        Assert.NotSame(first, refreshed);
        Assert.Equal(Assert.Single(first).Name, Assert.Single(refreshed).Name);
    }

    private static async ValueTask<MemoryStream> CreateDatabaseAsync(CancellationToken cancellationToken)
    {
        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken))
        {
            await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int))], [new IndexDefinition("IX_Id", "Id")], cancellationToken);
            await writer.InsertRowAsync("T", [1], cancellationToken);
        }

        stream.Position = 0;
        return stream;
    }
}
