namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
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
        Assert.True(Assert.IsType<IList<IndexMetadata>>(first, exactMatch: false).IsReadOnly);
        Assert.True(Assert.IsType<IList<IndexColumnReference>>(index.Columns, exactMatch: false).IsReadOnly);
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

    [Fact]
    public async Task ObservedDescriptorWrite_RefreshesTheMetadataForTheSameRoot()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream file = await CreateDatabaseAsync(ct);
        await using AccessReader reader = await AccessReader.OpenAsync(file, new AccessReaderOptions { PageCacheSize = 0, UseLockFile = false }, leaveOpen: true, ct);
        IReadOnlyList<IndexMetadata> first = await reader.ListIndexesAsync("T", ct);
        long root = (await reader.Services.Catalog.ResolveTableAsync("T", ct))!.Entry.TDefPage;
        int offset = checked((int)(root * reader.PageSize));
        byte[] before = file.ToArray().AsSpan(offset, reader.PageSize).ToArray();
        byte[] after = before.AsSpan().ToArray();
        int nameOffset = after.AsSpan().IndexOf(Encoding.Unicode.GetBytes("IX_Id"));
        Assert.True(nameOffset >= 0);
        Encoding.Unicode.GetBytes("IX_No").CopyTo(after, nameOffset);
        file.Position = offset;
        await file.WriteAsync(after, ct);
        reader.Database.TableDefs.OnPageWritten(root, before, after);

        IReadOnlyList<IndexMetadata> refreshed = await reader.ListIndexesAsync("T", ct);

        Assert.Equal("IX_Id", Assert.Single(first).Name);
        Assert.Equal("IX_No", Assert.Single(refreshed).Name);
    }

    [Fact]
    public async Task ConcurrentWarmMetadata_ReturnsTheVerifiedReadOnlySnapshot()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream file = await CreateDatabaseAsync(ct);
        await using var trace = new CountingStream(file);
        await using AccessReader reader = await AccessReader.OpenAsync(trace, new AccessReaderOptions { PageCacheSize = 0, UseLockFile = false }, leaveOpen: true, ct);
        reader.Database.TableDefs.VerifyOnHit = false;
        IReadOnlyList<IndexMetadata> first = await reader.ListIndexesAsync("T", ct);
        trace.Reset();

        await Task.WhenAll(Enumerable.Range(0, 16).Select(async _ => Assert.Same(first, await reader.ListIndexesAsync("T", ct))));

        Assert.Equal(0, trace.BytesRead);
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
