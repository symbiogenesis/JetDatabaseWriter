namespace JetDatabaseWriter.Tests.Schema;

using System;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Checks structural cache invalidation independently of the page reader.</summary>
public sealed class TDefImageCacheTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void CounterWrite_KeepsImageButDescriptorWriteInvalidates(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        var cache = new TDefImageCache(format);
        byte[] before = EmptyRoot(format);
        TDefImage image = TDefCodec.Parse(format, before)!;
        Assert.True(cache.Publish(5, image, [5, 9], cache.Epoch));
        byte[] after = before.AsSpan().ToArray();
        Wu32(after, format.TDefFormat.Header.NumRows, 10);
        Wu32(after, format.TDefFormat.Header.AutoNumber, 12);
        cache.OnPageWritten(5, before, after);
        Assert.True(cache.TryGet(5, out TDefImage? cached));
        Assert.Same(image, cached);
        after[format.TDefFormat.Header.TableType] ^= 1;
        cache.OnPageWritten(5, before, after);
        Assert.False(cache.TryGet(5, out _));
    }

    [Fact]
    public void ContinuationWrite_InvalidatesOnlyItsOwners()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        var cache = new TDefImageCache(format);
        byte[] page = EmptyRoot(format);
        TDefImage image = TDefCodec.Parse(format, page)!;
        Assert.True(cache.Publish(5, image, [5, 9], cache.Epoch));
        Assert.True(cache.Publish(6, image, [6], cache.Epoch));
        cache.OnPageWritten(9, page, page);
        Assert.False(cache.TryGet(5, out _));
        Assert.True(cache.TryGet(6, out _));
    }

    [Fact]
    public void WriteOrRollback_DuringRead_RejectsPublication()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        var cache = new TDefImageCache(format);
        byte[] page = EmptyRoot(format);
        TDefImage image = TDefCodec.Parse(format, page)!;
        long epoch = cache.Epoch;
        cache.OnPageWritten(5, page, page);
        Assert.False(cache.Publish(5, image, [5], epoch));
        epoch = cache.Epoch;
        cache.OnInvalidateAll();
        Assert.False(cache.Publish(5, image, [5], epoch));
        Assert.True(cache.Publish(5, image, [5], cache.Epoch));
        cache.OnInvalidateAll();
        Assert.False(cache.TryGet(5, out _));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void Definition_ProjectsPhysicalIdentityWithoutRecursiveInitialization(DatabaseFormat kind)
    {
        JetFormat format = JetFormat.ForNewDatabase(kind);
        TDefImage image = TDefCodec.Parse(format, EmptyRoot(format))!;
        image.TDefPageNumber = 5;
        Assert.Equal(5L, image.Definition.TDefPageNumber);
        Assert.Same(image.Definition, image.Definition);
        Assert.Equal(5L, TableSchema.CreateDefinition(image, properties: null).TDefPageNumber);
    }
    private static byte[] EmptyRoot(JetFormat format)
    {
        byte[] page = new byte[format.PageSize];
        page[0] = Constants.PageTypes.TableDefinition;
        return page;
    }
}
