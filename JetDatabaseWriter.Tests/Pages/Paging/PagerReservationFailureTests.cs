namespace JetDatabaseWriter.Tests.Pages.Paging;

using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Checks allocation cleanup after an initialized page cannot reach storage.</summary>
public sealed class PagerReservationFailureTests
{
    /// <summary>A failed initializer releases the reserved page and the next allocation reuses it.</summary>
    /// <param name="cacheSize">The pager frame capacity.</param>
    [Theory]
    [InlineData(0)]
    [InlineData(256)]
    public async Task FailedInitializer_ReleasesReservedPage_ForNextAllocation(int cacheSize)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        await using var stream = new WriteFaultStream();
        stream.SetLength(format.PageSize * 3L);
#pragma warning disable CA2000 // The awaited pager owns and disposes the codec.
        await using var pager = new Pager(stream, format.PageSize, new NoPageCodec(), true, typeof(AccessWriter), cacheSize);
#pragma warning restore CA2000
        var allocator = new PageAllocator(format, pager, new AccessWriterOptions());
        Assert.Equal(0, await allocator.ScrubFreePagesAsync(TestContext.Current.CancellationToken));
        byte[] initialized = new byte[format.PageSize];
        initialized[0] = Constants.PageTypes.Data;
        initialized[100] = 73;
        stream.FailOnWrite(1);

        await Assert.ThrowsAsync<IOException>(() => allocator.AllocatePageAsync(initialized, TestContext.Current.CancellationToken).AsTask());

        Assert.True(stream.Faulted);
        Assert.Equal(4, pager.PageCount);
        Assert.True(await allocator.IsPageFreeAsync(3, TestContext.Current.CancellationToken));
        byte[] freed = await pager.ReadPageCopyAsync(3, TestContext.Current.CancellationToken);
        Assert.Equal(Constants.PageTypes.Freed, freed[0]);
        Assert.Equal(0, freed[100]);
        Assert.Equal(3, await allocator.AllocatePageAsync(initialized, TestContext.Current.CancellationToken));
        Assert.Equal(4, pager.PageCount);
        Assert.False(await allocator.IsPageFreeAsync(3, TestContext.Current.CancellationToken));
        Assert.Equal(initialized, await pager.ReadPageCopyAsync(3, TestContext.Current.CancellationToken));
    }
}
