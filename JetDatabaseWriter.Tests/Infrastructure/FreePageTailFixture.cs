namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Pages.Paging;
using Xunit;

/// <summary>Creates physical free tails independently of table and catalog allocation order.</summary>
internal static class FreePageTailFixture
{
    /// <summary>Appends dirty, unlinked reservations and releases a verified physical tail.</summary>
    /// <param name="writer">The database writer.</param>
    /// <param name="pageCount">The minimum number of free tail pages.</param>
    /// <param name="requireDirtyPayload">Whether freed pages must retain bytes for a subsequent scrub.</param>
    /// <returns>A task completing after the tail is physically and globally free.</returns>
    internal static async Task AddFreeTailAsync(AccessWriter writer, int pageCount = 8, bool requireDirtyPayload = false)
    {
        ArgumentNullException.ThrowIfNull(writer);
        ArgumentOutOfRangeException.ThrowIfLessThan(pageCount, 2);
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        byte[] dirty = new byte[writer.Database.Format.PageSize];
        dirty.AsSpan().Fill(0xA7);
        dirty[0] = Constants.PageTypes.Freed;
        for (int attempt = 0; attempt < 3; attempt++)
        {
            long firstPage = await writer.Services.PageAllocator.ReserveContiguousPagesAsync(pageCount, cancellationToken, appendOnly: true);
            for (int index = 0; index < pageCount; index++)
            {
                long page = firstPage + index;
                await writer.Pager.WritePageAsync(page, dirty, cancellationToken);
                await writer.Services.PageAllocator.DeallocatePageAsync(page, cancellationToken);
            }

            // Growing the global map can append a live bitmap after this run.
            // Reserve another tail once that bitmap's capacity is available.
            if (writer.Pager.PageCount != firstPage + pageCount)
            {
                continue;
            }

            for (int index = 0; index < pageCount; index++)
            {
                long page = firstPage + index;
                Assert.True(await writer.Services.PageAllocator.IsPageFreeAsync(page, cancellationToken), $"Tail page {page} must be physically free and marked free in the global map.");
                if (requireDirtyPayload)
                {
                    byte[] freed = await writer.Database.Pages.ReadPageCopyAsync(page, cancellationToken);
                    Assert.Equal(0xA7, freed[writer.Database.Format.PageSize - 1]);
                }
            }

            return;
        }

        Assert.Fail("Global-map growth prevented a stable free tail.");
    }
}
