namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;

/// <summary>Installs an all-pinned native global free map in a separate append-only private decrypted destination.</summary>
/// <remarks>
/// The caller exclusively owns distinct source and destination backing storage, including through wrappers.
/// Every original page and every appended allocation remains used. Only the original page-one row zero changes;
/// old map storage remains pinned and any new bitmaps append contiguously. Their own allocation is included in
/// fixed-point coverage. Page-one headers, other rows and padding remain exact. The independent allocation ledger
/// must account for appended bitmaps as UsageMap storage owned by page one. This is neither compaction nor
/// reclamation, source repair, complete database acceptance, native mutation evidence or permission to publish.
/// Partial writes remain private on cancellation, I/O or flush failure; no completion evidence is returned.
/// </remarks>
internal static class CompactGlobalMapInstaller
{
    /// <summary>Preflights unchanged page one, appends self-accounted bitmaps, patches row zero and flushes.</summary>
    /// <param name="source">The immutable original decrypted page source; every original page stays pinned.</param>
    /// <param name="format">The verified original native format.</param>
    /// <param name="appendPointPages">The exact destination EOF before global-map installation.</param>
    /// <param name="privateDestination">The separate exclusively owned readable writable seekable decrypted stream.</param>
    /// <param name="budget">Total page, bitmap, read, write and working-storage limits.</param>
    /// <param name="cancellationToken">Cancels reads, construction, writes and flush.</param>
    /// <returns>Private completion evidence requiring independent reopened comparison.</returns>
    /// <exception cref="JetCorruptDataException">The source or private native map, unchanged surround or exact allocation geometry disagrees.</exception>
    internal static async ValueTask<CompactGlobalMapResult> InstallAsync(IPageSource source, JetFormat format, long appendPointPages, Stream privateDestination, CompactGlobalMapBudget budget, CancellationToken cancellationToken)
    {
        var io = new CompactGlobalMapIo(source, format, appendPointPages, privateDestination, budget, writable: true, cancellationToken);
        byte[] original = await io.ReadSourceAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            RowBound row = CompactGlobalMapIo.ReadOriginalRow(original, format);
            io.AdmitLayout(row, writable: true);
            byte[] page = new byte[format.PageSize];
            await io.ReadPrivateAsync(1, page, cancellationToken).ConfigureAwait(false);
            if (!page.AsSpan().SequenceEqual(original.AsSpan(0, format.PageSize)))
            {
                throw CompactGlobalMapIo.Corrupt("Private page one differs from the complete immutable original before native map installation.");
            }

            page.AsSpan(row.RowStart, row.RowSize).Clear();
            if (io.BitmapPages == 0)
            {
                int capacity = (row.RowSize - Constants.UsageMap.InlineMapHeaderSize) * 8;
                for (long free = io.FinalPages; free < capacity; free++)
                {
                    _ = UsageMap.TrySetInlinePageState(page, row.RowStart, row.RowSize, free, isMarked: true);
                }
            }
            else
            {
                page[row.RowStart] = Constants.UsageMap.ReferenceMapType;
                byte[] bitmap = new byte[format.PageSize];
                int windowSize = UsageMap.PagesPerReferenceMapPage(format.PageSize);
                for (int window = 0; window < io.BitmapPages; window++)
                {
                    io.Check(cancellationToken);
                    bitmap.AsSpan().Clear();
                    bitmap[0] = Constants.PageTypes.UsageMap;
                    bitmap[1] = 1;
                    long first = (long)window * windowSize;
                    long start = Math.Max(first, io.FinalPages);
                    for (long free = start; free < first + windowSize; free++)
                    {
                        _ = UsageMap.TrySetReferencePageState(bitmap, format.PageSize, free, isMarked: true);
                    }

                    BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(row.RowStart + 1 + (window * 4)), checked((int)(appendPointPages + window)));
                    await io.WritePrivateAsync(appendPointPages + window, bitmap, cancellationToken).ConfigureAwait(false);
                }
            }

            // Install the anchor only after all appended storage is complete. Failures still leave a private file.
            await io.WritePrivateAsync(1, page, cancellationToken).ConfigureAwait(false);
            io.Check(cancellationToken);
            await privateDestination.FlushAsync(cancellationToken).ConfigureAwait(false);
            io.Check(cancellationToken);
            privateDestination.Position = privateDestination.Length;
            return io.Result(row);
        }
        finally
        {
            PageBuffers.Return(original);
        }
    }
}
