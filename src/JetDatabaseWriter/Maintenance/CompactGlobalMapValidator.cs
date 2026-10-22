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

/// <summary>Independently compares a reopened all-pinned global map against original page one and pre-map EOF.</summary>
/// <remarks>
/// No installer result, generated row or generated bitmap supplies expected bytes. The caller keeps the source,
/// reopened separate destination and the independently recorded pre-map EOF immutable. Every page-one byte
/// outside original row zero compares exactly; every row byte and appended bitmap byte has a canonical expected
/// value. All existing pages, including old global storage and new bitmap storage, must be marked used. Future
/// represented pages are free. This validates only that conservative native map and its append allocation;
/// it does not compare other prefix pages, establish incoming ownership, validate the database or permit publication.
/// </remarks>
internal static class CompactGlobalMapValidator
{
    /// <summary>Checks the complete original page-one surround, map row and sequential self-accounted bitmaps.</summary>
    /// <param name="source">The immutable original decrypted page source.</param>
    /// <param name="format">The matching verified native format.</param>
    /// <param name="appendPointPages">The independently recorded exact destination EOF before map installation.</param>
    /// <param name="reopenedDestination">The separately reopened immutable readable seekable decrypted destination.</param>
    /// <param name="budget">Total page, bitmap, read and working-storage limits.</param>
    /// <param name="cancellationToken">Cancels every read and bitmap comparison.</param>
    /// <returns>Exact conservative map comparison without complete database acceptance.</returns>
    /// <exception cref="JetCorruptDataException">The source or private native map, unchanged surround or exact allocation geometry disagrees.</exception>
    internal static async ValueTask<CompactGlobalMapResult> ValidateAsync(IPageSource source, JetFormat format, long appendPointPages, Stream reopenedDestination, CompactGlobalMapBudget budget, CancellationToken cancellationToken)
    {
        var io = new CompactGlobalMapIo(source, format, appendPointPages, reopenedDestination, budget, writable: false, cancellationToken);
        long position = reopenedDestination.Position;
        byte[] original = await io.ReadSourceAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            RowBound row = CompactGlobalMapIo.ReadOriginalRow(original, format);
            io.AdmitLayout(row, writable: false);
            byte[] actual = new byte[format.PageSize];
            await io.ReadPrivateAsync(1, actual, cancellationToken).ConfigureAwait(false);
            if (!actual.AsSpan(0, row.RowStart).SequenceEqual(original.AsSpan(0, row.RowStart))
                || !actual.AsSpan(row.RowStart + row.RowSize).SequenceEqual(original.AsSpan(row.RowStart + row.RowSize, format.PageSize - row.RowStart - row.RowSize)))
            {
                throw CompactGlobalMapIo.Corrupt("A reopened page-one header, other row or opaque byte differs from its pinned original.");
            }

            byte expectedType = io.BitmapPages == 0 ? Constants.UsageMap.InlineMapType : Constants.UsageMap.ReferenceMapType;
            if (actual[row.RowStart] != expectedType)
            {
                throw CompactGlobalMapIo.Corrupt("The reopened global row type disagrees with native all-pinned coverage.");
            }

            if (io.BitmapPages == 0)
            {
                if (BinaryPrimitives.ReadInt32LittleEndian(actual.AsSpan(row.RowStart + 1)) != 0)
                {
                    throw CompactGlobalMapIo.Corrupt("The reopened global INLINE row is not anchored at page zero.");
                }

                for (int offset = Constants.UsageMap.InlineMapHeaderSize; offset < row.RowSize; offset++)
                {
                    long firstPage = (long)(offset - Constants.UsageMap.InlineMapHeaderSize) * 8;
                    if (actual[row.RowStart + offset] != ExpectedFreeByte(firstPage, io.FinalPages))
                    {
                        throw CompactGlobalMapIo.Corrupt("The reopened global INLINE membership differs from exact pinned and future coverage.");
                    }
                }
            }
            else
            {
                int pointers = UsageMap.ReferencePointerCount(row.RowSize);
                for (int index = 0; index < pointers; index++)
                {
                    int pointer = BinaryPrimitives.ReadInt32LittleEndian(actual.AsSpan(row.RowStart + 1 + (index * 4)));
                    long expected = index < io.BitmapPages ? appendPointPages + index : 0;
                    if (pointer != expected)
                    {
                        throw CompactGlobalMapIo.Corrupt("A reopened global bitmap pointer is missing, aliased, external or outside sequential private coverage.");
                    }
                }

                for (int offset = 1 + (pointers * 4); offset < row.RowSize; offset++)
                {
                    if (actual[row.RowStart + offset] != 0)
                    {
                        throw CompactGlobalMapIo.Corrupt("A reopened global REFERENCE row has noncanonical trailing bytes.");
                    }
                }

                int windowSize = UsageMap.PagesPerReferenceMapPage(format.PageSize);
                for (int window = 0; window < io.BitmapPages; window++)
                {
                    await io.ReadPrivateAsync(appendPointPages + window, actual, cancellationToken).ConfigureAwait(false);
                    for (int offset = 0; offset < format.PageSize; offset++)
                    {
                        byte expected = offset switch
                        {
                            0 => Constants.PageTypes.UsageMap,
                            1 => 1,
                            _ when offset < Constants.UsageMap.ReferenceMapBitmapOffset => 0,
                            _ => ExpectedFreeByte(((long)window * windowSize) + ((long)(offset - Constants.UsageMap.ReferenceMapBitmapOffset) * 8), io.FinalPages),
                        };
                        if (actual[offset] != expected)
                        {
                            throw CompactGlobalMapIo.Corrupt("A reopened global bitmap differs in header, padding, pinned membership, self-allocation or future coverage.");
                        }
                    }

                    io.Check(cancellationToken);
                }
            }

            io.Check(cancellationToken);
            return io.Result(row);
        }
        finally
        {
            PageBuffers.Return(original);
            reopenedDestination.Position = position;
        }
    }

    private static byte ExpectedFreeByte(long firstPage, long finalPages)
    {
        if (firstPage >= finalPages)
        {
            return byte.MaxValue;
        }

        long used = finalPages - firstPage;
        return used >= 8 ? (byte)0 : unchecked((byte)(byte.MaxValue << checked((int)used)));
    }
}
