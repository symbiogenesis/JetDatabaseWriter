namespace JetDatabaseWriter.LongValues;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Retains prepared map pointers only within one release while ownership references remain unchanged.</summary>
internal sealed class LongValueReleaseScope : IDisposable, IPageWriteObserver
{
    private readonly JetFormat format;
    private readonly Pager pager;
    private readonly Func<CancellationToken, ValueTask<List<(UsageMapPointer Owned, UsageMapPointer Free)>>> prepare;
    private IReadOnlyList<(UsageMapPointer Owned, UsageMapPointer Free)>? prepared;
    private long generation;
    private bool disposed;

    /// <summary>Initializes a new instance of the <see cref="LongValueReleaseScope"/> class and registers its temporary topology observer.</summary>
    /// <param name="format">The native format.</param>
    /// <param name="pager">The release's page writer.</param>
    /// <param name="prepare">Prepares independent column map pointers from the current graph.</param>
    internal LongValueReleaseScope(JetFormat format, Pager pager, Func<CancellationToken, ValueTask<List<(UsageMapPointer Owned, UsageMapPointer Free)>>> prepare)
    {
        this.format = format;
        this.pager = pager;
        this.prepare = prepare;
        pager.AddWriteObserver(this);
    }

    /// <summary>Returns pointers prepared since the last ownership topology change.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The column map pointers, without retaining membership or ownership proofs.</returns>
    /// <exception cref="ObjectDisposedException">The release has ended.</exception>
    internal async ValueTask<IReadOnlyList<(UsageMapPointer Owned, UsageMapPointer Free)>> GetMapsAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(this.disposed, this);
#else
        if (this.disposed)
        {
            throw new ObjectDisposedException(nameof(LongValueReleaseScope));
        }
#endif

        if (this.prepared is not null)
        {
            return this.prepared;
        }

        long before = this.generation;
        List<(UsageMapPointer Owned, UsageMapPointer Free)> result = await this.prepare(cancellationToken).ConfigureAwait(false);
        if (before == this.generation)
        {
            this.prepared = result;
        }

        // Preparation may isolate shared maps. Its result is immediately
        // usable, but only a later quiet preparation can be retained.
        return result;
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (!this.disposed)
        {
            this.disposed = true;
            this.prepared = null;
            this.pager.RemoveWriteObserver(this);
        }
    }

    /// <inheritdoc/>
    public void OnPageWritten(long pageNumber, ReadOnlySpan<byte> before, ReadOnlySpan<byte> after)
    {
        if (before.SequenceEqual(after))
        {
            return;
        }

        if (IsType(before, Constants.PageTypes.TableDefinition) || IsType(after, Constants.PageTypes.TableDefinition)
            || this.IsOwnedData(before, 2) || this.IsOwnedData(after, 2))
        {
            this.OnInvalidateAll();
            return;
        }

        if (this.IsOwnedData(before, 0) || this.IsOwnedData(after, 0))
        {
            if (!this.StableMapReferences(before, after))
            {
                this.OnInvalidateAll();
            }

            return;
        }

        if ((IsType(before, Constants.PageTypes.UsageMap) || IsType(after, Constants.PageTypes.UsageMap))
            && (!IsType(before, Constants.PageTypes.UsageMap) || !IsType(after, Constants.PageTypes.UsageMap)
                || before.Length < Constants.UsageMap.ReferenceMapBitmapOffset || after.Length < Constants.UsageMap.ReferenceMapBitmapOffset
                || !before[..Constants.UsageMap.ReferenceMapBitmapOffset].SequenceEqual(after[..Constants.UsageMap.ReferenceMapBitmapOffset])))
        {
            this.OnInvalidateAll();
        }
    }

    /// <inheritdoc/>
    public void OnInvalidateAll()
    {
        this.prepared = null;
        this.generation = unchecked(this.generation + 1);
    }

    private static bool IsType(ReadOnlySpan<byte> page, byte type) => !page.IsEmpty && page[0] == type;

    private bool IsOwnedData(ReadOnlySpan<byte> page, int owner)
        => page.Length >= this.format.DataPage.TDefOff + 4 && IsType(page, Constants.PageTypes.Data) && Ri32(page, this.format.DataPage.TDefOff) == owner;

    private bool StableMapReferences(ReadOnlySpan<byte> before, ReadOnlySpan<byte> after)
    {
        if (before.Length != this.format.PageSize || after.Length != this.format.PageSize
            || !this.IsOwnedData(before, 0) || !this.IsOwnedData(after, 0)
            || !before[..this.format.DataPage.RowsStart].SequenceEqual(after[..this.format.DataPage.RowsStart]))
        {
            return false;
        }

        int rows = Ru16(before, this.format.DataPage.NumRows);
        int slotsLength = rows * 2;
        if (rows == 0 || slotsLength > this.format.PageSize - this.format.DataPage.RowsStart
            || !before.Slice(this.format.DataPage.RowsStart, slotsLength).SequenceEqual(after.Slice(this.format.DataPage.RowsStart, slotsLength)))
        {
            return false;
        }

        for (int index = 0; index < rows; index++)
        {
            if (!UsageMap.TryGetRowBound(before, this.format.DataPage, this.format.PageSize, index, out RowBound row) || row.RowSize < 6)
            {
                return false;
            }

            int referenceBytes;
            if (before[row.RowStart] == Constants.UsageMap.InlineMapType)
            {
                referenceBytes = Constants.UsageMap.InlineMapHeaderSize;
            }
            else if (before[row.RowStart] == Constants.UsageMap.ReferenceMapType)
            {
                referenceBytes = row.RowSize;
            }
            else
            {
                return false;
            }

            if (!before.Slice(row.RowStart, referenceBytes).SequenceEqual(after.Slice(row.RowStart, referenceBytes)))
            {
                return false;
            }
        }

        return true;
    }
}
