namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;

/// <summary>Shared native geometry and bounded I/O; installation and reopened byte comparison are separate.</summary>
internal sealed class CompactGlobalMapIo
{
    private readonly IPageSource source;
    private readonly Stream destination;
    private readonly CompactGlobalMapBudget budget;
    private readonly int pageSize;
    private readonly long sourcePages;
    private long destinationLength;

    internal CompactGlobalMapIo(IPageSource source, JetFormat format, long appendPointPages, Stream destination, CompactGlobalMapBudget budget, bool writable, CancellationToken token)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(destination, nameof(destination));
        Guard.NotNull(budget, nameof(budget));
        source.ThrowIfDisposedOrCancelled(token);
        Guard.RequireReadableSeekableStream(destination, nameof(destination));
        Guard.InRange(budget.MaxPages, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxBitmapPages, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxReadBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxWriteBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxWorkingBytes, 1L, long.MaxValue, nameof(budget));
        if (ReferenceEquals(source, destination) || (writable && !destination.CanWrite))
        {
            throw new ArgumentException("Global-map work requires a separate exclusive readable writable private destination.", nameof(destination));
        }

        this.source = source;
        this.destination = destination;
        this.budget = budget;
        this.pageSize = format.PageSize;
        this.sourcePages = source.PageCount;
        this.destinationLength = destination.Length;
        this.AppendPoint = appendPointPages;
        if (source.PageSize != this.pageSize || this.sourcePages is < 3 or > 0x1000000
            || appendPointPages < this.sourcePages || appendPointPages > 0x1000000
            || this.destinationLength % this.pageSize != 0
            || (writable && this.destinationLength / this.pageSize != appendPointPages))
        {
            throw Corrupt("Source, private destination or independently supplied pre-map EOF has invalid native page geometry.");
        }

        if (appendPointPages > budget.MaxPages || (3L * this.pageSize) + 4096 > budget.MaxWorkingBytes)
        {
            throw Limit("Native global-map work exceeds MaxPages or MaxWorkingBytes.");
        }
    }

    internal long AppendPoint { get; }

    internal long FinalPages { get; private set; }

    internal int BitmapPages { get; private set; }

    internal long ReadBytes { get; private set; }

    internal long WrittenBytes { get; private set; }

    internal static JetCorruptDataException Corrupt(string reason)
        => new(JetErrorCode.CorruptUsageMap, reason, new JetErrorInfo { Reason = reason });

    internal static JetLimitationException Limit(string reason)
        => new(JetErrorCode.ValueTooLarge, reason, new JetErrorInfo { Reason = reason });

    internal static RowBound ReadOriginalRow(byte[] original, JetFormat format)
    {
        if (original.Length < format.PageSize || original[0] != Constants.PageTypes.Data || original[1] != 1
            || !UsageMap.TryGetFirstRowBound(original, format.DataPage, format.PageSize, out RowBound row)
            || row.RowSize <= Constants.UsageMap.InlineMapHeaderSize
            || original[row.RowStart] is not (Constants.UsageMap.InlineMapType or Constants.UsageMap.ReferenceMapType)
            || (original[row.RowStart] == Constants.UsageMap.InlineMapType && BinaryPrimitives.ReadInt32LittleEndian(original.AsSpan(row.RowStart + 1)) != 0))
        {
            throw Corrupt("The original page-one native global map row has invalid type, geometry or inline anchor.");
        }

        int count = BinaryPrimitives.ReadUInt16LittleEndian(original.AsSpan(format.DataPage.NumRows));
        int directoryEnd = format.DataPage.RowsStart + (count * 2);
        if (count > (format.PageSize - format.DataPage.RowsStart) / 2)
        {
            throw Corrupt("The complete page-one row directory is truncated.");
        }

        for (int index = 0; index < count; index++)
        {
            int start = BinaryPrimitives.ReadUInt16LittleEndian(original.AsSpan(format.DataPage.RowsStart + (index * 2))) & Constants.DataPage.RowOffsetMask;
            if (start < directoryEnd || start >= format.PageSize)
            {
                throw Corrupt("Another page-one row slot points outside the complete directory and payload bounds.");
            }

            for (int prior = 0; prior < index; prior++)
            {
                int other = BinaryPrimitives.ReadUInt16LittleEndian(original.AsSpan(format.DataPage.RowsStart + (prior * 2))) & Constants.DataPage.RowOffsetMask;
                if (start == other)
                {
                    throw Corrupt("Page-one row slots alias a payload; updating row zero could change another row identity.");
                }
            }
        }

        return row;
    }

    internal void AdmitLayout(RowBound row, bool writable)
    {
        int inlineCapacity = (row.RowSize - Constants.UsageMap.InlineMapHeaderSize) * 8;
        int window = UsageMap.PagesPerReferenceMapPage(this.pageSize);
        long count = this.AppendPoint <= inlineCapacity ? 0 : 1 + ((this.AppendPoint - 1) / window);
        if (count > 0)
        {
            while (true)
            {
                long next = 1 + ((this.AppendPoint + count - 1) / window);
                if (next == count)
                {
                    break;
                }

                count = next;
            }
        }

        this.FinalPages = this.AppendPoint + count;
        if (count > UsageMap.ReferencePointerCount(row.RowSize) || this.FinalPages > 0x1000000)
        {
            throw Limit("The all-pinned destination and its own bitmap storage exceed native global-map capacity.");
        }

        if (count > this.budget.MaxBitmapPages || this.FinalPages > this.budget.MaxPages
            || (writable && (count + 1) > this.budget.MaxWriteBytes / this.pageSize))
        {
            throw Limit("Native global-map layout exceeds MaxBitmapPages, MaxPages or MaxWriteBytes.");
        }

        this.BitmapPages = checked((int)count);
        if (!writable && this.destinationLength != this.FinalPages * this.pageSize)
        {
            throw Corrupt("The reopened native global-map storage does not end exactly at the independently derived EOF.");
        }
    }

    internal void Check(CancellationToken token)
    {
        this.source.ThrowIfDisposedOrCancelled(token);
        if (this.source.PageSize != this.pageSize || this.source.PageCount != this.sourcePages || this.destination.Length != this.destinationLength)
        {
            throw Corrupt("Source or private destination geometry changed during global-map work.");
        }
    }

    internal async ValueTask<byte[]> ReadSourceAsync(CancellationToken token)
    {
        this.Check(token);
        this.AdmitRead(this.pageSize);
        byte[] original = await this.source.ReadPageAsync(1, token).ConfigureAwait(false);
        try
        {
            this.Check(token);
            if (original.Length < this.pageSize)
            {
                throw Corrupt("The immutable source page one is truncated.");
            }

            return original;
        }
        catch
        {
            PageBuffers.Return(original);
            throw;
        }
    }

    internal async ValueTask ReadPrivateAsync(long number, byte[] bytes, CancellationToken token)
    {
        this.Check(token);
        if (number < 1 || number >= this.destinationLength / this.pageSize)
        {
            throw Corrupt("A native global-map read points outside complete private storage.");
        }

        this.destination.Position = number * this.pageSize;
        int offset = 0;
        while (offset < bytes.Length)
        {
            this.Check(token);
            int requested = bytes.Length - offset;
            this.AdmitRead(requested);
            int read = await this.destination.ReadAsync(bytes.AsMemory(offset), token).ConfigureAwait(false);
            if (read <= 0 || read > requested)
            {
                throw Corrupt("A private global-map page is truncated or returns an invalid read count.");
            }

            offset += read;
        }

        this.Check(token);
    }

    internal async ValueTask WritePrivateAsync(long number, byte[] bytes, CancellationToken token)
    {
        this.Check(token);
        if (bytes.Length > this.budget.MaxWriteBytes - this.WrittenBytes)
        {
            throw Limit("Native global-map writes exceed MaxWriteBytes.");
        }

        bool append = number * this.pageSize == this.destinationLength;
        if (number != 1 && !append)
        {
            throw Corrupt("Native global-map bitmap writes must append contiguously.");
        }

        this.destination.Position = number * this.pageSize;
        await this.destination.WriteAsync(bytes.AsMemory(), token).ConfigureAwait(false);
        this.WrittenBytes += bytes.Length;
        if (append)
        {
            this.destinationLength += bytes.Length;
        }

        this.Check(token);
    }

    internal CompactGlobalMapResult Result(RowBound row)
        => new(this.sourcePages, this.AppendPoint, this.FinalPages, this.BitmapPages, row.RowStart, row.RowSize, this.ReadBytes, this.WrittenBytes);

    private void AdmitRead(int requested)
    {
        if (requested > this.budget.MaxReadBytes - this.ReadBytes)
        {
            throw Limit("Native global-map work exceeds cumulative MaxReadBytes.");
        }

        this.ReadBytes += requested;
    }
}
