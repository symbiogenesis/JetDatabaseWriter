namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Verification;

/// <summary>Validates and searches a disk-capable monotone row ledger with fixed storage and cumulative I/O limits.</summary>
/// <remarks>
/// The caller exclusively owns the readable seekable stream and keeps its contents immutable after opening.
/// Increasing destination bookmarks preserve the source bookmark order used to break equal index keys.
/// This proves ledger uniqueness and order only, not row/key correspondence, incoming ownership or database validity.
/// The eight-byte scratch buffer, fixed object state and caller-owned stream are separate from discovery accounting.
/// </remarks>
internal sealed class CompactRowRelocationMap
{
    private readonly Stream ledger;
    private readonly long rowCount;
    private readonly long maxReadBytes;
    private readonly byte[] record = new byte[8];
    private int active;

    private CompactRowRelocationMap(Stream ledger, long rowCount, long maxReadBytes)
    {
        this.ledger = ledger;
        this.rowCount = rowCount;
        this.maxReadBytes = maxReadBytes;
    }

    /// <summary>Gets the bytes admitted for reads, including partial-read retries and failed I/O attempts.</summary>
    internal long ReadBytes { get; private set; }

    /// <summary>Validates every fixed-width record before permitting bounded binary searches.</summary>
    /// <param name="ledger">The caller-owned complete, immutable bookmark ledger.</param>
    /// <param name="rowCount">The successful data builder's expected row count.</param>
    /// <param name="maxReadBytes">The cumulative validation and lookup I/O admission limit.</param>
    /// <param name="cancellationToken">Cancels validation and source I/O.</param>
    /// <returns>A map borrowing the caller's immutable stream without owning its lifetime.</returns>
    /// <exception cref="ArgumentException">The stream is not readable and seekable.</exception>
    /// <exception cref="JetCorruptDataException">The ledger length, bookmarks or ordering are invalid.</exception>
    /// <exception cref="JetLimitationException">Validation exhausts the cumulative read allowance.</exception>
    internal static async ValueTask<CompactRowRelocationMap> OpenAsync(Stream ledger, long rowCount, long maxReadBytes, CancellationToken cancellationToken)
    {
        Guard.NotNull(ledger, nameof(ledger));
        Guard.InRange(rowCount, 0L, long.MaxValue / 8, nameof(rowCount));
        Guard.InRange(maxReadBytes, 1L, long.MaxValue, nameof(maxReadBytes));
        cancellationToken.ThrowIfCancellationRequested();
        if (!ledger.CanRead || !ledger.CanSeek)
        {
            throw new ArgumentException("The relocation ledger must be readable and seekable.", nameof(ledger));
        }

        var map = new CompactRowRelocationMap(ledger, rowCount, maxReadBytes);
        map.CheckLength();
        uint priorSource = 0;
        uint priorDestination = 0;
        for (long index = 0; index < rowCount; index++)
        {
            await map.ReadRecordAsync(index, cancellationToken).ConfigureAwait(false);
            uint source = BinaryPrimitives.ReadUInt32LittleEndian(map.record);
            uint destination = BinaryPrimitives.ReadUInt32LittleEndian(map.record.AsSpan(4));
            if (!IndexMutationKernel.AdmitRelocationPair(source, destination, priorSource, priorDestination))
            {
                throw Corrupt("Relocation bookmarks are invalid, aliased or not strictly increasing.");
            }

            priorSource = source;
            priorDestination = destination;
        }

        map.CheckLength();
        cancellationToken.ThrowIfCancellationRequested();
        return map;
    }

    /// <summary>Retains the exact stored key and replaces its bookmark through the verified ledger.</summary>
    /// <param name="entry">The borrowed stored-key entry naming an original live row header.</param>
    /// <param name="cancellationToken">Cancels lookup and each source read.</param>
    /// <returns>The unchanged key with its destination page and row slot.</returns>
    /// <exception cref="JetCorruptDataException">The bookmark has no mapping or the ledger length changed.</exception>
    /// <exception cref="JetOperationException">Another lookup is already active.</exception>
    /// <exception cref="JetLimitationException">Lookup exhausts the cumulative read allowance.</exception>
    internal async ValueTask<IndexEntry> RelocateAsync(IndexEntry entry, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (Interlocked.CompareExchange(ref this.active, 1, 0) != 0)
        {
            throw new JetOperationException(JetErrorCode.InvalidOperationState, "Relocation lookups must run serially.");
        }

        try
        {
            this.CheckLength();
            if (entry.DataPage is <= 0 or > 0xFFFFFF)
            {
                throw Corrupt("An index bookmark is outside the native page range.");
            }

            uint target = checked(((uint)entry.DataPage << 8) | entry.DataRow);
            long low = 0;
            long high = this.rowCount;
            while (low < high)
            {
                long middle = IndexMutationKernel.RelocationMiddle(low, high);
                await this.ReadRecordAsync(middle, cancellationToken).ConfigureAwait(false);
                uint source = BinaryPrimitives.ReadUInt32LittleEndian(this.record);
                IndexMutationKernel.AdvanceRelocationSearch(low, high, middle, source, target, out low, out high, out bool found);
                if (found)
                {
                    uint destination = BinaryPrimitives.ReadUInt32LittleEndian(this.record.AsSpan(4));
                    this.CheckLength();
                    cancellationToken.ThrowIfCancellationRequested();
                    return entry with { DataPage = destination >> 8, DataRow = checked((byte)(destination & 0xFF)) };
                }
            }

            throw Corrupt("An index bookmark has no staged live-row identity.");
        }
        finally
        {
            Volatile.Write(ref this.active, 0);
        }
    }

    private static JetCorruptDataException Corrupt(string reason)
        => new(JetErrorCode.CorruptIndex, reason, new JetErrorInfo { Reason = reason });

    private void CheckLength()
    {
        if (this.ledger.Length != this.rowCount * 8)
        {
            throw Corrupt("The relocation ledger length disagrees with its complete row count.");
        }
    }

    private async ValueTask ReadRecordAsync(long index, CancellationToken token)
    {
        this.CheckLength();
        this.ledger.Position = index * 8;
        int offset = 0;
        while (offset < this.record.Length)
        {
            token.ThrowIfCancellationRequested();
            int requested = this.record.Length - offset;
            if (requested > this.maxReadBytes - this.ReadBytes)
            {
                const string reason = "The cumulative relocation-ledger read allowance is exhausted.";
                throw new JetLimitationException(JetErrorCode.ValueTooLarge, reason, new JetErrorInfo { Reason = reason });
            }

            this.ReadBytes += requested;
            int read = await this.ledger.ReadAsync(this.record.AsMemory(offset, requested), token).ConfigureAwait(false);
            if (read <= 0 || read > requested)
            {
                throw Corrupt("The relocation ledger ended before a complete record.");
            }

            offset = StreamRead.AdvanceRead(this.record.Length, offset, read);
        }
    }
}
