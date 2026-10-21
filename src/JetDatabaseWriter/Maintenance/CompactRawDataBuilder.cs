namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages.Models;

/// <summary>Packs borrowed opaque rows and records original-header to destination bookmarks without table snapshots.</summary>
/// <remarks>
/// The caller exclusively owns a separate, decrypted private destination and empty disk-capable ledger.
/// Neither stream may alias the immutable source, including through wrappers or backing files.
/// Stored field bytes, including LVAL references and calculated envelopes, remain unchanged. Their references
/// still require relocation or pinned source storage; these pages alone are not a usable compact database.
/// Ledger records contain two little-endian UInt32 native bookmarks, source then destination, in source order.
/// Only successful completion and both flushes establish a complete ledger. Failures leave private partial bytes.
/// No source page, map, index or TDEF is modified, reclaimed or published.
/// </remarks>
internal static class CompactRawDataBuilder
{
    /// <summary>Appends dense direct-row pages while recording the original identities of moved rows.</summary>
    /// <param name="session">The bounded immutable source discovery session.</param>
    /// <param name="format">The source and destination format, which must match the session.</param>
    /// <param name="sourceOwner">The original table definition identity.</param>
    /// <param name="destinationOwner">The already reserved private destination table definition.</param>
    /// <param name="privateDestination">The writable seekable page-aligned private destination.</param>
    /// <param name="relocationLedger">A distinct empty writable seekable scratch stream, preferably disk-backed.</param>
    /// <param name="budget">The staging limits; source traversal also consumes the session's persistent allowances.</param>
    /// <param name="cancellationToken">Cancels discovery, copies, ledger writes and flushes.</param>
    /// <returns>The private allocation, requiring reference relocation and independent database validation.</returns>
    /// <exception cref="ArgumentException">The format, streams or private allocation are invalid.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A budget or destination owner is outside its allowed range.</exception>
    /// <exception cref="JetLimitationException">A staging or shared discovery allowance is exhausted.</exception>
    /// <exception cref="JetCorruptDataException">A raw row cannot be faithfully packed or bookmarked.</exception>
    internal static async ValueTask<CompactRawDataBuilderResult> BuildAsync(
        CompactDiscoverySession session,
        JetFormat format,
        long sourceOwner,
        long destinationOwner,
        Stream privateDestination,
        Stream relocationLedger,
        CompactRawDataBuilderBudget budget,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(session, nameof(session));
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(privateDestination, nameof(privateDestination));
        Guard.NotNull(relocationLedger, nameof(relocationLedger));
        Guard.NotNull(budget, nameof(budget));
        cancellationToken.ThrowIfCancellationRequested();
        Guard.InRange(budget.MaxRows, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxPages, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxOutputBytes, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxRelocationBytes, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxWorkingBytes, 1L, long.MaxValue, nameof(budget));
        if (format != session.SourceFormat)
        {
            throw new ArgumentException("The staging format must match the discovery session.", nameof(format));
        }

        if (ReferenceEquals(privateDestination, relocationLedger)
            || (privateDestination is FileStream destinationFile && relocationLedger is FileStream ledgerFile
                && string.Equals(destinationFile.Name, ledgerFile.Name, StringComparison.OrdinalIgnoreCase)))
        {
            throw new ArgumentException("The private destination and relocation ledger must be distinct.", nameof(relocationLedger));
        }

        if (!privateDestination.CanWrite || !privateDestination.CanSeek || privateDestination.Length % format.PageSize != 0)
        {
            throw new ArgumentException("The private destination must be writable, seekable and page aligned.", nameof(privateDestination));
        }

        if (!relocationLedger.CanWrite || !relocationLedger.CanSeek || relocationLedger.Length != 0)
        {
            throw new ArgumentException("The relocation ledger must be empty, writable and seekable.", nameof(relocationLedger));
        }

        long firstPage = privateDestination.Length / format.PageSize;
        if (firstPage is < 3 or > 0xFFFFFF || destinationOwner <= 0 || destinationOwner >= firstPage)
        {
            throw new ArgumentOutOfRangeException(nameof(destinationOwner), "The owner must precede the private allocation within the native bookmark range.");
        }

        long reservation = format.PageSize + 65_536L;
        if (reservation > budget.MaxWorkingBytes)
        {
            throw Limit("The raw data builder storage reservation exceeds MaxWorkingBytes.");
        }

        using IDisposable admission = session.Accounting.ReserveWorkingBytes(reservation, sourceOwner);
        return await new Builder(format, destinationOwner, firstPage, privateDestination, relocationLedger, budget)
            .RunAsync(session, sourceOwner, cancellationToken).ConfigureAwait(false);
    }

    private static JetLimitationException Limit(string reason)
        => new(JetErrorCode.ValueTooLarge, reason, new JetErrorInfo { Reason = reason });

    private sealed class Builder(JetFormat format, long owner, long firstPage, Stream destination, Stream ledger, CompactRawDataBuilderBudget budget)
    {
        private readonly byte[] page = new byte[format.PageSize];
        private readonly byte[] record = new byte[8];
        private int rowStart = format.PageSize;
        private int rowCount;
        private long pageCount;
        private long rows;
        private uint priorSource;

        internal async ValueTask<CompactRawDataBuilderResult> RunAsync(CompactDiscoverySession session, long sourceOwner, CancellationToken token)
        {
            destination.Position = destination.Length;
            ledger.Position = 0;
            await session.ScanRowsAsync(sourceOwner, this.AddAsync, token).ConfigureAwait(false);
            if (this.rowCount != 0)
            {
                await this.WritePageAsync(token).ConfigureAwait(false);
            }

            await destination.FlushAsync(token).ConfigureAwait(false);
            await ledger.FlushAsync(token).ConfigureAwait(false);
            token.ThrowIfCancellationRequested();
            return new(firstPage, this.pageCount, this.rows, this.rows * 8);
        }

        private async ValueTask AddAsync(RowLocation location, ReadOnlyMemory<byte> bytes, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            if (location.PageNumber is <= 0 or > 0xFFFFFF || location.RowIndex is < 0 or > byte.MaxValue
                || bytes.Length == 0 || bytes.Length > format.DataPage.MaxRowLength(format.PageSize))
            {
                const string reason = "A raw row cannot be represented by a direct native row and bookmark.";
                throw new JetCorruptDataException(JetErrorCode.MalformedValue, reason, new JetErrorInfo { PageNumber = location.PageNumber, Reason = reason });
            }

            uint sourceBookmark = checked(((uint)location.PageNumber << 8) | (uint)location.RowIndex);
            if (sourceBookmark <= this.priorSource)
            {
                throw new JetCorruptDataException(JetErrorCode.MalformedValue, "Source row identities are not strictly increasing.");
            }

            if (this.rows >= budget.MaxRows || this.rows >= budget.MaxRelocationBytes / 8)
            {
                throw Limit("Raw row staging exceeds MaxRows or MaxRelocationBytes.");
            }

            if (this.rowCount >= Constants.DataPage.MaxRowsPerPage || this.rowStart - bytes.Length < format.DataPage.RowsStart + ((this.rowCount + 1) * 2))
            {
                await this.WritePageAsync(token).ConfigureAwait(false);
            }

            if (this.rowCount == 0)
            {
                if (this.pageCount >= budget.MaxPages || this.pageCount >= budget.MaxOutputBytes / format.PageSize
                    || firstPage + this.pageCount > 0xFFFFFF)
                {
                    throw Limit("Raw row staging exceeds MaxPages, MaxOutputBytes or the native bookmark range.");
                }

                this.page[0] = Constants.PageTypes.Data;
                this.page[1] = 1;
                BinaryPrimitives.WriteInt32LittleEndian(this.page.AsSpan(format.DataPage.TDefOff), checked((int)owner));
            }

            this.rowStart -= bytes.Length;
            bytes.Span.CopyTo(this.page.AsSpan(this.rowStart));
            BinaryPrimitives.WriteUInt16LittleEndian(this.page.AsSpan(format.DataPage.RowsStart + (this.rowCount * 2)), checked((ushort)this.rowStart));
            uint destinationBookmark = checked(((uint)(firstPage + this.pageCount) << 8) | (uint)this.rowCount);
            this.rowCount++;
            BinaryPrimitives.WriteUInt16LittleEndian(this.page.AsSpan(format.DataPage.NumRows), checked((ushort)this.rowCount));
            BinaryPrimitives.WriteUInt16LittleEndian(this.page.AsSpan(2), checked((ushort)(this.rowStart - format.DataPage.RowsStart - (this.rowCount * 2))));
            BinaryPrimitives.WriteUInt32LittleEndian(this.record, sourceBookmark);
            BinaryPrimitives.WriteUInt32LittleEndian(this.record.AsSpan(4), destinationBookmark);
            await ledger.WriteAsync(this.record.AsMemory(), token).ConfigureAwait(false);
            this.priorSource = sourceBookmark;
            this.rows++;
        }

        private async ValueTask WritePageAsync(CancellationToken token)
        {
            await destination.WriteAsync(this.page.AsMemory(), token).ConfigureAwait(false);
            this.pageCount++;
            this.page.AsSpan().Clear();
            this.rowStart = format.PageSize;
            this.rowCount = 0;
        }
    }
}
