namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;

/// <summary>Densely stages complete raw LVAL rows and retargets only their defined chain and descriptor pointers.</summary>
/// <remarks>
/// The caller exclusively owns a separate decrypted private destination which must not alias the immutable source,
/// including through wrappers or backing files. Whole column and row slices, tokens and trailing bytes are preserved;
/// no text, OLE, calculated envelope or payload decoding occurs. The preceding row remains buffered until its successor's
/// destination bookmark is known, so staging needs neither a whole-value collection nor a relocation ledger.
/// Complete defined references are followed through terminal zero, including storage beyond the declared payload.
/// This copies only referenced rows: unreferenced storage, page padding, incoming references and ownership remain
/// unproven and pinned at the source. A result is private staging evidence, not authority to reclaim or publish storage.
/// Only successful traversal and destination flush yield a result; failures leave private partial bytes.
/// </remarks>
internal static class CompactRawLongValueBuilder
{
    /// <summary>Appends private LVAL pages and returns the original raw column with its first reference retargeted.</summary>
    /// <param name="session">The bounded immutable source discovery session.</param>
    /// <param name="format">The source and destination format, which must match the session.</param>
    /// <param name="column">The original descriptor and complete following column bytes, immutable until completion.</param>
    /// <param name="privateDestination">The separate writable seekable page-aligned private destination.</param>
    /// <param name="budget">The staging limits; traversal also consumes the session's persistent allowances.</param>
    /// <param name="cancellationToken">Cancels traversal, writes and the final flush.</param>
    /// <returns>Private allocation evidence and a retargeted raw column requiring independent database validation.</returns>
    /// <exception cref="ArgumentException">The destination or format is invalid.</exception>
    /// <exception cref="JetLimitationException">A staging or shared discovery allowance is exhausted.</exception>
    /// <exception cref="JetCorruptDataException">A complete raw long value cannot be faithfully observed or packed.</exception>
    internal static async ValueTask<CompactRawLongValueBuilderResult> BuildAsync(
        CompactDiscoverySession session,
        JetFormat format,
        ReadOnlyMemory<byte> column,
        Stream privateDestination,
        CompactRawLongValueBuilderBudget budget,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(session, nameof(session));
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(privateDestination, nameof(privateDestination));
        Guard.NotNull(budget, nameof(budget));
        cancellationToken.ThrowIfCancellationRequested();
        Guard.InRange(budget.MaxRows, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxPages, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxOutputBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxRawBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxWorkingBytes, 1L, long.MaxValue, nameof(budget));
        if (format != session.SourceFormat)
        {
            throw new ArgumentException("The staging format must match the discovery session.", nameof(format));
        }

        if (!privateDestination.CanWrite || !privateDestination.CanSeek || privateDestination.Length % format.PageSize != 0)
        {
            throw new ArgumentException("The private destination must be writable, seekable and page aligned.", nameof(privateDestination));
        }

        long firstPage = privateDestination.Length / format.PageSize;
        if (firstPage is < 3 or > 0xFFFFFF)
        {
            throw new ArgumentException("The private allocation must follow reserved pages within the native bookmark range.", nameof(privateDestination));
        }

        if (column.Length > budget.MaxRawBytes)
        {
            throw Limit("The original long-value column exceeds MaxRawBytes.");
        }

        long reservation = format.PageSize + 65_536L + column.Length;
        if (reservation > budget.MaxWorkingBytes)
        {
            throw Limit("The raw long-value builder storage reservation exceeds MaxWorkingBytes.");
        }

        using IDisposable admission = session.Accounting.ReserveWorkingBytes(reservation);
        return await new Builder(format, firstPage, privateDestination, budget, column.ToArray())
            .RunAsync(session, column, cancellationToken).ConfigureAwait(false);
    }

    private static JetLimitationException Limit(string reason)
        => new(JetErrorCode.ValueTooLarge, reason, new JetErrorInfo { Reason = reason });

    private sealed class Builder(JetFormat format, long firstPage, Stream destination, CompactRawLongValueBuilderBudget budget, byte[] column)
    {
        private readonly byte[] page = new byte[format.PageSize];
        private int rowStart = format.PageSize;
        private int rowCount;
        private int previousPrefix = -1;
        private long pageCount;
        private long rows;
        private long rawBytes = column.Length;

        internal async ValueTask<CompactRawLongValueBuilderResult> RunAsync(CompactDiscoverySession session, ReadOnlyMemory<byte> original, CancellationToken token)
        {
            destination.Position = destination.Length;
            await session.AuditLongValueAsync(original, this.ObserveAsync, token).ConfigureAwait(false);
            if (this.rowCount != 0)
            {
                await this.WritePageAsync(token).ConfigureAwait(false);
            }

            await destination.FlushAsync(token).ConfigureAwait(false);
            token.ThrowIfCancellationRequested();
            return new(column, firstPage, this.pageCount, this.rows, this.rawBytes);
        }

        private async ValueTask ObserveAsync(CompactLongValueRecord record, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            if (record.Kind != "lval-row")
            {
                return;
            }

            ReadOnlyMemory<byte> bytes = record.RawBytes;
            int capacity = format.PageSize - format.DataPage.RowsStart - sizeof(ushort);
            if (bytes.Length == 0 || bytes.Length > capacity)
            {
                const string reason = "A complete raw LVAL row cannot be represented in the private destination.";
                throw new JetCorruptDataException(JetErrorCode.MalformedValue, reason, new JetErrorInfo { PageNumber = record.PageNumber, Reason = reason });
            }

            if (this.rows >= budget.MaxRows || bytes.Length > budget.MaxRawBytes - this.rawBytes)
            {
                throw Limit("Raw long-value staging exceeds MaxRows or MaxRawBytes, including trailing storage.");
            }

            bool nextPage = this.rowCount >= Constants.DataPage.MaxRowsPerPage
                || this.rowStart - bytes.Length < format.DataPage.RowsStart + ((this.rowCount + 1) * sizeof(ushort));
            long number = firstPage + this.pageCount + (nextPage ? 1 : 0);
            if (number > 0xFFFFFF || ((this.rowCount == 0 || nextPage)
                && (number - firstPage >= budget.MaxPages || number - firstPage >= budget.MaxOutputBytes / format.PageSize)))
            {
                throw Limit("Raw long-value staging exceeds MaxPages, MaxOutputBytes or the native bookmark range.");
            }

            uint bookmark = checked(((uint)number << 8) | (uint)(nextPage ? 0 : this.rowCount));
            if (this.rows == 0)
            {
                BinaryPrimitives.WriteUInt32LittleEndian(column.AsSpan(4), bookmark);
            }
            else if (this.previousPrefix >= 0)
            {
                BinaryPrimitives.WriteUInt32LittleEndian(this.page.AsSpan(this.previousPrefix), bookmark);
            }

            if (nextPage)
            {
                await this.WritePageAsync(token).ConfigureAwait(false);
            }

            if (this.rowCount == 0)
            {
                this.page[0] = Constants.PageTypes.Data;
                this.page[1] = 1;
                "LVAL"u8.CopyTo(this.page.AsSpan(4));
            }

            this.rowStart -= bytes.Length;
            bytes.Span.CopyTo(this.page.AsSpan(this.rowStart));
            this.previousPrefix = record.NextPointer.HasValue ? this.rowStart : -1;
            BinaryPrimitives.WriteUInt16LittleEndian(this.page.AsSpan(format.DataPage.RowsStart + (this.rowCount * sizeof(ushort))), checked((ushort)this.rowStart));
            this.rowCount++;
            BinaryPrimitives.WriteUInt16LittleEndian(this.page.AsSpan(format.DataPage.NumRows), checked((ushort)this.rowCount));
            BinaryPrimitives.WriteUInt16LittleEndian(this.page.AsSpan(2), checked((ushort)(this.rowStart - format.DataPage.RowsStart - (this.rowCount * sizeof(ushort)))));
            this.rows++;
            this.rawBytes += bytes.Length;
        }

        private async ValueTask WritePageAsync(CancellationToken token)
        {
            await destination.WriteAsync(this.page.AsMemory(), token).ConfigureAwait(false);
            token.ThrowIfCancellationRequested();
            this.pageCount++;
            this.page.AsSpan().Clear();
            this.rowStart = format.PageSize;
            this.rowCount = 0;
            this.previousPrefix = -1;
        }
    }
}
