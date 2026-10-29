namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages.Models;

/// <summary>Independently compares a reopened private raw-row allocation and its complete ledger against the immutable source.</summary>
/// <remarks>
/// This validates exact opaque row bytes, original header identities, dense direct-row geometry and allocation coverage only.
/// LVAL pointers, indexes, maps, metadata, incoming ownership and the complete database still require separate validation.
/// Both caller-owned streams must remain immutable, distinct from each other and from the source, including through wrappers.
/// Fixed scratch storage is admitted alongside the source scan; runtime, pooled storage and caller streams are separate.
/// </remarks>
internal static class CompactRawDataValidator
{
    /// <summary>Validates every source row, destination slot and ledger record with fixed storage and bounded read admission.</summary>
    /// <param name="session">The immutable source and its cumulative traversal allowances.</param>
    /// <param name="sourceOwner">The original table identity.</param>
    /// <param name="destinationOwner">The expected private destination table identity.</param>
    /// <param name="destination">The reopened readable, seekable decrypted destination.</param>
    /// <param name="ledger">The reopened readable, seekable complete row relocation ledger.</param>
    /// <param name="allocation">The private raw data allocation to check.</param>
    /// <param name="maxReadBytes">The cumulative destination and ledger read admission, including partial-read retries.</param>
    /// <param name="cancellationToken">Cancels source scans, comparison and each destination read.</param>
    /// <returns>A task completed only after all declared raw rows and allocation pages match.</returns>
    /// <exception cref="ArgumentException">The streams cannot be read independently or their geometry is invalid.</exception>
    /// <exception cref="JetCorruptDataException">Allocation geometry, stored bytes or the ledger disagree.</exception>
    /// <exception cref="JetLimitationException">A source or destination work/storage allowance is exhausted.</exception>
    internal static async ValueTask ValidateAsync(CompactDiscoverySession session, long sourceOwner, long destinationOwner, Stream destination, Stream ledger, CompactRawDataBuilderResult allocation, long maxReadBytes, CancellationToken cancellationToken)
    {
        Guard.NotNull(session, nameof(session));
        Guard.NotNull(destination, nameof(destination));
        Guard.NotNull(ledger, nameof(ledger));
        Guard.NotNull(allocation, nameof(allocation));
        Guard.InRange(maxReadBytes, 1L, long.MaxValue, nameof(maxReadBytes));
        cancellationToken.ThrowIfCancellationRequested();
        if (!destination.CanRead || !destination.CanSeek || !ledger.CanRead || !ledger.CanSeek
            || ReferenceEquals(destination, ledger)
            || (destination is FileStream destinationFile && ledger is FileStream ledgerFile
                && string.Equals(destinationFile.Name, ledgerFile.Name, StringComparison.OrdinalIgnoreCase)))
        {
            throw new ArgumentException("The private destination and ledger must be distinct readable seekable streams.", nameof(destination));
        }

        JetFormat format = session.SourceFormat;
        long length = destination.Length;
        if (length % format.PageSize != 0 || allocation.FirstPageNumber is < 3 or > 0xFFFFFF
            || destinationOwner <= 0 || destinationOwner >= allocation.FirstPageNumber
            || allocation.PageCount < 0 || allocation.PageCount > 0x1000000L - allocation.FirstPageNumber
            || allocation.FirstPageNumber + allocation.PageCount > length / format.PageSize
            || allocation.RowCount < 0 || allocation.RowCount > long.MaxValue / 8
            || allocation.RowCount < allocation.PageCount
            || allocation.RowCount > allocation.PageCount * Constants.DataPage.MaxRowsPerPage
            || allocation.RelocationBytes != allocation.RowCount * 8 || ledger.Length != allocation.RelocationBytes)
        {
            throw Corrupt("The declared private row allocation or complete ledger has invalid geometry.");
        }

        using IDisposable admission = session.Accounting.ReserveWorkingBytes(format.PageSize + 65_536L, sourceOwner);
        await new Validator(format, destinationOwner, destination, ledger, allocation, length, maxReadBytes)
            .RunAsync(session, sourceOwner, cancellationToken).ConfigureAwait(false);
    }

    private static JetCorruptDataException Corrupt(string reason)
        => new(JetErrorCode.MalformedValue, reason, new JetErrorInfo { Reason = reason });

    private sealed class Validator(JetFormat format, long owner, Stream destination, Stream ledger, CompactRawDataBuilderResult allocation, long destinationLength, long maxReadBytes)
    {
        private readonly byte[] page = new byte[format.PageSize];
        private readonly byte[] record = new byte[8];
        private long readBytes;
        private long rows;
        private long pages;
        private int rowCount;
        private int nextRow;

        internal async ValueTask RunAsync(CompactDiscoverySession session, long sourceOwner, CancellationToken token)
        {
            ledger.Position = 0;
            await session.ScanRowsAsync(sourceOwner, this.CompareAsync, token).ConfigureAwait(false);
            if (this.rows != allocation.RowCount || this.pages != allocation.PageCount || this.nextRow != this.rowCount)
            {
                throw Corrupt("The source rows do not cover every declared destination row and allocation page.");
            }

            this.CheckLengths();
            token.ThrowIfCancellationRequested();
        }

        private async ValueTask CompareAsync(RowLocation location, ReadOnlyMemory<byte> bytes, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            this.CheckLengths();
            if (this.rows >= allocation.RowCount || location.PageNumber is <= 0 or > 0xFFFFFF || location.RowIndex is < 0 or > byte.MaxValue)
            {
                throw Corrupt("A source row has no representable entry in the declared private allocation.");
            }

            if (this.nextRow == this.rowCount)
            {
                await this.ReadPageAsync(token).ConfigureAwait(false);
            }

            await this.ReadAsync(ledger, this.record, token).ConfigureAwait(false);
            uint source = checked(((uint)location.PageNumber << 8) | (uint)location.RowIndex);
            uint target = checked(((uint)(allocation.FirstPageNumber + this.pages - 1) << 8) | (uint)this.nextRow);
            if (BinaryPrimitives.ReadUInt32LittleEndian(this.record) != source || BinaryPrimitives.ReadUInt32LittleEndian(this.record.AsSpan(4)) != target)
            {
                throw Corrupt("A relocation record does not match the original header and next dense destination identity.");
            }

            int start = BinaryPrimitives.ReadUInt16LittleEndian(this.page.AsSpan(format.DataPage.RowsStart + (this.nextRow * 2)));
            int end = this.nextRow == 0 ? format.PageSize : BinaryPrimitives.ReadUInt16LittleEndian(this.page.AsSpan(format.DataPage.RowsStart + ((this.nextRow - 1) * 2)));
            if (bytes.Length != end - start || !bytes.Span.SequenceEqual(this.page.AsSpan(start, end - start)))
            {
                throw Corrupt("The private stored row bytes differ from the original raw payload.");
            }

            this.nextRow++;
            this.rows++;
        }

        private async ValueTask ReadPageAsync(CancellationToken token)
        {
            if (this.pages >= allocation.PageCount)
            {
                throw Corrupt("The source rows exceed the private allocation's page count.");
            }

            destination.Position = (allocation.FirstPageNumber + this.pages) * format.PageSize;
            await this.ReadAsync(destination, this.page, token).ConfigureAwait(false);
            this.pages++;
            this.rowCount = BinaryPrimitives.ReadUInt16LittleEndian(this.page.AsSpan(format.DataPage.NumRows));
            if (this.page[0] != Constants.PageTypes.Data || this.page[1] != 1
                || BinaryPrimitives.ReadUInt32LittleEndian(this.page.AsSpan(format.DataPage.TDefOff)) != owner
                || this.rowCount is <= 0 or > Constants.DataPage.MaxRowsPerPage)
            {
                throw Corrupt("A private allocation page has an unexpected owner, type or row count.");
            }

            int directoryEnd = format.DataPage.RowsStart + (this.rowCount * 2);
            int prior = format.PageSize;
            for (int row = 0; row < this.rowCount; row++)
            {
                token.ThrowIfCancellationRequested();
                int start = BinaryPrimitives.ReadUInt16LittleEndian(this.page.AsSpan(format.DataPage.RowsStart + (row * 2)));
                if (start < directoryEnd || start >= prior || prior - start > format.DataPage.MaxRowLength(format.PageSize))
                {
                    throw Corrupt("A private allocation page is not densely packed direct-row storage.");
                }

                prior = start;
            }

            if (BinaryPrimitives.ReadUInt16LittleEndian(this.page.AsSpan(2)) != prior - directoryEnd)
            {
                throw Corrupt("A private allocation page's free-space count disagrees with its directory.");
            }

            this.nextRow = 0;
        }

        private async ValueTask ReadAsync(Stream stream, Memory<byte> buffer, CancellationToken token)
        {
            int offset = 0;
            while (offset < buffer.Length)
            {
                token.ThrowIfCancellationRequested();
                this.CheckLengths();
                int requested = buffer.Length - offset;
                if (requested > maxReadBytes - this.readBytes)
                {
                    const string reason = "The private row validation read allowance is exhausted.";
                    throw new JetLimitationException(JetErrorCode.ValueTooLarge, reason, new JetErrorInfo { Reason = reason });
                }

                this.readBytes += requested;
                int read = await stream.ReadAsync(buffer[offset..], token).ConfigureAwait(false);
                if (read <= 0 || read > requested)
                {
                    throw Corrupt("The private row allocation or ledger ended before a complete read.");
                }

                offset = StreamRead.AdvanceRead(buffer.Length, offset, read);
            }
        }

        private void CheckLengths()
        {
            if (destination.Length != destinationLength || ledger.Length != allocation.RelocationBytes)
            {
                throw Corrupt("The private row allocation or ledger length changed during validation.");
            }
        }
    }
}
