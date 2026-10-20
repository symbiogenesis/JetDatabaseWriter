namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Schema.Models;

/// <summary>Independently compares the complete per-column LVAL ledger with source fields and reopened raw allocations.</summary>
/// <remarks>
/// All three reopened streams and the immutable source have distinct backing storage and remain immutable throughout
/// validation, including through wrappers. Successful completion checks the original field identity of every allocation;
/// self-consistent map contents alone cannot establish that identity. It permits the immutable checked ledger to supply
/// per-column expected runs for separate map validation. This does not validate maps, TDEF installation, indexes,
/// incoming ownership, global allocation or the complete database and never authorizes reclamation or publication.
/// </remarks>
internal static class CompactRawTableAllocationValidator
{
    /// <summary>Compares every LVAL ledger record while independently validating source rows and raw external storage.</summary>
    /// <param name="session">The immutable source and its cumulative work/storage accounting.</param>
    /// <param name="owner">The pinned original table identity.</param>
    /// <param name="definition">The verified original definition.</param>
    /// <param name="destination">The reopened immutable private decrypted destination.</param>
    /// <param name="rowLedger">The reopened immutable complete row relocation ledger.</param>
    /// <param name="allocationLedger">The reopened immutable complete twelve-byte per-column allocation ledger.</param>
    /// <param name="allocation">The complete private table staging evidence.</param>
    /// <param name="budget">The separate raw row/LVAL comparison limits and additional fixed-buffer storage limit.</param>
    /// <param name="maxAllocationReadBytes">The separate cumulative allocation-ledger read admission, including partial retries.</param>
    /// <param name="cancellationToken">Cancels every source scan, comparison and private read.</param>
    /// <param name="hydratedColumns">Optional verified persisted ResultType projections.</param>
    /// <returns>Complete raw comparison and per-column ledger correspondence evidence only.</returns>
    /// <exception cref="ArgumentException">The allocation ledger cannot be read independently.</exception>
    /// <exception cref="JetCorruptDataException">Ledger geometry, column association or allocation ranges disagree.</exception>
    /// <exception cref="JetLimitationException">A comparison or allocation-ledger read limit is exhausted.</exception>
    internal static async ValueTask<CompactRawTableAllocationValidatorResult> ValidateAsync(
        CompactDiscoverySession session,
        long owner,
        TDefImage definition,
        Stream destination,
        Stream rowLedger,
        Stream allocationLedger,
        CompactRawTableBuilderResult allocation,
        CompactRawDataLongValueValidatorBudget budget,
        long maxAllocationReadBytes,
        CancellationToken cancellationToken,
        IReadOnlyList<ColumnInfo>? hydratedColumns = null)
    {
        Guard.NotNull(session, nameof(session));
        Guard.NotNull(definition, nameof(definition));
        Guard.NotNull(destination, nameof(destination));
        Guard.NotNull(rowLedger, nameof(rowLedger));
        Guard.NotNull(allocationLedger, nameof(allocationLedger));
        Guard.NotNull(allocation, nameof(allocation));
        Guard.NotNull(budget, nameof(budget));
        Guard.InRange(maxAllocationReadBytes, 0L, long.MaxValue, nameof(maxAllocationReadBytes));
        cancellationToken.ThrowIfCancellationRequested();
        if (!allocationLedger.CanRead || !allocationLedger.CanSeek || Aliases(allocationLedger, destination) || Aliases(allocationLedger, rowLedger))
        {
            throw new ArgumentException("The allocation ledger must be independently readable and seekable.", nameof(allocationLedger));
        }

        if (allocation.AllocationRecords < 0 || allocation.AllocationRecords > long.MaxValue / 12 || allocationLedger.Length != allocation.AllocationRecords * 12)
        {
            throw Corrupt("The complete per-column LVAL allocation ledger has invalid geometry.");
        }

        if (budget.MaxWorkingBytes < 4096)
        {
            throw new JetLimitationException(JetErrorCode.ValueTooLarge, "Per-column LVAL ledger validation exceeds MaxWorkingBytes.");
        }

        using IDisposable admission = session.Accounting.ReserveWorkingBytes(4096, owner);
        var validator = new Validator(allocationLedger, allocation.AllocationRecords, maxAllocationReadBytes);
        CompactRawDataLongValueValidatorResult raw = await CompactRawDataLongValueValidator.ValidateAsync(
            session, owner, owner, definition, destination, rowLedger, allocation.Data, allocation.LongValues, budget, cancellationToken, hydratedColumns, validator.CompareAsync).ConfigureAwait(false);
        validator.RequireComplete();
        cancellationToken.ThrowIfCancellationRequested();
        return new(raw, validator.Records, validator.ReadBytes);
    }

    private static bool Aliases(Stream first, Stream second)
        => ReferenceEquals(first, second) || (first is FileStream firstFile && second is FileStream secondFile
            && string.Equals(firstFile.Name, secondFile.Name, StringComparison.OrdinalIgnoreCase));

    private static JetCorruptDataException Corrupt(string reason) => new(JetErrorCode.MalformedValue, reason);

    private sealed class Validator(Stream ledger, long records, long maxReadBytes)
    {
        private readonly byte[] record = new byte[12];

        internal long Records { get; private set; }

        internal long ReadBytes { get; private set; }

        internal async ValueTask CompareAsync(CompactRawField field, CompactPageRun allocation, CancellationToken token)
        {
            if (this.Records >= records || ledger.Length != records * this.record.Length)
            {
                throw Corrupt("An independently compared external allocation has no complete per-column ledger record.");
            }

            ledger.Position = this.Records * this.record.Length;
            int offset = 0;
            while (offset < this.record.Length)
            {
                token.ThrowIfCancellationRequested();
                int request = this.record.Length - offset;
                if (request > maxReadBytes - this.ReadBytes)
                {
                    throw new JetLimitationException(JetErrorCode.ValueTooLarge, "Per-column LVAL ledger validation exceeds its cumulative read allowance.");
                }

                this.ReadBytes += request;
                int read = await ledger.ReadAsync(this.record.AsMemory(offset, request), token).ConfigureAwait(false);
                if (read <= 0 || read > request)
                {
                    throw Corrupt("A complete per-column LVAL allocation ledger record is truncated.");
                }

                offset += read;
            }

            token.ThrowIfCancellationRequested();
            if (BinaryPrimitives.ReadInt32LittleEndian(this.record) != field.Column.ColNum
                || BinaryPrimitives.ReadInt32LittleEndian(this.record.AsSpan(4)) != allocation.FirstPageNumber
                || BinaryPrimitives.ReadInt32LittleEndian(this.record.AsSpan(8)) != allocation.PageCount)
            {
                throw Corrupt("A per-column LVAL ledger record disagrees with its independently compared original field and private allocation.");
            }

            this.Records++;
        }

        internal void RequireComplete()
        {
            if (this.Records != records || ledger.Length != records * this.record.Length)
            {
                throw Corrupt("Independent external allocations do not cover the complete per-column LVAL ledger.");
            }
        }
    }
}
