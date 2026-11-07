namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;

/// <summary>
/// Audits and repairs out-of-sync <c>TDef</c> header counters (<c>NumRows</c>, <c>AutoNumber</c>,
/// and <c>ComplexAutoNumber</c>) from verified active table rows.
/// </summary>
internal static class CompactTableCounterRepairer
{
    internal static async ValueTask<CompactTableCounterEvaluation> EvaluateAsync(
        JetFormat format,
        long owner,
        string tableName,
        TDefImage definition,
        long activeRowCount,
        IReadOnlyList<(RowLocation Location, byte[] Payload)>? activeRows,
        bool repairTableCounters,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(definition, nameof(definition));
        cancellationToken.ThrowIfCancellationRequested();

        uint declaredRowCount = definition.Header.Counters.RowCount;
        uint declaredAutoNumber = definition.Header.Counters.AutoNumber;
        uint declaredComplexAutoNumber = definition.Header.Counters.ComplexAutoNumber;

        if (!repairTableCounters || owner < 3 || CompactDataPageSalvager.IsSystemTableName(tableName))
        {
            return new CompactTableCounterEvaluation(
                owner,
                tableName,
                declaredRowCount,
                declaredRowCount,
                declaredAutoNumber,
                declaredAutoNumber,
                declaredComplexAutoNumber,
                declaredComplexAutoNumber,
                CountersRepaired: false,
                Diagnostic: null);
        }

        bool hasAutoNumberColumn = false;
        bool hasComplexColumn = false;
        var autoNumberColNums = new HashSet<int>();
        var complexColNums = new HashSet<int>();
        foreach (ColumnInfo col in definition.Columns)
        {
            if (col.Type == ColumnType.ComplexType)
            {
                hasComplexColumn = true;
                _ = complexColNums.Add(col.ColNum);
            }
            else if ((col.Flags & Constants.ColumnDescriptorFlags.AutoNumber) != 0 && col.Type == ColumnType.LongIntegerType)
            {
                hasAutoNumberColumn = true;
                _ = autoNumberColNums.Add(col.ColNum);
            }
        }

        long maxObservedAutoNumber = 0;
        long maxObservedComplexRef = 0;
        if (activeRows is not null && (hasAutoNumberColumn || hasComplexColumn))
        {
            foreach ((RowLocation location, byte[] payload) in activeRows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await CompactRawFieldReader.ScanAsync(
                    format,
                    definition,
                    location,
                    payload,
                    (field, _) =>
                    {
                        if (field.Kind == CompactRawFieldKind.Stored && field.RawBytes.Length == 4)
                        {
                            int val = BinaryPrimitives.ReadInt32LittleEndian(field.RawBytes.Span);
                            if (val > 0)
                            {
                                if (autoNumberColNums.Contains(field.Column.ColNum) && val > maxObservedAutoNumber)
                                {
                                    maxObservedAutoNumber = val;
                                }
                                else if (complexColNums.Contains(field.Column.ColNum) && val > maxObservedComplexRef)
                                {
                                    maxObservedComplexRef = val;
                                }
                            }
                        }

                        return default;
                    },
                    cancellationToken).ConfigureAwait(false);
            }
        }

        uint repairedRowCount = checked((uint)activeRowCount);
        uint repairedAutoNumber = declaredAutoNumber;
        if (hasAutoNumberColumn)
        {
            if (activeRowCount == 0 && declaredAutoNumber != 0)
            {
                repairedAutoNumber = 0;
            }
            else if (activeRowCount > 0 && maxObservedAutoNumber > 0 && declaredAutoNumber < (uint)maxObservedAutoNumber)
            {
                repairedAutoNumber = checked((uint)maxObservedAutoNumber);
            }
        }

        uint repairedComplexAutoNumber = declaredComplexAutoNumber;
        if (format.TDef.ComplexAutoNumber >= 0
            && hasComplexColumn
            && activeRowCount > 0
            && maxObservedComplexRef > 0
            && declaredComplexAutoNumber < (uint)maxObservedComplexRef)
        {
            repairedComplexAutoNumber = checked((uint)maxObservedComplexRef);
        }

        bool corruptRowCount = repairedRowCount != declaredRowCount;
        bool corruptAutoNumber = activeRowCount > 0 && repairedAutoNumber != declaredAutoNumber;
        bool corruptComplexAutoNumber = repairedComplexAutoNumber != declaredComplexAutoNumber;
        bool corruptCounterRepaired = corruptRowCount || corruptAutoNumber || corruptComplexAutoNumber;
        bool countersRepaired = corruptCounterRepaired || repairedAutoNumber != declaredAutoNumber;

        AccessCompactDiagnostic? diagnostic = null;
        if (corruptCounterRepaired)
        {
            diagnostic = new AccessCompactDiagnostic(
                "repaired-table-counter",
                owner,
                owner,
                null,
                $"Repaired TDEF header counters on table '{tableName}' (owner {owner}): NumRows {declaredRowCount} -> {repairedRowCount}, AutoNumber {declaredAutoNumber} -> {repairedAutoNumber}.",
                AccessCompactDiagnosticCategory.RepairedTableCounter,
                tableName);
        }

        return new CompactTableCounterEvaluation(
            owner,
            tableName,
            declaredRowCount,
            repairedRowCount,
            declaredAutoNumber,
            repairedAutoNumber,
            declaredComplexAutoNumber,
            repairedComplexAutoNumber,
            countersRepaired,
            diagnostic);
    }

    internal static async ValueTask<(long ReadBytes, long WrittenBytes)> ApplyAndValidateAsync(
        Stream destination,
        JetFormat format,
        IReadOnlyList<CompactTableCounterEvaluation> evaluations,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(destination, nameof(destination));
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(evaluations, nameof(evaluations));
        cancellationToken.ThrowIfCancellationRequested();

        long readBytes = 0;
        long writtenBytes = 0;
        byte[] page = new byte[format.PageSize];
        byte[] verify = new byte[format.PageSize];

        foreach (CompactTableCounterEvaluation eval in evaluations)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!eval.CountersRepaired)
            {
                continue;
            }

            long pageOffset = checked(eval.OwnerTDefPage * format.PageSize);
            destination.Position = pageOffset;
            await ReadExactAsync(destination, page, cancellationToken).ConfigureAwait(false);
            readBytes = checked(readBytes + format.PageSize);

            if (page[0] != Constants.PageTypes.TableDefinition)
            {
                throw new JetCorruptDataException(
                    JetErrorCode.CorruptTableDefinition,
                    $"Destination root TDEF page {eval.OwnerTDefPage} has invalid page type 0x{page[0]:X2} during counter repair.");
            }

            BinaryPrimitives.WriteUInt32LittleEndian(page.AsSpan(format.TDef.NumRows, 4), eval.RepairedRowCount);
            BinaryPrimitives.WriteUInt32LittleEndian(page.AsSpan(format.TDef.AutoNumber, 4), eval.RepairedAutoNumber);
            if (format.TDef.ComplexAutoNumber >= 0)
            {
                BinaryPrimitives.WriteUInt32LittleEndian(page.AsSpan(format.TDef.ComplexAutoNumber, 4), eval.RepairedComplexAutoNumber);
            }

            if (!format.IsJet3 && eval.RepairedRowCount != eval.DeclaredRowCount)
            {
                int realIdxCount = TDefCodec.ReadCounts(format, page).RealIndexCount;
                int slotEnd = format.TDef.BlockEnd + (realIdxCount * format.TDef.RealIdxEntrySz);
                if (realIdxCount is > 0 and <= Constants.TableDefinition.MaxIndexes && slotEnd <= format.PageSize)
                {
                    for (int i = 0; i < realIdxCount; i++)
                    {
                        int countOffset = format.TDef.BlockEnd + (i * format.TDef.RealIdxEntrySz) + 4;
                        uint currentIdxRows = BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(countOffset, 4));
                        if (currentIdxRows == eval.DeclaredRowCount)
                        {
                            BinaryPrimitives.WriteUInt32LittleEndian(page.AsSpan(countOffset, 4), eval.RepairedRowCount);
                        }
                    }
                }
            }

            destination.Position = pageOffset;
            await destination.WriteAsync(page.AsMemory(0, format.PageSize), cancellationToken).ConfigureAwait(false);
            writtenBytes = checked(writtenBytes + format.PageSize);

            await destination.FlushAsync(cancellationToken).ConfigureAwait(false);
            destination.Position = pageOffset;
            await ReadExactAsync(destination, verify, cancellationToken).ConfigureAwait(false);
            readBytes = checked(readBytes + format.PageSize);

            if (!page.AsSpan(0, format.PageSize).SequenceEqual(verify.AsSpan(0, format.PageSize)))
            {
                throw new JetCorruptDataException(
                    JetErrorCode.CorruptTableDefinition,
                    $"Reopened verification of repaired TDEF counters on page {eval.OwnerTDefPage} disagreed with written bytes.");
            }
        }

        return (readBytes, writtenBytes);
    }

    private static async ValueTask ReadExactAsync(Stream stream, byte[] buffer, CancellationToken cancellationToken)
    {
        int offset = 0;
        while (offset < buffer.Length)
        {
            int read = await stream.ReadAsync(buffer.AsMemory(offset, buffer.Length - offset), cancellationToken).ConfigureAwait(false);
            if (read <= 0)
            {
                throw new JetCorruptDataException(JetErrorCode.CorruptPage, "Unexpected EOF while reading destination TDEF page for counter repair.");
            }

            offset += read;
        }
    }
}
