namespace JetDatabaseWriter.ValueDecoding;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.ValueDecoding.Models;

/// <summary>
/// Assigns scalar values directly and captures bound OLE slices during the same
/// row-layout traversal. Slice storage belongs to an enumeration, never the
/// cached plan; the source page stays live until all payload reads complete.
/// </summary>
/// <typeparam name="T">The mapped row type.</typeparam>
/// <param name="decode">The compiled scalar assignment and OLE slice capture.</param>
/// <param name="oleColumns">The bound OLE column names and compiled assignments.</param>
internal sealed class HybridRowDecodePlan<T>(
    HybridRowDecoder<T> decode,
    (string Name, Action<T, byte[]> Assign)[] oleColumns)
{
    internal int OleColumnCount => oleColumns.Length;

    internal bool TryDecode(
        JetFormat format,
        RowDecodePlan rowPlan,
        byte[] page,
        int rowStart,
        int rowSize,
        T target,
        ColumnSlice[] oleSlices)
        => decode(format, rowPlan, page, rowStart, rowSize, target, oleSlices);

    internal async ValueTask ResolveAsync(
        LongValueDecoder longValues,
        bool strictParsing,
        byte[] page,
        int rowStart,
        T target,
        ColumnSlice[] oleSlices,
        CancellationToken cancellationToken)
    {
        for (int i = 0; i < oleColumns.Length; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ColumnSlice slice = oleSlices[i];
            if (slice.Kind != ColumnSliceKind.Var)
            {
                continue;
            }

            byte[] bytes;
            try
            {
                bytes = slice.DataLen == 0
                    ? []
                    : await longValues.ReadLongValueRawBytesAsync(page, rowStart + slice.DataStart, slice.DataLen, cancellationToken).ConfigureAwait(false);
            }
            catch (InvalidDataException exception)
            {
                _ = LongValueReadPolicy.MissingValue(oleColumns[i].Name, exception, strictParsing);
                continue;
            }

            oleColumns[i].Assign(target, bytes);
        }
    }
}

/// <summary>Decodes scalar bindings and records bound OLE slices without boxing.</summary>
/// <typeparam name="T">The mapped row type.</typeparam>
/// <param name="format">The database format.</param>
/// <param name="decodePlan">The row-layout plan.</param>
/// <param name="page">The source page.</param>
/// <param name="rowStart">The row offset.</param>
/// <param name="rowSize">The row length.</param>
/// <param name="target">The target object.</param>
/// <param name="oleSlices">Enumeration-owned OLE slice storage.</param>
/// <returns>Whether the row layout is readable.</returns>
internal delegate bool HybridRowDecoder<T>(
    JetFormat format,
    RowDecodePlan decodePlan,
    byte[] page,
    int rowStart,
    int rowSize,
    T target,
    ColumnSlice[] oleSlices);
