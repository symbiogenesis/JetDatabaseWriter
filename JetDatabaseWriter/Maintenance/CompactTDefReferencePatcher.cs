namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Threading;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;

/// <summary>Retargets complete known storage references without regenerating a pinned raw table definition.</summary>
/// <remarks>
/// The supplied image and format must come from the same verified source. All physical TDEF roots and continuation
/// pages, catalog identities, relationship partner roots and uninterpreted incoming references remain pinned.
/// This operation replaces every declared physical index root/map and every existing known LVAL map pair, preserving
/// counters, sort metadata, descriptor identities, unknown header/trailer bytes and original padding verbatim.
/// It creates no new trailer records and proves neither destination map contents nor global ownership or publication safety.
/// The returned logical image requires installation on the original pinned chain, preserving its continuation headers.
/// Input collections remain immutable during the call. Caller-owned images/collections and runtime overhead are separate
/// from the conservative additional working-storage allowance.
/// </remarks>
internal static class CompactTDefReferencePatcher
{
    /// <summary>Copies a verified raw image and replaces only complete, known storage pointer words.</summary>
    /// <param name="format">The verified original native format.</param>
    /// <param name="image">The immutable original logical definition including its padding.</param>
    /// <param name="owned">The staged table-owned-pages map.</param>
    /// <param name="free">The staged table-free-space map.</param>
    /// <param name="indexes">One replacement for every physical descriptor, in its original slot order.</param>
    /// <param name="longValues">One map pair for every original LVAL trailer record, in its original order.</param>
    /// <param name="maxWorkingBytes">The additional image-copy and bounded parser-storage admission.</param>
    /// <param name="cancellationToken">Cancels before allocation and between descriptor patches.</param>
    /// <returns>An independent unvalidated logical image with all other bytes retained.</returns>
    /// <exception cref="ArgumentException">Replacement identities, counts or references are invalid.</exception>
    /// <exception cref="JetCorruptDataException">Descriptor or trailer boundaries are unverified.</exception>
    /// <exception cref="JetLimitationException">The additional working-storage allowance is exhausted.</exception>
    internal static byte[] Patch(
        JetFormat format,
        TDefImage image,
        UsageMapPointer owned,
        UsageMapPointer free,
        IReadOnlyList<CompactTDefIndexReferences> indexes,
        IReadOnlyList<CompactTDefLongValueReferences> longValues,
        long maxWorkingBytes,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(image, nameof(image));
        Guard.NotNull(indexes, nameof(indexes));
        Guard.NotNull(longValues, nameof(longValues));
        Guard.InRange(maxWorkingBytes, 1L, long.MaxValue, nameof(maxWorkingBytes));
        cancellationToken.ThrowIfCancellationRequested();
        if (image.LogicalByteLength + 65_536L > maxWorkingBytes)
        {
            throw new JetLimitationException(JetErrorCode.ValueTooLarge, "Raw TDEF reference patching exceeds MaxWorkingBytes.");
        }

        long end = (long)image.Header.LogicalLength + 8;
        if (image.Issues != TDefParseIssues.None || end < format.TDef.BlockEnd || end > image.LogicalByteLength
            || image.RealIndexes.Count != image.Header.RealIndexCount || image.LogicalIndexes.Count != image.Header.LogicalIndexCount)
        {
            throw Corrupt("The raw table definition does not have complete verified descriptor boundaries.");
        }

        if (indexes.Count != image.RealIndexes.Count || indexes.Count > Constants.TableDefinition.MaxIndexes
            || longValues.Count > Constants.TableDefinition.MaxColumns)
        {
            throw new ArgumentException("Every original physical index and LVAL map must have exactly one replacement.", nameof(indexes));
        }

        ValidatePointer(owned);
        ValidatePointer(free);
        byte[] bytes = image.CopyBytes();
        if (!LongValueUsageMaps.TryRead(format, bytes, out List<LongValueUsageMap> maps, out _))
        {
            throw Corrupt("The raw table definition has an unverified LVAL trailer boundary.");
        }

        if (maps.Count != longValues.Count)
        {
            throw new ArgumentException("Every original LVAL map must have exactly one replacement.", nameof(longValues));
        }

        WritePointer(bytes, format.TDef.UsedPages, owned, end);
        WritePointer(bytes, format.TDef.FreePages, free, end);
        for (int i = 0; i < indexes.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            CompactTDefIndexReferences replacement = indexes[i];
            if (replacement.RootPage is 0 or > 0xFFFFFF)
            {
                throw new ArgumentException("A staged index root must fit a nonzero native page reference.", nameof(indexes));
            }

            int root = image.RealIndexes[i].Slot.FirstDpOffset;
            ValidateOffset(root, end);
            BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(root, sizeof(uint)), replacement.RootPage);
            WritePointer(bytes, root - sizeof(uint), replacement.OwnedPages, end);
        }

        for (int i = 0; i < maps.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            CompactTDefLongValueReferences replacement = longValues[i];
            if (replacement.ColumnNumber != maps[i].ColumnNumber)
            {
                throw new ArgumentException("LVAL replacements must retain the original column identities and order.", nameof(longValues));
            }

            WritePointer(bytes, maps[i].OwnedOffset, replacement.OwnedPages, end);
            WritePointer(bytes, maps[i].FreeOffset, replacement.FreePages, end);
        }

        cancellationToken.ThrowIfCancellationRequested();
        return bytes;
    }

    private static void WritePointer(byte[] bytes, int offset, UsageMapPointer pointer, long end)
    {
        ValidatePointer(pointer);
        ValidateOffset(offset, end);
        UsageMap.WritePointer(bytes, offset, pointer.RowIndex, pointer.PageNumber);
    }

    private static void ValidatePointer(UsageMapPointer pointer)
    {
        if (pointer.PageNumber is <= 0 or > 0xFFFFFF || pointer.RowIndex is < 0 or > byte.MaxValue)
        {
            throw new ArgumentException("A staged map must fit a nonzero native page and row reference.", nameof(pointer));
        }
    }

    private static void ValidateOffset(int offset, long end)
    {
        if (offset < 12 || offset > end - sizeof(uint))
        {
            throw Corrupt("A known storage pointer extends beyond the declared table definition.");
        }
    }

    private static JetCorruptDataException Corrupt(string reason)
        => new(JetErrorCode.CorruptTableDefinition, reason, new JetErrorInfo { Reason = reason });
}

/// <summary>Replacement storage references for one original physical index descriptor.</summary>
/// <param name="RootPage">The staged native root page.</param>
/// <param name="OwnedPages">The staged index-owned-pages map.</param>
internal readonly record struct CompactTDefIndexReferences(uint RootPage, UsageMapPointer OwnedPages);

/// <summary>Replacement map references for one existing original LVAL trailer record.</summary>
/// <param name="ColumnNumber">The original stored column number.</param>
/// <param name="OwnedPages">The staged column-owned-pages map.</param>
/// <param name="FreePages">The staged column-free-space map.</param>
internal readonly record struct CompactTDefLongValueReferences(int ColumnNumber, UsageMapPointer OwnedPages, UsageMapPointer FreePages);
