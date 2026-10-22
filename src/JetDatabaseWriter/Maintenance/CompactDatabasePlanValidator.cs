namespace JetDatabaseWriter.Maintenance;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;

/// <summary>Independently binds a complete discovered-local-table plan to the immutable original source.</summary>
internal static class CompactDatabasePlanValidator
{
    /// <summary>Requires exactly one matching original image for every independently discovered local definition.</summary>
    /// <param name="source">The caller-owned immutable decrypted original page source.</param>
    /// <param name="format">The verified original format.</param>
    /// <param name="tables">The proposed original owner/image pairs, immutable throughout this call.</param>
    /// <param name="budget">The cumulative discovery and simultaneous storage limits.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <param name="accounting">Optional existing whole-session admission and work counters.</param>
    /// <returns>Exact discovered-local coverage evidence after the entire independent inventory succeeds.</returns>
    /// <remarks>
    /// Includes MSysObjects, hidden and system tables. Catalog-only links and opaque objects stay pinned and are never followed.
    /// Source bytes and parsed layout projections are compared against uncached definitions discovered from original catalog evidence.
    /// Inventory uncertainty is retained: successful plan coverage is not complete discovery, ownership or destination validation.
    /// Caller-owned images are not copied. The membership snapshot and inventory storage remain admitted across all awaited reads.
    /// </remarks>
    /// <exception cref="JetCorruptDataException">A proposed owner is missing, repeated, extra or has different original bytes, or independent inventory fails.</exception>
    /// <exception cref="JetLimitationException">A cumulative discovery or storage limit is exceeded.</exception>
    internal static async ValueTask<CompactDatabasePlanResult> ValidateAsync(
        IPageSource source,
        JetFormat format,
        IReadOnlyList<CompactDatabasePlanTable> tables,
        CompactDiscoveryBudget budget,
        CancellationToken cancellationToken,
        CompactDiscoveryAccounting? accounting = null)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(tables, nameof(tables));
        Guard.NotNull(budget, nameof(budget));
        Guard.InRange(budget.MaxObjects, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxCatalogObjects, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxSchemaBytes, 1L, long.MaxValue, nameof(budget));
        Guard.Positive(budget.MaxDefinitionBytes, nameof(budget));
        source.ThrowIfDisposedOrCancelled(cancellationToken);
        long sourcePages = source.PageCount;
        if (source.PageSize != format.PageSize || sourcePages < 3)
        {
            throw Corrupt(0, "The original source geometry disagrees with its verified format.");
        }

        if (sourcePages > budget.MaxPages || tables.Count > budget.MaxObjects || tables.Count > budget.MaxCatalogObjects)
        {
            throw Limit("The proposed plan exceeds its physical page or local-table allowance.");
        }

        accounting ??= new(budget);

        // Dictionary capacity and entries, retained image references and traversal
        // bookkeeping are admitted before the plan snapshot allocates storage.
        using IDisposable membership = accounting.ReserveWorkingBytes(1024L + (tables.Count * 128L));
        var remaining = new Dictionary<long, TDefImage>(tables.Count);
        long definitionBytes = 0;
        foreach (CompactDatabasePlanTable table in tables)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Guard.NotNull(table, nameof(tables));
            Guard.NotNull(table.Definition, nameof(tables));
            accounting.Charge(CompactDiscoveryCharge.Objects, 1, table.OwnerTDefPage);
            if (table.OwnerTDefPage < 2 || table.OwnerTDefPage >= sourcePages)
            {
                throw Corrupt(table.OwnerTDefPage, "A proposed local-table owner lies outside the original definition storage.");
            }

            if (table.Definition.LogicalByteLength > budget.MaxDefinitionBytes
                || table.Definition.LogicalByteLength > budget.MaxSchemaBytes - definitionBytes)
            {
                throw Limit("The proposed original images exceed the definition or aggregate schema allowance.", table.OwnerTDefPage);
            }

            definitionBytes += table.Definition.LogicalByteLength;
            if (!remaining.TryAdd(table.OwnerTDefPage, table.Definition))
            {
                throw Corrupt(table.OwnerTDefPage, "The proposed plan repeats an original local-table owner.");
            }
        }

        int matched = 0;
        int schemaBytes = checked((int)Math.Min(int.MaxValue, budget.MaxSchemaBytes));
        using var definitions = new TableDefReader(source, format, cacheResults: false, budget.MaxDefinitionBytes, schemaBytes);
        await CompactStorageInventory.ScanAsync(
            source,
            format,
            definitions,
            new()
            {
                Accounting = accounting,
                MaxPages = budget.MaxPages,
                MaxObjects = checked((int)Math.Min(int.MaxValue, budget.MaxCatalogObjects)),
                MaxReferences = budget.MaxReferences,
                MaxDiagnostics = checked((int)Math.Min(int.MaxValue, budget.MaxDiagnostics)),
                MaxSchemaBytes = schemaBytes,
                MaxPageReads = budget.MaxPageReads,
                MaxMapSteps = budget.MaxMapSteps,
                MaxRecords = budget.MaxRecords,
                MaxWorkingBytes = budget.MaxWorkingBytes,
            },
            IgnoreStorageAsync,
            cancellationToken,
            (owner, original, token) =>
            {
                token.ThrowIfCancellationRequested();
                if (!remaining.TryGetValue(owner, out TDefImage? proposed))
                {
                    throw Corrupt(owner, "The proposed plan omits an independently discovered original local definition.");
                }

                if (!OriginalImageMatches(original, proposed))
                {
                    throw Corrupt(owner, "The proposed original definition bytes or parsed layout differ from the independently reopened source image.");
                }

                _ = remaining.Remove(owner);
                matched++;
                return default;
            }).ConfigureAwait(false);
        source.ThrowIfDisposedOrCancelled(cancellationToken);
        if (source.PageSize != format.PageSize || source.PageCount != sourcePages)
        {
            throw Corrupt(0, "The immutable original source geometry changed during plan validation.");
        }

        if (remaining.Count != 0)
        {
            throw Corrupt(0, "The proposed plan contains an owner absent from the independently discovered local definitions.");
        }

        return new(matched, sourcePages);
    }

    private static bool OriginalImageMatches(TDefImage original, TDefImage proposed)
    {
        if (!original.Bytes.SequenceEqual(proposed.Bytes)
            || original.Header != proposed.Header
            || original.HasDeletedColumns != proposed.HasDeletedColumns
            || original.Issues != proposed.Issues
            || original.IndexSection != proposed.IndexSection
            || original.Columns.Count != proposed.Columns.Count
            || original.RealIndexes.Count != proposed.RealIndexes.Count
            || original.LogicalIndexes.Count != proposed.LogicalIndexes.Count)
        {
            return false;
        }

        for (int index = 0; index < original.Columns.Count; index++)
        {
            ColumnInfo left = original.Columns[index];
            ColumnInfo right = proposed.Columns[index];
            if (left.Type != right.Type || left.ColNum != right.ColNum || left.VarIdx != right.VarIdx
                || left.FixedOff != right.FixedOff || left.Size != right.Size || left.Flags != right.Flags
                || left.ExtraFlags != right.ExtraFlags || left.CalculatedResultType != right.CalculatedResultType
                || left.Name != right.Name || left.Misc != right.Misc || left.NativeIdentity != right.NativeIdentity
                || left.TextSortOrder != right.TextSortOrder || left.NumericPrecision != right.NumericPrecision
                || left.NumericScale != right.NumericScale || !left.RawDescriptor.SequenceEqual(right.RawDescriptor))
            {
                return false;
            }
        }

        for (int index = 0; index < original.RealIndexes.Count; index++)
        {
            TDefIndexDescriptor left = original.RealIndexes[index];
            TDefIndexDescriptor right = proposed.RealIndexes[index];
            if (left.Slot != right.Slot || left.RootPage != right.RootPage
                || !left.Columns.SequenceEqual(right.Columns) || !left.RawDescriptor.SequenceEqual(right.RawDescriptor))
            {
                return false;
            }
        }

        for (int index = 0; index < original.LogicalIndexes.Count; index++)
        {
            TDefLogicalIndexDescriptor left = original.LogicalIndexes[index];
            TDefLogicalIndexDescriptor right = proposed.LogicalIndexes[index];
            if (left.Entry != right.Entry || left.Name != right.Name || !left.RawDescriptor.SequenceEqual(right.RawDescriptor))
            {
                return false;
            }
        }

        return true;
    }

    private static ValueTask IgnoreStorageAsync(CompactStorageRecord record, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return default;
    }

    private static JetCorruptDataException Corrupt(long page, string reason)
        => new(JetErrorCode.CorruptTableDefinition, reason, new JetErrorInfo { PageNumber = page, Reason = reason });

    private static JetLimitationException Limit(string reason, long? page = null)
        => new(JetErrorCode.ValueTooLarge, reason, new JetErrorInfo { PageNumber = page, Reason = reason });
}
