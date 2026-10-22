namespace JetDatabaseWriter.Maintenance;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;

/// <summary>Independently compares discovered source objects with the reopened destination's active catalog and local definitions.</summary>
/// <remarks>Complete raw table and prefix comparisons establish byte preservation separately. Missing source evidence, opaque
/// incoming references, index semantics, native acceptance and safe reclamation remain outside this comparison.</remarks>
internal static class CompactPinnedDestinationInventory
{
    /// <summary>Requires exact discovered catalog identities and local owners through complete independently bounded inventories.</summary>
    /// <param name="source">The original discovery session and cumulative accounting.</param>
    /// <param name="destination">The exact independently reopened private page source bound to active membership.</param>
    /// <param name="activeCatalog">Independent active catalog row membership, held throughout inventory.</param>
    /// <param name="tables">The complete independently compared original local-table plan.</param>
    /// <param name="budget">Inventory, retained identity and simultaneous traversal admission.</param>
    /// <param name="cancellationToken">Cancels reservation, each identity comparison and every traversal.</param>
    /// <returns>The exact compared discovered object count.</returns>
    /// <exception cref="JetCorruptDataException">An identity, local owner or complete discovered membership differs.</exception>
    /// <exception cref="JetLimitationException">An inventory or simultaneous storage allowance is exhausted.</exception>
    internal static async ValueTask<int> ValidateAsync(
        CompactDiscoverySession source,
        IPageSource destination,
        CompactActiveCatalogRows activeCatalog,
        IReadOnlyList<CompactPinnedDatabaseTable> tables,
        CompactTraversalBudget budget,
        CancellationToken cancellationToken)
    {
        long retained = 4096L + (1024L * budget.MaxObjects) + (128L * tables.Count);
        if (budget.MaxObjects < 1 || retained >= budget.MaxWorkingBytes)
        {
            throw new JetLimitationException(JetErrorCode.ValueTooLarge, "Destination inventory retained identities exceed their object or working-storage allowance.");
        }

        cancellationToken.ThrowIfCancellationRequested();
        using IDisposable membership = source.Accounting.ReserveWorkingBytes(retained);
        var objects = new Dictionary<long, CompactStorageRecord>();
        var owners = new HashSet<long>();
        foreach (CompactPinnedDatabaseTable table in tables)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!owners.Add(table.Owner))
            {
                throw Corrupt("The independently compared local table plan repeats a definition owner.");
            }
        }

        int schemaBytes = budget.MaxSchemaBytes;
        int definitionBytes = Math.Min(schemaBytes, 16 * 1024 * 1024);
        CompactTraversalBudget traversal = new()
        {
            Accounting = source.Accounting,
            MaxPages = budget.MaxPages,
            MaxObjects = budget.MaxObjects,
            MaxReferences = budget.MaxReferences,
            MaxDiagnostics = budget.MaxDiagnostics,
            MaxSchemaBytes = schemaBytes,
            MaxPageReads = budget.MaxPageReads,
            MaxMapSteps = budget.MaxMapSteps,
            MaxRecords = budget.MaxRecords,
            MaxWorkingBytes = budget.MaxWorkingBytes - retained,
        };
        using var originals = new TableDefReader(source.SourcePages, source.SourceFormat, cacheResults: false, definitionBytes, schemaBytes);
        await CompactStorageInventory.ScanAsync(
            source.SourcePages,
            source.SourceFormat,
            originals,
            traversal,
            (record, token) =>
            {
                token.ThrowIfCancellationRequested();
                if (record.Kind == "catalog-object")
                {
                    if (record.ObjectId is not { } id || objects.Count >= budget.MaxObjects || !objects.TryAdd(id, record))
                    {
                        throw Corrupt("The independent source inventory has absent, duplicate or excessive catalog identities.");
                    }
                }

                return default;
            },
            cancellationToken).ConfigureAwait(false);
        int count = objects.Count;
        using var installed = new TableDefReader(destination, source.SourceFormat, cacheResults: false, definitionBytes, schemaBytes);
        await CompactStorageInventory.ScanAsync(
            destination,
            source.SourceFormat,
            installed,
            traversal,
            (record, token) =>
            {
                token.ThrowIfCancellationRequested();
                if (record.Kind == "catalog-object")
                {
                    if (record.ObjectId is not { } id || !objects.TryGetValue(id, out CompactStorageRecord? original)
                        || original.Name != record.Name || original.ObjectType != record.ObjectType
                        || original.Flags != record.Flags || original.ParentId != record.ParentId || original.OwnerTDefPage != record.OwnerTDefPage)
                    {
                        throw Corrupt("An active destination catalog identity differs from independent original object evidence.");
                    }

                    _ = objects.Remove(id);
                }

                return default;
            },
            cancellationToken,
            (owner, _, token) =>
            {
                token.ThrowIfCancellationRequested();
                if (!owners.Remove(owner))
                {
                    throw Corrupt("An active destination local definition is absent from the independently compared plan.");
                }

                return default;
            },
            activeCatalog: activeCatalog).ConfigureAwait(false);
        if (objects.Count != 0 || owners.Count != 0 || count != activeCatalog.ActiveRows)
        {
            throw Corrupt("Active destination discovery omits independently compared catalog identities or local definitions.");
        }

        return count;
    }

    private static JetCorruptDataException Corrupt(string reason) => new(JetErrorCode.CorruptCatalog, reason);
}
