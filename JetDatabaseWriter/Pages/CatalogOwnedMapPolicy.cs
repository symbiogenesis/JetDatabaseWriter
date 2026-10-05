namespace JetDatabaseWriter.Pages;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;

/// <summary>
/// The writer's <see cref="IOwnedMapPolicy"/>, decided from the catalog. The
/// writer may extend the owned-page usage map of a table whose map it created
/// (<see cref="RegisterWritable"/>), and of a table whose <c>MSysObjects</c>
/// row is a local table with a name that does not start with <c>MSys</c>. On
/// Jet3 it extends none, because it keeps no table usage maps there. It leaves
/// the maps of the system tables it did not create alone, <c>MSysObjects</c>'
/// own included: Access populates and manages them, and DAO's
/// <c>OpenDatabase</c> refuses a file in which they were changed ("Invalid
/// argument"). Both answers are kept by TDEF page, so only the first append
/// to a table scans <c>MSysObjects</c>.
/// </summary>
/// <param name="format">The file's format profile.</param>
/// <param name="tableDefs">Reads the <c>MSysObjects</c> table definition.</param>
/// <param name="catalogRows">Reads the <c>MSysObjects</c> rows that name a table's TDEF page.</param>
internal sealed class CatalogOwnedMapPolicy(JetFormat format, TableDefReader tableDefs, CatalogRowReader catalogRows) : IOwnedMapPolicy
{
#if NET9_0_OR_GREATER
    private readonly Lock ownedMapSetsLock = new();
#else
    private readonly object ownedMapSetsLock = new();
#endif
    private readonly HashSet<long> writableTdefs = [];
    private readonly HashSet<long> refusedTdefs = [];

    /// <inheritdoc/>
    public async ValueTask<bool> CanMaintainAsync(long tdefPageNumber, CancellationToken cancellationToken)
    {
        if (format.IsJet3 || tdefPageNumber <= 0)
        {
            return false;
        }

        if (this.TryGetDecision(tdefPageNumber, out bool decided))
        {
            return decided;
        }

        // MSysObjects' own map, unless this writer built and registered it.
        if (tdefPageNumber == 2)
        {
            return false;
        }

        bool writable = await this.IsUserTableAsync(tdefPageNumber, cancellationToken).ConfigureAwait(false);
        this.RecordDecision(tdefPageNumber, writable);
        return writable;
    }

    /// <inheritdoc/>
    public void RegisterWritable(long tdefPageNumber)
    {
        lock (this.ownedMapSetsLock)
        {
            _ = this.writableTdefs.Add(tdefPageNumber);
            _ = this.refusedTdefs.Remove(tdefPageNumber);
        }
    }

    /// <inheritdoc/>
    public OwnedMapPolicyState Capture()
    {
        lock (this.ownedMapSetsLock)
        {
            return new OwnedMapPolicyState([.. this.writableTdefs], [.. this.refusedTdefs]);
        }
    }

    /// <inheritdoc/>
    public void Restore(OwnedMapPolicyState state)
    {
        lock (this.ownedMapSetsLock)
        {
            this.writableTdefs.Clear();
            this.writableTdefs.UnionWith(state.WritableTdefs);
            this.refusedTdefs.Clear();
            this.refusedTdefs.UnionWith(state.RefusedTdefs);
        }
    }

    /// <summary>
    /// Returns whether the first <c>MSysObjects</c> table row that names
    /// <paramref name="tdefPageNumber"/> is a user table: one with a name that
    /// does not start with <c>MSys</c>.
    /// </summary>
    /// <param name="tdefPageNumber">The table's TDEF page number.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns><see langword="true"/> for a user table; <see langword="false"/> for a system table or a page no table row names.</returns>
    private async ValueTask<bool> IsUserTableAsync(long tdefPageNumber, CancellationToken cancellationToken)
    {
        TableDef? msys = await tableDefs.ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (msys is null)
        {
            return false;
        }

        List<CatalogRow> rows = await catalogRows.GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);
        foreach (CatalogRow row in rows)
        {
            if (row.TDefPage == tdefPageNumber && row.ObjectType == Constants.SystemObjects.UserTableType)
            {
                return !string.IsNullOrEmpty(row.Name) && !row.Name.StartsWith("MSys", StringComparison.OrdinalIgnoreCase);
            }
        }

        return false;
    }

    private bool TryGetDecision(long tdefPageNumber, out bool writable)
    {
        lock (this.ownedMapSetsLock)
        {
            writable = this.writableTdefs.Contains(tdefPageNumber);
            return writable || this.refusedTdefs.Contains(tdefPageNumber);
        }
    }

    private void RecordDecision(long tdefPageNumber, bool writable)
    {
        lock (this.ownedMapSetsLock)
        {
            HashSet<long> decided = writable ? this.writableTdefs : this.refusedTdefs;
            _ = decided.Add(tdefPageNumber);
        }
    }
}
