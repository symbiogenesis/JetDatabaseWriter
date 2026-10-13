namespace JetDatabaseWriter.Pages;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;

/// <summary>
/// Decides whether the writer may extend a local table's owned-page usage map.
/// User tables, MSysObjects and Jet4/ACE system tables need allocated data pages listed
/// so native scans and compact retain their rows. Other imported Jet3 system maps
/// remain untouched. Decisions are cached by TDEF page and restored on rollback;
/// registration also permits a newly created table at a formerly refused page.
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
        if (tdefPageNumber <= 0)
        {
            return false;
        }

        if (this.TryGetDecision(tdefPageNumber, out bool decided))
        {
            return decided;
        }

        // MSysObjects has a fixed root and does not need a catalog lookup.
        if (tdefPageNumber == 2)
        {
            return true;
        }

        bool writable = await this.IsLocalTableAsync(tdefPageNumber, cancellationToken).ConfigureAwait(false);
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
    /// <paramref name="tdefPageNumber"/> is a named local table.
    /// </summary>
    /// <param name="tdefPageNumber">The table's TDEF page number.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns><see langword="true"/> for a local table; <see langword="false"/> for an unnamed, nonlocal or unrepresented table.</returns>
    private async ValueTask<bool> IsLocalTableAsync(long tdefPageNumber, CancellationToken cancellationToken)
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
                return !string.IsNullOrEmpty(row.Name) && (!format.IsJet3 || (unchecked((uint)row.Flags) & Constants.SystemObjects.SystemTableMask) == 0);
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
