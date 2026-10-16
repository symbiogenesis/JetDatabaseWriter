namespace JetDatabaseWriter.Pages;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Decides whether the writer may extend a local table's owned-page usage map.
/// User tables, MSysObjects and Jet4/ACE system tables need allocated data pages listed
/// so native scans and compact retain their rows. Jet3 core system tables additionally
/// require exclusive INLINE map rows after reopening. Other imported Jet3 system maps
/// remain untouched. Decisions are cached by TDEF page and restored on rollback;
/// registration also permits a newly created table at a formerly refused page.
/// </summary>
/// <param name="format">The file's format profile.</param>
/// <param name="tableDefs">Reads the <c>MSysObjects</c> table definition.</param>
/// <param name="catalogRows">Reads the <c>MSysObjects</c> rows that name a table's TDEF page.</param>
/// <param name="pages">Reads physical map rows while proving exclusive ownership.</param>
internal sealed class CatalogOwnedMapPolicy(JetFormat format, TableDefReader tableDefs, CatalogRowReader catalogRows, IPageSource pages) : IOwnedMapPolicy
{
#if NET9_0_OR_GREATER
    private readonly Lock ownedMapSetsLock = new();
#else
    private readonly object ownedMapSetsLock = new();
#endif
    private readonly HashSet<long> writableTdefs = [];
    private readonly HashSet<long> refusedTdefs = [];

    private static JetNotSupportedException UnprovenMap(CatalogRow table, string reason)
        => new(
            JetErrorCode.FeatureNotSupported,
            $"The owned-page maps of Jet3 system table '{table.Name}' cannot be maintained safely: {reason}",
            new JetErrorInfo { TableName = table.Name, PageNumber = table.TDefPage, Reason = reason });

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

        int maxEntries = format.IsJet3 ? Math.Max(1, tableDefs.MaxLogicalBytes / format.TDef.BlockEnd) : int.MaxValue;
        List<CatalogRow> rows = await catalogRows.GetCatalogRowsAsync(msys, cancellationToken, maxEntries).ConfigureAwait(false);
        foreach (CatalogRow row in rows)
        {
            if (row.TDefPage == tdefPageNumber && row.ObjectType == Constants.SystemObjects.UserTableType)
            {
                if (!format.IsJet3 || (unchecked((uint)row.Flags) & Constants.SystemObjects.SystemTableMask) == 0)
                {
                    return !string.IsNullOrEmpty(row.Name);
                }

                if (string.Equals(row.Name, Constants.SystemTableNames.Aces, StringComparison.OrdinalIgnoreCase)
                    || string.Equals(row.Name, Constants.SystemTableNames.Queries, StringComparison.OrdinalIgnoreCase)
                    || string.Equals(row.Name, Constants.SystemTableNames.Relationships, StringComparison.OrdinalIgnoreCase))
                {
                    await this.RequireExclusiveCoreMapsAsync(row, msys, cancellationToken).ConfigureAwait(false);
                    return true;
                }

                return false;
            }
        }

        return false;
    }

    private async ValueTask RequireExclusiveCoreMapsAsync(CatalogRow table, TableDef msys, CancellationToken cancellationToken)
    {
        int budget = tableDefs.MaxLogicalBytes;
        List<CatalogRow> rows = await catalogRows.GetCatalogRowsAsync(msys, cancellationToken, Math.Max(1, budget / format.TDef.BlockEnd)).ConfigureAwait(false);
        await catalogRows.ValidateTableReferencesAsync(rows, cancellationToken).ConfigureAwait(false);
        byte[] target = await tableDefs.ReadTDefBytesAsync(table.TDefPage, cancellationToken).ConfigureAwait(false)
            ?? throw UnprovenMap(table, "the definition is unreadable");
        uint owned = Ru32(target, format.TDef.UsedPages);
        uint free = Ru32(target, format.TDef.FreePages);
        if (owned == free)
        {
            throw UnprovenMap(table, "the owned and free-space maps alias each other");
        }

        await this.RequireInlineMapAsync(table, owned, cancellationToken).ConfigureAwait(false);
        await this.RequireInlineMapAsync(table, free, cancellationToken).ConfigureAwait(false);
        var roots = new HashSet<long> { 2 };
        foreach (CatalogRow row in rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (row.ObjectType == Constants.SystemObjects.UserTableType
                || (row.ObjectType == Constants.SystemObjects.LinkedOdbcType && row.Id > 0))
            {
                _ = roots.Add(row.TDefPage);
            }
        }

        foreach (long root in roots)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] definition = root == table.TDefPage
                ? target
                : await tableDefs.ReadTDefBytesAsync(root, cancellationToken).ConfigureAwait(false)
                    ?? throw UnprovenMap(table, "a local definition is unreadable");
            if (definition.Length > budget)
            {
                throw UnprovenMap(table, "the aggregate definition budget is exceeded");
            }

            budget -= definition.Length;
            TDefImage? image = TDefCodec.Parse(format, definition);
            if (image is null || image.Issues != TDefParseIssues.None)
            {
                throw UnprovenMap(table, "a local definition has uncertain map references");
            }

            // Inspect opaque bytes too: native column-owned LVAL map references
            // are not fully modeled. An incidental match refuses conservatively.
            for (int offset = 0; offset <= definition.Length - 4; offset++)
            {
                if ((offset & 4095) == 0)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                }

                uint reference = Ru32(definition, offset);
                if ((reference == owned || reference == free)
                    && (root != table.TDefPage
                        || (offset != format.TDef.UsedPages && offset != format.TDef.FreePages)))
                {
                    throw UnprovenMap(table, "another table, index or opaque descriptor may reference the same map row");
                }
            }
        }
    }

    private async ValueTask RequireInlineMapAsync(CatalogRow table, uint reference, CancellationToken cancellationToken)
    {
        int pageNumber = checked((int)(reference >> 8));
        int rowIndex = checked((int)(reference & 0xFF));
        if (pageNumber <= 1 || pageNumber >= pages.PageCount)
        {
            throw UnprovenMap(table, "a map pointer is outside the local map storage");
        }

        byte[] map = await pages.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            if (map[0] != Constants.PageTypes.Data
                || !UsageMap.TryGetRowBound(map, format.DataPage, format.PageSize, rowIndex, out RowBound bound)
                || bound.RowSize < 6
                || map[bound.RowStart] != Constants.UsageMap.InlineMapType)
            {
                throw UnprovenMap(table, "a map is not a complete INLINE row");
            }

            int rowCount = Ru16(map, format.DataPage.NumRows);
            var starts = new HashSet<int>();
            for (int index = 0; index < rowCount; index++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (!UsageMap.TryGetRowBound(map, format.DataPage, format.PageSize, index, out RowBound row)
                    || !starts.Add(row.RowStart))
                {
                    throw UnprovenMap(table, "the map page has ambiguous or overlapping row storage");
                }
            }

            var mapped = new List<long>();
            if (!await UsageMap.TryEnumeratePagesAsync(map, bound, format.PageSize, pages.PageCount, 1, true, pages.ReadPageAsync, PageBuffers.Return, mapped, cancellationToken).ConfigureAwait(false))
            {
                throw UnprovenMap(table, "the INLINE bitmap is invalid");
            }
        }
        finally
        {
            PageBuffers.Return(map);
        }
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
