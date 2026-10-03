namespace JetDatabaseWriter.Relationships;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Pages.Models;
using static JetDatabaseWriter.Schema.JetTypeInfo;

internal sealed class RelationshipChildRowLocator(DatabaseFile db)
{
    public async ValueTask<List<(RowLocation Loc, TPayload Payload)>?> TrySeekChildLocationsAsync<TPayload>(
        CatalogEntry childEntry,
        ChildSeekIndex childSeek,
        IEnumerable<(object?[] OldPk, TPayload Payload)> requests,
        CancellationToken cancellationToken)
    {
        var pendingByLocation = new Dictionary<long, (long DataPage, int RowIndex, TPayload Payload)>();
        var cursor = new IndexCursor(
            (page, token) => RelationshipPageReader.ReadOwnedAsync(db, page, token),
            db.PageSizeBytes);

        foreach ((object?[] oldPrimaryKey, TPayload? payload) in requests)
        {
            byte[]? encoded = IndexHelpers.TryEncodeChildSeekKey(childSeek, oldPrimaryKey);
            if (encoded == null)
            {
                return null;
            }

            List<(long DataPage, int RowIndex)> hits = await cursor.FindRowLocationsAsync(
                childSeek.RootPage,
                encoded,
                cancellationToken).ConfigureAwait(false);

            foreach ((long dataPage, int rowIndex) in hits)
            {
                long key = (dataPage << 16) | (uint)rowIndex;
                if (!pendingByLocation.ContainsKey(key))
                {
                    pendingByLocation[key] = (dataPage, rowIndex, payload);
                }
            }
        }

        var result = new List<(RowLocation Loc, TPayload Payload)>(pendingByLocation.Count);
        if (pendingByLocation.Count == 0)
        {
            return result;
        }

        var byPage = new Dictionary<long, HashSet<int>>();
        foreach ((long dataPage, int rowIndex, _) in pendingByLocation.Values)
        {
            if (!byPage.TryGetValue(dataPage, out HashSet<int>? rowIndexes))
            {
                rowIndexes = [];
                byPage[dataPage] = rowIndexes;
            }

            _ = rowIndexes.Add(rowIndex);
        }

        foreach (KeyValuePair<long, HashSet<int>> pageRows in byPage)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] page = await db.ReadPageAsync(pageRows.Key, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != Constants.PageTypes.Data || Ri32(page, db.DataPage.TDefOff) != childEntry.TDefPage)
                {
                    return null;
                }

                foreach (RowBound rowBound in db.ComputeRowDirectory(page))
                {
                    if (!pageRows.Value.Contains(rowBound.RowIndex))
                    {
                        continue;
                    }

                    long key = (pageRows.Key << 16) | (uint)rowBound.RowIndex;
                    if (!pendingByLocation.TryGetValue(key, out (long DataPage, int RowIndex, TPayload Payload) entry))
                    {
                        continue;
                    }

                    var location = new RowLocation(pageRows.Key, rowBound.RowIndex, rowBound.RowStart, rowBound.RowSize);
                    if (rowBound.IsOverflowPointer)
                    {
                        // The index names an overflow row's header; the row's
                        // bytes are where the header points.
                        if (await db.TryResolveOverflowRowAsync(page, rowBound, db.ReadPageAsync, DatabaseFile.ReturnPage, cancellationToken).ConfigureAwait(false) is not { } target)
                        {
                            continue;
                        }

                        DatabaseFile.ReturnPage(target.Page);
                        location = new RowLocation(pageRows.Key, rowBound.RowIndex, target.Bound.RowStart, target.Bound.RowSize)
                        {
                            DataPageNumber = target.PageNumber,
                            DataRowIndex = target.RowIndex,
                        };
                    }

                    result.Add((location, entry.Payload));
                }
            }
            finally
            {
                DatabaseFile.ReturnPage(page);
            }
        }

        if (result.Count != pendingByLocation.Count)
        {
            return null;
        }

        return result;
    }
}
