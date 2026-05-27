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

internal sealed class RelationshipChildRowLocator(AccessWriter writer)
{
    public async ValueTask<List<(RowLocation Loc, TPayload Payload)>?> TrySeekChildLocationsAsync<TPayload>(
        CatalogEntry childEntry,
        ChildSeekIndex childSeek,
        IEnumerable<(object?[] OldPk, TPayload Payload)> requests,
        CancellationToken cancellationToken)
    {
        var pendingByLocation = new Dictionary<long, (long DataPage, int RowIndex, TPayload Payload)>();
        var cursor = new IndexCursor(
            (page, token) => RelationshipPageReader.ReadOwnedAsync(writer, page, token),
            writer._pgSz);

        foreach ((object?[] oldPrimaryKey, var payload) in requests)
        {
            byte[]? encoded = IndexHelpers.TryEncodeChildSeekKey(childSeek, oldPrimaryKey);
            if (encoded == null)
            {
                return null;
            }

            var hits = await cursor.FindRowLocationsAsync(
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
            if (!byPage.TryGetValue(dataPage, out var rowIndexes))
            {
                rowIndexes = [];
                byPage[dataPage] = rowIndexes;
            }

            _ = rowIndexes.Add(rowIndex);
        }

        foreach (var pageRows in byPage)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] page = await writer.ReadPageAsync(pageRows.Key, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != Constants.PageTypes.Data || Ri32(page, writer._dataPage.TDefOff) != childEntry.TDefPage)
                {
                    return null;
                }

                foreach (var rowBound in writer.EnumerateLiveRowBounds(page))
                {
                    if (!pageRows.Value.Contains(rowBound.RowIndex))
                    {
                        continue;
                    }

                    long key = (pageRows.Key << 16) | (uint)rowBound.RowIndex;
                    if (pendingByLocation.TryGetValue(key, out var entry))
                    {
                        result.Add((new RowLocation(pageRows.Key, rowBound.RowIndex, rowBound.RowStart, rowBound.RowSize), entry.Payload));
                    }
                }
            }
            finally
            {
                AccessBase.ReturnPage(page);
            }
        }

        if (result.Count != pendingByLocation.Count)
        {
            return null;
        }

        return result;
    }
}
