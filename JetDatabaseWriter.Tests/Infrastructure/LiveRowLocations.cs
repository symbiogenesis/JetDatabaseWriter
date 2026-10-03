namespace JetDatabaseWriter.Tests.Infrastructure;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Pages.Models;

/// <summary>
/// Collects the location of every live row of a table, for tests that inspect raw
/// row locations; writer code pairs rows with locations through
/// <see cref="JetDatabaseWriter.Tables.TableSnapshotReader.ReadRowsAsync"/>.
/// </summary>
internal static class LiveRowLocations
{
    /// <summary>
    /// Returns the location of every live row on the data pages owned by
    /// <paramref name="tdefPage"/>.
    /// </summary>
    /// <param name="db">The database file to read.</param>
    /// <param name="tdefPage">The table's TDEF page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The live row locations, in page and row order.</returns>
    public static async ValueTask<List<RowLocation>> GetLiveRowLocationsAsync(this DatabaseFile db, long tdefPage, CancellationToken cancellationToken)
    {
        var result = new List<RowLocation>();
        await db.ForEachLiveTableRowAsync(
            tdefPage,
            (row, _) =>
            {
                result.Add(row.Location);
                return new ValueTask<bool>(true);
            },
            cancellationToken).ConfigureAwait(false);

        return result;
    }
}
