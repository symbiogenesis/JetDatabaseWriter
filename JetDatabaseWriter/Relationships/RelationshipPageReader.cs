namespace JetDatabaseWriter.Relationships;

using System.Threading;
using System.Threading.Tasks;

internal static class RelationshipPageReader
{
    public static async ValueTask<byte[]> ReadOwnedAsync(
        DatabaseFile db,
        long pageNumber,
        CancellationToken cancellationToken)
    {
        byte[] page = await db.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            return (byte[])page.Clone();
        }
        finally
        {
            DatabaseFile.ReturnPage(page);
        }
    }
}
