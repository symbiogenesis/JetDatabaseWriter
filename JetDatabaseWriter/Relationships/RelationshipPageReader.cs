namespace JetDatabaseWriter.Relationships;

using System.Threading;
using System.Threading.Tasks;

internal static class RelationshipPageReader
{
    public static async ValueTask<byte[]> ReadOwnedAsync(
        AccessWriter writer,
        long pageNumber,
        CancellationToken cancellationToken)
    {
        byte[] page = await writer.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            return (byte[])page.Clone();
        }
        finally
        {
            AccessBase.ReturnPage(page);
        }
    }
}
