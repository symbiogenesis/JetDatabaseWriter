namespace JetDatabaseWriter.Relationships;

using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Pages.Paging;

internal static class RelationshipPageReader
{
    public static async ValueTask<byte[]> ReadOwnedAsync(
        IPageSource pageSource,
        long pageNumber,
        CancellationToken cancellationToken)
    {
        byte[] page = await pageSource.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            return (byte[])page.Clone();
        }
        finally
        {
            PageBuffers.Return(page);
        }
    }
}
