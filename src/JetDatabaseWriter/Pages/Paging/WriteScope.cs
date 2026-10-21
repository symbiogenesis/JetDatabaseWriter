namespace JetDatabaseWriter.Pages.Paging;

using System;
using System.Threading.Tasks;

/// <summary>A reference-counted call write-back scope.</summary>
/// <param name="pager">The owning pager.</param>
internal sealed class WriteScope(Pager pager) : IAsyncDisposable
{
    private bool disposed;

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        if (this.disposed)
        {
            return default;
        }

        this.disposed = true;
        return pager.EndWriteScopeAsync();
    }
}
