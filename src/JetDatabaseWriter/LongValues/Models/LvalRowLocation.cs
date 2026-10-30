namespace JetDatabaseWriter.LongValues.Models;

using System;
using JetDatabaseWriter.Pages.Paging;

internal readonly record struct LvalRowLocation(byte[] Page, int Start, int Size, string? Error) : IDisposable
{
    /// <summary>Gets the read lease retained through the awaited payload write, when the locator uses cached frames.</summary>
    internal PageLease? Lease { get; init; }

    public bool Failed => this.Error is not null;

    /// <summary>Releases the located image after its payload has been consumed.</summary>
    public void Dispose() => this.Lease?.Dispose();
}
