namespace JetDatabaseWriter.Pages.Paging;

using System;

/// <summary>Maintains derived state from plaintext pager writes.</summary>
internal interface IPageWriteObserver
{
    /// <summary>Receives a new page image.</summary>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="before">The previous image, or empty for an appended page.</param>
    /// <param name="after">The new image, valid during this call.</param>
    public void OnPageWritten(long pageNumber, ReadOnlySpan<byte> before, ReadOnlySpan<byte> after);

    /// <summary>Discards derived state after rollback, truncation or failure.</summary>
    public void OnInvalidateAll();
}
