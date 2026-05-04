namespace JetDatabaseWriter.Pages.Models;

internal sealed class PageInsertTarget
{
    public long PageNumber { get; set; }

    public byte[] Page { get; set; } = [];
}
