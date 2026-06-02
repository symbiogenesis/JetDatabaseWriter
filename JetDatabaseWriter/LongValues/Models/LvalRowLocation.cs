namespace JetDatabaseWriter.LongValues.Models;

internal readonly record struct LvalRowLocation(byte[] Page, int Start, int Size, string? Error)
{
    public bool Failed => this.Error is not null;
}
