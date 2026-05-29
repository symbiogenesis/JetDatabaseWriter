namespace JetDatabaseWriter.LongValues.Models;

/// <summary>
/// Result type for the internal LVAL chain reader.
/// </summary>
internal sealed record LvalChainResult
{
    private LvalChainResult(byte[]? data, string? error)
    {
        this.Data = data;
        this.Error = error;
    }

    public byte[]? Data { get; }

    public string? Error { get; }

    public static LvalChainResult Success(byte[] data) => new(data, null);

    public static LvalChainResult Failure(string error) => new(null, error);
}
