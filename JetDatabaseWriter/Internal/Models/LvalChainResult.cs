namespace JetDatabaseWriter.Internal.Models;

// Result type for the internal LVAL chain reader.
internal sealed record LvalChainResult
{
    private LvalChainResult(byte[]? data, string? error)
    {
        Data = data;
        Error = error;
    }

    public byte[]? Data { get; }

    public string? Error { get; }

    public static LvalChainResult Success(byte[] data) => new(data, null);

    public static LvalChainResult Failure(string error) => new(null, error);
}
