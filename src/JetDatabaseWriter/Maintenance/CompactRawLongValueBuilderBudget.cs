namespace JetDatabaseWriter.Maintenance;

/// <summary>Limits for staging one complete raw long value into private LVAL pages.</summary>
internal sealed record CompactRawLongValueBuilderBudget
{
    /// <summary>Gets the maximum copied external rows, including rows beyond the declared payload.</summary>
    public long MaxRows { get; init; } = 65_536;

    /// <summary>Gets the maximum appended LVAL pages.</summary>
    public long MaxPages { get; init; } = 65_536;

    /// <summary>Gets the maximum appended destination bytes.</summary>
    public long MaxOutputBytes { get; init; } = 256L * 1024 * 1024;

    /// <summary>Gets the maximum complete column and external-row bytes, including trailing storage.</summary>
    public long MaxRawBytes { get; init; } = 64L * 1024 * 1024;

    /// <summary>Gets the maximum admitted builder storage, held simultaneously with discovery storage.</summary>
    /// <remarks>Caller streams, runtime overhead and shared-pool retention are separate from this admission limit.</remarks>
    public long MaxWorkingBytes { get; init; } = 128 * 1024;
}
