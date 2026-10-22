namespace JetDatabaseWriter.Maintenance;

/// <summary>Cumulative admission for native encoding and independently reopened decrypted comparison.</summary>
internal sealed class CompactEncodedDatabaseBudget
{
    /// <summary>Gets maximum complete staging and encoded pages.</summary>
    public long MaxPages { get; init; } = 0x1000000;

    /// <summary>Gets maximum encoded output length.</summary>
    public long MaxOutputBytes { get; init; } = 64L * 1024 * 1024 * 1024;

    /// <summary>Gets cumulative requested read bytes across construction and comparison, including partial retries.</summary>
    public long MaxReadBytes { get; init; } = 256L * 1024 * 1024 * 1024;

    /// <summary>Gets cumulative requested write bytes, including failed writes.</summary>
    public long MaxWriteBytes { get; init; } = 64L * 1024 * 1024 * 1024;

    /// <summary>Gets conservative owned header, page-buffer and codec transient allocation admission.</summary>
    public long MaxWorkingBytes { get; init; } = 2L * 1024 * 1024;

    /// <summary>Gets the maximum individual native password hashing iteration count.</summary>
    public int MaxEncryptionSpinCount { get; init; } = 1_000_000;

    /// <summary>Gets aggregate password hashing iterations across construction and independent reopening.</summary>
    public long MaxTotalEncryptionSpinCount { get; init; } = 2_000_000;

    /// <summary>Gets maximum native encryption descriptor bytes.</summary>
    public int MaxEncryptionInfoBytes { get; init; } = 4096;
}
