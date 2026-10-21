namespace JetDatabaseWriter.Indexes.Models;

/// <summary>
/// Encoded lower or upper bound for an internal index range scan.
/// </summary>
/// <param name="Key">The encoded key bytes, or <see langword="null"/> for an unbounded side.</param>
/// <param name="Inclusive">Whether matching keys are included.</param>
/// <param name="IsPrefix">Whether <paramref name="Key"/> represents a leading-key prefix.</param>
internal readonly record struct EncodedIndexBound(byte[]? Key, bool Inclusive, bool IsPrefix)
{
    /// <summary>Gets an unbounded index side.</summary>
    public static EncodedIndexBound None => default;

    /// <summary>Gets a value indicating whether this side is unbounded.</summary>
    public bool IsUnbounded => this.Key == null;
}
