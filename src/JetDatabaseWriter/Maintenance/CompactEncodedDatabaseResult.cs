namespace JetDatabaseWriter.Maintenance;

using JetDatabaseWriter.Enums;

/// <summary>Private codec output compared page by page after independent reopening.</summary>
/// <param name="Provider">The unchanged native page provider.</param>
/// <param name="Pages">Complete staging and output pages compared.</param>
/// <param name="ReadBytes">Cumulative requested read bytes, including header binding and comparison.</param>
/// <param name="WrittenBytes">Cumulative requested output bytes.</param>
/// <remarks>Codec equality establishes neither database correctness, complete incoming ownership nor durable replacement.</remarks>
internal sealed record CompactEncodedDatabaseResult(AccessEncryptionFormat Provider, long Pages, long ReadBytes, long WrittenBytes)
{
    /// <summary>Gets a value indicating whether a complete database has been accepted; always false.</summary>
    public bool DatabaseValidated { get; }

    /// <summary>Gets a value indicating whether pinned storage may be reclaimed; always false.</summary>
    public bool ReclamationAuthorized { get; }

    /// <summary>Gets a value indicating whether output may replace the original; always false.</summary>
    public bool PublicationAuthorized { get; }
}
