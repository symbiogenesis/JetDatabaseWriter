namespace JetDatabaseWriter.Maintenance;

/// <summary>Complete original-prefix comparison with reconstructed definition pointers and optional independently validated native global maps.</summary>
/// <param name="PinnedPages">Every physical original page covered, including header and opaque storage.</param>
/// <param name="Definitions">The compared complete original definition chains.</param>
/// <param name="DefinitionPages">The original physical definition pages whose logical images were independently checked.</param>
/// <param name="PatchedBytes">Original byte positions whose expected values change through known reference replacement.</param>
/// <param name="ReadBytes">Cumulative admitted source/private read requests, including retries.</param>
internal sealed record CompactPinnedPrefixValidatorResult(long PinnedPages, int Definitions, long DefinitionPages, long PatchedBytes, long ReadBytes)
{
    /// <summary>Gets a value indicating whether the complete destination database has been validated; always false.</summary>
    public bool DatabaseValidated { get; }

    /// <summary>Gets a value indicating whether incoming references authorize reclamation; always false.</summary>
    public bool OwnershipProven { get; }
}
