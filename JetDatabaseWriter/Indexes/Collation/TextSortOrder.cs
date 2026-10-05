namespace JetDatabaseWriter.Indexes.Collation;

/// <summary>Access text sort order and its optional Jet4/ACE version.</summary>
/// <param name="Value">The sort-order identifier.</param>
/// <param name="Version">The sort-order version.</param>
/// <param name="HasVersion">Whether the descriptor has a version slot.</param>
internal readonly record struct TextSortOrder(ushort Value, byte Version, bool HasVersion)
{
    /// <summary>Gets whether an encoder supports this sort order.</summary>
    internal bool IsSupported => this.Value == 0 || (this.Value == 0x0409 && (!this.HasVersion || this.Version <= 1));
}
