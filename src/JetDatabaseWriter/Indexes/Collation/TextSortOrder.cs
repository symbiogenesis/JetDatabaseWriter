namespace JetDatabaseWriter.Indexes.Collation;

using System;
using JetDatabaseWriter.Enums;

/// <summary>Access text sort order and its optional Jet4/ACE version.</summary>
/// <param name="Value">The sort-order identifier.</param>
/// <param name="Version">The sort-order version.</param>
/// <param name="HasVersion">Whether the descriptor has a version slot.</param>
internal readonly record struct TextSortOrder(ushort Value, byte Version, bool HasVersion)
{
    /// <summary>Gets a value indicating whether an encoder supports this sort order.</summary>
    internal bool IsSupported => this.Value == 0 || (this.Value == 0x0409 && (!this.HasVersion || this.Version <= 1));

    /// <summary>Gets the supported encoder family, retaining unknown IDs separately.</summary>
    internal TextCollation? Collation => this switch
    {
        { IsSupported: false } => null,
        { Value: 0 } or { HasVersion: true, Version: 0 } => TextCollation.GeneralLegacy,
        { HasVersion: true } => TextCollation.General,
        _ => TextCollation.General97,
    };

    /// <summary>Resolves an explicitly requested supported encoder family.</summary>
    /// <param name="collation">The public collation choice.</param>
    /// <param name="paramName">The public parameter being validated.</param>
    /// <returns>The native descriptor fields.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The choice is not a supported collation.</exception>
    internal static TextSortOrder FromCollation(TextCollation collation, string paramName)
        => collation switch
        {
            TextCollation.General97 => new(0x0409, 0, false),
            TextCollation.GeneralLegacy => new(0x0409, 0, true),
            TextCollation.General => new(0x0409, 1, true),
            _ => throw new ArgumentOutOfRangeException(paramName, collation, "The text collation is not supported."),
        };
}
