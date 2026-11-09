namespace JetDatabaseWriter.EntityFrameworkCore.Query.Internal;

using Microsoft.EntityFrameworkCore.Metadata;

/// <summary>One scalar property and its requested query ordering.</summary>
/// <param name="Property">The ordered model property.</param>
/// <param name="Descending">Whether values are ordered from greatest to least.</param>
public readonly record struct JetOrdering(IProperty Property, bool Descending);
