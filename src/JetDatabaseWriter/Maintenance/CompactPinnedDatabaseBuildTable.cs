namespace JetDatabaseWriter.Maintenance;

using System.Collections.Generic;
using JetDatabaseWriter.Schema.Models;

/// <summary>Immutable original evidence for a local table included in a private database build.</summary>
/// <param name="Owner">The original pinned definition identity.</param>
/// <param name="Definition">The complete independent original definition.</param>
/// <param name="Indexes">Independent replayable original stored entries in physical descriptor order.</param>
/// <param name="HydratedColumns">Optional independently verified calculated ResultType projections.</param>
/// <param name="ActiveIndexOrdinal">Optional ordinary covering index used with native map evidence to corroborate active rows; unassigned rows remain pinned and reported.</param>
/// <remarks>Evidence remains caller-owned and immutable through construction and independent reopened comparison.</remarks>
internal sealed record CompactPinnedDatabaseBuildTable(
    long Owner,
    TDefImage Definition,
    IReadOnlyList<CompactPinnedTableIndexSource> Indexes,
    IReadOnlyList<ColumnInfo>? HydratedColumns = null,
    int? ActiveIndexOrdinal = null);
