namespace JetDatabaseWriter.Catalog.Models;

using System.Collections.Generic;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;

/// <summary>A table the catalog-artifact writer creates: its TDEF, catalog row, ACE rows and constraints.</summary>
/// <param name="TableName">The table name.</param>
/// <param name="Columns">The column definitions.</param>
/// <param name="Indexes">The index definitions.</param>
/// <param name="CatalogFlags">The <c>MSysObjects.Flags</c> value of the catalog row.</param>
/// <param name="ReservedTdefPageNumber">A TDEF page reserved for the table, or 0 to allocate one.</param>
/// <param name="EmitLvProp">Whether the catalog row carries an <c>LvProp</c> blob.</param>
/// <param name="EmitUsageMap">Whether a usage-map page is written for the table.</param>
/// <param name="MarkSystemTableTdef">Whether the TDEF is marked as a system table's when the flags say so.</param>
/// <param name="EmitAceRows">Whether <c>MSysACEs</c> rows are written; <see langword="null"/> decides by format.</param>
/// <param name="RegisterConstraints">Whether the columns' constraints are registered for the table.</param>
internal sealed record CatalogTableArtifact(
    string TableName,
    IReadOnlyList<ColumnDefinition> Columns,
    IReadOnlyList<IndexDefinition> Indexes,
    uint CatalogFlags,
    long ReservedTdefPageNumber = 0,
    bool EmitLvProp = true,
    bool EmitUsageMap = true,
    bool MarkSystemTableTdef = true,
    bool? EmitAceRows = null,
    bool RegisterConstraints = true)
{
    /// <summary>
    /// Gets the table's persisted properties, written as its <c>LvProp</c> blob in
    /// place of the one built from <see cref="Columns"/>. A schema rewrite sets it
    /// to the original table's properties projected onto the rebuilt columns
    /// (<see cref="Schema.PersistedPropertyProjector"/>), table-level target
    /// included, so properties the writer does not model survive the rebuild.
    /// </summary>
    public ColumnPropertyBlock? PersistedProperties { get; init; }
}
