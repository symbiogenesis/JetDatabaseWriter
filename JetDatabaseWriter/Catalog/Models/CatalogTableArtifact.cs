namespace JetDatabaseWriter.Catalog.Models;

using System.Collections.Generic;
using JetDatabaseWriter.Models;

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
    bool RegisterConstraints = true);
