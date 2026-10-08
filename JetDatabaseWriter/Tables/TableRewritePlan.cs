namespace JetDatabaseWriter.Tables;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.Schema.Models;

/// <summary>A prepared replacement schema and its original storage and dependent identities.</summary>
/// <param name="TableName">The original table name.</param>
/// <param name="Entry">The original catalog identity.</param>
/// <param name="Definition">The original schema including persisted calculated result types.</param>
/// <param name="Columns">The projected schema retaining raw source descriptors.</param>
/// <param name="Indexes">The surviving ordinary indexes.</param>
/// <param name="Properties">The projected persisted property block.</param>
/// <param name="PropertyBytes">The serialized catalog property payload.</param>
/// <param name="Rows">The validated projected row values.</param>
/// <param name="Relationships">The captured relationship identities.</param>
/// <param name="MapColumnName">The column identity projection.</param>
/// <param name="ComplexColumns">Surviving complex columns by their persisted ID.</param>
/// <param name="DroppedComplexColumns">Complex children to reclaim after replacement.</param>
/// <param name="RenamedComplexColumns">Complex artifact names to update after replacement.</param>
/// <param name="AutoNumberHighWater">The original AutoNumber counter captured before replacement creation.</param>
/// <param name="ComplexHighWater">The original complex reference counter captured before replacement creation.</param>
/// <param name="Transplant">Whether the replacement retains the original TDEF identity.</param>
internal sealed record TableRewritePlan(
    string TableName,
    CatalogEntry Entry,
    TableDef Definition,
    List<ColumnDefinition> Columns,
    List<IndexDefinition> Indexes,
    ColumnPropertyBlock Properties,
    byte[]? PropertyBytes,
    List<object[]> Rows,
    RelationshipRewriteState Relationships,
    Func<string, string?> MapColumnName,
    Dictionary<int, ColumnDefinition> ComplexColumns,
    List<(string Name, int ComplexId)> DroppedComplexColumns,
    List<(string OldName, string NewName, int ComplexId)> RenamedComplexColumns,
    long AutoNumberHighWater,
    long ComplexHighWater,
    bool Transplant);
