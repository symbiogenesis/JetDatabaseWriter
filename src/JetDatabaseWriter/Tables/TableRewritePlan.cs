namespace JetDatabaseWriter.Tables;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;

/// <summary>A prepared replacement schema and its original storage and dependent identities.</summary>
/// <param name="TableName">The original table name.</param>
/// <param name="Entry">The original catalog identity.</param>
/// <param name="Definition">The original schema including persisted calculated result types.</param>
/// <param name="SourceImage">The original immutable table-definition bytes.</param>
/// <param name="Columns">The projected schema retaining raw source descriptors.</param>
/// <param name="Indexes">The surviving ordinary indexes.</param>
/// <param name="Properties">The projected persisted property block.</param>
/// <param name="PropertyBytes">The serialized catalog property payload.</param>
/// <param name="ProjectRow">The row projection applied as source rows stream into the replacement table.</param>
/// <param name="ResolveLongValueColumns">Physical column mask selecting which external long values must be resolved during streaming copy.</param>
/// <param name="RewrittenRowCount">The validated source row count.</param>
/// <param name="AddedCalculatedEvaluator">Per-row evaluator for newly added calculated columns, or null when none were added.</param>
/// <param name="AddedComplexColumns">Projected column ordinals of newly added complex columns.</param>
/// <param name="AssignedComplexReferences">Per-row complex reference IDs to assign to null added complex slots, or null when no complex column was added.</param>
/// <param name="Relationships">The captured relationship identities.</param>
/// <param name="MapColumnName">The column identity projection.</param>
/// <param name="DroppedComplexColumns">Complex children to reclaim after replacement.</param>
/// <param name="RenamedComplexColumns">Complex artifact names to update after replacement.</param>
/// <param name="AutoNumberHighWater">The original AutoNumber counter captured before replacement creation.</param>
/// <param name="ComplexHighWater">The original complex reference counter captured before replacement creation.</param>
internal sealed record TableRewritePlan(
    string TableName,
    CatalogEntry Entry,
    TableDef Definition,
    TDefImage SourceImage,
    List<ColumnDefinition> Columns,
    List<IndexDefinition> Indexes,
    ColumnPropertyBlock Properties,
    byte[]? PropertyBytes,
    Func<object[], TableDef, object[]> ProjectRow,
    bool[] ResolveLongValueColumns,
    int RewrittenRowCount,
    ConstraintRegistry.AddedCalculatedEvaluator? AddedCalculatedEvaluator,
    IReadOnlyList<int> AddedComplexColumns,
    int?[]? AssignedComplexReferences,
    RelationshipRewriteState Relationships,
    Func<string, string?> MapColumnName,
    List<(string Name, int ComplexId)> DroppedComplexColumns,
    List<(string OldName, string NewName, int ComplexId)> RenamedComplexColumns,
    long AutoNumberHighWater,
    long ComplexHighWater);
