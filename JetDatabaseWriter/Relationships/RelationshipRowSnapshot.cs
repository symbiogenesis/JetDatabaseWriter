namespace JetDatabaseWriter.Relationships;

using JetDatabaseWriter.Pages.Models;

/// <summary>
/// Snapshot of one live <c>MSysRelationships</c> row in catalog column order.
/// </summary>
internal sealed record RelationshipRowSnapshot(
    RowLocation Location,
    string SzRelationship,
    string SzObject,
    string SzReferencedObject,
    string SzColumn,
    string SzReferencedColumn,
    int IColumn,
    int CColumn,
    int Grbit,
    object[] RowValues);
