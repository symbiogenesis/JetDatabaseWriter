namespace JetDatabaseWriter.Relationships;

using JetDatabaseWriter.Pages.Models;

/// <summary>
/// Snapshot of one live <c>MSysRelationships</c> row in catalog column order.
/// </summary>
/// <param name="Location">The location.</param>
/// <param name="SzRelationship">The size relationship.</param>
/// <param name="SzObject">The size object.</param>
/// <param name="SzReferencedObject">The size referenced object.</param>
/// <param name="SzColumn">The size column.</param>
/// <param name="SzReferencedColumn">The size referenced column.</param>
/// <param name="IColumn">The zero-based relationship column ordinal.</param>
/// <param name="CColumn">The relationship column count.</param>
/// <param name="Grbit">The grbit.</param>
/// <param name="RowValues">The row values.</param>
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
