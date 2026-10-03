namespace JetDatabaseWriter.Relationships;

using System.Collections.Generic;

/// <summary>
/// The relationship state of one table, captured by
/// <see cref="RelationshipManager.CaptureForRewriteAsync"/> before a
/// copy-and-swap schema rewrite moves the table to a new TDEF page.
/// </summary>
/// <param name="TableName">The rewritten table.</param>
/// <param name="TDefPage">The table's TDEF page before the rewrite.</param>
/// <param name="FkEntries">The table's FK logical-index entries, in TDEF order. Captured on every format; on Jet4 / ACE they are re-emitted on the rebuilt table, while on Jet3, which has no FK-entry emitter, they only identify the partner entries to remove.</param>
/// <param name="KeyColumns">The table's columns that <c>MSysRelationships</c> names as relationship key columns.</param>
internal sealed record RelationshipRewriteState(
    string TableName,
    long TDefPage,
    IReadOnlyList<FkLogicalIndexSnapshot> FkEntries,
    IReadOnlyList<RelationshipKeyColumn> KeyColumns)
{
    /// <summary>Gets a value indicating whether the table takes part in no relationship.</summary>
    public bool IsEmpty => this.FkEntries.Count == 0 && this.KeyColumns.Count == 0;
}
