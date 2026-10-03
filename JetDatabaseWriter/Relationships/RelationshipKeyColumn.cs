namespace JetDatabaseWriter.Relationships;

/// <summary>
/// A column of a table that an <c>MSysRelationships</c> row names as a key
/// column (<c>szColumn</c> on the child side, <c>szReferencedColumn</c> on
/// the parent side).
/// </summary>
/// <param name="RelationshipName">The relationship name.</param>
/// <param name="ColumnName">The key column name.</param>
internal readonly record struct RelationshipKeyColumn(string RelationshipName, string ColumnName);
