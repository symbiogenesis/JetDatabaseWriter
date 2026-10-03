namespace JetDatabaseWriter.Mapping;

using System.Reflection;

/// <summary>One property of an <see cref="EntityMap"/> and the column it binds to.</summary>
/// <param name="property">The mapped property.</param>
/// <param name="columnName">The column the property binds to.</param>
/// <param name="isExplicit">Whether the column name came from a <c>[Column("...")]</c> attribute.</param>
internal sealed class EntityProperty(PropertyInfo property, string columnName, bool isExplicit)
{
    /// <summary>Gets the mapped property.</summary>
    public PropertyInfo Property { get; } = property;

    /// <summary>Gets the column the property binds to.</summary>
    public string ColumnName { get; } = columnName;

    /// <summary>Gets a value indicating whether the column name came from a <c>[Column("...")]</c> attribute.</summary>
    public bool IsExplicit { get; } = isExplicit;
}
