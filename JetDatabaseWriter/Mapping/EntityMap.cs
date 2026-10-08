namespace JetDatabaseWriter.Mapping;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using JetDatabaseWriter.Infrastructure;

/// <summary>
/// The one mapping model that decides which column each property of a POCO binds to and
/// which table the type names. Every typed path goes through it: typed inserts, the
/// typed row readers (<c>Rows&lt;T&gt;</c>, <c>ReadTableAsync&lt;T&gt;</c>, index reads), the
/// LINQ provider's index inference and index-ordered reads, and <c>Include</c> eager loading.
/// </summary>
/// <remarks>
/// <para>
/// A public instance property with a setter and no index parameters is mapped unless it
/// carries <see cref="NotMappedAttribute"/>. It binds to the column named by
/// <see cref="ColumnAttribute.Name"/> when that is set, and otherwise to a column with the
/// property's own name. Column names compare case-insensitively, as Access does.
/// </para>
/// <para>
/// When an explicit <c>[Column("X")]</c> and a property literally named <c>X</c> claim the
/// same column, the explicit attribute wins and the other property is left unmapped. Two
/// explicit attributes naming the same column are a configuration error and throw
/// <see cref="InvalidOperationException"/>.
/// </para>
/// <para>
/// <see cref="TableName"/> is <see cref="TableAttribute.Name"/> when the type carries one,
/// and otherwise the type name.
/// </para>
/// </remarks>
internal sealed class EntityMap
{
    private static readonly ConcurrentDictionary<Type, EntityMap> Cache = new();

    private readonly Dictionary<string, EntityProperty> byColumn;
    private readonly Dictionary<string, EntityProperty> byPropertyName;

    private EntityMap([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] Type type)
    {
        this.TableName = type.GetCustomAttribute<TableAttribute>(inherit: true) is { Name.Length: > 0 } table
            ? table.Name
            : type.Name;

        this.byColumn = new Dictionary<string, EntityProperty>(StringComparer.OrdinalIgnoreCase);
        foreach (PropertyInfo property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (!property.CanWrite
                || property.GetIndexParameters().Length != 0
                || property.IsDefined(typeof(NotMappedAttribute), inherit: true))
            {
                continue;
            }

            string? explicitName = property.GetCustomAttribute<ColumnAttribute>(inherit: true)?.Name;
            bool isExplicit = !string.IsNullOrEmpty(explicitName);
            string columnName = isExplicit ? explicitName! : property.Name;

            if (this.byColumn.TryGetValue(columnName, out EntityProperty? existing) && existing.IsExplicit)
            {
                if (isExplicit)
                {
                    throw new InvalidOperationException(
                        $"Properties '{existing.Property.Name}' and '{property.Name}' on type '{type}' both map to column '{columnName}'. "
                        + "Give each property a distinct [Column(\"...\")] name or mark one [NotMapped].");
                }

                // An explicit [Column] claim beats a property that only matches by name.
                continue;
            }

            this.byColumn[columnName] = new EntityProperty(property, columnName, isExplicit);
        }

        this.byPropertyName = new Dictionary<string, EntityProperty>(this.byColumn.Count, StringComparer.Ordinal);
        var properties = new List<EntityProperty>(this.byColumn.Count);
        foreach (EntityProperty mapped in this.byColumn.Values)
        {
            this.byPropertyName[mapped.Property.Name] = mapped;
            properties.Add(mapped);
        }

        this.Properties = properties;
    }

    /// <summary>Gets the table name the type binds to: its <c>[Table]</c> name, or the type name.</summary>
    public string TableName { get; }

    /// <summary>Gets the mapped properties, one per column.</summary>
    public IReadOnlyList<EntityProperty> Properties { get; }

    /// <summary>Returns the cached mapping model for <paramref name="type"/>.</summary>
    /// <param name="type">The POCO type.</param>
    /// <returns>The mapping model.</returns>
    /// <exception cref="InvalidOperationException">Two properties of <paramref name="type"/> name the same column through <c>[Column]</c>.</exception>
    public static EntityMap For([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] Type type)
    {
        Guard.NotNull(type, nameof(type));
        return Cache.TryGetValue(type, out EntityMap? map) ? map : Cache.GetOrAdd(type, new EntityMap(type));
    }

    /// <summary>Finds the property bound to <paramref name="columnName"/> (case-insensitive).</summary>
    /// <param name="columnName">The column name.</param>
    /// <returns>The bound property, or <see langword="null"/> when no property maps to the column.</returns>
    public EntityProperty? FindByColumn(string columnName) =>
        this.byColumn.TryGetValue(columnName, out EntityProperty? property) ? property : null;

    /// <summary>
    /// Finds the mapping for the property a member expression accesses, so a LINQ
    /// expression over the entity can be translated to its column.
    /// </summary>
    /// <param name="member">The accessed member.</param>
    /// <returns>The mapped property, or <see langword="null"/> when the member is not a mapped column property.</returns>
    public EntityProperty? FindByMember(MemberInfo member) =>
        member is PropertyInfo && this.byPropertyName.TryGetValue(member.Name, out EntityProperty? property) ? property : null;
}
