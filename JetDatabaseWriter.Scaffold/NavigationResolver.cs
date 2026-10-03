namespace JetDatabaseWriter.Scaffold;

using System;
using System.Collections.Generic;
using System.Linq;
using JetDatabaseWriter.Models;

/// <summary>
/// Maps the database's foreign-key relationships onto per-table navigation
/// properties: a parent reference on each child table and a child collection on
/// each parent table. Relationships that touch a table not being scaffolded (for
/// example a system table) are ignored.
/// </summary>
internal static class NavigationResolver
{
    /// <summary>
    /// Builds the navigation properties for every scaffolded table, with the class names
    /// <see cref="ScaffoldNames.AllocateClassNames"/> gives the tables when their columns
    /// are not known.
    /// </summary>
    /// <param name="tables">The user tables being scaffolded.</param>
    /// <param name="relationships">The database relationships.</param>
    /// <returns>A map from table name to its navigation properties.</returns>
    public static Dictionary<string, List<ScaffoldNavigation>> Resolve(
        IReadOnlyList<string> tables,
        IReadOnlyList<RelationshipMetadata> relationships)
        => Resolve(
            ScaffoldNames.AllocateClassNames([.. tables.Select(table => (table, (IReadOnlyList<ColumnMetadata>)[]))]),
            relationships);

    /// <summary>
    /// Builds the navigation properties for the tables in <paramref name="classNames"/>.
    /// A relationship is skipped unless both of its tables are there, so a navigation
    /// never names a class that is not generated.
    /// </summary>
    /// <param name="classNames">The class name of each table being generated, keyed by table name.</param>
    /// <param name="relationships">The database relationships.</param>
    /// <returns>A map from table name to its navigation properties.</returns>
    public static Dictionary<string, List<ScaffoldNavigation>> Resolve(
        IReadOnlyDictionary<string, string> classNames,
        IReadOnlyList<RelationshipMetadata> relationships)
    {
        var byTable = new Dictionary<string, List<ScaffoldNavigation>>(StringComparer.OrdinalIgnoreCase);

        foreach (RelationshipMetadata relationship in relationships)
        {
            if (!classNames.TryGetValue(relationship.ForeignTable, out string? childClass)
                || !classNames.TryGetValue(relationship.PrimaryTable, out string? parentClass))
            {
                continue;
            }

            // Child -> parent reference, named after the FK column (stripped of its
            // "Id" suffix, EF-style) so multiple FKs to the same parent stay distinct.
            Add(byTable, relationship.ForeignTable, new ScaffoldNavigation(
                IsCollection: false,
                TargetClassName: parentClass,
                PreferredName: ReferenceName(relationship.ForeignColumns, parentClass)));

            // Parent -> child collection. Access tables are usually already plural,
            // so the child class name is used as-is when it already ends in "s";
            // otherwise it is pluralized (Category -> Categories).
            bool alreadyPlural = childClass.Length > 0 && childClass[^1] is 's' or 'S';
            string collectionName = alreadyPlural ? childClass : NameCleaner.Pluralize(childClass);
            Add(byTable, relationship.PrimaryTable, new ScaffoldNavigation(
                IsCollection: true,
                TargetClassName: childClass,
                PreferredName: collectionName));
        }

        return byTable;
    }

    private static void Add(Dictionary<string, List<ScaffoldNavigation>> map, string table, ScaffoldNavigation navigation)
    {
        if (!map.TryGetValue(table, out List<ScaffoldNavigation>? list))
        {
            list = [];
            map[table] = list;
        }

        list.Add(navigation);
    }

    private static string ReferenceName(IReadOnlyList<string> foreignColumns, string parentClass)
    {
        if (foreignColumns.Count == 1)
        {
            string column = foreignColumns[0];
            if (column.Length > 2 && column.EndsWith("ID", StringComparison.OrdinalIgnoreCase))
            {
                string head = NameCleaner.ToPropertyName(column[..^2]);
                if (head.Length > 0 && !string.Equals(head, "Unknown", StringComparison.Ordinal))
                {
                    return head;
                }
            }
        }

        return parentClass;
    }
}
