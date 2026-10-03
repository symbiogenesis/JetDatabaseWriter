namespace JetDatabaseWriter.Scaffold;

using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using JetDatabaseWriter.Models;
using Microsoft.CodeAnalysis.CSharp;

/// <summary>
/// Gives every scaffolded table a class name of its own, and checks the namespace, before
/// any file is written. The generated classes share one namespace, so a class must not
/// take a name the generated code itself uses (the attributes, the property types,
/// <c>System</c>), two tables must not get the same class or file name, and no segment of
/// the namespace may hide a type the generated code names.
/// </summary>
internal static class ScaffoldNames
{
    /// <summary>
    /// The types the generated code names without a namespace, besides the generated
    /// classes and the generic collection types: the <c>[Column]</c> and <c>[Table]</c>
    /// attribute classes and the property types the emitter spells out. A class, or a
    /// namespace segment, with one of these names would capture them.
    /// </summary>
    internal static readonly FrozenSet<string> GeneratedCodeTypeNames = new[]
    {
        "ColumnAttribute",
        "TableAttribute",
        "DateTime",
        "DateTimeOffset",
        "TimeSpan",
        "DateOnly",
        "TimeOnly",
        "Guid",
        "Hyperlink",
    }.ToFrozenSet(StringComparer.Ordinal);

    /// <summary>
    /// Names a generated class cannot take, because the generated code names them and a
    /// class in the same namespace would capture them: the <c>System</c> and
    /// <c>JetDatabaseWriter</c> namespaces, the <see cref="GeneratedCodeTypeNames"/>, and
    /// the object and record member names, which a record cannot share with its type.
    /// </summary>
    internal static readonly FrozenSet<string> ReservedTypeNames = new[]
    {
        "System",
        "JetDatabaseWriter",
    }.Concat(GeneratedCodeTypeNames).Concat(EntityEmitter.ReservedMemberNames).ToFrozenSet(StringComparer.Ordinal);

    /// <summary>
    /// Allocates a distinct class name to each table. A table whose cleaned name
    /// (<see cref="NameCleaner.ToClassName"/>) is its own name, ignoring case, and is not
    /// reserved claims it first, in list order. Every other table then takes its cleaned
    /// name, with <c>Entity</c> appended when that name is reserved, and a numeric suffix
    /// (<c>2</c>, <c>3</c>, ...) while the name is reserved or already taken. Names are
    /// compared ignoring case, so no two classes share a file on a case-insensitive file
    /// system.
    /// </summary>
    /// <param name="tables">Each table's name and columns, in the order the tables are scaffolded.</param>
    /// <returns>A map from table name, ignoring case, to class name.</returns>
    internal static Dictionary<string, string> AllocateClassNames(IReadOnlyList<(string Table, IReadOnlyList<ColumnMetadata> Columns)> tables)
    {
        var reserved = new HashSet<string>(ReservedTypeNames, StringComparer.Ordinal);
        foreach ((_, IReadOnlyList<ColumnMetadata> columns) in tables)
        {
            reserved.UnionWith(EntityEmitter.ReferencedTypeNames(columns));
        }

        var used = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var classNames = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach ((string table, _) in tables)
        {
            string cleaned = NameCleaner.ToClassName(table);
            if (string.Equals(cleaned, table, StringComparison.OrdinalIgnoreCase) && !reserved.Contains(cleaned) && !classNames.ContainsKey(table))
            {
                classNames[table] = Claim(cleaned, reserved, used);
            }
        }

        foreach ((string table, _) in tables)
        {
            if (classNames.ContainsKey(table))
            {
                continue;
            }

            string cleaned = NameCleaner.ToClassName(table);
            classNames[table] = Claim(reserved.Contains(cleaned) ? cleaned + "Entity" : cleaned, reserved, used);
        }

        return classNames;
    }

    private static string Claim(string name, HashSet<string> reserved, HashSet<string> used)
    {
        string candidate = name;
        for (int suffix = 2; reserved.Contains(candidate) || !used.Add(candidate); suffix++)
        {
            candidate = name + suffix.ToString(CultureInfo.InvariantCulture);
        }

        return candidate;
    }

    /// <summary>
    /// Determines whether <paramref name="ns"/> can be the namespace of the generated
    /// files: one or more <c>.</c>-separated identifiers, none of them a C# keyword.
    /// </summary>
    /// <param name="ns">The namespace.</param>
    /// <returns><see langword="true"/> when the namespace is valid.</returns>
    internal static bool IsValidNamespace(string ns)
    {
        foreach (string segment in ns.Split('.'))
        {
            if (!SyntaxFacts.IsValidIdentifier(segment) || SyntaxFacts.GetKeywordKind(segment) != SyntaxKind.None)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Finds the first segment of <paramref name="ns"/> named like a type the generated
    /// code names: one of the <see cref="GeneratedCodeTypeNames"/> or the type of a
    /// scaffolded column. The usings precede the namespace declaration, and C# looks a
    /// type name up in the generated namespace and each enclosing one before it consults
    /// them, so a segment named <c>DateTime</c> would hide <see cref="DateTime"/> from every
    /// property (CS0118), and one named <c>ColumnAttribute</c> every <c>[Column]</c>
    /// (CS0616). The comparison keeps case, as C# lookup does. <c>System</c> and
    /// <c>JetDatabaseWriter</c> segments are fine: the generated code names them only in
    /// the usings, which C# binds above the namespace, and as <c>global::System</c>.
    /// </summary>
    /// <param name="ns">A valid namespace (<see cref="IsValidNamespace"/>).</param>
    /// <param name="tables">Each scaffolded table's name and columns.</param>
    /// <returns>The first such segment, or <see langword="null"/> when there is none.</returns>
    internal static string? FindTypeHidingSegment(string ns, IReadOnlyList<(string Table, IReadOnlyList<ColumnMetadata> Columns)> tables)
    {
        var typeNames = new HashSet<string>(GeneratedCodeTypeNames, StringComparer.Ordinal);
        foreach ((_, IReadOnlyList<ColumnMetadata> columns) in tables)
        {
            typeNames.UnionWith(EntityEmitter.ReferencedTypeNames(columns));
        }

        return Array.Find(ns.Split('.'), typeNames.Contains);
    }
}
