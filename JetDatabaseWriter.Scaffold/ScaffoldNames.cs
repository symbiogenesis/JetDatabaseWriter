namespace JetDatabaseWriter.Scaffold;

using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Globalization;
using JetDatabaseWriter.Models;
using Microsoft.CodeAnalysis.CSharp;

/// <summary>
/// Allocates distinct class and file names and validates C# namespace syntax.
/// Object and record member names remain reserved; framework references are globally qualified.
/// </summary>
internal static class ScaffoldNames
{
    /// <summary>Names that conflict with compiler-generated record members.</summary>
    internal static readonly FrozenSet<string> ReservedTypeNames = EntityEmitter.ReservedMemberNames;

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

}
