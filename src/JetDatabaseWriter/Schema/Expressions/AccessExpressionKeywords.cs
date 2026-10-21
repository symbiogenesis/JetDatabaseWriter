namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;

/// <summary>
/// The words an Access expression reads as operators or values rather than as
/// names: the word operators (<c>And</c>, <c>Mod</c>, <c>Like</c>, ...) and the
/// literals <c>Null</c>, <c>True</c>, <c>False</c>, <c>Yes</c>, <c>No</c>,
/// <c>On</c> and <c>Off</c>. A bare word in this set is never a field
/// reference; <c>[Yes]</c> in brackets still is. The expression parser and
/// <see cref="ExpressionFieldReferences"/> share the set, so they agree on
/// which bare words name fields.
/// </summary>
internal static class AccessExpressionKeywords
{
    private static readonly HashSet<string> Words = new(StringComparer.OrdinalIgnoreCase)
    {
        "AND",
        "OR",
        "XOR",
        "EQV",
        "IMP",
        "MOD",
        "LIKE",
        "BETWEEN",
        "IN",
        "IS",
        "NOT",
        "NULL",
        "TRUE",
        "FALSE",
        "YES",
        "NO",
        "ON",
        "OFF",
    };

    /// <summary>Returns whether <paramref name="word"/> is an Access keyword (any case).</summary>
    /// <param name="word">The bare word.</param>
    /// <returns><see langword="true"/> for a word operator or a literal word.</returns>
    internal static bool IsKeyword(string word) => Words.Contains(word);

    /// <summary>
    /// Returns whether <paramref name="word"/> is a keyword that is a value
    /// (<c>Null</c>, <c>True</c>, <c>False</c>, <c>Yes</c>, <c>No</c>, <c>On</c>,
    /// <c>Off</c>) rather than an operator, so an operator, not an operand,
    /// follows it.
    /// </summary>
    /// <param name="word">The keyword.</param>
    /// <returns><see langword="true"/> for a literal word.</returns>
    internal static bool IsValueWord(string word)
        => word.ToUpperInvariant() is "NULL" or "TRUE" or "FALSE" or "YES" or "NO" or "ON" or "OFF";
}
