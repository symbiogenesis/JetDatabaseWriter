namespace JetDatabaseWriter.Schema;

using System;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text;
using JetDatabaseWriter.Infrastructure;

/// <summary>
/// The rules Microsoft Access applies to the name of a table, column, index
/// or relationship: 1 to 64 characters, not only white space, no leading
/// space, and none of <c>. ! ` [ ]</c> or a control character (U+0000 to
/// U+001F, and U+007F, which Jackcess also rejects). Trailing spaces are
/// allowed. The writer checks only the names a caller introduces. Names an
/// existing file already holds, such as Access's own <c>.rB</c> foreign-key
/// index names, are carried through schema rewrites unchecked, so those
/// objects can still be read, altered, renamed and dropped.
/// <see cref="ThrowIfNotStorable"/> adds the database's own limit: a Jet3
/// name must be in its code page.
/// </summary>
internal static class AccessObjectName
{
    /// <summary>The longest object name Access allows, in characters.</summary>
    internal const int MaxLength = 64;

    private const string Rules =
        "Access object names are 1 to 64 characters long, cannot start with a space or be only white space, and cannot contain . ! ` [ ] or control characters.";

    /// <summary>
    /// Returns why <paramref name="name"/> is not a valid Access object name,
    /// or <see langword="null"/> when it is. The rules are checked in this
    /// order: empty, only white space, a leading space, the length, then each
    /// character.
    /// </summary>
    /// <param name="name">The name to check.</param>
    /// <returns>The reason, phrased to follow "is not valid: ", or <see langword="null"/>.</returns>
    internal static string? FindViolation(string name)
    {
        if (name.Length == 0)
        {
            return "it is empty";
        }

        if (IsAllWhiteSpace(name))
        {
            return "it contains only white space";
        }

        if (name[0] == ' ')
        {
            return "it starts with a space";
        }

        if (name.Length > MaxLength)
        {
            return $"it is {name.Length} characters long; the limit is {MaxLength}";
        }

        foreach (char c in name)
        {
            if (c is '.' or '!' or '`' or '[' or ']')
            {
                return $"it contains '{c}'";
            }

            if (IsControl(c))
            {
                return $"it contains the control character U+{((int)c).ToString("X4", CultureInfo.InvariantCulture)}";
            }
        }

        return null;
    }

    /// <summary>
    /// Validates a new object name passed as an argument of its own, such as
    /// a table name or the new name of a rename.
    /// </summary>
    /// <param name="name">The new name.</param>
    /// <param name="paramName">The public parameter that carries the name.</param>
    /// <param name="kind">The kind of object, for the message: "table", "column" or "relationship".</param>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="name"/> is empty or breaks one of the rules.</exception>
    internal static void ThrowIfInvalid([NotNull] string? name, string paramName, string kind)
    {
        Guard.NotNullOrEmpty(name, paramName);
        if (FindViolation(name) is { } reason)
        {
            throw new ArgumentException(Describe(name, kind, position: null, reason), paramName);
        }
    }

    /// <summary>
    /// Validates the name of a definition the caller passes, such as a
    /// <see cref="JetDatabaseWriter.Models.ColumnDefinition"/> or an element of a list of them.
    /// A missing name throws <see cref="ArgumentException"/>, not
    /// <see cref="ArgumentNullException"/>, because the argument itself is present.
    /// </summary>
    /// <param name="name">The definition's name.</param>
    /// <param name="paramName">The public parameter that carries the definition.</param>
    /// <param name="kind">The kind of object, for the message: "column" or "index".</param>
    /// <param name="position">The definition's position in its list, or <see langword="null"/> for a single definition.</param>
    /// <exception cref="ArgumentException"><paramref name="name"/> is <see langword="null"/>, empty or breaks one of the rules.</exception>
    internal static void ThrowIfInvalidMember(string? name, string paramName, string kind, int? position = null)
    {
        if (string.IsNullOrEmpty(name))
        {
            throw new ArgumentException(
                position is { } index ? $"The {kind} at position {index} has no name." : $"The {kind} has no name.",
                paramName);
        }

        if (FindViolation(name) is { } reason)
        {
            throw new ArgumentException(Describe(name, kind, position, reason), paramName);
        }
    }

    /// <summary>
    /// Refuses a new name that <paramref name="db"/> cannot store as given. A
    /// Jet3 database stores names in its code page, where a character outside
    /// it would be stored as a best-fit match or <c>?</c>, so the stored name
    /// would no longer match the caller's (<see cref="DatabaseFile.DescribeUnstorableCharacter"/>).
    /// Jet4 and ACE store any name.
    /// </summary>
    /// <param name="db">The database the name is written to.</param>
    /// <param name="name">The name, already checked by <see cref="ThrowIfInvalid"/> or <see cref="ThrowIfInvalidMember"/>.</param>
    /// <param name="paramName">The public parameter that carries the name.</param>
    /// <param name="kind">The kind of object, for the message: "table", "column", "index" or "relationship".</param>
    /// <param name="position">The definition's position in its list, or <see langword="null"/>.</param>
    /// <exception cref="ArgumentException"><paramref name="name"/> holds a character the database's code page does not have.</exception>
    internal static void ThrowIfNotStorable(DatabaseFile db, string name, string paramName, string kind, int? position = null)
    {
        if (db.DescribeUnstorableCharacter(name) is { } character)
        {
            throw new ArgumentException(db.UnstorableTextMessage($"The {kind} name '{Display(name)}'{At(position)}", character), paramName);
        }
    }

    private static string Describe(string name, string kind, int? position, string reason)
        => $"The {kind} name '{Display(name)}'{At(position)} is not valid: {reason}. {Rules}";

    private static string At(int? position) => position is { } index ? $" at position {index}" : string.Empty;

    /// <summary>Returns <paramref name="name"/> with each control character written as <c>\uXXXX</c>.</summary>
    /// <param name="name">The name.</param>
    /// <returns>The printable name.</returns>
    private static string Display(string name)
    {
        StringBuilder? escaped = null;
        for (int i = 0; i < name.Length; i++)
        {
            char c = name[i];
            if (IsControl(c))
            {
                escaped ??= new StringBuilder(name.Length + 8).Append(name, 0, i);
                escaped.Append("\\u").Append(((int)c).ToString("X4", CultureInfo.InvariantCulture));
            }
            else
            {
                escaped?.Append(c);
            }
        }

        return escaped?.ToString() ?? name;
    }

    private static bool IsAllWhiteSpace(string name)
    {
        foreach (char c in name)
        {
            if (!char.IsWhiteSpace(c))
            {
                return false;
            }
        }

        return true;
    }

    private static bool IsControl(char c) => c is <= '\u001F' or '\u007F';
}
