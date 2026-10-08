namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;
using System.Text;

/// <summary>
/// Finds and renames the references to one field of one table in the text of
/// an Access expression: a calculated column's <c>Expression</c>, or a
/// column's <c>ValidationRule</c> or <c>DefaultValue</c>. It only scans the
/// text and never parses it, so it also works on a stored expression the
/// engine cannot parse, and a rename copies every character it does not
/// rename verbatim.
/// </summary>
/// <remarks>
/// <para>
/// The scan skips string literals (<c>"..."</c> or <c>'...'</c>, where a
/// doubled quote is a quote), <c>#date#</c> and <c>{guid {...}}</c> literals,
/// numbers, and <c>&amp;H</c>/<c>&amp;O</c> radix literals where an operand
/// is expected (after an operand, <c>&amp;</c> joins text, so in
/// <c>[A]&amp;HFF</c> the name <c>HFF</c> is a field). A name chain is
/// <c>[...]</c> or bare segments joined by <c>.</c> or <c>!</c>, including
/// whitespace around the separator. The scan conservatively preserves foreign
/// chains even when the evaluator refuses their stored syntax. A one-segment
/// chain is a candidate, and so is the second
/// segment of a two-segment chain whose first segment is the table's own name
/// (<c>[T].[Price]</c>, <c>T.Price</c>). Other chains, such as
/// <c>Forms![F]![Price]</c> or <c>[Other].[Price]</c>, never are.
/// </para>
/// <para>
/// A bracketed candidate is a field reference, as the evaluator reads it. A
/// bare candidate is not when it is a keyword (<see cref="AccessExpressionKeywords"/>),
/// a built-in constant such as <c>vbTrue</c>, a function call (followed by
/// <c>(</c>) or a name with a <c>$</c> suffix. Names compare without regard to
/// case, as the evaluator resolves them. An unterminated string, <c>[</c> or
/// <c>{</c> ends the scan, and the rest of the text is left as it is.
/// </para>
/// </remarks>
internal static class ExpressionFieldReferences
{
    /// <summary>
    /// Returns whether <paramref name="expression"/> refers to the field
    /// <paramref name="fieldName"/> of <paramref name="tableName"/>.
    /// </summary>
    /// <param name="expression">The expression text, or <see langword="null"/>.</param>
    /// <param name="fieldName">The field name.</param>
    /// <param name="tableName">The name of the table the expression belongs to, which may qualify a reference.</param>
    /// <returns><see langword="true"/> when the expression names the field.</returns>
    internal static bool References(string? expression, string fieldName, string tableName)
        => !string.IsNullOrEmpty(expression) && FindReferences(expression, fieldName, tableName).Count > 0;

    /// <summary>
    /// Rewrites every reference to the field <paramref name="oldName"/> of
    /// <paramref name="tableName"/> as <c>[<paramref name="newName"/>]</c>,
    /// keeping a table qualifier, and copies the rest of the text verbatim.
    /// The new name is always bracketed, so it cannot read as a keyword or a
    /// function.
    /// </summary>
    /// <param name="expression">The expression text, or <see langword="null"/>.</param>
    /// <param name="oldName">The field's current name.</param>
    /// <param name="newName">The field's new name.</param>
    /// <param name="tableName">The name of the table the expression belongs to, which may qualify a reference.</param>
    /// <returns>The rewritten text, or the same instance when nothing refers to the field.</returns>
    /// <exception cref="ArgumentException">The expression refers to the field and <paramref name="newName"/> contains <c>]</c>, which a <c>[field]</c> reference cannot hold.</exception>
    internal static string? Rename(string? expression, string oldName, string newName, string tableName)
    {
        if (string.IsNullOrEmpty(expression))
        {
            return expression;
        }

        List<(int Start, int End)> references = FindReferences(expression, oldName, tableName);
        if (references.Count == 0)
        {
            return expression;
        }

        if (newName.Contains(']', StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"The name '{newName}' contains ']', so it cannot be written as a [field] reference in expression '{expression}'.",
                nameof(newName));
        }

        var builder = new StringBuilder(expression.Length + (references.Count * (newName.Length + 2)));
        int copied = 0;
        foreach ((int start, int end) in references)
        {
            builder.Append(expression, copied, start - copied).Append('[').Append(newName).Append(']');
            copied = end;
        }

        return builder.Append(expression, copied, expression.Length - copied).ToString();
    }

    private static List<(int Start, int End)> FindReferences(string text, string fieldName, string tableName)
    {
        var found = new List<(int Start, int End)>();
        var segments = new List<Segment>(2);

        // Whether an operand is expected next, which decides whether '&'
        // starts a radix literal or joins text, as in the expression parser.
        bool operandExpected = true;
        int index = 0;
        while (index < text.Length)
        {
            char current = text[index];
            if (char.IsWhiteSpace(current))
            {
                index++;
            }
            else if (current is '"' or '\'')
            {
                index = SkipQuoted(text, index, current);
                if (index < 0)
                {
                    break;
                }

                operandExpected = false;
            }
            else if (current == '#')
            {
                // A #date# literal; a lone '#' is just a character.
                int close = text.IndexOf('#', index + 1);
                operandExpected = close < 0;
                index = close < 0 ? index + 1 : close + 1;
            }
            else if (current == '{')
            {
                // A {guid {...}} literal, as a GUID DefaultValue is stored.
                index = SkipBraces(text, index);
                if (index < 0)
                {
                    break;
                }

                operandExpected = false;
            }
            else if (char.IsDigit(current) || (current == '.' && index + 1 < text.Length && char.IsDigit(text[index + 1])))
            {
                index = SkipNumber(text, index);
                operandExpected = false;
            }
            else if (current == '&' && operandExpected && TrySkipRadixLiteral(text, ref index))
            {
                operandExpected = false;
            }
            else if (current == '[' || IsNameStart(current))
            {
                segments.Clear();
                index = ReadNameChain(text, index, segments);
                if (index < 0)
                {
                    break;
                }

                if (TryGetCandidate(segments, tableName, out Segment candidate)
                    && IsReferenceTo(candidate, fieldName, isCall: NextNonSpace(text, index) == '('))
                {
                    found.Add((candidate.Start, candidate.End));
                }

                // An operator word such as And or Mod expects an operand after
                // it; a name, a function and a literal word such as Null end one.
                Segment last = segments[^1];
                operandExpected = segments.Count == 1
                    && !last.Bracketed
                    && AccessExpressionKeywords.IsKeyword(last.Name)
                    && !AccessExpressionKeywords.IsValueWord(last.Name);
            }
            else
            {
                // An operator, '(' or ','; ')' ends an operand.
                operandExpected = current != ')';
                index++;
            }
        }

        return found;
    }

    /// <summary>
    /// Reads the name chain starting at <paramref name="index"/>: <c>[...]</c>
    /// or bare segments joined by <c>.</c> or <c>!</c>.
    /// </summary>
    /// <param name="text">The expression text.</param>
    /// <param name="index">The index of the chain's first character.</param>
    /// <param name="segments">Receives the chain's segments.</param>
    /// <returns>The index after the chain, or -1 when a <c>[</c> is not closed.</returns>
    private static int ReadNameChain(string text, int index, List<Segment> segments)
    {
        while (true)
        {
            int start = index;
            if (text[index] == '[')
            {
                int close = text.IndexOf(']', index + 1);
                if (close < 0)
                {
                    return -1;
                }

                segments.Add(new Segment(start, close + 1, text.Substring(index + 1, close - index - 1), Bracketed: true, HasDollarSuffix: false));
                index = close + 1;
            }
            else
            {
                index++;
                while (index < text.Length && IsNamePart(text[index]))
                {
                    index++;
                }

                int nameEnd = index;
                bool dollar = index < text.Length && text[index] == '$';
                if (dollar)
                {
                    index++;
                }

                segments.Add(new Segment(start, index, text[start..nameEnd], Bracketed: false, HasDollarSuffix: dollar));
            }

            int separator = SkipWhiteSpace(text, index);
            if (separator < text.Length && text[separator] is '.' or '!')
            {
                int next = SkipWhiteSpace(text, separator + 1);
                if (next < text.Length && (text[next] == '[' || IsNameStart(text[next])))
                {
                    index = next;
                    continue;
                }
            }

            return index;
        }
    }

    private static bool TryGetCandidate(List<Segment> segments, string tableName, out Segment candidate)
    {
        if (segments.Count == 1)
        {
            candidate = segments[0];
            return true;
        }

        if (segments.Count == 2 && string.Equals(segments[0].Name, tableName, StringComparison.OrdinalIgnoreCase))
        {
            candidate = segments[1];
            return true;
        }

        candidate = default;
        return false;
    }

    private static bool IsReferenceTo(Segment candidate, string fieldName, bool isCall)
    {
        if (!string.Equals(candidate.Name, fieldName, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        // The evaluator reads a bracketed name as a field whatever it spells;
        // a bare one only when it is not a keyword, constant or function.
        return candidate.Bracketed
            || (!candidate.HasDollarSuffix
                && !isCall
                && !AccessExpressionKeywords.IsKeyword(candidate.Name)
                && !CalculatedExpressionCoercion.TryGetBuiltinConstant(candidate.Name, out _));
    }

    /// <summary>Returns the index after the string literal opening at <paramref name="index"/>, or -1 when it is not closed.</summary>
    /// <param name="text">The expression text.</param>
    /// <param name="index">The index of the opening quote.</param>
    /// <param name="quote">The quote character.</param>
    /// <returns>The index after the closing quote, or -1.</returns>
    private static int SkipQuoted(string text, int index, char quote)
    {
        for (int i = index + 1; i < text.Length; i++)
        {
            if (text[i] != quote)
            {
                continue;
            }

            if (i + 1 < text.Length && text[i + 1] == quote)
            {
                i++;
                continue;
            }

            return i + 1;
        }

        return -1;
    }

    /// <summary>Returns the index after the brace group opening at <paramref name="index"/>, or -1 when it is not closed.</summary>
    /// <param name="text">The expression text.</param>
    /// <param name="index">The index of the opening brace.</param>
    /// <returns>The index after the matching closing brace, or -1.</returns>
    private static int SkipBraces(string text, int index)
    {
        int depth = 0;
        for (int i = index; i < text.Length; i++)
        {
            if (text[i] == '{')
            {
                depth++;
            }
            else if (text[i] == '}' && --depth == 0)
            {
                return i + 1;
            }
        }

        return -1;
    }

    private static int SkipNumber(string text, int index)
    {
        while (index < text.Length && (char.IsDigit(text[index]) || text[index] == '.'))
        {
            index++;
        }

        if (index < text.Length && text[index] is 'E' or 'e')
        {
            int exponent = index + 1;
            if (exponent < text.Length && text[exponent] is '+' or '-')
            {
                exponent++;
            }

            if (exponent < text.Length && char.IsDigit(text[exponent]))
            {
                index = exponent;
                while (index < text.Length && char.IsDigit(text[index]))
                {
                    index++;
                }
            }
        }

        // Letters run on after the digits belong to the literal, not to a name.
        while (index < text.Length && IsNamePart(text[index]))
        {
            index++;
        }

        return index;
    }

    /// <summary>
    /// Skips a VBA radix literal at <paramref name="index"/> (<c>&amp;H</c> and
    /// hex digits, <c>&amp;O</c> or <c>&amp;</c> and octal digits, with an
    /// optional <c>&amp;</c> suffix), as the expression parser reads one where
    /// an operand is expected.
    /// </summary>
    /// <param name="text">The expression text.</param>
    /// <param name="index">The index of the <c>&amp;</c>; advanced past the literal when one is read.</param>
    /// <returns><see langword="false"/> when no radix literal starts here.</returns>
    private static bool TrySkipRadixLiteral(string text, ref int index)
    {
        int digits = index + 1;
        if (digits >= text.Length)
        {
            return false;
        }

        bool hex;
        char marker = text[digits];
        if (marker is 'H' or 'h' or 'O' or 'o')
        {
            hex = marker is 'H' or 'h';
            digits++;
        }
        else if (marker is >= '0' and <= '7')
        {
            hex = false;
        }
        else
        {
            return false;
        }

        int end = digits;
        while (end < text.Length && (hex ? IsHexDigit(text[end]) : text[end] is >= '0' and <= '7'))
        {
            end++;
        }

        if (end == digits)
        {
            return false;
        }

        index = end < text.Length && text[end] == '&' ? end + 1 : end;
        return true;
    }

    private static char NextNonSpace(string text, int index)
    {
        index = SkipWhiteSpace(text, index);
        return index < text.Length ? text[index] : '\0';
    }

    private static int SkipWhiteSpace(string text, int index)
    {
        while (index < text.Length && char.IsWhiteSpace(text[index]))
        {
            index++;
        }

        return index;
    }

    private static bool IsNameStart(char c) => char.IsLetter(c) || c == '_';

    private static bool IsNamePart(char c) => char.IsLetterOrDigit(c) || c == '_';

    private static bool IsHexDigit(char c) => c is (>= '0' and <= '9') or (>= 'A' and <= 'F') or (>= 'a' and <= 'f');

    /// <summary>One segment of a name chain.</summary>
    /// <param name="Start">The index of the segment's first character (its <c>[</c> when bracketed).</param>
    /// <param name="End">The index after the segment (after its <c>]</c> or <c>$</c>).</param>
    /// <param name="Name">The name, without brackets or <c>$</c>.</param>
    /// <param name="Bracketed">Whether the segment is <c>[...]</c>.</param>
    /// <param name="HasDollarSuffix">Whether a bare segment ends in <c>$</c>.</param>
    private readonly record struct Segment(int Start, int End, string Name, bool Bracketed, bool HasDollarSuffix);
}
