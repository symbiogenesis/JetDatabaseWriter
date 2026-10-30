namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;

/// <summary>Native ANSI-89 LIKE matching with pinned casing and expansion boundaries.</summary>
/// <remarks>
/// A write observes cancellation around expression evaluation; the evaluator has
/// no cancellation-token parameter, so matching also bounds its input and work.
/// No host regular-expression engine or NLS tables participate.
/// </remarks>
internal static class AccessLikeMatcher
{
    /// <summary>The combined source and pattern UTF-16 input budget.</summary>
    private const int MaxInputUnits = 1024 * 1024;

    /// <summary>The deterministic matching and pattern-validation operation budget.</summary>
    private const int MaxSteps = 16 * 1024 * 1024;

    internal static object Evaluate(CalculatedFunctionInvocation function)
    {
        object source = function.Arg(0);
        object pattern = function.Arg(1);
        if (IsNull(source) || IsNull(pattern))
        {
            return DBNull.Value;
        }

        string sourceText = ToText(source);
        string patternText = ToText(pattern);
        try
        {
            return Match(sourceText, patternText);
        }
        catch (ArgumentException) when (function.Context.IsCalculatingColumn)
        {
            return new CalculatedError(93);
        }
    }

    /// <summary>Matches a complete string, retaining UTF-16 and digraph boundaries.</summary>
    /// <param name="source">The non-null source.</param>
    /// <param name="pattern">The non-null native pattern.</param>
    /// <returns>Whether the complete source matches.</returns>
    /// <exception cref="ArgumentException">A reached pattern region is invalid.</exception>
    /// <exception cref="JetLimitationException">The input or matching work exceeds its budget.</exception>
    internal static bool Match(string source, string pattern)
    {
        if ((long)source.Length + pattern.Length > MaxInputUnits)
        {
            throw BudgetExceeded();
        }

        Unit[] text = Expand(source);
        Unit[] mask = Expand(pattern);
        int[] closings = FindClosings(mask);
        int sourceIndex = 0;
        int patternIndex = 0;
        int starPattern = -1;
        int starSource = 0;
        int steps = 0;
        bool validatedTail = false;
        while (sourceIndex < text.Length)
        {
            Tick(ref steps);
            if (patternIndex < mask.Length && Kind(mask[patternIndex].Value) == '*')
            {
                if (!validatedTail)
                {
                    ValidateTail(mask, closings, patternIndex + 1, ref steps);
                    validatedTail = true;
                }

                starPattern = ++patternIndex;
                starSource = sourceIndex;
                continue;
            }

            int consumed = 0;
            int nextPattern = patternIndex + 1;
            bool matched = false;
            if (patternIndex < mask.Length)
            {
                switch (Kind(mask[patternIndex].Value))
                {
                    case '?':
                        matched = true;
                        consumed = text[sourceIndex].ExpansionStart ? 2 : 1;
                        break;
                    case '#':
                        matched = text[sourceIndex].Value is (>= '0' and <= '9') or (>= '\uFF10' and <= '\uFF19');
                        consumed = 1;
                        break;
                    case '[':
                        matched = MatchClass(text, sourceIndex, mask, closings, patternIndex, out consumed, out nextPattern, ref steps);
                        break;
                    default:
                        matched = text[sourceIndex].Value == mask[patternIndex].Value;
                        consumed = 1;
                        break;
                }
            }

            if (matched)
            {
                sourceIndex += consumed;
                patternIndex = nextPattern;
            }
            else if (starPattern >= 0)
            {
                // A star may stop between the two parts of an expanded digraph;
                // '?' instead consumes the complete original character.
                sourceIndex = ++starSource;
                patternIndex = starPattern;
            }
            else
            {
                return false;
            }
        }

        // With no source character left, native LIKE skips only stars and [];
        // an unreached malformed bracket therefore returns False, not error93.
        while (patternIndex < mask.Length)
        {
            Tick(ref steps);
            if (Kind(mask[patternIndex].Value) == '*')
            {
                patternIndex++;
            }
            else if (Kind(mask[patternIndex].Value) == '[' && patternIndex + 1 < mask.Length && Kind(mask[patternIndex + 1].Value) == ']')
            {
                patternIndex += 2;
            }
            else
            {
                return false;
            }
        }

        return true;
    }

    private static bool MatchClass(Unit[] text, int sourceIndex, Unit[] pattern, int[] closings, int start, out int consumed, out int nextPattern, ref int steps)
    {
        int close = closings[start];
        if (close < 0)
        {
            throw InvalidPattern();
        }

        consumed = 0;
        nextPattern = close + 1;
        int index = start + 1;
        if (index == close)
        {
            return true;
        }

        bool negate = Kind(pattern[index].Value) == '!';
        if (negate && ++index == close)
        {
            consumed = 1;
            return text[sourceIndex].Value == pattern[index - 1].Value;
        }

        while (index < close)
        {
            Tick(ref steps);
            Unit lower = pattern[index];
            int next = index + (lower.ExpansionStart ? 2 : 1);
            int length = ExpansionMatches(text, sourceIndex, pattern, index) ? 2 : 0;
            if (length == 0 && next + 1 < close && Kind(pattern[next].Value) == '-')
            {
                Unit upper = pattern[next + 1];
                length = ExpansionMatches(text, sourceIndex, pattern, next + 1) ? 2 : 0;
                if (length == 0)
                {
                    if (lower.Original > upper.Original)
                    {
                        throw InvalidPattern();
                    }

                    if (text[sourceIndex].Value >= lower.Original && text[sourceIndex].Value <= upper.Original)
                    {
                        length = 1;
                    }
                }

                next += upper.ExpansionStart ? 3 : 2;
            }
            else if (length == 0 && text[sourceIndex].Value == lower.Original)
            {
                length = 1;
            }

            if (length != 0)
            {
                consumed = length;
                return !negate;
            }

            index = next;
        }

        consumed = 1;
        return negate;
    }

    private static bool ExpansionMatches(Unit[] text, int sourceIndex, Unit[] pattern, int patternIndex)
        => pattern[patternIndex].ExpansionStart && sourceIndex + 1 < text.Length && text[sourceIndex].Value == pattern[patternIndex].Value && text[sourceIndex + 1].Value == pattern[patternIndex + 1].Value;

    private static void ValidateTail(Unit[] pattern, int[] closings, int start, ref int steps)
    {
        // Once a star is reached, ACE validates the whole remaining pattern.
        // Its compiled path requires strictly ascending endpoints at every
        // interior hyphen, including overlapping chains such as [a-z-a].
        for (int index = start; index < pattern.Length; index++)
        {
            Tick(ref steps);
            if (Kind(pattern[index].Value) != '[')
            {
                continue;
            }

            int close = closings[index];
            if (close < 0)
            {
                throw InvalidPattern();
            }

            int first = index + 1;
            if (first < close && Kind(pattern[first].Value) == '!')
            {
                first++;
            }

            for (int member = first + 1; member + 1 < close; member++)
            {
                Tick(ref steps);
                if (Kind(pattern[member].Value) == '-' && pattern[member - 1].Original >= pattern[member + 1].Original)
                {
                    throw InvalidPattern();
                }
            }

            index = close;
        }
    }

    private static Unit[] Expand(string value)
    {
        string folded = CalculatedExpressionTextFunctions.ChangeCase(value, upper: false);
        var units = new List<Unit>(folded.Length);
        foreach (char character in folded)
        {
            string? expansion = character switch { '\u00E6' => "ae", '\u00FE' => "th", '\u00DF' => "ss", '\u0153' => "oe", _ => null };
            if (expansion is null)
            {
                units.Add(new(character, character, false));
            }
            else
            {
                units.Add(new(expansion[0], character, true));
                units.Add(new(expansion[1], character, false));
            }
        }

        return units.ToArray();
    }

    private static int[] FindClosings(Unit[] pattern)
    {
        int[] closings = new int[pattern.Length];
        int next = -1;
        for (int index = pattern.Length - 1; index >= 0; index--)
        {
            closings[index] = next;
            if (Kind(pattern[index].Value) == ']')
            {
                next = index;
            }
        }

        return closings;
    }

    private static char Kind(char value)
        => value switch { '\uFF0A' => '*', '\uFF1F' => '?', '\uFF03' => '#', '\uFF3B' => '[', '\uFF3D' => ']', '\uFF01' => '!', '\uFF0D' => '-', _ => value };

    private static void Tick(ref int steps)
    {
        if (++steps > MaxSteps)
        {
            throw BudgetExceeded();
        }
    }

    private static ArgumentException InvalidPattern() => new("The reached LIKE pattern is invalid.", "pattern");

    private static JetLimitationException BudgetExceeded()
        => new(JetErrorCode.ValueTooLarge, "LIKE matching exceeds its input or operation budget.", new JetErrorInfo { Reason = "LIKE matching budget exceeded" });

    private readonly record struct Unit(char Value, char Original, bool ExpansionStart);
}
