namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using JetDatabaseWriter.Exceptions;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;
using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionFunctionRegistry;
using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionLimits;

#if NETSTANDARD2_1
internal static class CalculatedExpressionTextFunctions
#else
internal static partial class CalculatedExpressionTextFunctions
#endif
{
    private const string ValNumberPrefixPattern = "^[+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][+-]?\\d+)?";
    private const string WhitespacePattern = "\\s+";
    private const long MaxTextSearchWork = 16 * 1024 * 1024;

    private static readonly Encoding AnsiEncoding = CodePagesEncodingProvider.Instance.GetEncoding(1252) ?? throw new InvalidOperationException("Windows-1252 encoding is unavailable.");

#if NETSTANDARD2_1
    private static readonly Lazy<Regex> ValNumberPrefixRegexInstance = new(static () => new Regex(ValNumberPrefixPattern, RegexOptions.CultureInvariant));
    private static readonly Lazy<Regex> WhitespaceRegexInstance = new(static () => new Regex(WhitespacePattern));
#endif

    internal static void AddFunctions(Dictionary<string, CalculatedFunctionDescriptor> functions)
    {
        AddFunction(functions, "LEN", 1, 1, static function => Length(function.Arg(0), function.Context));
        AddFunction(functions, "LEFT", 2, 2, static function => EvaluateSlice(function, fromRight: false));
        AddFunction(functions, "RIGHT", 2, 2, static function => EvaluateSlice(function, fromRight: true));
        AddFunction(functions, "MID", 2, 3, static function => EvaluateMid(function));
        AddFunction(functions, "UCASE", 1, 1, static function => EvaluateCase(function, upper: true), "UPPER");
        AddFunction(functions, "LCASE", 1, 1, static function => EvaluateCase(function, upper: false), "LOWER");
        AddFunction(functions, "TRIM", 1, 1, static function => EvaluateTrim(function, trimStart: true, trimEnd: true));
        AddFunction(functions, "LTRIM", 1, 1, static function => EvaluateTrim(function, trimStart: true, trimEnd: false));
        AddFunction(functions, "RTRIM", 1, 1, static function => EvaluateTrim(function, trimStart: false, trimEnd: true));
        AddFunction(functions, "CSTR", 1, 1, static function => ToText(function.Arg(0)));
        AddFunction(functions, "HEX", 1, 1, static function => Convert.ToInt64(function.Arg(0), CultureInfo.InvariantCulture).ToString("X", CultureInfo.InvariantCulture));
        AddFunction(functions, "OCT", 1, 1, static function => Convert.ToString(Convert.ToInt64(function.Arg(0), CultureInfo.InvariantCulture), 8) ?? string.Empty);
        AddFunction(functions, "VAL", 1, 1, static function => Val(ToText(function.Arg(0))));
        AddFunction(functions, "ASC", 1, 1, static function => EvaluateAsc(function, asciiOnly: true));
        AddFunction(functions, "ASCW", 1, 1, static function => EvaluateAsc(function, asciiOnly: false));
        AddFunction(functions, "CHR", 1, 1, static function => char.ConvertFromUtf32(checked((int)ToDecimal(function.Arg(0)))), "CHRW");
        AddFunction(functions, "STR", 1, 1, static function => EvaluateStr(function));
        AddFunction(functions, "INSTR", 2, 4, static function => EvaluateInStr(function));
        AddFunction(functions, "INSTRREV", 2, 4, static function => EvaluateInStrRev(function));
        AddFunction(functions, "REPLACE", 3, 6, static function => EvaluateReplaceText(function));
        AddFunction(functions, "SPACE", 1, 1, static function => EvaluateSpace(function));
        AddFunction(functions, "STRCOMP", 2, 3, static function => Math.Sign(string.Compare(ToText(function.Arg(0)), ToText(function.Arg(1)), CompareOptions(function.Count > 2 ? function.Arg(2) : null))));
        AddFunction(functions, "STRING", 2, 2, static function => EvaluateString(function));
        AddFunction(functions, "STRREVERSE", 1, 1, static function => EvaluateStrReverse(function));
        AddFunction(functions, "STRCONV", 2, 3, static function => IsNull(function.Arg(0)) ? DBNull.Value : StrConv(ToText(function.Arg(0)), checked((int)ToDecimal(function.Arg(1)))));
    }

    internal static string ConcatText(object left, object right)
    {
        string leftText = ToText(left);
        string rightText = ToText(right);
        EnsureGeneratedTextLength((long)leftText.Length + rightText.Length);
        return leftText + rightText;
    }

    private static object Length(object value, CalculatedExpressionEvaluationContext context) => IsNull(value) ? DBNull.Value : ToText(value, context).Length;

    internal static string ChangeCase(string text, bool upper)
        => string.Create(text.Length, (Text: text, Upper: upper), static (characters, state) =>
        {
            for (int index = 0; index < characters.Length; index++)
            {
                char character = state.Text[index];
                if (char.IsHighSurrogate(character) && index + 1 < state.Text.Length && char.IsLowSurrogate(state.Text[index + 1]))
                {
                    int scalar = char.ConvertToUtf32(character, state.Text[index + 1]);
                    int mapped = CalculatedTextCasing.Change(scalar, state.Upper) - 0x10000;
                    characters[index] = checked((char)(0xD800 + (mapped >> 10)));
                    characters[++index] = checked((char)(0xDC00 + (mapped & 0x3FF)));
                }
                else
                {
                    characters[index] = checked((char)CalculatedTextCasing.Change(character, state.Upper));
                }
            }
        });

    private static object EvaluateSlice(CalculatedFunctionInvocation function, bool fromRight)
    {
        int count = TextInteger(function.Arg(1));
        object value = function.Arg(0);
        if (NullTextArgument(function, value))
        {
            return DBNull.Value;
        }

        ValidateTextCount(count);
        string text = ToText(value, function.Context);
        count = Math.Min(count, text.Length);
        return fromRight ? text.Substring(text.Length - count, count) : text[..count];
    }

    private static object EvaluateMid(CalculatedFunctionInvocation function)
    {
        int start = TextInteger(function.Arg(1));
        int count = function.Count > 2 ? TextInteger(function.Arg(2)) : int.MaxValue / 2;
        object value = function.Arg(0);
        if (NullTextArgument(function, value))
        {
            return DBNull.Value;
        }

        if (start < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(function), "Mid requires a positive starting position.");
        }

        ValidateTextCount(count);
        string text = ToText(value, function.Context);
        int zeroBasedStart = Math.Min(start - 1, text.Length);
        return text.Substring(zeroBasedStart, Math.Min(count, text.Length - zeroBasedStart));
    }

    private static object EvaluateCase(CalculatedFunctionInvocation function, bool upper)
    {
        object value = function.Arg(0);
        return NullTextArgument(function, value) ? DBNull.Value : ChangeCase(ToText(value, function.Context), upper);
    }

    private static object EvaluateTrim(CalculatedFunctionInvocation function, bool trimStart, bool trimEnd)
    {
        object value = function.Arg(0);
        if (NullTextArgument(function, value))
        {
            return DBNull.Value;
        }

        string text = ToText(value, function.Context);
        if (trimStart && trimEnd)
        {
            return text.Trim(' ');
        }

        return trimStart ? text.TrimStart(' ') : text.TrimEnd(' ');
    }

    private static object EvaluateStr(CalculatedFunctionInvocation function)
    {
        object value = function.Arg(0);
        if (NullTextArgument(function, value))
        {
            return DBNull.Value;
        }

        object number = value switch
        {
            double or float => value,
            string when function.Context.IsCalculatingColumn => ToDouble(value),
            _ => ToDecimal(value),
        };
        string text = ToText(number, function.Context);
        bool negative = text.StartsWith('-');
        int start = negative ? 1 : 0;
        if (text.Length > start + 1 && text[start] == '0' && text[start + 1] == '.')
        {
            text = text.Remove(start, 1);
        }

        return negative ? text : " " + text;
    }

#pragma warning disable CA1859 // Native table Asc boxes Integer; the shared interpreter boxes Long.
    private static object EvaluateAsc(CalculatedFunctionInvocation function, bool asciiOnly)
    {
        int value = Asc(function.Arg(0), asciiOnly, function.Context);
        if (function.Context.IsCalculatingColumn)
        {
            return checked((short)value);
        }

        return value;
    }
#pragma warning restore CA1859

    private static object EvaluateInStr(CalculatedFunctionInvocation function)
    {
        int start = 1;
        int textIndex = 0;
        if (function.Count > 2)
        {
            start = TextInteger(function.Arg(0));
            textIndex = 1;
        }

        if (start < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(function), "InStr requires a positive starting position.");
        }

        int comparison = function.Count > 3 ? TextInteger(function.Arg(3)) : 1;
        if (comparison is not (0 or 1))
        {
            throw new ArgumentOutOfRangeException(nameof(function), "InStr comparison must be binary (0) or text (1).");
        }

        object textValue = function.Arg(textIndex);
        object searchValue = function.Arg(textIndex + 1);
        if (IsNull(textValue) || IsNull(searchValue))
        {
            return DBNull.Value;
        }

        string text = ToText(textValue, function.Context);
        string search = ToText(searchValue, function.Context);
        if (search.Length == 0)
        {
            return start;
        }

        int zeroBasedStart = Math.Min(start - 1, text.Length);
        int found = comparison == 0
            ? text.IndexOf(search, zeroBasedStart, StringComparison.Ordinal)
            : FindCollatedText(function, text, search, zeroBasedStart);
        return found < 0 ? 0 : found + 1;
    }

    private static int FindCollatedText(CalculatedFunctionInvocation function, string text, string search, int start)
    {
        if (IsSimpleLatinText(text) && IsSimpleLatinText(search))
        {
            return text.IndexOf(search, start, StringComparison.OrdinalIgnoreCase);
        }

        byte[] searchKey = function.Context.TextCollation.EncodeComparisonKey(search);
        long work = 0;
        for (int position = start; position < text.Length; position++)
        {
            for (int length = 1; length <= text.Length - position; length++)
            {
                work += length;
                if (work > MaxTextSearchWork)
                {
                    throw new JetLimitationException(JetErrorCode.ValueTooLarge, "InStr text comparison exceeds its bounded comparison-work budget.");
                }

                byte[] candidateKey = function.Context.TextCollation.EncodeComparisonKey(text.Substring(position, length));
                if (candidateKey.AsSpan().SequenceEqual(searchKey))
                {
                    return position;
                }

                if (!function.Context.TextCollation.CanExtendComparisonKey(candidateKey, searchKey))
                {
                    break;
                }
            }
        }

        return -1;
    }

    private static bool IsSimpleLatinText(string text)
    {
        foreach (char character in text)
        {
            if (character is not (>= 'A' and <= 'Z') and not (>= 'a' and <= 'z') and not (>= '0' and <= '9') and not ' ')
            {
                return false;
            }
        }

        return true;
    }

    private static object EvaluateString(CalculatedFunctionInvocation function)
    {
        int count = TextInteger(function.Arg(0));
        object value = function.Arg(1);
        if (NullTextArgument(function, value))
        {
            return DBNull.Value;
        }

        ValidateTextCount(count);
        EnsureGeneratedTextLength(count);
        char character;
        if (value is string text)
        {
            if (text.Length == 0)
            {
                throw new ArgumentException("String requires a non-empty character argument.", nameof(function));
            }

            character = text[0];
        }
        else
        {
            Span<byte> bytes = [(byte)(AccessVariantOperators.ToRoundedLong(value) & 0xFF)];
            character = AnsiEncoding.GetString(bytes)[0];
        }

        return new string(character, count);
    }

    private static bool NullTextArgument(CalculatedFunctionInvocation function, object value)
    {
        if (!IsNull(value))
        {
            return false;
        }

        if (function.Name.EndsWith('$'))
        {
            throw new InvalidCastException("A string-returning dollar function cannot accept Null.");
        }

        return true;
    }

    private static int TextInteger(object value)
        => IsNull(value) ? throw new InvalidCastException("A Text function numeric argument cannot be Null.") : AccessVariantOperators.ToRoundedLong(value);

    private static void ValidateTextCount(int count)
    {
        if (count is < 0 or > 0x3FFF_FFFF)
        {
            throw new ArgumentOutOfRangeException(nameof(count), "The Text function length is outside the native UTF-16 length range.");
        }
    }

    private static int EvaluateInStrRev(CalculatedFunctionInvocation function)
    {
        string text = ToText(function.Arg(0));
        string search = ToText(function.Arg(1));
        int start = function.Count > 2 ? checked((int)ToDecimal(function.Arg(2))) : -1;
        if (start == -1 || start > text.Length)
        {
            start = text.Length;
        }

        if (search.Length == 0)
        {
            return start;
        }

        int found = text.LastIndexOf(search, start - 1, function.Count > 3 ? CompareOptions(function.Arg(3)) : StringComparison.OrdinalIgnoreCase);
        return found < 0 ? 0 : found + 1;
    }

    private static string EvaluateReplaceText(CalculatedFunctionInvocation function)
    {
        string text = ToText(function.Arg(0));
        string search = ToText(function.Arg(1));
        string replacement = ToText(function.Arg(2));
        int start = function.Count > 3 ? Math.Max(1, checked((int)ToDecimal(function.Arg(3)))) : 1;
        int count = function.Count > 4 ? checked((int)ToDecimal(function.Arg(4))) : -1;
        StringComparison comparison = function.Count > 5 ? CompareOptions(function.Arg(5)) : StringComparison.OrdinalIgnoreCase;
        if (start > text.Length || search.Length == 0 || count == 0)
        {
            return start > text.Length ? string.Empty : text[(start - 1)..];
        }

        var builder = new StringBuilder(Math.Min(MaxGeneratedTextLength, text.Length));
        AppendGeneratedText(builder, text, 0, start - 1);
        int replacements = 0;
        int position = start - 1;
        while (position < text.Length)
        {
            int found = text.IndexOf(search, position, comparison);
            if (found < 0 || (count >= 0 && replacements >= count))
            {
                AppendGeneratedText(builder, text, position, text.Length - position);
                break;
            }

            AppendGeneratedText(builder, text, position, found - position);
            AppendGeneratedText(builder, replacement);
            position = found + search.Length;
            replacements++;
        }

        return builder.ToString();
    }

    private static string EvaluateStrReverse(CalculatedFunctionInvocation function)
    {
        char[] chars = ToText(function.Arg(0)).ToCharArray();
        EnsureGeneratedTextLength(chars.Length);
        Array.Reverse(chars);
        return new string(chars);
    }

    private static void AppendGeneratedText(StringBuilder builder, string value)
    {
        EnsureGeneratedTextLength((long)builder.Length + value.Length);
        builder.Append(value);
    }

    private static void AppendGeneratedText(StringBuilder builder, string value, int startIndex, int count)
    {
        EnsureGeneratedTextLength((long)builder.Length + count);
        builder.Append(value, startIndex, count);
    }

    private static int Asc(object value, bool asciiOnly, CalculatedExpressionEvaluationContext context)
    {
        if (IsNull(value))
        {
            throw new InvalidCastException("Asc cannot accept Null.");
        }

        string text = ToText(value, context);
        if (string.IsNullOrEmpty(text))
        {
            throw new ArgumentException("Calculated-column Asc/AscW requires a non-empty string.");
        }

        int codePoint = text[0];
        if (asciiOnly)
        {
            Span<byte> bytes = stackalloc byte[1];
            AnsiEncoding.GetBytes(text.AsSpan(0, 1), bytes);
            return bytes[0];
        }

        return codePoint;
    }

    private static double Val(string text)
    {
        string compact = WhitespaceRegex().Replace(text, string.Empty);
        Match match = ValNumberPrefixRegex().Match(compact);
        return match.Success && double.TryParse(match.Value, NumberStyles.Float, CultureInfo.InvariantCulture, out double parsed)
            ? parsed
            : 0d;
    }

    private static StringComparison CompareOptions(object? value)
    {
        if (IsNull(value))
        {
            return StringComparison.OrdinalIgnoreCase;
        }

        return checked((int)ToDecimal(value)) == 0 ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase;
    }

    private static string EvaluateSpace(CalculatedFunctionInvocation function)
    {
        int count = TextInteger(function.Arg(0));
        ValidateTextCount(count);
        EnsureGeneratedTextLength(count);
        return new string(' ', count);
    }

    private static string StrConv(string text, int conversion)
    {
        int caseConversion = conversion & 0x03;
        int characterConversion = conversion & ~0x03;
        string result = caseConversion switch
        {
            0 => text,
            1 => text.ToUpperInvariant(),
            2 => CultureInfo.InvariantCulture.TextInfo.ToLower(text),
            3 => CultureInfo.InvariantCulture.TextInfo.ToTitleCase(CultureInfo.InvariantCulture.TextInfo.ToLower(text)),
            _ => text,
        };

        if (characterConversion is not 0 and not 64)
        {
            throw new ArgumentException($"Calculated-column StrConv character conversion '{characterConversion}' is not valid in this row-local evaluator.");
        }

        return result;
    }

#if NETSTANDARD2_1
    private static Regex WhitespaceRegex() => WhitespaceRegexInstance.Value;

    private static Regex ValNumberPrefixRegex() => ValNumberPrefixRegexInstance.Value;
#else
    [GeneratedRegex(WhitespacePattern)]
    private static partial Regex WhitespaceRegex();

    [GeneratedRegex(ValNumberPrefixPattern, RegexOptions.CultureInvariant)]
    private static partial Regex ValNumberPrefixRegex();
#endif
}
