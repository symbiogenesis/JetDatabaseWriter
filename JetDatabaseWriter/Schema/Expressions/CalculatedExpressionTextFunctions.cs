namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;
using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionFunctionRegistry;
using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionLimits;

internal static partial class CalculatedExpressionTextFunctions
{
    internal static void AddFunctions(Dictionary<string, CalculatedFunctionDescriptor> functions)
    {
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Text, "LEN", 1, 1, static function => ToText(function.Arg(0)).Length));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Text, "LEFT", 2, 2, static function => Left(ToText(function.Arg(0)), checked((int)ToDecimal(function.Arg(1))))));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Text, "RIGHT", 2, 2, static function => Right(ToText(function.Arg(0)), checked((int)ToDecimal(function.Arg(1))))));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Text, "MID", 2, 3, static function => Mid(ToText(function.Arg(0)), checked((int)ToDecimal(function.Arg(1))), function.Count > 2 ? checked((int)ToDecimal(function.Arg(2))) : int.MaxValue)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Text, "UCASE", 1, 1, static function => ToText(function.Arg(0)).ToUpperInvariant(), "UPPER"));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Text, "LCASE", 1, 1, static function => CultureInfo.InvariantCulture.TextInfo.ToLower(ToText(function.Arg(0))), "LOWER"));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Text, "TRIM", 1, 1, static function => ToText(function.Arg(0)).Trim()));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Text, "LTRIM", 1, 1, static function => ToText(function.Arg(0)).TrimStart()));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Text, "RTRIM", 1, 1, static function => ToText(function.Arg(0)).TrimEnd()));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Text, "CSTR", 1, 1, static function => ToText(function.Arg(0))));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Text, "HEX", 1, 1, static function => Convert.ToInt64(function.Arg(0), CultureInfo.InvariantCulture).ToString("X", CultureInfo.InvariantCulture)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Text, "OCT", 1, 1, static function => Convert.ToString(Convert.ToInt64(function.Arg(0), CultureInfo.InvariantCulture), 8) ?? string.Empty));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Text, "VAL", 1, 1, static function => Val(ToText(function.Arg(0)))));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Text, "ASC", 1, 1, static function => Asc(ToText(function.Arg(0)), asciiOnly: true)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Text, "ASCW", 1, 1, static function => Asc(ToText(function.Arg(0)), asciiOnly: false)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Text, "CHR", 1, 1, static function => char.ConvertFromUtf32(checked((int)ToDecimal(function.Arg(0)))), "CHRW"));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Text, "STR", 1, 1, static function => EvaluateStr(function)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Text, "INSTR", 2, 4, static function => EvaluateInStr(function)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Text, "INSTRREV", 2, 4, static function => EvaluateInStrRev(function)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Text, "REPLACE", 3, 6, static function => EvaluateReplaceText(function)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Text, "SPACE", 1, 1, static function => RepeatChar(checked((int)ToDecimal(function.Arg(0))), " ")));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Text, "STRCOMP", 2, 3, static function => Math.Sign(string.Compare(ToText(function.Arg(0)), ToText(function.Arg(1)), CompareOptions(function.Count > 2 ? function.Arg(2) : null)))));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Text, "STRING", 2, 2, static function => RepeatChar(checked((int)ToDecimal(function.Arg(0))), ToText(function.Arg(1)))));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Text, "STRREVERSE", 1, 1, static function => EvaluateStrReverse(function)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Text, "STRCONV", 2, 3, static function => IsNull(function.Arg(0)) ? DBNull.Value : StrConv(ToText(function.Arg(0)), checked((int)ToDecimal(function.Arg(1))))));
    }

    internal static string ConcatText(object left, object right)
    {
        string leftText = ToText(left);
        string rightText = ToText(right);
        EnsureGeneratedTextLength((long)leftText.Length + rightText.Length);
        return leftText + rightText;
    }

    private static string Left(string text, int count)
    {
        count = Math.Clamp(count, 0, text.Length);
        return text.Substring(0, count);
    }

    private static string Right(string text, int count)
    {
        count = Math.Clamp(count, 0, text.Length);
        return text.Substring(text.Length - count, count);
    }

    private static string Mid(string text, int start, int count)
    {
        int zeroBasedStart = Math.Clamp(start - 1, 0, text.Length);
        count = Math.Clamp(count, 0, text.Length - zeroBasedStart);
        return text.Substring(zeroBasedStart, count);
    }

    private static string EvaluateStr(CalculatedFunctionInvocation function)
    {
        decimal strValue = ToDecimal(function.Arg(0));
        string strText = strValue.ToString(CultureInfo.InvariantCulture);
        return strValue >= 0 ? " " + strText : strText;
    }

    private static int EvaluateInStr(CalculatedFunctionInvocation function)
    {
        int start = 1;
        int textIndex = 0;
        if (function.Count > 2)
        {
            start = checked((int)ToDecimal(function.Arg(0)));
            textIndex = 1;
        }

        string text = ToText(function.Arg(textIndex));
        string search = ToText(function.Arg(textIndex + 1));
        if (search.Length == 0)
        {
            return Math.Clamp(start, 1, text.Length + 1);
        }

        int zeroBasedStart = Math.Clamp(start - 1, 0, text.Length);
        int found = text.IndexOf(search, zeroBasedStart, function.Count > 3 ? CompareOptions(function.Arg(3)) : StringComparison.OrdinalIgnoreCase);
        return found < 0 ? 0 : found + 1;
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
            return start > text.Length ? string.Empty : text.Substring(start - 1);
        }

        string prefix = text.Substring(0, start - 1);
        string tail = text.Substring(start - 1);
        var builder = new StringBuilder(Math.Min(MaxGeneratedTextLength, prefix.Length + tail.Length));
        AppendGeneratedText(builder, prefix);
        int replacements = 0;
        int position = 0;
        while (position < tail.Length)
        {
            int found = tail.IndexOf(search, position, comparison);
            if (found < 0 || (count >= 0 && replacements >= count))
            {
                AppendGeneratedText(builder, tail, position, tail.Length - position);
                break;
            }

            AppendGeneratedText(builder, tail, position, found - position);
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

    private static int Asc(string text, bool asciiOnly)
    {
        if (string.IsNullOrEmpty(text))
        {
            throw new ArgumentException("Calculated-column Asc/AscW requires a non-empty string.");
        }

        int codePoint = text[0];
        if (asciiOnly && codePoint > 255)
        {
            throw new ArgumentException($"Calculated-column Asc character code '{codePoint}' is outside the ANSI byte range.");
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

    private static string RepeatChar(int count, string value)
    {
        if (count <= 0)
        {
            return string.Empty;
        }

        EnsureGeneratedTextLength(count);
        return new string(string.IsNullOrEmpty(value) ? '\0' : value[0], count);
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

    [GeneratedRegex("\\s+")]
    private static partial Regex WhitespaceRegex();

    [GeneratedRegex("^[+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][+-]?\\d+)?", RegexOptions.CultureInvariant)]
    private static partial Regex ValNumberPrefixRegex();
}
