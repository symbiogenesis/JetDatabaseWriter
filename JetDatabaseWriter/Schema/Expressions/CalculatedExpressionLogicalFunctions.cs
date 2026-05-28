namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;
using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionFunctionRegistry;
using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionLimits;

internal static class CalculatedExpressionLogicalFunctions
{
    internal static void AddFunctions(Dictionary<string, CalculatedFunctionDescriptor> functions)
    {
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Logical, "IIF", 3, 3, EvaluateIIf, "IF"));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Logical, "AND", 1, int.MaxValue, static function => EvaluateAnd(function)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Logical, "OR", 1, int.MaxValue, static function => EvaluateOr(function)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Logical, "NOT", 1, 1, static function => !ToBoolean(function.Arg(0))));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Logical, "XOR", 2, int.MaxValue, static function => EvaluateXor(function)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Logical, "EQV", 2, 2, static function => ToBoolean(function.Arg(0)) == ToBoolean(function.Arg(1))));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Logical, "IMP", 2, 2, static function => !ToBoolean(function.Arg(0)) || ToBoolean(function.Arg(1))));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Logical, "LIKE", 2, 2, EvaluateLike));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Logical, "BETWEEN", 3, 3, EvaluateBetween));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Logical, "IN", 2, int.MaxValue, EvaluateIn));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Logical, "CHOOSE", 2, int.MaxValue, EvaluateChoose));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Logical, "SWITCH", 2, int.MaxValue, EvaluateSwitch));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Logical, "NZ", 1, 2, EvaluateNz));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Logical, "ISNULL", 1, 1, static function => IsNull(function.Arg(0)), "ISBLANK"));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Logical, "ISNUMERIC", 1, 1, static function => TryConvertDecimal(function.Arg(0), out _), "ISNUMBER"));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Logical, "ISDATE", 1, 1, static function => TryConvertDateTime(function.Arg(0), out _)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Logical, "CBOOL", 1, 1, static function => ToBoolean(function.Arg(0))));
    }

    private static object EvaluateIIf(CalculatedFunctionInvocation function)
        => ToBoolean(function.Arg(0)) ? function.Arg(1) : function.Arg(2);

    private static bool EvaluateAnd(CalculatedFunctionInvocation function)
    {
        for (int argIndex = 0; argIndex < function.Count; argIndex++)
        {
            if (!ToBoolean(function.Arg(argIndex)))
            {
                return false;
            }
        }

        return true;
    }

    private static bool EvaluateOr(CalculatedFunctionInvocation function)
    {
        for (int argIndex = 0; argIndex < function.Count; argIndex++)
        {
            if (ToBoolean(function.Arg(argIndex)))
            {
                return true;
            }
        }

        return false;
    }

    private static bool EvaluateXor(CalculatedFunctionInvocation function)
    {
        bool xorResult = false;
        for (int argIndex = 0; argIndex < function.Count; argIndex++)
        {
            xorResult ^= ToBoolean(function.Arg(argIndex));
        }

        return xorResult;
    }

    private static object EvaluateLike(CalculatedFunctionInvocation function)
    {
        object value = function.Arg(0);
        if (IsNull(value))
        {
            return DBNull.Value;
        }

        object pattern = function.Arg(1);
        return IsNull(pattern) ? DBNull.Value : AccessLike(ToText(value), ToText(pattern));
    }

#pragma warning disable CA1859 // False positive: EvaluateBetween can return either DBNull.Value or a Boolean result.
    private static object EvaluateBetween(CalculatedFunctionInvocation function)
    {
        object value = function.Arg(0);
        if (IsNull(value))
        {
            return DBNull.Value;
        }

        object lowerBound = function.Arg(1);
        if (IsNull(lowerBound))
        {
            return DBNull.Value;
        }

        object upperBound = function.Arg(2);
        if (IsNull(upperBound))
        {
            return DBNull.Value;
        }

        return CompareNonNullValues(value, lowerBound, static comparison => comparison >= 0)
            && CompareNonNullValues(value, upperBound, static comparison => comparison <= 0);
    }
#pragma warning restore CA1859

    private static object EvaluateIn(CalculatedFunctionInvocation function)
    {
        object inValue = function.Arg(0);
        if (IsNull(inValue))
        {
            return DBNull.Value;
        }

        for (int argIndex = 1; argIndex < function.Count; argIndex++)
        {
            object candidateValue = function.Arg(argIndex);
            if (!IsNull(candidateValue) && CompareNonNullValues(inValue, candidateValue, static comparison => comparison == 0))
            {
                return true;
            }
        }

        return false;
    }

    private static object EvaluateChoose(CalculatedFunctionInvocation function)
    {
        int choiceIndex = checked((int)ToDecimal(function.Arg(0)));
        return choiceIndex >= 1 && choiceIndex < function.Count ? function.Arg(choiceIndex) : DBNull.Value;
    }

    private static object EvaluateSwitch(CalculatedFunctionInvocation function)
    {
        if ((function.Count % 2) != 0)
        {
            throw new ArgumentException("Calculated-column function 'SWITCH' expects condition/value argument pairs.");
        }

        for (int argIndex = 0; argIndex < function.Count; argIndex += 2)
        {
            if (ToBoolean(function.Arg(argIndex)))
            {
                return function.Arg(argIndex + 1);
            }
        }

        return DBNull.Value;
    }

    private static object EvaluateNz(CalculatedFunctionInvocation function)
    {
        object nzValue = function.Arg(0);
        if (!IsNull(nzValue))
        {
            return nzValue;
        }

        return function.Count > 1 ? function.Arg(1) : string.Empty;
    }

    private static bool AccessLike(string text, string pattern)
    {
        var builder = new StringBuilder(pattern.Length * 2);
        builder.Append('^');
        for (int patternIndex = 0; patternIndex < pattern.Length; patternIndex++)
        {
            char current = pattern[patternIndex];
            switch (current)
            {
                case '*':
                    builder.Append(".*");
                    break;
                case '?':
                    builder.Append('.');
                    break;
                case '#':
                    builder.Append("\\d");
                    break;
                case '[':
                    int endBracket = pattern.IndexOf(']', patternIndex + 1);
                    if (endBracket < 0)
                    {
                        return false;
                    }

                    string charClass = pattern.Substring(patternIndex + 1, endBracket - patternIndex - 1);
                    builder.Append('[');
                    if (charClass.StartsWith('!'))
                    {
                        builder.Append('^').Append(charClass, 1, charClass.Length - 1);
                    }
                    else
                    {
                        builder.Append(charClass);
                    }

                    builder.Append(']');
                    patternIndex = endBracket;
                    break;
                default:
                    builder.Append(Regex.Escape(current.ToString()));
                    break;
            }
        }

        builder.Append('$');
        try
        {
            return Regex.IsMatch(
                text,
                builder.ToString(),
                RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.CultureInvariant,
                LikeRegexTimeout);
        }
        catch (ArgumentException)
        {
            return false;
        }
        catch (RegexMatchTimeoutException)
        {
            return false;
        }
    }
}
