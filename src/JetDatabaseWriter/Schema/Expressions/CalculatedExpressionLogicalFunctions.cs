namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Models;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;
using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionFunctionRegistry;

internal static class CalculatedExpressionLogicalFunctions
{
    internal static void AddFunctions(Dictionary<string, CalculatedFunctionDescriptor> functions)
    {
        AddFunction(functions, "IIF", 3, 3, EvaluateIIf, "IF");

        // The word operators And, Or, Not, Xor, Eqv and Imp, which the parser
        // lowers to these calls: logical on Booleans, bitwise on numbers, and
        // three-valued with Null, as in VBA (AccessVariantOperators).
        AddFunction(functions, "AND", 2, 2, static function => AccessVariantOperators.And(function.Arg(0), function.Arg(1)));
        AddFunction(functions, "OR", 2, 2, static function => AccessVariantOperators.Or(function.Arg(0), function.Arg(1)));
        AddFunction(functions, "NOT", 1, 1, static function => AccessVariantOperators.Not(function.Context.IsCalculatingColumn ? ToCalculatedBitwiseOperand(function.Arg(0)) : function.Arg(0)));
        AddFunction(functions, "XOR", 2, 2, static function => AccessVariantOperators.Xor(function.Arg(0), function.Arg(1)));
        AddFunction(functions, "EQV", 2, 2, static function => AccessVariantOperators.Eqv(function.Arg(0), function.Arg(1)));
        AddFunction(functions, "IMP", 2, 2, static function => AccessVariantOperators.Imp(function.Arg(0), function.Arg(1)));
        AddFunction(functions, "LIKE", 2, 2, EvaluateLike);
        AddFunction(functions, "BETWEEN", 3, 3, EvaluateBetween);
        AddFunction(functions, "IN", 2, int.MaxValue, EvaluateIn);
        AddFunction(functions, "CHOOSE", 2, int.MaxValue, EvaluateChoose);
        AddFunction(functions, "SWITCH", 2, int.MaxValue, EvaluateSwitch);
        AddFunction(functions, "NZ", 1, 2, EvaluateNz);
        AddFunction(functions, "ISNULL", 1, 1, static function => IsNull(function.Arg(0)), "ISBLANK");
        AddFunction(functions, "ISEMPTY", 1, 1, static function =>
        {
            _ = function.Arg(0);
            return false;
        });
        AddFunction(functions, "ISNUMERIC", 1, 1, static function => TryConvertDecimal(function.Arg(0), out _), "ISNUMBER");
        AddFunction(functions, "ISDATE", 1, 1, static function => IsDateValue(function.Arg(0)));
        AddFunction(functions, "CBOOL", 1, 1, static function => ToBoolean(function.Arg(0)));
    }

    private static object EvaluateIIf(CalculatedFunctionInvocation function)
        => (function.Context.IsCalculatingColumn ? ToCalculatedCondition(function.Arg(0)) : ToBoolean(function.Arg(0))) ? function.Arg(1) : function.Arg(2);

    /// <summary>
    /// VBA's <c>IsDate</c>: True for a date, or for text that parses as one. A number is
    /// not a date even though it converts to one (<c>IsDate(5)</c> is False).
    /// </summary>
    /// <param name="value">The argument.</param>
    /// <returns>Whether the argument is a date.</returns>
    private static bool IsDateValue(object value)
        => value is NativeDate or DateTime || (value is string && TryConvertDateTime(value, out _));

    private static object EvaluateLike(CalculatedFunctionInvocation function)
        => AccessLikeMatcher.Evaluate(function);

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

        return CompareNonNullValues(value, lowerBound, static comparison => comparison >= 0, function.Context.TextCollation)
            && CompareNonNullValues(value, upperBound, static comparison => comparison <= 0, function.Context.TextCollation);
    }

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
            if (!IsNull(candidateValue) && CompareNonNullValues(inValue, candidateValue, static comparison => comparison == 0, function.Context.TextCollation))
            {
                return true;
            }
        }

        return false;
    }

    private static object EvaluateChoose(CalculatedFunctionInvocation function)
    {
        object[] values = new object[function.Count];
        for (int index = 0; index < values.Length; index++)
        {
            values[index] = function.Arg(index);
        }

        int choiceIndex = checked((int)ToDecimal(values[0]));
        return choiceIndex >= 1 && choiceIndex < values.Length ? values[choiceIndex] : DBNull.Value;
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
}
