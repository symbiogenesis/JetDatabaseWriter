namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;
using System.Globalization;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;
using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionFunctionRegistry;

internal static class CalculatedExpressionNumericFunctions
{
    internal static void AddFunctions(Dictionary<string, CalculatedFunctionDescriptor> functions)
    {
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Numeric, "MOD", 2, 2, static function => EvaluateNumeric(function.Arg(0), function.Arg(1), static (leftValue, rightValue) => leftValue % rightValue)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Numeric, "INTDIV", 2, 2, EvaluateIntegerDivision));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Numeric, "ABS", 1, 1, static function => Math.Abs(ToDecimal(function.Arg(0)))));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Numeric, "ATAN", 1, 1, static function => Math.Atan(ToDouble(function.Arg(0))), "ATN"));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Numeric, "COS", 1, 1, static function => Math.Cos(ToDouble(function.Arg(0)))));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Numeric, "EXP", 1, 1, static function => Math.Exp(ToDouble(function.Arg(0)))));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Numeric, "LOG", 1, 1, static function => Math.Log(ToDouble(function.Arg(0)))));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Numeric, "RND", 0, 1, static function => function.Context.NextRandom(function.Count > 0 ? function.Arg(0) : DBNull.Value)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Numeric, "SGN", 1, 1, static function => Math.Sign(ToDecimal(function.Arg(0)))));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Numeric, "SIN", 1, 1, static function => Math.Sin(ToDouble(function.Arg(0)))));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Numeric, "SQR", 1, 1, static function => Math.Sqrt(ToDouble(function.Arg(0))), "SQRT"));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Numeric, "TAN", 1, 1, static function => Math.Tan(ToDouble(function.Arg(0)))));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Numeric, "ROUND", 1, 2, static function => Math.Round(ToDecimal(function.Arg(0)), function.Count > 1 ? checked((int)ToDecimal(function.Arg(1))) : 0, MidpointRounding.ToEven)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Numeric, "INT", 1, 1, static function => Math.Floor(ToDecimal(function.Arg(0)))));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Numeric, "FIX", 1, 1, static function => EvaluateFix(function)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Numeric, "CINT", 1, 1, static function => Convert.ToInt16(function.Arg(0), CultureInfo.InvariantCulture)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Numeric, "CLNG", 1, 1, static function => Convert.ToInt32(function.Arg(0), CultureInfo.InvariantCulture)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Numeric, "CDBL", 1, 1, static function => Convert.ToDouble(function.Arg(0), CultureInfo.InvariantCulture)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Numeric, "CSNG", 1, 1, static function => Convert.ToSingle(function.Arg(0), CultureInfo.InvariantCulture)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Numeric, "CDEC", 1, 1, static function => ToDecimal(function.Arg(0)), "CCUR"));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Numeric, "CBYTE", 1, 1, static function => Convert.ToByte(function.Arg(0), CultureInfo.InvariantCulture)));
    }

    private static object EvaluateIntegerDivision(CalculatedFunctionInvocation function)
        => IsNull(function.Arg(0)) || IsNull(function.Arg(1))
            ? DBNull.Value
            : Math.Truncate(ToDecimal(function.Arg(0)) / ToDecimal(function.Arg(1)));

    private static decimal EvaluateFix(CalculatedFunctionInvocation function)
    {
        decimal fixValue = ToDecimal(function.Arg(0));
        return fixValue < 0 ? Math.Ceiling(fixValue) : Math.Floor(fixValue);
    }
}
