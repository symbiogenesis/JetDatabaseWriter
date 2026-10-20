namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Exceptions;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;
using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionFunctionRegistry;
using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionLimits;

internal static class CalculatedExpressionMetadataFunctions
{
    internal static void AddFunctions(Dictionary<string, CalculatedFunctionDescriptor> functions)
    {
        AddFunction(functions, "GENGUID", 0, 0, static _ => Guid.NewGuid());
        AddFunction(functions, "CURRENTUSER", 0, 0, static _ => "Admin");
        AddFunction(functions, "CVAR", 1, 1, static function => function.Arg(0));
        AddFunction(functions, "VARTYPE", 1, 1, static function => VarType(function.Arg(0)));
        AddFunction(functions, "TYPENAME", 1, 1, static function => TypeName(function.Arg(0)));
        AddFunction(functions, "DLOOKUP", 0, MaxFunctionArguments, EvaluateDomainAggregate, "DCOUNT", "DSUM", "DAVG", "DMIN", "DMAX");
    }

    private static object EvaluateDomainAggregate(CalculatedFunctionInvocation function)
        => throw new JetNotSupportedException(
            JetErrorCode.UnsupportedExpression,
            $"Access table calculated columns reject domain aggregate function '{function.Name}' because it requires domain/query evaluation.");

    private static int VarType(object? value)
    {
        if (IsNull(value))
        {
            return 1;
        }

        return value switch
        {
            bool => 11,
            byte => 17,
            short => 2,
            int or long => 3,
            float => 4,
            double => 5,
            decimal => 14,
            DateTime => 7,
            string => 8,
            _ => 12,
        };
    }

    private static string TypeName(object? value)
    {
        if (IsNull(value))
        {
            return "Null";
        }

        return value switch
        {
            bool => "Boolean",
            byte => "Byte",
            short => "Integer",
            int or long => "Long",
            float => "Single",
            double => "Double",
            decimal => "Decimal",
            DateTime => "Date",
            string => "String",
            _ => "Variant",
        };
    }
}
