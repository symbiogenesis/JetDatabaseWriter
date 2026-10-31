namespace JetDatabaseWriter.Schema.Expressions;

using System.Collections.Generic;
using JetDatabaseWriter.Models;

internal sealed class CalculatedExpressionFunctionNode(string name, IReadOnlyList<CalculatedExpressionNode> args) : CalculatedExpressionNode
{
    internal override bool HasStaleQualifier(string tableName)
    {
        foreach (CalculatedExpressionNode argument in args)
        {
            if (argument.HasStaleQualifier(tableName))
            {
                return true;
            }
        }

        return false;
    }

    internal override bool HasUntypedNullResult { get; } = ReturnsUntypedNull(name, args);

    internal override (object Value, bool IsLiteral) EvaluateMembershipOperand(CalculatedExpressionEvaluationContext context)
    {
        if (context.IsCalculatingColumn && args.Count == 3 && name.Equals("IIF", System.StringComparison.OrdinalIgnoreCase))
        {
            var invocation = new CalculatedFunctionInvocation(name, args, context);
            int selected = CalculatedExpressionCoercion.ToCalculatedCondition(invocation.Arg(0)) ? 1 : 2;
            return invocation.MembershipArg(selected);
        }

        return base.EvaluateMembershipOperand(context);
    }

    public override object Evaluate(CalculatedExpressionEvaluationContext context)
    {
        try
        {
            if (args.Count == 2 && name.ToUpperInvariant() is "AND" or "OR")
            {
                object left = args[0].Evaluate(context);
                object right = args[1].Evaluate(context);
                if (context.IsCalculatingColumn)
                {
                    left = CalculatedExpressionLogicalOperand.Normalize(left);
                    right = CalculatedExpressionLogicalOperand.Normalize(right);
                }

                if (left is CalculatedError || right is CalculatedError)
                {
                    object result = name.Equals("AND", System.StringComparison.OrdinalIgnoreCase)
                        ? AccessVariantOperators.And(left is CalculatedError ? System.DBNull.Value : left, right is CalculatedError ? System.DBNull.Value : right)
                        : AccessVariantOperators.Or(left is CalculatedError ? System.DBNull.Value : left, right is CalculatedError ? System.DBNull.Value : right);
                    if (result is not System.DBNull)
                    {
                        return context.IsCalculatingColumn ? CalculatedExpressionLogicalOperand.IntegerResult(result) : result;
                    }

                    return left is CalculatedError ? left : right;
                }

                object logicalResult = name.Equals("AND", System.StringComparison.OrdinalIgnoreCase)
                    ? AccessVariantOperators.And(left, right)
                    : AccessVariantOperators.Or(left, right);
                return context.IsCalculatingColumn ? CalculatedExpressionLogicalOperand.IntegerResult(logicalResult) : logicalResult;
            }

            return CalculatedExpressionFunctionRegistry.Evaluate(name, args, context);
        }
        catch (CalculatedErrorPropagationException exception)
        {
            return exception.Error;
        }
        catch (System.ArgumentException) when (context.IsCalculatingColumn && MapsArgumentErrors(name))
        {
            return new CalculatedError(5);
        }
        catch (System.OverflowException) when (context.IsCalculatingColumn && MapsArgumentErrors(name))
        {
            return new CalculatedError(6);
        }
        catch (System.Exception exception) when (context.IsCalculatingColumn && MapsArgumentErrors(name) && exception is System.InvalidCastException or System.FormatException)
        {
            return new CalculatedError(13);
        }
    }

    private static bool ReturnsUntypedNull(string functionName, IReadOnlyList<CalculatedExpressionNode> arguments)
    {
        if (functionName.Equals("IIF", System.StringComparison.OrdinalIgnoreCase))
        {
            return arguments.Count == 3 && arguments[1].HasUntypedNullResult && arguments[2].HasUntypedNullResult;
        }

        if (functionName.Equals("CHOOSE", System.StringComparison.OrdinalIgnoreCase))
        {
            for (int index = 1; index < arguments.Count; index++)
            {
                if (arguments[index].HasUntypedNullResult)
                {
                    return true;
                }
            }
        }

        return false;
    }

    private static bool MapsArgumentErrors(string functionName)
        => functionName.TrimEnd('$').ToUpperInvariant() is "AND" or "OR" or "ABS" or "ATN" or "COS" or "EXP" or "LOG" or "SGN" or "SIN" or "SQR" or "TAN" or "ROUND" or "INT" or "FIX" or "CDBL" or "INSTR" or "ASC" or "LEN" or "LEFT" or "RIGHT" or "MID" or "UCASE" or "LCASE" or "TRIM" or "STR" or "STRING" or "CHOOSE" or "NOT" or "FV" or "PV" or "PMT" or "NPER" or "IPMT" or "PPMT" or "DDB" or "SLN" or "SYD" or "RATE";
}
