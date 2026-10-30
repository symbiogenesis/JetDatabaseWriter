namespace JetDatabaseWriter.Schema.Expressions;

using System.Collections.Generic;
using JetDatabaseWriter.Models;

internal sealed class CalculatedExpressionFunctionNode(string name, IReadOnlyList<CalculatedExpressionNode> args) : CalculatedExpressionNode
{
    public override object Evaluate(CalculatedExpressionEvaluationContext context)
    {
        if (args.Count == 2 && name.ToUpperInvariant() is "AND" or "OR")
        {
            object left = args[0].Evaluate(context);
            object right = args[1].Evaluate(context);
            if (context.IsCalculatingColumn)
            {
                left = CalculatedExpressionCoercion.ToCalculatedBitwiseOperand(left);
                right = CalculatedExpressionCoercion.ToCalculatedBitwiseOperand(right);
            }

            if (left is CalculatedError || right is CalculatedError)
            {
                object result = name.Equals("AND", System.StringComparison.OrdinalIgnoreCase)
                    ? AccessVariantOperators.And(left is CalculatedError ? System.DBNull.Value : left, right is CalculatedError ? System.DBNull.Value : right)
                    : AccessVariantOperators.Or(left is CalculatedError ? System.DBNull.Value : left, right is CalculatedError ? System.DBNull.Value : right);
                if (result is not System.DBNull)
                {
                    return result;
                }

                return left is CalculatedError ? left : right;
            }

            return name.Equals("AND", System.StringComparison.OrdinalIgnoreCase)
                ? AccessVariantOperators.And(left, right)
                : AccessVariantOperators.Or(left, right);
        }

        try
        {
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

    private static bool MapsArgumentErrors(string functionName)
        => functionName.TrimEnd('$').ToUpperInvariant() is "INSTR" or "ASC" or "LEN" or "LEFT" or "RIGHT" or "MID" or "UCASE" or "LCASE" or "TRIM" or "STR" or "STRING" or "CHOOSE" or "NOT" or "FV" or "PV" or "PMT" or "NPER" or "IPMT" or "PPMT" or "DDB" or "SLN" or "SYD" or "RATE";
}
