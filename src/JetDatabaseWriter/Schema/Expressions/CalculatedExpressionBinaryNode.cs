namespace JetDatabaseWriter.Schema.Expressions;

using System;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;

internal sealed class CalculatedExpressionBinaryNode(string operation, CalculatedExpressionNode left, CalculatedExpressionNode right) : CalculatedExpressionNode
{
    public override object Evaluate(CalculatedExpressionEvaluationContext context)
    {
        object leftValue = left.Evaluate(context);
        object rightValue = right.Evaluate(context);
        if (leftValue is CalculatedError || rightValue is CalculatedError)
        {
            return leftValue is CalculatedError ? leftValue : rightValue;
        }

        // Access concatenates when both operands of + are text, and does date
        // arithmetic when either operand is a date (AccessVariantOperators).
        try
        {
            if (context.IsCalculatingColumn && !IsNull(leftValue) && !IsNull(rightValue))
            {
                if (operation == "/" && (double.IsNaN(ToDouble(leftValue)) || double.IsNaN(ToDouble(rightValue))))
                {
                    return new CalculatedError(6);
                }

                if (operation == "/" && double.IsInfinity(ToDouble(leftValue)))
                {
                    return new CalculatedError(11);
                }

                if (operation == "/" && ToDouble(rightValue) == 0d)
                {
                    return new CalculatedError(ToDouble(leftValue) == 0d ? 6 : 11);
                }

                if (operation == "/" && double.IsInfinity(ToDouble(rightValue)))
                {
                    return ToDouble(leftValue) / ToDouble(rightValue);
                }

                if (operation == "^" && ((ToDouble(leftValue) == 0d && ToDouble(rightValue) < 0d)
                    || (ToDouble(leftValue) < 0d && !double.IsInfinity(ToDouble(rightValue)) && ToDouble(rightValue) % 1d != 0d)))
                {
                    return new CalculatedError(440);
                }
            }

            return operation switch
            {
                "&" => CalculatedExpressionTextFunctions.ConcatText(leftValue, rightValue),
                "+" => AccessVariantOperators.Add(leftValue, rightValue),
                "-" => AccessVariantOperators.Subtract(leftValue, rightValue),
                "*" => AccessVariantOperators.Multiply(leftValue, rightValue),
                "/" => AccessVariantOperators.Divide(leftValue, rightValue),
                "^" => AccessVariantOperators.Power(leftValue, rightValue),
                "=" => CompareValues(leftValue, rightValue, static c => c == 0, context.TextCollation),
                "<>" => CompareValues(leftValue, rightValue, static c => c != 0, context.TextCollation),
                ">" => CompareValues(leftValue, rightValue, static c => c > 0, context.TextCollation),
                ">=" => CompareValues(leftValue, rightValue, static c => c >= 0, context.TextCollation),
                "<" => CompareValues(leftValue, rightValue, static c => c < 0, context.TextCollation),
                "<=" => CompareValues(leftValue, rightValue, static c => c <= 0, context.TextCollation),
                _ => throw new JetOperationException(JetErrorCode.InvalidExpression, $"Unexpected calculated-column binary operation '{operation}'."),
            };
        }
        catch (OverflowException) when (context.IsCalculatingColumn)
        {
            return new CalculatedError(6);
        }
        catch (Exception exception) when (context.IsCalculatingColumn && exception is InvalidCastException or FormatException)
        {
            return new CalculatedError(13);
        }
    }
}
