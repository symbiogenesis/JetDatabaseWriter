namespace JetDatabaseWriter.Schema.Expressions;

using System;
using ClosedXML.Parser;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;

internal sealed class CalculatedExpressionBinaryNode(BinaryOperation operation, CalculatedExpressionNode left, CalculatedExpressionNode right) : CalculatedExpressionNode
{
    public override object Evaluate(CalculatedExpressionEvaluationContext context, CalculatedExpressionPlan plan)
    {
        object leftValue = left.Evaluate(context, plan);
        object rightValue = right.Evaluate(context, plan);

        // Access concatenates when both operands of + are text, and does date
        // arithmetic when either operand is a date (AccessVariantOperators).
        return operation switch
        {
            BinaryOperation.Concat => CalculatedExpressionTextFunctions.ConcatText(leftValue, rightValue),
            BinaryOperation.Addition => AccessVariantOperators.Add(leftValue, rightValue),
            BinaryOperation.Subtraction => AccessVariantOperators.Subtract(leftValue, rightValue),
            BinaryOperation.Multiplication => AccessVariantOperators.Multiply(leftValue, rightValue),
            BinaryOperation.Division => AccessVariantOperators.Divide(leftValue, rightValue),
            BinaryOperation.Power => AccessVariantOperators.Power(leftValue, rightValue),
            BinaryOperation.Equal => CompareValues(leftValue, rightValue, static c => c == 0, context.TextCollation),
            BinaryOperation.NotEqual => CompareValues(leftValue, rightValue, static c => c != 0, context.TextCollation),
            BinaryOperation.GreaterThan => CompareValues(leftValue, rightValue, static c => c > 0, context.TextCollation),
            BinaryOperation.GreaterOrEqualThan => CompareValues(leftValue, rightValue, static c => c >= 0, context.TextCollation),
            BinaryOperation.LessThan => CompareValues(leftValue, rightValue, static c => c < 0, context.TextCollation),
            BinaryOperation.LessOrEqualThan => CompareValues(leftValue, rightValue, static c => c <= 0, context.TextCollation),
            BinaryOperation.Union or BinaryOperation.Intersection or BinaryOperation.Range => throw new NotSupportedException(
                $"Calculated-column binary operation '{operation}' is a spreadsheet range operation and is not valid in Access calculated columns."),
            _ => throw new InvalidOperationException($"ClosedXML.Parser produced unexpected calculated-column binary operation '{operation}'."),
        };
    }
}
