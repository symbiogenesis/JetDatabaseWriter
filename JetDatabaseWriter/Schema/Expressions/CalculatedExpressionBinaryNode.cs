namespace JetDatabaseWriter.Schema.Expressions;

using System;
using ClosedXML.Parser;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;

internal sealed class CalculatedExpressionBinaryNode(BinaryOperation operation, CalculatedExpressionNode left, CalculatedExpressionNode right) : CalculatedExpressionNode
{
    public override object Evaluate(CalculatedExpressionEvaluator.EvaluationContext context, CalculatedExpressionEvaluator.Plan plan)
    {
        object leftValue = left.Evaluate(context, plan);
        object rightValue = right.Evaluate(context, plan);
        return operation switch
        {
            BinaryOperation.Concat => CalculatedExpressionTextFunctions.ConcatText(leftValue, rightValue),
            BinaryOperation.Addition => EvaluateNumeric(leftValue, rightValue, static (l, r) => l + r),
            BinaryOperation.Subtraction => EvaluateNumeric(leftValue, rightValue, static (l, r) => l - r),
            BinaryOperation.Multiplication => EvaluateNumeric(leftValue, rightValue, static (l, r) => l * r),
            BinaryOperation.Division => EvaluateNumeric(leftValue, rightValue, static (l, r) => l / r),
            BinaryOperation.Power => IsNull(leftValue) || IsNull(rightValue) ? DBNull.Value : Math.Pow(ToDouble(leftValue), ToDouble(rightValue)),
            BinaryOperation.Equal => CompareValues(leftValue, rightValue, static c => c == 0),
            BinaryOperation.NotEqual => CompareValues(leftValue, rightValue, static c => c != 0),
            BinaryOperation.GreaterThan => CompareValues(leftValue, rightValue, static c => c > 0),
            BinaryOperation.GreaterOrEqualThan => CompareValues(leftValue, rightValue, static c => c >= 0),
            BinaryOperation.LessThan => CompareValues(leftValue, rightValue, static c => c < 0),
            BinaryOperation.LessOrEqualThan => CompareValues(leftValue, rightValue, static c => c <= 0),
            BinaryOperation.Union or BinaryOperation.Intersection or BinaryOperation.Range => throw new NotSupportedException(
                $"Calculated-column binary operation '{operation}' is a spreadsheet range operation and is not valid in Access calculated columns."),
            _ => throw new InvalidOperationException($"ClosedXML.Parser produced unexpected calculated-column binary operation '{operation}'."),
        };
    }
}
