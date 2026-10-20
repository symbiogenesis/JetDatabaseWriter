namespace JetDatabaseWriter.Schema.Expressions;

using JetDatabaseWriter.Exceptions;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;

internal sealed class CalculatedExpressionBinaryNode(string operation, CalculatedExpressionNode left, CalculatedExpressionNode right) : CalculatedExpressionNode
{
    public override object Evaluate(CalculatedExpressionEvaluationContext context)
    {
        object leftValue = left.Evaluate(context);
        object rightValue = right.Evaluate(context);

        // Access concatenates when both operands of + are text, and does date
        // arithmetic when either operand is a date (AccessVariantOperators).
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
}
