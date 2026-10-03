namespace JetDatabaseWriter.Schema.Expressions;

using System;
using ClosedXML.Parser;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;

internal sealed class CalculatedExpressionUnaryNode(UnaryOperation operation, CalculatedExpressionNode operand) : CalculatedExpressionNode
{
    public override object Evaluate(CalculatedExpressionEvaluationContext context, CalculatedExpressionPlan plan)
    {
        object value = operand.Evaluate(context, plan);
        if (IsNull(value))
        {
            return DBNull.Value;
        }

        // Access has no '%' operator. The normalizer rejects it before parsing,
        // so the Percent arm only guards against a plan built some other way.
        return operation switch
        {
            UnaryOperation.Plus => ToDecimal(value),
            UnaryOperation.Minus => -ToDecimal(value),
            UnaryOperation.Percent => throw new NotSupportedException(
                "Calculated-column expressions cannot use '%': it is a spreadsheet operator, not an Access operator."),
            UnaryOperation.ImplicitIntersection or UnaryOperation.SpillRange => throw new NotSupportedException(
                $"Calculated-column unary operation '{operation}' is a spreadsheet dynamic-array operation and is not valid in Access calculated columns."),
            _ => throw new InvalidOperationException($"ClosedXML.Parser produced unexpected calculated-column unary operation '{operation}'."),
        };
    }
}
