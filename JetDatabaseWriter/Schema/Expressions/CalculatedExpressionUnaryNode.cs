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

        return operation switch
        {
            UnaryOperation.Plus => ToDecimal(value),
            UnaryOperation.Minus => -ToDecimal(value),
            UnaryOperation.Percent => ToDecimal(value) / 100m,
            UnaryOperation.ImplicitIntersection or UnaryOperation.SpillRange => throw new NotSupportedException(
                $"Calculated-column unary operation '{operation}' is a spreadsheet dynamic-array operation and is not valid in Access calculated columns."),
            _ => throw new InvalidOperationException($"ClosedXML.Parser produced unexpected calculated-column unary operation '{operation}'."),
        };
    }
}
