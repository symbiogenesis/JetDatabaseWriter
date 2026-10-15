namespace JetDatabaseWriter.Schema.Expressions;

using System;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;

internal sealed class CalculatedExpressionUnaryNode(bool negate, CalculatedExpressionNode operand) : CalculatedExpressionNode
{
    public override object Evaluate(CalculatedExpressionEvaluationContext context)
    {
        object value = operand.Evaluate(context);
        if (IsNull(value))
        {
            return DBNull.Value;
        }

        return negate ? AccessVariantOperators.Negate(value) : AccessVariantOperators.Identity(value);
    }
}
