namespace JetDatabaseWriter.Schema.Expressions;

using System;
using JetDatabaseWriter.Models;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;

internal sealed class CalculatedExpressionUnaryNode(bool negate, CalculatedExpressionNode operand) : CalculatedExpressionNode
{
    internal override bool HasStaleQualifier(string tableName) => operand.HasStaleQualifier(tableName);

    internal override bool HasUntypedNullResult { get; } = operand.HasUntypedNullResult;

    public override object Evaluate(CalculatedExpressionEvaluationContext context)
    {
        object value = operand.Evaluate(context);
        if (value is CalculatedError)
        {
            return value;
        }

        if (IsNull(value))
        {
            return DBNull.Value;
        }

        return negate ? AccessVariantOperators.Negate(value) : AccessVariantOperators.Identity(value);
    }
}
