namespace JetDatabaseWriter.Schema.Expressions;

using System;

internal sealed class CalculatedExpressionValueNode(object? value) : CalculatedExpressionNode
{
    public override object Evaluate(CalculatedExpressionEvaluationContext context, CalculatedExpressionPlan plan) => value ?? DBNull.Value;
}
