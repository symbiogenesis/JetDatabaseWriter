namespace JetDatabaseWriter.Schema.Expressions;

using System;

internal sealed class CalculatedExpressionValueNode(object? value) : CalculatedExpressionNode
{
    public override object Evaluate(CalculatedExpressionEvaluator.EvaluationContext context, CalculatedExpressionEvaluator.Plan plan) => value ?? DBNull.Value;
}
