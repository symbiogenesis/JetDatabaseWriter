namespace JetDatabaseWriter.Schema.Expressions;

using System;

internal sealed class CalculatedExpressionUnsupportedNode(string reason) : CalculatedExpressionNode
{
    public override object Evaluate(CalculatedExpressionEvaluator.EvaluationContext context, CalculatedExpressionEvaluator.Plan plan) => throw new NotSupportedException(reason);
}
