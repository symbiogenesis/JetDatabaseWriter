namespace JetDatabaseWriter.Schema.Expressions;

using System;

internal sealed class CalculatedExpressionUnsupportedNode(string reason) : CalculatedExpressionNode
{
    public override object Evaluate(CalculatedExpressionEvaluationContext context, CalculatedExpressionPlan plan) => throw new NotSupportedException(reason);
}
