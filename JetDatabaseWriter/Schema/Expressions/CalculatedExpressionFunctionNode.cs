namespace JetDatabaseWriter.Schema.Expressions;

using System.Collections.Generic;

internal sealed class CalculatedExpressionFunctionNode(string name, IReadOnlyList<CalculatedExpressionNode> args) : CalculatedExpressionNode
{
    public override object Evaluate(CalculatedExpressionEvaluationContext context, CalculatedExpressionPlan plan)
        => CalculatedExpressionFunctionRegistry.Evaluate(name, args, context, plan);
}
