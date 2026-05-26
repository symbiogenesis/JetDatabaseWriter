namespace JetDatabaseWriter.Schema.Expressions;

using System.Collections.Generic;

internal sealed class CalculatedExpressionFunctionNode(string name, IReadOnlyList<CalculatedExpressionNode> args) : CalculatedExpressionNode
{
    public override object Evaluate(CalculatedExpressionEvaluator.EvaluationContext context, CalculatedExpressionEvaluator.Plan plan)
        => CalculatedExpressionFunctionRegistry.Evaluate(name, args, context, plan);
}
