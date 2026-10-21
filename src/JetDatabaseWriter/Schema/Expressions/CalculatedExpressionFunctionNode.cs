namespace JetDatabaseWriter.Schema.Expressions;

using System.Collections.Generic;

internal sealed class CalculatedExpressionFunctionNode(string name, IReadOnlyList<CalculatedExpressionNode> args) : CalculatedExpressionNode
{
    public override object Evaluate(CalculatedExpressionEvaluationContext context)
        => CalculatedExpressionFunctionRegistry.Evaluate(name, args, context);
}
