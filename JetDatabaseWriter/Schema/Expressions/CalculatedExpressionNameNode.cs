namespace JetDatabaseWriter.Schema.Expressions;

using System;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;

internal sealed class CalculatedExpressionNameNode(string name) : CalculatedExpressionNode
{
    public override object Evaluate(CalculatedExpressionEvaluator.EvaluationContext context, CalculatedExpressionEvaluator.Plan plan)
    {
        if (TryGetBuiltinConstant(name, out object constantValue))
        {
            return constantValue;
        }

        return name.ToUpperInvariant() switch
        {
            "TRUE" => true,
            "FALSE" => false,
            "YES" => true,
            "NO" => false,
            "ON" => true,
            "OFF" => false,
            "NULL" => DBNull.Value,
            _ => context.GetNameValue(name, plan),
        };
    }
}
