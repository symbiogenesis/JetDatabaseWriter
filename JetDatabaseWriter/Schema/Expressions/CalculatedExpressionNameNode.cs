namespace JetDatabaseWriter.Schema.Expressions;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;

internal sealed class CalculatedExpressionNameNode(string name) : CalculatedExpressionNode
{
    public override object Evaluate(CalculatedExpressionEvaluationContext context)
    {
        if (TryGetBuiltinConstant(name, out object constantValue))
        {
            return constantValue;
        }

        return context.GetFieldValue(name);
    }
}
