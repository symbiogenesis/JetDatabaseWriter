namespace JetDatabaseWriter.Schema.Expressions;

internal abstract class CalculatedExpressionNode
{
    public abstract object Evaluate(CalculatedExpressionEvaluationContext context, CalculatedExpressionPlan plan);
}
