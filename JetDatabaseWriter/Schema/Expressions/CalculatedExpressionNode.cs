namespace JetDatabaseWriter.Schema.Expressions;

internal abstract class CalculatedExpressionNode
{
    public abstract object Evaluate(CalculatedExpressionEvaluator.EvaluationContext context, CalculatedExpressionEvaluator.Plan plan);
}
