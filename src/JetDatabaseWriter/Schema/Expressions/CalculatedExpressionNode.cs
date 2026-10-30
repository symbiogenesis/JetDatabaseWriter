namespace JetDatabaseWriter.Schema.Expressions;

internal abstract class CalculatedExpressionNode
{
    /// <summary>Gets a value indicating whether the native expression result has the untyped Null discriminator.</summary>
    internal virtual bool HasUntypedNullResult => false;

    public abstract object Evaluate(CalculatedExpressionEvaluationContext context);
}
