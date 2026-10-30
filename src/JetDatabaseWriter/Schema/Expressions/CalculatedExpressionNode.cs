namespace JetDatabaseWriter.Schema.Expressions;

internal abstract class CalculatedExpressionNode
{
    /// <summary>Checks every syntactic reference, including unused branches, for a stale bracketed table.</summary>
    /// <param name="tableName">The current table name.</param>
    /// <returns>Whether native Access leaves this plan's cache unmaintained.</returns>
    internal virtual bool HasStaleQualifier(string tableName) => false;

    /// <summary>Gets a value indicating whether the native expression result has the untyped Null discriminator.</summary>
    internal virtual bool HasUntypedNullResult => false;

    public abstract object Evaluate(CalculatedExpressionEvaluationContext context);
}
