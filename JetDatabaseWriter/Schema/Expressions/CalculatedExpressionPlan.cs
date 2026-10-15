namespace JetDatabaseWriter.Schema.Expressions;

internal sealed class CalculatedExpressionPlan(CalculatedExpressionNode root)
{
    public CalculatedExpressionNode Root { get; } = root;

    public static CalculatedExpressionPlan Parse(string expression, bool isCalculatedColumn = true)
        => new(CalculatedExpressionParser.Parse(expression, isCalculatedColumn));
}
