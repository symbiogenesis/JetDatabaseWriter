namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;
using ClosedXML.Parser;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionLimits;

internal sealed class CalculatedExpressionPlan
{
    private CalculatedExpressionPlan(CalculatedExpressionNode root, Dictionary<string, string> placeholderToColumn)
    {
        this.Root = root;
        this.PlaceholderToColumn = placeholderToColumn;
    }

    public CalculatedExpressionNode Root { get; }

    public Dictionary<string, string> PlaceholderToColumn { get; }

    public static CalculatedExpressionPlan Parse(string expression)
    {
        ValidateExpressionShape(expression, MaxExpressionLength, "Calculated-column expression");
        string normalized = CalculatedExpressionNormalizer.Normalize(expression, out Dictionary<string, string>? placeholderToColumn);
        ValidateExpressionShape(normalized, MaxNormalizedExpressionLength, "Normalized calculated-column expression");
        try
        {
            CalculatedExpressionNode root = FormulaParser<CalculatedExpressionNode, CalculatedExpressionNode, Dictionary<string, string>>.CellFormulaA1(
                normalized,
                placeholderToColumn,
                CalculatedExpressionAstFactory.Instance);
            return new CalculatedExpressionPlan(root, placeholderToColumn);
        }
        catch (ParsingException ex)
        {
            throw new ArgumentException($"Calculated-column expression '{expression}' is not valid expression syntax.", nameof(expression), ex);
        }
    }
}
