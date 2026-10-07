namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;
using ClosedXML.Parser;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionLimits;

internal sealed class CalculatedExpressionPlan
{
    private CalculatedExpressionPlan(CalculatedExpressionNode root, Dictionary<string, string> placeholderToColumn, Dictionary<string, string> placeholderToTable)
    {
        this.Root = root;
        this.PlaceholderToColumn = placeholderToColumn;
        this.PlaceholderToTable = placeholderToTable;
    }

    public CalculatedExpressionNode Root { get; }

    public Dictionary<string, string> PlaceholderToColumn { get; }

    public Dictionary<string, string> PlaceholderToTable { get; }

    public static CalculatedExpressionPlan Parse(string expression, bool allowQualifiedReferences = true)
    {
        ValidateExpressionShape(expression, MaxExpressionLength, "Calculated-column expression");
        string normalized = CalculatedExpressionNormalizer.Normalize(expression, out Dictionary<string, string> placeholderToColumn, out Dictionary<string, string> placeholderToTable);
        ValidateExpressionShape(normalized, MaxNormalizedExpressionLength, "Normalized calculated-column expression");
        if (!allowQualifiedReferences && placeholderToTable.Count != 0)
        {
            throw new NotSupportedException("Access table and field rules and defaults do not accept table-qualified field references.");
        }

        try
        {
            CalculatedExpressionNode root = FormulaParser<CalculatedExpressionNode, CalculatedExpressionNode, Dictionary<string, string>>.CellFormulaA1(
                normalized,
                placeholderToColumn,
                CalculatedExpressionAstFactory.Instance);
            return new CalculatedExpressionPlan(root, placeholderToColumn, placeholderToTable);
        }
        catch (ParsingException ex)
        {
            throw new ArgumentException($"Calculated-column expression '{expression}' is not valid expression syntax.", ex);
        }
    }
}
