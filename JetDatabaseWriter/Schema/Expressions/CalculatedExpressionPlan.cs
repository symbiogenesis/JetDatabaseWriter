namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;
using ClosedXML.Parser;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionLimits;

internal sealed class CalculatedExpressionPlan
{
    private CalculatedExpressionPlan(CalculatedExpressionNode root, Dictionary<string, string> placeholderToColumn, Dictionary<string, string> placeholderToTable, HashSet<string> rowLocalQualifiedNames)
    {
        this.Root = root;
        this.PlaceholderToColumn = placeholderToColumn;
        this.PlaceholderToTable = placeholderToTable;
        this.RowLocalQualifiedNames = rowLocalQualifiedNames;
    }

    public CalculatedExpressionNode Root { get; }

    public Dictionary<string, string> PlaceholderToColumn { get; }

    public Dictionary<string, string> PlaceholderToTable { get; }

    public HashSet<string> RowLocalQualifiedNames { get; }

    public static CalculatedExpressionPlan Parse(string expression, bool isCalculatedColumn = true)
    {
        ValidateExpressionShape(expression, MaxExpressionLength, "Calculated-column expression");
        string normalized = CalculatedExpressionNormalizer.Normalize(expression, out Dictionary<string, string> placeholderToColumn, out Dictionary<string, string> placeholderToTable, out HashSet<string> functions, out HashSet<string> rowLocalQualifiedNames);
        ValidateExpressionShape(normalized, MaxNormalizedExpressionLength, "Normalized calculated-column expression");
        if (!isCalculatedColumn && placeholderToTable.Count != 0)
        {
            throw new NotSupportedException("Access table and field rules and defaults do not accept table-qualified field references.");
        }

        try
        {
            CalculatedExpressionNode root = FormulaParser<CalculatedExpressionNode, CalculatedExpressionNode, Dictionary<string, string>>.CellFormulaA1(
                normalized,
                placeholderToColumn,
                CalculatedExpressionAstFactory.Instance);
            if (isCalculatedColumn)
            {
                foreach (string function in functions)
                {
                    if (function.ToUpperInvariant() is "PARTITION" or "CBOOL" or "CBYTE" or "CINT" or "CLNG" or "CSNG" or "CCUR" or "CDEC" or "CSTR" or "CDATE" or "TIMEVALUE" or "DATEVALUE")
                    {
                        throw new NotSupportedException($"Access table calculated columns do not accept the {function} function.");
                    }
                }
            }

            return new CalculatedExpressionPlan(root, placeholderToColumn, placeholderToTable, rowLocalQualifiedNames);
        }
        catch (ParsingException ex)
        {
            throw new ArgumentException($"Calculated-column expression '{expression}' is not valid expression syntax.", ex);
        }
    }
}
