namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;
using ClosedXML.Parser;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionLimits;

internal sealed class CalculatedExpressionAstFactory : IAstFactory<CalculatedExpressionNode, CalculatedExpressionNode, Dictionary<string, string>>
{
    public static readonly CalculatedExpressionAstFactory Instance = new();

    public CalculatedExpressionNode LogicalValue(Dictionary<string, string> context, SymbolRange range, bool value) => new CalculatedExpressionValueNode(value);

    public CalculatedExpressionNode NumberValue(Dictionary<string, string> context, SymbolRange range, double value) => new CalculatedExpressionValueNode(value);

    public CalculatedExpressionNode TextValue(Dictionary<string, string> context, SymbolRange range, string value) => new CalculatedExpressionValueNode(value);

    public CalculatedExpressionNode ErrorValue(Dictionary<string, string> context, SymbolRange range, ReadOnlySpan<char> error) => new CalculatedExpressionUnsupportedNode($"Calculated-column error literal '{error.ToString()}' is not supported.");

    public CalculatedExpressionNode ArrayNode(Dictionary<string, string> context, SymbolRange range, int rows, int columns, IReadOnlyList<CalculatedExpressionNode> elements) => new CalculatedExpressionUnsupportedNode("Calculated-column array literals are not supported.");

    public CalculatedExpressionNode BlankNode(Dictionary<string, string> context, SymbolRange range) => new CalculatedExpressionValueNode(DBNull.Value);

    public CalculatedExpressionNode LogicalNode(Dictionary<string, string> context, SymbolRange range, bool value) => new CalculatedExpressionValueNode(value);

    public CalculatedExpressionNode ErrorNode(Dictionary<string, string> context, SymbolRange range, ReadOnlySpan<char> error) => new CalculatedExpressionUnsupportedNode($"Calculated-column error literal '{error.ToString()}' is not supported.");

    public CalculatedExpressionNode NumberNode(Dictionary<string, string> context, SymbolRange range, double value) => new CalculatedExpressionValueNode(value);

    public CalculatedExpressionNode TextNode(Dictionary<string, string> context, SymbolRange range, string value) => new CalculatedExpressionValueNode(value);

    public CalculatedExpressionNode Reference(Dictionary<string, string> context, SymbolRange range, ReferenceArea reference) => new CalculatedExpressionUnsupportedNode("Calculated-column cell references are not supported; use column names instead.");

    public CalculatedExpressionNode SheetReference(Dictionary<string, string> context, SymbolRange range, string sheet, ReferenceArea reference) => new CalculatedExpressionUnsupportedNode("Calculated-column sheet references are not supported.");

    public CalculatedExpressionNode BangReference(Dictionary<string, string> context, SymbolRange range, ReferenceArea reference) => new CalculatedExpressionUnsupportedNode("Calculated-column sheet references are not supported.");

    public CalculatedExpressionNode Reference3D(Dictionary<string, string> context, SymbolRange range, string firstSheet, string lastSheet, ReferenceArea reference) => new CalculatedExpressionUnsupportedNode("Calculated-column 3D references are not supported.");

    public CalculatedExpressionNode ExternalSheetReference(Dictionary<string, string> context, SymbolRange range, int workbookIndex, string sheet, ReferenceArea reference) => new CalculatedExpressionUnsupportedNode("Calculated-column external references are not supported.");

    public CalculatedExpressionNode ExternalReference3D(Dictionary<string, string> context, SymbolRange range, int workbookIndex, string firstSheet, string lastSheet, ReferenceArea reference) => new CalculatedExpressionUnsupportedNode("Calculated-column external references are not supported.");

    public CalculatedExpressionNode Function(Dictionary<string, string> context, SymbolRange range, ReadOnlySpan<char> functionName, IReadOnlyList<CalculatedExpressionNode> args) => CreateFunctionNode(functionName.ToString(), args);

    public CalculatedExpressionNode Function(Dictionary<string, string> context, SymbolRange range, string prefix, ReadOnlySpan<char> functionName, IReadOnlyList<CalculatedExpressionNode> args) => CreateFunctionNode(functionName.ToString(), args);

    public CalculatedExpressionNode ExternalFunction(Dictionary<string, string> context, SymbolRange range, int workbookIndex, string prefix, ReadOnlySpan<char> functionName, IReadOnlyList<CalculatedExpressionNode> args) => new CalculatedExpressionUnsupportedNode("Calculated-column external functions are not supported.");

    public CalculatedExpressionNode ExternalFunction(Dictionary<string, string> context, SymbolRange range, int workbookIndex, ReadOnlySpan<char> functionName, IReadOnlyList<CalculatedExpressionNode> args) => new CalculatedExpressionUnsupportedNode("Calculated-column external functions are not supported.");

    public CalculatedExpressionNode CellFunction(Dictionary<string, string> context, SymbolRange range, RowCol cell, IReadOnlyList<CalculatedExpressionNode> args) => new CalculatedExpressionUnsupportedNode("Calculated-column cell functions are not supported.");

    public CalculatedExpressionNode StructureReference(Dictionary<string, string> context, SymbolRange range, StructuredReferenceArea area, string? firstColumn, string? lastColumn) => new CalculatedExpressionUnsupportedNode("Calculated-column structured references are not supported.");

    public CalculatedExpressionNode StructureReference(Dictionary<string, string> context, SymbolRange range, string table, StructuredReferenceArea area, string? firstColumn, string? lastColumn) => new CalculatedExpressionUnsupportedNode("Calculated-column structured references are not supported.");

    public CalculatedExpressionNode ExternalStructureReference(Dictionary<string, string> context, SymbolRange range, int workbookIndex, string table, StructuredReferenceArea area, string? firstColumn, string? lastColumn) => new CalculatedExpressionUnsupportedNode("Calculated-column structured references are not supported.");

    public CalculatedExpressionNode Name(Dictionary<string, string> context, SymbolRange range, string name) => new CalculatedExpressionNameNode(name);

    public CalculatedExpressionNode SheetName(Dictionary<string, string> context, SymbolRange range, string sheet, string name) => new CalculatedExpressionUnsupportedNode("Calculated-column sheet names are not supported.");

    public CalculatedExpressionNode BangName(Dictionary<string, string> context, SymbolRange range, string name) => new CalculatedExpressionUnsupportedNode("Calculated-column sheet names are not supported.");

    public CalculatedExpressionNode ExternalName(Dictionary<string, string> context, SymbolRange range, int workbookIndex, string name) => new CalculatedExpressionUnsupportedNode("Calculated-column external names are not supported.");

    public CalculatedExpressionNode ExternalSheetName(Dictionary<string, string> context, SymbolRange range, int workbookIndex, string sheet, string name) => new CalculatedExpressionUnsupportedNode("Calculated-column external names are not supported.");

    public CalculatedExpressionNode BinaryNode(Dictionary<string, string> context, SymbolRange range, BinaryOperation operation, CalculatedExpressionNode leftNode, CalculatedExpressionNode rightNode) => new CalculatedExpressionBinaryNode(operation, leftNode, rightNode);

    public CalculatedExpressionNode Unary(Dictionary<string, string> context, SymbolRange range, UnaryOperation operation, CalculatedExpressionNode node) => new CalculatedExpressionUnaryNode(operation, node);

    public CalculatedExpressionNode Nested(Dictionary<string, string> context, SymbolRange range, CalculatedExpressionNode node) => node;

    private static CalculatedExpressionFunctionNode CreateFunctionNode(string functionName, IReadOnlyList<CalculatedExpressionNode> args)
    {
        ValidateFunctionArgumentCount(functionName, args.Count);
        return new CalculatedExpressionFunctionNode(functionName, args);
    }
}
