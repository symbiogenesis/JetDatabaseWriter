namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;

/// <summary>Binds all syntactic dependencies before a calculated declaration or mutation.</summary>
internal static class CalculatedExpressionBindings
{
    /// <summary>Validates a declaration against the complete available column set.</summary>
    /// <param name="columns">The final table's columns, including persisted dependencies.</param>
    /// <param name="paramName">The declaration argument name.</param>
    /// <param name="rootIndex">The newly added column, or -1 to check every calculated column.</param>
    /// <exception cref="ArgumentException">A field is absent, prohibited as a source, or part of a cycle.</exception>
    internal static void ValidateDeclarations(IReadOnlyList<ColumnDefinition> columns, string paramName, int rootIndex = -1)
    {
        var bindings = new Binding[columns.Count];
        for (int i = 0; i < columns.Count; i++)
        {
            ColumnDefinition column = columns[i];
            ColumnType type = column.SourceColumn is { } source ? JetTypeInfo.ResolveValueType(source) : JetTypeInfo.TypeCodeFromDefinition(column, paramName);
            bindings[i] = new(column.Name, type, column.IsCalculated, column.CalculationExpression);
        }

        try
        {
            Validate(bindings, rootIndex);
        }
        catch (Exception ex) when (ex is ArgumentException or NotSupportedException)
        {
            throw new ArgumentException(ex.Message, paramName, ex);
        }
    }

    /// <summary>Validates persisted expression dependencies before compiling a write plan.</summary>
    /// <param name="columns">The hydrated constraints, with effective result types.</param>
    /// <param name="rootIndex">The calculated column being compiled.</param>
    internal static void ValidatePlan(IReadOnlyList<ColumnConstraint> columns, int rootIndex)
    {
        var bindings = new Binding[columns.Count];
        for (int i = 0; i < columns.Count; i++)
        {
            ColumnConstraint column = columns[i];
            ColumnType type = column.IsCalculated && column.CalculatedResultType != default ? column.CalculatedResultType : column.StorageType;
            bindings[i] = new(column.Name, type, column.IsCalculated, column.CalculationExpression);
        }

        Validate(bindings, rootIndex);
    }

    private static void Validate(Binding[] columns, int rootIndex)
    {
        var indexes = new Dictionary<string, int>(columns.Length, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < columns.Length; i++)
        {
            indexes[columns[i].Name] = i;
        }

        byte[] state = new byte[columns.Length];
        if (rootIndex >= 0)
        {
            Visit(columns, indexes, state, rootIndex);
        }
        else
        {
            for (int i = 0; i < columns.Length; i++)
            {
                Visit(columns, indexes, state, i);
            }
        }
    }

    private static void Visit(Binding[] columns, Dictionary<string, int> indexes, byte[] state, int index)
    {
        Binding column = columns[index];
        if (!column.IsCalculated || state[index] == 2)
        {
            return;
        }

        if (state[index] == 1)
        {
            throw new ArgumentException($"Calculated column '{column.Name}' participates in a circular expression dependency.");
        }

        string expression = column.Expression ?? throw new ArgumentException($"Calculated column '{column.Name}' has no calculation expression.");
        CalculatedExpressionLimits.ValidateExpressionShape(expression, CalculatedExpressionLimits.MaxExpressionLength, "Calculated-column expression");
        state[index] = 1;
        List<CalculatedExpressionParser.Token> tokens = CalculatedExpressionParser.Tokenize(expression, isCalculatedColumn: true, skipLeadingEquals: true);
        for (int i = 0; i < tokens.Count; i++)
        {
            string? name = GetReferencedName(tokens, i);
            if (name is null)
            {
                continue;
            }

            if (!indexes.TryGetValue(name, out int referencedIndex))
            {
                throw new ArgumentException($"Calculated column '{column.Name}' references unknown field '{name}'.");
            }

            Binding referenced = columns[referencedIndex];
            if (referenced.Type is ColumnType.BinaryType or ColumnType.OleType or ColumnType.BigBinaryType or ColumnType.ComplexType)
            {
                throw new JetNotSupportedException(JetErrorCode.UnsupportedExpression, $"Calculated column '{column.Name}' cannot reference field '{name}' of type {JetTypeInfo.GetTypeDisplayName(referenced.Type)}; Access excludes binary and multi-value source fields.");
            }

            Visit(columns, indexes, state, referencedIndex);
        }

        state[index] = 2;
    }

    private static string? GetReferencedName(List<CalculatedExpressionParser.Token> tokens, int index)
    {
        CalculatedExpressionParser.Token token = tokens[index];
        if (token.Node is CalculatedExpressionFieldNode field)
        {
            return field.Name;
        }

        return token.Kind == CalculatedExpressionParser.TokenKind.Identifier && (index + 1 == tokens.Count || tokens[index + 1].Kind != CalculatedExpressionParser.TokenKind.OpenParen) ? token.Text : null;
    }

    private readonly record struct Binding(string Name, ColumnType Type, bool IsCalculated, string? Expression);
}
