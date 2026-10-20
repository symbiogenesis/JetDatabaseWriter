namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Globalization;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Schema.Models;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;

/// <summary>
/// Builds the exception a calculated column's evaluation failure surfaces as:
/// a structured failure carrying its table and column,
/// with a message that names the table, column and expression (and, when the
/// result could not be stored, the value and result type), and the original
/// exception as <see cref="Exception.InnerException"/>.
/// </summary>
internal static class CalculatedExpressionErrors
{
    /// <summary>
    /// Returns whether <paramref name="exception"/> is a failure this class wraps:
    /// an arithmetic, conversion, argument or unsupported-operation failure.
    /// </summary>
    /// <param name="exception">The exception.</param>
    /// <returns><see langword="true"/> when the exception should be wrapped.</returns>
    internal static bool IsWrappable(Exception exception) =>
        exception is ArithmeticException
            or InvalidCastException
            or FormatException
            or NotSupportedException
            or ArgumentException
            or InvalidOperationException
            or JetLimitationException;

    /// <summary>
    /// Returns whether <paramref name="exception"/> already names a calculated column.
    /// </summary>
    /// <param name="exception">The exception.</param>
    /// <returns><see langword="true"/> when an inner calculated column already wrapped it.</returns>
    internal static bool NamesColumn(Exception exception) => exception is IJetException { ErrorInfo.ColumnName: not null };

    /// <summary>
    /// Wraps an evaluation failure of <paramref name="column"/> in a new exception of
    /// the same category whose message names the column.
    /// </summary>
    /// <param name="column">The calculated column.</param>
    /// <param name="tableName">The table, or <see langword="null"/> when unknown.</param>
    /// <param name="inner">The failure.</param>
    /// <param name="storing">
    /// Whether the expression evaluated and the failure was converting its result
    /// to the column's result type.
    /// </param>
    /// <param name="raw">The evaluated value, when <paramref name="storing"/>.</param>
    /// <returns>The exception to throw.</returns>
    internal static Exception ForColumn(ColumnConstraint column, string? tableName, Exception inner, bool storing, object? raw)
    {
        string where = tableName is null
            ? $"Calculated column '{column.Name}'"
            : $"Calculated column '{column.Name}' on table '{tableName}'";
        string message = storing
            ? $"{where}: expression '{column.CalculationExpression}' evaluated to {Describe(raw)}, which a {ResultTypeName(column)} result cannot hold."
            : $"{where}: expression '{column.CalculationExpression}' could not be evaluated: {inner.Message}";

        var context = new JetErrorInfo { TableName = tableName, ColumnName = column.Name };
        return inner switch
        {
            OverflowException => new JetLimitationException(JetErrorCode.NumericOverflow, message, context, inner),
            JetLimitationException limitation => new JetLimitationException(limitation.ErrorCode, message, context, inner),
            NotSupportedException => new JetNotSupportedException(JetErrorCode.UnsupportedExpression, message, context, inner),
            _ => new JetOperationException(JetErrorCode.InvalidExpression, message, context, inner),
        };
    }

    private static string Describe(object? value)
    {
        if (IsNull(value))
        {
            return "Null";
        }

        return value is string text
            ? "\"" + text + "\""
            : Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
    }

    private static string ResultTypeName(ColumnConstraint column)
        => column.CalculatedResultType != default
            ? JetTypeInfo.GetTypeDisplayName(column.CalculatedResultType)
            : column.ClrType.Name;
}
