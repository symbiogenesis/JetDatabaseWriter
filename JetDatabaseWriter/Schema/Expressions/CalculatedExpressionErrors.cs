namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Globalization;
using JetDatabaseWriter.Schema.Models;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;

/// <summary>
/// Builds the exception a calculated column's evaluation failure surfaces as:
/// the same exception category as the failure, so callers and
/// <see cref="ColumnValidationRule.IsEvaluationFailure"/> classify it as before,
/// with a message that names the table, column and expression (and, when the
/// result could not be stored, the value and result type), and the original
/// exception as <see cref="Exception.InnerException"/>.
/// </summary>
internal static class CalculatedExpressionErrors
{
    /// <summary>
    /// The <see cref="Exception.Data"/> key set on an exception that already names
    /// a calculated column, so a column whose expression references another
    /// calculated column does not wrap the inner column's error again.
    /// </summary>
    internal const string ColumnDataKey = "JetDatabaseWriter.CalculatedColumn";

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
            or InvalidOperationException;

    /// <summary>
    /// Returns whether <paramref name="exception"/> already names a calculated column.
    /// </summary>
    /// <param name="exception">The exception.</param>
    /// <returns><see langword="true"/> when an inner calculated column already wrapped it.</returns>
    internal static bool NamesColumn(Exception exception) => exception.Data.Contains(ColumnDataKey);

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

        Exception wrapped = inner switch
        {
            OverflowException => new OverflowException(message, inner),
            DivideByZeroException => new DivideByZeroException(message, inner),
            ArithmeticException => new ArithmeticException(message, inner),
            InvalidCastException => new InvalidCastException(message, inner),
            FormatException => new FormatException(message, inner),
            NotSupportedException => new NotSupportedException(message, inner),
            ArgumentException => new ArgumentException(message, inner),
            _ => new InvalidOperationException(message, inner),
        };
        wrapped.Data[ColumnDataKey] = column.Name;
        return wrapped;
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
