namespace JetDatabaseWriter.Schema.Expressions;

using System;
using JetDatabaseWriter.Models;

internal sealed class CalculatedExpressionValueNode(object? value) : CalculatedExpressionNode
{
    public override object Evaluate(CalculatedExpressionEvaluationContext context)
        => context.IsCalculatingColumn && value is DateTime date ? NativeDate.FromDateTime(date) : value ?? DBNull.Value;
}
