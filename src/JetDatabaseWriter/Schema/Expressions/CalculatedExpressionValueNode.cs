namespace JetDatabaseWriter.Schema.Expressions;

using System;
using JetDatabaseWriter.Models;

internal sealed class CalculatedExpressionValueNode(object? value) : CalculatedExpressionNode
{
    internal override bool HasUntypedNullResult { get; } = value is null or DBNull;

    public override object Evaluate(CalculatedExpressionEvaluationContext context)
    {
        object result = value ?? DBNull.Value;
        if (!context.IsCalculatingColumn)
        {
            return result;
        }

        return result switch
        {
            DateTime date => NativeDate.FromDateTime(date),
            bool boolean => (short)(boolean ? -1 : 0),
            _ => result,
        };
    }
}
