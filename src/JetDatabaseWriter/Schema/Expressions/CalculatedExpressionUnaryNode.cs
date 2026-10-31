namespace JetDatabaseWriter.Schema.Expressions;

using System;
using JetDatabaseWriter.Models;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;

internal sealed class CalculatedExpressionUnaryNode(bool negate, CalculatedExpressionNode operand) : CalculatedExpressionNode
{
    internal override bool HasStaleQualifier(string tableName) => operand.HasStaleQualifier(tableName);

    internal override bool HasUntypedNullResult { get; } = operand.HasUntypedNullResult;

    public override object Evaluate(CalculatedExpressionEvaluationContext context)
    {
        object value = operand.Evaluate(context);
        if (value is CalculatedError)
        {
            return value;
        }

        if (IsNull(value))
        {
            return DBNull.Value;
        }

        try
        {
            if (context.IsCalculatingColumn && value is string)
            {
                // Native unary + retains text; unary - converts it to Double.
                return negate ? -ToDouble(value) : value;
            }

            return negate ? AccessVariantOperators.Negate(value) : AccessVariantOperators.Identity(value);
        }
        catch (OverflowException) when (context.IsCalculatingColumn)
        {
            return new CalculatedError(6);
        }
        catch (Exception exception) when (context.IsCalculatingColumn && exception is InvalidCastException or FormatException)
        {
            return new CalculatedError(13);
        }
    }
}
