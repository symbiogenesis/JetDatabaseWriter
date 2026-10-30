namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Globalization;
using JetDatabaseWriter.Models;

/// <summary>Table expressions coerce logical operands to Boolean before AND, OR and NOT.</summary>
internal static class CalculatedExpressionLogicalOperand
{
    internal static object Normalize(object value) => value switch
    {
        DBNull or CalculatedError or bool => value,
        string text => !double.TryParse(text, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out double number) || FloatingTruth(number),
        Guid => true,
        NativeDate date => FloatingTruth(date.Serial),
        DateTime date => FloatingTruth(date.ToOADate()),
        AccessCurrency currency => currency.Value is < -0.5m or > 0.5m,
        decimal number => number is < -0.5m or > 0.5m,
        _ => FloatingTruth(CalculatedExpressionCoercion.ToDouble(value)),
    };

    private static bool FloatingTruth(double value) => double.IsNaN(value) || value is < -0.5d or > 0.5d;
}
