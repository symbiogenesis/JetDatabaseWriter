namespace JetDatabaseWriter.Schema.Expressions;

using System;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;

/// <summary>Native comparison coercions differ from the arithmetic promotion rules.</summary>
internal static class CalculatedExpressionNumericComparison
{
    internal static bool IsNumber(object value)
        => value is bool or byte or short or int or long or float or double or decimal or AccessCurrency;

    internal static int Compare(object left, object right)
    {
        double leftFloating = ToDouble(left);
        double rightFloating = ToDouble(right);
        if (double.IsNaN(leftFloating) || double.IsNaN(rightFloating))
        {
            // ACE's native comparison reports neither operand less nor greater,
            // including a financial NaN against a finite number.
            return 0;
        }

        if (left is decimal || right is decimal)
        {
            if (TryConvertDecimal(left, out decimal leftDecimal) && TryConvertDecimal(right, out decimal rightDecimal))
            {
                return leftDecimal.CompareTo(rightDecimal);
            }

            if (left is decimal)
            {
                return rightFloating < 0d ? 1 : -1;
            }

            return leftFloating < 0d ? -1 : 1;
        }

        if (left is AccessCurrency && right is float or double)
        {
            return CompareCurrencyToFloating((AccessCurrency)left, rightFloating);
        }

        if (right is AccessCurrency && left is float or double)
        {
            return -CompareCurrencyToFloating((AccessCurrency)right, leftFloating);
        }

        if ((left is float && right is double) || (left is double && right is float))
        {
            return ((float)leftFloating).CompareTo((float)rightFloating);
        }

        if (left is float or double || right is float or double)
        {
            return leftFloating.CompareTo(rightFloating);
        }

        return ToDecimal(left).CompareTo(ToDecimal(right));
    }

    private static int CompareCurrencyToFloating(AccessCurrency currency, double number)
    {
        try
        {
            return currency.Value.CompareTo(AccessCurrency.FromDouble(number).Value);
        }
        catch (OverflowException)
        {
            // An out-of-range floating operand remains ordered beyond Currency's
            // endpoints; converting Currency to Double loses the boundary bits.
            return number < 0d ? 1 : -1;
        }
    }
}
