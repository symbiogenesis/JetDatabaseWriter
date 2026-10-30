namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;
using System.Globalization;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;
using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionFunctionRegistry;

internal static class CalculatedExpressionFinancialFunctions
{
    internal static void AddFunctions(Dictionary<string, CalculatedFunctionDescriptor> functions)
    {
        AddFunction(functions, "FV", 3, 5, static function => FinancialFutureValue(function));
        AddFunction(functions, "PV", 3, 5, static function => FinancialPresentValue(function));
        AddFunction(functions, "PMT", 3, 5, static function => FinancialPayment(function));
        AddFunction(functions, "NPER", 3, 5, static function => FinancialPeriods(function));
        AddFunction(functions, "IPMT", 4, 6, static function => FinancialInterestPayment(function));
        AddFunction(functions, "PPMT", 4, 6, static function => FinancialPrincipalPayment(function));
        AddFunction(functions, "DDB", 4, 5, static function => FinancialDoubleDecliningBalance(function));
        AddFunction(functions, "SLN", 3, 3, static function => FinancialStraightLineDepreciation(function));
        AddFunction(functions, "SYD", 4, 4, static function => FinancialSumOfYearsDepreciation(function));
        AddFunction(functions, "RATE", 3, 6, static function => FinancialRate(function));
    }

    private static double FinancialFutureValue(CalculatedFunctionInvocation function)
        => CalculateFutureValue(ToDouble(function.Arg(0)), ToDouble(function.Arg(1)), ToDouble(function.Arg(2)), GetOptionalDouble(function, 3, 0d), GetPaymentType(function, 4));

    private static double FinancialPresentValue(CalculatedFunctionInvocation function)
        => CalculatePresentValue(ToDouble(function.Arg(0)), ToDouble(function.Arg(1)), ToDouble(function.Arg(2)), GetOptionalDouble(function, 3, 0d), GetPaymentType(function, 4));

    private static double FinancialPayment(CalculatedFunctionInvocation function)
        => CalculatePayment(ToDouble(function.Arg(0)), ToDouble(function.Arg(1)), ToDouble(function.Arg(2)), GetOptionalDouble(function, 3, 0d), GetPaymentType(function, 4));

    private static double FinancialPeriods(CalculatedFunctionInvocation function)
    {
        double rate = ToDouble(function.Arg(0));
        double payment = ToDouble(function.Arg(1));
        double presentValue = ToDouble(function.Arg(2));
        double futureValue = GetOptionalDouble(function, 3, 0d);
        int paymentType = GetPaymentType(function, 4);
        if (rate <= -1d)
        {
            throw new ArgumentException("NPer requires a rate greater than -1.", nameof(function));
        }

        if (rate == 0d)
        {
            if (payment == 0d)
            {
                throw new ArgumentException("NPer requires a nonzero payment when the rate is zero.", nameof(function));
            }

            return -1d * (futureValue + presentValue) / payment;
        }

        double compoundPayment = ((paymentType == 1) ? 1d + rate : 1d) * payment / rate;
        double numerator = -futureValue + compoundPayment;
        double denominator = presentValue + compoundPayment;
        if (numerator < 0d && denominator < 0d)
        {
            numerator = -numerator;
            denominator = -denominator;
        }
        else if (numerator <= 0d || denominator <= 0d)
        {
            throw new ArgumentException("NPer cannot calculate a period count for these cash flows.", nameof(function));
        }

        return (Math.Log(numerator) - Math.Log(denominator)) / Math.Log(1d + rate);
    }

    private static double FinancialInterestPayment(CalculatedFunctionInvocation function)
    {
        double rate = ToDouble(function.Arg(0));
        double period = ToDouble(function.Arg(1));
        double periods = ToDouble(function.Arg(2));
        double presentValue = ToDouble(function.Arg(3));
        double futureValue = GetOptionalDouble(function, 4, 0d);
        int paymentType = GetPaymentType(function, 5);
        return CalculateInterestPayment(rate, period, periods, presentValue, futureValue, paymentType);
    }

    private static double CalculateInterestPayment(double rate, double period, double periods, double presentValue, double futureValue, int paymentType)
    {
        if (period <= 0d || period >= periods + 1d)
        {
            throw new ArgumentException("IPmt and PPmt require a positive period less than NPer + 1.", nameof(period));
        }

        if (period == 1d && paymentType == 1)
        {
            return 0d;
        }

        double payment = CalculatePayment(rate, periods, presentValue, futureValue, paymentType);
        if (paymentType == 1)
        {
            presentValue += payment;
        }

        return CalculateFutureValue(rate, period - (paymentType == 1 ? 2d : 1d), payment, presentValue, 0) * rate;
    }

    private static double FinancialPrincipalPayment(CalculatedFunctionInvocation function)
    {
        double rate = ToDouble(function.Arg(0));
        double period = ToDouble(function.Arg(1));
        double periods = ToDouble(function.Arg(2));
        double presentValue = ToDouble(function.Arg(3));
        double futureValue = GetOptionalDouble(function, 4, 0d);
        int paymentType = GetPaymentType(function, 5);
        double payment = CalculatePayment(rate, periods, presentValue, futureValue, paymentType);
        double interestPayment = CalculateInterestPayment(rate, period, periods, presentValue, futureValue, paymentType);
        return payment - interestPayment;
    }

    private static double FinancialDoubleDecliningBalance(CalculatedFunctionInvocation function)
    {
        double cost = ToDouble(function.Arg(0));
        double salvage = ToDouble(function.Arg(1));
        double life = ToDouble(function.Arg(2));
        double period = ToDouble(function.Arg(3));
        double factor = GetOptionalDouble(function, 4, 2d);
        if (factor <= 0d || salvage < 0d || period <= 0d || period > life)
        {
            throw new ArgumentException("DDB requires a positive factor and period, a nonnegative salvage, and a period within the asset life.", nameof(function));
        }

        if (cost <= 0d || (life == 2d && period > 1d))
        {
            return 0d;
        }

        if (life < 2d || (life == 2d && period <= 1d))
        {
            return cost - salvage;
        }

        double firstPeriod = factor * cost / life;
        if (period <= 1d)
        {
            return Math.Min(firstPeriod, cost - salvage);
        }

        double decline = (life - factor) / life;
        double salvageAdjustment = Math.Max(salvage - (cost * Math.Pow(decline, period)), 0d);
        return Math.Max((firstPeriod * Math.Pow(decline, period - 1d)) - salvageAdjustment, 0d);
    }

    private static double FinancialStraightLineDepreciation(CalculatedFunctionInvocation function)
    {
        double cost = ToDouble(function.Arg(0));
        double salvage = ToDouble(function.Arg(1));
        double life = ToDouble(function.Arg(2));
        if (life == 0d)
        {
            throw new ArgumentException("SLN requires a nonzero asset life.", nameof(function));
        }

        return (cost - salvage) / life;
    }

    private static double FinancialSumOfYearsDepreciation(CalculatedFunctionInvocation function)
    {
        double cost = ToDouble(function.Arg(0));
        double salvage = ToDouble(function.Arg(1));
        double life = ToDouble(function.Arg(2));
        double period = ToDouble(function.Arg(3));
        if (salvage < 0d || period <= 0d || period > life)
        {
            throw new ArgumentException("SYD requires a nonnegative salvage and a positive period within the asset life.", nameof(function));
        }

        return (cost - salvage) * (life - period + 1d) * 2d / (life * (life + 1d));
    }

    private static double FinancialRate(CalculatedFunctionInvocation function)
    {
        double periods = ToDouble(function.Arg(0));
        double payment = ToDouble(function.Arg(1));
        double presentValue = ToDouble(function.Arg(2));
        double futureValue = GetOptionalDouble(function, 3, 0d);
        int paymentType = GetPaymentType(function, 4);
        double firstRate = GetOptionalDouble(function, 5, 0.1d);
        if (periods <= 0d)
        {
            throw new ArgumentException("Rate requires a positive period count.", nameof(function));
        }

        // Bounded secant control is adapted from .NET Foundation Financial.vb (MIT):
        // https://github.com/dotnet/runtime/blob/2201016c1e13bdb9abf49e2e38cadf4ee0568df2/src/libraries/Microsoft.VisualBasic.Core/src/Microsoft/VisualBasic/Financial.vb
        double firstValue = CalculateRateValue(firstRate, periods, payment, presentValue, futureValue, paymentType);
        double secondRate = firstValue > 0d ? firstRate / 2d : firstRate * 2d;
        double secondValue = CalculateRateValue(secondRate, periods, payment, presentValue, futureValue, paymentType);
        for (int iteration = 0; iteration < 40; iteration++)
        {
            if (secondValue == firstValue)
            {
                firstRate += secondRate > firstRate ? -0.00001d : 0.00001d;
                firstValue = CalculateRateValue(firstRate, periods, payment, presentValue, futureValue, paymentType);
                if (secondValue == firstValue)
                {
                    throw new ArgumentException("Rate cannot calculate a distinct secant value.", nameof(function));
                }
            }

            firstRate = secondRate - ((secondRate - firstRate) * secondValue / (secondValue - firstValue));
            firstValue = CalculateRateValue(firstRate, periods, payment, presentValue, futureValue, paymentType);
            if (Math.Abs(firstValue) < 0.0000001d)
            {
                return firstRate;
            }

            (firstValue, secondValue) = (secondValue, firstValue);
            (firstRate, secondRate) = (secondRate, firstRate);
        }

        throw new ArgumentException("Rate did not converge within the native iteration bound.", nameof(function));
    }

    private static double CalculateRateValue(double rate, double periods, double payment, double presentValue, double futureValue, int paymentType)
    {
        if (rate == 0d)
        {
            return presentValue + (payment * periods) + futureValue;
        }

        double compound = Math.Pow(rate + 1d, periods);
        double paymentFactor = paymentType == 1 ? 1d + rate : 1d;
        return (presentValue * compound) + (payment * paymentFactor * (compound - 1d) / rate) + futureValue;
    }

    private static double GetOptionalDouble(CalculatedFunctionInvocation function, int index, double defaultValue)
        => function.Count > index ? ToDouble(function.Arg(index)) : defaultValue;

    private static int GetPaymentType(CalculatedFunctionInvocation function, int index)
        => function.Count > index && Convert.ToInt16(AsAccessNumber(function.Arg(index)), CultureInfo.InvariantCulture) != 0 ? 1 : 0;

    private static double CalculateFutureValue(double rate, double periods, double payment, double presentValue, int paymentType)
    {
        if (rate == 0d)
        {
            return -1d * (presentValue + (periods * payment));
        }

        double paymentFactor = paymentType == 1 ? rate + 1d : 1d;
        double compound = Math.Pow(rate + 1d, periods);
        return ((1d - compound) * paymentFactor * payment / rate) - (presentValue * compound);
    }

    private static double CalculatePresentValue(double rate, double periods, double payment, double futureValue, int paymentType)
    {
        if (rate == 0d)
        {
            return -1d * ((periods * payment) + futureValue);
        }

        double paymentFactor = paymentType == 1 ? rate + 1d : 1d;
        double compound = Math.Pow(rate + 1d, periods);
        return (((1d - compound) * paymentFactor * payment / rate) - futureValue) / compound;
    }

    private static double CalculatePayment(double rate, double periods, double presentValue, double futureValue, int paymentType)
    {
        if (periods == 0d)
        {
            throw new ArgumentException("Pmt requires a nonzero period count.", nameof(periods));
        }

        if (rate == 0d)
        {
            return -1d * (futureValue + presentValue) / periods;
        }

        double paymentFactor = paymentType == 1 ? rate + 1d : 1d;
        double compound = Math.Pow(rate + 1d, periods);
        return (-futureValue - (presentValue * compound)) / (paymentFactor * (compound - 1d)) * rate;
    }
}
