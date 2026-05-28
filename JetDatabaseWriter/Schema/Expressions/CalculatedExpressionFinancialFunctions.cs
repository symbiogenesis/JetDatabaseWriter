namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;
using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionFunctionRegistry;

internal static class CalculatedExpressionFinancialFunctions
{
    internal static void AddFunctions(Dictionary<string, CalculatedFunctionDescriptor> functions)
    {
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Financial, "FV", 3, 5, static function => FinancialFutureValue(function)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Financial, "PV", 3, 5, static function => FinancialPresentValue(function)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Financial, "PMT", 3, 5, static function => FinancialPayment(function)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Financial, "NPER", 3, 5, static function => FinancialPeriods(function)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Financial, "IPMT", 4, 6, static function => FinancialInterestPayment(function)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Financial, "PPMT", 4, 6, static function => FinancialPrincipalPayment(function)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Financial, "DDB", 4, 5, static function => FinancialDoubleDecliningBalance(function)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Financial, "SLN", 3, 3, static function => (ToDouble(function.Arg(0)) - ToDouble(function.Arg(1))) / ToDouble(function.Arg(2))));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Financial, "SYD", 4, 4, static function => FinancialSumOfYearsDepreciation(function)));
        AddFunction(functions, new CalculatedFunctionDescriptor(CalculatedFunctionDomain.Financial, "RATE", 3, 6, static function => FinancialRate(function)));
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
        if (rate == 0d)
        {
            return -1d * (futureValue + presentValue) / payment;
        }

        double compoundPayment = ((paymentType == 1) ? 1d + rate : 1d) * payment / rate;
        double numerator = Math.Log(Math.Abs(futureValue - compoundPayment));
        double denominator = Math.Log(Math.Abs(-presentValue - compoundPayment));
        return (numerator - denominator) / Math.Log(1d + rate);
    }

    private static double FinancialInterestPayment(CalculatedFunctionInvocation function)
    {
        double rate = ToDouble(function.Arg(0));
        double period = ToDouble(function.Arg(1));
        double periods = ToDouble(function.Arg(2));
        double presentValue = ToDouble(function.Arg(3));
        double futureValue = GetOptionalDouble(function, 4, 0d);
        int paymentType = GetPaymentType(function, 5);
        if (period == 1d && paymentType == 1)
        {
            return 0d;
        }

        double payment = CalculatePayment(rate, periods, presentValue, futureValue, paymentType);
        double result = CalculateFutureValue(rate, period - 1d, payment, presentValue, paymentType) * rate;
        return paymentType == 1 ? result / (1d + rate) : result;
    }

    private static double FinancialPrincipalPayment(CalculatedFunctionInvocation function)
    {
        double rate = ToDouble(function.Arg(0));
        double periods = ToDouble(function.Arg(2));
        double presentValue = ToDouble(function.Arg(3));
        double futureValue = GetOptionalDouble(function, 4, 0d);
        int paymentType = GetPaymentType(function, 5);
        double payment = CalculatePayment(rate, periods, presentValue, futureValue, paymentType);
        double interestPayment = FinancialInterestPayment(function);
        return payment - interestPayment;
    }

    private static double FinancialDoubleDecliningBalance(CalculatedFunctionInvocation function)
    {
        double cost = ToDouble(function.Arg(0));
        double salvage = ToDouble(function.Arg(1));
        double life = ToDouble(function.Arg(2));
        double period = ToDouble(function.Arg(3));
        double factor = GetOptionalDouble(function, 4, 2d);
        if (cost < 0d || (life == 2d && period > 1d))
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

    private static double FinancialSumOfYearsDepreciation(CalculatedFunctionInvocation function)
    {
        double cost = ToDouble(function.Arg(0));
        double salvage = ToDouble(function.Arg(1));
        double life = ToDouble(function.Arg(2));
        double period = ToDouble(function.Arg(3));
        return (cost - salvage) * (life - period + 1d) * 2d / (life * (life + 1d));
    }

    private static double FinancialRate(CalculatedFunctionInvocation function)
    {
        double periods = ToDouble(function.Arg(0));
        double payment = ToDouble(function.Arg(1));
        double presentValue = ToDouble(function.Arg(2));
        double futureValue = GetOptionalDouble(function, 3, 0d);
        int paymentType = GetPaymentType(function, 4);
        double rate = GetOptionalDouble(function, 5, 0.1d);
        double previousRate = 0d;
        double previousValue = presentValue + (payment * periods) + futureValue;
        for (int iteration = 0; iteration < 20; iteration++)
        {
            double factor = Math.Abs(rate) < 0.0000001d ? 1d + (periods * rate) : Math.Pow(1d + rate, periods);
            double currentValue = Math.Abs(rate) < 0.0000001d
                ? (presentValue * (1d + (periods * rate))) + (payment * (1d + (rate * paymentType)) * periods) + futureValue
                : (presentValue * factor) + (payment * ((1d / rate) + paymentType) * (factor - 1d)) + futureValue;
            if (Math.Abs(previousValue - currentValue) <= 0.0000001d)
            {
                return rate;
            }

            double nextRate = ((currentValue * previousRate) - (previousValue * rate)) / (currentValue - previousValue);
            previousRate = rate;
            previousValue = currentValue;
            rate = nextRate;
        }

        return rate;
    }

    private static double GetOptionalDouble(CalculatedFunctionInvocation function, int index, double defaultValue)
        => function.Count > index ? ToDouble(function.Arg(index)) : defaultValue;

    private static int GetPaymentType(CalculatedFunctionInvocation function, int index)
        => function.Count > index && ToDecimal(function.Arg(index)) != 0m ? 1 : 0;

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
        return (((1d - compound) / rate * paymentFactor * payment) - futureValue) / compound;
    }

    private static double CalculatePayment(double rate, double periods, double presentValue, double futureValue, int paymentType)
    {
        if (rate == 0d)
        {
            return -1d * (futureValue + presentValue) / periods;
        }

        double paymentFactor = paymentType == 1 ? rate + 1d : 1d;
        double compound = Math.Pow(rate + 1d, periods);
        return (futureValue + (presentValue * compound)) * rate / (paymentFactor * (1d - compound));
    }
}
