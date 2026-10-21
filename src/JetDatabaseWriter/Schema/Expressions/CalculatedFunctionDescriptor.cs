namespace JetDatabaseWriter.Schema.Expressions;

using System;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionLimits;

internal sealed class CalculatedFunctionDescriptor(int minArgs, int maxArgs, CalculatedFunctionEvaluator evaluator)
{
    public CalculatedFunctionEvaluator Evaluator { get; } = evaluator;

    public void ValidateArgumentCount(string functionName, int actual)
    {
        int effectiveMax = Math.Min(maxArgs, MaxFunctionArguments);
        if (actual < minArgs || actual > effectiveMax)
        {
            throw new ArgumentException(
                $"Calculated-column function '{functionName}' expects {minArgs}" + (minArgs == effectiveMax ? string.Empty : $"..{effectiveMax}") + $" argument(s), got {actual}.");
        }
    }
}
