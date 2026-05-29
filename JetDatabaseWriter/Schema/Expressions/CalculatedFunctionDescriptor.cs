namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionLimits;

internal sealed class CalculatedFunctionDescriptor(CalculatedFunctionDomain domain, string name, int minArgs, int maxArgs, CalculatedFunctionEvaluator evaluator, params string[] aliases)
{
    public CalculatedFunctionDomain Domain { get; } = domain;

    public string Name { get; } = name;

    public int MinArgs { get; } = minArgs;

    public int MaxArgs { get; } = maxArgs;

    public CalculatedFunctionEvaluator Evaluator { get; } = evaluator;

    public string[] Aliases { get; } = aliases;

    public IEnumerable<string> Names
    {
        get
        {
            yield return this.Name;
            for (int i = 0; i < this.Aliases.Length; i++)
            {
                yield return this.Aliases[i];
            }
        }
    }

    public void ValidateArgumentCount(string functionName, int actual)
    {
        int effectiveMax = Math.Min(this.MaxArgs, MaxFunctionArguments);
        if (actual < this.MinArgs || actual > effectiveMax)
        {
            throw new ArgumentException(
                $"Calculated-column function '{functionName}' expects {this.MinArgs}" + (this.MinArgs == effectiveMax ? string.Empty : $"..{effectiveMax}") + $" argument(s), got {actual}.");
        }
    }
}
