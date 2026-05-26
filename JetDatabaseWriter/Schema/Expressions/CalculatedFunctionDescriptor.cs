namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionLimits;

internal sealed class CalculatedFunctionDescriptor
{
    public CalculatedFunctionDescriptor(CalculatedFunctionDomain domain, string name, int minArgs, int maxArgs, CalculatedFunctionEvaluator evaluator, params string[] aliases)
    {
        Domain = domain;
        Name = name;
        MinArgs = minArgs;
        MaxArgs = maxArgs;
        Evaluator = evaluator;
        Aliases = aliases;
    }

    public CalculatedFunctionDomain Domain { get; }

    public string Name { get; }

    public int MinArgs { get; }

    public int MaxArgs { get; }

    public CalculatedFunctionEvaluator Evaluator { get; }

    public string[] Aliases { get; }

    public IEnumerable<string> Names
    {
        get
        {
            yield return Name;
            for (int i = 0; i < Aliases.Length; i++)
            {
                yield return Aliases[i];
            }
        }
    }

    public void ValidateArgumentCount(string functionName, int actual)
    {
        int effectiveMax = Math.Min(MaxArgs, MaxFunctionArguments);
        if (actual < MinArgs || actual > effectiveMax)
        {
            throw new ArgumentException(
                $"Calculated-column function '{functionName}' expects {MinArgs}" + (MinArgs == effectiveMax ? string.Empty : $"..{effectiveMax}") + $" argument(s), got {actual}.");
        }
    }
}
