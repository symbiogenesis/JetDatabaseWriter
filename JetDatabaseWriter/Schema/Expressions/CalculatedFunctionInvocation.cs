namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;

internal readonly struct CalculatedFunctionInvocation(
    string name,
    string normalizedName,
    IReadOnlyList<CalculatedExpressionNode> args,
    CalculatedExpressionEvaluator.EvaluationContext context,
    CalculatedExpressionEvaluator.Plan plan)
{
    public string Name { get; } = name;

    public string NormalizedName { get; } = normalizedName;

    public int Count => args.Count;

    public CalculatedExpressionEvaluator.EvaluationContext Context => context;

    public object Arg(int index) => index < args.Count ? args[index].Evaluate(context, plan) : DBNull.Value;
}
