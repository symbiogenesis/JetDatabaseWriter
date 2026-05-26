namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;

internal readonly struct CalculatedFunctionInvocation
{
    private readonly IReadOnlyList<CalculatedExpressionNode> args;
    private readonly CalculatedExpressionEvaluator.EvaluationContext context;
    private readonly CalculatedExpressionEvaluator.Plan plan;

    public CalculatedFunctionInvocation(
        string name,
        string normalizedName,
        IReadOnlyList<CalculatedExpressionNode> args,
        CalculatedExpressionEvaluator.EvaluationContext context,
        CalculatedExpressionEvaluator.Plan plan)
    {
        Name = name;
        NormalizedName = normalizedName;
        this.args = args;
        this.context = context;
        this.plan = plan;
    }

    public string Name { get; }

    public string NormalizedName { get; }

    public int Count => args.Count;

    public CalculatedExpressionEvaluator.EvaluationContext Context => context;

    public object Arg(int index) => index < args.Count ? args[index].Evaluate(context, plan) : DBNull.Value;
}
