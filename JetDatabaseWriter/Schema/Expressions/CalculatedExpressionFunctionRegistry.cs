namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;
using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionLimits;

internal static class CalculatedExpressionFunctionRegistry
{
    private static readonly Dictionary<string, CalculatedFunctionDescriptor> FunctionDescriptors = BuildFunctionDescriptors();

    public static object Evaluate(
        string name,
        IReadOnlyList<CalculatedExpressionNode> args,
        CalculatedExpressionEvaluator.EvaluationContext context,
        CalculatedExpressionEvaluator.Plan plan)
    {
        string normalizedName = NormalizeFunctionName(name);
        ValidateFunctionArgumentCount(normalizedName, args.Count);
        if (!FunctionDescriptors.TryGetValue(normalizedName, out var descriptor))
        {
            throw new NotSupportedException($"Calculated-column function '{name}' is not supported.");
        }

        descriptor.ValidateArgumentCount(normalizedName, args.Count);
        return descriptor.Evaluator(new CalculatedFunctionInvocation(name, normalizedName, args, context, plan));
    }

    internal static void AddFunction(Dictionary<string, CalculatedFunctionDescriptor> descriptors, CalculatedFunctionDescriptor descriptor)
    {
        foreach (string name in descriptor.Names)
        {
            string normalizedName = NormalizeFunctionName(name);
            if (descriptors.ContainsKey(normalizedName))
            {
                throw new InvalidOperationException($"Calculated-column function '{normalizedName}' is registered more than once.");
            }

            descriptors.Add(normalizedName, descriptor);
        }
    }

    private static Dictionary<string, CalculatedFunctionDescriptor> BuildFunctionDescriptors()
    {
        var descriptors = new Dictionary<string, CalculatedFunctionDescriptor>(StringComparer.OrdinalIgnoreCase);
        CalculatedExpressionLogicalFunctions.AddFunctions(descriptors);
        CalculatedExpressionTextFunctions.AddFunctions(descriptors);
        CalculatedExpressionDateTimeFunctions.AddFunctions(descriptors);
        CalculatedExpressionNumericFunctions.AddFunctions(descriptors);
        CalculatedExpressionFormattingFunctions.AddFunctions(descriptors);
        CalculatedExpressionFinancialFunctions.AddFunctions(descriptors);
        CalculatedExpressionMetadataFunctions.AddFunctions(descriptors);
        return descriptors;
    }
}
