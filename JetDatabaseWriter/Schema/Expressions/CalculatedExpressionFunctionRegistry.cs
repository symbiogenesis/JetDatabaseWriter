namespace JetDatabaseWriter.Schema.Expressions;

using System;
#if NET8_0_OR_GREATER
using System.Collections.Frozen;
#endif
using System.Collections.Generic;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;
using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionLimits;

internal static class CalculatedExpressionFunctionRegistry
{
#if NET8_0_OR_GREATER
    private static readonly FrozenDictionary<string, CalculatedFunctionDescriptor> FunctionDescriptors = BuildFunctionDescriptors();
#else
    private static readonly Dictionary<string, CalculatedFunctionDescriptor> FunctionDescriptors = BuildFunctionDescriptors();
#endif

    public static object Evaluate(
        string name,
        IReadOnlyList<CalculatedExpressionNode> args,
        CalculatedExpressionEvaluationContext context)
    {
        string normalizedName = NormalizeFunctionName(name);
        ValidateFunctionArgumentCount(normalizedName, args.Count);
        if (!FunctionDescriptors.TryGetValue(normalizedName, out CalculatedFunctionDescriptor? descriptor))
        {
            throw new NotSupportedException($"Calculated-column function '{name}' is not supported.");
        }

        descriptor.ValidateArgumentCount(normalizedName, args.Count);
        return descriptor.Evaluator(new CalculatedFunctionInvocation(name, args, context));
    }

    internal static void AddFunction(
        Dictionary<string, CalculatedFunctionDescriptor> descriptors,
        string name,
        int minArgs,
        int maxArgs,
        CalculatedFunctionEvaluator evaluator,
        params string[] aliases)
    {
        var descriptor = new CalculatedFunctionDescriptor(minArgs, maxArgs, evaluator);
        AddName(name);
        foreach (string alias in aliases)
        {
            AddName(alias);
        }

        void AddName(string registeredName)
        {
            string normalizedName = NormalizeFunctionName(registeredName);
            if (!descriptors.TryAdd(normalizedName, descriptor))
            {
                throw new InvalidOperationException($"Calculated-column function '{normalizedName}' is registered more than once.");
            }
        }
    }

#if NET8_0_OR_GREATER
    private static FrozenDictionary<string, CalculatedFunctionDescriptor> BuildFunctionDescriptors()
#else
    private static Dictionary<string, CalculatedFunctionDescriptor> BuildFunctionDescriptors()
#endif
    {
        var descriptors = new Dictionary<string, CalculatedFunctionDescriptor>(StringComparer.OrdinalIgnoreCase);
        CalculatedExpressionLogicalFunctions.AddFunctions(descriptors);
        CalculatedExpressionTextFunctions.AddFunctions(descriptors);
        CalculatedExpressionDateTimeFunctions.AddFunctions(descriptors);
        CalculatedExpressionNumericFunctions.AddFunctions(descriptors);
        CalculatedExpressionFormattingFunctions.AddFunctions(descriptors);
        CalculatedExpressionFinancialFunctions.AddFunctions(descriptors);
        CalculatedExpressionMetadataFunctions.AddFunctions(descriptors);
#if NET8_0_OR_GREATER
        return descriptors.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);
#else
        return descriptors;
#endif
    }
}
