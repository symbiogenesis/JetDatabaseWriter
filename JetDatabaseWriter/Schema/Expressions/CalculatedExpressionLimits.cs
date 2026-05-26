namespace JetDatabaseWriter.Schema.Expressions;

using System;

internal static class CalculatedExpressionLimits
{
    internal const string PlaceholderPrefix = "__JdwCalcCol";
    internal const int MaxExpressionLength = 4096;
    internal const int MaxNormalizedExpressionLength = 8192;
    internal const int MaxExpressionNesting = 128;
    internal const int MaxColumnReferences = 1024;
    internal const int MaxFunctionArguments = 256;
    internal const int MaxGeneratedTextLength = 32768;
    internal const int MaxFormatDecimalDigits = 28;
    internal static readonly TimeSpan LikeRegexTimeout = TimeSpan.FromMilliseconds(100);

    internal static void ValidateExpressionShape(string expression, int maxLength, string description)
    {
        if (expression.Length > maxLength)
        {
            throw new ArgumentException(
                $"{description} length {expression.Length} exceeds the safety limit of {maxLength} characters.",
                nameof(expression));
        }

        int nestingDepth = 0;
        int columnReferences = 0;
        for (int charIndex = 0; charIndex < expression.Length; charIndex++)
        {
            char current = expression[charIndex];
            if (current == '"')
            {
                charIndex = SkipQuotedString(expression, charIndex);
                continue;
            }

            if (current == '[')
            {
                columnReferences++;
                if (columnReferences > MaxColumnReferences)
                {
                    throw new ArgumentException(
                        $"{description} references more than {MaxColumnReferences} columns.",
                        nameof(expression));
                }

                int endBracket = expression.IndexOf(']', charIndex + 1);
                if (endBracket >= 0)
                {
                    charIndex = endBracket;
                }

                continue;
            }

            if (current == '(')
            {
                nestingDepth++;
                if (nestingDepth > MaxExpressionNesting)
                {
                    throw new ArgumentException(
                        $"{description} nesting depth exceeds the safety limit of {MaxExpressionNesting}.",
                        nameof(expression));
                }
            }
            else if (current == ')' && nestingDepth > 0)
            {
                nestingDepth--;
            }
        }
    }

    internal static void ValidateFunctionArgumentCount(string functionName, int actual)
    {
        if (actual > MaxFunctionArguments)
        {
            throw new ArgumentException(
                $"Calculated-column function '{functionName}' has {actual} arguments, exceeding the safety limit of {MaxFunctionArguments}.");
        }
    }

    internal static void EnsureGeneratedTextLength(long length)
    {
        if (length > MaxGeneratedTextLength)
        {
            throw new ArgumentException(
                $"Calculated-column generated text length {length} exceeds the safety limit of {MaxGeneratedTextLength} characters.");
        }
    }

    private static int SkipQuotedString(string expression, int quoteIndex)
    {
        for (int charIndex = quoteIndex + 1; charIndex < expression.Length; charIndex++)
        {
            if (expression[charIndex] != '"')
            {
                continue;
            }

            if (charIndex + 1 < expression.Length && expression[charIndex + 1] == '"')
            {
                charIndex++;
                continue;
            }

            return charIndex;
        }

        return expression.Length - 1;
    }
}
