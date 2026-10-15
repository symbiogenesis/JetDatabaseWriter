namespace JetDatabaseWriter.Schema.Expressions;

using System;

internal static class CalculatedExpressionLimits
{
    internal const int MaxExpressionLength = 4096;
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
                $"{description} length {expression.Length} exceeds the safety limit of {maxLength} characters.");
        }

        int nestingDepth = 0;
        int columnReferences = 0;
        for (int charIndex = 0; charIndex < expression.Length; charIndex++)
        {
            char current = expression[charIndex];
            if (current is '"' or '\'')
            {
                int end = AccessExpressionText.SkipQuoted(expression, charIndex);
                charIndex = end < 0 ? expression.Length - 1 : end - 1;
                continue;
            }

            if (current == '[')
            {
                columnReferences++;
                if (columnReferences > MaxColumnReferences)
                {
                    throw new ArgumentException(
                        $"{description} references more than {MaxColumnReferences} columns.");
                }

                int endBracket = AccessExpressionText.SkipNameSegment(expression, charIndex);
                if (endBracket >= 0)
                {
                    charIndex = endBracket - 1;
                }

                continue;
            }

            if (current == '(')
            {
                nestingDepth++;
                if (nestingDepth > MaxExpressionNesting)
                {
                    throw new ArgumentException(
                        $"{description} nesting depth exceeds the safety limit of {MaxExpressionNesting}.");
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
}
