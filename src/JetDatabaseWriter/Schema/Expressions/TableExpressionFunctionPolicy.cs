namespace JetDatabaseWriter.Schema.Expressions;

using JetDatabaseWriter.Exceptions;

using static JetDatabaseWriter.Schema.Expressions.AccessExpressionText;

/// <summary>Excludes Access application functions from persisted table defaults and rules.</summary>
internal static class TableExpressionFunctionPolicy
{
    /// <summary>Checks function calls without interpreting unsupported stored expression syntax.</summary>
    /// <param name="expression">The stored or declared default or validation expression.</param>
    /// <exception cref="JetNotSupportedException">The expression calls a function unavailable to native table constraints.</exception>
    internal static void Validate(string? expression)
    {
        if (string.IsNullOrWhiteSpace(expression))
        {
            return;
        }

        CalculatedExpressionLimits.ValidateExpressionShape(expression, CalculatedExpressionLimits.MaxExpressionLength, "Table expression");
        int index = 0;
        while (index < expression.Length)
        {
            char current = expression[index];
            if (current is '\'' or '"')
            {
                index = SkipQuoted(expression, index);
                if (index < 0)
                {
                    return;
                }
            }
            else if (current == '[')
            {
                index = SkipNameSegment(expression, index);
                if (index < 0)
                {
                    return;
                }
            }
            else if (current == '#')
            {
                int close = expression.IndexOf('#', index + 1);
                index = close < 0 ? expression.Length : close + 1;
            }
            else if (IsNameStart(current))
            {
                int start = index;
                index = SkipNameSegment(expression, index);
                string name = expression[start..index];
                if (index < expression.Length && expression[index] == '$')
                {
                    index++;
                }

                int next = SkipWhiteSpace(expression, index);
                if (next < expression.Length && expression[next] == '('
                    && name.ToUpperInvariant() is "CURRENTUSER" or "EVAL" or "DLOOKUP" or "DCOUNT" or "DSUM" or "DAVG" or "DMIN" or "DMAX"
                        or "DFIRST" or "DLAST" or "DSTDEV" or "DSTDEVP" or "DVAR" or "DVARP")
                {
                    throw new JetNotSupportedException(JetErrorCode.UnsupportedExpression, $"Access table defaults and validation rules do not accept the {name} function.");
                }
            }
            else
            {
                index++;
            }
        }
    }
}
