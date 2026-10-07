namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;

/// <summary>
/// An Access column <c>ValidationRule</c> compiled for evaluation against a row.
/// </summary>
/// <remarks>
/// <para>
/// Access writes a column rule with an implicit left operand: <c>&gt;=0 And &lt;=100</c>,
/// <c>Is Not Null</c>, <c>Between 1 And 10</c>, <c>In (1,2,3)</c>, <c>Like "A*"</c>,
/// <c>&lt;=Date()</c>. The rule is split into the terms joined by <c>And</c>, <c>Or</c>,
/// <c>Xor</c>, <c>Eqv</c>, <c>Imp</c> and prefix <c>Not</c> (an <c>And</c> that closes a
/// <c>Between</c> stays in its term). A term that starts with a comparison operator or
/// with <c>Like</c>, <c>Between</c>, <c>In</c>, <c>Is</c> or <c>Not Like/Between/In</c>
/// gets the column as its left operand. A term with no top-level comparison of its own is
/// a value the column must equal, so <c>0 Or &gt;100</c> means "0, or above 100". Any
/// other term, such as <c>Len([Code]) = 3</c>, is used as written.
/// </para>
/// <para>
/// Each term is evaluated by the calculated-column expression engine and the terms are
/// combined with Access's three-valued logic. A comparison with Null is Null, and Access
/// rejects a value only when the rule is False, so a rule that does not test for Null
/// accepts Null.
/// </para>
/// </remarks>
internal sealed class ColumnValidationRule
{
    /// <summary>
    /// The rule for a column whose rule text this library cannot parse. It cannot be evaluated.
    /// </summary>
    public static readonly ColumnValidationRule Unsupported = new(null);

    private readonly RuleNode? root;

    private ColumnValidationRule(RuleNode? root) => this.root = root;

    private enum TokenKind
    {
        Word = 0,
        Comparison = 1,
        OpenParen = 2,
        CloseParen = 3,
        Literal = 4,
        Other = 5,
    }

    private enum LogicalOperator
    {
        And = 0,
        Or = 1,
        Xor = 2,
        Eqv = 3,
        Imp = 4,
    }

    /// <summary>Gets a value indicating whether the rule parsed and can be evaluated.</summary>
    public bool IsSupported => this.root is not null;

    /// <summary>
    /// Compiles <paramref name="rule"/> for the column named <paramref name="columnName"/>.
    /// Returns <see cref="Unsupported"/> when the rule is not syntax this library can parse.
    /// </summary>
    /// <param name="rule">The Access validation rule text.</param>
    /// <param name="columnName">The column the rule belongs to.</param>
    /// <returns>The compiled rule.</returns>
    public static ColumnValidationRule Compile(string rule, string columnName)
    {
        try
        {
            CalculatedExpressionLimits.ValidateExpressionShape(rule, CalculatedExpressionLimits.MaxExpressionLength, "Validation rule");
            List<Token> tokens = Tokenize(rule);
            var parser = new Parser(rule, tokens, "[" + columnName + "]");
            return new ColumnValidationRule(parser.ParseRule(0, tokens.Count));
        }
        catch (Exception ex) when (ex is ArgumentException or NotSupportedException or InvalidOperationException or FormatException)
        {
            return Unsupported;
        }
    }

    /// <summary>
    /// Returns whether an exception thrown while evaluating a column expression means the
    /// expression uses something this library cannot evaluate (an unknown function or name,
    /// a value it cannot convert), rather than a defect.
    /// </summary>
    /// <param name="ex">The exception.</param>
    /// <returns><see langword="true"/> when the expression should be treated as unsupported.</returns>
    public static bool IsEvaluationFailure(Exception ex) =>
        ex is ArgumentException
            or NotSupportedException
            or InvalidOperationException
            or FormatException
            or InvalidCastException
            or ArithmeticException
            or IndexOutOfRangeException;

    /// <summary>
    /// Evaluates the rule against the row behind <paramref name="context"/>. Returns
    /// <see langword="false"/> only when the rule is False. A rule that is True or Null,
    /// accept the value. Unsupported rules and evaluation failures throw.
    /// </summary>
    /// <param name="context">The evaluation context over the candidate row.</param>
    /// <returns>Whether the value is accepted.</returns>
    public bool Accepts(CalculatedExpressionEvaluationContext context)
        => this.root is null
            ? throw new NotSupportedException("The validation rule cannot be evaluated.")
            : this.root.Evaluate(context) != false;
    private static List<Token> Tokenize(string rule)
    {
        var tokens = new List<Token>();
        int index = 0;
        while (index < rule.Length)
        {
            char current = rule[index];
            if (char.IsWhiteSpace(current))
            {
                index++;
                continue;
            }

            int start = index;
            TokenKind kind;
            switch (current)
            {
                case '"':
                case '\'':
                    index = SkipQuoted(rule, index, current);
                    kind = TokenKind.Literal;
                    break;
                case '[':
                    index = SkipPast(rule, index, ']');
                    kind = TokenKind.Literal;
                    break;
                case '#':
                    index = SkipPast(rule, index, '#');
                    kind = TokenKind.Literal;
                    break;
                case '(':
                    index++;
                    kind = TokenKind.OpenParen;
                    break;
                case ')':
                    index++;
                    kind = TokenKind.CloseParen;
                    break;
                case '<':
                case '>':
                case '=':
                    index++;
                    if (current != '=' && index < rule.Length && (rule[index] == '=' || (current == '<' && rule[index] == '>')))
                    {
                        index++;
                    }

                    kind = TokenKind.Comparison;
                    break;
                default:
                    if (char.IsLetter(current) || current == '_')
                    {
                        index++;
                        while (index < rule.Length && (char.IsLetterOrDigit(rule[index]) || rule[index] == '_' || rule[index] == '$'))
                        {
                            index++;
                        }

                        kind = TokenKind.Word;
                    }
                    else if (char.IsDigit(current) || current == '.')
                    {
                        index++;
                        while (index < rule.Length && (char.IsLetterOrDigit(rule[index]) || rule[index] == '.'))
                        {
                            index++;
                        }

                        kind = TokenKind.Literal;
                    }
                    else
                    {
                        index++;
                        kind = TokenKind.Other;
                    }

                    break;
            }

            tokens.Add(new Token(kind, start, index, rule[start..index]));
        }

        return tokens;
    }

    private static int SkipQuoted(string text, int quoteIndex, char quote)
    {
        for (int index = quoteIndex + 1; index < text.Length; index++)
        {
            if (text[index] != quote)
            {
                continue;
            }

            if (index + 1 < text.Length && text[index + 1] == quote)
            {
                index++;
                continue;
            }

            return index + 1;
        }

        throw new ArgumentException("Validation rule has an unterminated string literal.");
    }

    private static int SkipPast(string text, int openIndex, char close)
    {
        int end = text.IndexOf(close, openIndex + 1);
        return end < 0
            ? throw new ArgumentException($"Validation rule is missing a closing '{close}'.")
            : end + 1;
    }

    private static bool? ToTriState(object value)
    {
        if (IsNull(value))
        {
            return null;
        }

        if (value is bool boolean)
        {
            return boolean;
        }

        return TryConvertDecimal(value, out decimal numeric)
            ? numeric != 0m
            : throw new NotSupportedException("Validation rule term did not evaluate to a Boolean.");
    }

    private readonly record struct Token(TokenKind Kind, int Start, int End, string Text)
    {
        public bool IsWord(string word) => this.Kind == TokenKind.Word && this.Text.Equals(word, StringComparison.OrdinalIgnoreCase);

        public bool IsComparisonKeyword() => this.IsWord("LIKE") || this.IsWord("BETWEEN") || this.IsWord("IN") || this.IsWord("IS");
    }

    private sealed class Parser(string source, List<Token> tokens, string operand)
    {
        /// <summary>
        /// The logical operators from lowest precedence to highest. <c>Not</c> binds
        /// tighter than all of them.
        /// </summary>
        private static readonly (string Word, LogicalOperator Operator)[] Levels =
        [
            ("IMP", LogicalOperator.Imp),
            ("EQV", LogicalOperator.Eqv),
            ("XOR", LogicalOperator.Xor),
            ("OR", LogicalOperator.Or),
            ("AND", LogicalOperator.And),
        ];

        public RuleNode ParseRule(int start, int end)
        {
            int position = start;
            RuleNode node = this.ParseLevel(ref position, end, 0);
            return position == end
                ? node
                : throw new ArgumentException($"Unexpected '{tokens[position].Text}' in validation rule.");
        }

        private RuleNode ParseLevel(ref int position, int end, int level)
        {
            if (level == Levels.Length)
            {
                return this.ParseNot(ref position, end);
            }

            RuleNode left = this.ParseLevel(ref position, end, level + 1);
            while (position < end && tokens[position].IsWord(Levels[level].Word))
            {
                position++;
                RuleNode right = this.ParseLevel(ref position, end, level + 1);
                left = new LogicalNode(Levels[level].Operator, left, right);
            }

            return left;
        }

        private RuleNode ParseNot(ref int position, int end)
        {
            // "Not Like ...", "Not Between ..." and "Not In (...)" are postfix forms on
            // the implicit operand; any other leading Not negates the term after it.
            if (position < end
                && tokens[position].IsWord("NOT")
                && !(position + 1 < end && (tokens[position + 1].IsWord("LIKE") || tokens[position + 1].IsWord("BETWEEN") || tokens[position + 1].IsWord("IN"))))
            {
                position++;
                return new NotNode(this.ParseNot(ref position, end));
            }

            return this.ParseTerm(ref position, end);
        }

        private RuleNode ParseTerm(ref int position, int end)
        {
            int start = position;
            int depth = 0;
            bool betweenNeedsAnd = false;
            for (; position < end; position++)
            {
                Token token = tokens[position];
                if (token.Kind == TokenKind.OpenParen)
                {
                    depth++;
                    continue;
                }

                if (token.Kind == TokenKind.CloseParen)
                {
                    depth = depth > 0 ? depth - 1 : throw new ArgumentException("Validation rule has an unbalanced ')'.");
                    continue;
                }

                if (depth > 0 || token.Kind != TokenKind.Word)
                {
                    continue;
                }

                if (token.IsWord("BETWEEN"))
                {
                    betweenNeedsAnd = true;
                }
                else if (token.IsWord("AND"))
                {
                    if (!betweenNeedsAnd)
                    {
                        break;
                    }

                    betweenNeedsAnd = false;
                }
                else if (token.IsWord("OR") || token.IsWord("XOR") || token.IsWord("EQV") || token.IsWord("IMP"))
                {
                    break;
                }
            }

            if (depth != 0)
            {
                throw new ArgumentException("Validation rule has an unbalanced '('.");
            }

            return position == start
                ? throw new ArgumentException("Validation rule has an empty term.")
                : this.BuildTerm(start, position);
        }

        private RuleNode BuildTerm(int start, int end)
        {
            // A term that is one parenthesised group is a nested rule: "(>=0 And <=10) Or 99".
            if (tokens[start].Kind == TokenKind.OpenParen && this.FindClose(start) == end - 1)
            {
                return end - start == 2
                    ? throw new ArgumentException("Validation rule has an empty group.")
                    : this.ParseRule(start + 1, end - 1);
            }

            string text = source[tokens[start].Start..tokens[end - 1].End];
            string expression;
            if (this.StartsWithImplicitOperand(start, end))
            {
                expression = operand + " " + text;
            }
            else if (this.HasTopLevelComparison(start, end))
            {
                expression = text;
            }
            else
            {
                expression = operand + " = (" + text + ")";
            }

            return new TermNode(CalculatedExpressionPlan.Parse(expression));
        }

        private int FindClose(int open)
        {
            int depth = 0;
            for (int index = open; index < tokens.Count; index++)
            {
                if (tokens[index].Kind == TokenKind.OpenParen)
                {
                    depth++;
                }
                else if (tokens[index].Kind == TokenKind.CloseParen && --depth == 0)
                {
                    return index;
                }
            }

            return -1;
        }

        private bool StartsWithImplicitOperand(int start, int end)
        {
            Token first = tokens[start];
            if (first.Kind == TokenKind.Comparison || first.IsComparisonKeyword())
            {
                return true;
            }

            return first.IsWord("NOT")
                && start + 1 < end
                && (tokens[start + 1].IsWord("LIKE") || tokens[start + 1].IsWord("BETWEEN") || tokens[start + 1].IsWord("IN"));
        }

        private bool HasTopLevelComparison(int start, int end)
        {
            int depth = 0;
            for (int index = start; index < end; index++)
            {
                Token token = tokens[index];
                if (token.Kind == TokenKind.OpenParen)
                {
                    depth++;
                }
                else if (token.Kind == TokenKind.CloseParen)
                {
                    depth--;
                }
                else if (depth == 0 && (token.Kind == TokenKind.Comparison || token.IsComparisonKeyword()))
                {
                    return true;
                }
            }

            return false;
        }
    }

    private abstract class RuleNode
    {
        public abstract bool? Evaluate(CalculatedExpressionEvaluationContext context);
    }

    private sealed class TermNode(CalculatedExpressionPlan plan) : RuleNode
    {
        public override bool? Evaluate(CalculatedExpressionEvaluationContext context)
            => ToTriState(plan.Root.Evaluate(context, plan));
    }

    private sealed class NotNode(RuleNode operand) : RuleNode
    {
        public override bool? Evaluate(CalculatedExpressionEvaluationContext context)
            => !operand.Evaluate(context);
    }

    private sealed class LogicalNode(LogicalOperator op, RuleNode left, RuleNode right) : RuleNode
    {
        public override bool? Evaluate(CalculatedExpressionEvaluationContext context)
        {
            bool? l = left.Evaluate(context);

            // Short-circuit where the left side alone decides the result.
            if ((op == LogicalOperator.And && l == false) || (op == LogicalOperator.Or && l == true))
            {
                return l;
            }

            if (op == LogicalOperator.Imp && l == false)
            {
                return true;
            }

            bool? r = right.Evaluate(context);

            // A False right side decides And; a True right side decides Or and Imp.
            if (op == LogicalOperator.And && r == false)
            {
                return false;
            }

            if ((op == LogicalOperator.Or || op == LogicalOperator.Imp) && r == true)
            {
                return true;
            }

            // Otherwise any Null operand makes the result Null.
            if (l is null || r is null)
            {
                return null;
            }

            return op switch
            {
                LogicalOperator.And => true,
                LogicalOperator.Or => false,
                LogicalOperator.Xor => l != r,
                LogicalOperator.Eqv => l == r,
                LogicalOperator.Imp => false,
                _ => throw new InvalidOperationException($"Unexpected validation-rule operator '{op}'."),
            };
        }
    }
}
