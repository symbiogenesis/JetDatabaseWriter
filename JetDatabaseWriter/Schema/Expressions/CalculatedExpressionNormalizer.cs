namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionLimits;

/// <summary>
/// Rewrites an Access (Jet/VBA) calculated-column expression into a formula
/// for the ClosedXML parser, parenthesized wherever Excel's grammar would
/// group it differently. Every expression goes through the Access-precedence
/// Pratt parser below, so the Excel grammar of the downstream parser never
/// decides precedence: <c>^</c> binds tighter than unary minus and associates
/// left to right, then <c>*</c> <c>/</c>, <c>\</c>, <c>Mod</c>, <c>+</c>
/// <c>-</c>, <c>&amp;</c>, comparisons, <c>Not</c>, <c>And</c>, <c>Or</c>,
/// <c>Xor</c>, <c>Eqv</c>, <c>Imp</c>. Syntax the Access grammar does not
/// accept (including Excel's postfix <c>%</c>) throws
/// <see cref="ArgumentException"/> naming the expression.
/// </summary>
internal static class CalculatedExpressionNormalizer
{
    internal static string Normalize(string expression, out Dictionary<string, string> placeholderToColumn)
    {
        string prepared = ReplaceFieldReferencesAndDateLiterals(expression, out placeholderToColumn);
        return AccessExpressionNormalizer.Normalize(prepared, expression);
    }

    /// <summary>
    /// Definition-time check run when a calculated column is created or added:
    /// rejects operators that Access does not have, so a spreadsheet-only
    /// expression such as <c>5%</c> is refused before it reaches the file.
    /// </summary>
    /// <param name="columnName">The calculated column's name, for the error message.</param>
    /// <param name="expression">The expression text as the caller supplied it.</param>
    /// <exception cref="ArgumentException">The expression uses the spreadsheet <c>%</c> operator.</exception>
    internal static void ValidateDefinition(string columnName, string expression)
    {
        string prepared = ReplaceFieldReferencesAndDateLiterals(expression, out _);
        if (AccessExpressionNormalizer.ContainsPercentOperator(prepared))
        {
            throw new ArgumentException(
                $"Column '{columnName}': calculated-column expression '{expression}' uses '%', which is not an Access operator. Divide by 100 instead.",
                nameof(expression));
        }
    }

    private static string ReplaceFieldReferencesAndDateLiterals(string expression, out Dictionary<string, string> placeholderToColumn)
    {
        placeholderToColumn = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        string trimmed = expression.Trim();
        if (trimmed.StartsWith('='))
        {
            trimmed = trimmed[1..].TrimStart();
        }

        var builder = new StringBuilder(trimmed.Length + 16);
        int placeholderIndex = 0;
        for (int i = 0; i < trimmed.Length; i++)
        {
            char ch = trimmed[i];
            if (ch == '"')
            {
                builder.Append(ch);
                i++;
                while (i < trimmed.Length)
                {
                    builder.Append(trimmed[i]);
                    if (trimmed[i] == '"')
                    {
                        if (i + 1 < trimmed.Length && trimmed[i + 1] == '"')
                        {
                            i++;
                            builder.Append(trimmed[i]);
                            i++;
                            continue;
                        }

                        break;
                    }

                    i++;
                }
            }
            else if (ch == '[')
            {
                int end = trimmed.IndexOf(']', i + 1);
                if (end < 0)
                {
                    builder.Append(ch);
                    continue;
                }

                string columnName = trimmed.Substring(i + 1, end - i - 1);
                string placeholder = PlaceholderPrefix + placeholderIndex.ToString(CultureInfo.InvariantCulture);
                placeholderIndex++;
                placeholderToColumn[placeholder] = columnName;
                builder.Append(placeholder);
                i = end;
            }
            else if (ch == '#')
            {
                int end = trimmed.IndexOf('#', i + 1);
                if (end < 0)
                {
                    builder.Append(ch);
                    continue;
                }

                string dateLiteral = trimmed.Substring(i + 1, end - i - 1).Replace("\"", "\"\"", StringComparison.Ordinal);
                builder.Append("DATEVALUE(\"")
                    .Append(dateLiteral)
                    .Append("\")");
                i = end;
            }
            else
            {
                builder.Append(ch);
            }
        }

        return builder.ToString();
    }

    private sealed class AccessExpressionNormalizer
    {
        private const int UnaryLevel = 6;
        private const int AtomLevel = 8;

        private static readonly Dictionary<string, string> WordOperators = new(StringComparer.OrdinalIgnoreCase)
        {
            ["AND"] = "AND",
            ["OR"] = "OR",
            ["XOR"] = "XOR",
            ["EQV"] = "EQV",
            ["IMP"] = "IMP",
            ["MOD"] = "MOD",
            ["LIKE"] = "LIKE",
            ["BETWEEN"] = "BETWEEN",
            ["IN"] = "IN",
            ["IS"] = "IS",
            ["NOT"] = "NOT",
            ["NULL"] = "NULL",
            ["TRUE"] = "TRUE",
            ["FALSE"] = "FALSE",
            ["YES"] = "YES",
            ["NO"] = "NO",
            ["ON"] = "ON",
            ["OFF"] = "OFF",
        };

        private readonly List<Token> tokens;
        private readonly string originalExpression;
        private int position;
        private bool stopAtBetweenAnd;

        private AccessExpressionNormalizer(List<Token> tokens, string originalExpression)
        {
            this.tokens = tokens;
            this.originalExpression = originalExpression;
        }

        public static string Normalize(string expression, string originalExpression)
        {
            List<Token> tokens = Tokenize(expression);
            if (tokens.Exists(static token => token.IsPercent))
            {
                throw new ArgumentException(
                    $"Calculated-column expression '{originalExpression}' uses '%', which is not an Access operator. Divide by 100 instead.",
                    nameof(expression));
            }

            var normalizer = new AccessExpressionNormalizer(tokens, originalExpression);
            string normalized = normalizer.ParseExpression(0).Text;
            Token trailing = normalizer.Peek();
            if (trailing.Kind != TokenKind.End)
            {
                throw normalizer.SyntaxError($"unexpected {Describe(trailing)} after a complete expression");
            }

            return normalized;
        }

        public static bool ContainsPercentOperator(string expression)
            => Tokenize(expression).Exists(static token => token.IsPercent);

        private static List<Token> Tokenize(string expression)
        {
            var result = new List<Token>();
            for (int charIndex = 0; charIndex < expression.Length;)
            {
                char current = expression[charIndex];
                if (char.IsWhiteSpace(current))
                {
                    charIndex++;
                    continue;
                }

                if (current == '"')
                {
                    int start = charIndex;
                    charIndex++;
                    while (charIndex < expression.Length)
                    {
                        if (expression[charIndex] == '"')
                        {
                            if (charIndex + 1 < expression.Length && expression[charIndex + 1] == '"')
                            {
                                charIndex += 2;
                                continue;
                            }

                            charIndex++;
                            break;
                        }

                        charIndex++;
                    }

                    result.Add(new Token(TokenKind.Value, expression[start..charIndex]));
                    continue;
                }

                if (char.IsLetter(current) || current == '_')
                {
                    int start = charIndex;
                    charIndex++;
                    while (charIndex < expression.Length && (char.IsLetterOrDigit(expression[charIndex]) || expression[charIndex] == '_' || expression[charIndex] == '.'))
                    {
                        charIndex++;
                    }

                    if (charIndex < expression.Length && expression[charIndex] == '$')
                    {
                        charIndex++;
                    }

                    string text = expression[start..charIndex];
                    result.Add(new Token(WordOperators.ContainsKey(text) ? TokenKind.Word : TokenKind.Identifier, text));
                    continue;
                }

                if (char.IsDigit(current) || current == '.')
                {
                    int start = charIndex;
                    charIndex++;
                    while (charIndex < expression.Length && (char.IsDigit(expression[charIndex]) || expression[charIndex] == '.' || expression[charIndex] == 'E' || expression[charIndex] == 'e' || expression[charIndex] == '+' || expression[charIndex] == '-'))
                    {
                        char previous = expression[charIndex - 1];
                        char next = expression[charIndex];
                        if ((next == '+' || next == '-') && previous != 'E' && previous != 'e')
                        {
                            break;
                        }

                        charIndex++;
                    }

                    result.Add(new Token(TokenKind.Value, expression[start..charIndex]));
                    continue;
                }

                if (current == '(')
                {
                    result.Add(new Token(TokenKind.OpenParen, "("));
                    charIndex++;
                    continue;
                }

                if (current == ')')
                {
                    result.Add(new Token(TokenKind.CloseParen, ")"));
                    charIndex++;
                    continue;
                }

                if (current == ',')
                {
                    result.Add(new Token(TokenKind.Comma, ","));
                    charIndex++;
                    continue;
                }

                if (current == '\\')
                {
                    result.Add(new Token(TokenKind.Backslash, "\\"));
                    charIndex++;
                    continue;
                }

                if (charIndex + 1 < expression.Length)
                {
                    string twoChar = expression.Substring(charIndex, 2);
                    if (twoChar is "<>" or "<=" or ">=")
                    {
                        result.Add(new Token(TokenKind.Operator, twoChar));
                        charIndex += 2;
                        continue;
                    }
                }

                result.Add(new Token(TokenKind.Operator, current.ToString()));
                charIndex++;
            }

            result.Add(new Token(TokenKind.End, string.Empty));
            return result;
        }

        private static BinaryOperatorInfo? GetBinaryOperator(Token token)
        {
            if (token.Kind == TokenKind.Backslash)
            {
                return new BinaryOperatorInfo("INTDIV", 10, AtomLevel);
            }

            if (token.Kind == TokenKind.Operator)
            {
                // Every Access binary operator is left-associative, ^ included
                // (2^3^2 = 64). ExcelLevel is the operator's rank in the
                // downstream Excel grammar, used to decide where the emitted
                // formula needs parentheses.
                return token.Text switch
                {
                    "^" => new BinaryOperatorInfo("^", 12, 5),
                    "*" => new BinaryOperatorInfo("*", 11, 4),
                    "/" => new BinaryOperatorInfo("/", 11, 4),
                    "+" => new BinaryOperatorInfo("+", 8, 3),
                    "-" => new BinaryOperatorInfo("-", 8, 3),
                    "&" => new BinaryOperatorInfo("&", 7, 2),
                    "=" or "<>" or "<" or "<=" or ">" or ">=" => new BinaryOperatorInfo(token.Text, 6, 1),
                    _ => null,
                };
            }

            if (token.Kind != TokenKind.Word)
            {
                return null;
            }

            return token.Text.ToUpperInvariant() switch
            {
                "IMP" => new BinaryOperatorInfo("IMP", 1, AtomLevel),
                "EQV" => new BinaryOperatorInfo("EQV", 2, AtomLevel),
                "XOR" => new BinaryOperatorInfo("XOR", 3, AtomLevel),
                "OR" => new BinaryOperatorInfo("OR", 4, AtomLevel),
                "AND" => new BinaryOperatorInfo("AND", 5, AtomLevel),
                "IS" => new BinaryOperatorInfo("IS", 6, AtomLevel),
                "LIKE" => new BinaryOperatorInfo("LIKE", 6, AtomLevel),
                "BETWEEN" => new BinaryOperatorInfo("BETWEEN", 6, AtomLevel),
                "IN" => new BinaryOperatorInfo("IN", 6, AtomLevel),
                "NOT" => new BinaryOperatorInfo("NOT", 6, AtomLevel),
                "MOD" => new BinaryOperatorInfo("MOD", 9, AtomLevel),
                _ => null,
            };
        }

        private static string Parenthesize(Fragment fragment, bool needed)
            => needed ? "(" + fragment.Text + ")" : fragment.Text;

        private static Fragment Atom(string text) => new(text, AtomLevel);

        private static string Describe(Token token)
            => token.Text.StartsWith(PlaceholderPrefix, StringComparison.Ordinal) ? "a [field] reference" : $"'{token.Text}'";

        private Fragment ParseExpression(int minimumPrecedence)
        {
            Fragment left = this.ParsePrefix();
            while (true)
            {
                Token token = this.Peek();
                if (token.Kind is TokenKind.End or TokenKind.CloseParen or TokenKind.Comma)
                {
                    break;
                }

                if (this.stopAtBetweenAnd && token.IsWord("AND"))
                {
                    break;
                }

                BinaryOperatorInfo? info = GetBinaryOperator(token);
                if (info is null || info.Value.Precedence < minimumPrecedence)
                {
                    break;
                }

                this.Read();
                left = info.Value.Name switch
                {
                    "IS" => this.ParseIs(left),
                    "NOT" => this.ParsePostfixNot(left, info.Value.Precedence),
                    "BETWEEN" => this.ParseBetween(left, negate: false),
                    "IN" => this.ParseIn(left, negate: false),
                    "LIKE" => this.ParseFunctionBinary("LIKE", left, info.Value),
                    "MOD" => this.ParseFunctionBinary("MOD", left, info.Value),
                    "INTDIV" => this.ParseFunctionBinary("INTDIV", left, info.Value),
                    "AND" or "OR" or "XOR" or "EQV" or "IMP" => this.ParseFunctionBinary(info.Value.Name, left, info.Value),
                    _ => this.ParseInfix(left, info.Value),
                };
            }

            return left;
        }

        private Fragment ParsePrefix()
        {
            Token token = this.Peek();
            if (token.IsWord("NOT"))
            {
                this.Read();
                return Atom("NOT(" + this.ParseExpression(6).Text + ")");
            }

            if (token.Kind == TokenKind.Operator && (token.Text == "+" || token.Text == "-"))
            {
                // Access ranks ^ above unary minus, so the operand takes any
                // exponentiation with it: -2^2 is -(2^2) = -4. Excel ranks
                // unary minus above ^, so an operand built from any binary
                // operator is parenthesized to keep that grouping.
                this.Read();
                Fragment operand = this.ParseExpression(12);
                return new Fragment(token.Text + Parenthesize(operand, operand.Level < UnaryLevel), UnaryLevel);
            }

            return this.ParsePrimary();
        }

        private Fragment ParsePrimary()
        {
            Token token = this.Read();
            switch (token.Kind)
            {
                case TokenKind.Identifier:
                    if (this.Peek().Kind == TokenKind.OpenParen)
                    {
                        return this.ParseFunctionCall(token.Text);
                    }

                    return Atom(token.Text);
                case TokenKind.Word:
                    return token.Text.ToUpperInvariant() switch
                    {
                        "YES" or "ON" or "TRUE" => Atom("TRUE"),
                        "NO" or "OFF" or "FALSE" => Atom("FALSE"),
                        "NULL" => Atom(token.Text),
                        _ => throw this.SyntaxError($"unexpected {Describe(token)} where an operand is expected"),
                    };
                case TokenKind.Value:
                    return Atom(token.Text);
                case TokenKind.OpenParen:
                    Fragment inner = this.ParseExpression(0);
                    this.Expect(TokenKind.CloseParen, ")");
                    return Atom("(" + inner.Text + ")");
                case TokenKind.End:
                case TokenKind.Operator:
                case TokenKind.Backslash:
                case TokenKind.CloseParen:
                case TokenKind.Comma:
                    throw this.SyntaxError(token.Kind == TokenKind.End ? "the expression ends where an operand is expected" : $"unexpected {Describe(token)} where an operand is expected");
                default:
                    throw new InvalidOperationException($"Unexpected calculated-column token kind '{token.Kind}'.");
            }
        }

        private Fragment ParseFunctionCall(string name)
        {
            if (name.EndsWith('$'))
            {
                name = name[..^1];
            }

            this.Expect(TokenKind.OpenParen, "(");
            var arguments = new List<string>();
            if (this.Peek().Kind != TokenKind.CloseParen)
            {
                while (true)
                {
                    arguments.Add(this.ParseExpression(0).Text);
                    ValidateFunctionArgumentCount(name, arguments.Count);
                    if (this.Peek().Kind != TokenKind.Comma)
                    {
                        break;
                    }

                    this.Read();
                }
            }

            this.Expect(TokenKind.CloseParen, ")");
            return Atom(name + "(" + string.Join(",", arguments) + ")");
        }

        private Fragment ParseIs(Fragment left)
        {
            bool negate = false;
            if (this.Peek().IsWord("NOT"))
            {
                this.Read();
                negate = true;
            }

            Token token = this.Read();
            if (!token.IsWord("NULL"))
            {
                throw this.SyntaxError("'Is' is only supported for Null checks");
            }

            string call = "ISNULL(" + left.Text + ")";
            return Atom(negate ? "NOT(" + call + ")" : call);
        }

        private Fragment ParsePostfixNot(Fragment left, int precedence)
        {
            Token token = this.Read();
            if (token.IsWord("LIKE"))
            {
                return Atom("NOT(" + this.ParseFunctionBinary("LIKE", left, new BinaryOperatorInfo("LIKE", precedence, AtomLevel)).Text + ")");
            }

            if (token.IsWord("IN"))
            {
                return this.ParseIn(left, negate: true);
            }

            if (token.IsWord("BETWEEN"))
            {
                return this.ParseBetween(left, negate: true);
            }

            throw this.SyntaxError($"unexpected {Describe(token)} after Not");
        }

        private Fragment ParseBetween(Fragment left, bool negate)
        {
            bool previousStop = this.stopAtBetweenAnd;
            this.stopAtBetweenAnd = true;
            Fragment lower;
            try
            {
                lower = this.ParseExpression(0);
            }
            finally
            {
                this.stopAtBetweenAnd = previousStop;
            }

            Token separator = this.Read();
            if (!separator.IsWord("AND"))
            {
                throw this.SyntaxError("Between is missing its And separator");
            }

            Fragment upper = this.ParseExpression(7);
            string call = "BETWEEN(" + left.Text + "," + lower.Text + "," + upper.Text + ")";
            return Atom(negate ? "NOT(" + call + ")" : call);
        }

        private Fragment ParseIn(Fragment left, bool negate)
        {
            this.Expect(TokenKind.OpenParen, "(");
            var values = new List<string> { left.Text };
            if (this.Peek().Kind != TokenKind.CloseParen)
            {
                while (true)
                {
                    values.Add(this.ParseExpression(0).Text);
                    if (this.Peek().Kind != TokenKind.Comma)
                    {
                        break;
                    }

                    this.Read();
                }
            }

            this.Expect(TokenKind.CloseParen, ")");
            string call = "IN(" + string.Join(",", values) + ")";
            return Atom(negate ? "NOT(" + call + ")" : call);
        }

        private Fragment ParseFunctionBinary(string functionName, Fragment left, BinaryOperatorInfo info)
        {
            Fragment right = this.ParseExpression(info.Precedence + 1);
            return Atom(functionName + "(" + left.Text + "," + right.Text + ")");
        }

        private Fragment ParseInfix(Fragment left, BinaryOperatorInfo info)
        {
            // Left-associative in both grammars: a left operand of the same
            // Excel rank needs no parentheses, a right operand of the same
            // rank does. Keeping chains such as [A]+[B]+[C] flat keeps the
            // emitted formula inside the nesting limit.
            Fragment right = this.ParseExpression(info.Precedence + 1);
            string text = Parenthesize(left, left.Level < info.ExcelLevel)
                + info.Name
                + Parenthesize(right, right.Level <= info.ExcelLevel);
            return new Fragment(text, info.ExcelLevel);
        }

        private ArgumentException SyntaxError(string detail)
            => new($"Calculated-column expression '{this.originalExpression}' is not valid Access expression syntax: {detail}.");

        private Token Peek() => this.tokens[this.position];

        private Token Read() => this.tokens[this.position++];

        private void Expect(TokenKind kind, string text)
        {
            Token token = this.Read();
            if (token.Kind != kind || (text.Length > 0 && token.Text != text))
            {
                throw this.SyntaxError(token.Kind == TokenKind.End ? $"expected '{text}' before the end of the expression" : $"expected '{text}' but found {Describe(token)}");
            }
        }

        private readonly record struct BinaryOperatorInfo(string Name, int Precedence, int ExcelLevel);

        /// <summary>A piece of the emitted Excel-syntax formula.</summary>
        /// <param name="Text">The formula text.</param>
        /// <param name="Level">
        /// The fragment's rank in the Excel grammar: 1 comparison, 2 <c>&amp;</c>,
        /// 3 <c>+ -</c>, 4 <c>* /</c>, 5 <c>^</c>, 6 unary sign, 8 operand or function call.
        /// </param>
        private readonly record struct Fragment(string Text, int Level);

        private readonly record struct Token(TokenKind Kind, string Text)
        {
            public bool IsPercent => this.Kind == TokenKind.Operator && this.Text == "%";

            public bool IsWord(string text) => this.Kind == TokenKind.Word && this.Text.Equals(text, StringComparison.OrdinalIgnoreCase);
        }

        private enum TokenKind
        {
            End = 0,
            Identifier = 1,
            Value = 2,
            Word = 3,
            Operator = 4,
            Backslash = 5,
            OpenParen = 6,
            CloseParen = 7,
            Comma = 8,
        }
    }
}
