namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionLimits;

internal static class CalculatedExpressionNormalizer
{
    internal static string Normalize(string expression, out Dictionary<string, string> placeholderToColumn)
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

        return AccessExpressionNormalizer.Normalize(builder.ToString());
    }

    private sealed class AccessExpressionNormalizer
    {
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
        private int position;
        private bool stopAtBetweenAnd;

        private AccessExpressionNormalizer(List<Token> tokens)
        {
            this.tokens = tokens;
        }

        public static string Normalize(string expression)
        {
            List<Token> tokens = Tokenize(expression);
            if (!tokens.Exists(static token => token.Kind is TokenKind.Word or TokenKind.Backslash || (token.Kind == TokenKind.Identifier && token.Text.EndsWith('$'))))
            {
                return expression;
            }

            var normalizer = new AccessExpressionNormalizer(tokens);
            string normalized = normalizer.ParseExpression(0);
            return normalizer.Peek().Kind == TokenKind.End ? normalized : expression;
        }

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
                return new BinaryOperatorInfo("INTDIV", 10, false);
            }

            if (token.Kind == TokenKind.Operator)
            {
                return token.Text switch
                {
                    "^" => new BinaryOperatorInfo("^", 12, true),
                    "*" => new BinaryOperatorInfo("*", 11, false),
                    "/" => new BinaryOperatorInfo("/", 11, false),
                    "+" => new BinaryOperatorInfo("+", 8, false),
                    "-" => new BinaryOperatorInfo("-", 8, false),
                    "&" => new BinaryOperatorInfo("&", 7, false),
                    "=" or "<>" or "<" or "<=" or ">" or ">=" => new BinaryOperatorInfo(token.Text, 6, false),
                    _ => null,
                };
            }

            if (token.Kind != TokenKind.Word)
            {
                return null;
            }

            return token.Text.ToUpperInvariant() switch
            {
                "IMP" => new BinaryOperatorInfo("IMP", 1, false),
                "EQV" => new BinaryOperatorInfo("EQV", 2, false),
                "XOR" => new BinaryOperatorInfo("XOR", 3, false),
                "OR" => new BinaryOperatorInfo("OR", 4, false),
                "AND" => new BinaryOperatorInfo("AND", 5, false),
                "IS" => new BinaryOperatorInfo("IS", 6, false),
                "LIKE" => new BinaryOperatorInfo("LIKE", 6, false),
                "BETWEEN" => new BinaryOperatorInfo("BETWEEN", 6, false),
                "IN" => new BinaryOperatorInfo("IN", 6, false),
                "NOT" => new BinaryOperatorInfo("NOT", 6, false),
                "MOD" => new BinaryOperatorInfo("MOD", 9, false),
                _ => null,
            };
        }

        private string ParseExpression(int minimumPrecedence)
        {
            string left = this.ParsePrefix();
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

        private string ParsePrefix()
        {
            Token token = this.Peek();
            if (token.IsWord("NOT"))
            {
                this.Read();
                return "NOT(" + this.ParseExpression(6) + ")";
            }

            if (token.Kind == TokenKind.Operator && (token.Text == "+" || token.Text == "-"))
            {
                this.Read();
                return token.Text + this.ParseExpression(12);
            }

            return this.ParsePrimary();
        }

        private string ParsePrimary()
        {
            Token token = this.Read();
            switch (token.Kind)
            {
                case TokenKind.Identifier:
                    if (this.Peek().Kind == TokenKind.OpenParen)
                    {
                        return this.ParseFunctionCall(token.Text);
                    }

                    return token.Text;
                case TokenKind.Word:
                    return token.Text.ToUpperInvariant() switch
                    {
                        "YES" or "ON" => "TRUE",
                        "NO" or "OFF" => "FALSE",
                        _ => token.Text,
                    };
                case TokenKind.Value:
                    return token.Text;
                case TokenKind.OpenParen:
                    string inner = this.ParseExpression(0);
                    this.Expect(TokenKind.CloseParen, ")");
                    return "(" + inner + ")";
                case TokenKind.End:
                case TokenKind.Operator:
                case TokenKind.Backslash:
                case TokenKind.CloseParen:
                case TokenKind.Comma:
                    throw new ArgumentException($"Unexpected token '{token.Text}' in calculated-column expression.");
                default:
                    throw new InvalidOperationException($"Unexpected calculated-column token kind '{token.Kind}'.");
            }
        }

        private string ParseFunctionCall(string name)
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
                    arguments.Add(this.ParseExpression(0));
                    ValidateFunctionArgumentCount(name, arguments.Count);
                    if (this.Peek().Kind != TokenKind.Comma)
                    {
                        break;
                    }

                    this.Read();
                }
            }

            this.Expect(TokenKind.CloseParen, ")");
            return name + "(" + string.Join(",", arguments) + ")";
        }

        private string ParseIs(string left)
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
                throw new ArgumentException("Calculated-column 'Is' expressions are only supported for Null checks.");
            }

            string call = "ISNULL(" + left + ")";
            return negate ? "NOT(" + call + ")" : call;
        }

        private string ParsePostfixNot(string left, int precedence)
        {
            Token token = this.Read();
            if (token.IsWord("LIKE"))
            {
                return "NOT(" + this.ParseFunctionBinary("LIKE", left, new BinaryOperatorInfo("LIKE", precedence, false)) + ")";
            }

            if (token.IsWord("IN"))
            {
                return this.ParseIn(left, negate: true);
            }

            if (token.IsWord("BETWEEN"))
            {
                return this.ParseBetween(left, negate: true);
            }

            throw new ArgumentException($"Unexpected token '{token.Text}' after postfix Not in calculated-column expression.");
        }

        private string ParseBetween(string left, bool negate)
        {
            bool previousStop = this.stopAtBetweenAnd;
            this.stopAtBetweenAnd = true;
            string lower;
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
                throw new ArgumentException("Calculated-column Between expression is missing the And separator.");
            }

            string upper = this.ParseExpression(7);
            string call = "BETWEEN(" + left + "," + lower + "," + upper + ")";
            return negate ? "NOT(" + call + ")" : call;
        }

        private string ParseIn(string left, bool negate)
        {
            this.Expect(TokenKind.OpenParen, "(");
            var values = new List<string> { left };
            if (this.Peek().Kind != TokenKind.CloseParen)
            {
                while (true)
                {
                    values.Add(this.ParseExpression(0));
                    if (this.Peek().Kind != TokenKind.Comma)
                    {
                        break;
                    }

                    this.Read();
                }
            }

            this.Expect(TokenKind.CloseParen, ")");
            string call = "IN(" + string.Join(",", values) + ")";
            return negate ? "NOT(" + call + ")" : call;
        }

        private string ParseFunctionBinary(string functionName, string left, BinaryOperatorInfo info)
        {
            string right = this.ParseExpression(info.RightAssociative ? info.Precedence : info.Precedence + 1);
            return functionName + "(" + left + "," + right + ")";
        }

        private string ParseInfix(string left, BinaryOperatorInfo info)
        {
            string right = this.ParseExpression(info.RightAssociative ? info.Precedence : info.Precedence + 1);
            return "(" + left + info.Name + right + ")";
        }

        private Token Peek() => this.tokens[this.position];

        private Token Read() => this.tokens[this.position++];

        private void Expect(TokenKind kind, string text)
        {
            Token token = this.Read();
            if (token.Kind != kind || (text.Length > 0 && token.Text != text))
            {
                throw new ArgumentException($"Expected '{text}' in calculated-column expression, got '{token.Text}'.");
            }
        }

        private readonly record struct BinaryOperatorInfo(string Name, int Precedence, bool RightAssociative);

        private readonly record struct Token(TokenKind Kind, string Text)
        {
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
