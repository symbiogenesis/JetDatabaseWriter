namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;
using System.Globalization;
using JetDatabaseWriter.Exceptions;

using static JetDatabaseWriter.Schema.Expressions.AccessExpressionText;
using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionLimits;

/// <summary>
/// Parses Access expressions directly into evaluator nodes. Binary operators are
/// left-associative, including exponentiation, which binds more tightly than unary
/// signs. Field references retain their qualifier without rewriting source text.
/// </summary>
internal sealed class CalculatedExpressionParser
{
    private readonly string expression;
    private readonly bool isCalculatedColumn;
    private readonly List<Token> tokens;
    private readonly int end;
    private int position;
    private int depth;
    private bool stopAtBetweenAnd;

    private CalculatedExpressionParser(string expression, bool isCalculatedColumn, List<Token> tokens, int start, int end)
    {
        this.expression = expression;
        this.isCalculatedColumn = isCalculatedColumn;
        this.tokens = tokens;
        this.position = start;
        this.end = end;
    }

    public static CalculatedExpressionNode Parse(string expression, bool isCalculatedColumn = true)
    {
        ValidateExpressionShape(expression, MaxExpressionLength, "Calculated-column expression");
        List<Token> tokens = Tokenize(expression, isCalculatedColumn, skipLeadingEquals: true);
        return Parse(expression, tokens, 0, tokens.Count, isCalculatedColumn);
    }

    internal static CalculatedExpressionNode Parse(string expression, List<Token> tokens, int start, int end, bool isCalculatedColumn, CalculatedExpressionNode? left = null)
    {
        var parser = new CalculatedExpressionParser(expression, isCalculatedColumn, tokens, start, end);
        CalculatedExpressionNode root = parser.ParseExpression(0, left);
        if (parser.Peek().Kind != TokenKind.End)
        {
            throw parser.SyntaxError($"unexpected '{parser.Peek().Text}' after a complete expression");
        }

        return root;
    }

    private static bool IsOperandPosition(List<Token> tokens)
    {
        if (tokens.Count == 0)
        {
            return true;
        }

        Token previous = tokens[^1];
        return previous.Kind == TokenKind.Word
            ? !AccessExpressionKeywords.IsValueWord(previous.Text)
            : previous.Kind is TokenKind.Operator or TokenKind.OpenParen or TokenKind.Comma;
    }

    private static BinaryOperatorInfo? GetBinaryOperator(Token token)
    {
        if (token.Kind == TokenKind.Operator)
        {
            return token.Text switch
            {
                "^" => new BinaryOperatorInfo("^", 12),
                "*" or "/" => new BinaryOperatorInfo(token.Text, 11),
                "\\" => new BinaryOperatorInfo("INTDIV", 10),
                "+" or "-" => new BinaryOperatorInfo(token.Text, 8),
                "&" => new BinaryOperatorInfo("&", 7),
                "=" or "<>" or "<" or "<=" or ">" or ">=" => new BinaryOperatorInfo(token.Text, 6),
                _ => null,
            };
        }

        return token.Kind != TokenKind.Word ? null : token.Text.ToUpperInvariant() switch
        {
            "IMP" => new BinaryOperatorInfo("IMP", 1),
            "EQV" => new BinaryOperatorInfo("EQV", 2),
            "XOR" => new BinaryOperatorInfo("XOR", 3),
            "OR" => new BinaryOperatorInfo("OR", 4),
            "AND" => new BinaryOperatorInfo("AND", 5),
            "IS" or "LIKE" or "BETWEEN" or "IN" or "NOT" => new BinaryOperatorInfo(token.Text.ToUpperInvariant(), 6),
            "MOD" => new BinaryOperatorInfo("MOD", 9),
            _ => null,
        };
    }

    private static CalculatedExpressionFunctionNode Function(string name, params CalculatedExpressionNode[] args)
    {
        ValidateFunctionArgumentCount(name, args.Length);
        return new CalculatedExpressionFunctionNode(name, args);
    }

    internal static List<Token> Tokenize(string expression, bool isCalculatedColumn, bool skipLeadingEquals = false)
        => new Lexer(expression, isCalculatedColumn).Tokenize(skipLeadingEquals);

    private static ArgumentException InvalidSyntax(string expression, string detail)
        => new($"Calculated-column expression '{expression}' is not valid Access expression syntax: {detail}.");

    private CalculatedExpressionNode ParseExpression(int minimumPrecedence, CalculatedExpressionNode? left = null)
    {
        // Count recursive grammar edges, not a flat left-associative operator chain.
        if (this.depth > MaxExpressionNesting)
        {
            throw this.SyntaxError($"nesting depth exceeds the safety limit of {MaxExpressionNesting}");
        }

        this.depth++;
        try
        {
            left ??= this.ParsePrefix();
            while (true)
            {
                Token token = this.Peek();
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
                    "LIKE" or "MOD" or "INTDIV" or "AND" or "OR" or "XOR" or "EQV" or "IMP" =>
                        Function(info.Value.Name, left, this.ParseExpression(info.Value.Precedence + 1)),
                    _ => new CalculatedExpressionBinaryNode(info.Value.Name, left, this.ParseExpression(info.Value.Precedence + 1)),
                };
            }

            return left;
        }
        finally
        {
            this.depth--;
        }
    }

    private CalculatedExpressionNode ParsePrefix()
    {
        Token token = this.Read();
        if (token.IsWord("NOT"))
        {
            return Function("NOT", this.ParseExpression(6));
        }

        if (token.Kind == TokenKind.Operator && (token.Text is "+" or "-"))
        {
            return new CalculatedExpressionUnaryNode(token.Text == "-", this.ParseExpression(12));
        }

        switch (token.Kind)
        {
            case TokenKind.Identifier:
                return this.Peek().Kind == TokenKind.OpenParen
                    ? this.ParseFunctionCall(token.Text)
                    : new CalculatedExpressionNameNode(token.Text);
            case TokenKind.Word:
                return token.Text.ToUpperInvariant() switch
                {
                    "YES" or "ON" or "TRUE" => new CalculatedExpressionValueNode(true),
                    "NO" or "OFF" or "FALSE" => new CalculatedExpressionValueNode(false),
                    "NULL" => new CalculatedExpressionValueNode(DBNull.Value),
                    _ => throw this.SyntaxError($"unexpected '{token.Text}' where an operand is expected"),
                };
            case TokenKind.Value:
                return token.Node!;
            case TokenKind.OpenParen:
                CalculatedExpressionNode inner = this.ParseExpression(0);
                this.Expect(TokenKind.CloseParen, ")");
                return inner;
            case TokenKind.End:
            case TokenKind.Operator:
            case TokenKind.CloseParen:
            case TokenKind.Comma:
                throw this.SyntaxError(token.Kind == TokenKind.End ? "the expression ends where an operand is expected" : $"unexpected '{token.Text}' where an operand is expected");
            default:
                throw new JetOperationException(JetErrorCode.InvalidExpression, $"Unexpected calculated-column token kind '{token.Kind}'.");
        }
    }

    private CalculatedExpressionFunctionNode ParseFunctionCall(string name)
    {
        if (name.EndsWith('$'))
        {
            name = name[..^1];
        }

        if (this.isCalculatedColumn && name.ToUpperInvariant() is "PARTITION" or "CBOOL" or "CBYTE" or "CINT" or "CLNG" or "CSNG" or "CCUR" or "CDEC" or "CSTR" or "CDATE" or "TIMEVALUE" or "DATEVALUE")
        {
            throw new JetNotSupportedException(JetErrorCode.UnsupportedExpression, $"Access table calculated columns do not accept the {name} function.");
        }

        this.Expect(TokenKind.OpenParen, "(");
        var arguments = new List<CalculatedExpressionNode>();
        if (this.Peek().Kind != TokenKind.CloseParen)
        {
            do
            {
                arguments.Add(this.ParseExpression(0));
                ValidateFunctionArgumentCount(name, arguments.Count);
            }
            while (this.ReadComma());
        }

        this.Expect(TokenKind.CloseParen, ")");
        return new CalculatedExpressionFunctionNode(name, arguments);
    }

    private CalculatedExpressionNode ParseIs(CalculatedExpressionNode left)
    {
        bool negate = this.Peek().IsWord("NOT");
        if (negate)
        {
            this.Read();
        }

        if (!this.Read().IsWord("NULL"))
        {
            throw this.SyntaxError("'Is' is only supported for Null checks");
        }

        CalculatedExpressionNode node = Function("ISNULL", left);
        return negate ? Function("NOT", node) : node;
    }

    private CalculatedExpressionNode ParsePostfixNot(CalculatedExpressionNode left, int precedence)
    {
        Token token = this.Read();
        return token.Text.ToUpperInvariant() switch
        {
            "LIKE" when token.Kind == TokenKind.Word => Function("NOT", Function("LIKE", left, this.ParseExpression(precedence + 1))),
            "IN" when token.Kind == TokenKind.Word => this.ParseIn(left, negate: true),
            "BETWEEN" when token.Kind == TokenKind.Word => this.ParseBetween(left, negate: true),
            _ => throw this.SyntaxError($"unexpected '{token.Text}' after Not"),
        };
    }

    private CalculatedExpressionNode ParseBetween(CalculatedExpressionNode left, bool negate)
    {
        bool previousStop = this.stopAtBetweenAnd;
        this.stopAtBetweenAnd = true;
        CalculatedExpressionNode lower;
        try
        {
            lower = this.ParseExpression(0);
        }
        finally
        {
            this.stopAtBetweenAnd = previousStop;
        }

        if (!this.Read().IsWord("AND"))
        {
            throw this.SyntaxError("Between is missing its And separator");
        }

        CalculatedExpressionNode node = Function("BETWEEN", left, lower, this.ParseExpression(7));
        return negate ? Function("NOT", node) : node;
    }

    private CalculatedExpressionFunctionNode ParseIn(CalculatedExpressionNode left, bool negate)
    {
        this.Expect(TokenKind.OpenParen, "(");
        var values = new List<CalculatedExpressionNode> { left };
        if (this.Peek().Kind != TokenKind.CloseParen)
        {
            do
            {
                values.Add(this.ParseExpression(0));
                ValidateFunctionArgumentCount("IN", values.Count);
            }
            while (this.ReadComma());
        }

        this.Expect(TokenKind.CloseParen, ")");
        var node = new CalculatedExpressionFunctionNode("IN", values);
        return negate ? Function("NOT", node) : node;
    }

    private bool ReadComma()
    {
        if (this.Peek().Kind != TokenKind.Comma)
        {
            return false;
        }

        this.Read();
        return true;
    }

    private ArgumentException SyntaxError(string detail)
        => InvalidSyntax(this.expression, detail);

    private Token Peek() => this.position < this.end ? this.tokens[this.position] : new Token(TokenKind.End, string.Empty);

    private Token Read()
    {
        Token token = this.Peek();
        if (token.Kind != TokenKind.End)
        {
            this.position++;
        }

        return token;
    }

    private void Expect(TokenKind kind, string text)
    {
        Token token = this.Read();
        if (token.Kind != kind)
        {
            throw this.SyntaxError(token.Kind == TokenKind.End ? $"expected '{text}' before the end of the expression" : $"expected '{text}' but found '{token.Text}'");
        }
    }

    private sealed class Lexer(string expression, bool isCalculatedColumn)
    {
        public List<Token> Tokenize(bool skipLeadingEquals)
        {
            var result = new List<Token>();
            int index = SkipWhiteSpace(expression, 0);
            if (skipLeadingEquals && index < expression.Length && expression[index] == '=')
            {
                index++;
            }

            while (index < expression.Length)
            {
                char current = expression[index];
                if (char.IsWhiteSpace(current))
                {
                    index++;
                    continue;
                }

                int start = index;
                if (current is '\'' or '"')
                {
                    string value = this.ReadQuotedText(ref index);
                    result.Add(new Token(TokenKind.Value, expression[start..index], new CalculatedExpressionValueNode(value)));
                }
                else if (current == '#')
                {
                    int end = expression.IndexOf('#', index + 1);
                    if (end < 0)
                    {
                        throw this.SyntaxError("unterminated date literal");
                    }

                    string value = expression[(index + 1)..end];
                    index = end + 1;

                    // A date literal is permitted where an explicit CDate call is not.
                    result.Add(new Token(TokenKind.Value, expression[start..index], Function("CDATE", new CalculatedExpressionValueNode(value))));
                }
                else if (current == '[' || IsNameStart(current))
                {
                    result.Add(this.ReadName(ref index));
                }
                else if (char.IsDigit(current) || current == '.')
                {
                    result.Add(this.ReadNumber(ref index));
                }
                else if (current == '&' && IsOperandPosition(result) && this.TryReadRadixLiteral(ref index, out object? value))
                {
                    result.Add(new Token(TokenKind.Value, expression[start..index], new CalculatedExpressionValueNode(value)));
                }
                else
                {
                    index++;
                    TokenKind kind = current switch
                    {
                        '(' => TokenKind.OpenParen,
                        ')' => TokenKind.CloseParen,
                        ',' => TokenKind.Comma,
                        '%' => throw this.SyntaxError("'%' is not an Access operator. Divide by 100 instead"),
                        _ => TokenKind.Operator,
                    };
                    if (index < expression.Length && (expression[start..(index + 1)] is "<>" or "<=" or ">="))
                    {
                        index++;
                    }

                    result.Add(new Token(kind, expression[start..index]));
                }
            }

            return result;
        }

        private Token ReadName(ref int index)
        {
            int start = index;
            string first = this.ReadReferenceSegment(ref index, out bool bracketed);
            if (index < expression.Length && (expression[index] is '.' or '!'))
            {
                index++;
                string column = this.ReadReferenceSegment(ref index, out _);
                if (index < expression.Length && (expression[index] is '.' or '!'))
                {
                    throw this.SyntaxError("object references with more than two segments cannot be evaluated in a row context");
                }

                if (!isCalculatedColumn)
                {
                    throw new JetNotSupportedException(JetErrorCode.UnsupportedExpression, "Access table and field rules and defaults do not accept table-qualified field references.");
                }

                return new Token(TokenKind.Value, expression[start..index], new CalculatedExpressionFieldNode(column, first, rowLocalQualifier: !bracketed));
            }

            if (bracketed)
            {
                return new Token(TokenKind.Value, expression[start..index], new CalculatedExpressionFieldNode(first));
            }

            if (index < expression.Length && expression[index] == '$')
            {
                index++;
            }

            string name = expression[start..index];
            return new Token(AccessExpressionKeywords.IsKeyword(name) ? TokenKind.Word : TokenKind.Identifier, name);
        }

        private string ReadReferenceSegment(ref int index, out bool bracketed)
        {
            bracketed = index < expression.Length && expression[index] == '[';
            if (!bracketed && (index >= expression.Length || !IsNameStart(expression[index])))
            {
                throw this.SyntaxError("a qualified reference has no valid name after its separator");
            }

            int start = index;
            index = SkipNameSegment(expression, index);
            if (index < 0 || (bracketed && index == start + 2))
            {
                throw this.SyntaxError("empty or unterminated field reference");
            }

            return bracketed ? expression[(start + 1)..(index - 1)] : expression[start..index];
        }

        private string ReadQuotedText(ref int index)
        {
            char quote = expression[index];
            int start = index + 1;
            index = SkipQuoted(expression, index);
            return index < 0
                ? throw this.SyntaxError("unterminated text literal")
                : expression[start..(index - 1)].Replace(new string(quote, 2), quote.ToString(), StringComparison.Ordinal);
        }

        private Token ReadNumber(ref int index)
        {
            int start = index;
            index = SkipNumber(expression, index);
            string text = expression[start..index];
            if (!double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out double value) || double.IsInfinity(value))
            {
                throw this.SyntaxError($"invalid numeric literal '{text}'");
            }

            return new Token(TokenKind.Value, text, new CalculatedExpressionValueNode(value));
        }

        private bool TryReadRadixLiteral(ref int index, out object? literal)
        {
            literal = null;
            int cursor = index;
            int radix = ReadRadixPrefix(expression, ref cursor);
            if (radix == 0)
            {
                return false;
            }

            int digitsStart = cursor;
            ulong value = 0;
            while (cursor < expression.Length && TryGetDigit(expression[cursor], radix, out int digit))
            {
                value = (value * (ulong)radix) + (ulong)digit;
                cursor++;
                if (value > uint.MaxValue)
                {
                    throw this.SyntaxError("a radix literal is too large for a Long");
                }
            }

            if (cursor == digitsStart)
            {
                throw this.SyntaxError($"'{expression[index..cursor]}' must be followed by {(radix == 16 ? "hexadecimal" : "octal")} digits");
            }

            bool longSuffix = cursor < expression.Length && expression[cursor] == '&';
            if (longSuffix)
            {
                cursor++;
            }

            // Box separately so the conditional expression cannot promote Integer to Long.
            literal = longSuffix || value > ushort.MaxValue
                ? (object)unchecked((int)(uint)value)
                : unchecked((short)(ushort)value);
            index = cursor;
            return true;
        }

        private ArgumentException SyntaxError(string detail) => InvalidSyntax(expression, detail);
    }

    private readonly record struct BinaryOperatorInfo(string Name, int Precedence);

    internal readonly record struct Token(TokenKind Kind, string Text, CalculatedExpressionNode? Node = null)
    {
        public bool IsWord(string text) => this.Kind == TokenKind.Word && this.Text.Equals(text, StringComparison.OrdinalIgnoreCase);

        public bool IsComparison => this.Kind == TokenKind.Operator && this.Text is "=" or "<>" or "<" or "<=" or ">" or ">=";

        public bool IsComparisonKeyword() => this.IsWord("LIKE") || this.IsWord("BETWEEN") || this.IsWord("IN") || this.IsWord("IS");
    }

    internal enum TokenKind
    {
        End = 0,
        Identifier = 1,
        Value = 2,
        Word = 3,
        Operator = 4,
        OpenParen = 5,
        CloseParen = 6,
        Comma = 7,
    }
}
