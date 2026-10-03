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
/// <c>Xor</c>, <c>Eqv</c>, <c>Imp</c>. Text literals may be double- or
/// single-quoted (<c>'it''s'</c>), and <c>&amp;H</c>/<c>&amp;O</c> radix
/// literals are typed as in VBA. Syntax the Access grammar does not accept
/// (including Excel's postfix <c>%</c>) throws <see cref="ArgumentException"/>
/// naming the expression.
/// </summary>
internal static class CalculatedExpressionNormalizer
{
    internal static string Normalize(string expression, out Dictionary<string, string> placeholderToColumn)
    {
        string prepared = ReplaceFieldReferencesAndDateLiterals(expression, out placeholderToColumn);
        return AccessExpressionNormalizer.Normalize(prepared, expression);
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
            if (ch == '\'' && TryFindClosingQuote(trimmed, i, '\'', out int closingQuote))
            {
                // Access also accepts single-quoted text ('' is a quote). Rewrite it
                // as the double-quoted literal the downstream grammar reads, so the
                // brackets, # and % inside it stay text.
                string inner = trimmed.Substring(i + 1, closingQuote - i - 1).Replace("''", "'", StringComparison.Ordinal);
                builder.Append('"')
                    .Append(inner.Replace("\"", "\"\"", StringComparison.Ordinal))
                    .Append('"');
                i = closingQuote;
            }
            else if (ch == '"')
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

    /// <summary>
    /// Finds the quote that closes the string literal opening at
    /// <paramref name="openingQuote"/>, where a doubled quote is an escaped one.
    /// </summary>
    /// <param name="text">The expression text.</param>
    /// <param name="openingQuote">The index of the opening quote.</param>
    /// <param name="quote">The quote character.</param>
    /// <param name="closingQuote">The index of the closing quote.</param>
    /// <returns><see langword="false"/> when the literal is not terminated.</returns>
    private static bool TryFindClosingQuote(string text, int openingQuote, char quote, out int closingQuote)
    {
        for (int index = openingQuote + 1; index < text.Length; index++)
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

            closingQuote = index;
            return true;
        }

        closingQuote = -1;
        return false;
    }

    private sealed class AccessExpressionNormalizer
    {
        private const int UnaryLevel = 6;
        private const int AtomLevel = 8;

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
            List<Token> tokens = Tokenize(expression, originalExpression);
            if (tokens.Exists(static token => token.IsPercent))
            {
                throw new ArgumentException(
                    $"Calculated-column expression '{originalExpression}' uses '%', which is not an Access operator. Divide by 100 instead.");
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

        private static List<Token> Tokenize(string expression, string originalExpression)
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
                    result.Add(new Token(AccessExpressionKeywords.IsKeyword(text) ? TokenKind.Word : TokenKind.Identifier, text));
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

                if (current == '&' && IsOperandPosition(result) && TryReadRadixLiteral(expression, ref charIndex, originalExpression, out string literal))
                {
                    result.Add(new Token(TokenKind.Value, literal));
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

        /// <summary>
        /// Returns whether the next token starts an operand, where <c>&amp;</c> begins a
        /// radix literal rather than joining text: at the start, or after an operator,
        /// <c>(</c>, <c>,</c> or a word operator such as <c>And</c> or <c>Mod</c>.
        /// </summary>
        /// <param name="tokens">The tokens read so far.</param>
        /// <returns><see langword="true"/> when an operand is expected.</returns>
        private static bool IsOperandPosition(List<Token> tokens)
        {
            if (tokens.Count == 0)
            {
                return true;
            }

            Token previous = tokens[^1];
            if (previous.Kind == TokenKind.Word)
            {
                return !AccessExpressionKeywords.IsValueWord(previous.Text);
            }

            return previous.Kind is TokenKind.Operator or TokenKind.Backslash or TokenKind.OpenParen or TokenKind.Comma;
        }

        /// <summary>
        /// Reads a VBA radix literal at <paramref name="charIndex"/>: <c>&amp;H</c> and hex
        /// digits, <c>&amp;O</c> and octal digits, or <c>&amp;</c> and octal digits, with an
        /// optional <c>&amp;</c> suffix. As in VBA, a value up to <c>&amp;HFFFF</c> is an
        /// Integer (so <c>&amp;HFFFF</c> is -1), a larger one is a Long
        /// (<c>&amp;HFFFFFFFF</c> is -1), and the suffix makes it a Long
        /// (<c>&amp;HFFFF&amp;</c> is 65535).
        /// </summary>
        /// <param name="expression">The prepared expression text.</param>
        /// <param name="charIndex">The index of the <c>&amp;</c>; advanced past the literal when one is read.</param>
        /// <param name="originalExpression">The expression as written, for error messages.</param>
        /// <param name="literal">The literal's value as decimal text, parenthesized when negative.</param>
        /// <returns><see langword="false"/> when no radix literal starts here.</returns>
        /// <exception cref="ArgumentException"><c>&amp;H</c> or <c>&amp;O</c> has no digits, or the value needs more than 32 bits.</exception>
        private static bool TryReadRadixLiteral(string expression, ref int charIndex, string originalExpression, out string literal)
        {
            literal = string.Empty;
            int index = charIndex + 1;
            if (index >= expression.Length)
            {
                return false;
            }

            char marker = expression[index];
            int radix;
            if (marker is 'H' or 'h')
            {
                radix = 16;
                index++;
            }
            else if (marker is 'O' or 'o')
            {
                radix = 8;
                index++;
            }
            else if (marker is >= '0' and <= '7')
            {
                radix = 8;
            }
            else
            {
                return false;
            }

            int digitsStart = index;
            ulong value = 0;
            bool tooLarge = false;
            while (index < expression.Length && TryGetDigit(expression[index], radix, out int digit))
            {
                if (!tooLarge)
                {
                    value = (value * (ulong)radix) + (ulong)digit;
                    tooLarge = value > uint.MaxValue;
                }

                index++;
            }

            if (index == digitsStart)
            {
                throw new ArgumentException(
                    $"Calculated-column expression '{originalExpression}' is not valid Access expression syntax: '{expression[charIndex..digitsStart]}' must be followed by {(radix == 16 ? "hexadecimal" : "octal")} digits.");
            }

            bool longSuffix = index < expression.Length && expression[index] == '&';
            if (longSuffix)
            {
                index++;
            }

            if (tooLarge)
            {
                throw new ArgumentException(
                    $"Calculated-column expression '{originalExpression}' is not valid Access expression syntax: the literal '{expression[charIndex..index]}' is too large for a Long.");
            }

            int number = longSuffix || value > ushort.MaxValue
                ? unchecked((int)(uint)value)
                : unchecked((short)(ushort)value);
            string text = number.ToString(CultureInfo.InvariantCulture);
            literal = number < 0 ? "(" + text + ")" : text;
            charIndex = index;
            return true;
        }

        private static bool TryGetDigit(char ch, int radix, out int digit)
        {
            digit = ch switch
            {
                >= '0' and <= '9' => ch - '0',
                >= 'A' and <= 'F' => ch - 'A' + 10,
                >= 'a' and <= 'f' => ch - 'a' + 10,
                _ => radix,
            };
            return digit < radix;
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
