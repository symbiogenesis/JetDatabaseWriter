namespace JetDatabaseWriter.Queries;

using System;
using System.Collections.Generic;
using System.Linq.Expressions;

/// <summary>Reuses bounded compiled templates while binding each execution's original constants.</summary>
/// <param name="capacity">The maximum number of retained templates.</param>
internal sealed class QueryLambdaCompiler(int capacity)
{
#if NET9_0_OR_GREATER
    private readonly System.Threading.Lock gate = new();
#else
    private readonly object gate = new();
#endif
    private readonly Dictionary<Shape, LinkedListNode<Entry>> entries = [];
    private readonly LinkedList<Entry> recent = new();

    internal static QueryLambdaCompiler Shared { get; } = new(64);

    internal int Count
    {
        get
        {
            lock (this.gate)
            {
                return this.entries.Count;
            }
        }
    }

    /// <summary>Binds a typed delegate without evaluating any captured members.</summary>
    /// <typeparam name="TDelegate">The lambda's delegate type.</typeparam>
    /// <param name="expression">The query lambda.</param>
    /// <returns>The delegate bound to this lambda's constants.</returns>
    internal TDelegate Compile<TDelegate>(Expression<TDelegate> expression)
        where TDelegate : Delegate => (TDelegate)this.Compile((LambdaExpression)expression);

    /// <summary>Binds a runtime-resolved query lambda to a bounded immutable template.</summary>
    /// <param name="expression">The query lambda.</param>
    /// <returns>The delegate bound to this lambda's constants.</returns>
    internal Delegate Compile(LambdaExpression expression)
    {
        var visitor = new TemplateVisitor();
        var template = (LambdaExpression)visitor.Visit(expression)!;
        if (!visitor.Supported || capacity <= 0)
        {
            return expression.Compile();
        }

        var shape = new Shape(visitor.Tokens.ToArray());
        Func<object?[], Delegate> factory;
        lock (this.gate)
        {
            if (this.entries.TryGetValue(shape, out LinkedListNode<Entry>? existing))
            {
                this.recent.Remove(existing);
                this.recent.AddFirst(existing);
                factory = existing.Value.Factory;
            }
            else
            {
                // Only constants become parameters: fields, getters, conversions and user
                // calls stay in their original evaluation and short-circuit positions.
                factory = Expression.Lambda<Func<object?[], Delegate>>(
                    Expression.Convert(template, typeof(Delegate)), visitor.Values).Compile();
                LinkedListNode<Entry> added = this.recent.AddFirst(new Entry(shape, factory));
                this.entries.Add(shape, added);
                if (this.entries.Count > capacity && this.recent.Last is { } oldest)
                {
                    this.entries.Remove(oldest.Value.Shape);
                    this.recent.RemoveLast();
                }
            }
        }

        // The cache retains metadata and parameterized code, never the original constants,
        // query root or closure. Every returned delegate owns its independent binding array.
        return factory(visitor.Constants.ToArray());
    }

    private sealed record Entry(Shape Shape, Func<object?[], Delegate> Factory);

    private sealed class Shape(object?[] tokens) : IEquatable<Shape>
    {
        public bool Equals(Shape? other)
        {
            if (other is null || tokens.Length != other.Tokens.Length)
            {
                return false;
            }

            for (int i = 0; i < tokens.Length; i++)
            {
                if (!Equals(tokens[i], other.Tokens[i]))
                {
                    return false;
                }
            }

            return true;
        }

        public override bool Equals(object? obj) => obj is Shape other && this.Equals(other);

        public override int GetHashCode()
        {
            HashCode hash = default;
            foreach (object? token in tokens)
            {
                hash.Add(token);
            }

            return hash.ToHashCode();
        }

        private object?[] Tokens => tokens;
    }

    private sealed class TemplateVisitor : ExpressionVisitor
    {
        private const int MaximumNodes = 256;
        private const int MaximumDepth = 64;
        private readonly Dictionary<ParameterExpression, int> parameters = [];
        private int nodes;
        private int depth;

        internal List<object?> Tokens { get; } = [];

        internal List<object?> Constants { get; } = [];

        internal ParameterExpression Values { get; } = Expression.Parameter(typeof(object[]), "values");

        internal bool Supported { get; private set; } = true;

        public override Expression? Visit(Expression? node)
        {
            if (!this.Supported)
            {
                return node;
            }

            if (node is null)
            {
                this.Tokens.Add(null);
                return null;
            }

            if (++this.nodes > MaximumNodes || this.depth >= MaximumDepth || !IsSupported(node))
            {
                this.Supported = false;
                return node;
            }

            this.Tokens.Add(node.NodeType);
            this.Tokens.Add(node.Type);
            this.AddMetadata(node);
            this.depth++;
            Expression? result = base.Visit(node);
            this.depth--;
            return result;
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            int index = this.Constants.Count;
            this.Constants.Add(node.Value);
            return Expression.Convert(Expression.ArrayIndex(this.Values, Expression.Constant(index)), node.Type);
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (!this.parameters.TryGetValue(node, out int index))
            {
                index = this.parameters.Count;
                this.parameters.Add(node, index);
            }

            this.Tokens.Add(index);
            this.Tokens.Add(node.IsByRef);
            this.Tokens.Add(node.Name);
            return node;
        }

        protected override MemberBinding VisitMemberBinding(MemberBinding node)
        {
            if (!this.Supported || ++this.nodes > MaximumNodes || this.depth >= MaximumDepth)
            {
                this.Supported = false;
                return node;
            }

            this.Tokens.Add(node.BindingType);
            this.Tokens.Add(node.Member);
            this.Tokens.Add(node switch
            {
                MemberMemberBinding members => members.Bindings.Count,
                MemberListBinding list => list.Initializers.Count,
                _ => 0,
            });
            this.depth++;
            MemberBinding result = base.VisitMemberBinding(node);
            this.depth--;
            return result;
        }

        protected override ElementInit VisitElementInit(ElementInit node)
        {
            if (!this.Supported || ++this.nodes > MaximumNodes)
            {
                this.Supported = false;
                return node;
            }

            this.Tokens.Add(node.AddMethod);
            this.Tokens.Add(node.Arguments.Count);
            return base.VisitElementInit(node);
        }

        private static bool IsSupported(Expression node) => node.NodeType != ExpressionType.Quote && node is BinaryExpression
            or UnaryExpression or ConstantExpression or ParameterExpression or MemberExpression
            or MethodCallExpression or ConditionalExpression or NewArrayExpression
            or InvocationExpression or LambdaExpression or NewExpression or TypeBinaryExpression
            or MemberInitExpression or ListInitExpression or DefaultExpression or IndexExpression;

        private void AddMetadata(Expression node)
        {
            switch (node)
            {
                case BinaryExpression binary:
                    this.Tokens.Add(binary.Method);
                    this.Tokens.Add(binary.IsLiftedToNull);
                    break;
                case UnaryExpression unary:
                    this.Tokens.Add(unary.Method);
                    this.Tokens.Add(unary.IsLiftedToNull);
                    break;
                case MemberExpression member:
                    this.Tokens.Add(member.Member);
                    break;
                case MethodCallExpression call:
                    this.Tokens.Add(call.Method);
                    this.Tokens.Add(call.Arguments.Count);
                    break;
                case LambdaExpression lambda:
                    this.Tokens.Add(lambda.Parameters.Count);
                    this.Tokens.Add(lambda.TailCall);
                    this.Tokens.Add(lambda.Name);
                    break;
                case NewExpression creation:
                    this.Tokens.Add(creation.Constructor);
                    this.Tokens.Add(creation.Arguments.Count);
                    this.Tokens.Add(creation.Members?.Count ?? -1);
                    if (creation.Members is { } members)
                    {
                        this.Tokens.AddRange(members);
                    }

                    break;
                case NewArrayExpression array:
                    this.Tokens.Add(array.Expressions.Count);
                    break;
                case InvocationExpression invocation:
                    this.Tokens.Add(invocation.Arguments.Count);
                    break;
                case TypeBinaryExpression type:
                    this.Tokens.Add(type.TypeOperand);
                    break;
                case IndexExpression index:
                    this.Tokens.Add(index.Indexer);
                    this.Tokens.Add(index.Arguments.Count);
                    break;
                case MemberInitExpression initialization:
                    this.Tokens.Add(initialization.Bindings.Count);
                    break;
                case ListInitExpression list:
                    this.Tokens.Add(list.Initializers.Count);
                    break;
                default:
                    this.Tokens.Add(null);
                    break;
            }
        }
    }
}
