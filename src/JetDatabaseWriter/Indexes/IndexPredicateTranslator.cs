namespace JetDatabaseWriter.Indexes;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Mapping;
using JetDatabaseWriter.Models;

/// <summary>
/// Extracts the index-seekable necessary conditions from a typed row predicate so
/// the reader can infer an index for it.
/// </summary>
/// <remarks>
/// <para>
/// Only conjuncts combined with logical AND (<c>&amp;&amp;</c>) are extracted.
/// Each AND conjunct is a necessary condition that every matching row must
/// satisfy, so an index seek returns a <em>superset</em> of the true matches;
/// the caller then applies the fully compiled predicate to discard the surplus.
/// Comparisons are extracted in evaluation order until the first conjunct the
/// translator cannot model — OR branches, method calls, computed members such
/// as <c>o.When.Year</c>, column-to-column comparisons, and so on. That conjunct
/// and every later one stay in the client-side filter: seeking a later condition
/// could skip a row whose earlier residual condition must throw.
/// </para>
/// <para>
/// Seek operands are limited to literals, captured instance fields and lossless
/// conversions that can be read without running user code or throwing. Property
/// getters, static members and computed operands remain in the residual predicate,
/// where earlier conjuncts and an empty source can prevent their evaluation.
/// </para>
/// <para>
/// Each property resolves to its column through <see cref="EntityMap"/>, the same
/// mapping the POCO row mapper uses, so <c>[Column("...")]</c> renames are honoured and
/// <c>[NotMapped]</c> properties are never pushed. The reader matches the column names
/// to index key columns case-insensitively.
/// </para>
/// <para>
/// Column-side conversions are stripped only when they preserve every value and null:
/// numeric widenings, enum conversions with lossless underlying types, nullable lifting,
/// and reference upcasts. Narrowing, rounding, nullable unwrapping and user-defined
/// conversions stay in the residual predicate, because seeking the original column could
/// miss a converted match or suppress an exception.
/// </para>
/// <para>
/// The translator does not know the columns' types. Before it plans a seek, the reader keeps
/// only the comparisons <see cref="IndexSeekFilter"/> finds a seek answers exactly, for
/// example not an enum property bound to a Text column, whose operand is the enum's integer.
/// </para>
/// </remarks>
internal static class IndexPredicateTranslator
{
    private const int TemplateCacheCapacity = 64;

    private static readonly BoundedCache<Shape, SeekCriteriaTemplate> Templates = new(TemplateCacheCapacity);

    private static readonly Dictionary<ExpressionType, ColumnPredicateOperator> OperatorMap = new()
    {
        [ExpressionType.Equal] = ColumnPredicateOperator.Equal,
        [ExpressionType.GreaterThan] = ColumnPredicateOperator.GreaterThan,
        [ExpressionType.GreaterThanOrEqual] = ColumnPredicateOperator.GreaterThanOrEqual,
        [ExpressionType.LessThan] = ColumnPredicateOperator.LessThan,
        [ExpressionType.LessThanOrEqual] = ColumnPredicateOperator.LessThanOrEqual,
    };

    internal static int CachedTemplateCount => Templates.Count;

    /// <summary>
    /// Pulls the leading AND-combined comparisons that can be modelled as
    /// <see cref="ColumnPredicate"/> values out of <paramref name="predicate"/>.
    /// </summary>
    /// <param name="predicate">The row predicate to inspect.</param>
    /// <returns>
    /// The pushable conjuncts as a <see cref="RowCriteria"/>. The result is always
    /// a (possibly empty) subset of the predicate's necessary conditions.
    /// </returns>
    [RequiresUnreferencedCode("Runtime predicates discover the entity mapping from expression metadata.")]
    public static RowCriteria ExtractPushableCriteria(LambdaExpression predicate)
    {
        Guard.NotNull(predicate, nameof(predicate));
        return ExtractPushableCriteria(predicate, predicate.Parameters.Count == 1 ? EntityMap.For(predicate.Parameters[0].Type) : null);
    }

    /// <summary>Extracts typed predicates while preserving the entity's mapped properties.</summary>
    /// <typeparam name="T">The mapped entity type.</typeparam>
    /// <param name="predicate">The row predicate.</param>
    /// <returns>The index-seekable criteria.</returns>
    public static RowCriteria ExtractPushableCriteria<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(Expression<Func<T, bool>> predicate)
    {
        Guard.NotNull(predicate, nameof(predicate));
        return ExtractPushableCriteria(predicate, EntityMap.For(typeof(T)));
    }

    private static RowCriteria ExtractPushableCriteria(LambdaExpression predicate, EntityMap? map)
    {
        Guard.NotNull(predicate, nameof(predicate));

        var criteria = new RowCriteria();
        if (predicate.Parameters.Count != 1 || map is null)
        {
            return criteria;
        }

        ParameterExpression parameter = predicate.Parameters[0];
        if (!HasCollectibleType(parameter.Type))
        {
            var visitor = new ShapeVisitor(parameter);
            _ = visitor.Visit(predicate.Body);
            if (visitor.Supported)
            {
                var shape = new Shape(parameter.Type, visitor.Tokens.ToArray());
                SeekCriteriaTemplate template = Templates.GetOrCreate(
                    shape,
                    _ => BuildTemplate(predicate.Body, parameter, map, visitor.ConstantSlots));
                return template.Instantiate(visitor.Constants);
            }
        }

        var conjuncts = new List<Expression>();
        CollectAndConjuncts(predicate.Body, conjuncts);

        foreach (Expression conjunct in conjuncts)
        {
            if (!TryTranslateComparison(conjunct, parameter, map, out ColumnPredicate? model))
            {
                // A later seek must not suppress exceptions from this residual conjunct.
                break;
            }

            criteria.Add(model);
        }

        return criteria;
    }

    private static SeekCriteriaTemplate BuildTemplate(
        Expression body,
        ParameterExpression parameter,
        EntityMap map,
        Dictionary<ConstantExpression, int> constantSlots)
    {
        var conjuncts = new List<Expression>();
        CollectAndConjuncts(body, conjuncts);

        var slots = new List<ComparisonSlot>(conjuncts.Count);
        foreach (Expression conjunct in conjuncts)
        {
            if (!TryTranslateComparisonSlot(conjunct, parameter, map, constantSlots, out ComparisonSlot slot))
            {
                break;
            }

            slots.Add(slot);
        }

        return new SeekCriteriaTemplate(slots.ToArray());
    }

    private static bool TryTranslateComparisonSlot(
        Expression expression,
        ParameterExpression parameter,
        EntityMap map,
        Dictionary<ConstantExpression, int> constantSlots,
        out ComparisonSlot slot)
    {
        slot = default;
        if (expression is not BinaryExpression binary || !HasNonThrowingComparisonOperator(binary))
        {
            return false;
        }

        if (MapOperator(binary.NodeType) is not ColumnPredicateOperator @operator)
        {
            return false;
        }

        if (TryResolveColumn(binary.Left, parameter, map, out string? leftColumn)
            && TryBuildOperandTemplate(binary.Right, constantSlots, out int rightConstant, out FieldInfo[]? rightFields, out (Type From, Type To)[]? rightConversions))
        {
            slot = new ComparisonSlot(leftColumn!, @operator, rightConstant, rightFields!, rightConversions!);
            return true;
        }

        if (TryResolveColumn(binary.Right, parameter, map, out string? rightColumn)
            && TryBuildOperandTemplate(binary.Left, constantSlots, out int leftConstant, out FieldInfo[]? leftFields, out (Type From, Type To)[]? leftConversions))
        {
            slot = new ComparisonSlot(rightColumn!, Flip(@operator), leftConstant, leftFields!, leftConversions!);
            return true;
        }

        return false;
    }

    private static bool TryBuildOperandTemplate(
        Expression expression,
        Dictionary<ConstantExpression, int> constantSlots,
        out int constantIndex,
        [NotNullWhen(true)] out FieldInfo[]? fields,
        [NotNullWhen(true)] out (Type From, Type To)[]? conversions)
    {
        constantIndex = -1;
        fields = null;
        conversions = null;

        if (!IsNonThrowingSeekOperand(expression))
        {
            return false;
        }

        var conversionList = new List<(Type From, Type To)>();
        Expression current = expression;
        while (current is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } convert)
        {
            if ((convert.Method is not null && !ClosureValueReader.IsDecimalWidening(convert.Method))
                || !ClosureValueReader.CanConvertTypes(convert.Operand.Type, convert.Type))
            {
                return false;
            }

            conversionList.Add((convert.Operand.Type, convert.Type));
            current = convert.Operand;
        }

        var fieldList = new List<FieldInfo>();
        while (current is MemberExpression { Member: FieldInfo { IsStatic: false } field, Expression: not null } member)
        {
            fieldList.Add(field);
            current = member.Expression;
        }

        if (current is not ConstantExpression constant || !constantSlots.TryGetValue(constant, out constantIndex))
        {
            return false;
        }

        fieldList.Reverse();
        conversionList.Reverse();
        fields = fieldList.Count == 0 ? [] : fieldList.ToArray();
        conversions = conversionList.Count == 0 ? [] : conversionList.ToArray();
        return true;
    }

    private static bool HasCollectibleType(Type type) =>
        Queries.QueryPlanTemplateCache.HasCollectibleType(type);

    private static void CollectAndConjuncts(Expression expression, List<Expression> conjuncts)
    {
        Expression unwrapped = StripConvert(expression);
        if (unwrapped is BinaryExpression { NodeType: ExpressionType.AndAlso, Method: null } and)
        {
            CollectAndConjuncts(and.Left, conjuncts);
            CollectAndConjuncts(and.Right, conjuncts);
            return;
        }

        conjuncts.Add(unwrapped);
    }

    private static bool TryTranslateComparison(
        Expression expression,
        ParameterExpression parameter,
        EntityMap map,
        [NotNullWhen(true)] out ColumnPredicate? predicate)
    {
        predicate = null;
        if (expression is not BinaryExpression binary || !HasNonThrowingComparisonOperator(binary))
        {
            return false;
        }

        if (MapOperator(binary.NodeType) is not ColumnPredicateOperator @operator)
        {
            return false;
        }

        if (TryResolveColumn(binary.Left, parameter, map, out string? leftColumn)
            && TryReadSeekValue(binary.Right, out object? rightValue))
        {
            predicate = Build(leftColumn!, @operator, rightValue);
            return predicate is not null;
        }

        if (TryResolveColumn(binary.Right, parameter, map, out string? rightColumn)
            && TryReadSeekValue(binary.Left, out object? leftValue))
        {
            // `value < o.X`  ==>  `o.X > value`.
            predicate = Build(rightColumn!, Flip(@operator), leftValue);
            return predicate is not null;
        }

        return false;
    }

    /// <summary>Recognizes built-in operators and nonthrowing framework comparisons.</summary>
    /// <param name="comparison">The comparison expression.</param>
    /// <returns>Whether the operator is safe to seek or skip before a later conjunct.</returns>
    private static bool HasNonThrowingComparisonOperator(BinaryExpression comparison) =>
        comparison.Method is not MethodInfo method
        || method.DeclaringType == typeof(string)
        || method.DeclaringType == typeof(decimal)
        || method.DeclaringType == typeof(DateTime)
        || method.DeclaringType == typeof(DateTimeOffset)
        || method.DeclaringType == typeof(TimeSpan)
        || method.DeclaringType == typeof(Guid);

    private static ColumnPredicate? Build(string column, ColumnPredicateOperator @operator, object? value) => @operator switch
    {
        ColumnPredicateOperator.Equal => ColumnPredicate.EqualTo(column, value),
        ColumnPredicateOperator.GreaterThan => value is null ? null : ColumnPredicate.GreaterThan(column, value),
        ColumnPredicateOperator.GreaterThanOrEqual => value is null ? null : ColumnPredicate.GreaterThanOrEqual(column, value),
        ColumnPredicateOperator.LessThan => value is null ? null : ColumnPredicate.LessThan(column, value),
        ColumnPredicateOperator.LessThanOrEqual => value is null ? null : ColumnPredicate.LessThanOrEqual(column, value),
        ColumnPredicateOperator.NotEqual => null,
        ColumnPredicateOperator.Between => null,
        ColumnPredicateOperator.In => null,
        ColumnPredicateOperator.IsNull => null,
        ColumnPredicateOperator.IsNotNull => null,
        _ => null,
    };

    private static ColumnPredicateOperator? MapOperator(ExpressionType nodeType) =>
        OperatorMap.TryGetValue(nodeType, out ColumnPredicateOperator @operator) ? @operator : null;

    private static ColumnPredicateOperator Flip(ColumnPredicateOperator @operator) => @operator switch
    {
        ColumnPredicateOperator.GreaterThan => ColumnPredicateOperator.LessThan,
        ColumnPredicateOperator.GreaterThanOrEqual => ColumnPredicateOperator.LessThanOrEqual,
        ColumnPredicateOperator.LessThan => ColumnPredicateOperator.GreaterThan,
        ColumnPredicateOperator.LessThanOrEqual => ColumnPredicateOperator.GreaterThanOrEqual,
        ColumnPredicateOperator.Equal => ColumnPredicateOperator.Equal,
        ColumnPredicateOperator.NotEqual => ColumnPredicateOperator.NotEqual,
        ColumnPredicateOperator.Between => ColumnPredicateOperator.Between,
        ColumnPredicateOperator.In => ColumnPredicateOperator.In,
        ColumnPredicateOperator.IsNull => ColumnPredicateOperator.IsNull,
        ColumnPredicateOperator.IsNotNull => ColumnPredicateOperator.IsNotNull,
        _ => @operator,
    };

    private static bool TryResolveColumn(Expression expression, ParameterExpression parameter, EntityMap map, out string? columnName)
    {
        columnName = null;
        if (StripConvert(expression) is not MemberExpression member)
        {
            return false;
        }

        if (member.Member is not PropertyInfo property)
        {
            return false;
        }

        // Only a direct member of the lambda parameter (`o.Column`) maps to a
        // column. Nested members such as `o.When.Year` resolve their inner
        // expression to another member, not the parameter, and are left to the
        // client-side filter.
        if (member.Expression is null || StripConvert(member.Expression) != parameter)
        {
            return false;
        }

        // The column is the one the row mapper binds the property to, so a
        // [Column("Last Name")] property seeks the "Last Name" index. A
        // [NotMapped] or read-only property is not a column and is left to the
        // client-side filter.
        if (map.FindByMember(property) is not EntityProperty mapped)
        {
            return false;
        }

        columnName = mapped.ColumnName;
        return true;
    }

    private static bool TryReadSeekValue(Expression expression, out object? value)
    {
        // A null field-chain receiver stays residual; never compile a fallback here.
        value = null;
        return IsNonThrowingSeekOperand(expression)
            && ClosureValueReader.TryRead(expression, out value);
    }

    private static bool IsNonThrowingSeekOperand(Expression expression) => expression switch
    {
        ConstantExpression => true,
        MemberExpression { Member: FieldInfo { IsStatic: false }, Expression: not null } member =>
            IsNonThrowingSeekOperand(member.Expression),
        UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } conversion =>
            IsLosslessColumnConversion(conversion) && IsNonThrowingSeekOperand(conversion.Operand),
        _ => false,
    };

    private static Expression StripConvert(Expression expression)
    {
        Expression current = expression;
        while (current is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary
            && IsLosslessColumnConversion(unary))
        {
            current = unary.Operand;
        }

        return current;
    }

    private static bool IsLosslessColumnConversion(UnaryExpression conversion)
    {
        Type? fromNullable = Nullable.GetUnderlyingType(conversion.Operand.Type);
        Type? toNullable = Nullable.GetUnderlyingType(conversion.Type);
        if (fromNullable is not null && toNullable is null)
        {
            // Unwrapping null throws; an index seek could skip the row that must throw.
            return false;
        }

        Type from = fromNullable ?? conversion.Operand.Type;
        Type to = toNullable ?? conversion.Type;
        if (from.IsEnum)
        {
            from = Enum.GetUnderlyingType(from);
        }

        if (to.IsEnum)
        {
            to = Enum.GetUnderlyingType(to);
        }

        bool widening = ClosureValueReader.IsLosslessWidening(from, to);
        if (conversion.Method is not null)
        {
            // Integral-to-decimal conversions use an operator method. Other user-defined
            // conversions can change values even when their source and target types agree.
            return widening
                && conversion.Method.DeclaringType == typeof(decimal)
                && conversion.Method.Name == "op_Implicit";
        }

        return from == to
            || widening
            || (!from.IsValueType && !to.IsValueType && to.IsAssignableFrom(from));
    }

    private readonly record struct ComparisonSlot(
        string Column,
        ColumnPredicateOperator Operator,
        int ConstantIndex,
        FieldInfo[] Fields,
        (Type From, Type To)[] Conversions)
    {
        public bool TryEvaluate(IReadOnlyList<object?> constants, out object? value)
        {
            value = constants[this.ConstantIndex];
            foreach (FieldInfo field in this.Fields)
            {
                if (value is null)
                {
                    return false;
                }

                value = field.GetValue(value);
            }

            foreach ((Type from, Type to) in this.Conversions)
            {
                if (!ClosureValueReader.TryConvert(value, from, to, out value))
                {
                    return false;
                }
            }

            return true;
        }
    }

    private sealed class SeekCriteriaTemplate(ComparisonSlot[] slots)
    {
        public RowCriteria Instantiate(IReadOnlyList<object?> constants)
        {
            var criteria = new RowCriteria();
            foreach (ComparisonSlot slot in slots)
            {
                if (!slot.TryEvaluate(constants, out object? value))
                {
                    break;
                }

                ColumnPredicate? predicate = Build(slot.Column, slot.Operator, value);
                if (predicate is null)
                {
                    break;
                }

                criteria.Add(predicate);
            }

            return criteria;
        }
    }

    private sealed class Shape(Type parameterType, object?[] tokens) : IEquatable<Shape>
    {
        private Type ParameterType => parameterType;

        private object?[] Tokens => tokens;

        public bool Equals(Shape? other)
        {
            if (other is null || parameterType != other.ParameterType || tokens.Length != other.Tokens.Length)
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
            hash.Add(parameterType);
            foreach (object? token in tokens)
            {
                hash.Add(token);
            }

            return hash.ToHashCode();
        }
    }

    private sealed class ShapeVisitor(ParameterExpression rootParameter) : ExpressionVisitor
    {
        private const int MaximumNodes = 256;
        private const int MaximumDepth = 64;
        private int nodes;
        private int depth;

        internal List<object?> Tokens { get; } = [];

        internal List<object?> Constants { get; } = [];

        internal Dictionary<ConstantExpression, int> ConstantSlots { get; } = [];

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

            if (++this.nodes > MaximumNodes || this.depth >= MaximumDepth || HasCollectibleType(node.Type))
            {
                this.Supported = false;
                return node;
            }

            this.Tokens.Add(node.NodeType);
            this.Tokens.Add(node.Type);
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
                case ConstantExpression constant:
                    if (!this.ConstantSlots.TryGetValue(constant, out int slot))
                    {
                        slot = this.Constants.Count;
                        this.ConstantSlots.Add(constant, slot);
                        this.Constants.Add(constant.Value);
                    }

                    this.Tokens.Add(slot);
                    return node;
                case ParameterExpression parameter:
                    this.Tokens.Add(ReferenceEquals(parameter, rootParameter) ? 0 : -1);
                    this.Tokens.Add(parameter.Name);
                    return node;
                default:
                    this.Tokens.Add(null);
                    break;
            }

            this.depth++;
            Expression? result = base.Visit(node);
            this.depth--;
            return result;
        }
    }
}
