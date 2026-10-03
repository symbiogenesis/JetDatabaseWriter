namespace JetDatabaseWriter.Infrastructure;

using System;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;

/// <summary>
/// Reads the value of a LINQ operand that does not depend on the lambda's parameter, such
/// as a captured variable, through reflection instead of compiling the expression. The LINQ
/// translators evaluate every captured comparison value, <c>Skip</c> count and <c>Take</c>
/// count this way, so a query does not pay an expression compile per operand on every call,
/// and needs neither the non-generic <see cref="Expression.Lambda(Expression, ParameterExpression[])"/>
/// nor <see cref="Delegate.DynamicInvoke(object[])"/>, which require dynamic code.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="TryRead(Expression, out object)"/> reads exactly these shapes, and returns
/// <see langword="false"/> for anything else so the caller compiles it:
/// </para>
/// <list type="bullet">
/// <item><description>a constant;</description></item>
/// <item><description>a static field or property;</description></item>
/// <item><description>a chain of instance fields and properties rooted at a constant, which is how the C# compiler captures locals (closure fields), <c>this</c> and the objects reached from them;</description></item>
/// <item><description>a conversion of one of those to <see cref="object"/>, between a value type and its <see cref="Nullable{T}"/> (unwrapping only a value that is not null), between an enum and its underlying type, or a lossless numeric widening such as <see cref="int"/> to <see cref="long"/>.</description></item>
/// </list>
/// <para>
/// Each shape reads the same value the compiled expression would, and a property getter
/// that fails throws the same exception. A null object in the middle of a chain, or a null
/// <see cref="Nullable{T}"/> converted to its underlying type, is left to the compiled
/// expression, which throws as C# does.
/// </para>
/// </remarks>
internal static class ClosureValueReader
{
    /// <summary>
    /// Evaluates <paramref name="expression"/>: reads it when <see cref="TryRead(Expression, out object)"/>
    /// can, and otherwise compiles it once into a <see cref="Func{TResult}"/> of <see cref="object"/>
    /// and calls that.
    /// </summary>
    /// <param name="expression">An expression that does not reference a lambda parameter.</param>
    /// <returns>The expression's value, boxed.</returns>
    public static object? Evaluate(Expression expression)
    {
        Guard.NotNull(expression, nameof(expression));
        if (TryRead(expression, out object? value))
        {
            return value;
        }

        Expression body = expression.Type.IsValueType
            ? Expression.Convert(expression, typeof(object))
            : expression;
        return Expression.Lambda<Func<object?>>(body).Compile()();
    }

    /// <summary>
    /// Reads <paramref name="expression"/>'s value without compiling it, when it has one of
    /// the shapes the remarks of <see cref="ClosureValueReader"/> list.
    /// </summary>
    /// <param name="expression">The operand to read.</param>
    /// <param name="value">The value, boxed, when the method returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when the value was read; <see langword="false"/> when the caller must compile the expression.</returns>
    public static bool TryRead(Expression expression, out object? value)
    {
        Guard.NotNull(expression, nameof(expression));
        switch (expression)
        {
            case ConstantExpression constant:
                value = constant.Value;
                return true;

            case MemberExpression { Expression: null } staticMember:
                return TryReadStatic(staticMember.Member, out value);

            case MemberExpression member when TryReadChain(member.Expression, out object? instance):
                return TryReadInstance(member.Member, instance, out value);

            case UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } convert
                when (convert.Method is null || IsDecimalWidening(convert.Method)) && TryRead(convert.Operand, out object? operand):
                return TryConvert(operand, convert.Operand.Type, convert.Type, out value);

            default:
                value = null;
                return false;
        }
    }

    /// <summary>
    /// Reads the object a member chain starts from: a constant, or an instance field or
    /// property of such an object. A static member here is not a closure chain, so it is
    /// left to the compiled expression.
    /// </summary>
    /// <param name="expression">The member's target expression.</param>
    /// <param name="value">The target's value when the method returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when the target was read.</returns>
    private static bool TryReadChain(Expression? expression, out object? value)
    {
        switch (expression)
        {
            case ConstantExpression constant:
                value = constant.Value;
                return true;

            case MemberExpression { Expression: not null } member when TryReadChain(member.Expression, out object? instance):
                return TryReadInstance(member.Member, instance, out value);

            default:
                value = null;
                return false;
        }
    }

    private static bool TryReadStatic(MemberInfo member, out object? value)
    {
        switch (member)
        {
            case FieldInfo field:
                value = field.GetValue(obj: null);
                return true;

            case PropertyInfo { GetMethod: not null } property when property.GetIndexParameters().Length == 0:
                value = GetPropertyValue(property, instance: null);
                return true;

            default:
                value = null;
                return false;
        }
    }

    private static bool TryReadInstance(MemberInfo member, object? instance, out object? value)
    {
        if (member.DeclaringType is { IsGenericType: true } declaring && declaring.GetGenericTypeDefinition() == typeof(Nullable<>))
        {
            // A boxed Nullable<T> is its T, or null when it has no value.
            return TryReadNullableMember(member.Name, instance, out value);
        }

        value = null;
        if (instance is null)
        {
            // Reading a member of null throws; only the compiled expression reproduces that.
            return false;
        }

        switch (member)
        {
            case FieldInfo field:
                value = field.GetValue(instance);
                return true;

            case PropertyInfo { GetMethod: not null } property when property.GetIndexParameters().Length == 0:
                value = GetPropertyValue(property, instance);
                return true;

            default:
                return false;
        }
    }

    private static bool TryReadNullableMember(string name, object? instance, out object? value)
    {
        switch (name)
        {
            case nameof(Nullable<>.Value) when instance is not null:
                value = instance;
                return true;

            case nameof(Nullable<>.HasValue):
                value = instance is not null;
                return true;

            default:
                // Value of a null Nullable<T> throws; only the compiled expression reproduces that.
                value = null;
                return false;
        }
    }

    /// <summary>
    /// Calls a property getter. <see cref="BindingFlags.DoNotWrapExceptions"/> makes a getter
    /// that throws throw its own exception, as the compiled expression would, not a
    /// <see cref="TargetInvocationException"/>.
    /// </summary>
    /// <param name="property">The property.</param>
    /// <param name="instance">The target, or <see langword="null"/> for a static property.</param>
    /// <returns>The property's value.</returns>
    private static object? GetPropertyValue(PropertyInfo property, object? instance) =>
        property.GetValue(instance, BindingFlags.DoNotWrapExceptions, binder: null, index: null, culture: null);

    /// <summary>
    /// Returns <see langword="true"/> for the implicit conversion operator <see cref="decimal"/>
    /// declares from an integral type: expression trees express those widenings as a call to
    /// the operator, where the other numeric widenings have no method.
    /// </summary>
    /// <param name="method">The conversion's method.</param>
    /// <returns><see langword="true"/> for an integral-to-decimal implicit operator.</returns>
    private static bool IsDecimalWidening(MethodInfo method) =>
        method.DeclaringType == typeof(decimal)
        && method.Name == "op_Implicit"
        && method.ReturnType == typeof(decimal)
        && method.GetParameters() is [{ ParameterType: Type source }]
        && IsLosslessWidening(source, typeof(decimal));

    private static bool TryConvert(object? value, Type from, Type to, out object? result)
    {
        result = null;
        if (to == typeof(object))
        {
            result = value;
            return true;
        }

        Type? toUnderlying = Nullable.GetUnderlyingType(to);
        Type fromCore = Nullable.GetUnderlyingType(from) ?? from;
        Type toCore = toUnderlying ?? to;
        if (!fromCore.IsValueType || !toCore.IsValueType)
        {
            return false;
        }

        if (value is null)
        {
            // A lifted conversion of null is null. Unwrapping a null Nullable<T> throws,
            // which only the compiled expression reproduces.
            return toUnderlying is not null && CanConvert(fromCore, toCore);
        }

        if (!CanConvert(fromCore, toCore))
        {
            return false;
        }

        if (fromCore == toCore)
        {
            result = value;
        }
        else if (toCore.IsEnum)
        {
            result = Enum.ToObject(toCore, value);
        }
        else
        {
            result = Convert.ChangeType(value, toCore, CultureInfo.InvariantCulture);
        }

        return true;
    }

    private static bool CanConvert(Type from, Type to) =>
        from == to
        || (from.IsEnum && Enum.GetUnderlyingType(from) == to)
        || (to.IsEnum && Enum.GetUnderlyingType(to) == from)
        || IsLosslessWidening(from, to);

    /// <summary>
    /// Returns <see langword="true"/> when every value of <paramref name="from"/> converts to
    /// <paramref name="to"/> exactly: the integral widenings, and the conversions to
    /// <see cref="float"/>, <see cref="double"/> and <see cref="decimal"/> that keep every
    /// digit. The implicit conversions that can round (<see cref="int"/> to <see cref="float"/>,
    /// <see cref="long"/> to <see cref="double"/>, and so on) are left to the compiled expression.
    /// </summary>
    /// <param name="from">The operand's type.</param>
    /// <param name="to">The conversion's type.</param>
    /// <returns><see langword="true"/> for a lossless widening.</returns>
    private static bool IsLosslessWidening(Type from, Type to)
    {
        if (from == typeof(sbyte))
        {
            return to == typeof(short) || to == typeof(int) || to == typeof(long) || IsFloatingOrDecimal(to);
        }

        if (from == typeof(byte))
        {
            return to == typeof(short) || to == typeof(ushort) || to == typeof(int) || to == typeof(uint)
                || to == typeof(long) || to == typeof(ulong) || IsFloatingOrDecimal(to);
        }

        if (from == typeof(short))
        {
            return to == typeof(int) || to == typeof(long) || IsFloatingOrDecimal(to);
        }

        if (from == typeof(ushort))
        {
            return to == typeof(int) || to == typeof(uint) || to == typeof(long) || to == typeof(ulong) || IsFloatingOrDecimal(to);
        }

        if (from == typeof(int))
        {
            return to == typeof(long) || to == typeof(double) || to == typeof(decimal);
        }

        if (from == typeof(uint))
        {
            return to == typeof(long) || to == typeof(ulong) || to == typeof(double) || to == typeof(decimal);
        }

        if (from == typeof(long) || from == typeof(ulong))
        {
            return to == typeof(decimal);
        }

        return from == typeof(float) && to == typeof(double);
    }

    private static bool IsFloatingOrDecimal(Type type) =>
        type == typeof(float) || type == typeof(double) || type == typeof(decimal);
}
