namespace JetDatabaseWriter.EntityFrameworkCore.Query.Internal;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;

internal static partial class JetAsyncQueryExecutor
{
    private static readonly ConditionalWeakTable<Type, Func<object, Func<object?, object?>, object>> ResultPromoters = new();

    private static Func<object, Func<object?, object?>, object> CreateResultPromoter(Type type)
    {
        ParameterExpression value = Expression.Parameter(typeof(object), "value");
        ParameterExpression promote = Expression.Parameter(typeof(Func<object?, object?>), "promote");
        Expression typed = Expression.Convert(value, type);
        bool immutable = (type.IsDefined(typeof(CompilerGeneratedAttribute), inherit: false)
                && type.Name.Contains("AnonymousType", StringComparison.Ordinal))
            || (type.IsValueType && type.FullName?.StartsWith("System.ValueTuple`", StringComparison.Ordinal) == true);
        MemberInfo[] members = type.IsValueType
            ? type.GetFields(BindingFlags.Public | BindingFlags.Instance).Cast<MemberInfo>().ToArray()
            : type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(static p => p.GetMethod?.IsPublic == true && p.GetIndexParameters().Length == 0)
                .Cast<MemberInfo>().ToArray();
        ConstructorInfo? constructor = immutable ? type.GetConstructors().SingleOrDefault(c => c.GetParameters().Length == members.Length) : null;
        if (immutable && constructor is null)
        {
            return static (item, _) => item;
        }

        List<ParameterExpression> variables = [];
        List<Expression> expressions = [];
        List<Expression> arguments = [];
        Expression changed = Expression.Constant(false);
        foreach (MemberInfo member in members)
        {
            Expression access = Expression.MakeMemberAccess(typed, member);
            ParameterExpression original = Expression.Variable(typeof(object), "original");
            ParameterExpression replacement = Expression.Variable(typeof(object), "replacement");
            variables.Add(original);
            variables.Add(replacement);
            expressions.Add(Expression.Assign(original, Expression.Convert(access, typeof(object))));
            expressions.Add(Expression.Assign(replacement, Expression.Invoke(promote, original)));
            Expression differs = Expression.Not(Expression.ReferenceEqual(original, replacement));
            changed = Expression.OrElse(changed, differs);
            arguments.Add(Expression.Convert(replacement, access.Type));
            if (!immutable && member is PropertyInfo { SetMethod.IsPublic: true })
            {
                expressions.Add(Expression.IfThen(differs, Expression.Assign(access, Expression.Convert(replacement, access.Type))));
            }
        }

        expressions.Add(immutable
            ? Expression.Condition(changed, Expression.Convert(Expression.New(constructor!, arguments), typeof(object)), value)
            : value);
        return Expression.Lambda<Func<object, Func<object?, object?>, object>>(
            Expression.Block(variables, expressions), value, promote).Compile();
    }
}
