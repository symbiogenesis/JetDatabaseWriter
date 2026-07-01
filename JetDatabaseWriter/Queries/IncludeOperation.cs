namespace JetDatabaseWriter.Queries;

using System;
using System.Collections.Generic;
using System.Linq.Expressions;

/// <summary>
/// One operator applied to a filtered / ordered collection include, EF-style
/// (<c>Include(o =&gt; o.Items.Where(...).OrderBy(...).Take(n))</c>). Each parent's loaded
/// children run through the operations in written order before the navigation is set, so a
/// <c>Take</c> bounds the children per parent and a following <c>ThenInclude</c> descends
/// only into the kept rows. The operations run in memory and may only reference the child
/// being filtered.
/// </summary>
internal abstract class IncludeOperation
{
    /// <summary>Applies this operation to a parent's already-loaded child sequence.</summary>
    /// <param name="source">The child entities loaded for one parent.</param>
    /// <returns>The transformed sequence.</returns>
    public abstract IEnumerable<object> Apply(IEnumerable<object> source);

    /// <summary>
    /// Compiles a child predicate (<c>i =&gt; i.Flag</c>) into a delegate over the boxed
    /// child entity by rebinding the lambda parameter through a cast from <see cref="object"/>.
    /// </summary>
    /// <param name="predicate">The single-parameter predicate lambda.</param>
    /// <returns>The compiled predicate over a boxed child entity.</returns>
    protected static Func<object, bool> CompilePredicate(LambdaExpression predicate)
    {
        ParameterExpression element = Expression.Parameter(typeof(object), "e");
        Expression body = RebindToObject(predicate, element);
        return Expression.Lambda<Func<object, bool>>(body, element).Compile();
    }

    /// <summary>
    /// Compiles a child key selector (<c>i =&gt; i.Name</c>) into a delegate that returns the
    /// boxed key from the boxed child entity, for use with <see cref="QueryKeyComparer"/>.
    /// </summary>
    /// <param name="selector">The single-parameter key-selector lambda.</param>
    /// <returns>The compiled key selector over a boxed child entity.</returns>
    protected static Func<object, object?> CompileSelector(LambdaExpression selector)
    {
        ParameterExpression element = Expression.Parameter(typeof(object), "e");
        Expression body = RebindToObject(selector, element);
        return Expression.Lambda<Func<object, object?>>(Expression.Convert(body, typeof(object)), element).Compile();
    }

    private static Expression RebindToObject(LambdaExpression lambda, ParameterExpression element)
    {
        Expression typed = Expression.Convert(element, lambda.Parameters[0].Type);
        return new ReplaceParameterVisitor(lambda.Parameters[0], typed).Visit(lambda.Body);
    }

    private sealed class ReplaceParameterVisitor(ParameterExpression from, Expression to) : ExpressionVisitor
    {
        protected override Expression VisitParameter(ParameterExpression node) =>
            node == from ? to : base.VisitParameter(node);
    }
}
