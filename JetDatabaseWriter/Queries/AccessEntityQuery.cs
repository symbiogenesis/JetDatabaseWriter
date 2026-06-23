namespace JetDatabaseWriter.Queries;

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Interfaces;

/// <summary>
/// Immutable, composable entity query returned by <see cref="AccessReader.Query{T}(string)"/>.
/// Filtering reuses the reader's index inference; <see cref="Include{TProperty}"/> resolves
/// the navigation's relationship from the catalog and eagerly loads it on materialization.
/// </summary>
/// <typeparam name="T">The entity type mapped from the table's rows.</typeparam>
internal sealed class AccessEntityQuery<T> : IAccessEntityQuery<T>
    where T : class, new()
{
    private readonly AccessReader reader;
    private readonly string tableName;
    private readonly Expression<Func<T, bool>>? predicate;
    private readonly IReadOnlyList<PropertyInfo> includes;

    public AccessEntityQuery(AccessReader reader, string tableName)
        : this(reader, tableName, predicate: null, [])
    {
    }

    private AccessEntityQuery(
        AccessReader reader,
        string tableName,
        Expression<Func<T, bool>>? predicate,
        IReadOnlyList<PropertyInfo> includes)
    {
        this.reader = reader;
        this.tableName = tableName;
        this.predicate = predicate;
        this.includes = includes;
    }

    /// <inheritdoc/>
    public IAccessEntityQuery<T> Where(Expression<Func<T, bool>> predicate)
    {
        Guard.NotNull(predicate, nameof(predicate));
        Expression<Func<T, bool>> combined = this.predicate is null ? predicate : Combine(this.predicate, predicate);
        return new AccessEntityQuery<T>(this.reader, this.tableName, combined, this.includes);
    }

    /// <inheritdoc/>
    public IAccessEntityQuery<T> Include<TProperty>(Expression<Func<T, TProperty>> navigation)
    {
        Guard.NotNull(navigation, nameof(navigation));
        PropertyInfo property = ResolveProperty(navigation);
        var next = new List<PropertyInfo>(this.includes) { property };
        return new AccessEntityQuery<T>(this.reader, this.tableName, this.predicate, next);
    }

    /// <inheritdoc/>
    public async ValueTask<List<T>> ToListAsync(CancellationToken cancellationToken = default)
    {
        List<T> roots = await this.LoadRootsAsync(cancellationToken).ConfigureAwait(false);
        if (this.includes.Count > 0)
        {
            await IncludeLoader.ApplyAsync(this.reader, this.tableName, roots, this.includes, cancellationToken).ConfigureAwait(false);
        }

        return roots;
    }

    /// <inheritdoc/>
    public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        // With includes, results must be materialized so the related rows can be
        // batch-loaded and stitched before anything is yielded. Without includes the
        // query streams straight from the (index-inferred) row reader.
        if (this.includes.Count > 0)
        {
            foreach (T item in await this.ToListAsync(cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }

            yield break;
        }

        await foreach (T item in this.EnumerateRootsAsync(cancellationToken).ConfigureAwait(false))
        {
            yield return item;
        }
    }

    private static PropertyInfo ResolveProperty<TProperty>(Expression<Func<T, TProperty>> navigation)
    {
        Expression body = navigation.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary)
        {
            body = unary.Operand;
        }

        if (body is MemberExpression { Member: PropertyInfo property })
        {
            return property;
        }

        throw new ArgumentException(
            "An Include navigation must be a property access, for example o => o.Customer or c => c.Orders.",
            nameof(navigation));
    }

    private static Expression<Func<T, bool>> Combine(Expression<Func<T, bool>> first, Expression<Func<T, bool>> second)
    {
        ParameterExpression parameter = first.Parameters[0];
        Expression rebound = new ReplaceParameterVisitor(second.Parameters[0], parameter).Visit(second.Body);
        return Expression.Lambda<Func<T, bool>>(Expression.AndAlso(first.Body, rebound), parameter);
    }

    private async ValueTask<List<T>> LoadRootsAsync(CancellationToken cancellationToken)
    {
        var roots = new List<T>();
        await foreach (T item in this.EnumerateRootsAsync(cancellationToken).ConfigureAwait(false))
        {
            roots.Add(item);
        }

        return roots;
    }

    private async IAsyncEnumerable<T> EnumerateRootsAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        IAsyncEnumerable<T> source = this.predicate is null
            ? this.reader.Rows<T>(this.tableName, progress: null, cancellationToken)
            : this.reader.Rows<T>(this.tableName, this.predicate, progress: null, cancellationToken);

        await foreach (T item in source.ConfigureAwait(false))
        {
            yield return item;
        }
    }

    private sealed class ReplaceParameterVisitor(ParameterExpression from, ParameterExpression to) : ExpressionVisitor
    {
        protected override Expression VisitParameter(ParameterExpression node) =>
            node == from ? to : base.VisitParameter(node);
    }
}
