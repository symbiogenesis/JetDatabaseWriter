namespace JetDatabaseWriter.Queries;

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;

/// <summary>
/// <see cref="IQueryProvider"/> for <see cref="AccessQueryable{T}"/>. Translates the
/// supported LINQ operators into an ordered <see cref="QueryStage"/> pipeline and runs
/// the stages in written order: a leading run of filters is pushed into the reader's
/// index inference, later stages (filter / order / page) run over the stream, and
/// includes eager-load inferred relationships onto the final set. The provider is
/// generic on the entity type so it can map rows; <see cref="AccessQueryable{T}"/>
/// reaches it through <see cref="IAccessQueryEngine"/>.
/// </summary>
/// <typeparam name="T">The entity type mapped from the table's rows.</typeparam>
/// <param name="reader">The reader the query executes against.</param>
/// <param name="table">The table being queried.</param>
internal sealed class AccessQueryProvider<T>(AccessReader reader, string table) : IQueryProvider, IAccessQueryEngine
    where T : class, new()
{
    public IQueryable CreateQuery(Expression expression) =>
        throw new NotSupportedException("Untyped CreateQuery is not supported; use the generic LINQ query operators.");

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression) =>
        IsOrderingOperator(expression)
            ? new AccessOrderedQueryable<TElement>(this, expression)
            : new AccessQueryable<TElement>(this, expression);

    public object? Execute(Expression expression)
    {
        Guard.NotNull(expression, nameof(expression));
        (AccessQueryPlan plan, Expression boundary) = AccessQueryTranslator.Translate(expression);
        List<T> rows = this.MaterializeSync(plan);
        if (ReferenceEquals(boundary, expression))
        {
            return rows;
        }

        (IQueryProvider provider, Expression rewritten) = BuildTail(rows, expression, boundary);
        return provider.Execute(rewritten);
    }

    public TResult Execute<TResult>(Expression expression)
    {
        Guard.NotNull(expression, nameof(expression));

        // The engine evaluates the supported prefix (so leading filters still infer
        // indexes); any tail — a scalar terminal such as Count/First/Sum, a Select
        // projection, or operators after one — replays over the materialized rows with
        // LINQ-to-Objects for faithful LINQ semantics.
        (AccessQueryPlan plan, Expression boundary) = AccessQueryTranslator.Translate(expression);
        List<T> rows = this.MaterializeSync(plan);
        if (!ReferenceEquals(boundary, expression))
        {
            (IQueryProvider provider, Expression rewritten) = BuildTail(rows, expression, boundary);
            return provider.Execute<TResult>(rewritten);
        }

        if (rows is TResult typed)
        {
            return typed;
        }

        throw new NotSupportedException(
            $"This query yields a sequence of '{typeof(T).Name}'; materialize it with ToList()/ToListAsync() or reduce it with a scalar operator such as Count(), Any(), or First().");
    }

    public IEnumerable ExecuteSyncList(Expression expression)
    {
        Guard.NotNull(expression, nameof(expression));
        (AccessQueryPlan plan, Expression boundary) = AccessQueryTranslator.Translate(expression);
        List<T> rows = this.MaterializeSync(plan);
        if (ReferenceEquals(boundary, expression))
        {
            return rows;
        }

        (IQueryProvider provider, Expression rewritten) = BuildTail(rows, expression, boundary);
        return provider.CreateQuery(rewritten);
    }

    public async IAsyncEnumerable<object> ExecuteStreamAsync(Expression expression, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        Guard.NotNull(expression, nameof(expression));
        (AccessQueryPlan plan, Expression boundary) = AccessQueryTranslator.Translate(expression);

        // No tail: stream the engine pipeline straight through so Take/First can
        // short-circuit before the whole table is read.
        if (ReferenceEquals(boundary, expression))
        {
            await foreach (T item in this.ExecuteEngineAsync(plan, cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }

            yield break;
        }

        // A tail (projection or post-projection operators) replays in memory, so the
        // engine prefix is materialized first.
        List<T> rows = await this.MaterializeAsync(plan, cancellationToken).ConfigureAwait(false);
        (IQueryProvider provider, Expression rewritten) = BuildTail(rows, expression, boundary);
        foreach (object item in provider.CreateQuery(rewritten))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return item;
        }
    }

    public async ValueTask<long> CountAsync(Expression expression, CancellationToken cancellationToken)
    {
        Guard.NotNull(expression, nameof(expression));
        (AccessQueryPlan plan, Expression boundary) = AccessQueryTranslator.Translate(expression);

        // Fast path: counting the whole table — no stages, no includes, no in-memory tail —
        // tallies the live row slots without decoding rows or building POCOs. The declared
        // TDEF row count is deliberately not used: it is not decremented on delete, so it
        // overcounts; GetRealRowCountAsync scans the row-offset slots and is exact.
        if (ReferenceEquals(boundary, expression) && plan.Stages.Count == 0 && plan.IncludePaths.Count == 0)
        {
            return await reader.GetRealRowCountAsync(table, cancellationToken).ConfigureAwait(false);
        }

        // Every other shape (a filter, paging, a projection, or includes) streams the rows
        // and counts them without buffering a list.
        long count = 0;
        await foreach (object unused in this.ExecuteStreamAsync(expression, cancellationToken).ConfigureAwait(false))
        {
            count++;
        }

        return count;
    }

    /// <summary>
    /// Determines whether <paramref name="expression"/>'s outermost node is a LINQ
    /// ordering operator (<c>OrderBy</c> / <c>OrderByDescending</c> / <c>ThenBy</c> /
    /// <c>ThenByDescending</c>). Only those results are surfaced as
    /// <see cref="IOrderedQueryable{T}"/> (via <see cref="AccessOrderedQueryable{T}"/>), so
    /// <c>ThenBy</c> / <c>ThenByDescending</c> stay callable only after an ordering
    /// operator, matching LINQ semantics.
    /// </summary>
    /// <param name="expression">The composed query expression.</param>
    /// <returns><see langword="true"/> when the outermost operator establishes an ordering.</returns>
    private static bool IsOrderingOperator(Expression expression) =>
        expression is MethodCallExpression call
        && call.Method.DeclaringType == typeof(Queryable)
        && call.Method.Name is "OrderBy" or "OrderByDescending" or "ThenBy" or "ThenByDescending";

    private static (IQueryProvider Provider, Expression Rewritten) BuildTail(List<T> rows, Expression root, Expression boundary)
    {
        // Replay the tail over the materialized engine rows: rebind the engine boundary to
        // an in-memory queryable and hand the rewritten tree to LINQ-to-Objects, which
        // turns the Queryable operators into their Enumerable equivalents.
        IQueryable<T> materialized = rows.AsQueryable();
        Expression rewritten = new RebindSourceVisitor(boundary, materialized.Expression).Visit(root);
        return (materialized.Provider, rewritten);
    }

    /// <summary>
    /// The single sync bridge behind <see cref="IQueryable{T}"/>'s <see cref="IEnumerable{T}"/>
    /// surface; <c>foreach</c> and the sync LINQ terminals reach it through <c>Execute</c>,
    /// <c>Execute&lt;TResult&gt;</c>, and <c>ExecuteSyncList</c>.
    /// </summary>
    /// <remarks>
    /// The async stack already uses <c>ConfigureAwait(false)</c> end to end, but rather than rely on
    /// that discipline holding across the whole transitive read stack, the work runs through
    /// <c>Task.Run</c>: a thread-pool thread carries no <see cref="SynchronizationContext"/>, so no
    /// continuation can post back to a UI/classic-ASP.NET context the caller is blocking on. That
    /// removes the sync-over-async deadlock class at the bridge instead of merely documenting it. It
    /// still blocks one pool thread, so async callers should prefer the async terminals
    /// (<c>ToListAsync</c> / <c>await foreach</c>) on hot paths.
    /// </remarks>
    /// <param name="plan">The translated query plan to materialize.</param>
    /// <returns>The fully materialized rows.</returns>
    private List<T> MaterializeSync(AccessQueryPlan plan) =>
        Task.Run(() => this.MaterializeAsync(plan, CancellationToken.None).AsTask()).GetAwaiter().GetResult();

    private async ValueTask<List<T>> MaterializeAsync(AccessQueryPlan plan, CancellationToken cancellationToken)
    {
        var list = new List<T>();
        await foreach (T item in this.ExecuteEngineAsync(plan, cancellationToken).ConfigureAwait(false))
        {
            list.Add(item);
        }

        return list;
    }

    private async IAsyncEnumerable<T> ExecuteEngineAsync(AccessQueryPlan plan, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        IAsyncEnumerable<T> sequence = await this.BuildPipelineAsync(plan, cancellationToken).ConfigureAwait(false);

        // Without includes the pipeline streams straight through, so Take/First can
        // short-circuit before the whole table is read.
        if (plan.IncludePaths.Count == 0)
        {
            await foreach (T item in sequence.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }

            yield break;
        }

        // Eager loads stitch onto the final set, so materialize the pipeline first.
        var result = new List<T>();
        await foreach (T item in sequence.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            result.Add(item);
        }

        await IncludeLoader.ApplyAsync(reader, table, result, plan.IncludePaths, cancellationToken).ConfigureAwait(false);
        foreach (T item in result)
        {
            yield return item;
        }
    }

    private async ValueTask<IAsyncEnumerable<T>> BuildPipelineAsync(AccessQueryPlan plan, CancellationToken cancellationToken)
    {
        List<QueryStage> stages = plan.Stages;

        // Push the leading run of consecutive filters into the reader so its index
        // inference can seek rather than scan. Filters are mutually commutative, so
        // collapsing only the leading run preserves LINQ ordering semantics; every later
        // stage — including a filter that follows ordering or paging — runs in order.
        int next = 0;
        var leading = new List<FilterStage>();
        while (next < stages.Count && stages[next] is FilterStage filter)
        {
            leading.Add(filter);
            next++;
        }

        Expression<Func<T, bool>>? pushed = FilterStage.Combine<T>(leading);

        // When no leading filter consumed the stream and the next stage orders by a covering
        // unique integer-keyed index, read the source straight from that index in key order
        // and skip the in-memory sort. A unique index has no key ties, so its order is
        // identical to the stable LINQ sort, and a following Take/Skip then bounds how many
        // rows are materialized instead of buffering and sorting the whole table.
        IAsyncEnumerable<T>? ordered = pushed is null && next < stages.Count && stages[next] is OrderStage order
            ? await this.TryBuildOrderedSourceAsync(order, cancellationToken).ConfigureAwait(false)
            : null;

        IAsyncEnumerable<T> sequence;
        if (ordered is not null)
        {
            sequence = ordered;
            next++;
        }
        else
        {
            sequence = pushed is null
                ? reader.Rows<T>(table, progress: null, cancellationToken)
                : reader.Rows(table, pushed, progress: null, cancellationToken);
        }

        for (; next < stages.Count; next++)
        {
            sequence = stages[next].Apply(sequence, cancellationToken);
        }

        return sequence;
    }

    private async ValueTask<IAsyncEnumerable<T>?> TryBuildOrderedSourceAsync(OrderStage order, CancellationToken cancellationToken)
    {
        // Index seeks are Jet4/ACE-only.
        if (reader.Format == DatabaseFormat.Jet3Mdb)
        {
            return null;
        }

        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(table, cancellationToken).ConfigureAwait(false);
        return order.FindCoveringIndex(indexes) is { } index
            ? reader.ReadIndexRowsAsync<T>(table, index.Name, IndexQueryCriteria.All, cancellationToken)
            : null;
    }

    private sealed class RebindSourceVisitor(Expression target, Expression replacement) : ExpressionVisitor
    {
        [return: NotNullIfNotNull(nameof(node))]
        public override Expression? Visit(Expression? node) =>
            ReferenceEquals(node, target) ? replacement : base.Visit(node);
    }
}
