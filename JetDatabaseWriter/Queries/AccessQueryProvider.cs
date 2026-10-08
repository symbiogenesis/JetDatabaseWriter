namespace JetDatabaseWriter.Queries;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tables;

/// <summary>
/// <see cref="IQueryProvider"/> for <see cref="AccessQueryable{T}"/>. Translates the
/// supported LINQ operators into an ordered <see cref="QueryStage"/> pipeline and runs
/// the stages in written order: a leading run of filters is pushed into the index
/// reader's predicate inference, later stages (filter / order / page) run over the stream, and
/// includes eager-load inferred relationships onto the final set. The operators above that
/// pipeline (a <c>Select</c> projection and what follows it) run through
/// <see cref="InMemoryTail"/> over the rows as they stream. Execution is
/// async-only: the synchronous <see cref="IQueryProvider"/> entry points throw (see
/// <see cref="AsyncOnlyQuery"/>), so nothing blocks a thread on the async read stack. The
/// provider is generic on the entity type so it can map rows; <see cref="AccessQueryable{T}"/>
/// reaches it through <see cref="IAccessQueryEngine"/>.
/// </summary>
/// <typeparam name="T">The entity type mapped from the table's rows.</typeparam>
/// <param name="tables">Streams and counts the table's rows.</param>
/// <param name="indexes">Serves pushed-down filters and index-ordered reads.</param>
/// <param name="schema">Supplies the relationship and column metadata that includes need.</param>
/// <param name="table">The table being queried.</param>
[RequiresUnreferencedCode("LINQ queries and Include discover entity types and members at runtime. Use typed row readers for trimmed applications.")]
[RequiresDynamicCode("LINQ queries and Include construct generic types at runtime. Use typed row readers for NativeAOT applications.")]
internal sealed class AccessQueryProvider<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(TableReader tables, IndexRowReader indexes, SchemaReader schema, string table) : IQueryProvider, IAccessQueryEngine
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
        throw AsyncOnlyQuery.ExecutionNotSupported(expression);
    }

    public TResult Execute<TResult>(Expression expression)
    {
        // The synchronous Queryable terminals (Count, First, Sum, ...) land here; query
        // results are async-only, so each one is refused before anything is read.
        Guard.NotNull(expression, nameof(expression));
        throw AsyncOnlyQuery.ExecutionNotSupported(expression);
    }

    public IAsyncEnumerable<object?> ExecuteStreamAsync(Expression expression, CancellationToken cancellationToken)
    {
        Guard.NotNull(expression, nameof(expression));
        (AccessQueryPlan plan, Expression boundary) = AccessQueryTranslator.Translate(expression);

        // A tail (a projection, or operators the engine does not translate) runs over the
        // engine's rows as they stream. It is composed here, before a row is read, so an
        // operator it does not run throws first, including one in a second query source.
        // Enumeration remains deferred; streaming stages let Take/First stop the read.
        IAsyncEnumerable<object?> rows = this.ExecuteEngineAsync(plan, cancellationToken);
        if (!ReferenceEquals(boundary, expression))
        {
            rows = InMemoryTail.Apply(rows, expression, boundary, cancellationToken);
        }

        return ReadPreparedAsync(rows, cancellationToken);
    }

    public async ValueTask<long> CountAsync(Expression expression, CancellationToken cancellationToken)
    {
        Guard.NotNull(expression, nameof(expression));
        (AccessQueryPlan plan, Expression boundary) = AccessQueryTranslator.Translate(expression);

        // Fast path: counting the whole table (no stages, no includes, no in-memory tail)
        // calls GetRealRowCountAsync, which counts the live rows whose layout decodes (the
        // rows a scan yields) without decoding cell values or building POCOs. The declared
        // TDEF row count is not used because it can be stale (see TableStat.RowCount).
        if (ReferenceEquals(boundary, expression) && plan.Stages.Count == 0 && plan.IncludePaths.Count == 0)
        {
            return await tables.GetRealRowCountAsync(table, cancellationToken).ConfigureAwait(false);
        }

        // Every other shape (a filter, paging, a projection, or includes) streams the rows
        // and counts them without buffering a list.
        long count = 0;
        await foreach (object? unused in this.ExecuteStreamAsync(expression, cancellationToken).ConfigureAwait(false))
        {
            count++;
        }

        return count;
    }

    private static async IAsyncEnumerable<object?> ReadPreparedAsync(
        IAsyncEnumerable<object?> rows,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // A buffered stage or a synthetic element need not advance the table reader,
        // so enforce cancellation even after the read itself has finished.
        cancellationToken.ThrowIfCancellationRequested();
        await foreach (object? item in rows.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return item;
        }
    }

    /// <summary>
    /// Determines whether <paramref name="expression"/>'s outermost node is a LINQ
    /// ordering operator (<c>OrderBy</c> / <c>OrderByDescending</c> / <c>ThenBy</c> /
    /// <c>ThenByDescending</c> / <c>Order</c> / <c>OrderDescending</c>). Only those results
    /// are surfaced as <see cref="IOrderedQueryable{T}"/> (via
    /// <see cref="AccessOrderedQueryable{T}"/>), which <see cref="Queryable"/> casts each of
    /// them to, so <c>ThenBy</c> / <c>ThenByDescending</c> stay callable only after an
    /// ordering operator, matching LINQ semantics.
    /// </summary>
    /// <param name="expression">The composed query expression.</param>
    /// <returns><see langword="true"/> when the outermost operator establishes an ordering.</returns>
    private static bool IsOrderingOperator(Expression expression) =>
        expression is MethodCallExpression call
        && call.Method.DeclaringType == typeof(Queryable)
        && call.Method.Name is "OrderBy" or "OrderByDescending" or "ThenBy" or "ThenByDescending" or "Order" or "OrderDescending";

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

        await IncludeLoader.ApplyAsync(tables, indexes, schema, table, result, plan.IncludePaths, cancellationToken).ConfigureAwait(false);
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
                ? tables.Rows<T>(table, progress: null, cancellationToken)
                : indexes.Rows(table, pushed, progress: null, cancellationToken);
        }

        for (; next < stages.Count; next++)
        {
            sequence = stages[next].Apply(sequence, cancellationToken);
        }

        return sequence;
    }

    private async ValueTask<IAsyncEnumerable<T>?> TryBuildOrderedSourceAsync(OrderStage order, CancellationToken cancellationToken)
    {
        // Index seeks use the database format layout.
        if (!indexes.CanSeek)
        {
            return null;
        }

        IReadOnlyList<IndexMetadata> tableIndexes = await indexes.ListIndexesAsync(table, cancellationToken).ConfigureAwait(false);
        return order.FindCoveringIndex(tableIndexes) is { } index
            ? indexes.ReadIndexRowsAsync<T>(table, index.Name, IndexQueryCriteria.All, cancellationToken)
            : null;
    }
}
