namespace JetDatabaseWriter;

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Provides a fluent API for querying table data with filtering and limiting.
/// Supports both typed (<c>object[]</c>) and string (<c>string[]</c>) row access.
///
/// Typed chain  (recommended): <see cref="Where"/>      → <see cref="ExecuteAsync"/>              / <see cref="FirstOrDefaultAsync"/>        / <see cref="CountAsync"/>.
/// String chain (compat):      <see cref="WhereAsStrings"/> → <see cref="ExecuteAsStringsAsync"/> / <see cref="FirstOrDefaultAsStringsAsync"/> / <see cref="CountAsStringsAsync"/>.
/// </summary>
public sealed class TableQuery
{
    private readonly IAccessReader _reader;
    private readonly string _tableName;
    private int? _limit;
    private Func<object[], bool>? _typedFilter;
    private Func<string[], bool>? _stringFilter;

    internal TableQuery(IAccessReader reader, string tableName)
    {
        _reader = reader;
        _tableName = tableName;
    }

    // ── Filter ────────────────────────────────────────────────────────

    /// <summary>
    /// Filters rows using a predicate over properly typed values.
    /// Works with <see cref="ExecuteAsync"/>, <see cref="FirstOrDefaultAsync"/>, and <see cref="CountAsync"/>.
    /// </summary>
    /// <returns></returns>
    public TableQuery Where(Func<object[], bool> predicate)
    {
        Guard.NotNull(predicate, nameof(predicate));
        _typedFilter = predicate;
        return this;
    }

    /// <summary>
    /// Filters rows using a predicate over string values.
    /// Works with <see cref="ExecuteAsStringsAsync"/>, <see cref="FirstOrDefaultAsStringsAsync"/>, and <see cref="CountAsStringsAsync"/>.
    /// </summary>
    /// <returns></returns>
    public TableQuery WhereAsStrings(Func<string[], bool> predicate)
    {
        Guard.NotNull(predicate, nameof(predicate));
        _stringFilter = predicate;
        return this;
    }

    // ── Limit ─────────────────────────────────────────────────────────

    /// <summary>
    /// Limits the number of rows returned by any Execute method.
    /// </summary>
    /// <returns></returns>
    public TableQuery Take(int count)
    {
        Guard.Positive(count, nameof(count));
        _limit = count;
        return this;
    }

    // ── Typed execution ───────────────────────────────────────────────

    /// <summary>
    /// Executes the query asynchronously and returns matching rows as properly typed object arrays.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel asynchronous enumeration.</param>
    /// <returns></returns>
    public async IAsyncEnumerable<object[]> ExecuteAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        int yielded = 0;
        await foreach (object[] row in _reader.StreamRowsAsync(_tableName, cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            if (_typedFilter != null && !_typedFilter(row))
            {
                continue;
            }

            yield return row;
            yielded++;

            if (_limit.HasValue && yielded >= _limit.Value)
            {
                yield break;
            }
        }
    }

    /// <summary>
    /// Executes the query asynchronously and returns the first matching typed row, or null if no matches.
    /// </summary>
    /// <returns></returns>
    public async ValueTask<object[]?> FirstOrDefaultAsync(CancellationToken cancellationToken = default)
    {
        await foreach (object[] row in ExecuteAsync(cancellationToken).ConfigureAwait(false))
        {
            return row;
        }

        return null;
    }

    /// <summary>
    /// Executes the query asynchronously and returns the count of typed rows matching the filter.
    /// </summary>
    /// <returns></returns>
    public async ValueTask<int> CountAsync(CancellationToken cancellationToken = default)
    {
        int count = 0;
        await foreach (object[] row in ExecuteAsync(cancellationToken).ConfigureAwait(false))
        {
            count++;
        }

        return count;
    }

    // ── Generic POCO execution ────────────────────────────────────────

    /// <summary>
    /// Executes the query asynchronously and maps matching rows to instances of <typeparamref name="T"/>.
    /// Property names are matched to column headers (case-insensitive).
    /// </summary>
    /// <typeparam name="T">A class with a parameterless constructor whose public settable properties match column names.</typeparam>
    /// <returns></returns>
    public async IAsyncEnumerable<T> ExecuteAsync<T>([EnumeratorCancellation] CancellationToken cancellationToken = default)
        where T : class, new()
    {
        List<ColumnMetadata> meta = await _reader.GetColumnMetadataAsync(_tableName, cancellationToken).ConfigureAwait(false);
        var headers = meta.ConvertAll(m => m.Name);
        RowMapper<T>.Accessor?[] index = RowMapper<T>.BuildIndex(headers);

        await foreach (object[] row in ExecuteAsync(cancellationToken).ConfigureAwait(false))
        {
            yield return RowMapper<T>.Map(row, index);
        }
    }

    /// <summary>
    /// Executes the query asynchronously and returns the first matching row mapped to <typeparamref name="T"/>,
    /// or null if no matches.
    /// </summary>
    /// <typeparam name="T">A class with a parameterless constructor whose public settable properties match column names.</typeparam>
    /// <returns></returns>
    public async ValueTask<T?> FirstOrDefaultAsync<T>(CancellationToken cancellationToken = default)
        where T : class, new()
    {
        await foreach (T row in ExecuteAsync<T>(cancellationToken).ConfigureAwait(false))
        {
            return row;
        }

        return null;
    }

    // ── String execution ──────────────────────────────────────────────

    /// <summary>
    /// Executes the query asynchronously and returns matching rows as string arrays.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel asynchronous enumeration.</param>
    /// <returns></returns>
    public async IAsyncEnumerable<string[]> ExecuteAsStringsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        int yielded = 0;
        await foreach (string[] row in _reader.StreamRowsAsStringsAsync(_tableName, cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            if (_stringFilter != null && !_stringFilter(row))
            {
                continue;
            }

            yield return row;
            yielded++;

            if (_limit.HasValue && yielded >= _limit.Value)
            {
                yield break;
            }
        }
    }

    /// <summary>
    /// Executes the query asynchronously and returns the first matching string row, or null if no matches.
    /// </summary>
    /// <returns></returns>
    public async ValueTask<string[]?> FirstOrDefaultAsStringsAsync(CancellationToken cancellationToken = default)
    {
        await foreach (string[] row in ExecuteAsStringsAsync(cancellationToken).ConfigureAwait(false))
        {
            return row;
        }

        return null;
    }

    /// <summary>
    /// Executes the query asynchronously and returns the count of string rows matching the filter.
    /// </summary>
    /// <returns></returns>
    public async ValueTask<int> CountAsStringsAsync(CancellationToken cancellationToken = default)
    {
        int count = 0;
        await foreach (string[] row in ExecuteAsStringsAsync(cancellationToken).ConfigureAwait(false))
        {
            count++;
        }

        return count;
    }
}
