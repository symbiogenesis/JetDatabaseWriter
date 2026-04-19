namespace JetDatabaseReader;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

/// <summary>
/// Provides a fluent API for querying table data with filtering and limiting.
/// Supports both typed (<c>object[]</c>) and string (<c>string[]</c>) row access.
///
/// Typed chain  (recommended): <see cref="Where"/>      → <see cref="Execute"/>              / <see cref="FirstOrDefault"/>        / <see cref="Count"/>.
/// String chain (compat):      <see cref="WhereAsStrings"/> → <see cref="ExecuteAsStrings"/> / <see cref="FirstOrDefaultAsStrings"/> / <see cref="CountAsStrings"/>.
/// </summary>
public sealed class TableQuery
{
    private readonly AccessReader _reader;
    private readonly string _tableName;
    private int? _limit;
    private Func<object[], bool>? _typedFilter;
    private Func<string[], bool>? _stringFilter;

    internal TableQuery(AccessReader reader, string tableName)
    {
        _reader = reader;
        _tableName = tableName;
    }

    // ── Filter ────────────────────────────────────────────────────────

    /// <summary>
    /// Filters rows using a predicate over properly typed values.
    /// Works with <see cref="Execute"/>, <see cref="FirstOrDefault"/>, and <see cref="Count"/>.
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
    /// Works with <see cref="ExecuteAsStrings"/>, <see cref="FirstOrDefaultAsStrings"/>, and <see cref="CountAsStrings"/>.
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
    /// Executes the query and returns matching rows as properly typed object arrays.
    /// Each element is the native CLR type (int, DateTime, decimal, etc.) or <see cref="DBNull.Value"/> for nulls.
    /// </summary>
    /// <returns></returns>
    public IEnumerable<object[]> Execute()
    {
        IEnumerable<object[]> rows = _reader.StreamRows(_tableName);

        if (_typedFilter != null)
        {
            rows = rows.Where(_typedFilter);
        }

        if (_limit.HasValue)
        {
            rows = rows.Take(_limit.Value);
        }

        return rows;
    }

    /// <summary>
    /// Executes the query and returns the first matching typed row, or null if no matches.
    /// </summary>
    /// <returns></returns>
    public object[] FirstOrDefault()
    {
        return Execute().FirstOrDefault();
    }

    /// <summary>
    /// Executes the query and returns the count of typed rows matching the filter.
    /// </summary>
    /// <returns></returns>
    public int Count()
    {
        return Execute().Count();
    }

    // ── Generic POCO execution ────────────────────────────────────────

    /// <summary>
    /// Executes the query and maps matching rows to instances of <typeparamref name="T"/>.
    /// Property names are matched to column headers (case-insensitive).
    /// </summary>
    /// <typeparam name="T">A class with a parameterless constructor whose public settable properties match column names.</typeparam>
    /// <returns></returns>
    public IEnumerable<T> Execute<T>()
        where T : class, new()
    {
        List<ColumnMetadata> meta = _reader.GetColumnMetadata(_tableName);
        var headers = meta.ConvertAll(m => m.Name);
        PropertyInfo?[] index = RowMapper<T>.BuildIndex(headers);

        foreach (object[] row in Execute())
        {
            yield return RowMapper<T>.Map(row, index);
        }
    }

    /// <summary>
    /// Executes the query and returns the first matching row mapped to <typeparamref name="T"/>, or null if no matches.
    /// </summary>
    /// <typeparam name="T">A class with a parameterless constructor whose public settable properties match column names.</typeparam>
    /// <returns></returns>
    public T? FirstOrDefault<T>()
        where T : class, new()
    {
        return Execute<T>().FirstOrDefault();
    }

    // ── String execution ──────────────────────────────────────────────

    /// <summary>
    /// Executes the query and returns matching rows as string arrays.
    /// Use this for compatibility scenarios or when you need raw string data.
    /// For most use cases, prefer <see cref="Execute"/> which returns properly typed data.
    /// </summary>
    /// <returns></returns>
    public IEnumerable<string[]> ExecuteAsStrings()
    {
        IEnumerable<string[]> rows = _reader.StreamRowsAsStrings(_tableName);

        if (_stringFilter != null)
        {
            rows = rows.Where(_stringFilter);
        }

        if (_limit.HasValue)
        {
            rows = rows.Take(_limit.Value);
        }

        return rows;
    }

    /// <summary>
    /// Executes the query and returns the first matching string row, or null if no matches.
    /// </summary>
    /// <returns></returns>
    public string[] FirstOrDefaultAsStrings()
    {
        return ExecuteAsStrings().FirstOrDefault();
    }

    /// <summary>
    /// Executes the query and returns the count of string rows matching the filter.
    /// </summary>
    /// <returns></returns>
    public int CountAsStrings()
    {
        return ExecuteAsStrings().Count();
    }
}
