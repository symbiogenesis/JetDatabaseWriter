namespace JetDatabaseWriter.Queries;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using JetDatabaseWriter.Linq;

/// <summary>
/// Builds the <see cref="NotSupportedException"/> that synchronous use of a
/// <c>Query&lt;T&gt;</c> result throws. Query results are async-only: the read stack is
/// asynchronous end to end, so running it from <c>GetEnumerator</c> or
/// <c>IQueryProvider.Execute</c> would block a thread until it finished. Every message names
/// <c>ToListAsync</c>, and a synchronous terminal's message also names its async counterpart in
/// <see cref="AccessQueryExtensions"/> when there is one.
/// </summary>
internal static class AsyncOnlyQuery
{
    private static readonly HashSet<string> AsyncTerminalNames = new(
        typeof(AccessQueryExtensions).GetMethods(BindingFlags.Public | BindingFlags.Static).Select(m => m.Name),
        StringComparer.Ordinal);

    /// <summary>Returns the exception that enumerating a query synchronously throws.</summary>
    /// <returns>The exception to throw.</returns>
    public static NotSupportedException EnumerationNotSupported() =>
        new("Query<T> results are async-only and cannot be enumerated synchronously. "
            + "Use ToListAsync, another async terminal in JetDatabaseWriter.Linq, or await foreach over AsAsyncEnumerable().");

    /// <summary>
    /// Returns the exception that executing <paramref name="expression"/> synchronously throws:
    /// the enumeration message for a sequence, or a message naming the synchronous terminal
    /// (<c>Count</c>, <c>First</c>, <c>Sum</c>, ...) that <see cref="Queryable"/> passed in.
    /// </summary>
    /// <param name="expression">The expression handed to <c>IQueryProvider.Execute</c>.</param>
    /// <returns>The exception to throw.</returns>
    public static NotSupportedException ExecutionNotSupported(Expression expression)
    {
        if (expression is not MethodCallExpression call || typeof(IQueryable).IsAssignableFrom(expression.Type))
        {
            return EnumerationNotSupported();
        }

        string name = call.Method.Name;
        string asyncName = name + "Async";
        string remedy = AsyncTerminalNames.Contains(asyncName)
            ? $"Use {asyncName}, or materialize the rows with ToListAsync and apply '{name}' with LINQ to Objects."
            : $"Materialize the rows with ToListAsync and apply '{name}' with LINQ to Objects.";
        return new NotSupportedException($"Query<T> results are async-only, so the synchronous operator '{name}' cannot run. {remedy}");
    }
}
