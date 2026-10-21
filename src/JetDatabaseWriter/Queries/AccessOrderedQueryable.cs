namespace JetDatabaseWriter.Queries;

using System.Linq;
using System.Linq.Expressions;

/// <summary>
/// The <see cref="IOrderedQueryable{T}"/> form of <see cref="AccessQueryable{T}"/>, which
/// the provider produces only for the result of a LINQ ordering operator (<c>OrderBy</c> /
/// <c>OrderByDescending</c> / <c>ThenBy</c> / <c>ThenByDescending</c>). Restricting the
/// <see cref="IOrderedQueryable{T}"/> marker to ordered results is what keeps
/// <c>ThenBy</c> / <c>ThenByDescending</c> callable only after an ordering operator,
/// matching LINQ semantics. All behavior is inherited from <see cref="AccessQueryable{T}"/>;
/// this type only adds the ordered marker interface.
/// </summary>
/// <typeparam name="T">The element type.</typeparam>
/// <param name="provider">The provider that runs the composed expression.</param>
/// <param name="expression">The ordering expression that produced this query.</param>
internal sealed class AccessOrderedQueryable<T>(IQueryProvider provider, Expression expression)
    : AccessQueryable<T>(provider, expression), IOrderedQueryable<T>;
