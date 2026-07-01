namespace JetDatabaseWriter;

using System.Linq;

/// <summary>
/// A query that has just declared an eager-loaded navigation via
/// <see cref="AccessQueryExtensions.Include{T, TProperty}"/> (or a subsequent
/// <c>ThenInclude</c>), carrying the most recently included property type so the next
/// <c>ThenInclude</c> can extend the chain into a nested navigation.
/// </summary>
/// <typeparam name="TEntity">The query element type.</typeparam>
/// <typeparam name="TProperty">
/// The type of the navigation included by the immediately preceding <c>Include</c> /
/// <c>ThenInclude</c>. Covariant so a collection navigation (for example
/// <see cref="System.Collections.Generic.List{T}"/>) binds to the collection
/// <c>ThenInclude</c> overload through <see cref="System.Collections.Generic.IEnumerable{T}"/>.
/// </typeparam>
public interface IIncludableQueryable<out TEntity, out TProperty> : IQueryable<TEntity>;
