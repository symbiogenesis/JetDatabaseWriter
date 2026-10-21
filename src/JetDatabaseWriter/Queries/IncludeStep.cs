namespace JetDatabaseWriter.Queries;

using System.Collections.Generic;
using System.Reflection;

/// <summary>
/// One navigation in an <c>Include</c> / <c>ThenInclude</c> path: the navigation property to
/// load plus any inline collection operators applied to it (the filtered / ordered include
/// form <c>Include(o =&gt; o.Items.Where(...).OrderBy(...).Take(n))</c>). A plain property
/// access carries no operations.
/// </summary>
/// <param name="navigation">The navigation property to eager-load.</param>
/// <param name="operations">
/// The inline collection operators applied to the navigation, in written order; empty for a
/// plain property access or a reference navigation.
/// </param>
internal sealed class IncludeStep(PropertyInfo navigation, IReadOnlyList<IncludeOperation> operations)
{
    public PropertyInfo Navigation { get; } = navigation;

    public IReadOnlyList<IncludeOperation> Operations { get; } = operations;
}
