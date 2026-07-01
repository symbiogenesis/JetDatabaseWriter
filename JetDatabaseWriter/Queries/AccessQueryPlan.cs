namespace JetDatabaseWriter.Queries;

using System;
using System.Collections.Generic;

/// <summary>
/// The translated form of a query expression tree: the operators the provider knows
/// how to execute, kept as an ordered pipeline so they run in the sequence the caller
/// wrote them. Includes are order-independent eager loads applied to the final set,
/// each recorded as a navigation <em>path</em> (a single <c>Include</c>, or an
/// <c>Include</c> followed by one or more <c>ThenInclude</c> steps) whose steps may carry
/// inline collection operators (filtered / ordered includes).
/// </summary>
internal sealed class AccessQueryPlan
{
    public List<QueryStage> Stages { get; } = [];

    public List<List<IncludeStep>> IncludePaths { get; } = [];

    public void StartInclude(IncludeStep step) => this.IncludePaths.Add([step]);

    public void ExtendInclude(IncludeStep step)
    {
        if (this.IncludePaths.Count == 0)
        {
            throw new NotSupportedException("ThenInclude must follow an Include in the query.");
        }

        this.IncludePaths[^1].Add(step);
    }
}
