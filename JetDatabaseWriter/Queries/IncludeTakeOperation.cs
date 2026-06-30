namespace JetDatabaseWriter.Queries;

using System.Collections.Generic;
using System.Linq;

/// <summary>A <c>Take</c> applied to a parent's children, keeping a leading count.</summary>
/// <param name="count">The number of leading children to keep per parent.</param>
internal sealed class IncludeTakeOperation(int count) : IncludeOperation
{
    public override IEnumerable<object> Apply(IEnumerable<object> source) => source.Take(count);
}
