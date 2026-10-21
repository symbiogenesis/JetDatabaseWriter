namespace JetDatabaseWriter.Queries;

using System.Collections.Generic;
using System.Linq;

/// <summary>A <c>Skip</c> applied to a parent's children, discarding a leading count.</summary>
/// <param name="count">The number of leading children to discard per parent.</param>
internal sealed class IncludeSkipOperation(int count) : IncludeOperation
{
    public override IEnumerable<object> Apply(IEnumerable<object> source) => source.Skip(count);
}
