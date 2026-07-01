namespace JetDatabaseWriter.Queries;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

/// <summary>A <c>Where</c> applied to a parent's children, keeping the matches.</summary>
/// <param name="predicate">The child predicate lambda; compiled once over the boxed child entity.</param>
internal sealed class IncludeFilterOperation(LambdaExpression predicate) : IncludeOperation
{
    private readonly Func<object, bool> predicate = CompilePredicate(predicate);

    public override IEnumerable<object> Apply(IEnumerable<object> source) => source.Where(this.predicate);
}
