namespace JetDatabaseWriter.Queries;

using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

/// <summary>
/// The accumulated, translated form of a query expression tree: the operators the
/// provider knows how to execute against the reader.
/// </summary>
internal sealed class AccessQueryPlan
{
    public List<LambdaExpression> Predicates { get; } = [];

    public List<(LambdaExpression KeySelector, bool Descending)> Orderings { get; } = [];

    public List<PropertyInfo> Includes { get; } = [];

    public int? Skip { get; set; }

    public int? Take { get; set; }
}
