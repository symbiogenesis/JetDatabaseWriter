namespace JetDatabaseWriter.EntityFrameworkCore.Query.Internal;

using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;

internal static partial class JetAsyncQueryExecutor
{
    private const string QueryFilterContextParameterName = "__jet_provider_query_filter_context";

    private static Expression GetQueryFilterExpression(IQueryFilter filter)
        => new QueryFilterContextParameterizer().Visit(filter.Expression)!;

    private sealed class QueryFilterContextParameterizer : ExpressionVisitor
    {
        public override Expression? Visit(Expression? node)
        {
            if (node is not null
                && (typeof(DbContext).IsAssignableFrom(node.Type) || node is ConstantExpression { Value: DbContext }))
            {
                // Model filters outlive the context that built the model. Bind context access
                // through runtime parameters so residual predicates and index seeks agree.
                return new QueryParameterExpression(QueryFilterContextParameterName, node.Type);
            }

            return base.Visit(node);
        }
    }
}
