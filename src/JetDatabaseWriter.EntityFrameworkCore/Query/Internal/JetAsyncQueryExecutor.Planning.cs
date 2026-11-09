namespace JetDatabaseWriter.EntityFrameworkCore.Query.Internal;

using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;

internal static partial class JetAsyncQueryExecutor
{
    private sealed partial class JetQueryPlanExtractor
    {
        private readonly Dictionary<EntityQueryRootExpression, ParameterExpression> rootParameters =
            [with(ReferenceEqualityComparer.Instance)];

        private readonly Dictionary<ParameterExpression, EntityQueryRootExpression> parameterRoots =
            [with(ReferenceEqualityComparer.Instance)];

        private static Expression ReduceShape(Expression expression) => new ShapeMemberReducer().Visit(expression);

        private static IEnumerable<Expression> SplitConjunction(Expression expression)
        {
            if (expression is BinaryExpression { NodeType: ExpressionType.AndAlso } binary)
            {
                foreach (Expression left in SplitConjunction(binary.Left))
                {
                    yield return left;
                }

                foreach (Expression right in SplitConjunction(binary.Right))
                {
                    yield return right;
                }
            }
            else
            {
                yield return expression;
            }
        }

        private static bool ContainsOuterJoin(Expression source)
        {
            if (source is not MethodCallExpression call)
            {
                return false;
            }

            return (call.Method.DeclaringType == typeof(Queryable)
                && call.Method.Name is nameof(Queryable.LeftJoin) or nameof(Queryable.GroupJoin))
                || call.Arguments.Any(ContainsOuterJoin);
        }

        private Expression? ResolveSourceShape(Expression source, bool allowBounds = false)
        {
            if (source is EntityQueryRootExpression root)
            {
                if (!this.rootParameters.TryGetValue(root, out ParameterExpression? parameter))
                {
                    parameter = Expression.Parameter(root.EntityType.ClrType, "root");
                    this.rootParameters.Add(root, parameter);
                    this.parameterRoots.Add(parameter, root);
                }

                return parameter;
            }

            if (source is not MethodCallExpression call || call.Arguments.Count == 0)
            {
                return null;
            }

            if (call.Method.DeclaringType == typeof(EntityFrameworkQueryableExtensions))
            {
                return this.ResolveSourceShape(call.Arguments[0], allowBounds);
            }

            if (call.Method.DeclaringType != typeof(Queryable))
            {
                return null;
            }

            if (call.Method.Name is nameof(Queryable.Where) or nameof(Queryable.OrderBy)
                or nameof(Queryable.OrderByDescending) or nameof(Queryable.ThenBy) or nameof(Queryable.ThenByDescending)
                || (allowBounds && call.Method.Name is nameof(Queryable.Skip) or nameof(Queryable.Take) or nameof(Queryable.Distinct)
                    or nameof(Queryable.First) or nameof(Queryable.FirstOrDefault)
                    or nameof(Queryable.Single) or nameof(Queryable.SingleOrDefault)
                    or nameof(Queryable.Last) or nameof(Queryable.LastOrDefault)
                    or nameof(Queryable.ElementAt) or nameof(Queryable.ElementAtOrDefault)))
            {
                return this.ResolveSourceShape(call.Arguments[0], allowBounds);
            }

            if (call.Method.Name == nameof(Queryable.Select)
                && StripQuotes(call.Arguments[1]) is LambdaExpression { Parameters.Count: 1 } selector
                && this.ResolveSourceShape(call.Arguments[0], allowBounds) is Expression selectedSource)
            {
                return ReduceShape(new ExpressionReplacer(selector.Parameters[0], selectedSource).Visit(selector.Body)!);
            }

            if (call.Method.Name is nameof(Queryable.Join) or nameof(Queryable.LeftJoin)
                && call.Arguments.Count == 5
                && StripQuotes(call.Arguments[4]) is LambdaExpression { Parameters.Count: 2 } result
                && this.ResolveSourceShape(call.Arguments[0], allowBounds) is Expression outer
                && this.ResolveSourceShape(call.Arguments[1], allowBounds) is Expression inner)
            {
                Expression body = new ExpressionReplacer(result.Parameters[0], outer).Visit(result.Body)!;
                return ReduceShape(new ExpressionReplacer(result.Parameters[1], inner).Visit(body)!);
            }

            return null;
        }

        private HashSet<IProperty>? ComputeRequiredProperties(Expression query, IEntityType entityType)
        {
            Expression? resultShape = this.ResolveSourceShape(query, allowBounds: true);
            ResultEntityFinder resultFinder = new(model);
            _ = resultFinder.Visit(resultShape);
            EntityEqualityFinder equalityFinder = new(model);
            _ = equalityFinder.Visit(query);
            if (model.FindEntityType(UnwrapSequenceType(query.Type)) is not null
                || resultFinder.ContainsEntity
                || equalityFinder.ContainsEntityEquality
                || entityType.GetNavigations().Any(static navigation => navigation.TargetEntityType.IsOwned()))
            {
                // Entities in the result expose their complete fixed-up navigation graph.
                return null;
            }

            RootEntityPropertyUsageAnalyzer analyzer = new(model, entityType, ignoreTransparentEntities: true);
            this.VisitUsage(query, analyzer);
            if (analyzer.EntityEscapes)
            {
                return null;
            }

            if (resultShape is not null)
            {
                RootEntityPropertyUsageAnalyzer resultAnalyzer = new(model, entityType);
                _ = resultAnalyzer.Visit(resultShape);
                if (resultAnalyzer.EntityEscapes)
                {
                    return null;
                }

                analyzer.ReferencedProperties.UnionWith(resultAnalyzer.ReferencedProperties);
            }

            if (!this.IgnoreQueryFilters)
            {
                foreach (IQueryFilter filter in GetAllQueryFilters(entityType, this.IgnoredQueryFilterNames))
                {
                    _ = analyzer.Visit(GetQueryFilterExpression(filter));
                }
            }

            if (analyzer.EntityEscapes)
            {
                return null;
            }

            if (entityType.FindPrimaryKey() is IKey primaryKey)
            {
                analyzer.ReferencedProperties.UnionWith(primaryKey.Properties);
            }

            if (entityType.FindDiscriminatorProperty() is IProperty discriminator)
            {
                _ = analyzer.ReferencedProperties.Add(discriminator);
            }

            foreach (INavigationBase navigation in this.queryReferencedNavigations)
            {
                if (navigation is INavigation reference)
                {
                    if (reference.ForeignKey.DeclaringEntityType.GetRootType() == entityType)
                    {
                        analyzer.ReferencedProperties.UnionWith(reference.ForeignKey.Properties);
                    }

                    if (reference.ForeignKey.PrincipalEntityType.GetRootType() == entityType)
                    {
                        analyzer.ReferencedProperties.UnionWith(reference.ForeignKey.PrincipalKey.Properties);
                    }
                }
            }

            return analyzer.ReferencedProperties;
        }

        private void VisitUsage(Expression expression, RootEntityPropertyUsageAnalyzer analyzer)
        {
            if (expression is EntityQueryRootExpression)
            {
                return;
            }

            if (expression is not MethodCallExpression call || call.Arguments.Count == 0)
            {
                _ = analyzer.Visit(expression);
                return;
            }

            this.VisitUsage(call.Arguments[0], analyzer);
            bool join = call.Method.DeclaringType == typeof(Queryable)
                && call.Method.Name is nameof(Queryable.Join) or nameof(Queryable.GroupJoin) or nameof(Queryable.LeftJoin);
            if (join)
            {
                this.VisitUsage(call.Arguments[1], analyzer);
            }

            for (int i = join ? 2 : 1; i < call.Arguments.Count; i++)
            {
                if (StripQuotes(call.Arguments[i]) is not LambdaExpression lambda)
                {
                    continue;
                }

                Expression body = lambda.Body;
                if ((!join || i != 3) && this.ResolveSourceShape(call.Arguments[0], allowBounds: true) is Expression outer
                    && lambda.Parameters[0].Type == outer.Type)
                {
                    body = new ExpressionReplacer(lambda.Parameters[0], outer).Visit(body)!;
                }

                if (join && this.ResolveSourceShape(call.Arguments[1], allowBounds: true) is Expression inner)
                {
                    if (i == 3)
                    {
                        body = new ExpressionReplacer(lambda.Parameters[0], inner).Visit(lambda.Body)!;
                    }
                    else if (i == 4 && lambda.Parameters.Count == 2 && call.Method.Name != nameof(Queryable.GroupJoin))
                    {
                        body = new ExpressionReplacer(lambda.Parameters[1], inner).Visit(body)!;
                    }
                }

                _ = analyzer.Visit(ReduceShape(body));
            }
        }

        private bool TryExtractMappedJoinKeys(
            Expression source,
            LambdaExpression keySelector,
            out EntityQueryRootExpression? root,
            out List<IProperty>? properties)
        {
            root = null;
            properties = null;
            if (keySelector.Parameters.Count != 1 || this.ResolveSourceShape(source) is not Expression shape)
            {
                return false;
            }

            Expression key = ReduceShape(new ExpressionReplacer(keySelector.Parameters[0], shape).Visit(keySelector.Body)!);
            IReadOnlyList<Expression> components = key is NewExpression tuple ? tuple.Arguments : [key];
            List<IProperty> found = [];
            foreach (Expression component in components)
            {
                bool matched = false;
                foreach ((ParameterExpression parameter, EntityQueryRootExpression candidateRoot) in this.parameterRoots)
                {
                    if (JetIndexSeekPlanner.TryExtractPropertyAccess(candidateRoot.EntityType.GetRootType(), parameter, component, out IProperty? property))
                    {
                        if (root is not null && !ReferenceEquals(root, candidateRoot))
                        {
                            return false;
                        }

                        root = candidateRoot;
                        found.Add(property!);
                        matched = true;
                        break;
                    }
                }

                if (!matched)
                {
                    return false;
                }
            }

            properties = found;
            return root is not null && found.Count > 0;
        }

        private void ExtractMappedPredicates(Expression source, LambdaExpression predicate)
        {
            // Moving predicates across an outer join can create new unmatched rows.
            if (predicate.Parameters.Count != 1 || ContainsOuterJoin(source)
                || this.ResolveSourceShape(source) is not Expression shape)
            {
                return;
            }

            Expression body = ReduceShape(new ExpressionReplacer(predicate.Parameters[0], shape).Visit(predicate.Body)!);
            foreach (Expression conjunct in SplitConjunction(body))
            {
                ParameterUseFinder finder = new(this.parameterRoots.Keys);
                _ = finder.Visit(conjunct);
                if (finder.Used.Count != 1)
                {
                    continue;
                }

                ParameterExpression parameter = finder.Used.Single();
                EntityQueryRootExpression root = this.parameterRoots[parameter];
                JetTableLoadPlan plan = this.GetOrCreateRootPlan(root);
                LambdaExpression lambda = Expression.Lambda(conjunct, parameter);
                if (TryCompileValueBufferFilter(model, plan.RootEntityType, plan.QueryEntityType, [lambda], out _))
                {
                    plan.ScalarPredicates.Add(lambda);
                    plan.SeekConditions.AddRange(JetIndexSeekPlanner.ExtractSeekConditions(plan.RootEntityType, lambda));
                }
            }
        }

        private sealed class ShapeMemberReducer : ExpressionVisitor
        {
            protected override Expression VisitMember(MemberExpression node)
            {
                Expression? owner = this.Visit(node.Expression);
                if (owner is NewExpression { Members: not null } created)
                {
                    for (int i = 0; i < created.Members.Count; i++)
                    {
                        if (created.Members[i] == node.Member)
                        {
                            return this.Visit(created.Arguments[i]);
                        }
                    }
                }

                if (owner is MemberInitExpression initialized)
                {
                    foreach (MemberBinding binding in initialized.Bindings)
                    {
                        if (binding is MemberAssignment assignment && assignment.Member == node.Member)
                        {
                            return this.Visit(assignment.Expression);
                        }
                    }
                }

                return node.Update(owner);
            }
        }

        private sealed class ResultEntityFinder(IModel model) : ExpressionVisitor
        {
            public bool ContainsEntity { get; private set; }

            protected override Expression VisitMember(MemberExpression node)
            {
                if (model.FindEntityType(UnwrapSequenceType(node.Type)) is not null)
                {
                    this.ContainsEntity = true;
                    return node;
                }

                if (node.Expression is not null
                    && model.FindEntityType(node.Expression.Type)?.FindProperty(node.Member) is not null)
                {
                    return node;
                }

                return base.VisitMember(node);
            }

            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                if (model.FindEntityType(UnwrapSequenceType(node.Type)) is not null)
                {
                    this.ContainsEntity = true;
                    return node;
                }

                return base.VisitMethodCall(node);
            }

            protected override Expression VisitParameter(ParameterExpression node)
            {
                if (model.FindEntityType(node.Type) is not null)
                {
                    this.ContainsEntity = true;
                }

                return node;
            }
        }

        private sealed class EntityEqualityFinder(IModel model) : ExpressionVisitor
        {
            public bool ContainsEntityEquality { get; private set; }

            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                if (node.Method.DeclaringType == typeof(Queryable)
                    && node.Method.Name is nameof(Queryable.Distinct) or nameof(Queryable.Union)
                        or nameof(Queryable.Intersect) or nameof(Queryable.Except)
                        or nameof(Queryable.DistinctBy) or nameof(Queryable.UnionBy)
                        or nameof(Queryable.IntersectBy) or nameof(Queryable.ExceptBy)
                        or nameof(Queryable.Contains) or nameof(Queryable.SequenceEqual)
                    && node.Arguments.Count > 0
                    && this.ContainsMappedType(UnwrapSequenceType(node.Arguments[0].Type)))
                {
                    this.ContainsEntityEquality = true;
                }

                return base.VisitMethodCall(node);
            }

            private bool ContainsMappedType(System.Type type)
                => model.FindEntityType(type) is not null
                    || (type.IsGenericType && type.GetGenericArguments().Any(this.ContainsMappedType));
        }

        private sealed class ParameterUseFinder(IEnumerable<ParameterExpression> candidates) : ExpressionVisitor
        {
            private readonly HashSet<ParameterExpression> candidates = [.. candidates];

            public HashSet<ParameterExpression> Used { get; } = [];

            protected override Expression VisitParameter(ParameterExpression node)
            {
                if (this.candidates.Contains(node))
                {
                    _ = this.Used.Add(node);
                }

                return node;
            }
        }
    }
}
