namespace JetDatabaseWriter.EntityFrameworkCore.Query.Internal;

using System;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

/// <summary>
/// Translates LINQ query methods into <see cref="ShapedQueryExpression"/> instances for the <see cref="JetDatabaseWriter"/> provider.
/// </summary>
public sealed class JetQueryableMethodTranslatingExpressionVisitor : QueryableMethodTranslatingExpressionVisitor
{
    /// <summary>
    /// Initializes a new instance of the <see cref="JetQueryableMethodTranslatingExpressionVisitor"/> class.
    /// </summary>
    /// <param name="dependencies">Parameter object containing dependencies for this service.</param>
    /// <param name="queryCompilationContext">The query compilation context.</param>
    public JetQueryableMethodTranslatingExpressionVisitor(
        QueryableMethodTranslatingExpressionVisitorDependencies dependencies,
        QueryCompilationContext queryCompilationContext)
        : base(dependencies, queryCompilationContext, subquery: false)
    {
    }

    private JetQueryableMethodTranslatingExpressionVisitor(
        JetQueryableMethodTranslatingExpressionVisitor parentVisitor)
        : base(parentVisitor.Dependencies, parentVisitor.QueryCompilationContext, subquery: true)
    {
    }

    /// <inheritdoc/>
    protected override QueryableMethodTranslatingExpressionVisitor CreateSubqueryVisitor()
        => new JetQueryableMethodTranslatingExpressionVisitor(this);

    /// <inheritdoc/>
    protected override ShapedQueryExpression CreateShapedQueryExpression(IEntityType entityType)
    {
        ArgumentNullException.ThrowIfNull(entityType);

        JetQueryExpression queryExpression = new(entityType);
        return new ShapedQueryExpression(
            queryExpression,
            new StructuralTypeShaperExpression(
                entityType,
                new ProjectionBindingExpression(queryExpression, new ProjectionMember(), typeof(ValueBuffer)),
                nullable: false));
    }

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateAll(ShapedQueryExpression source, LambdaExpression predicate) => source;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateAny(ShapedQueryExpression source, LambdaExpression? predicate) => source;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateAverage(ShapedQueryExpression source, LambdaExpression? selector, Type resultType) => source;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateCast(ShapedQueryExpression source, Type castType) => source;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateConcat(ShapedQueryExpression source1, ShapedQueryExpression source2) => source1;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateContains(ShapedQueryExpression source, Expression item) => source;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateCount(ShapedQueryExpression source, LambdaExpression? predicate) => source;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateDefaultIfEmpty(ShapedQueryExpression source, Expression? defaultValue) => source;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateDistinct(ShapedQueryExpression source) => source;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateElementAtOrDefault(ShapedQueryExpression source, Expression index, bool returnDefault) => source;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateExcept(ShapedQueryExpression source1, ShapedQueryExpression source2) => source1;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateFirstOrDefault(ShapedQueryExpression source, LambdaExpression? predicate, Type returnType, bool returnDefault)
    {
        ArgumentNullException.ThrowIfNull(source);
        return source.UpdateResultCardinality(returnDefault ? ResultCardinality.SingleOrDefault : ResultCardinality.Single);
    }

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateGroupBy(ShapedQueryExpression source, LambdaExpression keySelector, LambdaExpression? elementSelector, LambdaExpression? resultSelector) => source;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateGroupJoin(ShapedQueryExpression outer, ShapedQueryExpression inner, LambdaExpression outerKeySelector, LambdaExpression innerKeySelector, LambdaExpression resultSelector) => outer;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateIntersect(ShapedQueryExpression source1, ShapedQueryExpression source2) => source1;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateJoin(ShapedQueryExpression outer, ShapedQueryExpression inner, LambdaExpression outerKeySelector, LambdaExpression innerKeySelector, LambdaExpression resultSelector) => outer;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateLeftJoin(ShapedQueryExpression outer, ShapedQueryExpression inner, LambdaExpression outerKeySelector, LambdaExpression innerKeySelector, LambdaExpression resultSelector) => outer;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateRightJoin(ShapedQueryExpression outer, ShapedQueryExpression inner, LambdaExpression outerKeySelector, LambdaExpression innerKeySelector, LambdaExpression resultSelector) => outer;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateLastOrDefault(ShapedQueryExpression source, LambdaExpression? predicate, Type returnType, bool returnDefault)
    {
        ArgumentNullException.ThrowIfNull(source);
        return source.UpdateResultCardinality(returnDefault ? ResultCardinality.SingleOrDefault : ResultCardinality.Single);
    }

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateLongCount(ShapedQueryExpression source, LambdaExpression? predicate) => source;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateMax(ShapedQueryExpression source, LambdaExpression? selector, Type resultType) => source;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateMin(ShapedQueryExpression source, LambdaExpression? selector, Type resultType) => source;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateOfType(ShapedQueryExpression source, Type resultType) => source;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateOrderBy(ShapedQueryExpression source, LambdaExpression keySelector, bool ascending) => source;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateReverse(ShapedQueryExpression source) => source;

    /// <inheritdoc/>
    protected override ShapedQueryExpression TranslateSelect(ShapedQueryExpression source, LambdaExpression selector) => source;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateSelectMany(ShapedQueryExpression source, LambdaExpression collectionSelector, LambdaExpression resultSelector) => source;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateSelectMany(ShapedQueryExpression source, LambdaExpression selector) => source;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateSingleOrDefault(ShapedQueryExpression source, LambdaExpression? predicate, Type returnType, bool returnDefault)
    {
        ArgumentNullException.ThrowIfNull(source);
        return source.UpdateResultCardinality(returnDefault ? ResultCardinality.SingleOrDefault : ResultCardinality.Single);
    }

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateSkip(ShapedQueryExpression source, Expression count) => source;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateSkipWhile(ShapedQueryExpression source, LambdaExpression predicate) => source;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateSum(ShapedQueryExpression source, LambdaExpression? selector, Type resultType) => source;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateTake(ShapedQueryExpression source, Expression count) => source;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateTakeWhile(ShapedQueryExpression source, LambdaExpression predicate) => source;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateThenBy(ShapedQueryExpression source, LambdaExpression keySelector, bool ascending) => source;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateUnion(ShapedQueryExpression source1, ShapedQueryExpression source2) => source1;

    /// <inheritdoc/>
    protected override ShapedQueryExpression? TranslateWhere(ShapedQueryExpression source, LambdaExpression predicate) => source;
}
