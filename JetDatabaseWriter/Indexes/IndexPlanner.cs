namespace JetDatabaseWriter.Indexes;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;

/// <summary>
/// Chooses the best available index to satisfy a row predicate and translates the
/// predicate into an <see cref="IndexQueryCriteria"/> seek.
/// </summary>
/// <remarks>
/// <para>
/// Matching is the classic leading-key-column rule: an index is usable when its
/// leading key columns are pinned by equality predicates, optionally terminated
/// by a single range predicate on the next key column. The number of key columns
/// the predicate constrains is the match score; the highest score wins, with
/// deterministic tie-breaks so the same inputs always select the same index.
/// </para>
/// <para>
/// The seek is sound but not necessarily exact — a key-prefix or range seek can
/// return rows the full predicate rejects — so callers must still apply the
/// compiled predicate to every row the seek yields. Descending range columns are
/// intentionally not pushed (their bound direction inverts in key space); the
/// residual filter enforces them instead.
/// </para>
/// </remarks>
internal static class IndexPlanner
{
    /// <summary>
    /// Selects the best index for <paramref name="criteria"/> from
    /// <paramref name="indexes"/>, or returns <see langword="null"/> when none is
    /// usable.
    /// </summary>
    /// <param name="indexes">Candidate indexes for the table.</param>
    /// <param name="criteria">The pushable necessary conditions extracted from the predicate.</param>
    /// <returns>The chosen plan, or <see langword="null"/> for a full scan.</returns>
    public static IndexPlan? TryPlan(IReadOnlyList<IndexMetadata> indexes, RowCriteria criteria)
    {
        Guard.NotNull(indexes, nameof(indexes));
        Guard.NotNull(criteria, nameof(criteria));

        if (indexes.Count == 0 || criteria.Count == 0)
        {
            return null;
        }

        Dictionary<string, ColumnConstraint> constraints = BuildConstraints(criteria);
        if (constraints.Count == 0)
        {
            return null;
        }

        IndexPlan? best = null;
        foreach (IndexMetadata index in indexes)
        {
            IndexPlan? candidate = TryPlanIndex(index, constraints);
            if (candidate is not null && IsBetter(candidate, best))
            {
                best = candidate;
            }
        }

        return best;
    }

    private static Dictionary<string, ColumnConstraint> BuildConstraints(RowCriteria criteria)
    {
        var map = new Dictionary<string, ColumnConstraint>(StringComparer.OrdinalIgnoreCase);
        foreach (ColumnPredicate predicate in criteria.Predicates)
        {
            ColumnPredicateOperator op = predicate.Operator;

            // NotEqual / In / IsNull / IsNotNull are not seek-helpful and are left
            // entirely to the residual client-side filter, so they are ignored here.
            if (op == ColumnPredicateOperator.Equal)
            {
                GetOrAdd(map, predicate.ColumnName).ApplyEquality(predicate.Operand);
            }
            else if (op is ColumnPredicateOperator.GreaterThan or ColumnPredicateOperator.GreaterThanOrEqual)
            {
                GetOrAdd(map, predicate.ColumnName).ApplyLower(predicate.Operand, op == ColumnPredicateOperator.GreaterThanOrEqual);
            }
            else if (op is ColumnPredicateOperator.LessThan or ColumnPredicateOperator.LessThanOrEqual)
            {
                GetOrAdd(map, predicate.ColumnName).ApplyUpper(predicate.Operand, op == ColumnPredicateOperator.LessThanOrEqual);
            }
            else if (op == ColumnPredicateOperator.Between)
            {
                ColumnConstraint constraint = GetOrAdd(map, predicate.ColumnName);
                constraint.ApplyLower(predicate.Operand, inclusive: true);
                constraint.ApplyUpper(predicate.UpperOperand, inclusive: true);
            }
        }

        return map;
    }

    private static ColumnConstraint GetOrAdd(Dictionary<string, ColumnConstraint> map, string columnName)
    {
        if (!map.TryGetValue(columnName, out ColumnConstraint? constraint))
        {
            constraint = new ColumnConstraint();
            map[columnName] = constraint;
        }

        return constraint;
    }

    private static IndexPlan? TryPlanIndex(IndexMetadata index, Dictionary<string, ColumnConstraint> constraints)
    {
        if (index.FirstDp <= 0 || index.Columns.Count == 0)
        {
            return null;
        }

        var equalityValues = new List<object?>();
        ColumnConstraint? rangeConstraint = null;

        foreach (IndexColumnReference keyColumn in index.Columns)
        {
            if (!constraints.TryGetValue(keyColumn.Name, out ColumnConstraint? constraint))
            {
                break;
            }

            if (constraint.HasEquality)
            {
                equalityValues.Add(constraint.EqualityValue);
                continue;
            }

            // A range terminates the match, and only on an ascending column.
            if (constraint.HasRange && keyColumn.IsAscending)
            {
                rangeConstraint = constraint;
            }

            break;
        }

        int matched = equalityValues.Count + (rangeConstraint is not null ? 1 : 0);
        if (matched == 0)
        {
            return null;
        }

        IndexQueryCriteria criteria = BuildCriteria(index, equalityValues, rangeConstraint);
        return new IndexPlan(index, criteria, matched);
    }

    private static IndexQueryCriteria BuildCriteria(
        IndexMetadata index,
        List<object?> equalityValues,
        ColumnConstraint? rangeConstraint)
    {
        if (rangeConstraint is null)
        {
            return equalityValues.Count == index.Columns.Count
                ? IndexQueryCriteria.Exact(equalityValues)
                : IndexQueryCriteria.KeyPrefix(equalityValues);
        }

        IndexKeyBound? lower = BuildBound(equalityValues, rangeConstraint.HasLower, rangeConstraint.LowerValue, rangeConstraint.LowerInclusive);
        IndexKeyBound? upper = BuildBound(equalityValues, rangeConstraint.HasUpper, rangeConstraint.UpperValue, rangeConstraint.UpperInclusive);
        return IndexQueryCriteria.Range(lower, upper);
    }

    private static IndexKeyBound? BuildBound(List<object?> equalityValues, bool hasBound, object? boundValue, bool inclusive)
    {
        if (hasBound)
        {
            var values = new List<object?>(equalityValues) { boundValue };
            return new IndexKeyBound(values, inclusive);
        }

        // No bound on this side: the equality prefix still pins the leading key
        // columns. With no equality prefix either, the side is unbounded.
        return equalityValues.Count > 0 ? new IndexKeyBound(equalityValues, isInclusive: true) : null;
    }

    private static bool IsBetter(IndexPlan candidate, IndexPlan? current)
    {
        if (current is null)
        {
            return true;
        }

        if (candidate.MatchedKeyColumns != current.MatchedKeyColumns)
        {
            return candidate.MatchedKeyColumns > current.MatchedKeyColumns;
        }

        bool candidateExact = candidate.Criteria.Kind == IndexQueryKind.Exact;
        bool currentExact = current.Criteria.Kind == IndexQueryKind.Exact;
        if (candidateExact != currentExact)
        {
            return candidateExact;
        }

        if (candidate.Index.EnforcesUniqueness != current.Index.EnforcesUniqueness)
        {
            return candidate.Index.EnforcesUniqueness;
        }

        if (candidate.Index.Columns.Count != current.Index.Columns.Count)
        {
            return candidate.Index.Columns.Count < current.Index.Columns.Count;
        }

        if (candidate.Index.IndexNumber != current.Index.IndexNumber)
        {
            return candidate.Index.IndexNumber < current.Index.IndexNumber;
        }

        return string.CompareOrdinal(candidate.Index.Name, current.Index.Name) < 0;
    }

    private sealed class ColumnConstraint
    {
        public bool HasEquality { get; private set; }

        public object? EqualityValue { get; private set; }

        public bool HasLower { get; private set; }

        public object? LowerValue { get; private set; }

        public bool LowerInclusive { get; private set; }

        public bool HasUpper { get; private set; }

        public object? UpperValue { get; private set; }

        public bool UpperInclusive { get; private set; }

        public bool HasRange => this.HasLower || this.HasUpper;

        public void ApplyEquality(object? value)
        {
            if (!this.HasEquality)
            {
                this.HasEquality = true;
                this.EqualityValue = value;
            }
        }

        public void ApplyLower(object? value, bool inclusive)
        {
            if (!this.HasLower)
            {
                this.HasLower = true;
                this.LowerValue = value;
                this.LowerInclusive = inclusive;
            }
        }

        public void ApplyUpper(object? value, bool inclusive)
        {
            if (!this.HasUpper)
            {
                this.HasUpper = true;
                this.UpperValue = value;
                this.UpperInclusive = inclusive;
            }
        }
    }
}
