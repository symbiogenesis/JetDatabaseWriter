namespace JetDatabaseWriter.EntityFrameworkCore.Query.Internal;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

internal static partial class JetAsyncQueryExecutor
{
    private static bool TryBuildStreamingAggregate<TResult>(
        Expression query,
        JetQueryPlan plan,
        IModel model,
        out Func<QueryContext, Task<TResult>>? executor)
    {
        executor = null;
        if (!CanExecuteScalarBuffers(plan) || plan.RootTables.Count != 1
            || UnwrapBufferQueryModifiers(query) is not MethodCallExpression terminal
            || terminal.Method.DeclaringType != typeof(Queryable)
            || terminal.Method.Name is not (nameof(Queryable.Sum) or nameof(Queryable.Average)
                or nameof(Queryable.Min) or nameof(Queryable.Max) or nameof(Queryable.All)))
        {
            return false;
        }

        Expression source = UnwrapBufferQueryModifiers(terminal.Arguments[0]);
        LambdaExpression? selector = terminal.Arguments.Count == 2 ? StripQuotesAndObjectConvert(terminal.Arguments[1]) as LambdaExpression : null;
        if (source is MethodCallExpression projection && projection.Method.DeclaringType == typeof(Queryable)
            && projection.Method.Name == nameof(Queryable.Select) && selector is null)
        {
            selector = StripQuotesAndObjectConvert(projection.Arguments[1]) as LambdaExpression;
            source = UnwrapBufferQueryModifiers(projection.Arguments[0]);
        }

        if (selector is null || selector.Parameters.Count != 1)
        {
            return false;
        }

        List<LambdaExpression> predicates = [];
        while (source is MethodCallExpression where && where.Method.DeclaringType == typeof(Queryable)
            && where.Method.Name == nameof(Queryable.Where)
            && StripQuotesAndObjectConvert(where.Arguments[1]) is LambdaExpression { Parameters.Count: 1 } predicate)
        {
            predicates.Insert(0, predicate);
            source = UnwrapBufferQueryModifiers(where.Arguments[0]);
        }

        if (source is not EntityQueryRootExpression)
        {
            return false;
        }

        if (!plan.IgnoreQueryFilters)
        {
            predicates.InsertRange(0, GetAllQueryFilters(plan.RootTables[0].QueryEntityType, plan.IgnoredQueryFilterNames).Select(GetQueryFilterExpression).OfType<LambdaExpression>());
        }

        ParameterExpression context = Expression.Parameter(typeof(QueryContext), "context");
        ParameterExpression buffer = Expression.Parameter(typeof(ValueBuffer), "buffer");
        ParameterExpression roots = Expression.Parameter(typeof(Dictionary<int, IReadOnlyList<ValueBuffer>>), "roots");
        BufferQueryRewriter rewriter = new(context, roots, plan, model, streaming: false);
        try
        {
            var mapped = (LambdaExpression)rewriter.Visit(selector);
            if (mapped.Parameters[0].Type != typeof(ValueBuffer) || mapped.ReturnType == typeof(ValueBuffer))
            {
                return false;
            }

            Expression value = new ExpressionReplacer(mapped.Parameters[0], buffer).Visit(mapped.Body)!;
            LambdaExpression evaluate = Expression.Lambda(
                typeof(Func<,,>).MakeGenericType(typeof(QueryContext), typeof(ValueBuffer), value.Type), value, context, buffer);
            Expression keep = BufferDiscriminatorPredicate(plan.RootTables[0], buffer);
            foreach (LambdaExpression predicate in predicates)
            {
                var rewritten = (LambdaExpression)rewriter.Visit(predicate);
                keep = Expression.AndAlso(keep, new ExpressionReplacer(rewritten.Parameters[0], buffer).Visit(rewritten.Body)!);
            }

            Func<QueryContext, ValueBuffer, bool> filter = Expression.Lambda<Func<QueryContext, ValueBuffer, bool>>(keep, context, buffer).Compile();
            MethodInfo build = typeof(JetAsyncQueryExecutor).GetMethod(nameof(BuildStreamingAggregate), BindingFlags.NonPublic | BindingFlags.Static)!
                .MakeGenericMethod(value.Type, typeof(TResult));
            executor = InvokeBuilderMethod<Func<QueryContext, Task<TResult>>>(build, [plan.RootTables[0], terminal.Method.Name, evaluate.Compile(), filter]);
            NarrowBufferProperties(plan, rewriter.ReferencedProperties);
            return true;
        }
        catch (BufferTranslationException)
        {
            return false;
        }
    }

    private static Func<QueryContext, Task<TResult>> BuildStreamingAggregate<TValue, TResult>(
        JetTableLoadPlan table,
        string operation,
        Func<QueryContext, ValueBuffer, TValue> select,
        Func<QueryContext, ValueBuffer, bool> keep)
        => async context =>
        {
            AggregateAccumulator<TValue> accumulator = new(operation);
            await ((JetQueryContext)context).Connection.ScanTableRowsAsync(
                table.RootEntityType,
                table.SeekConditions,
                context.Parameters,
                table.RequiredProperties,
                buffer => !keep(context, buffer) || accumulator.Add(select(context, buffer)),
                context.CancellationToken).ConfigureAwait(false);
            return (TResult)accumulator.GetResult()!;
        };

    private sealed class AggregateAccumulator<T>(string operation)
    {
        private readonly Type valueType = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);
        private long count;
        private long integerSum;
        private double floatingSum;
        private decimal decimalSum;
        private T? extreme;
        private bool all = true;

        public bool Add(T value)
        {
            if (operation == nameof(Queryable.All))
            {
                this.all = (bool)(object)value!;
                return this.all;
            }

            if (value is null)
            {
                return true;
            }

            if (operation is nameof(Queryable.Min) or nameof(Queryable.Max))
            {
                int comparison = this.count == 0 ? 0 : Comparer<T>.Default.Compare(value, this.extreme!);
                if (this.count == 0 || (operation == nameof(Queryable.Min) ? comparison < 0 : comparison > 0))
                {
                    this.extreme = value;
                }
            }
            else if (this.valueType == typeof(int))
            {
                int number = (int)(object)value;
                this.integerSum = operation == nameof(Queryable.Sum) ? checked((int)this.integerSum + number) : checked(this.integerSum + number);
            }
            else if (this.valueType == typeof(long))
            {
                this.integerSum = checked(this.integerSum + (long)(object)value);
            }
            else if (this.valueType == typeof(decimal))
            {
                this.decimalSum += (decimal)(object)value;
            }
            else if (this.valueType == typeof(float))
            {
                this.floatingSum += (float)(object)value;
            }
            else
            {
                this.floatingSum += (double)(object)value;
            }

            this.count++;
            return true;
        }

        public object? GetResult()
        {
            if (operation == nameof(Queryable.All))
            {
                return this.all;
            }

            if (this.count == 0 && operation != nameof(Queryable.Sum))
            {
                return default(T) is null ? null : throw new InvalidOperationException("Sequence contains no elements");
            }

            if (operation is nameof(Queryable.Min) or nameof(Queryable.Max))
            {
                return this.extreme;
            }

            if (this.valueType == typeof(decimal))
            {
                return operation == nameof(Queryable.Average) ? this.decimalSum / this.count : this.decimalSum;
            }

            if (this.valueType == typeof(float))
            {
                return (float)(operation == nameof(Queryable.Average) ? this.floatingSum / this.count : this.floatingSum);
            }

            if (this.valueType == typeof(double))
            {
                return operation == nameof(Queryable.Average) ? this.floatingSum / this.count : this.floatingSum;
            }

            if (operation == nameof(Queryable.Average))
            {
                return (double)this.integerSum / this.count;
            }

            if (this.valueType == typeof(int))
            {
                return (int)this.integerSum;
            }

            return this.integerSum;
        }
    }
}
