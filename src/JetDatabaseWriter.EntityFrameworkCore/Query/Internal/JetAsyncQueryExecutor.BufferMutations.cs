namespace JetDatabaseWriter.EntityFrameworkCore.Query.Internal;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Metadata.Internal;
using JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;
using JetDatabaseWriter.Models;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

internal static partial class JetAsyncQueryExecutor
{
    private static bool TryCompileMutationSource(
        Expression source,
        JetQueryPlan plan,
        IModel model,
        out Func<QueryContext, Dictionary<int, IReadOnlyList<ValueBuffer>>, IEnumerable<ValueBuffer>>? evaluate,
        out BufferQueryRewriter? rewriter,
        out ParameterExpression context,
        out ParameterExpression roots)
    {
        evaluate = null;
        rewriter = null;
        context = Expression.Parameter(typeof(QueryContext), "context");
        roots = Expression.Parameter(typeof(Dictionary<int, IReadOnlyList<ValueBuffer>>), "roots");
        if (!CanExecuteBuffers(plan) || plan.RootTables.Count != 1)
        {
            return false;
        }

        rewriter = new(context, roots, plan, model, streaming: false);
        try
        {
            Expression rewritten = rewriter.Visit(source);
            if (!typeof(IEnumerable<ValueBuffer>).IsAssignableFrom(rewritten.Type))
            {
                return false;
            }

            evaluate = Expression.Lambda<Func<QueryContext, Dictionary<int, IReadOnlyList<ValueBuffer>>, IEnumerable<ValueBuffer>>>(
                rewritten, context, roots).Compile();
            return true;
        }
        catch (BufferTranslationException)
        {
            return false;
        }
    }

    private static bool TryBuildBufferDelete(
        Expression source,
        JetQueryPlan plan,
        IModel model,
        out Func<QueryContext, Task<int>>? executor)
    {
        executor = null;
        if (!TryCompileMutationSource(source, plan, model, out Func<QueryContext, Dictionary<int, IReadOnlyList<ValueBuffer>>, IEnumerable<ValueBuffer>>? evaluate, out BufferQueryRewriter? rewriter, out _, out _))
        {
            return false;
        }

        JetTableLoadPlan table = plan.RootTables[0];
        IKey key = table.RootEntityType.FindPrimaryKey()
            ?? throw new InvalidOperationException("ExecuteDelete requires a primary key.");
        table.RequiredProperties = new HashSet<IProperty>(rewriter!.ReferencedProperties.Concat(key.Properties));
        executor = async context =>
        {
            IJetClientConnection connection = ((JetQueryContext)context).Connection;
            (AccessWriter writer, bool owned) = await connection.RentWriterAsync(context.CancellationToken).ConfigureAwait(false);
            try
            {
                Dictionary<int, IReadOnlyList<ValueBuffer>> buffers = await LoadBufferRootsAsync(context, plan).ConfigureAwait(false);
                HashSet<StructuralKey> seen = [];
                List<RowCriteria> criteria = [];
                foreach (ValueBuffer buffer in evaluate!(context, buffers))
                {
                    context.CancellationToken.ThrowIfCancellationRequested();
                    if (seen.Add(ExtractKey(key, buffer)))
                    {
                        criteria.Add(BufferKeyCriteria(key, buffer));
                    }
                }

                return criteria.Count == 0 ? 0 : await writer.DeleteRowsAsync(JetEntityTypeMetadata.GetTableName(table.RootEntityType), criteria, context.CancellationToken).ConfigureAwait(false);
            }
            finally
            {
                await connection.ReleaseWriterAsync(writer, owned).ConfigureAwait(false);
            }
        };
        return true;
    }

    private static bool TryBuildBufferUpdate(
        Expression source,
        Expression setters,
        JetQueryPlan plan,
        IModel model,
        out Func<QueryContext, Task<int>>? executor)
    {
        executor = null;
        if (!TryCompileMutationSource(source, plan, model, out Func<QueryContext, Dictionary<int, IReadOnlyList<ValueBuffer>>, IEnumerable<ValueBuffer>>? evaluate, out BufferQueryRewriter? rewriter, out ParameterExpression? context, out _))
        {
            return false;
        }

        if (StripQuotesAndObjectConvert(setters) is not NewArrayExpression { Expressions.Count: > 0 } assignments)
        {
            return false;
        }

        JetTableLoadPlan table = plan.RootTables[0];
        IKey key = table.RootEntityType.FindPrimaryKey()
            ?? throw new InvalidOperationException("ExecuteUpdate requires a primary key.");
        List<(IProperty Property, Func<QueryContext, ValueBuffer, object?> Evaluate)> compiled = [];
        try
        {
            foreach (Expression assignment in assignments.Expressions)
            {
                if (StripQuotesAndObjectConvert(assignment) is not NewExpression { Arguments.Count: 2 } pair
                    || StripQuotesAndObjectConvert(pair.Arguments[0]) is not LambdaExpression selector)
                {
                    return false;
                }

                IProperty property = ResolveTargetPropertyForUpdate(table.RootEntityType, model, selector);
                if (property.IsPrimaryKey() || JetEntityTypeMetadata.GetCalculatedExpression(property) is not null)
                {
                    throw new InvalidOperationException($"The property '{property.Name}' cannot be modified by ExecuteUpdate.");
                }

                Expression value = StripQuotesAndObjectConvert(pair.Arguments[1]);
                ParameterExpression buffer = Expression.Parameter(typeof(ValueBuffer), "buffer");
                Expression body;
                if (value is LambdaExpression lambda)
                {
                    var rewritten = (LambdaExpression)rewriter!.Visit(lambda);
                    body = new ExpressionReplacer(rewritten.Parameters[0], buffer).Visit(rewritten.Body)!;
                }
                else
                {
                    body = rewriter!.Visit(value)!;
                }

                compiled.Add((property, Expression.Lambda<Func<QueryContext, ValueBuffer, object?>>(
                    Expression.Convert(body, typeof(object)), context, buffer).Compile()));
            }
        }
        catch (BufferTranslationException)
        {
            return false;
        }

        table.RequiredProperties = new HashSet<IProperty>(rewriter!.ReferencedProperties.Concat(key.Properties));
        executor = async qc =>
        {
            IJetClientConnection connection = ((JetQueryContext)qc).Connection;
            (AccessWriter writer, bool owned) = await connection.RentWriterAsync(qc.CancellationToken).ConfigureAwait(false);
            try
            {
                Dictionary<int, IReadOnlyList<ValueBuffer>> buffers = await LoadBufferRootsAsync(qc, plan).ConfigureAwait(false);
                HashSet<StructuralKey> seen = [];
                List<RowUpdate> updates = [];
                foreach (ValueBuffer buffer in evaluate!(qc, buffers))
                {
                    qc.CancellationToken.ThrowIfCancellationRequested();
                    if (!seen.Add(ExtractKey(key, buffer)))
                    {
                        continue;
                    }

                    RowValues changes = [];
                    foreach ((IProperty property, Func<QueryContext, ValueBuffer, object?> evaluateSetter) in compiled)
                    {
                        changes[JetEntityTypeMetadata.GetColumnName(property)] =
                            JetEntityTypeMetadata.ConvertToProvider(property, evaluateSetter(qc, buffer));
                    }

                    updates.Add(new RowUpdate(BufferKeyCriteria(key, buffer), changes));
                }

                return updates.Count == 0 ? 0 : await writer.UpdateRowsAsync(JetEntityTypeMetadata.GetTableName(table.RootEntityType), updates, qc.CancellationToken).ConfigureAwait(false);
            }
            finally
            {
                await connection.ReleaseWriterAsync(writer, owned).ConfigureAwait(false);
            }
        };
        return true;
    }

    private static RowCriteria BufferKeyCriteria(IKey key, ValueBuffer buffer)
    {
        RowCriteria criteria = [];
        foreach (IProperty property in key.Properties)
        {
            criteria.Add(ColumnPredicate.EqualTo(
                JetEntityTypeMetadata.GetColumnName(property),
                JetEntityTypeMetadata.ConvertToProvider(property, buffer[property.GetIndex()])));
        }

        return criteria;
    }
}
