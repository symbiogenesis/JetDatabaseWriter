namespace JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.EntityFrameworkCore.Metadata.Internal;
using JetDatabaseWriter.EntityFrameworkCore.Query.Internal;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;

/// <summary>
/// The main <see cref="Database"/> implementation for the <see cref="JetDatabaseWriter"/> Entity Framework Core provider,
/// translating change-tracker <see cref="IUpdateEntry"/> operations directly to <see cref="AccessWriter"/> calls
/// and compiling LINQ expression trees to asynchronous <see cref="JetDatabaseWriter"/> table and index queries.
/// </summary>
public sealed class JetDatabase : Database
{
    private readonly IJetClientConnection connection;
    private readonly IQueryTranslationPreprocessorFactory queryTranslationPreprocessorFactory;
    private readonly IShapedQueryCompilingExpressionVisitorFactory shapedQueryCompilingExpressionVisitorFactory;

    /// <summary>
    /// Initializes a new instance of the <see cref="JetDatabase"/> class.
    /// </summary>
    /// <param name="dependencies">Parameter object containing dependencies for this service.</param>
    /// <param name="connection">The client connection for the current context scope.</param>
    /// <param name="queryTranslationPreprocessorFactory">The query translation preprocessor factory.</param>
    /// <param name="shapedQueryCompilingExpressionVisitorFactory">The shaped query compiling expression visitor factory.</param>
    public JetDatabase(
        DatabaseDependencies dependencies,
        IJetClientConnection connection,
        IQueryTranslationPreprocessorFactory queryTranslationPreprocessorFactory,
        IShapedQueryCompilingExpressionVisitorFactory shapedQueryCompilingExpressionVisitorFactory)
        : base(dependencies)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(queryTranslationPreprocessorFactory);
        ArgumentNullException.ThrowIfNull(shapedQueryCompilingExpressionVisitorFactory);

        this.connection = connection;
        this.queryTranslationPreprocessorFactory = queryTranslationPreprocessorFactory;
        this.shapedQueryCompilingExpressionVisitorFactory = shapedQueryCompilingExpressionVisitorFactory;
    }

    /// <inheritdoc/>
    public override Func<QueryContext, TResult> CompileQuery<TResult>(Expression query, bool async)
        => JetAsyncQueryExecutor.Compile<TResult>(
            query,
            async,
            this.Dependencies,
            this.queryTranslationPreprocessorFactory,
            this.shapedQueryCompilingExpressionVisitorFactory);

    /// <inheritdoc/>
    public override int SaveChanges(IList<IUpdateEntry> entries)
        => throw new NotSupportedException(
            "Synchronous SaveChanges is not supported by JetDatabaseWriter.EntityFrameworkCore. Use SaveChangesAsync instead.");

    /// <inheritdoc/>
    public override async Task<int> SaveChangesAsync(
        IList<IUpdateEntry> entries,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entries);
        cancellationToken.ThrowIfCancellationRequested();

        if (entries.Count == 0)
        {
            return 0;
        }

        List<IUpdateEntry> orderedEntries = OrderEntries(entries);
        (AccessWriter writer, bool ownedByCaller) = await this.connection.RentWriterAsync(cancellationToken).ConfigureAwait(false);
        JetTransaction? batchTransaction = null;
        try
        {
            if (this.connection.ActiveTransaction is null && orderedEntries.Count > 1)
            {
                batchTransaction = await writer.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            }

            int totalAffected = 0;
            foreach (IUpdateEntry entry in orderedEntries)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (entry.EntityState == EntityState.Added)
                {
                    await ExecuteInsertAsync(writer, entry, cancellationToken).ConfigureAwait(false);
                    totalAffected++;
                }
                else if (entry.EntityState == EntityState.Modified)
                {
                    totalAffected += await ExecuteUpdateAsync(writer, entry, cancellationToken).ConfigureAwait(false);
                }
                else if (entry.EntityState == EntityState.Deleted)
                {
                    totalAffected += await ExecuteDeleteAsync(writer, entry, cancellationToken).ConfigureAwait(false);
                }
            }

            if (batchTransaction is not null)
            {
                await batchTransaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            }

            return totalAffected;
        }
        catch
        {
            if (batchTransaction is not null)
            {
#pragma warning disable CA1031, RCS1075 // Preserve original exception when rollback fails.
                try
                {
                    await batchTransaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception)
                {
                }
#pragma warning restore CA1031, RCS1075
            }

            throw;
        }
        finally
        {
            if (batchTransaction is not null)
            {
                await batchTransaction.DisposeAsync().ConfigureAwait(false);
            }

            await this.connection.ReleaseWriterAsync(writer, ownedByCaller).ConfigureAwait(false);
        }
    }

    private static List<IUpdateEntry> OrderEntries(IList<IUpdateEntry> entries)
    {
        List<IUpdateEntry> added = [];
        List<IUpdateEntry> modified = [];
        List<IUpdateEntry> deleted = [];

        foreach (IUpdateEntry entry in entries)
        {
            if (entry.EntityState == EntityState.Added)
            {
                added.Add(entry);
            }
            else if (entry.EntityState == EntityState.Modified)
            {
                modified.Add(entry);
            }
            else if (entry.EntityState == EntityState.Deleted)
            {
                deleted.Add(entry);
            }
        }

        List<IUpdateEntry> sortedAdded = SortByDependencyOrder(added, principalsFirst: true);
        List<IUpdateEntry> sortedDeleted = SortByDependencyOrder(deleted, principalsFirst: false);

        return [.. sortedAdded, .. modified, .. sortedDeleted];
    }

    private static List<IUpdateEntry> SortByDependencyOrder(List<IUpdateEntry> items, bool principalsFirst)
    {
        if (items.Count <= 1)
        {
            return items;
        }

        List<IUpdateEntry> sorted = [.. items];
        sorted.Sort((left, right) =>
        {
            IEntityType leftType = left.EntityType.GetRootType();
            IEntityType rightType = right.EntityType.GetRootType();
            if (leftType == rightType)
            {
                return 0;
            }

            bool leftDependsOnRight = DependsOn(leftType, rightType, []);
            bool rightDependsOnLeft = DependsOn(rightType, leftType, []);
            if (leftDependsOnRight && !rightDependsOnLeft)
            {
                return principalsFirst ? 1 : -1;
            }

            if (rightDependsOnLeft && !leftDependsOnRight)
            {
                return principalsFirst ? -1 : 1;
            }

            return 0;
        });

        return sorted;
    }

    private static bool DependsOn(IEntityType dependentCandidate, IEntityType principalCandidate, HashSet<IEntityType> visited)
    {
        if (!visited.Add(dependentCandidate))
        {
            return false;
        }

        foreach (IForeignKey fk in dependentCandidate.GetForeignKeys())
        {
            IEntityType principal = fk.PrincipalEntityType.GetRootType();
            if (principal == principalCandidate)
            {
                return true;
            }

            if (principal != dependentCandidate && DependsOn(principal, principalCandidate, visited))
            {
                return true;
            }
        }

        return false;
    }

    private static async ValueTask ExecuteInsertAsync(
        AccessWriter writer,
        IUpdateEntry entry,
        CancellationToken cancellationToken)
    {
        IEntityType entityType = entry.EntityType;
        string tableName = JetEntityTypeMetadata.GetTableName(entityType);
        RowValues rowValues = [];
        IProperty? autoIncrementProperty = null;

        foreach (IProperty property in entityType.GetProperties())
        {
            string columnName = JetEntityTypeMetadata.GetColumnName(property);
            if (JetEntityTypeMetadata.IsAutoIncrementProperty(property))
            {
                object? currentVal = entry.GetCurrentValue(property);
                bool hasTempOrDefault = entry.HasTemporaryValue(property)
                    || currentVal is null
                    || (currentVal is int i && i <= 0)
                    || (currentVal is short s && s <= 0)
                    || (currentVal is long l && l <= 0);
                if (hasTempOrDefault)
                {
                    autoIncrementProperty = property;
                    continue;
                }
            }

            if (property.ValueGenerated == ValueGenerated.OnAdd && property.ClrType == typeof(Guid))
            {
                object? currentGuid = entry.GetCurrentValue(property);
                if (entry.HasTemporaryValue(property) || currentGuid is null || (currentGuid is Guid g && g == Guid.Empty))
                {
                    var generatedGuid = Guid.NewGuid();
                    entry.SetStoreGeneratedValue(property, generatedGuid);
                    rowValues[columnName] = generatedGuid;
                    continue;
                }
            }

            object? modelValue = entry.GetCurrentValue(property);
            rowValues[columnName] = JetEntityTypeMetadata.ConvertToProvider(property, modelValue);
        }

        await writer.InsertRowAsync(tableName, rowValues, cancellationToken).ConfigureAwait(false);

        if (autoIncrementProperty is not null)
        {
            ResolvedTable? resolved = await writer.Services.Snapshots.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            if (resolved is not null)
            {
                using PageLease lease = await writer.Services.SnapshotPages.ReadPageAsync(resolved.Entry.TDefPage, cancellationToken).ConfigureAwait(false);
                int autoNumberOffset = writer.PageSize == 2048
                    ? TDefHeaderLayout.Jet3.AutoNumber
                    : TDefHeaderLayout.Jet4.AutoNumber;
                long highWater = JetTypeInfo.Ru32(lease.Buffer, autoNumberOffset);
                object? storeGenerated = JetEntityTypeMetadata.ConvertFromProvider(autoIncrementProperty, highWater);
                entry.SetStoreGeneratedValue(autoIncrementProperty, storeGenerated);
            }
        }
    }

    private static async ValueTask<int> ExecuteUpdateAsync(
        AccessWriter writer,
        IUpdateEntry entry,
        CancellationToken cancellationToken)
    {
        IEntityType entityType = entry.EntityType;
        string tableName = JetEntityTypeMetadata.GetTableName(entityType);
        RowCriteria criteria = BuildIdentityAndConcurrencyCriteria(entry);
        RowValues updatedValues = [];

        foreach (IProperty property in entityType.GetProperties())
        {
            if (property.IsPrimaryKey() || !entry.IsModified(property))
            {
                continue;
            }

            string columnName = JetEntityTypeMetadata.GetColumnName(property);
            object? currentValue = entry.GetCurrentValue(property);
            updatedValues[columnName] = JetEntityTypeMetadata.ConvertToProvider(property, currentValue);
        }

        if (updatedValues.Count == 0)
        {
            return 1;
        }

        int affected = await writer.UpdateRowsAsync(tableName, criteria, updatedValues, cancellationToken).ConfigureAwait(false);
        if (affected == 0)
        {
            throw new DbUpdateConcurrencyException(
                "The database operation was expected to affect 1 row(s), but actually affected 0 row(s); data may have been modified or deleted since entities were loaded.",
                [entry]);
        }

        return affected;
    }

    private static async ValueTask<int> ExecuteDeleteAsync(
        AccessWriter writer,
        IUpdateEntry entry,
        CancellationToken cancellationToken)
    {
        IEntityType entityType = entry.EntityType;
        string tableName = JetEntityTypeMetadata.GetTableName(entityType);
        RowCriteria criteria = BuildIdentityAndConcurrencyCriteria(entry);

        int affected = await writer.DeleteRowsAsync(tableName, criteria, cancellationToken).ConfigureAwait(false);
        if (affected == 0)
        {
            throw new DbUpdateConcurrencyException(
                "The database operation was expected to affect 1 row(s), but actually affected 0 row(s); data may have been modified or deleted since entities were loaded.",
                [entry]);
        }

        return affected;
    }

    private static RowCriteria BuildIdentityAndConcurrencyCriteria(IUpdateEntry entry)
    {
        IEntityType entityType = entry.EntityType;
        IKey primaryKey = entityType.FindPrimaryKey()
            ?? throw new InvalidOperationException($"Entity type '{entityType.DisplayName()}' does not define a primary key.");

        RowCriteria criteria = [];
        foreach (IProperty pkProperty in primaryKey.Properties)
        {
            string columnName = JetEntityTypeMetadata.GetColumnName(pkProperty);
            object? originalKey = JetEntityTypeMetadata.ConvertToProvider(pkProperty, entry.GetOriginalValue(pkProperty));
            criteria.Add(ColumnPredicate.EqualTo(columnName, originalKey));
        }

        foreach (IProperty property in entityType.GetProperties())
        {
            if (property.IsConcurrencyToken && !property.IsPrimaryKey())
            {
                string columnName = JetEntityTypeMetadata.GetColumnName(property);
                object? originalToken = JetEntityTypeMetadata.ConvertToProvider(property, entry.GetOriginalValue(property));
                criteria.Add(ColumnPredicate.EqualTo(columnName, originalToken));
            }
        }

        return criteria;
    }
}
