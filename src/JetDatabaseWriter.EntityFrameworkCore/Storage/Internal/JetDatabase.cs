namespace JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.EntityFrameworkCore.Metadata.Internal;
using JetDatabaseWriter.EntityFrameworkCore.Query.Internal;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
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
        IUpdateEntry? activeEntry = null;
        try
        {
            bool canOpenBatchScope = this.connection is not JetClientConnection jetConn
                || jetConn.ActiveTransactionNestingDepth < 5;
            if (orderedEntries.Count > 1 && canOpenBatchScope)
            {
                batchTransaction = await writer.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            }

            Dictionary<string, long> tdefPageByTable = [with(StringComparer.OrdinalIgnoreCase)];
            int totalAffected = 0;
            int index = 0;
            while (index < orderedEntries.Count)
            {
                cancellationToken.ThrowIfCancellationRequested();
                IUpdateEntry entry = orderedEntries[index];
                activeEntry = entry;

                if (entry.EntityState == EntityState.Added)
                {
                    (RowValues rowValues, IProperty? autoIncrementProperty, List<IProperty>? serverGeneratedProps) = PrepareInsertRow(entry);
                    string tableName = JetEntityTypeMetadata.GetTableName(entry.EntityType);

                    if (autoIncrementProperty is null)
                    {
                        List<RowValues> batchRows = [rowValues];
                        List<(IUpdateEntry Entry, List<IProperty>? ServerProps)> batchEntries = [(entry, serverGeneratedProps)];
                        int nextIdx = index + 1;
                        while (nextIdx < orderedEntries.Count
                            && orderedEntries[nextIdx].EntityState == EntityState.Added
                            && string.Equals(JetEntityTypeMetadata.GetTableName(orderedEntries[nextIdx].EntityType), tableName, StringComparison.OrdinalIgnoreCase))
                        {
                            (RowValues nextRow, IProperty? nextAutoInc, List<IProperty>? nextServerProps) = PrepareInsertRow(orderedEntries[nextIdx]);
                            if (nextAutoInc is not null)
                            {
                                break;
                            }

                            batchRows.Add(nextRow);
                            batchEntries.Add((orderedEntries[nextIdx], nextServerProps));
                            nextIdx++;
                        }

                        if (batchRows.Count == 1)
                        {
                            await writer.InsertRowAsync(tableName, rowValues, cancellationToken).ConfigureAwait(false);
                        }
                        else
                        {
                            await writer.InsertRowsAsync(tableName, batchRows, cancellationToken).ConfigureAwait(false);
                        }

                        foreach ((IUpdateEntry itemEntry, List<IProperty>? itemServerProps) in batchEntries)
                        {
                            if (itemServerProps is { Count: > 0 })
                            {
                                await PopulateStoreGeneratedColumnsAsync(writer, tableName, itemEntry, itemServerProps, cancellationToken).ConfigureAwait(false);
                            }
                        }

                        totalAffected += batchRows.Count;
                        index = nextIdx;
                        continue;
                    }

                    List<(RowValues Values, IUpdateEntry Entry, IProperty Property, List<IProperty>? ServerProps)> autoIncBatch =
                        [(rowValues, entry, autoIncrementProperty, serverGeneratedProps)];
                    int nextAutoIdx = index + 1;
                    if (!HasSelfReferencingForeignKey(entry.EntityType))
                    {
                        while (nextAutoIdx < orderedEntries.Count
                            && orderedEntries[nextAutoIdx].EntityState == EntityState.Added
                            && string.Equals(JetEntityTypeMetadata.GetTableName(orderedEntries[nextAutoIdx].EntityType), tableName, StringComparison.OrdinalIgnoreCase)
                            && !HasSelfReferencingForeignKey(orderedEntries[nextAutoIdx].EntityType))
                        {
                            (RowValues nextRow, IProperty? nextAutoInc, List<IProperty>? nextServerProps) = PrepareInsertRow(orderedEntries[nextAutoIdx]);
                            if (nextAutoInc is null)
                            {
                                break;
                            }

                            autoIncBatch.Add((nextRow, orderedEntries[nextAutoIdx], nextAutoInc, nextServerProps));
                            nextAutoIdx++;
                        }
                    }

                    await ExecuteAutoIncrementBatchInsertAsync(
                        writer,
                        tableName,
                        autoIncBatch,
                        tdefPageByTable,
                        cancellationToken).ConfigureAwait(false);
                    totalAffected += autoIncBatch.Count;
                    index = nextAutoIdx;
                    continue;
                }
                else if (entry.EntityState == EntityState.Modified)
                {
                    string tableName = JetEntityTypeMetadata.GetTableName(entry.EntityType);
                    int nextIdx = index + 1;
                    while (nextIdx < orderedEntries.Count
                        && orderedEntries[nextIdx].EntityState == EntityState.Modified
                        && string.Equals(JetEntityTypeMetadata.GetTableName(orderedEntries[nextIdx].EntityType), tableName, StringComparison.OrdinalIgnoreCase))
                    {
                        nextIdx++;
                    }

                    if (nextIdx == index + 1)
                    {
                        totalAffected += await ExecuteUpdateAsync(writer, entry, cancellationToken).ConfigureAwait(false);
                        index++;
                        continue;
                    }

                    var batchModifiedEntries = new List<IUpdateEntry>(nextIdx - index);
                    for (int i = index; i < nextIdx; i++)
                    {
                        batchModifiedEntries.Add(orderedEntries[i]);
                    }

                    totalAffected += await ExecuteBatchUpdateAsync(writer, tableName, batchModifiedEntries, cancellationToken).ConfigureAwait(false);
                    index = nextIdx;
                    continue;
                }
                else if (entry.EntityState == EntityState.Deleted)
                {
                    totalAffected += await ExecuteDeleteAsync(writer, entry, cancellationToken).ConfigureAwait(false);
                }

                index++;
            }

            activeEntry = null;
            if (batchTransaction is not null)
            {
                await batchTransaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            }

            return totalAffected;
        }
        catch (Exception ex)
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

            if (ex is IJetException || (ex is ArgumentException && activeEntry is not null))
            {
                IReadOnlyList<IUpdateEntry> failedEntries = activeEntry is not null ? [activeEntry] : [.. orderedEntries];
                throw new DbUpdateException(ex.Message, ex, failedEntries);
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
        List<IUpdateEntry> candidates = [];
        foreach (IUpdateEntry entry in entries)
        {
            if (entry.EntityState is EntityState.Added or EntityState.Modified or EntityState.Deleted)
            {
                candidates.Add(entry);
            }
        }

        int count = candidates.Count;
        if (count <= 1)
        {
            return candidates;
        }

        var outgoing = new List<int>[count];
        var outgoingSets = new HashSet<int>[count];
        int[] indegree = new int[count];
        for (int i = 0; i < count; i++)
        {
            outgoing[i] = [];
            outgoingSets[i] = [];
        }

        void AddEdge(int from, int to)
        {
            if (from == to || !outgoingSets[from].Add(to))
            {
                return;
            }

            outgoing[from].Add(to);
            indegree[to]++;
        }

        Dictionary<(IEntityType, IEntityType), bool> dependencyCache = [];
        bool CachedDependsOn(IEntityType dependent, IEntityType principal)
        {
            if (dependencyCache.TryGetValue((dependent, principal), out bool cached))
            {
                return cached;
            }

            bool result = DependsOn(dependent, principal, []);
            dependencyCache[(dependent, principal)] = result;
            return result;
        }

        for (int i = 0; i < count; i++)
        {
            IUpdateEntry left = candidates[i];
            IEntityType leftRoot = left.EntityType.GetRootType();

            for (int j = 0; j < count; j++)
            {
                if (i == j)
                {
                    continue;
                }

                IUpdateEntry right = candidates[j];
                IEntityType rightRoot = right.EntityType.GetRootType();

                if (leftRoot == rightRoot)
                {
                    if (left.EntityState == EntityState.Deleted && right.EntityState == EntityState.Added)
                    {
                        AddEdge(i, j);
                    }
                    else if (left.EntityState == EntityState.Deleted && right.EntityState == EntityState.Modified)
                    {
                        if (ReferencesSameTypeEntry(right, left, useOriginalForChild: true))
                        {
                            AddEdge(j, i);
                        }
                        else
                        {
                            AddEdge(i, j);
                        }
                    }
                    else if (left.EntityState == right.EntityState
                        && left.EntityState is EntityState.Added or EntityState.Deleted
                        && ReferencesSameTypeEntry(left, right, useOriginalForChild: left.EntityState == EntityState.Deleted))
                    {
                        if (left.EntityState == EntityState.Added)
                        {
                            AddEdge(j, i);
                        }
                        else
                        {
                            AddEdge(i, j);
                        }
                    }

                    continue;
                }

                if (CachedDependsOn(leftRoot, rightRoot))
                {
                    if (right.EntityState == EntityState.Added && left.EntityState is EntityState.Added or EntityState.Modified)
                    {
                        AddEdge(j, i);
                    }

                    if (right.EntityState == EntityState.Deleted && left.EntityState is EntityState.Deleted or EntityState.Modified)
                    {
                        AddEdge(i, j);
                    }
                }
            }
        }

        List<IUpdateEntry> ordered = [];
        bool[] placed = new bool[count];
        for (int step = 0; step < count; step++)
        {
            int chosen = -1;
            int chosenPriority = int.MaxValue;
            for (int i = 0; i < count; i++)
            {
                if (placed[i] || indegree[i] != 0)
                {
                    continue;
                }

                int priority = GetStatePriority(candidates[i].EntityState);
                if (priority < chosenPriority)
                {
                    chosen = i;
                    chosenPriority = priority;
                }
            }

            if (chosen < 0)
            {
                for (int i = 0; i < count; i++)
                {
                    if (!placed[i])
                    {
                        chosen = i;
                        break;
                    }
                }
            }

            placed[chosen] = true;
            ordered.Add(candidates[chosen]);
            foreach (int neighbor in outgoing[chosen])
            {
                indegree[neighbor]--;
            }
        }

        return ordered;
    }

    private static int GetStatePriority(EntityState state)
    {
        if (state == EntityState.Deleted)
        {
            return 0;
        }

        if (state == EntityState.Added)
        {
            return 1;
        }

        if (state == EntityState.Modified)
        {
            return 2;
        }

        return 3;
    }

    private static bool ReferencesSameTypeEntry(
        IUpdateEntry childCandidate,
        IUpdateEntry parentCandidate,
        bool useOriginalForChild)
    {
        IEntityType rootType = childCandidate.EntityType.GetRootType();
        foreach (IForeignKey fk in childCandidate.EntityType.GetForeignKeys())
        {
            if (fk.PrincipalEntityType.GetRootType() != rootType
                || fk.Properties.Count == 0
                || fk.Properties.Count != fk.PrincipalKey.Properties.Count)
            {
                continue;
            }

            bool matchesAll = true;
            for (int i = 0; i < fk.Properties.Count; i++)
            {
                object? fkVal = useOriginalForChild
                    ? childCandidate.GetOriginalValue(fk.Properties[i])
                    : childCandidate.GetCurrentValue(fk.Properties[i]);
                object? pkVal = parentCandidate.EntityState == EntityState.Deleted
                    ? parentCandidate.GetOriginalValue(fk.PrincipalKey.Properties[i])
                    : parentCandidate.GetCurrentValue(fk.PrincipalKey.Properties[i]);
                if (fkVal is null || !Equals(fkVal, pkVal))
                {
                    matchesAll = false;
                    break;
                }
            }

            if (matchesAll)
            {
                return true;
            }
        }

        return false;
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

    private static (RowValues Values, IProperty? AutoIncrementProperty, List<IProperty>? ServerGeneratedProperties) PrepareInsertRow(IUpdateEntry entry)
    {
        IEntityType entityType = entry.EntityType;
        RowValues rowValues = [];
        HashSet<string> populatedColumns = [with(StringComparer.OrdinalIgnoreCase)];
        IProperty? autoIncrementProperty = null;
        List<IProperty>? serverGeneratedProperties = null;

        foreach (IProperty property in entityType.GetProperties())
        {
            string columnName = JetEntityTypeMetadata.GetColumnName(property);
            _ = populatedColumns.Add(columnName);
            if (!string.IsNullOrWhiteSpace(JetEntityTypeMetadata.GetCalculatedExpression(property)))
            {
                serverGeneratedProperties ??= [];
                serverGeneratedProperties.Add(property);
                continue;
            }

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
            if (property.ValueGenerated == ValueGenerated.OnAdd
                && !string.IsNullOrWhiteSpace(JetEntityTypeMetadata.GetDefaultValueExpression(property))
                && (entry.HasTemporaryValue(property) || IsClrDefaultOrSentinel(property, modelValue)))
            {
                serverGeneratedProperties ??= [];
                serverGeneratedProperties.Add(property);
                continue;
            }

            if (property.ValueGenerated == ValueGenerated.OnAdd
                && JetEntityTypeMetadata.TryGetDefaultValue(property, out object? defaultValue)
                && defaultValue is not (null or DBNull)
                && (entry.HasTemporaryValue(property) || IsClrDefaultOrSentinel(property, modelValue)))
            {
                entry.SetStoreGeneratedValue(property, defaultValue);
                modelValue = defaultValue;
            }

            rowValues[columnName] = JetEntityTypeMetadata.ConvertToProvider(property, modelValue);
        }

        foreach (IProperty hierarchyProp in JetEntityTypeMetadata.GetHierarchyProperties(entityType.GetRootType()))
        {
            if (JetEntityTypeMetadata.IsAutoIncrementProperty(hierarchyProp)
                || !string.IsNullOrWhiteSpace(JetEntityTypeMetadata.GetCalculatedExpression(hierarchyProp)))
            {
                continue;
            }

            string siblingColumnName = JetEntityTypeMetadata.GetColumnName(hierarchyProp);
            if (populatedColumns.Add(siblingColumnName))
            {
                rowValues[siblingColumnName] = null;
            }
        }

        return (rowValues, autoIncrementProperty, serverGeneratedProperties);
    }

    private static bool IsClrDefaultOrSentinel(IProperty property, object? value)
    {
        if (property.Sentinel is not null)
        {
            return Equals(value, property.Sentinel);
        }

        if (value is null)
        {
            return true;
        }

        Type clrType = property.ClrType;
        if (!clrType.IsValueType || Nullable.GetUnderlyingType(clrType) is not null)
        {
            return false;
        }

        return value switch
        {
            bool b => !b,
            byte u8 => u8 == 0,
            sbyte i8 => i8 == 0,
            short i16 => i16 == 0,
            ushort u16 => u16 == 0,
            int i32 => i32 == 0,
            uint u32 => u32 == 0,
            long i64 => i64 == 0L,
            ulong u64 => u64 == 0UL,
            float f32 => f32 == 0f,
            double f64 => f64 == 0d,
            decimal dec => dec == 0m,
            char c => c == '\0',
            DateTime dt => dt == default,
            DateTimeOffset dto => dto == default,
            DateOnly d => d == default,
            TimeOnly t => t == default,
            TimeSpan ts => ts == default,
            Guid g => g == Guid.Empty,
            Enum e => Convert.ToInt64(e, System.Globalization.CultureInfo.InvariantCulture) == 0L,
            _ => false,
        };
    }

    private static bool HasSelfReferencingForeignKey(IEntityType entityType)
    {
        IEntityType rootType = entityType.GetRootType();
        foreach (IForeignKey fk in entityType.GetForeignKeys())
        {
            if (fk.PrincipalEntityType.GetRootType() == rootType)
            {
                return true;
            }
        }

        return false;
    }

    private static async ValueTask ExecuteAutoIncrementBatchInsertAsync(
        AccessWriter writer,
        string tableName,
        List<(RowValues Values, IUpdateEntry Entry, IProperty Property, List<IProperty>? ServerProps)> batch,
        Dictionary<string, long> tdefPageByTable,
        CancellationToken cancellationToken)
    {
        if (batch.Count == 1)
        {
            await writer.InsertRowAsync(tableName, batch[0].Values, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            List<RowValues> rows = [with(batch.Count)];
            foreach ((RowValues values, _, _, _) in batch)
            {
                rows.Add(values);
            }

            await writer.InsertRowsAsync(tableName, rows, cancellationToken).ConfigureAwait(false);
        }

        if (!tdefPageByTable.TryGetValue(tableName, out long tdefPage))
        {
            ResolvedTable? resolved = await writer.Services.Snapshots.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            if (resolved is null)
            {
                return;
            }

            tdefPage = resolved.Entry.TDefPage;
            tdefPageByTable[tableName] = tdefPage;
        }

        using (PageLease lease = await writer.Services.SnapshotPages.ReadPageAsync(tdefPage, cancellationToken).ConfigureAwait(false))
        {
            int autoNumberOffset = writer.PageSize == 2048
                ? TDefHeaderLayout.Jet3.AutoNumber
                : TDefHeaderLayout.Jet4.AutoNumber;
            long highWater = JetTypeInfo.Ru32(lease.Buffer, autoNumberOffset);
            long firstAssigned = highWater - batch.Count + 1;
            for (int i = 0; i < batch.Count; i++)
            {
                (_, IUpdateEntry itemEntry, IProperty itemProperty, _) = batch[i];
                object? storeGenerated = JetEntityTypeMetadata.ConvertFromProvider(itemProperty, firstAssigned + i);
                itemEntry.SetStoreGeneratedValue(itemProperty, storeGenerated);
            }
        }

        for (int i = 0; i < batch.Count; i++)
        {
            (_, IUpdateEntry itemEntry, _, List<IProperty>? serverProps) = batch[i];
            if (serverProps is { Count: > 0 })
            {
                await PopulateStoreGeneratedColumnsAsync(writer, tableName, itemEntry, serverProps, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private static async ValueTask PopulateStoreGeneratedColumnsAsync(
        AccessWriter writer,
        string tableName,
        IUpdateEntry entry,
        List<IProperty> serverProperties,
        CancellationToken cancellationToken)
    {
        IKey? primaryKey = entry.EntityType.FindPrimaryKey();
        if (primaryKey is null || primaryKey.Properties.Count == 0)
        {
            return;
        }

        ResolvedTable? resolved = await writer.Services.Snapshots.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved is null)
        {
            return;
        }

        RowCriteria pkCriteria = [];
        foreach (IProperty pkProp in primaryKey.Properties)
        {
            string colName = JetEntityTypeMetadata.GetColumnName(pkProp);
            object? pkVal = JetEntityTypeMetadata.ConvertToProvider(pkProp, entry.GetCurrentValue(pkProp));
            pkCriteria.Add(ColumnPredicate.EqualTo(colName, pkVal));
        }

        List<LocatedRow>? matched = await writer.Services.Snapshots.TryReadIndexedRowsAsync(resolved, pkCriteria, cancellationToken).ConfigureAwait(false);
        if (matched is not { Count: > 0 })
        {
            return;
        }

        object?[] rowValues = matched[0].Values;
        foreach (IProperty prop in serverProperties)
        {
            string colName = JetEntityTypeMetadata.GetColumnName(prop);
            int colIdx = resolved.Definition.FindColumnIndex(colName);
            if ((uint)colIdx < (uint)rowValues.Length)
            {
                object? converted = JetEntityTypeMetadata.ConvertFromProvider(prop, rowValues[colIdx]);
                entry.SetStoreGeneratedValue(prop, converted);
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
        List<IProperty>? calculatedProperties = null;

        foreach (IProperty property in entityType.GetProperties())
        {
            if (!string.IsNullOrWhiteSpace(JetEntityTypeMetadata.GetCalculatedExpression(property)))
            {
                calculatedProperties ??= [];
                calculatedProperties.Add(property);
                continue;
            }

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

        if (calculatedProperties is { Count: > 0 })
        {
            await PopulateStoreGeneratedColumnsAsync(writer, tableName, entry, calculatedProperties, cancellationToken).ConfigureAwait(false);
        }

        return affected;
    }

    private static async ValueTask<int> ExecuteBatchUpdateAsync(
        AccessWriter writer,
        string tableName,
        List<IUpdateEntry> entries,
        CancellationToken cancellationToken)
    {
        var updates = new List<RowUpdate>(entries.Count);
        var updateEntries = new List<IUpdateEntry>(entries.Count);
        var calculatedByEntry = new List<(IUpdateEntry Entry, List<IProperty> Properties)>();
        int noOpCount = 0;

        foreach (IUpdateEntry entry in entries)
        {
            IEntityType entityType = entry.EntityType;
            RowCriteria criteria = BuildIdentityAndConcurrencyCriteria(entry);
            RowValues updatedValues = [];
            List<IProperty>? calculatedProperties = null;

            foreach (IProperty property in entityType.GetProperties())
            {
                if (!string.IsNullOrWhiteSpace(JetEntityTypeMetadata.GetCalculatedExpression(property)))
                {
                    calculatedProperties ??= [];
                    calculatedProperties.Add(property);
                    continue;
                }

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
                noOpCount++;
                continue;
            }

            updates.Add(new RowUpdate(criteria, updatedValues));
            updateEntries.Add(entry);
            if (calculatedProperties is { Count: > 0 })
            {
                calculatedByEntry.Add((entry, calculatedProperties));
            }
        }

        if (updates.Count == 0)
        {
            return noOpCount;
        }

        int affected = await writer.UpdateRowsAsync(tableName, updates, cancellationToken).ConfigureAwait(false);
        if (affected < updates.Count)
        {
            throw new DbUpdateConcurrencyException(
                $"The database operation was expected to affect {updates.Count} row(s), but actually affected {affected} row(s); data may have been modified or deleted since entities were loaded.",
                updateEntries);
        }

        foreach ((IUpdateEntry itemEntry, List<IProperty> calcProps) in calculatedByEntry)
        {
            await PopulateStoreGeneratedColumnsAsync(writer, tableName, itemEntry, calcProps, cancellationToken).ConfigureAwait(false);
        }

        return affected + noOpCount;
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
