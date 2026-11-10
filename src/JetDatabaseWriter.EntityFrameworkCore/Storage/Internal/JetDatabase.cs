namespace JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.EntityFrameworkCore.Metadata.Internal;
using JetDatabaseWriter.EntityFrameworkCore.Query.Internal;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.ValueDecoding;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;

/// <summary>
/// The main <see cref="Database"/> implementation for the <see cref="JetDatabaseWriter"/> Entity Framework Core provider,
/// translating change-tracker <see cref="IUpdateEntry"/> operations directly to <see cref="AccessWriter"/> calls
/// and compiling LINQ expression trees to <see cref="JetDatabaseWriter"/> table and index queries.
/// </summary>
public sealed partial class JetDatabase : Database
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

        (List<IUpdateEntry> primaryEntries, TableSplitEntryIndex tableSplitIndex) = PartitionAndOrderEntries(entries, this.Dependencies.UpdateAdapterFactory);
        if (primaryEntries.Count == 0 && tableSplitIndex.StandaloneUpdates.Count == 0)
        {
            return 0;
        }

        (AccessWriter writer, bool ownedByCaller) = await this.connection.RentWriterAsync(cancellationToken).ConfigureAwait(false);
        JetTransaction? batchTransaction = null;
        IUpdateEntry? activeEntry = null;
        try
        {
            int totalOperations = primaryEntries.Count;
            if (totalOperations > 1)
            {
                if (this.connection is JetClientConnection { ActiveTransactionNestingDepth: >= 5 })
                {
                    throw new InvalidOperationException(
                        "SaveChangesAsync cannot preserve batch atomicity at the current transaction nesting limit. Release a savepoint before saving multiple rows.");
                }

                batchTransaction = await writer.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            }

            primaryEntries = await OrderUniqueHandoffsAsync(primaryEntries, tableSplitIndex, writer, cancellationToken).ConfigureAwait(false);
            Dictionary<string, ResolvedTable> resolvedTableCache = [with(StringComparer.OrdinalIgnoreCase)];
            int totalAffected = 0;
            int index = 0;
            while (index < primaryEntries.Count)
            {
                cancellationToken.ThrowIfCancellationRequested();
                IUpdateEntry entry = primaryEntries[index];
                activeEntry = entry;

                if (entry.EntityState == EntityState.Added)
                {
                    (RowValues rowValues, IProperty? autoIncrementProperty, List<(IUpdateEntry TargetEntry, IProperty Property)>? serverGeneratedProps, List<IUpdateEntry>? attachedOwnedEntries) =
                        PrepareInsertRow(entry, tableSplitIndex, writer.Database.Format.Kind);
                    string tableName = JetEntityTypeMetadata.GetTableName(entry.EntityType);

                    if (autoIncrementProperty is null)
                    {
                        List<RowValues> batchRows = [rowValues];
                        List<(IUpdateEntry Entry, List<(IUpdateEntry TargetEntry, IProperty Property)>? ServerProps)> batchEntries =
                            [(entry, serverGeneratedProps)];
                        int nextIdx = index + 1;
                        while (nextIdx < primaryEntries.Count
                            && primaryEntries[nextIdx].EntityState == EntityState.Added
                            && string.Equals(JetEntityTypeMetadata.GetTableName(primaryEntries[nextIdx].EntityType), tableName, StringComparison.OrdinalIgnoreCase))
                        {
                            (RowValues nextRow, IProperty? nextAutoInc, List<(IUpdateEntry TargetEntry, IProperty Property)>? nextServerProps, _) =
                                PrepareInsertRow(primaryEntries[nextIdx], tableSplitIndex, writer.Database.Format.Kind);
                            if (nextAutoInc is not null)
                            {
                                break;
                            }

                            batchRows.Add(nextRow);
                            batchEntries.Add((primaryEntries[nextIdx], nextServerProps));
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

                        foreach ((IUpdateEntry itemEntry, List<(IUpdateEntry TargetEntry, IProperty Property)>? itemServerProps) in batchEntries)
                        {
                            if (itemServerProps is { Count: > 0 })
                            {
                                await PopulateStoreGeneratedColumnsAsync(writer, tableName, itemEntry, itemServerProps, resolvedTableCache, tableSplitIndex, cancellationToken).ConfigureAwait(false);
                            }
                        }

                        totalAffected += batchRows.Count;
                        index = nextIdx;
                        continue;
                    }

                    List<(RowValues Values, IUpdateEntry Entry, IProperty Property, List<(IUpdateEntry TargetEntry, IProperty Property)>? ServerProps, List<IUpdateEntry>? OwnedEntries)> autoIncBatch =
                        [(rowValues, entry, autoIncrementProperty, serverGeneratedProps, attachedOwnedEntries)];
                    int nextAutoIdx = index + 1;
                    if (!HasSelfReferencingForeignKey(entry.EntityType))
                    {
                        while (nextAutoIdx < primaryEntries.Count
                            && primaryEntries[nextAutoIdx].EntityState == EntityState.Added
                            && string.Equals(JetEntityTypeMetadata.GetTableName(primaryEntries[nextAutoIdx].EntityType), tableName, StringComparison.OrdinalIgnoreCase)
                            && !HasSelfReferencingForeignKey(primaryEntries[nextAutoIdx].EntityType))
                        {
                            (RowValues nextRow, IProperty? nextAutoInc, List<(IUpdateEntry TargetEntry, IProperty Property)>? nextServerProps, List<IUpdateEntry>? nextOwned) =
                                PrepareInsertRow(primaryEntries[nextAutoIdx], tableSplitIndex, writer.Database.Format.Kind);
                            if (nextAutoInc is null)
                            {
                                break;
                            }

                            autoIncBatch.Add((nextRow, primaryEntries[nextAutoIdx], nextAutoInc, nextServerProps, nextOwned));
                            nextAutoIdx++;
                        }
                    }

                    await ExecuteAutoIncrementBatchInsertAsync(
                        writer,
                        tableName,
                        autoIncBatch,
                        resolvedTableCache,
                        tableSplitIndex,
                        cancellationToken).ConfigureAwait(false);
                    totalAffected += autoIncBatch.Count;
                    index = nextAutoIdx;
                    continue;
                }
                else if (IsRowUpdate(entry, tableSplitIndex))
                {
                    string tableName = JetEntityTypeMetadata.GetTableName(entry.EntityType);
                    int nextIdx = index + 1;
                    while (nextIdx < primaryEntries.Count
                        && IsRowUpdate(primaryEntries[nextIdx], tableSplitIndex)
                        && string.Equals(JetEntityTypeMetadata.GetTableName(primaryEntries[nextIdx].EntityType), tableName, StringComparison.OrdinalIgnoreCase))
                    {
                        nextIdx++;
                    }

                    if (nextIdx == index + 1)
                    {
                        totalAffected += await ExecuteUpdateAsync(writer, entry, tableSplitIndex, resolvedTableCache, cancellationToken).ConfigureAwait(false);
                        index++;
                        continue;
                    }

                    var batchModifiedEntries = new List<IUpdateEntry>(nextIdx - index);
                    for (int i = index; i < nextIdx; i++)
                    {
                        batchModifiedEntries.Add(primaryEntries[i]);
                    }

                    totalAffected += await ExecuteBatchUpdateAsync(writer, tableName, batchModifiedEntries, tableSplitIndex, resolvedTableCache, cancellationToken).ConfigureAwait(false);
                    index = nextIdx;
                    continue;
                }
                else if (entry.EntityState == EntityState.Deleted)
                {
                    if (HasSelfReferencingForeignKey(entry.EntityType))
                    {
                        // A set-based delete does not preserve the sorted dependency order.
                        totalAffected += await ExecuteDeleteAsync(writer, entry, tableSplitIndex, cancellationToken).ConfigureAwait(false);
                        index++;
                        continue;
                    }

                    string tableName = JetEntityTypeMetadata.GetTableName(entry.EntityType);
                    int nextIdx = index + 1;
                    while (nextIdx < primaryEntries.Count
                        && primaryEntries[nextIdx].EntityState == EntityState.Deleted
                        && string.Equals(JetEntityTypeMetadata.GetTableName(primaryEntries[nextIdx].EntityType), tableName, StringComparison.OrdinalIgnoreCase))
                    {
                        nextIdx++;
                    }

                    if (nextIdx == index + 1)
                    {
                        totalAffected += await ExecuteDeleteAsync(writer, entry, tableSplitIndex, cancellationToken).ConfigureAwait(false);
                        index++;
                        continue;
                    }

                    var batchDeletedEntries = new List<IUpdateEntry>(nextIdx - index);
                    for (int i = index; i < nextIdx; i++)
                    {
                        batchDeletedEntries.Add(primaryEntries[i]);
                    }

                    totalAffected += await ExecuteBatchDeleteAsync(writer, tableName, batchDeletedEntries, tableSplitIndex, cancellationToken).ConfigureAwait(false);
                    index = nextIdx;
                    continue;
                }

                index++;
            }

            activeEntry = null;
            if (batchTransaction is not null)
            {
                await batchTransaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            }

            ApplySharedConcurrencyValues(tableSplitIndex);
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
                IReadOnlyList<IUpdateEntry> failedEntries = activeEntry is not null ? [activeEntry] : [.. primaryEntries];
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

    private static (List<IUpdateEntry> OrderedPrimaryEntries, TableSplitEntryIndex TableSplitIndex) PartitionAndOrderEntries(
        IList<IUpdateEntry> entries,
        IUpdateAdapterFactory updateAdapterFactory)
    {
        List<IUpdateEntry> primaryCandidates = [];
        List<IUpdateEntry> tableSplitCandidates = [];
        foreach (IUpdateEntry entry in entries)
        {
            if (entry.EntityState is not (EntityState.Added or EntityState.Modified or EntityState.Deleted))
            {
                continue;
            }

            if (JetEntityTypeMetadata.IsTableSplitOwned(entry.EntityType))
            {
                tableSplitCandidates.Add(entry);
            }
            else
            {
                primaryCandidates.Add(entry);
            }
        }

        List<IUpdateEntry> orderedPrimary = OrderEntriesLinear(primaryCandidates);
        TableSplitEntryIndex splitIndex = BuildTableSplitEntryIndex(orderedPrimary, tableSplitCandidates, updateAdapterFactory);
        orderedPrimary.AddRange(splitIndex.StandaloneUpdates.Select(static update => update.OperationEntry));
        return (orderedPrimary, splitIndex);
    }

    private static bool IsRowUpdate(IUpdateEntry entry, TableSplitEntryIndex splitIndex)
        => entry.EntityState == EntityState.Modified || splitIndex.StandaloneOperationEntries.Contains(entry);

    private static List<IUpdateEntry> OrderEntriesLinear(List<IUpdateEntry> candidates)
    {
        int count = candidates.Count;
        if (count <= 1)
        {
            return candidates;
        }

        List<IEntityType> distinctRootTypes = [];
        Dictionary<IEntityType, RootTypeEntryGroup> groupsByRoot = [];
        foreach (IUpdateEntry entry in candidates)
        {
            IEntityType rootType = entry.EntityType.GetRootType();
            if (!groupsByRoot.TryGetValue(rootType, out RootTypeEntryGroup? group))
            {
                group = new RootTypeEntryGroup(rootType);
                groupsByRoot[rootType] = group;
                distinctRootTypes.Add(rootType);
            }

            if (entry.EntityState == EntityState.Deleted)
            {
                group.Deleted.Add(entry);
            }
            else if (entry.EntityState == EntityState.Added)
            {
                group.Added.Add(entry);
            }
            else if (entry.EntityState == EntityState.Modified)
            {
                group.Modified.Add(entry);
            }
        }

        List<EntryBucket> buckets = [];
        Dictionary<(IEntityType RootType, BucketKind Kind), int> bucketIndexMap = [];

        void AddBucketIfNonEmpty(IEntityType rootType, BucketKind kind, List<IUpdateEntry> bucketEntries, int priority)
        {
            if (bucketEntries.Count == 0)
            {
                return;
            }

            int idx = buckets.Count;
            buckets.Add(new EntryBucket(rootType, kind, bucketEntries, priority));
            bucketIndexMap[(rootType, kind)] = idx;
        }

        foreach (IEntityType rootType in distinctRootTypes)
        {
            RootTypeEntryGroup group = groupsByRoot[rootType];
            List<IUpdateEntry> preDeleteModified = [];
            List<IUpdateEntry> regularModified = group.Modified;

            if (HasSelfReferencingForeignKey(rootType))
            {
                if (group.Deleted.Count > 0 && group.Modified.Count > 0)
                {
                    Dictionary<IKey, HashSet<StructuralKey>> deletedKeys = BuildPrincipalKeySets(rootType, group.Deleted, useOriginalValues: true);
                    regularModified = [];
                    foreach (IUpdateEntry modEntry in group.Modified)
                    {
                        if (ReferencesAnyKeyInSameType(modEntry, rootType, deletedKeys, useOriginalForChild: true))
                        {
                            preDeleteModified.Add(modEntry);
                        }
                        else
                        {
                            regularModified.Add(modEntry);
                        }
                    }
                }

                if (group.Deleted.Count > 1)
                {
                    group.Deleted = OrderSameTypeSelfReferencing(group.Deleted, rootType, parentBeforeChild: false);
                }

                if (group.Added.Count > 1)
                {
                    group.Added = OrderSameTypeSelfReferencing(group.Added, rootType, parentBeforeChild: true);
                }
            }

            AddBucketIfNonEmpty(rootType, BucketKind.PreDeleteModified, preDeleteModified, priority: 0);
            AddBucketIfNonEmpty(rootType, BucketKind.Deleted, group.Deleted, priority: 0);
            AddBucketIfNonEmpty(rootType, BucketKind.Added, group.Added, priority: 1);
            AddBucketIfNonEmpty(rootType, BucketKind.Modified, regularModified, priority: 2);
        }

        int bucketCount = buckets.Count;
        if (bucketCount <= 1)
        {
            return buckets.Count == 1 ? buckets[0].Entries : [];
        }

        var outgoing = new List<int>[bucketCount];
        var outgoingSets = new HashSet<int>[bucketCount];
        int[] indegree = new int[bucketCount];
        for (int i = 0; i < bucketCount; i++)
        {
            outgoing[i] = [];
            outgoingSets[i] = [];
        }

        void AddBucketEdge(int from, int to)
        {
            if (from < 0 || to < 0 || from == to || !outgoingSets[from].Add(to))
            {
                return;
            }

            outgoing[from].Add(to);
            indegree[to]++;
        }

        int FindBucket(IEntityType rootType, BucketKind kind)
            => bucketIndexMap.TryGetValue((rootType, kind), out int idx) ? idx : -1;

        foreach (IEntityType rootType in distinctRootTypes)
        {
            int preDel = FindBucket(rootType, BucketKind.PreDeleteModified);
            int del = FindBucket(rootType, BucketKind.Deleted);
            int add = FindBucket(rootType, BucketKind.Added);
            int mod = FindBucket(rootType, BucketKind.Modified);

            if (preDel >= 0 && del >= 0)
            {
                AddBucketEdge(preDel, del);
            }

            if (del >= 0 && add >= 0)
            {
                AddBucketEdge(del, add);
            }

            if (del >= 0 && mod >= 0)
            {
                AddBucketEdge(del, mod);
            }
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

        for (int i = 0; i < distinctRootTypes.Count; i++)
        {
            IEntityType leftRoot = distinctRootTypes[i];
            for (int j = 0; j < distinctRootTypes.Count; j++)
            {
                if (i == j)
                {
                    continue;
                }

                IEntityType rightRoot = distinctRootTypes[j];
                if (!CachedDependsOn(leftRoot, rightRoot))
                {
                    continue;
                }

                int rightAdd = FindBucket(rightRoot, BucketKind.Added);
                if (rightAdd >= 0)
                {
                    AddBucketEdge(rightAdd, FindBucket(leftRoot, BucketKind.Added));
                    AddBucketEdge(rightAdd, FindBucket(leftRoot, BucketKind.Modified));
                }

                int rightDel = FindBucket(rightRoot, BucketKind.Deleted);
                if (rightDel >= 0)
                {
                    AddBucketEdge(FindBucket(leftRoot, BucketKind.PreDeleteModified), rightDel);
                    AddBucketEdge(FindBucket(leftRoot, BucketKind.Deleted), rightDel);
                    AddBucketEdge(FindBucket(leftRoot, BucketKind.Modified), rightDel);
                }
            }
        }

        List<IUpdateEntry> ordered = [];
        bool[] placed = new bool[bucketCount];
        for (int step = 0; step < bucketCount; step++)
        {
            int chosen = -1;
            int chosenPriority = int.MaxValue;
            for (int i = 0; i < bucketCount; i++)
            {
                if (placed[i] || indegree[i] != 0)
                {
                    continue;
                }

                int priority = buckets[i].Priority;
                if (priority < chosenPriority)
                {
                    chosen = i;
                    chosenPriority = priority;
                }
            }

            if (chosen < 0)
            {
                for (int i = 0; i < bucketCount; i++)
                {
                    if (!placed[i])
                    {
                        chosen = i;
                        break;
                    }
                }
            }

            placed[chosen] = true;
            ordered.AddRange(buckets[chosen].Entries);
            foreach (int neighbor in outgoing[chosen])
            {
                indegree[neighbor]--;
            }
        }

        return ordered;
    }

    private static List<IUpdateEntry> OrderSameTypeSelfReferencing(
        List<IUpdateEntry> items,
        IEntityType rootType,
        bool parentBeforeChild)
    {
        int n = items.Count;
        bool useOriginal = !parentBeforeChild;
        Dictionary<IKey, Dictionary<StructuralKey, List<int>>> indicesByPrincipalKey = [];

        var outgoing = new List<int>[n];
        int[] indegree = new int[n];
        for (int i = 0; i < n; i++)
        {
            outgoing[i] = [];
        }

        for (int childIdx = 0; childIdx < n; childIdx++)
        {
            IUpdateEntry child = items[childIdx];
            foreach (IForeignKey fk in child.EntityType.GetForeignKeys())
            {
                if (fk.PrincipalEntityType.GetRootType() != rootType
                    || !TryExtractEntryKey(child, fk.Properties, useOriginal, out StructuralKey fkKey, fk.PrincipalKey.Properties))
                {
                    continue;
                }

                if (!indicesByPrincipalKey.TryGetValue(fk.PrincipalKey, out Dictionary<StructuralKey, List<int>>? principalIndex))
                {
                    principalIndex = BuildPrincipalKeyIndex(items, fk.PrincipalKey, useOriginal);
                    indicesByPrincipalKey[fk.PrincipalKey] = principalIndex;
                }

                if (!principalIndex.TryGetValue(fkKey, out List<int>? parentIndices))
                {
                    continue;
                }

                foreach (int parentIdx in parentIndices)
                {
                    if (parentIdx == childIdx)
                    {
                        continue;
                    }

                    int from = parentBeforeChild ? parentIdx : childIdx;
                    int to = parentBeforeChild ? childIdx : parentIdx;
                    if (!outgoing[from].Contains(to))
                    {
                        outgoing[from].Add(to);
                        indegree[to]++;
                    }
                }
            }
        }

        Queue<int> ready = new(n);
        for (int i = 0; i < n; i++)
        {
            if (indegree[i] == 0)
            {
                ready.Enqueue(i);
            }
        }

        List<IUpdateEntry> result = [];
        bool[] placed = new bool[n];
        while (ready.Count > 0)
        {
            int curr = ready.Dequeue();
            placed[curr] = true;
            result.Add(items[curr]);
            foreach (int next in outgoing[curr])
            {
                if (--indegree[next] == 0)
                {
                    ready.Enqueue(next);
                }
            }
        }

        if (result.Count < n)
        {
            for (int i = 0; i < n; i++)
            {
                if (!placed[i])
                {
                    result.Add(items[i]);
                }
            }
        }

        return result;
    }

    private static Dictionary<StructuralKey, List<int>> BuildPrincipalKeyIndex(
        List<IUpdateEntry> entries,
        IKey principalKey,
        bool useOriginalValues)
    {
        Dictionary<StructuralKey, List<int>> result = [];
        for (int i = 0; i < entries.Count; i++)
        {
            IUpdateEntry entry = entries[i];
            if (!principalKey.DeclaringEntityType.IsAssignableFrom(entry.EntityType)
                || !TryExtractEntryKey(entry, principalKey.Properties, useOriginalValues, out StructuralKey key))
            {
                continue;
            }

            if (!result.TryGetValue(key, out List<int>? indices))
            {
                indices = [];
                result[key] = indices;
            }

            indices.Add(i);
        }

        return result;
    }

    private static Dictionary<IKey, HashSet<StructuralKey>> BuildPrincipalKeySets(
        IEntityType rootType,
        List<IUpdateEntry> entries,
        bool useOriginalValues)
    {
        Dictionary<IKey, HashSet<StructuralKey>> result = [];
        foreach (IEntityType entityType in rootType.GetDerivedTypesInclusive())
        {
            foreach (IKey principalKey in entityType.GetKeys())
            {
                if (result.ContainsKey(principalKey))
                {
                    continue;
                }

                HashSet<StructuralKey> keys = [];
                foreach (IUpdateEntry entry in entries)
                {
                    if (principalKey.DeclaringEntityType.IsAssignableFrom(entry.EntityType)
                        && TryExtractEntryKey(entry, principalKey.Properties, useOriginalValues, out StructuralKey key))
                    {
                        _ = keys.Add(key);
                    }
                }

                result[principalKey] = keys;
            }
        }

        return result;
    }

    private static bool ReferencesAnyKeyInSameType(
        IUpdateEntry childCandidate,
        IEntityType rootType,
        Dictionary<IKey, HashSet<StructuralKey>> parentKeys,
        bool useOriginalForChild)
    {
        foreach (IForeignKey fk in childCandidate.EntityType.GetForeignKeys())
        {
            if (fk.PrincipalEntityType.GetRootType() == rootType
                && parentKeys.TryGetValue(fk.PrincipalKey, out HashSet<StructuralKey>? keys)
                && TryExtractEntryKey(childCandidate, fk.Properties, useOriginalForChild, out StructuralKey fkKey, fk.PrincipalKey.Properties)
                && keys.Contains(fkKey))
            {
                return true;
            }
        }

        return false;
    }

    private static bool TryExtractEntryKey(
        IUpdateEntry entry,
        IReadOnlyList<IProperty> properties,
        bool useOriginalValues,
        out StructuralKey key,
        IReadOnlyList<IProperty>? comparisonProperties = null)
    {
        object?[] values = new object?[properties.Count];
        for (int i = 0; i < properties.Count; i++)
        {
            object? val = useOriginalValues
                ? entry.GetOriginalValue(properties[i])
                : entry.GetCurrentValue(properties[i]);
            if (val is null or DBNull)
            {
                key = default;
                return false;
            }

            values[i] = val;
        }

        key = new StructuralKey(values, comparisonProperties ?? properties);
        return true;
    }

    private static TableSplitEntryIndex BuildTableSplitEntryIndex(
        List<IUpdateEntry> primaryEntries,
        List<IUpdateEntry> tableSplitEntries,
        IUpdateAdapterFactory updateAdapterFactory)
    {
        TableSplitEntryIndex index = new();
        IUpdateAdapter? updateAdapter = null;

        foreach (IUpdateEntry owner in primaryEntries)
        {
            if (JetEntityTypeMetadata.GetTableSplitOwnedEntityTypes(owner.EntityType.GetRootType()).Count > 0)
            {
                updateAdapter ??= updateAdapterFactory.Create();
                _ = GetTableSplitRowEntries(owner, updateAdapter, index);
            }
        }

        if (tableSplitEntries.Count == 0)
        {
            return index;
        }

        Dictionary<(IEntityType OwnerRoot, StructuralKey Key), IUpdateEntry> addedOwners = [];
        Dictionary<(IEntityType OwnerRoot, StructuralKey Key), IUpdateEntry> modifiedOwners = [];
        HashSet<(IEntityType OwnerRoot, StructuralKey Key)> deletedOwners = [];

        foreach (IUpdateEntry owner in primaryEntries)
        {
            IEntityType root = owner.EntityType.GetRootType();
            IKey? pk = root.FindPrimaryKey();
            if (pk is null)
            {
                continue;
            }

            if (owner.EntityState == EntityState.Added
                && TryExtractEntryKey(owner, pk.Properties, useOriginalValues: false, out StructuralKey addKey))
            {
                addedOwners[(root, addKey)] = owner;
            }
            else if (owner.EntityState == EntityState.Modified
                && TryExtractEntryKey(owner, pk.Properties, useOriginalValues: true, out StructuralKey modKey))
            {
                modifiedOwners[(root, modKey)] = owner;
            }
            else if (owner.EntityState == EntityState.Deleted
                && TryExtractEntryKey(owner, pk.Properties, useOriginalValues: true, out StructuralKey delKey))
            {
                _ = deletedOwners.Add((root, delKey));
            }
        }

        tableSplitEntries.Sort(static (a, b) => GetEntityStateOrderForSplit(a.EntityState).CompareTo(GetEntityStateOrderForSplit(b.EntityState)));
        Dictionary<(IEntityType OwnerRoot, StructuralKey Key), StandaloneTableSplitUpdate> standaloneByOwner = [];

        foreach (IUpdateEntry owned in tableSplitEntries)
        {
            updateAdapter ??= updateAdapterFactory.Create();
            List<IUpdateEntry> rowEntries = GetTableSplitRowEntries(owned, updateAdapter, index);
            IEntityType ownerRoot = JetEntityTypeMetadata.GetTableOwnerEntityType(owned.EntityType);
            IReadOnlyList<IProperty>? ownershipProps = GetRootOwnershipProperties(owned.EntityType);
            if (ownershipProps is null || ownershipProps.Count == 0)
            {
                continue;
            }

            bool useOriginal = owned.EntityState == EntityState.Deleted;
            if (!TryExtractEntryKey(owned, ownershipProps, useOriginal, out StructuralKey ownerKey, ownerRoot.FindPrimaryKey()!.Properties))
            {
                continue;
            }

            if (deletedOwners.Contains((ownerRoot, ownerKey)))
            {
                continue;
            }

            if (owned.EntityState == EntityState.Added && addedOwners.TryGetValue((ownerRoot, ownerKey), out IUpdateEntry? addedOwner))
            {
                if (!index.ByOwnerEntry.TryGetValue(addedOwner, out List<IUpdateEntry>? list))
                {
                    list = [];
                    index.ByOwnerEntry[addedOwner] = list;
                }

                list.Add(owned);
                continue;
            }

            if (modifiedOwners.TryGetValue((ownerRoot, ownerKey), out IUpdateEntry? modifiedOwner))
            {
                if (!index.ByOwnerEntry.TryGetValue(modifiedOwner, out List<IUpdateEntry>? list))
                {
                    list = [];
                    index.ByOwnerEntry[modifiedOwner] = list;
                }

                list.Add(owned);
                continue;
            }

            if (!standaloneByOwner.TryGetValue((ownerRoot, ownerKey), out StandaloneTableSplitUpdate? standalone))
            {
                standalone = new StandaloneTableSplitUpdate(owned, rowEntries[0]);
                standaloneByOwner[(ownerRoot, ownerKey)] = standalone;
                index.StandaloneUpdates.Add(standalone);
                _ = index.StandaloneOperationEntries.Add(standalone.OperationEntry);
            }
        }

        foreach (StandaloneTableSplitUpdate standalone in index.StandaloneUpdates)
        {
            index.ByOwnerEntry[standalone.OperationEntry] = index.RowEntries[standalone.RepresentativeEntry]
                .Where(entry => !ReferenceEquals(entry, standalone.OperationEntry)
                    && entry.EntityState is EntityState.Added or EntityState.Modified or EntityState.Deleted).ToList();
        }

        return index;
    }

    private static List<IUpdateEntry> GetTableSplitRowEntries(IUpdateEntry entry, IUpdateAdapter updateAdapter, TableSplitEntryIndex index)
    {
        if (index.RowEntries.TryGetValue(entry, out List<IUpdateEntry>? existing))
        {
            return existing;
        }

        IUpdateEntry owner = entry;
        while (JetEntityTypeMetadata.IsTableSplitOwned(owner.EntityType)
            && owner.EntityType.FindOwnership() is { } ownership
            && updateAdapter.FindPrincipal(owner, ownership) is { } principal)
        {
            owner = principal;
        }

        if (!index.RowEntries.TryGetValue(owner, out List<IUpdateEntry>? rowEntries))
        {
            rowEntries = [owner];
            index.RowEntries[owner] = rowEntries;
            for (int i = 0; i < rowEntries.Count; i++)
            {
                IUpdateEntry current = rowEntries[i];
                foreach (IForeignKey foreignKey in current.EntityType.GetReferencingForeignKeys())
                {
                    if (!foreignKey.IsOwnership || !JetEntityTypeMetadata.IsTableSplitOwned(foreignKey.DeclaringEntityType))
                    {
                        continue;
                    }

                    foreach (IUpdateEntry dependent in updateAdapter.GetDependents(current, foreignKey))
                    {
                        if (index.RowEntries.TryAdd(dependent, rowEntries))
                        {
                            rowEntries.Add(dependent);
                        }
                    }
                }
            }
        }

        if (index.RowEntries.TryAdd(entry, rowEntries))
        {
            rowEntries.Add(entry);
        }

        return rowEntries;
    }

    private static int GetEntityStateOrderForSplit(EntityState state)
    {
        if (state == EntityState.Deleted)
        {
            return 0;
        }

        if (state == EntityState.Modified)
        {
            return 1;
        }

        return state == EntityState.Added ? 2 : 3;
    }

    private static IReadOnlyList<IProperty>? GetRootOwnershipProperties(IEntityType ownedType)
    {
        IForeignKey? ownership = ownedType.FindOwnership();
        return ownership?.Properties;
    }

    private static void ApplyTableSplitUpdateValues(IUpdateEntry ownedEntry, RowValues targetValues, List<IUpdateEntry> rowEntries)
    {
        foreach (IProperty prop in ownedEntry.EntityType.GetFlattenedProperties())
        {
            if (prop.IsPrimaryKey() || IsOwnershipProperty(prop) || !string.IsNullOrWhiteSpace(JetEntityTypeMetadata.GetCalculatedExpression(prop)))
            {
                continue;
            }

            string colName = JetEntityTypeMetadata.GetColumnName(prop);
            if (ownedEntry.EntityState == EntityState.Deleted)
            {
                if (!IsSharedWithSurvivingEntry(ownedEntry, colName, rowEntries))
                {
                    targetValues[colName] = null;
                }
            }
            else if (ownedEntry.EntityState == EntityState.Added || ownedEntry.IsModified(prop))
            {
                object? val = JetEntityTypeMetadata.GetPropertyCurrentValue(ownedEntry, prop);
                targetValues[colName] = JetEntityTypeMetadata.ConvertToProvider(prop, val);
            }
        }
    }

    private static bool IsSharedWithSurvivingEntry(IUpdateEntry removedEntry, string columnName, List<IUpdateEntry> rowEntries)
    {
        foreach (IUpdateEntry rowEntry in rowEntries)
        {
            if (ReferenceEquals(rowEntry, removedEntry) || rowEntry.EntityState == EntityState.Deleted)
            {
                continue;
            }

            foreach (IProperty property in rowEntry.EntityType.GetFlattenedProperties())
            {
                if (string.Equals(JetEntityTypeMetadata.GetColumnName(property), columnName, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }
        }

        return false;
    }

    private static void AppendConcurrencyCriteria(RowCriteria criteria, List<IUpdateEntry> rowEntries, IUpdateEntry? excludedEntry = null)
    {
        foreach (IUpdateEntry rowEntry in rowEntries)
        {
            if (ReferenceEquals(rowEntry, excludedEntry) || rowEntry.EntityState == EntityState.Added)
            {
                continue;
            }

            foreach (IProperty property in rowEntry.EntityType.GetFlattenedProperties())
            {
                if (property.IsConcurrencyToken && !property.IsPrimaryKey())
                {
                    string columnName = JetEntityTypeMetadata.GetColumnName(property);
                    object? originalValue = JetEntityTypeMetadata.ConvertToProvider(property, rowEntry.GetOriginalValue(property));
                    criteria.Add(ColumnPredicate.EqualTo(columnName, originalValue));
                }
            }
        }
    }

    private static void RecordSharedConcurrencyValues(TableSplitEntryIndex index, IUpdateEntry entry, RowValues updatedValues)
    {
        if (!index.RowEntries.TryGetValue(entry, out List<IUpdateEntry>? rowEntries))
        {
            return;
        }

        foreach (IUpdateEntry rowEntry in rowEntries)
        {
            if (rowEntry.EntityState == EntityState.Deleted)
            {
                continue;
            }

            foreach (IProperty property in rowEntry.EntityType.GetFlattenedProperties())
            {
                if (property.IsConcurrencyToken && updatedValues.TryGetValue(JetEntityTypeMetadata.GetColumnName(property), out object? providerValue))
                {
                    index.SharedConcurrencyValues[(rowEntry, property)] = JetEntityTypeMetadata.ConvertFromProvider(property, providerValue);
                }
            }
        }
    }

    private static List<(IUpdateEntry TargetEntry, IProperty Property)>? CollectCalculatedConcurrencyProperties(
        TableSplitEntryIndex index,
        IUpdateEntry entry)
    {
        if (!index.RowEntries.TryGetValue(entry, out List<IUpdateEntry>? rowEntries))
        {
            return null;
        }

        List<(IUpdateEntry TargetEntry, IProperty Property)>? properties = null;
        foreach (IUpdateEntry rowEntry in rowEntries)
        {
            if (rowEntry.EntityState == EntityState.Deleted)
            {
                continue;
            }

            foreach (IProperty property in rowEntry.EntityType.GetFlattenedProperties())
            {
                if (property.IsConcurrencyToken && !string.IsNullOrWhiteSpace(JetEntityTypeMetadata.GetCalculatedExpression(property)))
                {
                    properties ??= [];
                    properties.Add((rowEntry, property));
                }
            }
        }

        return properties;
    }

    private static void ApplySharedConcurrencyValues(TableSplitEntryIndex index)
    {
        foreach (KeyValuePair<(IUpdateEntry Entry, IProperty Property), object?> pair in index.SharedConcurrencyValues)
        {
            (IUpdateEntry entry, IProperty property) = pair.Key;
#pragma warning disable EF1001 // Shared columns must update tracked values without marking unchanged row peers Modified.
            entry.ToEntityEntry().GetInfrastructure().SetProperty(property, pair.Value, isMaterialization: false, setModified: false);
#pragma warning restore EF1001
            if (entry.EntityState == EntityState.Unchanged)
            {
                entry.SetOriginalValue(property, pair.Value);
            }
        }
    }

    private static bool DependsOn(IEntityType dependentCandidate, IEntityType principalCandidate, HashSet<IEntityType> visited)
    {
        if (!visited.Add(dependentCandidate))
        {
            return false;
        }

        foreach (IForeignKey fk in dependentCandidate.GetForeignKeys())
        {
            IEntityType principal = JetEntityTypeMetadata.GetTableOwnerEntityType(fk.PrincipalEntityType);
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

    private static (RowValues Values, IProperty? AutoIncrementProperty, List<(IUpdateEntry TargetEntry, IProperty Property)>? ServerGeneratedProperties, List<IUpdateEntry>? AttachedOwnedEntries) PrepareInsertRow(
        IUpdateEntry entry,
        TableSplitEntryIndex tableSplitIndex,
        DatabaseFormat format)
    {
        IEntityType entityType = entry.EntityType;
        RowValues rowValues = [];
        HashSet<string> populatedColumns = [with(StringComparer.OrdinalIgnoreCase)];
        IProperty? autoIncrementProperty = null;
        List<(IUpdateEntry TargetEntry, IProperty Property)>? serverGeneratedProperties = null;

        void PopulateFromEntry(IUpdateEntry currentEntry, bool isOwnerEntry)
        {
            foreach (IProperty property in currentEntry.EntityType.GetFlattenedProperties())
            {
                if (!isOwnerEntry && (property.IsPrimaryKey() || IsOwnershipProperty(property)))
                {
                    continue;
                }

                string columnName = JetEntityTypeMetadata.GetColumnName(property);
                _ = populatedColumns.Add(columnName);
                if (!string.IsNullOrWhiteSpace(JetEntityTypeMetadata.GetCalculatedExpression(property)))
                {
                    serverGeneratedProperties ??= [];
                    serverGeneratedProperties.Add((currentEntry, property));
                    continue;
                }

                if (isOwnerEntry && JetEntityTypeMetadata.IsAutoIncrementProperty(property))
                {
                    object? currentVal = currentEntry.GetCurrentValue(property);
                    bool hasTempOrDefault = currentEntry.HasTemporaryValue(property)
                        || IsClrDefaultOrSentinel(property, currentVal);
                    if (hasTempOrDefault)
                    {
                        autoIncrementProperty = property;
                        continue;
                    }
                }

                if (property.ValueGenerated == ValueGenerated.OnAdd && property.ClrType == typeof(Guid)
                    && !(JetEntityTypeMetadata.TryGetDefaultValue(property, out object? literalGuidDefault) && literalGuidDefault is not null)
                    && string.IsNullOrWhiteSpace(JetEntityTypeMetadata.GetDefaultValueExpression(property)))
                {
                    object? currentGuid = JetEntityTypeMetadata.GetPropertyCurrentValue(currentEntry, property);
                    if (currentEntry.HasTemporaryValue(property) || IsClrDefaultOrSentinel(property, currentGuid))
                    {
                        var generatedGuid = Guid.NewGuid();
                        currentEntry.SetStoreGeneratedValue(property, generatedGuid);
                        rowValues[columnName] = generatedGuid;
                        continue;
                    }
                }

                object? modelValue = JetEntityTypeMetadata.GetPropertyCurrentValue(currentEntry, property);
                if (property.ValueGenerated == ValueGenerated.OnAdd
                    && !string.IsNullOrWhiteSpace(JetEntityTypeMetadata.GetDefaultValueExpression(property))
                    && (currentEntry.HasTemporaryValue(property) || IsClrDefaultOrSentinel(property, modelValue)))
                {
                    serverGeneratedProperties ??= [];
                    serverGeneratedProperties.Add((currentEntry, property));
                    continue;
                }

                if (property.ValueGenerated == ValueGenerated.OnAdd
                    && JetEntityTypeMetadata.TryGetDefaultValue(property, out object? defaultValue)
                    && defaultValue is not (null or DBNull)
                    && (currentEntry.HasTemporaryValue(property) || IsClrDefaultOrSentinel(property, modelValue)))
                {
                    object? providerDefault = JetLiteralDefaultValidator.NormalizeProviderValue(property, JetEntityTypeMetadata.ConvertToProvider(property, defaultValue), format);
                    currentEntry.SetStoreGeneratedValue(property, JetEntityTypeMetadata.ConvertFromProvider(property, providerDefault));
                    rowValues[columnName] = providerDefault;
                    continue;
                }

                rowValues[columnName] = JetEntityTypeMetadata.ConvertToProvider(property, modelValue);
            }
        }

        PopulateFromEntry(entry, isOwnerEntry: true);

        List<IUpdateEntry>? attachedOwned = null;
        if (tableSplitIndex.ByOwnerEntry.TryGetValue(entry, out List<IUpdateEntry>? splitEntries))
        {
            attachedOwned = splitEntries;
            foreach (IUpdateEntry splitEntry in splitEntries)
            {
                if (splitEntry.EntityState is EntityState.Added or EntityState.Modified)
                {
                    PopulateFromEntry(splitEntry, isOwnerEntry: false);
                }
            }
        }

        IEntityType rootType = entityType.GetRootType();
        foreach (IProperty hierarchyProp in JetEntityTypeMetadata.GetHierarchyProperties(rootType))
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

        foreach (IEntityType splitOwnedType in JetEntityTypeMetadata.GetTableSplitOwnedEntityTypes(rootType))
        {
            foreach (IProperty splitProp in splitOwnedType.GetFlattenedProperties())
            {
                if (splitProp.IsPrimaryKey()
                    || IsOwnershipProperty(splitProp)
                    || !string.IsNullOrWhiteSpace(JetEntityTypeMetadata.GetCalculatedExpression(splitProp)))
                {
                    continue;
                }

                string splitColumnName = JetEntityTypeMetadata.GetColumnName(splitProp);
                if (populatedColumns.Add(splitColumnName))
                {
                    rowValues[splitColumnName] = null;
                }
            }
        }

        return (rowValues, autoIncrementProperty, serverGeneratedProperties, attachedOwned);
    }

    private static bool IsOwnershipProperty(IProperty property)
    {
        foreach (IForeignKey foreignKey in property.GetContainingForeignKeys())
        {
            if (foreignKey.IsOwnership)
            {
                return true;
            }
        }

        return false;
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
        foreach (IEntityType hierarchyType in rootType.GetDerivedTypesInclusive())
        {
            foreach (IForeignKey fk in hierarchyType.GetForeignKeys())
            {
                if (fk.PrincipalEntityType.GetRootType() == rootType)
                {
                    return true;
                }
            }
        }

        return false;
    }

    private static async ValueTask ExecuteAutoIncrementBatchInsertAsync(
        AccessWriter writer,
        string tableName,
        List<(RowValues Values, IUpdateEntry Entry, IProperty Property, List<(IUpdateEntry TargetEntry, IProperty Property)>? ServerProps, List<IUpdateEntry>? OwnedEntries)> batch,
        Dictionary<string, ResolvedTable> resolvedTableCache,
        TableSplitEntryIndex tableSplitIndex,
        CancellationToken cancellationToken)
    {
        if (batch.Count == 1)
        {
            await writer.InsertRowAsync(tableName, batch[0].Values, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            List<RowValues> rows = [with(batch.Count)];
            foreach ((RowValues values, _, _, _, _) in batch)
            {
                rows.Add(values);
            }

            await writer.InsertRowsAsync(tableName, rows, cancellationToken).ConfigureAwait(false);
        }

        if (!resolvedTableCache.TryGetValue(tableName, out ResolvedTable? resolved))
        {
            resolved = await writer.Services.Snapshots.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            if (resolved is null)
            {
                return;
            }

            resolvedTableCache[tableName] = resolved;
        }

        using (PageLease lease = await writer.Services.SnapshotPages.ReadPageAsync(resolved.Entry.TDefPage, cancellationToken).ConfigureAwait(false))
        {
            int autoNumberOffset = writer.PageSize == 2048
                ? TDefHeaderLayout.Jet3.AutoNumber
                : TDefHeaderLayout.Jet4.AutoNumber;
            long highWater = JetTypeInfo.Ru32(lease.Buffer, autoNumberOffset);
            long firstAssigned = highWater - batch.Count + 1;
            for (int i = 0; i < batch.Count; i++)
            {
                (_, IUpdateEntry itemEntry, IProperty itemProperty, _, List<IUpdateEntry>? ownedEntries) = batch[i];
                object? storeGenerated = JetEntityTypeMetadata.ConvertFromProvider(itemProperty, firstAssigned + i);
                itemEntry.SetStoreGeneratedValue(itemProperty, storeGenerated);

                if (ownedEntries is not null)
                {
                    foreach (IUpdateEntry owned in ownedEntries)
                    {
                        if (owned.EntityType.FindOwnership() is { Properties.Count: 1 } ownership)
                        {
                            IProperty ownedKeyProp = ownership.Properties[0];
                            if (owned.HasTemporaryValue(ownedKeyProp))
                            {
                                object? convertedOwnedKey = JetEntityTypeMetadata.ConvertFromProvider(ownedKeyProp, firstAssigned + i);
                                owned.SetStoreGeneratedValue(ownedKeyProp, convertedOwnedKey);
                            }
                        }
                    }
                }
            }
        }

        for (int i = 0; i < batch.Count; i++)
        {
            (_, IUpdateEntry itemEntry, _, List<(IUpdateEntry TargetEntry, IProperty Property)>? serverProps, _) = batch[i];
            if (serverProps is { Count: > 0 })
            {
                await PopulateStoreGeneratedColumnsAsync(writer, tableName, itemEntry, serverProps, resolvedTableCache, tableSplitIndex, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private static async ValueTask PopulateStoreGeneratedColumnsAsync(
        AccessWriter writer,
        string tableName,
        IUpdateEntry entry,
        List<(IUpdateEntry TargetEntry, IProperty Property)> serverProperties,
        Dictionary<string, ResolvedTable> resolvedTableCache,
        TableSplitEntryIndex tableSplitIndex,
        CancellationToken cancellationToken)
    {
        IKey? primaryKey = entry.EntityType.FindPrimaryKey();
        if (primaryKey is null || primaryKey.Properties.Count == 0)
        {
            return;
        }

        if (!resolvedTableCache.TryGetValue(tableName, out ResolvedTable? resolved))
        {
            resolved = await writer.Services.Snapshots.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            if (resolved is null)
            {
                return;
            }

            resolvedTableCache[tableName] = resolved;
        }

        RowCriteria pkCriteria = [];
        foreach (IProperty pkProp in primaryKey.Properties)
        {
            string colName = JetEntityTypeMetadata.GetColumnName(pkProp);
            object? pkVal = JetEntityTypeMetadata.ConvertToProvider(pkProp, entry.GetCurrentValue(pkProp));
            pkCriteria.Add(ColumnPredicate.EqualTo(colName, pkVal));
        }

        List<LocatedRow>? matched = await writer.Services.Snapshots.TryReadIndexedRowsAsync(resolved, pkCriteria, cancellationToken).ConfigureAwait(false);
        matched ??= await writer.Services.Snapshots.ReadMatchingRowsAsync(
            resolved,
            RowCriteriaEvaluator.Compile(pkCriteria, resolved.Definition, tableName, nameof(pkCriteria)),
            cancellationToken).ConfigureAwait(false);
        if (matched is not { Count: > 0 })
        {
            return;
        }

        object?[] rowValues = matched[0].Values;
        RowValues? generatedConcurrencyValues = null;
        foreach ((IUpdateEntry targetEntry, IProperty prop) in serverProperties)
        {
            string colName = JetEntityTypeMetadata.GetColumnName(prop);
            int colIdx = resolved.Definition.FindColumnIndex(colName);
            if ((uint)colIdx < (uint)rowValues.Length)
            {
                if (prop.IsConcurrencyToken && tableSplitIndex.RowEntries.ContainsKey(targetEntry))
                {
                    generatedConcurrencyValues ??= [];
                    generatedConcurrencyValues[colName] = rowValues[colIdx];
                    continue;
                }

                object? converted = JetEntityTypeMetadata.ConvertFromProvider(prop, rowValues[colIdx]);
                targetEntry.SetStoreGeneratedValue(prop, converted);
            }
        }

        if (generatedConcurrencyValues is not null)
        {
            RecordSharedConcurrencyValues(tableSplitIndex, entry, generatedConcurrencyValues);
        }
    }

    private static async ValueTask<int> ExecuteUpdateAsync(
        AccessWriter writer,
        IUpdateEntry entry,
        TableSplitEntryIndex tableSplitIndex,
        Dictionary<string, ResolvedTable> resolvedTableCache,
        CancellationToken cancellationToken)
    {
        IEntityType entityType = entry.EntityType;
        string tableName = JetEntityTypeMetadata.GetTableName(entityType);
        RowCriteria criteria = BuildIdentityAndConcurrencyCriteria(entry, tableSplitIndex);
        RowValues updatedValues = [];
        List<(IUpdateEntry TargetEntry, IProperty Property)>? calculatedProperties = CollectCalculatedConcurrencyProperties(tableSplitIndex, entry);

        foreach (IProperty property in entityType.GetFlattenedProperties())
        {
            if (!string.IsNullOrWhiteSpace(JetEntityTypeMetadata.GetCalculatedExpression(property)))
            {
                if (!property.IsConcurrencyToken || !tableSplitIndex.RowEntries.ContainsKey(entry))
                {
                    calculatedProperties ??= [];
                    calculatedProperties.Add((entry, property));
                }

                continue;
            }

            if (property.IsPrimaryKey() || !entry.IsModified(property))
            {
                continue;
            }

            string columnName = JetEntityTypeMetadata.GetColumnName(property);
            object? currentValue = JetEntityTypeMetadata.GetPropertyCurrentValue(entry, property);
            updatedValues[columnName] = JetEntityTypeMetadata.ConvertToProvider(property, currentValue);
        }

        if (tableSplitIndex.ByOwnerEntry.TryGetValue(entry, out List<IUpdateEntry>? splitEntries))
        {
            foreach (IUpdateEntry splitEntry in splitEntries)
            {
                ApplyTableSplitUpdateValues(splitEntry, updatedValues, tableSplitIndex.RowEntries[entry]);
            }
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
            await PopulateStoreGeneratedColumnsAsync(writer, tableName, entry, calculatedProperties, resolvedTableCache, tableSplitIndex, cancellationToken).ConfigureAwait(false);
        }

        RecordSharedConcurrencyValues(tableSplitIndex, entry, updatedValues);
        return affected;
    }

    private static async ValueTask<int> ExecuteBatchUpdateAsync(
        AccessWriter writer,
        string tableName,
        List<IUpdateEntry> entries,
        TableSplitEntryIndex tableSplitIndex,
        Dictionary<string, ResolvedTable> resolvedTableCache,
        CancellationToken cancellationToken)
    {
        var updates = new List<RowUpdate>(entries.Count);
        var updateEntries = new List<IUpdateEntry>(entries.Count);
        var calculatedByEntry = new List<(IUpdateEntry Entry, List<(IUpdateEntry TargetEntry, IProperty Property)> Properties)>();
        int noOpCount = 0;

        foreach (IUpdateEntry entry in entries)
        {
            IEntityType entityType = entry.EntityType;
            RowCriteria criteria = BuildIdentityAndConcurrencyCriteria(entry, tableSplitIndex);
            RowValues updatedValues = [];
            List<(IUpdateEntry TargetEntry, IProperty Property)>? calculatedProperties = CollectCalculatedConcurrencyProperties(tableSplitIndex, entry);

            foreach (IProperty property in entityType.GetFlattenedProperties())
            {
                if (!string.IsNullOrWhiteSpace(JetEntityTypeMetadata.GetCalculatedExpression(property)))
                {
                    if (!property.IsConcurrencyToken || !tableSplitIndex.RowEntries.ContainsKey(entry))
                    {
                        calculatedProperties ??= [];
                        calculatedProperties.Add((entry, property));
                    }

                    continue;
                }

                if (property.IsPrimaryKey() || !entry.IsModified(property))
                {
                    continue;
                }

                string columnName = JetEntityTypeMetadata.GetColumnName(property);
                object? currentValue = JetEntityTypeMetadata.GetPropertyCurrentValue(entry, property);
                updatedValues[columnName] = JetEntityTypeMetadata.ConvertToProvider(property, currentValue);
            }

            if (tableSplitIndex.ByOwnerEntry.TryGetValue(entry, out List<IUpdateEntry>? splitEntries))
            {
                foreach (IUpdateEntry splitEntry in splitEntries)
                {
                    ApplyTableSplitUpdateValues(splitEntry, updatedValues, tableSplitIndex.RowEntries[entry]);
                }
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

        foreach ((IUpdateEntry itemEntry, List<(IUpdateEntry TargetEntry, IProperty Property)> calcProps) in calculatedByEntry)
        {
            await PopulateStoreGeneratedColumnsAsync(writer, tableName, itemEntry, calcProps, resolvedTableCache, tableSplitIndex, cancellationToken).ConfigureAwait(false);
        }

        for (int i = 0; i < updates.Count; i++)
        {
            RecordSharedConcurrencyValues(tableSplitIndex, updateEntries[i], updates[i].UpdatedValues);
        }

        return affected + noOpCount;
    }

    private static async ValueTask<int> ExecuteDeleteAsync(
        AccessWriter writer,
        IUpdateEntry entry,
        TableSplitEntryIndex tableSplitIndex,
        CancellationToken cancellationToken)
    {
        IEntityType entityType = entry.EntityType;
        string tableName = JetEntityTypeMetadata.GetTableName(entityType);
        RowCriteria criteria = BuildIdentityAndConcurrencyCriteria(entry, tableSplitIndex);

        int affected = await writer.DeleteRowsAsync(tableName, criteria, cancellationToken).ConfigureAwait(false);
        if (affected == 0)
        {
            throw new DbUpdateConcurrencyException(
                "The database operation was expected to affect 1 row(s), but actually affected 0 row(s); data may have been modified or deleted since entities were loaded.",
                [entry]);
        }

        return affected;
    }

    private static async ValueTask<int> ExecuteBatchDeleteAsync(
        AccessWriter writer,
        string tableName,
        List<IUpdateEntry> entries,
        TableSplitEntryIndex tableSplitIndex,
        CancellationToken cancellationToken)
    {
        var criteriaList = new List<RowCriteria>(entries.Count);
        foreach (IUpdateEntry entry in entries)
        {
            criteriaList.Add(BuildIdentityAndConcurrencyCriteria(entry, tableSplitIndex));
        }

        int affected = await writer.DeleteRowsAsync(tableName, criteriaList, cancellationToken).ConfigureAwait(false);
        if (affected < entries.Count)
        {
            throw new DbUpdateConcurrencyException(
                $"The database operation was expected to affect {entries.Count} row(s), but actually affected {affected} row(s); data may have been modified or deleted since entities were loaded.",
                entries);
        }

        return affected;
    }

    private static RowCriteria BuildIdentityAndConcurrencyCriteria(IUpdateEntry entry, TableSplitEntryIndex tableSplitIndex)
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

        foreach (IProperty property in entityType.GetFlattenedProperties())
        {
            if (property.IsConcurrencyToken && !property.IsPrimaryKey())
            {
                string columnName = JetEntityTypeMetadata.GetColumnName(property);
                object? originalToken = JetEntityTypeMetadata.ConvertToProvider(property, entry.GetOriginalValue(property));
                criteria.Add(ColumnPredicate.EqualTo(columnName, originalToken));
            }
        }

        if (tableSplitIndex.RowEntries.TryGetValue(entry, out List<IUpdateEntry>? rowEntries))
        {
            AppendConcurrencyCriteria(criteria, rowEntries, excludedEntry: entry);
        }

        return criteria;
    }

    private enum BucketKind
    {
        PreDeleteModified = 0,
        Deleted = 1,
        Added = 2,
        Modified = 3,
    }

    private sealed class RootTypeEntryGroup(IEntityType rootType)
    {
        public IEntityType RootType { get; } = rootType;

        public List<IUpdateEntry> Deleted { get; set; } = [];

        public List<IUpdateEntry> Added { get; set; } = [];

        public List<IUpdateEntry> Modified { get; } = [];
    }

    private sealed class EntryBucket(IEntityType rootType, BucketKind kind, List<IUpdateEntry> entries, int priority)
    {
        public IEntityType RootType { get; } = rootType;

        public BucketKind Kind { get; } = kind;

        public List<IUpdateEntry> Entries { get; } = entries;

        public int Priority { get; } = priority;
    }

    private sealed class TableSplitEntryIndex
    {
        public Dictionary<IUpdateEntry, List<IUpdateEntry>> ByOwnerEntry { get; } = [with(ReferenceEqualityComparer.Instance)];

        public Dictionary<IUpdateEntry, List<IUpdateEntry>> RowEntries { get; } = [with(ReferenceEqualityComparer.Instance)];

        public Dictionary<(IUpdateEntry Entry, IProperty Property), object?> SharedConcurrencyValues { get; } = [];

        public List<StandaloneTableSplitUpdate> StandaloneUpdates { get; } = [];

        public HashSet<IUpdateEntry> StandaloneOperationEntries { get; } = [with(ReferenceEqualityComparer.Instance)];
    }

    private sealed class StandaloneTableSplitUpdate(IUpdateEntry representativeEntry, IUpdateEntry operationEntry)
    {
        public IUpdateEntry RepresentativeEntry { get; } = representativeEntry;

        public IUpdateEntry OperationEntry { get; } = operationEntry;
    }

    private readonly struct StructuralKey(object?[] values, IReadOnlyList<IProperty> properties) : IEquatable<StructuralKey>
    {
        public object?[] Values { get; } = values;

        public bool Equals(StructuralKey other)
        {
            if (this.Values.Length != other.Values.Length)
            {
                return false;
            }

            for (int i = 0; i < this.Values.Length; i++)
            {
                if (!properties[i].GetKeyValueComparer().Equals(this.Values[i], other.Values[i]))
                {
                    return false;
                }
            }

            return true;
        }

        public override bool Equals(object? obj)
            => obj is StructuralKey other && this.Equals(other);

        public override int GetHashCode()
        {
            HashCode hash = default;
            for (int i = 0; i < this.Values.Length; i++)
            {
                hash.Add(properties[i].GetKeyValueComparer().GetHashCode(this.Values[i]));
            }

            return hash.ToHashCode();
        }
    }
}
