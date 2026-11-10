namespace JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.EntityFrameworkCore.Infrastructure.Internal;
using JetDatabaseWriter.EntityFrameworkCore.Metadata.Internal;
using JetDatabaseWriter.EntityFrameworkCore.Query.Internal;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tables;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;

/// <summary>
/// Default implementation of <see cref="IJetClientConnection"/> coordinating <see cref="AccessReader"/> and <see cref="AccessWriter"/>.
/// </summary>
public sealed class JetClientConnection : IJetClientConnection
{
    private static readonly List<(WeakReference<JetClientConnection> Connection, SemaphoreSlim LifecycleGate)> LiveConnections = [];
    private static readonly Lock LiveConnectionsGate = new();
    private static readonly AccessReaderOptions DefaultReaderOptions = new();
    private static readonly AccessWriterOptions DefaultWriterOptions = new() { UseTransactionalWrites = true };
    private static readonly ConcurrentDictionary<Type, object> BoxedDefaultValues = [];

    private readonly SemaphoreSlim connectionGate = new(1, 1);
#pragma warning disable CA2213 // Shared by every live connection to this target; disposing one connection must preserve peers and pending acquisitions.
    private readonly SemaphoreSlim lifecycleGate;
#pragma warning restore CA2213
    private readonly List<(string Name, JetTransaction Scope)> savepoints = [];
    private readonly JetOptionsExtension optionsExtension;
    private Stream? resolvedStream;
    private AccessReader? activeReader;
    private AccessWriter? cachedWriter;
    private bool writerRented;
    private bool disposed;
    private bool disposalCompleted;

    /// <summary>
    /// Initializes a new instance of the <see cref="JetClientConnection"/> class.
    /// </summary>
    /// <param name="options">The context options containing <see cref="JetOptionsExtension"/>.</param>
    public JetClientConnection(IDbContextOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        this.optionsExtension = options.FindExtension<JetOptionsExtension>()
            ?? throw new InvalidOperationException("JetOptionsExtension was not found in IDbContextOptions.");
        if (!string.IsNullOrWhiteSpace(this.optionsExtension.DatabasePath))
        {
            this.DatabasePath = Path.GetFullPath(this.optionsExtension.DatabasePath);
        }

        if (this.DatabasePath is null)
        {
            _ = this.GetOrCreateStream();
        }

        this.lifecycleGate = RegisterConnection(this);
    }

    /// <inheritdoc/>
    public string? DatabasePath { get; }

    /// <inheritdoc/>
    public DatabaseFormat Format => this.optionsExtension.Format;

    /// <inheritdoc/>
    public AccessWriter? ActiveWriter { get; private set; }

    /// <inheritdoc/>
    public JetTransaction? ActiveTransaction { get; private set; }

    internal int ActiveTransactionNestingDepth => this.ActiveTransaction is null ? 0 : 1 + this.savepoints.Count;

    /// <inheritdoc/>
    public async ValueTask<AccessReader> GetReaderAsync(CancellationToken cancellationToken = default)
    {
        await this.EnterLifecycleAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await this.ClosePeerConnectionsAsync(closeReaders: false, cancellationToken).ConfigureAwait(false);
            return await this.GetReaderCoreAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            this.ExitLifecycle();
        }
    }

    /// <inheritdoc/>
    public async ValueTask<bool> HasUserTablesAsync(CancellationToken cancellationToken = default)
    {
        await this.EnterLifecycleAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (this.ActiveWriter is { } writer)
            {
                return (await writer.Services.Catalog.GetUserTablesAsync(cancellationToken).ConfigureAwait(false)).Count > 0;
            }

            await this.ClosePeerConnectionsAsync(closeReaders: false, cancellationToken).ConfigureAwait(false);
            AccessReader reader = await this.GetReaderCoreAsync(cancellationToken).ConfigureAwait(false);
            return (await reader.ListTablesAsync(cancellationToken).ConfigureAwait(false)).Count > 0;
        }
        finally
        {
            this.ExitLifecycle();
        }
    }

    /// <inheritdoc/>
    public async ValueTask CloseReaderAsync()
    {
        if (this.disposed)
        {
            return;
        }

        await this.lifecycleGate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (this.disposed)
            {
                return;
            }

            await this.connectionGate.WaitAsync().ConfigureAwait(false);
            try
            {
                await this.CloseReaderCoreAsync().ConfigureAwait(false);
            }
            finally
            {
                this.connectionGate.Release();
            }
        }
        finally
        {
            this.lifecycleGate.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask<(AccessWriter Writer, bool OwnedByCaller)> RentWriterAsync(CancellationToken cancellationToken = default)
    {
        await this.EnterLifecycleAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (this.ActiveWriter is null && this.cachedWriter is null)
            {
                await this.CloseAllReadersForSameTargetAsync(cancellationToken).ConfigureAwait(false);
                this.cachedWriter = await this.OpenWriterCoreAsync(cancellationToken).ConfigureAwait(false);
            }

            this.writerRented = true;
            return (this.ActiveWriter ?? this.cachedWriter!, false);
        }
        catch
        {
            this.connectionGate.Release();
            throw;
        }
        finally
        {
            this.lifecycleGate.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask ReleaseWriterAsync(AccessWriter writer, bool ownedByCaller)
    {
        ArgumentNullException.ThrowIfNull(writer);
        if (!this.writerRented || (!ReferenceEquals(writer, this.ActiveWriter) && !ReferenceEquals(writer, this.cachedWriter)))
        {
            throw new InvalidOperationException("The writer does not have an outstanding rental on this connection.");
        }

        try
        {
            if (ownedByCaller)
            {
                this.cachedWriter = null;
                await writer.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            this.writerRented = false;
            this.connectionGate.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask<AccessWriter> CreateDatabaseWriterAsync(CancellationToken cancellationToken = default)
    {
        await this.EnterLifecycleAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (this.ActiveTransaction is not null)
            {
                throw new InvalidOperationException("Cannot recreate a database while a transaction is active.");
            }

            await this.CloseAllReadersForSameTargetAsync(cancellationToken).ConfigureAwait(false);
            AccessWriterOptions writerOptions = this.BuildWriterOptions();
            if (!string.IsNullOrWhiteSpace(this.DatabasePath))
            {
                string? directory = Path.GetDirectoryName(this.DatabasePath);
                if (!string.IsNullOrEmpty(directory))
                {
                    _ = Directory.CreateDirectory(directory);
                }

                this.cachedWriter = await AccessWriter.CreateDatabaseAsync(
                    this.DatabasePath,
                    this.optionsExtension.Format,
                    writerOptions,
                    cancellationToken).ConfigureAwait(false);
            }
            else
            {
                Stream stream = this.GetOrCreateStream();
                this.cachedWriter = await AccessWriter.CreateDatabaseAsync(
                    stream,
                    this.optionsExtension.Format,
                    writerOptions,
                    leaveOpen: true,
                    cancellationToken).ConfigureAwait(false);
            }

            this.writerRented = true;
            return this.cachedWriter;
        }
        catch
        {
            this.connectionGate.Release();
            throw;
        }
        finally
        {
            this.lifecycleGate.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask<JetTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        await this.EnterLifecycleAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await this.BeginTransactionCoreAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            this.ExitLifecycle();
        }
    }

    /// <inheritdoc/>
    public async ValueTask CommitTransactionAsync(CancellationToken cancellationToken = default)
    {
        await this.EnterLifecycleAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await this.CommitTransactionCoreAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            this.ExitLifecycle();
        }
    }

    /// <inheritdoc/>
    public async ValueTask RollbackTransactionAsync(CancellationToken cancellationToken = default)
    {
        await this.EnterLifecycleAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await this.RollbackTransactionCoreAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            this.ExitLifecycle();
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeTransactionAsync()
    {
        await this.EnterLifecycleAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            await this.DisposeTransactionCoreAsync().ConfigureAwait(false);
        }
        finally
        {
            this.ExitLifecycle();
        }
    }

    /// <inheritdoc/>
    public async ValueTask CreateSavepointAsync(string name, CancellationToken cancellationToken = default)
    {
        await this.EnterLifecycleAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await this.CreateSavepointCoreAsync(name, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            this.ExitLifecycle();
        }
    }

    /// <inheritdoc/>
    public async ValueTask RollbackToSavepointAsync(string name, CancellationToken cancellationToken = default)
    {
        await this.EnterLifecycleAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await this.RollbackToSavepointCoreAsync(name, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            this.ExitLifecycle();
        }
    }

    /// <inheritdoc/>
    public async ValueTask ReleaseSavepointAsync(string name, CancellationToken cancellationToken = default)
    {
        await this.EnterLifecycleAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await this.ReleaseSavepointCoreAsync(name, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            this.ExitLifecycle();
        }
    }

    /// <inheritdoc/>
    public async ValueTask<bool> CanConnectAsync(CancellationToken cancellationToken = default)
    {
        await this.EnterLifecycleAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (this.ActiveWriter is not null || this.cachedWriter is not null || this.activeReader is not null)
            {
                return true;
            }

            if (!string.IsNullOrWhiteSpace(this.DatabasePath))
            {
                if (!File.Exists(this.DatabasePath) || new FileInfo(this.DatabasePath).Length < 2048)
                {
                    return false;
                }
            }
            else if (this.GetOrCreateStream().Length < 2048)
            {
                return false;
            }

            await this.ClosePeerConnectionsAsync(closeReaders: false, cancellationToken).ConfigureAwait(false);
            _ = await this.GetReaderCoreAsync(cancellationToken).ConfigureAwait(false);
            await this.CloseReaderCoreAsync().ConfigureAwait(false);
            return true;
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return false;
        }
        finally
        {
            this.ExitLifecycle();
        }
    }

    /// <inheritdoc/>
    public async ValueTask<bool> DeleteDatabaseAsync(CancellationToken cancellationToken = default)
    {
        await this.EnterLifecycleAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await this.DeleteDatabaseCoreAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            this.ExitLifecycle();
        }
    }

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<ValueBuffer>> ReadTableValueBuffersAsync(
        IEntityType entityType,
        IReadOnlyList<JetIndexSeekCondition>? seekConditions,
        IReadOnlyDictionary<string, object?> parameters,
        CancellationToken cancellationToken = default)
        => this.ReadTableValueBuffersAsync(
            entityType,
            seekConditions,
            parameters,
            requiredProperties: null,
            maxRows: null,
            orderedProperties: null,
            rowFilter: null,
            cancellationToken);

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<ValueBuffer>> ReadTableValueBuffersAsync(
        IEntityType entityType,
        IReadOnlyList<JetIndexSeekCondition>? seekConditions,
        IReadOnlyDictionary<string, object?> parameters,
        IReadOnlySet<IProperty>? requiredProperties,
        CancellationToken cancellationToken = default)
        => this.ReadTableValueBuffersAsync(
            entityType,
            seekConditions,
            parameters,
            requiredProperties,
            maxRows: null,
            orderedProperties: null,
            rowFilter: null,
            cancellationToken);

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<ValueBuffer>> ReadTableValueBuffersAsync(
        IEntityType entityType,
        IReadOnlyList<JetIndexSeekCondition>? seekConditions,
        IReadOnlyDictionary<string, object?> parameters,
        IReadOnlySet<IProperty>? requiredProperties,
        int? maxRows,
        CancellationToken cancellationToken = default)
        => this.ReadTableValueBuffersAsync(
            entityType,
            seekConditions,
            parameters,
            requiredProperties,
            maxRows,
            orderedProperties: null,
            rowFilter: null,
            cancellationToken);

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyList<ValueBuffer>> ReadTableValueBuffersAsync(
        IEntityType entityType,
        IReadOnlyList<JetIndexSeekCondition>? seekConditions,
        IReadOnlyDictionary<string, object?> parameters,
        IReadOnlySet<IProperty>? requiredProperties,
        int? maxRows,
        IReadOnlyList<JetOrdering>? orderedProperties,
        Func<ValueBuffer, bool>? rowFilter,
        CancellationToken cancellationToken = default)
    {
        List<ValueBuffer> rows = [];
        await foreach (ValueBuffer row in this.StreamTableValueBuffersAsync(
            entityType, seekConditions, parameters, requiredProperties, maxRows, orderedProperties, rowFilter, cancellationToken).ConfigureAwait(false))
        {
            rows.Add(row);
        }

        return rows;
    }

    /// <inheritdoc/>
    public IAsyncEnumerable<ValueBuffer> StreamTableValueBuffersAsync(
        IEntityType entityType,
        IReadOnlyList<JetIndexSeekCondition>? seekConditions,
        IReadOnlyDictionary<string, object?> parameters,
        IReadOnlySet<IProperty>? requiredProperties,
        int? maxRows,
        IReadOnlyList<JetOrdering>? orderedProperties,
        Func<ValueBuffer, bool>? rowFilter,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entityType);
        ArgumentNullException.ThrowIfNull(parameters);
        ObjectDisposedException.ThrowIf(this.disposed, this);
        return this.StreamTableValueBuffersCoreAsync(entityType, seekConditions, parameters, requiredProperties, maxRows, orderedProperties, rowFilter, cancellationToken);
    }

    /// <inheritdoc/>
    public async ValueTask ScanTableRowsAsync(
        IEntityType entityType,
        IReadOnlyList<JetIndexSeekCondition>? seekConditions,
        IReadOnlyDictionary<string, object?> parameters,
        IReadOnlySet<IProperty>? requiredProperties,
        Func<ValueBuffer, bool> visitor,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entityType);
        ArgumentNullException.ThrowIfNull(parameters);
        ArgumentNullException.ThrowIfNull(visitor);
        ObjectDisposedException.ThrowIf(this.disposed, this);
        bool ownsGate = !this.writerRented;
        if (ownsGate)
        {
            await this.EnterLifecycleAsync(cancellationToken).ConfigureAwait(false);
        }

        bool lifecycleHeld = ownsGate;
        try
        {
            IEntityType rootType = entityType.GetRootType();
            string tableName = JetEntityTypeMetadata.GetTableName(rootType);
            IReadOnlyList<IProperty> properties = JetEntityTypeMetadata.GetHierarchyProperties(rootType);
            AccessWriter? writer = this.ActiveWriter ?? this.cachedWriter;
            if (writer is not null)
            {
                ResolvedTable resolved = await writer.Services.Catalog.ResolveRequiredTableAsync(tableName, cancellationToken).ConfigureAwait(false);
                int?[] columnMap = BuildPropertyColumnMapFromTableDef(properties, resolved.Definition);
                bool[] wanted = BuildWantedColumnsMask(rootType, properties, columnMap, resolved.Definition.Columns.Count, requiredProperties);
                ValueBufferFactory factory = new(rootType, properties, columnMap, wanted, reuseBuffer: true);
                if (lifecycleHeld && this.DatabasePath is not null)
                {
                    this.lifecycleGate.Release();
                    lifecycleHeld = false;
                }

                bool Visit(object?[] row) => !factory.TryCreate(row, out ValueBuffer buffer) || visitor(buffer);
                if (seekConditions is { Count: > 0 }
                    && await JetIndexSeekPlanner.TryReadFromActiveWriterIndexAsync(writer, resolved, seekConditions, orderedProperties: null, parameters, wanted, (row, _) => Visit(row), cancellationToken).ConfigureAwait(false))
                {
                    return;
                }

                bool wrapHyperlinks = resolved.Definition.HasHyperlinkColumns && TableReader.HasWantedHyperlinkColumn(resolved.Definition.ClrTypes, wanted);
                await writer.Services.Snapshots.ForEachTypedRowAsync(
                    resolved.Entry.TDefPage,
                    resolved.Definition,
                    wanted,
                    (_, row, _) =>
                    {
                        if (wrapHyperlinks)
                        {
                            TableReader.WrapHyperlinkColumns(row, resolved.Definition.ClrTypes);
                        }

                        return new ValueTask<bool>(Visit(row));
                    },
                    cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await this.ClosePeerConnectionsAsync(closeReaders: false, cancellationToken).ConfigureAwait(false);
                AccessReader reader = await this.GetReaderCoreAsync(cancellationToken).ConfigureAwait(false);
                ResolvedTable? resolved = await reader.Services.Tables.ResolveLocalTableAsync(tableName, cancellationToken).ConfigureAwait(false);
                int?[] columnMap;
                bool[]? wanted = null;
                IAsyncEnumerable<object?[]> rows;
                if (resolved is not null)
                {
                    columnMap = BuildPropertyColumnMapFromTableDef(properties, resolved.Definition);
                    wanted = BuildWantedColumnsMask(rootType, properties, columnMap, resolved.Definition.Columns.Count, requiredProperties);
                    rows = (seekConditions is { Count: > 0 }
                        ? await JetIndexSeekPlanner.TryBuildIndexQueryAsync(reader, resolved, seekConditions, parameters, wanted, cancellationToken).ConfigureAwait(false)
                        : null) ?? reader.Services.Tables.Rows(tableName, wanted, progress: null, cancellationToken);
                }
                else
                {
                    columnMap = BuildPropertyColumnMap(properties, await reader.GetColumnMetadataAsync(tableName, cancellationToken).ConfigureAwait(false));
                    rows = reader.Rows(tableName, progress: null, cancellationToken);
                }

                if (lifecycleHeld && this.DatabasePath is not null)
                {
                    this.lifecycleGate.Release();
                    lifecycleHeld = false;
                }

                ValueBufferFactory factory = new(rootType, properties, columnMap, wanted, reuseBuffer: true);
                await foreach (object?[] row in rows.WithCancellation(cancellationToken).ConfigureAwait(false))
                {
                    if (factory.TryCreate(row, out ValueBuffer buffer) && !visitor(buffer))
                    {
                        break;
                    }
                }
            }
        }
        finally
        {
            if (lifecycleHeld)
            {
                this.lifecycleGate.Release();
            }

            if (ownsGate)
            {
                this.connectionGate.Release();
            }
        }
    }

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyList<ValueBuffer>> ReadTableValueBuffersByKeysAsync(
        IEntityType entityType,
        IReadOnlyList<IProperty> keyProperties,
        IReadOnlyList<object?[]> keyTuples,
        IReadOnlySet<IProperty>? requiredProperties = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entityType);
        ArgumentNullException.ThrowIfNull(keyProperties);
        ArgumentNullException.ThrowIfNull(keyTuples);
        ObjectDisposedException.ThrowIf(this.disposed, this);
        if (keyProperties.Count == 0 || keyTuples.Count == 0)
        {
            return [];
        }

        bool ownsGate = !this.writerRented;
        if (ownsGate)
        {
            await this.EnterLifecycleAsync(cancellationToken).ConfigureAwait(false);
        }

        bool lifecycleHeld = ownsGate;
        try
        {
            IEntityType rootType = entityType.GetRootType();
            string tableName = JetEntityTypeMetadata.GetTableName(rootType);
            IReadOnlyList<IProperty> properties = JetEntityTypeMetadata.GetHierarchyProperties(rootType);
            AccessWriter? writer = this.ActiveWriter ?? this.cachedWriter;
            AccessReader? reader = null;
            if (writer is null)
            {
                await this.ClosePeerConnectionsAsync(closeReaders: false, cancellationToken).ConfigureAwait(false);
                reader = await this.GetReaderCoreAsync(cancellationToken).ConfigureAwait(false);
            }

            if (lifecycleHeld && this.DatabasePath is not null)
            {
                this.lifecycleGate.Release();
                lifecycleHeld = false;
            }

            return writer is not null
                ? await ReadByKeysFromActiveWriterAsync(writer, tableName, rootType, properties, keyProperties, keyTuples, requiredProperties, cancellationToken).ConfigureAwait(false)
                : await ReadByKeysFromReaderAsync(reader!, tableName, rootType, properties, keyProperties, keyTuples, requiredProperties, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (lifecycleHeld)
            {
                this.lifecycleGate.Release();
            }

            if (ownsGate)
            {
                this.connectionGate.Release();
            }
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        await this.lifecycleGate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (this.disposed)
            {
                return;
            }

            await this.connectionGate.WaitAsync().ConfigureAwait(false);
            try
            {
                if (this.disposed)
                {
                    return;
                }

                this.disposed = true;
                try
                {
                    await this.DisposeTransactionCoreAsync().ConfigureAwait(false);
                    await this.CloseReaderCoreAsync().ConfigureAwait(false);
                    Stream? stream = this.optionsExtension.DatabaseStream ?? this.resolvedStream;
                    if (stream is not null && !this.optionsExtension.LeaveStreamOpen)
                    {
                        await stream.DisposeAsync().ConfigureAwait(false);
                        this.resolvedStream = null;
                    }
                }
                finally
                {
                    this.disposalCompleted = true;
                    UnregisterConnection(this);
                }
            }
            finally
            {
                this.connectionGate.Release();
                this.connectionGate.Dispose();
            }
        }
        finally
        {
            this.lifecycleGate.Release();
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (this.disposed)
        {
            return;
        }

#pragma warning disable RS0030, MA0042 // IDisposable.Dispose is a required synchronous callback boundary.
        this.DisposeAsync().AsTask().GetAwaiter().GetResult();
#pragma warning restore RS0030, MA0042
    }

    internal AccessReaderOptions BuildReaderOptions()
    {
        AccessReaderOptions template = this.optionsExtension.ReaderOptions ?? DefaultReaderOptions;
        ReadOnlyMemory<char> password = !string.IsNullOrEmpty(this.optionsExtension.Password)
            ? this.optionsExtension.Password.AsMemory()
            : template.Password;
        return template.WithConnectionSettings(password, this.DatabasePath is not null && template.UseLockFile);
    }

    internal AccessWriterOptions BuildWriterOptions()
    {
        AccessWriterOptions template = this.optionsExtension.WriterOptions ?? DefaultWriterOptions;
        ReadOnlyMemory<char> password = !string.IsNullOrEmpty(this.optionsExtension.Password)
            ? this.optionsExtension.Password.AsMemory()
            : template.Password;
        return template.WithConnectionSettings(password, this.DatabasePath is not null && template.UseLockFile);
    }

    private static SemaphoreSlim RegisterConnection(JetClientConnection connection)
    {
        lock (LiveConnectionsGate)
        {
            SemaphoreSlim? lifecycleGate = null;
            Stream? stream = connection.optionsExtension.DatabaseStream ?? connection.resolvedStream;
            for (int i = LiveConnections.Count - 1; i >= 0; i--)
            {
                if (!LiveConnections[i].Connection.TryGetTarget(out JetClientConnection? existing) || existing.disposalCompleted)
                {
                    LiveConnections.RemoveAt(i);
                    continue;
                }

                if ((connection.DatabasePath is not null
                    && existing.DatabasePath is not null
                    && string.Equals(connection.DatabasePath, existing.DatabasePath, StringComparison.Ordinal))
                    || (stream is not null && ReferenceEquals(stream, existing.optionsExtension.DatabaseStream ?? existing.resolvedStream)))
                {
                    lifecycleGate = LiveConnections[i].LifecycleGate;
                }
            }

            lifecycleGate ??= new SemaphoreSlim(1, 1);
            LiveConnections.Add((new WeakReference<JetClientConnection>(connection), lifecycleGate));
            return lifecycleGate;
        }
    }

    private static void UnregisterConnection(JetClientConnection connection)
    {
        lock (LiveConnectionsGate)
        {
            for (int i = LiveConnections.Count - 1; i >= 0; i--)
            {
                if (!LiveConnections[i].Connection.TryGetTarget(out JetClientConnection? existing)
                    || existing.disposalCompleted
                    || ReferenceEquals(existing, connection))
                {
                    LiveConnections.RemoveAt(i);
                }
            }
        }
    }

    private static int ComputeValueBufferLength(IReadOnlyList<IProperty> properties)
    {
        int length = properties.Count;
        for (int i = 0; i < properties.Count; i++)
        {
            int idx = properties[i].GetIndex();
            if (idx + 1 > length)
            {
                length = idx + 1;
            }
        }

        return length;
    }

    private static bool[] BuildWantedColumnsMask(
        IEntityType rootType,
        IReadOnlyList<IProperty> properties,
        int?[] propertyToColumnOrdinal,
        int columnCount,
        IReadOnlySet<IProperty>? requiredProperties,
        IReadOnlyList<IProperty>? extraProperties = null)
    {
        bool[] wanted = new bool[columnCount];
        bool isOptionalTableSplit = JetEntityTypeMetadata.IsTableSplitOwned(rootType)
            && rootType.FindOwnership() is { IsRequiredDependent: false };
        if (requiredProperties is null || isOptionalTableSplit)
        {
            for (int i = 0; i < propertyToColumnOrdinal.Length; i++)
            {
                if (propertyToColumnOrdinal[i] is int colIdx && (uint)colIdx < (uint)columnCount)
                {
                    wanted[colIdx] = true;
                }
            }

            return wanted;
        }

        IProperty? discriminator = rootType.FindDiscriminatorProperty();
        bool anySelected = false;
        for (int i = 0; i < properties.Count; i++)
        {
            if (propertyToColumnOrdinal[i] is not int colIdx || (uint)colIdx >= (uint)columnCount)
            {
                continue;
            }

            IProperty prop = properties[i];
            if (requiredProperties.Contains(prop)
                || prop.IsPrimaryKey()
                || prop.IsKey()
                || prop.IsConcurrencyToken
                || ReferenceEquals(prop, discriminator)
                || (extraProperties is not null && ContainsProperty(extraProperties, prop)))
            {
                wanted[colIdx] = true;
                anySelected = true;
            }
        }

        if (!anySelected)
        {
            Array.Fill(wanted, true);
        }

        return wanted;
    }

    private static bool ContainsProperty(IReadOnlyList<IProperty> list, IProperty candidate)
    {
        for (int i = 0; i < list.Count; i++)
        {
            if (ReferenceEquals(list[i], candidate))
            {
                return true;
            }
        }

        return false;
    }

    private static async ValueTask<IReadOnlyList<ValueBuffer>> ReadByKeysFromReaderAsync(
        AccessReader reader,
        string tableName,
        IEntityType rootType,
        IReadOnlyList<IProperty> properties,
        IReadOnlyList<IProperty> keyProperties,
        IReadOnlyList<object?[]> keyTuples,
        IReadOnlySet<IProperty>? requiredProperties,
        CancellationToken cancellationToken)
    {
        ResolvedTable? resolved = await reader.Services.Tables.ResolveLocalTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved is not null)
        {
            int?[] propertyToColumnOrdinal = BuildPropertyColumnMapFromTableDef(properties, resolved.Definition);
            bool[] wantedColumns = BuildWantedColumnsMask(
                rootType,
                properties,
                propertyToColumnOrdinal,
                resolved.Definition.Columns.Count,
                requiredProperties,
                keyProperties);
            ValueBufferFactory factory = new(rootType, properties, propertyToColumnOrdinal, wantedColumns, reuseBuffer: false);

            List<object[]>? indexedRows = await JetIndexSeekPlanner.TryReadMultiKeyIndexRowsAsync(
                reader,
                resolved,
                keyProperties,
                keyTuples,
                wantedColumns,
                cancellationToken).ConfigureAwait(false);
            if (indexedRows is not null)
            {
                List<ValueBuffer> indexedBuffers = [];
                foreach (object[] row in indexedRows)
                {
                    if (factory.TryCreate(row, out ValueBuffer buffer))
                    {
                        indexedBuffers.Add(buffer);
                    }
                }

                return indexedBuffers;
            }

            HashSet<KeyTupleValue> targetSet = BuildKeyTupleSet(keyTuples);
            List<ValueBuffer> scanResults = [];
            await foreach (object[] row in reader.Services.Tables.Rows(tableName, wantedColumns, progress: null, cancellationToken).WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                if (factory.TryCreate(row, out ValueBuffer buffer)
                    && TryExtractBufferKeyTuple(keyProperties, buffer, out KeyTupleValue candidateKey)
                    && targetSet.Contains(candidateKey))
                {
                    scanResults.Add(buffer);
                }
            }

            return scanResults;
        }

        IReadOnlyList<ColumnMetadata> columns = await reader.GetColumnMetadataAsync(tableName, cancellationToken).ConfigureAwait(false);
        int?[] fallbackColumnMap = BuildPropertyColumnMap(properties, columns);
        ValueBufferFactory fallbackFactory = new(rootType, properties, fallbackColumnMap, wantedColumns: null, reuseBuffer: false);
        HashSet<KeyTupleValue> fallbackTargetSet = BuildKeyTupleSet(keyTuples);
        List<ValueBuffer> fallbackResults = [];
        await foreach (object[] row in reader.Rows(tableName, progress: null, cancellationToken).WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (fallbackFactory.TryCreate(row, out ValueBuffer buffer)
                && TryExtractBufferKeyTuple(keyProperties, buffer, out KeyTupleValue candidateKey)
                && fallbackTargetSet.Contains(candidateKey))
            {
                fallbackResults.Add(buffer);
            }
        }

        return fallbackResults;
    }

    private static int?[] BuildPropertyColumnMap(
        IReadOnlyList<IProperty> properties,
        IReadOnlyList<ColumnMetadata> columns)
    {
        Dictionary<string, int> columnIndexByName = [with(StringComparer.OrdinalIgnoreCase)];
        for (int i = 0; i < columns.Count; i++)
        {
            columnIndexByName[columns[i].Name] = i;
        }

        int?[] map = new int?[properties.Count];
        for (int i = 0; i < properties.Count; i++)
        {
            string columnName = JetEntityTypeMetadata.GetColumnName(properties[i]);
            if (columnIndexByName.TryGetValue(columnName, out int colIdx))
            {
                map[i] = colIdx;
            }
        }

        return map;
    }

    private static int?[] BuildPropertyColumnMapFromTableDef(
        IReadOnlyList<IProperty> properties,
        TableDef tableDef)
    {
        int?[] map = new int?[properties.Count];
        for (int i = 0; i < properties.Count; i++)
        {
            string columnName = JetEntityTypeMetadata.GetColumnName(properties[i]);
            int colIdx = tableDef.FindColumnIndex(columnName);
            if (colIdx >= 0)
            {
                map[i] = colIdx;
            }
        }

        return map;
    }

    private static bool IsAllNullTableSplitOwnedRow(
        IReadOnlyList<IProperty> properties,
        int?[] propertyToColumnOrdinal,
        object?[] row)
    {
        bool hasNonKeyProperty = false;
        for (int i = 0; i < properties.Count; i++)
        {
            IProperty prop = properties[i];
            if (prop.IsPrimaryKey())
            {
                continue;
            }

            hasNonKeyProperty = true;
            if (propertyToColumnOrdinal[i] is int colIdx
                && (uint)colIdx < (uint)row.Length
                && row[colIdx] is not (null or DBNull))
            {
                return false;
            }
        }

        return hasNonKeyProperty;
    }

    private static IReadOnlyEntityType? FindMatchingEntityType(IEntityType rootType, object? discriminatorValue)
    {
        if (discriminatorValue is null)
        {
            return null;
        }

        if (Equals(rootType.GetDiscriminatorValue(), discriminatorValue))
        {
            return rootType;
        }

        foreach (IEntityType derived in rootType.GetDerivedTypes())
        {
            if (Equals(derived.GetDiscriminatorValue(), discriminatorValue))
            {
                return derived;
            }
        }

        return null;
    }

    private static async ValueTask<IReadOnlyList<ValueBuffer>> ReadByKeysFromActiveWriterAsync(
        AccessWriter writer,
        string tableName,
        IEntityType rootType,
        IReadOnlyList<IProperty> properties,
        IReadOnlyList<IProperty> keyProperties,
        IReadOnlyList<object?[]> keyTuples,
        IReadOnlySet<IProperty>? requiredProperties,
        CancellationToken cancellationToken)
    {
        ResolvedTable resolved = await writer.Services.Catalog.ResolveRequiredTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        int?[] propertyToColumnOrdinal = BuildPropertyColumnMapFromTableDef(properties, resolved.Definition);
        bool[] wantedColumns = BuildWantedColumnsMask(
            rootType,
            properties,
            propertyToColumnOrdinal,
            resolved.Definition.Columns.Count,
            requiredProperties,
            keyProperties);
        ValueBufferFactory factory = new(rootType, properties, propertyToColumnOrdinal, wantedColumns, reuseBuffer: false);

        List<ValueBuffer> indexedBuffers = [];
        bool indexedUsed = await JetIndexSeekPlanner.TryReadMultiKeyFromActiveWriterIndexAsync(
            writer,
            resolved,
            indexes: null,
            keyProperties,
            keyTuples,
            wantedColumns,
            values =>
            {
                if (factory.TryCreate(values, out ValueBuffer buffer))
                {
                    indexedBuffers.Add(buffer);
                }

                return true;
            },
            cancellationToken).ConfigureAwait(false);
        if (indexedUsed)
        {
            return indexedBuffers;
        }

        bool wrapScannedHyperlinks = resolved.Definition.HasHyperlinkColumns
            && TableReader.HasWantedHyperlinkColumn(resolved.Definition.ClrTypes, wantedColumns);
        HashSet<KeyTupleValue> targetSet = BuildKeyTupleSet(keyTuples);
        List<ValueBuffer> scanResults = [];
        await writer.Services.Snapshots.ForEachTypedRowAsync(
            resolved.Entry.TDefPage,
            resolved.Definition,
            wantedColumns,
            (_, values, _) =>
            {
                if (wrapScannedHyperlinks)
                {
                    TableReader.WrapHyperlinkColumns(values, resolved.Definition.ClrTypes);
                }

                if (factory.TryCreate(values, out ValueBuffer buffer)
                    && TryExtractBufferKeyTuple(keyProperties, buffer, out KeyTupleValue candidateKey)
                    && targetSet.Contains(candidateKey))
                {
                    scanResults.Add(buffer);
                }

                return new ValueTask<bool>(true);
            },
            cancellationToken).ConfigureAwait(false);

        return scanResults;
    }

    private static async IAsyncEnumerable<object?[]> StreamWriterRowsAsync(
        AccessWriter writer,
        ResolvedTable table,
        bool[] wantedColumns,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var channel = Channel.CreateBounded<object?[]>(new BoundedChannelOptions(1)
        {
            SingleReader = true,
            SingleWriter = true,
            AllowSynchronousContinuations = false,
        });
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        // Decode another row only when the consumer advances beyond the current yield.
        using SemaphoreSlim advance = new(0, 1);
        Task producer = ProduceAsync();
        try
        {
            await foreach (object?[] row in channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                yield return row;
                advance.Release();
            }
        }
        finally
        {
            await stop.CancelAsync().ConfigureAwait(false);
            try
            {
                await producer.ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested && stop.IsCancellationRequested)
            {
            }
        }

        async Task ProduceAsync()
        {
            try
            {
                bool wrapHyperlinks = table.Definition.HasHyperlinkColumns && TableReader.HasWantedHyperlinkColumn(table.Definition.ClrTypes, wantedColumns);
                await writer.Services.Snapshots.ForEachTypedRowAsync(
                    table.Entry.TDefPage,
                    table.Definition,
                    wantedColumns,
                    async (_, values, token) =>
                    {
                        if (wrapHyperlinks)
                        {
                            TableReader.WrapHyperlinkColumns(values, table.Definition.ClrTypes);
                        }

                        await channel.Writer.WriteAsync(values, token).ConfigureAwait(false);
                        await advance.WaitAsync(token).ConfigureAwait(false);
                        return true;
                    },
                    stop.Token).ConfigureAwait(false);
                channel.Writer.TryComplete();
            }
            catch (Exception exception)
            {
                channel.Writer.TryComplete(exception);
                throw;
            }
        }
    }

    private static HashSet<KeyTupleValue> BuildKeyTupleSet(IReadOnlyList<object?[]> keyTuples)
    {
        HashSet<KeyTupleValue> set = [];
        foreach (object?[] tuple in keyTuples)
        {
            _ = set.Add(new KeyTupleValue(tuple));
        }

        return set;
    }

    private static bool TryExtractBufferKeyTuple(
        IReadOnlyList<IProperty> keyProperties,
        ValueBuffer buffer,
        out KeyTupleValue key)
    {
        object?[] values = new object?[keyProperties.Count];
        for (int i = 0; i < keyProperties.Count; i++)
        {
            int idx = keyProperties[i].GetIndex();
            object? val = (uint)idx < (uint)buffer.Count ? buffer[idx] : null;
            if (val is null or DBNull)
            {
                key = default;
                return false;
            }

            values[i] = val;
        }

        key = new KeyTupleValue(values);
        return true;
    }

    private async IAsyncEnumerable<ValueBuffer> StreamTableValueBuffersCoreAsync(
        IEntityType entityType,
        IReadOnlyList<JetIndexSeekCondition>? seekConditions,
        IReadOnlyDictionary<string, object?> parameters,
        IReadOnlySet<IProperty>? requiredProperties,
        int? maxRows,
        IReadOnlyList<JetOrdering>? orderedProperties,
        Func<ValueBuffer, bool>? rowFilter,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (maxRows is <= 0)
        {
            yield break;
        }

        bool ownsGate = !this.writerRented;
        if (ownsGate)
        {
            await this.EnterLifecycleAsync(cancellationToken).ConfigureAwait(false);
        }

        bool lifecycleHeld = ownsGate;
        try
        {
            IEntityType rootType = entityType.GetRootType();
            string tableName = JetEntityTypeMetadata.GetTableName(rootType);
            IReadOnlyList<IProperty> properties = JetEntityTypeMetadata.GetHierarchyProperties(rootType);
            AccessWriter? writer = this.ActiveWriter ?? this.cachedWriter;
            ResolvedTable? resolved;
            IAsyncEnumerable<object?[]> rows;
            bool ordered = false;
            int?[] columnMap;
            bool[]? wanted = null;
            if (writer is not null)
            {
                resolved = await writer.Services.Catalog.ResolveRequiredTableAsync(tableName, cancellationToken).ConfigureAwait(false);
                columnMap = BuildPropertyColumnMapFromTableDef(properties, resolved.Definition);
                wanted = BuildWantedColumnsMask(rootType, properties, columnMap, resolved.Definition.Columns.Count, requiredProperties);
                (IAsyncEnumerable<object?[]> Rows, bool Ordered)? indexQuery = await JetIndexSeekPlanner.TryBuildActiveWriterIndexQueryAsync(
                    writer, resolved, seekConditions, orderedProperties, parameters, wanted, cancellationToken).ConfigureAwait(false);
                rows = indexQuery?.Rows ?? StreamWriterRowsAsync(writer, resolved, wanted, cancellationToken);
                ordered = indexQuery?.Ordered ?? false;
            }
            else
            {
                await this.ClosePeerConnectionsAsync(closeReaders: false, cancellationToken).ConfigureAwait(false);
                AccessReader reader = await this.GetReaderCoreAsync(cancellationToken).ConfigureAwait(false);
                resolved = await reader.Services.Tables.ResolveLocalTableAsync(tableName, cancellationToken).ConfigureAwait(false);
                if (resolved is not null)
                {
                    columnMap = BuildPropertyColumnMapFromTableDef(properties, resolved.Definition);
                    wanted = BuildWantedColumnsMask(rootType, properties, columnMap, resolved.Definition.Columns.Count, requiredProperties);
                    IAsyncEnumerable<object[]>? indexRows = null;
                    if (orderedProperties is { Count: > 0 })
                    {
                        indexRows = await JetIndexSeekPlanner.TryBuildOrderedIndexQueryAsync(reader, resolved, orderedProperties, wanted, cancellationToken).ConfigureAwait(false);
                        ordered = indexRows is not null;
                    }
                    else if (seekConditions is { Count: > 0 })
                    {
                        indexRows = await JetIndexSeekPlanner.TryBuildIndexQueryAsync(reader, resolved, seekConditions, parameters, wanted, cancellationToken).ConfigureAwait(false);
                    }

                    rows = indexRows ?? reader.Services.Tables.Rows(tableName, wanted, progress: null, cancellationToken);
                }
                else
                {
                    columnMap = BuildPropertyColumnMap(properties, await reader.GetColumnMetadataAsync(tableName, cancellationToken).ConfigureAwait(false));
                    rows = reader.Rows(tableName, progress: null, cancellationToken);
                }
            }

            if (lifecycleHeld && this.DatabasePath is not null)
            {
                this.lifecycleGate.Release();
                lifecycleHeld = false;
            }

            ValueBufferFactory factory = new(rootType, properties, columnMap, wanted, reuseBuffer: false);
            int? limit = orderedProperties is null or { Count: 0 } || ordered ? maxRows : null;
            List<ValueBuffer>? sortBuffer = orderedProperties is { Count: > 0 } && !ordered ? [] : null;
            int emitted = 0;
            await foreach (object?[] row in rows.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                if (!factory.TryCreate(row, out ValueBuffer buffer) || (rowFilter is not null && !rowFilter(buffer)))
                {
                    continue;
                }

                if (sortBuffer is not null)
                {
                    sortBuffer.Add(buffer);
                    continue;
                }

                yield return buffer;
                emitted++;
                if (limit is int maximum && emitted >= maximum)
                {
                    yield break;
                }
            }

            if (sortBuffer is not null)
            {
                foreach (ValueBuffer buffer in JetBufferOrdering.Apply(sortBuffer, orderedProperties!))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    yield return buffer;
                    if (maxRows is int maximum && ++emitted >= maximum)
                    {
                        yield break;
                    }
                }
            }
        }
        finally
        {
            if (lifecycleHeld)
            {
                this.lifecycleGate.Release();
            }

            if (ownsGate)
            {
                this.connectionGate.Release();
            }
        }
    }

    private async ValueTask<JetTransaction> BeginTransactionCoreAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(this.disposed, this);

        if (this.ActiveTransaction is not null)
        {
            throw new InvalidOperationException("A transaction is already active on this connection.");
        }

        AccessWriter writer;
        if (this.cachedWriter is not null)
        {
            writer = this.cachedWriter;
        }
        else
        {
            await this.CloseAllReadersForSameTargetAsync(cancellationToken).ConfigureAwait(false);
            writer = await this.OpenWriterCoreAsync(cancellationToken).ConfigureAwait(false);
            this.cachedWriter = writer;
        }

        try
        {
            JetTransaction transaction = await writer.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            this.ActiveWriter = writer;
            this.ActiveTransaction = transaction;
            return transaction;
        }
        catch
        {
            if (ReferenceEquals(this.cachedWriter, writer))
            {
                this.cachedWriter = null;
            }

            await writer.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private async ValueTask CommitTransactionCoreAsync(CancellationToken cancellationToken = default)
    {
        if (this.ActiveTransaction is null)
        {
            throw new InvalidOperationException("No transaction is currently active on this connection.");
        }

        try
        {
            for (int i = this.savepoints.Count - 1; i >= 0; i--)
            {
                JetTransaction scope = this.savepoints[i].Scope;
                this.savepoints.RemoveAt(i);
                try
                {
                    await scope.CommitAsync(cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    await scope.DisposeAsync().ConfigureAwait(false);
                }
            }

            await this.ActiveTransaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            await this.DisposeTransactionCoreAsync().ConfigureAwait(false);
        }
    }

    private async ValueTask RollbackTransactionCoreAsync(CancellationToken cancellationToken = default)
    {
        if (this.ActiveTransaction is null)
        {
            throw new InvalidOperationException("No transaction is currently active on this connection.");
        }

        try
        {
            for (int i = this.savepoints.Count - 1; i >= 0; i--)
            {
                JetTransaction scope = this.savepoints[i].Scope;
                this.savepoints.RemoveAt(i);
                try
                {
                    await scope.RollbackAsync(cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    await scope.DisposeAsync().ConfigureAwait(false);
                }
            }

            await this.ActiveTransaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            await this.DisposeTransactionCoreAsync().ConfigureAwait(false);
        }
    }

    private async ValueTask DisposeTransactionCoreAsync()
    {
        for (int i = this.savepoints.Count - 1; i >= 0; i--)
        {
            JetTransaction scope = this.savepoints[i].Scope;
            this.savepoints.RemoveAt(i);
            await scope.DisposeAsync().ConfigureAwait(false);
        }

        JetTransaction? transaction = this.ActiveTransaction;
        AccessWriter? writer = this.ActiveWriter;
        this.ActiveTransaction = null;
        this.ActiveWriter = null;

        try
        {
            if (transaction is not null)
            {
                await transaction.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            if (writer is not null && !ReferenceEquals(writer, this.cachedWriter))
            {
                await writer.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    private async ValueTask CreateSavepointCoreAsync(string name, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ObjectDisposedException.ThrowIf(this.disposed, this);

        if (this.ActiveWriter is null || this.ActiveTransaction is null)
        {
            throw new InvalidOperationException("Cannot create a savepoint when no transaction is active.");
        }

        JetTransaction scope = await this.ActiveWriter.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        this.savepoints.Add((name, scope));
    }

    private async ValueTask RollbackToSavepointCoreAsync(string name, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ObjectDisposedException.ThrowIf(this.disposed, this);

        if (this.ActiveWriter is null || this.ActiveTransaction is null)
        {
            throw new InvalidOperationException("Cannot roll back to a savepoint when no transaction is active.");
        }

        int targetIndex = this.FindSavepointIndex(name);
        if (targetIndex < 0)
        {
            throw new InvalidOperationException($"Savepoint '{name}' does not exist in the current transaction.");
        }

        for (int i = this.savepoints.Count - 1; i >= targetIndex; i--)
        {
            JetTransaction scope = this.savepoints[i].Scope;
            this.savepoints.RemoveAt(i);
            try
            {
                await scope.RollbackAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                await scope.DisposeAsync().ConfigureAwait(false);
            }
        }

        JetTransaction replacementScope = await this.ActiveWriter.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        this.savepoints.Add((name, replacementScope));
    }

    private async ValueTask ReleaseSavepointCoreAsync(string name, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ObjectDisposedException.ThrowIf(this.disposed, this);

        if (this.ActiveWriter is null || this.ActiveTransaction is null)
        {
            throw new InvalidOperationException("Cannot release a savepoint when no transaction is active.");
        }

        int targetIndex = this.FindSavepointIndex(name);
        if (targetIndex < 0)
        {
            throw new InvalidOperationException($"Savepoint '{name}' does not exist in the current transaction.");
        }

        for (int i = this.savepoints.Count - 1; i >= targetIndex; i--)
        {
            JetTransaction scope = this.savepoints[i].Scope;
            this.savepoints.RemoveAt(i);
            try
            {
                await scope.CommitAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                await scope.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    private async ValueTask<bool> DeleteDatabaseCoreAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(this.disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        await this.DisposeTransactionCoreAsync().ConfigureAwait(false);
        await this.CloseAllReadersForSameTargetAsync(cancellationToken).ConfigureAwait(false);

        if (!string.IsNullOrWhiteSpace(this.DatabasePath))
        {
            if (!File.Exists(this.DatabasePath))
            {
                return false;
            }

            File.Delete(this.DatabasePath);
            return true;
        }

        Stream stream = this.GetOrCreateStream();
        if (stream.Length == 0)
        {
            return false;
        }

        stream.SetLength(0);
        stream.Position = 0;
        return true;
    }

    private async ValueTask EnterLifecycleAsync(CancellationToken cancellationToken)
    {
        await this.lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(this.disposed, this);
            await this.connectionGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                ObjectDisposedException.ThrowIf(this.disposed, this);
            }
            catch
            {
                this.connectionGate.Release();
                throw;
            }
        }
        catch
        {
            this.lifecycleGate.Release();
            throw;
        }
    }

    private void ExitLifecycle()
    {
        this.connectionGate.Release();
        this.lifecycleGate.Release();
    }

    private async ValueTask CloseAllReadersForSameTargetAsync(CancellationToken cancellationToken = default)
    {
        await this.CloseReaderCoreAsync().ConfigureAwait(false);
        await this.ClosePeerConnectionsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask ClosePeerConnectionsAsync(bool closeReaders = true, CancellationToken cancellationToken = default)
    {
        List<JetClientConnection> peers = [];
        Stream? myStream = this.optionsExtension.DatabaseStream ?? this.resolvedStream;
        lock (LiveConnectionsGate)
        {
            for (int i = LiveConnections.Count - 1; i >= 0; i--)
            {
                if (!LiveConnections[i].Connection.TryGetTarget(out JetClientConnection? peer) || peer.disposalCompleted)
                {
                    LiveConnections.RemoveAt(i);
                    continue;
                }

                if (ReferenceEquals(peer, this))
                {
                    continue;
                }

                if ((this.DatabasePath is not null
                    && peer.DatabasePath is not null
                    && string.Equals(this.DatabasePath, peer.DatabasePath, StringComparison.Ordinal))
                    || (myStream is not null && ReferenceEquals(myStream, peer.optionsExtension.DatabaseStream ?? peer.resolvedStream)))
                {
                    peers.Add(peer);
                }
            }
        }

        foreach (JetClientConnection peer in peers)
        {
            if (!closeReaders && peer.cachedWriter is null && peer.ActiveWriter is null)
            {
                continue;
            }

            await peer.connectionGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                if (peer.ActiveTransaction is not null)
                {
                    throw new InvalidOperationException("Another connection has an active transaction on this database.");
                }

                if (closeReaders)
                {
                    await peer.CloseReaderCoreAsync().ConfigureAwait(false);
                }
                else
                {
                    await peer.CloseCachedWriterCoreAsync().ConfigureAwait(false);
                }
            }
            finally
            {
                peer.connectionGate.Release();
            }
        }
    }

    private async ValueTask<AccessWriter> OpenWriterCoreAsync(CancellationToken cancellationToken)
    {
        AccessWriterOptions writerOptions = this.BuildWriterOptions();
        if (!string.IsNullOrWhiteSpace(this.DatabasePath))
        {
            return await AccessWriter.OpenAsync(this.DatabasePath, writerOptions, cancellationToken).ConfigureAwait(false);
        }

        Stream stream = this.GetOrCreateStream();
        stream.Position = 0;
        return await AccessWriter.OpenAsync(stream, writerOptions, leaveOpen: true, cancellationToken).ConfigureAwait(false);
    }

    private Stream GetOrCreateStream()
    {
        if (this.optionsExtension.DatabaseStream is not null)
        {
            return this.optionsExtension.DatabaseStream;
        }

        if (this.resolvedStream is not null)
        {
            return this.resolvedStream;
        }

        if (this.optionsExtension.StreamFactory is not null)
        {
            this.resolvedStream = this.optionsExtension.StreamFactory();
            return this.resolvedStream;
        }

        throw new InvalidOperationException("No database path or stream is configured.");
    }

    private async ValueTask<AccessReader> GetReaderCoreAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(this.disposed, this);

        if (this.activeReader is not null)
        {
            return this.activeReader;
        }

        if (this.ActiveWriter is not null)
        {
            throw new InvalidOperationException("Cannot open AccessReader while an AccessWriter is active.");
        }

        if (this.cachedWriter is not null)
        {
            AccessWriter writer = this.cachedWriter;
            this.cachedWriter = null;
            await writer.DisposeAsync().ConfigureAwait(false);
        }

        AccessReaderOptions readerOptions = this.BuildReaderOptions();
        if (!string.IsNullOrWhiteSpace(this.DatabasePath))
        {
            this.activeReader = await AccessReader.OpenAsync(this.DatabasePath, readerOptions, cancellationToken).ConfigureAwait(false);
            return this.activeReader;
        }

        Stream stream = this.GetOrCreateStream();
        stream.Position = 0;
        this.activeReader = await AccessReader.OpenAsync(stream, readerOptions, leaveOpen: true, cancellationToken).ConfigureAwait(false);
        return this.activeReader;
    }

    private async ValueTask CloseReaderCoreAsync()
    {
        if (this.activeReader is not null)
        {
            AccessReader reader = this.activeReader;
            this.activeReader = null;
            await reader.DisposeAsync().ConfigureAwait(false);
        }

        await this.CloseCachedWriterCoreAsync().ConfigureAwait(false);
    }

    private async ValueTask CloseCachedWriterCoreAsync()
    {
        if (this.cachedWriter is not null && this.ActiveWriter is null)
        {
            AccessWriter writer = this.cachedWriter;
            this.cachedWriter = null;
            await writer.DisposeAsync().ConfigureAwait(false);
        }
    }

    private int FindSavepointIndex(string name)
    {
        for (int i = this.savepoints.Count - 1; i >= 0; i--)
        {
            if (string.Equals(this.savepoints[i].Name, name, StringComparison.Ordinal))
            {
                return i;
            }
        }

        return -1;
    }

    private sealed class ValueBufferFactory
    {
        private readonly IEntityType rootType;
        private readonly IReadOnlyList<IProperty> properties;
        private readonly int?[] columnMap;
        private readonly (IProperty Property, int Source, int Target)[] projectedProperties;
        private readonly Dictionary<IReadOnlyEntityType, object?[]> concreteDefaults = [];
        private readonly object?[] defaults;
        private readonly object?[]? reusedBuffer;
        private readonly IProperty? discriminator;
        private readonly int? discriminatorColumn;

        internal ValueBufferFactory(
            IEntityType rootType,
            IReadOnlyList<IProperty> properties,
            int?[] columnMap,
            bool[]? wantedColumns,
            bool reuseBuffer)
        {
            this.rootType = rootType;
            this.properties = properties;
            this.columnMap = columnMap;
            this.discriminator = rootType.FindDiscriminatorProperty();
            int length = ComputeValueBufferLength(properties);
            this.defaults = this.BuildDefaults(length, concreteType: null);
            this.reusedBuffer = reuseBuffer ? new object?[length] : null;
            List<(IProperty Property, int Source, int Target)> projected = [];
            for (int i = 0; i < properties.Count; i++)
            {
                if (columnMap[i] is int source && wantedColumns?[source] != false)
                {
                    IProperty property = properties[i];
                    int target = property.GetIndex();
                    projected.Add((property, source, (uint)target < (uint)length ? target : i));
                    if (ReferenceEquals(property, this.discriminator))
                    {
                        this.discriminatorColumn = source;
                    }
                }
            }

            this.projectedProperties = [.. projected];
            if (this.discriminator is not null)
            {
                this.concreteDefaults[rootType] = this.BuildDefaults(length, rootType);
                foreach (IEntityType derived in rootType.GetDerivedTypes())
                {
                    this.concreteDefaults[derived] = this.BuildDefaults(length, derived);
                }
            }
        }

        internal bool TryCreate(object?[] row, out ValueBuffer valueBuffer)
        {
            if (JetEntityTypeMetadata.IsTableSplitOwned(this.rootType)
                && this.rootType.FindOwnership() is { IsRequiredDependent: false }
                && IsAllNullTableSplitOwnedRow(this.properties, this.columnMap, row))
            {
                valueBuffer = default;
                return false;
            }

            IReadOnlyEntityType? concreteType = null;
            if (this.discriminator is not null && this.discriminatorColumn is int discriminatorSource && (uint)discriminatorSource < (uint)row.Length)
            {
                concreteType = FindMatchingEntityType(this.rootType, JetEntityTypeMetadata.ConvertFromProvider(this.discriminator, row[discriminatorSource]));
            }

            object?[] template = concreteType is not null && this.concreteDefaults.TryGetValue(concreteType, out object?[]? defaultsForType)
                ? defaultsForType
                : this.defaults;
            object?[] buffer = this.reusedBuffer ?? new object?[template.Length];
            Array.Copy(template, buffer, template.Length);
            foreach ((IProperty property, int source, int target) in this.projectedProperties)
            {
                if ((uint)source >= (uint)row.Length
                    || row[source] is null or DBNull
                    || (concreteType is not null && !property.DeclaringType.ContainingEntityType.IsAssignableFrom(concreteType)))
                {
                    continue;
                }

                object? converted = JetEntityTypeMetadata.ConvertFromProvider(property, row[source]);
                if (converted is not null || buffer[target] is null)
                {
                    buffer[target] = converted;
                }
            }

            valueBuffer = new ValueBuffer(buffer);
            return true;
        }

        private object?[] BuildDefaults(int length, IReadOnlyEntityType? concreteType)
        {
            object?[] template = new object?[length];
            for (int i = 0; i < this.properties.Count; i++)
            {
                IProperty property = this.properties[i];
                if (concreteType is not null && !property.DeclaringType.ContainingEntityType.IsAssignableFrom(concreteType))
                {
                    continue;
                }

                int target = property.GetIndex();
                if ((uint)target >= (uint)length)
                {
                    target = i;
                }

                object? value = JetEntityTypeMetadata.ConvertFromProvider(property, null);
                if (value is null && property.ClrType.IsValueType && Nullable.GetUnderlyingType(property.ClrType) is null)
                {
                    value = BoxedDefaultValues.GetOrAdd(property.ClrType, static type =>
                        Expression.Lambda<Func<object>>(Expression.Convert(Expression.Default(type), typeof(object))).Compile()());
                }

                if (value is not null || template[target] is null)
                {
                    template[target] = value;
                }
            }

            return template;
        }
    }

    private readonly struct KeyTupleValue(object?[] values) : IEquatable<KeyTupleValue>
    {
        private readonly object?[] values = values;

        public bool Equals(KeyTupleValue other)
        {
            if (this.values.Length != other.values.Length)
            {
                return false;
            }

            for (int i = 0; i < this.values.Length; i++)
            {
                if (!Equals(this.values[i], other.values[i]))
                {
                    return false;
                }
            }

            return true;
        }

        public override bool Equals(object? obj)
            => obj is KeyTupleValue other && this.Equals(other);

        public override int GetHashCode()
        {
            HashCode hash = default;
            foreach (object? val in this.values)
            {
                hash.Add(val);
            }

            return hash.ToHashCode();
        }
    }
}
