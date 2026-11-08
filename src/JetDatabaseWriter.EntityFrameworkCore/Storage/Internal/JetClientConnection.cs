namespace JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.EntityFrameworkCore.Infrastructure.Internal;
using JetDatabaseWriter.EntityFrameworkCore.Metadata.Internal;
using JetDatabaseWriter.EntityFrameworkCore.Query.Internal;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Tables;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;

/// <summary>
/// Default implementation of <see cref="IJetClientConnection"/> coordinating <see cref="AccessReader"/> and <see cref="AccessWriter"/>.
/// </summary>
public sealed class JetClientConnection : IJetClientConnection
{
    private static readonly List<WeakReference<JetClientConnection>> LiveConnections = [];
    private static readonly Lock LiveConnectionsGate = new();
    private static readonly bool DefaultWriterByteRangeLocks = new AccessWriterOptions().UseByteRangeLocks;

    private readonly SemaphoreSlim connectionGate = new(1, 1);
    private readonly JetOptionsExtension optionsExtension;
    private readonly string? normalizedDatabasePath;
    private Stream? resolvedStream;
    private AccessReader? activeReader;
    private bool disposed;

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
            this.normalizedDatabasePath = Path.GetFullPath(this.optionsExtension.DatabasePath);
        }

        RegisterConnection(this);
    }

    /// <inheritdoc/>
    public string? DatabasePath => this.optionsExtension.DatabasePath;

    /// <inheritdoc/>
    public DatabaseFormat Format => this.optionsExtension.Format;

    /// <inheritdoc/>
    public AccessWriter? ActiveWriter { get; private set; }

    /// <inheritdoc/>
    public JetTransaction? ActiveTransaction { get; private set; }

    /// <inheritdoc/>
    public async ValueTask<AccessReader> GetReaderAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(this.disposed, this);

        await this.connectionGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await this.GetReaderCoreAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            this.connectionGate.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask CloseReaderAsync()
    {
        try
        {
            await this.connectionGate.WaitAsync().ConfigureAwait(false);
        }
        catch (ObjectDisposedException)
        {
            return;
        }

        try
        {
            await this.CloseReaderCoreAsync().ConfigureAwait(false);
        }
        finally
        {
            this.connectionGate.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask<(AccessWriter Writer, bool OwnedByCaller)> RentWriterAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(this.disposed, this);

        if (this.ActiveWriter is not null)
        {
            return (this.ActiveWriter, false);
        }

        await this.CloseAllReadersForSameTargetAsync().ConfigureAwait(false);
        AccessWriter writer = await this.OpenWriterCoreAsync(cancellationToken).ConfigureAwait(false);
        return (writer, true);
    }

    /// <inheritdoc/>
    public async ValueTask ReleaseWriterAsync(AccessWriter writer, bool ownedByCaller)
    {
        ArgumentNullException.ThrowIfNull(writer);

        if (ownedByCaller)
        {
            await writer.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <inheritdoc/>
    public async ValueTask<AccessWriter> CreateDatabaseWriterAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(this.disposed, this);

        await this.CloseAllReadersForSameTargetAsync().ConfigureAwait(false);
        AccessWriterOptions writerOptions = this.BuildWriterOptions();
        if (!string.IsNullOrWhiteSpace(this.optionsExtension.DatabasePath))
        {
            string? directory = Path.GetDirectoryName(this.optionsExtension.DatabasePath);
            if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
            {
                _ = Directory.CreateDirectory(directory);
            }

            return await AccessWriter.CreateDatabaseAsync(
                this.optionsExtension.DatabasePath,
                this.optionsExtension.Format,
                writerOptions,
                cancellationToken).ConfigureAwait(false);
        }

        Stream stream = this.GetOrCreateStream();
        stream.SetLength(0);
        stream.Position = 0;
        return await AccessWriter.CreateDatabaseAsync(
            stream,
            this.optionsExtension.Format,
            writerOptions,
            leaveOpen: true,
            cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async ValueTask<JetTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(this.disposed, this);

        if (this.ActiveTransaction is not null)
        {
            throw new InvalidOperationException("A transaction is already active on this connection.");
        }

        await this.CloseAllReadersForSameTargetAsync().ConfigureAwait(false);
        AccessWriter writer = await this.OpenWriterCoreAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            JetTransaction transaction = await writer.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            this.ActiveWriter = writer;
            this.ActiveTransaction = transaction;
            return transaction;
        }
        catch
        {
            await writer.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <inheritdoc/>
    public async ValueTask CommitTransactionAsync(CancellationToken cancellationToken = default)
    {
        if (this.ActiveTransaction is null)
        {
            throw new InvalidOperationException("No transaction is currently active on this connection.");
        }

        try
        {
            await this.ActiveTransaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            await this.DisposeTransactionAsync().ConfigureAwait(false);
        }
    }

    /// <inheritdoc/>
    public async ValueTask RollbackTransactionAsync(CancellationToken cancellationToken = default)
    {
        if (this.ActiveTransaction is null)
        {
            throw new InvalidOperationException("No transaction is currently active on this connection.");
        }

        try
        {
            await this.ActiveTransaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            await this.DisposeTransactionAsync().ConfigureAwait(false);
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeTransactionAsync()
    {
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
            if (writer is not null)
            {
                await writer.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <inheritdoc/>
    public async ValueTask<bool> CanConnectAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(this.disposed, this);

        if (this.ActiveWriter is not null || this.activeReader is not null)
        {
            return true;
        }

        if (!string.IsNullOrWhiteSpace(this.optionsExtension.DatabasePath))
        {
            if (!File.Exists(this.optionsExtension.DatabasePath))
            {
                return false;
            }

            FileInfo fileInfo = new(this.optionsExtension.DatabasePath);
            if (fileInfo.Length < 2048)
            {
                return false;
            }
        }
        else
        {
            Stream stream = this.GetOrCreateStream();
            if (stream.Length < 2048)
            {
                return false;
            }
        }

        await this.connectionGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (this.ActiveWriter is not null || this.activeReader is not null)
            {
                return true;
            }

            _ = await this.GetReaderCoreAsync(cancellationToken).ConfigureAwait(false);
            await this.CloseReaderCoreAsync().ConfigureAwait(false);
            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return false;
        }
        finally
        {
            this.connectionGate.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask<bool> DeleteDatabaseAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(this.disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        await this.DisposeTransactionAsync().ConfigureAwait(false);
        await this.CloseAllReadersForSameTargetAsync().ConfigureAwait(false);

        if (!string.IsNullOrWhiteSpace(this.optionsExtension.DatabasePath))
        {
            if (!File.Exists(this.optionsExtension.DatabasePath))
            {
                return false;
            }

            File.Delete(this.optionsExtension.DatabasePath);
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

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<ValueBuffer>> ReadTableValueBuffersAsync(
        IEntityType entityType,
        IReadOnlyList<JetIndexSeekCondition>? seekConditions,
        IReadOnlyDictionary<string, object?> parameters,
        CancellationToken cancellationToken = default)
        => this.ReadTableValueBuffersAsync(entityType, seekConditions, parameters, requiredProperties: null, cancellationToken);

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyList<ValueBuffer>> ReadTableValueBuffersAsync(
        IEntityType entityType,
        IReadOnlyList<JetIndexSeekCondition>? seekConditions,
        IReadOnlyDictionary<string, object?> parameters,
        IReadOnlySet<IProperty>? requiredProperties,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entityType);
        ArgumentNullException.ThrowIfNull(parameters);
        ObjectDisposedException.ThrowIf(this.disposed, this);

        IEntityType rootType = entityType.GetRootType();
        string tableName = JetEntityTypeMetadata.GetTableName(rootType);
        List<IProperty> properties = JetEntityTypeMetadata.GetHierarchyProperties(rootType);
        int bufferLength = ComputeValueBufferLength(properties);

        if (this.ActiveWriter is not null)
        {
            return await ReadFromActiveWriterAsync(
                this.ActiveWriter,
                tableName,
                rootType,
                properties,
                bufferLength,
                seekConditions,
                parameters,
                requiredProperties,
                cancellationToken).ConfigureAwait(false);
        }

        await this.connectionGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            AccessReader reader = await this.GetReaderCoreAsync(cancellationToken).ConfigureAwait(false);
            return await ReadFromReaderAsync(
                reader,
                tableName,
                rootType,
                properties,
                bufferLength,
                seekConditions,
                parameters,
                requiredProperties,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            this.connectionGate.Release();
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

        IEntityType rootType = entityType.GetRootType();
        string tableName = JetEntityTypeMetadata.GetTableName(rootType);
        List<IProperty> properties = JetEntityTypeMetadata.GetHierarchyProperties(rootType);
        int bufferLength = ComputeValueBufferLength(properties);

        if (this.ActiveWriter is not null)
        {
            return await ReadByKeysFromActiveWriterAsync(
                this.ActiveWriter,
                tableName,
                rootType,
                properties,
                bufferLength,
                keyProperties,
                keyTuples,
                requiredProperties,
                cancellationToken).ConfigureAwait(false);
        }

        await this.connectionGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            AccessReader reader = await this.GetReaderCoreAsync(cancellationToken).ConfigureAwait(false);
            return await ReadByKeysFromReaderAsync(
                reader,
                tableName,
                rootType,
                properties,
                bufferLength,
                keyProperties,
                keyTuples,
                requiredProperties,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            this.connectionGate.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (this.disposed)
        {
            return;
        }

        try
        {
            await this.connectionGate.WaitAsync().ConfigureAwait(false);
        }
        catch (ObjectDisposedException)
        {
            return;
        }

        try
        {
            if (this.disposed)
            {
                return;
            }

            this.disposed = true;
            UnregisterConnection(this);
            await this.DisposeTransactionAsync().ConfigureAwait(false);
            await this.CloseReaderCoreAsync().ConfigureAwait(false);
            if (this.resolvedStream is not null && !this.optionsExtension.LeaveStreamOpen)
            {
                await this.resolvedStream.DisposeAsync().ConfigureAwait(false);
                this.resolvedStream = null;
            }
        }
        finally
        {
            this.connectionGate.Release();
            this.connectionGate.Dispose();
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
        AccessReaderOptions? template = this.optionsExtension.ReaderOptions;
        bool isStream = string.IsNullOrWhiteSpace(this.optionsExtension.DatabasePath);
        ReadOnlyMemory<char> password = !string.IsNullOrEmpty(this.optionsExtension.Password)
            ? this.optionsExtension.Password.AsMemory()
            : (template?.Password ?? ReadOnlyMemory<char>.Empty);
        return new AccessReaderOptions
        {
            Password = password,
            MaxEncryptionSpinCount = template?.MaxEncryptionSpinCount ?? 1_000_000,
            MaxEncryptionInfoBytes = template?.MaxEncryptionInfoBytes ?? (1024 * 1024),
            MaxTableDefinitionBytes = template?.MaxTableDefinitionBytes ?? (16 * 1024 * 1024),
            MaxSchemaTraversalBytes = template?.MaxSchemaTraversalBytes ?? (64 * 1024 * 1024),
            UseLockFile = !isStream && (template?.UseLockFile ?? true),
            LockFileUserName = template?.LockFileUserName,
            LockFileMachineName = template?.LockFileMachineName,
            UseByteRangeLocks = template?.UseByteRangeLocks ?? false,
            LockTimeoutMilliseconds = template?.LockTimeoutMilliseconds ?? 5000,
            MaxLongValueBytes = template?.MaxLongValueBytes ?? 0xFFFFFF,
            MaxAttachmentContentBytes = template?.MaxAttachmentContentBytes ?? (64 * 1024 * 1024),
            MaxAttachmentContentBytesPerOperation = template?.MaxAttachmentContentBytesPerOperation ?? (256L * 1024 * 1024),
            MaxAttachmentDecodeWorkBytesPerOperation = template?.MaxAttachmentDecodeWorkBytesPerOperation ?? (2L * 1024 * 1024 * 1024),
            MaxComplexDiscoveryEntries = template?.MaxComplexDiscoveryEntries ?? 65536,
            MaxComplexItemIdentityEntriesPerOperation = template?.MaxComplexItemIdentityEntriesPerOperation ?? 1_048_576,
            MaxTotalEncryptionSpinCount = template?.MaxTotalEncryptionSpinCount ?? 10_000_000,
            PageCacheSize = template?.PageCacheSize ?? 256,
            MaxActivePageLeases = template?.MaxActivePageLeases ?? 4096,
            MaxIncludeMaterializedRowsPerOperation = template?.MaxIncludeMaterializedRowsPerOperation ?? 65536,
            MaxIncludeMaterializedPayloadBytesPerOperation = template?.MaxIncludeMaterializedPayloadBytesPerOperation ?? (256L * 1024 * 1024),
            DiagnosticsEnabled = template?.DiagnosticsEnabled ?? false,
            PageReadOptimizationMode = template?.PageReadOptimizationMode ?? PageReadOptimizationMode.Auto,
            ValidateOnOpen = template?.ValidateOnOpen ?? true,
            StrictParsing = template?.StrictParsing ?? true,
            FileAccess = template?.FileAccess ?? FileAccess.Read,
            FileShare = template?.FileShare ?? FileShare.Read,
            LinkedSourceResolver = template?.LinkedSourceResolver,
            LinkedSourceMaxDepth = template?.LinkedSourceMaxDepth ?? 32,
            LinkedSourcePasswordResolver = template?.LinkedSourcePasswordResolver,
            LinkedTextMaxRecordLength = template?.LinkedTextMaxRecordLength ?? 1_048_576,
            LinkedTextMaxFieldLength = template?.LinkedTextMaxFieldLength ?? 1_048_576,
            LinkedTextMaxColumnCount = template?.LinkedTextMaxColumnCount ?? 255,
            LinkedTextMaxSourceFileBytes = template?.LinkedTextMaxSourceFileBytes,
            LinkedTextMaxMaterializedRows = template?.LinkedTextMaxMaterializedRows,
        };
    }

    internal AccessWriterOptions BuildWriterOptions()
    {
        AccessWriterOptions? template = this.optionsExtension.WriterOptions;
        bool isStream = string.IsNullOrWhiteSpace(this.optionsExtension.DatabasePath);
        ReadOnlyMemory<char> password = !string.IsNullOrEmpty(this.optionsExtension.Password)
            ? this.optionsExtension.Password.AsMemory()
            : (template?.Password ?? ReadOnlyMemory<char>.Empty);
        return new AccessWriterOptions
        {
            Password = password,
            MaxEncryptionSpinCount = template?.MaxEncryptionSpinCount ?? 1_000_000,
            MaxEncryptionInfoBytes = template?.MaxEncryptionInfoBytes ?? (1024 * 1024),
            MaxTableDefinitionBytes = template?.MaxTableDefinitionBytes ?? (16 * 1024 * 1024),
            MaxSchemaTraversalBytes = template?.MaxSchemaTraversalBytes ?? (64 * 1024 * 1024),
            UseLockFile = !isStream && (template?.UseLockFile ?? true),
            LockFileUserName = template?.LockFileUserName,
            LockFileMachineName = template?.LockFileMachineName,
            UseByteRangeLocks = template?.UseByteRangeLocks ?? DefaultWriterByteRangeLocks,
            LockTimeoutMilliseconds = template?.LockTimeoutMilliseconds ?? 5000,
            RespectExistingLockFile = template?.RespectExistingLockFile ?? true,
            MaxTransactionPageBudget = template?.MaxTransactionPageBudget ?? 16_384,
            PageCacheSize = template?.PageCacheSize ?? 256,
            CalculatedExpressionCulture = template?.CalculatedExpressionCulture,
            SecureEraseMode = template?.SecureEraseMode ?? SecureEraseMode.None,
            UseTransactionalWrites = template?.UseTransactionalWrites ?? true,
        };
    }

    private static void RegisterConnection(JetClientConnection connection)
    {
        lock (LiveConnectionsGate)
        {
            for (int i = LiveConnections.Count - 1; i >= 0; i--)
            {
                if (!LiveConnections[i].TryGetTarget(out JetClientConnection? existing) || existing.disposed)
                {
                    LiveConnections.RemoveAt(i);
                }
            }

            LiveConnections.Add(new WeakReference<JetClientConnection>(connection));
        }
    }

    private static void UnregisterConnection(JetClientConnection connection)
    {
        lock (LiveConnectionsGate)
        {
            for (int i = LiveConnections.Count - 1; i >= 0; i--)
            {
                if (!LiveConnections[i].TryGetTarget(out JetClientConnection? existing)
                    || existing.disposed
                    || ReferenceEquals(existing, connection))
                {
                    LiveConnections.RemoveAt(i);
                }
            }
        }
    }

    private static int ComputeValueBufferLength(List<IProperty> properties)
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
        List<IProperty> properties,
        int?[] propertyToColumnOrdinal,
        int columnCount,
        IReadOnlySet<IProperty>? requiredProperties,
        IReadOnlyList<IProperty>? extraProperties = null)
    {
        bool[] wanted = new bool[columnCount];
        if (requiredProperties is null)
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

    private static async ValueTask<IReadOnlyList<ValueBuffer>> ReadFromReaderAsync(
        AccessReader reader,
        string tableName,
        IEntityType rootType,
        List<IProperty> properties,
        int bufferLength,
        IReadOnlyList<JetIndexSeekCondition>? seekConditions,
        IReadOnlyDictionary<string, object?> parameters,
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
                requiredProperties);

            IAsyncEnumerable<object[]>? rowSource = null;
            if (seekConditions is { Count: > 0 })
            {
                rowSource = await JetIndexSeekPlanner.TryBuildIndexQueryAsync(
                    reader,
                    resolved,
                    seekConditions,
                    parameters,
                    wantedColumns,
                    cancellationToken).ConfigureAwait(false);
            }

            rowSource ??= reader.Services.Tables.Rows(tableName, wantedColumns, progress: null, cancellationToken);

            List<ValueBuffer> localResults = [];
            await foreach (object[] row in rowSource.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                localResults.Add(CreateValueBuffer(rootType, properties, bufferLength, propertyToColumnOrdinal, row));
            }

            return localResults;
        }

        IReadOnlyList<ColumnMetadata> columns = await reader.GetColumnMetadataAsync(tableName, cancellationToken).ConfigureAwait(false);
        int?[] fallbackColumnMap = BuildPropertyColumnMap(properties, columns);
        List<ValueBuffer> results = [];
        await foreach (object[] row in reader.Rows(tableName, progress: null, cancellationToken).WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            results.Add(CreateValueBuffer(rootType, properties, bufferLength, fallbackColumnMap, row));
        }

        return results;
    }

    private static async ValueTask<IReadOnlyList<ValueBuffer>> ReadByKeysFromReaderAsync(
        AccessReader reader,
        string tableName,
        IEntityType rootType,
        List<IProperty> properties,
        int bufferLength,
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
                    indexedBuffers.Add(CreateValueBuffer(rootType, properties, bufferLength, propertyToColumnOrdinal, row));
                }

                return indexedBuffers;
            }

            HashSet<KeyTupleValue> targetSet = BuildKeyTupleSet(keyTuples);
            List<ValueBuffer> scanResults = [];
            await foreach (object[] row in reader.Services.Tables.Rows(tableName, wantedColumns, progress: null, cancellationToken).WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                ValueBuffer buffer = CreateValueBuffer(rootType, properties, bufferLength, propertyToColumnOrdinal, row);
                if (TryExtractBufferKeyTuple(keyProperties, buffer, out KeyTupleValue candidateKey)
                    && targetSet.Contains(candidateKey))
                {
                    scanResults.Add(buffer);
                }
            }

            return scanResults;
        }

        IReadOnlyList<ColumnMetadata> columns = await reader.GetColumnMetadataAsync(tableName, cancellationToken).ConfigureAwait(false);
        int?[] fallbackColumnMap = BuildPropertyColumnMap(properties, columns);
        HashSet<KeyTupleValue> fallbackTargetSet = BuildKeyTupleSet(keyTuples);
        List<ValueBuffer> fallbackResults = [];
        await foreach (object[] row in reader.Rows(tableName, progress: null, cancellationToken).WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            ValueBuffer buffer = CreateValueBuffer(rootType, properties, bufferLength, fallbackColumnMap, row);
            if (TryExtractBufferKeyTuple(keyProperties, buffer, out KeyTupleValue candidateKey)
                && fallbackTargetSet.Contains(candidateKey))
            {
                fallbackResults.Add(buffer);
            }
        }

        return fallbackResults;
    }

    private static int?[] BuildPropertyColumnMap(
        List<IProperty> properties,
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
        List<IProperty> properties,
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

    private static ValueBuffer CreateValueBuffer(
        IEntityType rootType,
        List<IProperty> properties,
        int bufferLength,
        int?[] propertyToColumnOrdinal,
        object?[] row)
    {
        object?[] buffer = new object?[bufferLength];
        IProperty? discriminatorProperty = rootType.FindDiscriminatorProperty();
        IReadOnlyEntityType? concreteType = null;
        if (discriminatorProperty is not null)
        {
            for (int i = 0; i < properties.Count; i++)
            {
                if (ReferenceEquals(properties[i], discriminatorProperty)
                    && propertyToColumnOrdinal[i] is int discColIdx
                    && (uint)discColIdx < (uint)row.Length)
                {
                    object? discValue = JetEntityTypeMetadata.ConvertFromProvider(discriminatorProperty, row[discColIdx]);
                    int discTargetIdx = discriminatorProperty.GetIndex();
                    if ((uint)discTargetIdx < (uint)buffer.Length)
                    {
                        buffer[discTargetIdx] = discValue;
                    }

                    concreteType = FindMatchingEntityType(rootType, discValue);
                    break;
                }
            }
        }

        for (int i = 0; i < properties.Count; i++)
        {
            IProperty property = properties[i];
            if (concreteType is not null && !property.DeclaringType.IsAssignableFrom(concreteType))
            {
                continue;
            }

            int targetIndex = property.GetIndex();
            if ((uint)targetIndex >= (uint)buffer.Length)
            {
                targetIndex = i;
            }

            if (propertyToColumnOrdinal[i] is int colIdx && (uint)colIdx < (uint)row.Length)
            {
                object? converted = JetEntityTypeMetadata.ConvertFromProvider(property, row[colIdx]);
                if (converted is not null || buffer[targetIndex] is null)
                {
                    buffer[targetIndex] = converted;
                }
            }
        }

        return new ValueBuffer(buffer);
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

    private static async ValueTask<IReadOnlyList<ValueBuffer>> ReadFromActiveWriterAsync(
        AccessWriter writer,
        string tableName,
        IEntityType rootType,
        List<IProperty> properties,
        int bufferLength,
        IReadOnlyList<JetIndexSeekCondition>? seekConditions,
        IReadOnlyDictionary<string, object?> parameters,
        IReadOnlySet<IProperty>? requiredProperties,
        CancellationToken cancellationToken)
    {
        ResolvedTable resolved = await writer.Services.Catalog.ResolveRequiredTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        int?[] propertyToColumnOrdinal = BuildPropertyColumnMapFromTableDef(properties, resolved.Definition);
        bool wrapHyperlinks = resolved.Definition.HasHyperlinkColumns;

        if (seekConditions is { Count: > 0 }
            && JetIndexSeekPlanner.TryBuildExactRowCriteria(seekConditions, parameters, resolved.Definition) is RowCriteria criteria)
        {
            List<LocatedRow>? indexedRows = await writer.Services.Snapshots.TryReadIndexedRowsAsync(resolved, criteria, cancellationToken).ConfigureAwait(false);
            if (indexedRows is not null)
            {
                List<ValueBuffer> seekResults = [];
                foreach (LocatedRow located in indexedRows)
                {
                    if (wrapHyperlinks)
                    {
                        TableReader.WrapHyperlinkColumns(located.Values, resolved.Definition.ClrTypes);
                    }

                    seekResults.Add(CreateValueBuffer(rootType, properties, bufferLength, propertyToColumnOrdinal, located.Values));
                }

                return seekResults;
            }
        }

        bool[] wantedColumns = BuildWantedColumnsMask(
            rootType,
            properties,
            propertyToColumnOrdinal,
            resolved.Definition.Columns.Count,
            requiredProperties);
        bool wrapScannedHyperlinks = wrapHyperlinks
            && TableReader.HasWantedHyperlinkColumn(resolved.Definition.ClrTypes, wantedColumns);
        List<ValueBuffer> results = [];
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

                results.Add(CreateValueBuffer(rootType, properties, bufferLength, propertyToColumnOrdinal, values));
                return new ValueTask<bool>(true);
            },
            cancellationToken).ConfigureAwait(false);

        return results;
    }

    private static async ValueTask<IReadOnlyList<ValueBuffer>> ReadByKeysFromActiveWriterAsync(
        AccessWriter writer,
        string tableName,
        IEntityType rootType,
        List<IProperty> properties,
        int bufferLength,
        IReadOnlyList<IProperty> keyProperties,
        IReadOnlyList<object?[]> keyTuples,
        IReadOnlySet<IProperty>? requiredProperties,
        CancellationToken cancellationToken)
    {
        ResolvedTable resolved = await writer.Services.Catalog.ResolveRequiredTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        int?[] propertyToColumnOrdinal = BuildPropertyColumnMapFromTableDef(properties, resolved.Definition);
        bool wrapHyperlinks = resolved.Definition.HasHyperlinkColumns;

        List<ValueBuffer> indexedBuffers = [];
        bool allIndexedSucceeded = true;
        foreach (object?[] tuple in keyTuples)
        {
            RowCriteria criteria = [];
            for (int i = 0; i < keyProperties.Count; i++)
            {
                string colName = JetEntityTypeMetadata.GetColumnName(keyProperties[i]);
                object? providerVal = JetEntityTypeMetadata.ConvertToProvider(keyProperties[i], tuple[i]);
                criteria.Add(ColumnPredicate.EqualTo(colName, providerVal));
            }

            List<LocatedRow>? rows = await writer.Services.Snapshots.TryReadIndexedRowsAsync(resolved, criteria, cancellationToken).ConfigureAwait(false);
            if (rows is null)
            {
                allIndexedSucceeded = false;
                break;
            }

            foreach (LocatedRow located in rows)
            {
                if (wrapHyperlinks)
                {
                    TableReader.WrapHyperlinkColumns(located.Values, resolved.Definition.ClrTypes);
                }

                indexedBuffers.Add(CreateValueBuffer(rootType, properties, bufferLength, propertyToColumnOrdinal, located.Values));
            }
        }

        if (allIndexedSucceeded)
        {
            return indexedBuffers;
        }

        bool[] wantedColumns = BuildWantedColumnsMask(
            rootType,
            properties,
            propertyToColumnOrdinal,
            resolved.Definition.Columns.Count,
            requiredProperties,
            keyProperties);
        bool wrapScannedHyperlinks = wrapHyperlinks
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

                ValueBuffer buffer = CreateValueBuffer(rootType, properties, bufferLength, propertyToColumnOrdinal, values);
                if (TryExtractBufferKeyTuple(keyProperties, buffer, out KeyTupleValue candidateKey)
                    && targetSet.Contains(candidateKey))
                {
                    scanResults.Add(buffer);
                }

                return new ValueTask<bool>(true);
            },
            cancellationToken).ConfigureAwait(false);

        return scanResults;
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

    private async ValueTask CloseAllReadersForSameTargetAsync()
    {
        await this.CloseReaderAsync().ConfigureAwait(false);

        List<JetClientConnection> peers = [];
        Stream? myStream = this.optionsExtension.DatabaseStream ?? this.resolvedStream;
        lock (LiveConnectionsGate)
        {
            for (int i = LiveConnections.Count - 1; i >= 0; i--)
            {
                if (!LiveConnections[i].TryGetTarget(out JetClientConnection? peer) || peer.disposed)
                {
                    LiveConnections.RemoveAt(i);
                    continue;
                }

                if (ReferenceEquals(peer, this) || peer.activeReader is null)
                {
                    continue;
                }

                if (this.normalizedDatabasePath is not null
                    && peer.normalizedDatabasePath is not null
                    && string.Equals(this.normalizedDatabasePath, peer.normalizedDatabasePath, StringComparison.OrdinalIgnoreCase))
                {
                    peers.Add(peer);
                }
                else if (myStream is not null
                    && ReferenceEquals(myStream, peer.optionsExtension.DatabaseStream ?? peer.resolvedStream))
                {
                    peers.Add(peer);
                }
            }
        }

        foreach (JetClientConnection peer in peers)
        {
            await peer.CloseReaderAsync().ConfigureAwait(false);
        }
    }

    private async ValueTask<AccessWriter> OpenWriterCoreAsync(CancellationToken cancellationToken)
    {
        AccessWriterOptions writerOptions = this.BuildWriterOptions();
        if (!string.IsNullOrWhiteSpace(this.optionsExtension.DatabasePath))
        {
            return await AccessWriter.OpenAsync(this.optionsExtension.DatabasePath, writerOptions, cancellationToken).ConfigureAwait(false);
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

        AccessReaderOptions readerOptions = this.BuildReaderOptions();
        if (!string.IsNullOrWhiteSpace(this.optionsExtension.DatabasePath))
        {
            this.activeReader = await AccessReader.OpenAsync(this.optionsExtension.DatabasePath, readerOptions, cancellationToken).ConfigureAwait(false);
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
