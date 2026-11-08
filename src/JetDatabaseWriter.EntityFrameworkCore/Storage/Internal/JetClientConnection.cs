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
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;

/// <summary>
/// Default implementation of <see cref="IJetClientConnection"/> coordinating <see cref="AccessReader"/> and <see cref="AccessWriter"/>.
/// </summary>
public sealed class JetClientConnection : IJetClientConnection
{
    private readonly JetOptionsExtension optionsExtension;
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

    /// <inheritdoc/>
    public async ValueTask CloseReaderAsync()
    {
        if (this.activeReader is not null)
        {
            AccessReader reader = this.activeReader;
            this.activeReader = null;
            await reader.DisposeAsync().ConfigureAwait(false);
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

        await this.CloseReaderAsync().ConfigureAwait(false);
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

        await this.CloseReaderAsync().ConfigureAwait(false);
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

        await this.CloseReaderAsync().ConfigureAwait(false);
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
            if (fileInfo.Length < 4096)
            {
                return false;
            }
        }
        else
        {
            Stream stream = this.GetOrCreateStream();
            if (stream.Length < 4096)
            {
                return false;
            }
        }

        try
        {
            _ = await this.GetReaderAsync(cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return false;
        }
    }

    /// <inheritdoc/>
    public async ValueTask<bool> DeleteDatabaseAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(this.disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        await this.DisposeTransactionAsync().ConfigureAwait(false);
        await this.CloseReaderAsync().ConfigureAwait(false);

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
    public async ValueTask<IReadOnlyList<ValueBuffer>> ReadTableValueBuffersAsync(
        IEntityType entityType,
        IReadOnlyList<JetIndexSeekCondition>? seekConditions,
        IReadOnlyDictionary<string, object?> parameters,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entityType);
        ArgumentNullException.ThrowIfNull(parameters);
        ObjectDisposedException.ThrowIf(this.disposed, this);

        string tableName = JetEntityTypeMetadata.GetTableName(entityType);
        IReadOnlyList<IProperty> properties = [.. entityType.GetProperties()];

        if (this.ActiveWriter is not null)
        {
            return await ReadFromActiveWriterAsync(
                this.ActiveWriter,
                tableName,
                properties,
                seekConditions,
                parameters,
                cancellationToken).ConfigureAwait(false);
        }

        AccessReader reader = await this.GetReaderAsync(cancellationToken).ConfigureAwait(false);
        return await ReadFromReaderAsync(
            reader,
            tableName,
            properties,
            seekConditions,
            parameters,
            cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (this.disposed)
        {
            return;
        }

        this.disposed = true;
        await this.DisposeTransactionAsync().ConfigureAwait(false);
        await this.CloseReaderAsync().ConfigureAwait(false);
        if (this.resolvedStream is not null && !this.optionsExtension.LeaveStreamOpen)
        {
            await this.resolvedStream.DisposeAsync().ConfigureAwait(false);
            this.resolvedStream = null;
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (this.disposed)
        {
            return;
        }

        this.disposed = true;
        if (this.resolvedStream is not null && !this.optionsExtension.LeaveStreamOpen)
        {
            this.resolvedStream.Dispose();
            this.resolvedStream = null;
        }
    }

    private static async ValueTask<IReadOnlyList<ValueBuffer>> ReadFromReaderAsync(
        AccessReader reader,
        string tableName,
        IReadOnlyList<IProperty> properties,
        IReadOnlyList<JetIndexSeekCondition>? seekConditions,
        IReadOnlyDictionary<string, object?> parameters,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<ColumnMetadata> columns = await reader.GetColumnMetadataAsync(tableName, cancellationToken).ConfigureAwait(false);
        int?[] propertyToColumnOrdinal = BuildPropertyColumnMap(properties, columns);

        IAsyncEnumerable<object[]>? rowSource = null;
        if (seekConditions is { Count: > 0 })
        {
            IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(tableName, cancellationToken).ConfigureAwait(false);
            rowSource = JetIndexSeekPlanner.TryBuildIndexQuery(reader, tableName, indexes, seekConditions, parameters);
        }

        rowSource ??= reader.Rows(tableName, progress: null, cancellationToken);

        List<ValueBuffer> results = [];
        await foreach (object[] row in rowSource.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            results.Add(CreateValueBuffer(properties, propertyToColumnOrdinal, row));
        }

        return results;
    }

    private static int?[] BuildPropertyColumnMap(
        IReadOnlyList<IProperty> properties,
        IReadOnlyList<ColumnMetadata> columns)
    {
        Dictionary<string, int> columnIndexByName = new(columns.Count, StringComparer.OrdinalIgnoreCase);
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

    private static ValueBuffer CreateValueBuffer(
        IReadOnlyList<IProperty> properties,
        int?[] propertyToColumnOrdinal,
        object?[] row)
    {
        object?[] buffer = new object?[properties.Count];
        for (int i = 0; i < properties.Count; i++)
        {
            IProperty property = properties[i];
            int targetIndex = property.GetIndex();
            if ((uint)targetIndex >= (uint)buffer.Length)
            {
                targetIndex = i;
            }

            if (propertyToColumnOrdinal[i] is int colIdx && (uint)colIdx < (uint)row.Length)
            {
                buffer[targetIndex] = JetEntityTypeMetadata.ConvertFromProvider(property, row[colIdx]);
            }
        }

        return new ValueBuffer(buffer);
    }

    private static async ValueTask<IReadOnlyList<ValueBuffer>> ReadFromActiveWriterAsync(
        AccessWriter writer,
        string tableName,
        IReadOnlyList<IProperty> properties,
        IReadOnlyList<JetIndexSeekCondition>? seekConditions,
        IReadOnlyDictionary<string, object?> parameters,
        CancellationToken cancellationToken)
    {
        ResolvedTable? resolved = await writer.Services.Snapshots.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved is null)
        {
            return [];
        }

        int?[] propertyToColumnOrdinal = BuildPropertyColumnMapFromTableDef(properties, resolved.Definition);

        if (seekConditions is { Count: > 0 }
            && JetIndexSeekPlanner.TryBuildExactRowCriteria(seekConditions, parameters) is RowCriteria criteria)
        {
            List<LocatedRow>? indexedRows = await writer.Services.Snapshots.TryReadIndexedRowsAsync(resolved, criteria, cancellationToken).ConfigureAwait(false);
            if (indexedRows is not null)
            {
                List<ValueBuffer> seekResults = [];
                foreach (LocatedRow located in indexedRows)
                {
                    seekResults.Add(CreateValueBuffer(properties, propertyToColumnOrdinal, located.Values));
                }

                return seekResults;
            }
        }

        bool[] wantedColumns = new bool[resolved.Definition.Columns.Count];
        Array.Fill(wantedColumns, true);
        List<ValueBuffer> results = [];
        await writer.Services.Snapshots.ForEachTypedRowAsync(
            resolved.Entry.TDefPage,
            resolved.Definition,
            wantedColumns,
            (_, values, _) =>
            {
                results.Add(CreateValueBuffer(properties, propertyToColumnOrdinal, values));
                return new ValueTask<bool>(true);
            },
            cancellationToken).ConfigureAwait(false);

        return results;
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

    private AccessReaderOptions BuildReaderOptions()
    {
        AccessReaderOptions? template = this.optionsExtension.ReaderOptions;
        bool isStream = string.IsNullOrWhiteSpace(this.optionsExtension.DatabasePath);
        ReadOnlyMemory<char> password = !string.IsNullOrEmpty(this.optionsExtension.Password)
            ? this.optionsExtension.Password.AsMemory()
            : (template?.Password ?? ReadOnlyMemory<char>.Empty);
        return new AccessReaderOptions
        {
            Password = password,
            UseLockFile = !isStream && (template?.UseLockFile ?? true),
            StrictParsing = template?.StrictParsing ?? true,
            ValidateOnOpen = template?.ValidateOnOpen ?? true,
            PageCacheSize = template?.PageCacheSize ?? 256,
            PageReadOptimizationMode = template?.PageReadOptimizationMode ?? PageReadOptimizationMode.Auto,
            DiagnosticsEnabled = template?.DiagnosticsEnabled ?? false,
            LockTimeoutMilliseconds = template?.LockTimeoutMilliseconds ?? 5000,
        };
    }

    private AccessWriterOptions BuildWriterOptions()
    {
        AccessWriterOptions? template = this.optionsExtension.WriterOptions;
        bool isStream = string.IsNullOrWhiteSpace(this.optionsExtension.DatabasePath);
        ReadOnlyMemory<char> password = !string.IsNullOrEmpty(this.optionsExtension.Password)
            ? this.optionsExtension.Password.AsMemory()
            : (template?.Password ?? ReadOnlyMemory<char>.Empty);
        return new AccessWriterOptions
        {
            Password = password,
            UseLockFile = !isStream && (template?.UseLockFile ?? true),
            UseTransactionalWrites = template?.UseTransactionalWrites ?? true,
            LockTimeoutMilliseconds = template?.LockTimeoutMilliseconds ?? 5000,
        };
    }
}
