namespace JetDatabaseWriter.Tables;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;

/// <summary>
/// Reads point-in-time snapshots of the writer's own database through a
/// transient, uncached <see cref="AccessReader"/> opened over the same backing
/// file or stream. Used wherever a writer workflow needs fully decoded rows,
/// index metadata, or persisted column properties before it mutates pages.
/// </summary>
/// <param name="databasePath">The database file path, or empty when the writer was opened from a caller-owned stream.</param>
/// <param name="databaseStream">The writer's backing stream.</param>
/// <param name="readThroughStream">
/// <see langword="true"/> to always read through <paramref name="databaseStream"/>
/// even when <paramref name="databasePath"/> is set; required when the stream holds
/// the decrypted inner database of an Office Crypto (Agile) container.
/// </param>
/// <param name="password">The database password.</param>
internal sealed class TableSnapshotReader(
    string databasePath,
    Stream databaseStream,
    bool readThroughStream,
    ReadOnlyMemory<char> password)
{
    /// <summary>
    /// Returns the values of a snapshot row with <see langword="null"/> cells
    /// replaced by <see cref="DBNull.Value"/>, matching the writer's row-array
    /// convention.
    /// </summary>
    /// <param name="row">The snapshot row.</param>
    internal static object[] GetDbNullNormalizedItemArray(DataRow row)
    {
        Guard.NotNull(row, nameof(row));

        object?[] values = row.ItemArray;
        for (int i = 0; i < values.Length; i++)
        {
            values[i] ??= DBNull.Value;
        }

        return (object[])values;
    }

    /// <summary>
    /// Reads every live row of <paramref name="tableName"/> into a
    /// <see cref="DataTable"/> using the schema-rewrite decode rules.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<DataTable> ReadTableSnapshotAsync(string tableName, CancellationToken cancellationToken = default)
    {
        AccessReader reader = await this.OpenReaderAsync(useLockFile: true, cancellationToken).ConfigureAwait(false);
        await using (reader)
        {
            return await reader.ReadDataTableForSchemaRewriteAsync(tableName, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Enumerates <paramref name="tableName"/>'s logical indexes via the same parser
    /// that <see cref="Interfaces.IAccessReader.ListIndexesAsync"/> uses, so schema
    /// rewrites can forward existing index definitions.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<IReadOnlyList<IndexMetadata>> ReadIndexMetadataSnapshotAsync(string tableName, CancellationToken cancellationToken = default)
    {
        AccessReader reader = await this.OpenReaderAsync(useLockFile: true, cancellationToken).ConfigureAwait(false);
        await using (reader)
        {
            return await reader.ListIndexesAsync(tableName, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Reads and parses the <c>MSysObjects.LvProp</c> blob for the catalog row whose
    /// <c>Id</c> low-24 bits equal <paramref name="tdefPage"/>. Returns
    /// <see langword="null"/> when the catalog has no <c>LvProp</c> column or the row
    /// has no property blob.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<ColumnPropertyBlock?> ReadLvPropBlockAsync(long tdefPage, CancellationToken cancellationToken)
    {
        AccessReader reader = await this.OpenReaderAsync(useLockFile: false, cancellationToken).ConfigureAwait(false);
        await using (reader)
        {
            return await reader.ReadLvPropForTableAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        }
    }

    private ValueTask<AccessReader> OpenReaderAsync(bool useLockFile, CancellationToken cancellationToken)
    {
        var options = new AccessReaderOptions
        {
            FileShare = FileShare.ReadWrite,
            ValidateOnOpen = false,
            PageCacheSize = -1,
            UseLockFile = useLockFile,
            Password = password,
        };

        if (!string.IsNullOrEmpty(databasePath) && !readThroughStream)
        {
            return AccessReader.OpenUncachedAsync(databasePath, options, cancellationToken);
        }

        databaseStream.Position = 0;
        return AccessReader.OpenUncachedAsync(databaseStream, options, leaveOpen: true, cancellationToken);
    }
}
