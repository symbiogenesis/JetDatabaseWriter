namespace JetDatabaseWriter.Tests.Fuzz;

using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;

/// <summary>Exercises one bounded database input without authorizing any external sources.</summary>
internal static class AccessReaderFuzzExercise
{
    private const int MaxTables = 8;
    private const int MaxRows = 8;
    private const int MaxIndexes = 4;
    private const int MaxEncryptionIterations = 300_000;

    /// <summary>Opens the input with a small fixed password corpus, then exercises schema, rows and indexes.</summary>
    /// <param name="bytes">The hosted driver's bounded database input.</param>
    /// <param name="log">The operation and failure logger.</param>
    /// <param name="cancellationToken">The operation cancellation token.</param>
    /// <returns>The completed exercise.</returns>
    internal static async Task RunAsync(byte[] bytes, Action<string> log, CancellationToken cancellationToken)
    {
        // A shared budget bounds all attempts, including incorrect passwords.
        var encryptionBudget = new EncryptionWorkBudget(MaxEncryptionIterations);
        foreach (string password in (string[])[string.Empty, "Native123", "test", "Test123"])
        {
            cancellationToken.ThrowIfCancellationRequested();
            log("[FuzzCoverage] reader:encryption-attempt");
            try
            {
                await using var stream = new MemoryStream(bytes, writable: false);
                AccessReaderOptions options = CreateOptions(bytes, password.AsMemory(), encryptionBudget, log);
                log("[FuzzCoverage] reader:open");
                await using AccessReader reader = await AccessReader.OpenAsync(stream, options, cancellationToken: cancellationToken);
                log("[FuzzCoverage] reader:open:completed");
                if (password.Length > 0)
                {
                    log("[FuzzCoverage] reader:encrypted:completed");
                }

                await ExerciseReaderAsync(reader, log, cancellationToken);
                return;
            }
            catch (Exception exception) when (IsExpectedInputException(exception))
            {
                log($"[Fuzzing] Expected reader input failure: {exception}");
            }
        }
    }

    /// <summary>Creates bounded options with a mandatory deny policy for every linked source.</summary>
    /// <param name="bytes">The input used to choose deterministic option variations.</param>
    /// <param name="password">The bounded password attempt.</param>
    /// <param name="encryptionBudget">The budget shared across all password attempts.</param>
    /// <param name="log">The operation logger.</param>
    /// <returns>The bounded options.</returns>
    internal static AccessReaderOptions CreateOptions(byte[] bytes, ReadOnlyMemory<char> password, EncryptionWorkBudget encryptionBudget, Action<string> log)
    {
        // Hash the whole input so a common database header does not freeze option selection.
        var random = FuzzRandom.Create(SHA256.HashData(bytes));
        return new AccessReaderOptions
        {
            Password = password,
            UseLockFile = false,
            StrictParsing = random.Next(2) == 0,
            PageCacheSize = random.Next(0, 17),
            PageReadOptimizationMode = (PageReadOptimizationMode)random.Next(3),
            MaxLongValueBytes = 256 * 1024,
            MaxAttachmentContentBytes = 256 * 1024,
            MaxAttachmentContentBytesPerOperation = 1024 * 1024,
            MaxAttachmentDecodeWorkBytesPerOperation = 4 * 1024 * 1024,
            MaxComplexDiscoveryEntries = 1024,
            MaxTableDefinitionBytes = 512 * 1024,
            MaxEncryptionInfoBytes = 64 * 1024,
            MaxEncryptionSpinCount = 100_000,
            MaxTotalEncryptionSpinCount = MaxEncryptionIterations,
            EncryptionWorkBudget = encryptionBudget,
            LinkedSourceMaxDepth = 0,
            LinkedSourcePathValidator = (_, _) =>
            {
                log("[FuzzCoverage] reader:link-denied");
                return false;
            },
            LinkedTextMaxRecordLength = 4096,
            LinkedTextMaxFieldLength = 4096,
            LinkedTextMaxColumnCount = 32,
            LinkedTextMaxSourceFileBytes = 64 * 1024,
            LinkedTextMaxMaterializedRows = MaxRows,
        };
    }

    /// <summary>Returns whether a failure documents malformed or unsupported input.</summary>
    /// <param name="exception">The observed failure.</param>
    /// <returns>Whether the reader may refuse this input with this failure.</returns>
    internal static bool IsExpectedInputException(Exception exception)
        => exception is IOException or InvalidDataException or UnauthorizedAccessException or NotSupportedException or JetLimitationException;

    /// <summary>Reads at most the requested row count and disposes the enumerator immediately.</summary>
    /// <param name="rows">The row sequence.</param>
    /// <param name="maxRows">The maximum rows to consume.</param>
    /// <param name="cancellationToken">The operation cancellation token.</param>
    /// <param name="log">The optional count logger.</param>
    /// <returns>The first row, for constructing a type-correct seek key.</returns>
    internal static async Task<object[]?> ReadBoundedRowsAsync(IAsyncEnumerable<object[]> rows, int maxRows, CancellationToken cancellationToken, Action<string>? log = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxRows);
        object[]? first = null;
        int count = 0;
        await foreach (object[] row in rows.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            first ??= row;
            count++;
            if (count >= maxRows)
            {
                break;
            }
        }

        log?.Invoke($"[Fuzzing] rows consumed: {count}");
        return first;
    }

    private static async Task ExerciseReaderAsync(AccessReader reader, Action<string> log, CancellationToken cancellationToken)
    {
        IReadOnlyList<LinkedTableInfo>? links = null;
        await RunOperationAsync("links", async () => links = await reader.ListLinkedTablesAsync(cancellationToken), log);
        for (int linkNumber = 0; linkNumber < Math.Min(links?.Count ?? 0, MaxTables); linkNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            string linkName = links![linkNumber].Name;
            await RunOperationAsync("linked-schema", async () => _ = await reader.GetColumnMetadataAsync(linkName, cancellationToken), log);
            await RunOperationAsync("linked-rows", async () => _ = await ReadBoundedRowsAsync(reader.Rows(linkName, cancellationToken: cancellationToken), MaxRows, cancellationToken, log), log);
        }

        await RunOperationAsync("relationships", async () => _ = await reader.ListRelationshipsAsync(cancellationToken), log);
        log("[FuzzCoverage] reader:catalog");
        IReadOnlyList<string> tables = await reader.ListTablesAsync(cancellationToken);
        log("[FuzzCoverage] reader:catalog:completed");
        for (int tableNumber = 0; tableNumber < Math.Min(tables.Count, MaxTables); tableNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            string table = tables[tableNumber];
            IReadOnlyList<ColumnMetadata>? columns = null;
            IReadOnlyList<IndexMetadata>? indexes = null;
            object[]? firstRow = null;
            await RunOperationAsync(
                "schema",
                async () =>
            {
                columns = await reader.GetColumnMetadataAsync(table, cancellationToken);
                _ = await reader.GetTableValidationRuleAsync(table, cancellationToken);
                _ = await reader.GetComplexColumnsAsync(table, cancellationToken);
            },
                log);
            await RunOperationAsync("index-list", async () => indexes = await reader.ListIndexesAsync(table, cancellationToken), log);
            await RunOperationAsync(
                "rows",
                async () => firstRow = await ReadBoundedRowsAsync(reader.Rows(table, cancellationToken: cancellationToken), MaxRows, cancellationToken, log),
                log);

            for (int indexNumber = 0; indexNumber < Math.Min(indexes?.Count ?? 0, MaxIndexes); indexNumber++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                IndexMetadata index = indexes![indexNumber];
                object?[]? key = TryCreateKey(index, columns, firstRow);
                await RunOperationAsync(
                    "seek",
                    async () =>
                {
                    IAsyncEnumerable<object[]> rows = key is null
                        ? reader.FromIndex(table, index.Name).ToRowsAsync(cancellationToken)
                        : reader.SeekRowsAsync(table, index.Name, key, cancellationToken);
                    _ = await ReadBoundedRowsAsync(rows, MaxRows, cancellationToken, log);
                },
                    log);
            }
        }
    }

    private static object?[]? TryCreateKey(IndexMetadata index, IReadOnlyList<ColumnMetadata>? columns, object[]? row)
    {
        if (columns is null || row is null || index.Columns.Count is 0 or > 10)
        {
            return null;
        }

        object?[] key = new object?[index.Columns.Count];
        for (int i = 0; i < key.Length; i++)
        {
            ColumnMetadata? match = null;
            foreach (ColumnMetadata column in columns)
            {
                if (string.Equals(column.Name, index.Columns[i].Name, StringComparison.OrdinalIgnoreCase))
                {
                    match = column;
                    break;
                }
            }

            if (match is null || match.Ordinal < 0 || match.Ordinal >= row.Length)
            {
                return null;
            }

            key[i] = row[match.Ordinal];
        }

        return key;
    }

    private static async Task RunOperationAsync(string operation, Func<Task> action, Action<string> log)
    {
        log($"[FuzzCoverage] reader:{operation}");
        try
        {
            await action().ConfigureAwait(false);
            log($"[FuzzCoverage] reader:{operation}:completed");
        }
        catch (Exception exception) when (IsExpectedInputException(exception))
        {
            log($"[Fuzzing] Expected {operation} input failure: {exception}");
        }
    }
}
