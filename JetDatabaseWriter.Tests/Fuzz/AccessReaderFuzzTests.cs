namespace JetDatabaseWriter.Tests.Fuzz;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using SharpFuzz;
using Xunit;

/// <summary>
/// Fuzz test for AccessReader. This test is designed to find crashes and robustness issues by exploring random input data.
/// It is NOT required for full code coverage and should be run as an explicit <c>Category=Fuzz</c> test because it is slow and non-deterministic.
/// For full coverage, prefer targeted unit tests that systematically exercise each feature and branch.
/// </summary>
/// <param name="output">The output.</param>
public class AccessReaderFuzzTests(ITestOutputHelper output)
{
    [Trait("Category", "Fuzz")]
    [Fact(Explicit = true)]
    public void FuzzAccessReader()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Fuzzer.Run(async stream =>
        {
            output.WriteLine($"--- Fuzzing iteration started at {DateTime.UtcNow:O} ---");
            byte[]? fuzzedBytes = null;
            try
            {
                // Read fuzzed input for logging and saving on crash
                fuzzedBytes = new byte[stream.Length];
                await stream.ReadExactlyAsync(fuzzedBytes);
                stream.Position = 0;

                var random = FuzzRandom.Create(fuzzedBytes);
                output.WriteLine($"[Fuzzing] FuzzRandom bytes: {fuzzedBytes.Length}");

                // Preprocess fuzzed input: overlay onto a valid MDB file if needed
                Stream processedStream = await PreprocessFuzzedInputAsync(new MemoryStream(fuzzedBytes), random);
                var options = new AccessReaderOptions();
                await using AccessReader reader = await AccessReader.OpenAsync(processedStream, options, cancellationToken: ct);

                LogReaderState(output, reader);
                await ReadDiscoveredTablesAsync(output, reader, random, ct);
            }
            catch (IOException ex)
            {
                LogExpectedIterationException(output, fuzzedBytes, ex);
            }
            catch (UnauthorizedAccessException ex)
            {
                LogExpectedIterationException(output, fuzzedBytes, ex);
            }
            catch (InvalidDataException ex)
            {
                LogExpectedIterationException(output, fuzzedBytes, ex);
            }
            catch (InvalidOperationException ex)
            {
                LogExpectedIterationException(output, fuzzedBytes, ex);
            }
            catch (NotSupportedException ex)
            {
                LogExpectedIterationException(output, fuzzedBytes, ex);
            }
            catch (ArgumentException ex)
            {
                LogExpectedIterationException(output, fuzzedBytes, ex);
            }
            catch (FormatException ex)
            {
                LogExpectedIterationException(output, fuzzedBytes, ex);
            }
            catch (OverflowException ex)
            {
                LogExpectedIterationException(output, fuzzedBytes, ex);
            }
            catch (IndexOutOfRangeException ex)
            {
                LogExpectedIterationException(output, fuzzedBytes, ex);
            }
            catch (KeyNotFoundException ex)
            {
                LogExpectedIterationException(output, fuzzedBytes, ex);
            }
            catch (JetLimitationException ex)
            {
                LogExpectedIterationException(output, fuzzedBytes, ex);
            }

            output.WriteLine($"""
                --- Fuzzing iteration completed at {DateTime.UtcNow:O} ---

                """);
        });
    }

    private static void LogReaderState(ITestOutputHelper output, AccessReader reader)
    {
        output.WriteLine($"CodePage: {reader.CodePage}");
        output.WriteLine($"DatabaseFormat: {reader.DatabaseFormat}");
        output.WriteLine($"PageReadOptimizationMode: {reader.PageReadOptimizationMode}");
        output.WriteLine($"PageCacheSize: {reader.PageCacheSize}");
        output.WriteLine($"PageSize: {reader.PageSize}");
        output.WriteLine($"DiagnosticsEnabled: {reader.DiagnosticsEnabled}");
        output.WriteLine($"HostDatabasePath: {reader.HostDatabasePath}");
        output.WriteLine($"IoGate: {reader.IoGate}");
        output.WriteLine($"LastDiagnostics: {reader.LastDiagnostics}");
        output.WriteLine($"LinkedSourceOpenOptions: {reader.LinkedSourceOpenOptions}");
    }

    private static async Task ReadDiscoveredTablesAsync(ITestOutputHelper output, AccessReader reader, FuzzRandom random, CancellationToken cancellationToken)
    {
        DataTable tables = await reader.GetTablesAsDataTableAsync(cancellationToken);
        foreach (DataRow row in tables.Rows)
        {
            string? tableName = row["TableName"] as string;
            if (string.IsNullOrEmpty(tableName))
            {
                continue;
            }

            output.WriteLine($"Reading table: {tableName}");
            await RunExpectedReaderOperationAsync(output, $"reading table {tableName}", async () =>
            {
                int maxRows = random.Next(1, 11);
                int count = 0;
                await foreach (object[] dataRow in reader.Rows(tableName, cancellationToken: cancellationToken))
                {
                    _ = dataRow;
                    count++;
                    if (count > maxRows)
                    {
                        break;
                    }
                }
            });

            await RunExpectedReaderOperationAsync(output, $"reading schema for {tableName}", async () =>
            {
                IReadOnlyList<ColumnMetadata> columns = await reader.GetColumnMetadataAsync(tableName, cancellationToken);
                output.WriteLine($"Schema columns: {columns.Count}");
            });

            await RunExpectedReaderOperationAsync(output, $"reading indexes for {tableName}", async () =>
            {
                IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(tableName, cancellationToken);
                output.WriteLine($"Index count: {indexes.Count}");
            });
        }
    }

    private static async Task RunExpectedReaderOperationAsync(ITestOutputHelper output, string operation, Func<Task> action)
    {
        try
        {
            await action().ConfigureAwait(false);
        }
        catch (IOException ex)
        {
            LogExpectedException(output, operation, ex);
        }
        catch (UnauthorizedAccessException ex)
        {
            LogExpectedException(output, operation, ex);
        }
        catch (InvalidDataException ex)
        {
            LogExpectedException(output, operation, ex);
        }
        catch (InvalidOperationException ex)
        {
            LogExpectedException(output, operation, ex);
        }
        catch (NotSupportedException ex)
        {
            LogExpectedException(output, operation, ex);
        }
        catch (ArgumentException ex)
        {
            LogExpectedException(output, operation, ex);
        }
        catch (FormatException ex)
        {
            LogExpectedException(output, operation, ex);
        }
        catch (OverflowException ex)
        {
            LogExpectedException(output, operation, ex);
        }
        catch (IndexOutOfRangeException ex)
        {
            LogExpectedException(output, operation, ex);
        }
        catch (KeyNotFoundException ex)
        {
            LogExpectedException(output, operation, ex);
        }
        catch (JetLimitationException ex)
        {
            LogExpectedException(output, operation, ex);
        }
    }

    private static void LogExpectedException(ITestOutputHelper output, string operation, Exception ex) =>
        output.WriteLine($"""
            [Fuzzing] Expected exception while {operation}: {ex.GetType().Name}
            {ex}
            """);

    private static void LogExpectedIterationException(ITestOutputHelper output, byte[]? fuzzedBytes, Exception ex)
    {
        output.WriteLine($"""
            [Fuzzing] Expected exception during fuzzing iteration: {ex.GetType().Name}
            {ex}
            """);

        if (fuzzedBytes != null)
        {
            SaveCrashInput(output, fuzzedBytes);
        }
    }

    private static void SaveCrashInput(ITestOutputHelper output, byte[] fuzzedBytes)
    {
        try
        {
            string crashDir = Path.Combine(AppContext.BaseDirectory, "..", "..", "Fuzz", "Crashes");
            Directory.CreateDirectory(crashDir);
            string filePath = Path.Combine(crashDir, $"crash_{DateTime.UtcNow:yyyyMMdd_HHmmssfff}.bin");
            File.WriteAllBytes(filePath, fuzzedBytes);
            output.WriteLine($"[Fuzzing] Saved expected-failure input to: {filePath}");
        }
        catch (IOException ex)
        {
            output.WriteLine($"[Fuzzing] Failed to save expected-failure input: {ex}");
        }
        catch (UnauthorizedAccessException ex)
        {
            output.WriteLine($"[Fuzzing] Failed to save expected-failure input: {ex}");
        }
        catch (NotSupportedException ex)
        {
            output.WriteLine($"[Fuzzing] Failed to save expected-failure input: {ex}");
        }
        catch (ArgumentException ex)
        {
            output.WriteLine($"[Fuzzing] Failed to save expected-failure input: {ex}");
        }
    }

    /// <summary>
    /// If the fuzzed input is too small or doesn't look like an MDB/ACCDB file, overlay it onto a valid minimal MDB file.
    /// </summary>
    /// <param name="fuzzed">The fuzzed.</param>
    /// <param name="random">The random.</param>
    private static async Task<Stream> PreprocessFuzzedInputAsync(Stream fuzzed, FuzzRandom? random = null)
    {
        // Known MDB file signatures: 0x00 0x01 0x00 0x00 (Jet3), 0x00 0x01 0x00 0x00 0x00 0x00 0x00 0x00 (Jet4), etc.
        // We'll check the first 4 bytes for Jet3 signature as a simple heuristic.
        const int minHeaderSize = 128; // MDB header is 128 bytes
        byte[] header = new byte[minHeaderSize];
        int read = await fuzzed.ReadAsync(header.AsMemory(0, minHeaderSize));
        fuzzed.Position = 0;

        bool looksLikeMdb = read >= 4 && header[0] == 0x00 && header[1] == 0x01 && header[2] == 0x00 && header[3] == 0x00;
        if (looksLikeMdb && read >= minHeaderSize)
        {
            // Already looks like an MDB file
            return fuzzed;
        }

        // Overlay onto a random valid MDB/ACCDB test fixture if available
        byte[] baseDb = await TryGetRandomTestFixtureAsync(random) ?? GetMinimalValidMdb();
        byte[] fuzzedBytes = new byte[fuzzed.Length];
        await fuzzed.ReadExactlyAsync(fuzzedBytes);

        // Overlay fuzzed bytes onto the base DB (up to the length of the base DB)
        int overlayLen = Math.Min(baseDb.Length, fuzzedBytes.Length);
        Array.Copy(fuzzedBytes, 0, baseDb, 0, overlayLen);
        return new MemoryStream(baseDb, writable: false);
    }

    /// <summary>
    /// Attempts to find and load a random MDB or ACCDB test fixture from the test data directory.
    /// </summary>
    /// <param name="random">The random.</param>
    private static async Task<byte[]?> TryGetRandomTestFixtureAsync(FuzzRandom? random = null)
    {
        try
        {
            // Adjust this path if your test fixtures are elsewhere
            string testDataDir = Path.Combine(AppContext.BaseDirectory, "..", "..", "Fuzz", "Fixtures");
            if (!Directory.Exists(testDataDir))
            {
                return null;
            }

            string[] fileTypes = ["*.mdb", "*.accdb"];

            string[] files = fileTypes
                .SelectMany(pattern => Directory.GetFiles(testDataDir, pattern))
                .ToArray();

            if (files.Length == 0)
            {
                return null;
            }

#pragma warning disable CA5394 // Using non-cryptographic random for fuzz testing is acceptable.
            int idx = random?.Next(0, files.Length) ?? new Random().Next(files.Length);
#pragma warning restore CA5394 // Using non-cryptographic random for fuzz testing is acceptable.
            string chosen = files[idx];
            return await File.ReadAllBytesAsync(chosen);
        }
        catch (IOException)
        {
            return null;
        }
        catch (UnauthorizedAccessException)
        {
            return null;
        }
        catch (NotSupportedException)
        {
            return null;
        }
        catch (ArgumentException)
        {
            return null;
        }
    }

    /// <summary>
    /// Returns a minimal valid MDB file as a byte array. Replace this with your own minimal file as needed.
    /// </summary>
    private static byte[] GetMinimalValidMdb()
    {
        // This is a minimal Jet3 MDB header (first 128 bytes) with essential fields set.
        // For robust fuzzing, prefer using a real file, but this is enough for basic structure recognition.
        byte[] mdb = new byte[4096];

        // Signature (Jet3)
        mdb[0] = 0x00;
        mdb[1] = 0x01;
        mdb[2] = 0x00;
        mdb[3] = 0x00;

        // Page size (bytes 4-5, little endian: 0x0200 = 512 bytes)
        mdb[4] = 0x00;
        mdb[5] = 0x02;

        // Database type (byte 12: 0x01 = Access 2/95/97)
        mdb[12] = 0x01;

        // Engine version (bytes 24-27: 0x01 0x00 0x00 0x00 = Jet 3)
        mdb[24] = 0x01;
        mdb[25] = 0x00;
        mdb[26] = 0x00;
        mdb[27] = 0x00;

        // Set a plausible date for creation (bytes 40-47, FILETIME, optional)
        // These can be left zero for fuzzing.
        // Set a plausible database state (byte 66: 0x01 = consistent)
        mdb[66] = 0x01;

        // Set a plausible code page (bytes 63-64: 0x4E4 = 1252 Latin1)
        mdb[63] = 0xE4;
        mdb[64] = 0x04;

        // The rest can be zero for a minimal stub.
        return mdb;
    }
}
