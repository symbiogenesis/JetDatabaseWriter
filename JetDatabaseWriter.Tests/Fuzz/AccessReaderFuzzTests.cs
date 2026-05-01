namespace JetDatabaseWriter.Tests.Fuzz;

using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using SharpFuzz;
using Xunit;

#pragma warning disable CA1031 // Catching all exceptions is intentional for fuzz testing.
#pragma warning disable CA5394 // Using non-cryptographic random for fuzz testing is acceptable.

/// <summary>
/// Fuzz test for AccessReader. This test is designed to find crashes and robustness issues by exploring random input data.
/// It is NOT required for full code coverage and is intentionally skipped by default because it is slow and non-deterministic.
/// For full coverage, prefer targeted unit tests that systematically exercise each feature and branch.
/// </summary>
public class AccessReaderFuzzTests(ITestOutputHelper output)
{
    [Trait("Category", "Fuzz")]
    [Fact]
    public async Task FuzzAccessReader()
    {
        Fuzzer.Run(async stream =>
        {
            output.WriteLine($"--- Fuzzing iteration started at {DateTime.UtcNow:O} ---");
            byte[]? fuzzedBytes = null;
            FuzzRandom? random = null;
            try
            {
                // Read fuzzed input for logging and saving on crash
                fuzzedBytes = new byte[stream.Length];
                await stream.ReadExactlyAsync(fuzzedBytes);
                stream.Position = 0;

                random = FuzzRandom.Create(fuzzedBytes);
                output.WriteLine($"[Fuzzing] FuzzRandom bytes: {fuzzedBytes.Length}");

                // Preprocess fuzzed input: overlay onto a valid MDB file if needed
                var processedStream = await PreprocessFuzzedInputAsync(new System.IO.MemoryStream(fuzzedBytes), random);
                var options = new AccessReaderOptions();
                await using var reader = await AccessReader.OpenAsync(processedStream, options);

                // Try accessing more properties/methods for broader coverage
                try
                {
                    output.WriteLine($"CodePage: {reader.CodePage}");
                    output.WriteLine($"DatabaseFormat: {reader.DatabaseFormat}");
                    output.WriteLine($"ParallelPageReadsEnabled: {reader.ParallelPageReadsEnabled}");
                    output.WriteLine($"PageCacheSize: {reader.PageCacheSize}");
                    output.WriteLine($"PageSize: {reader.PageSize}");
                    output.WriteLine($"DiagnosticsEnabled: {reader.DiagnosticsEnabled}");
                    output.WriteLine($"HostDatabasePath: {reader.HostDatabasePath}");
                    output.WriteLine($"IoGate: {reader.IoGate}");
                    output.WriteLine($"IsJet4: {reader.IsJet4}");
                    output.WriteLine($"LastDiagnostics: {reader.LastDiagnostics}");
                    output.WriteLine($"LinkedSourceOpenOptions: {reader.LinkedSourceOpenOptions}");
                    output.WriteLine($"LinkedSourcePathAllowlist: {reader.LinkedSourcePathAllowlist}");
                    output.WriteLine($"LinkedSourcePathValidator: {reader.LinkedSourcePathValidator}");
                }
                catch (Exception ex)
                {
                    output.WriteLine($"Exception accessing properties: {ex.GetType().Name}\n{ex}");
                }

                // Try reading all tables
                DataTable tables = await reader.GetTablesAsDataTableAsync();
                foreach (DataRow row in tables.Rows)
                {
                    string? tableName = row["TableName"] as string;
                    if (string.IsNullOrEmpty(tableName))
                    {
                        continue;
                    }

                    output.WriteLine($"Reading table: {tableName}");
                    try
                    {
                        // Randomize the number of rows to read (1-10)
                        int maxRows = random.Next(1, 11);
                        int count = 0;
                        await foreach (object[] dataRow in reader.Rows(tableName!))
                        {
                            count++;
                            if (count > maxRows)
                            {
                                break;
                            }
                        }

                        // Try reading schema and columns
                        try
                        {
                            var columns = await reader.GetColumnMetadataAsync(tableName!);
                            output.WriteLine($"Schema columns: {columns?.Count}");
                        }
                        catch (Exception ex)
                        {
                            output.WriteLine($"Exception reading schema for {tableName}: {ex.GetType().Name}\n{ex}");
                        }

                        // Try reading indexes
                        try
                        {
                            var indexes = await reader.ListIndexesAsync(tableName!);
                            output.WriteLine($"Index count: {indexes?.Count}");
                        }
                        catch (Exception ex)
                        {
                            output.WriteLine($"Exception reading indexes for {tableName}: {ex.GetType().Name}\n{ex}");
                        }
                    }
                    catch (Exception ex)
                    {
                        output.WriteLine($"Exception reading table {tableName}: {ex.GetType().Name}\n{ex}");
                    }
                }
            }
            catch (Exception ex)
            {
                output.WriteLine($"[Fuzzing] Caught exception during fuzzing iteration: {ex.GetType().Name}\n{ex}");
                if (fuzzedBytes != null)
                {
                    try
                    {
                        var crashDir = System.IO.Path.Combine(AppContext.BaseDirectory, "..", "..", "Fuzz", "Crashes");
                        System.IO.Directory.CreateDirectory(crashDir);
                        var filePath = System.IO.Path.Combine(crashDir, $"crash_{DateTime.UtcNow:yyyyMMdd_HHmmssfff}.bin");
                        System.IO.File.WriteAllBytes(filePath, fuzzedBytes);
                        output.WriteLine($"[Fuzzing] Saved crashing input to: {filePath}");
                    }
                    catch (Exception saveEx)
                    {
                        output.WriteLine($"[Fuzzing] Failed to save crashing input: {saveEx}");
                    }
                }
            }

            output.WriteLine($"--- Fuzzing iteration completed at {DateTime.UtcNow:O} ---\n");
        });
    }

    /// <summary>
    /// If the fuzzed input is too small or doesn't look like an MDB/ACCDB file, overlay it onto a valid minimal MDB file.
    /// </summary>
    private static async Task<System.IO.Stream> PreprocessFuzzedInputAsync(System.IO.Stream fuzzed, FuzzRandom? random = null)
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
        return new System.IO.MemoryStream(baseDb, writable: false);
    }

    /// <summary>
    /// Attempts to find and load a random MDB or ACCDB test fixture from the test data directory.
    /// </summary>
    private static async Task<byte[]?> TryGetRandomTestFixtureAsync(FuzzRandom? random = null)
    {
        try
        {
            // Adjust this path if your test fixtures are elsewhere
            string testDataDir = System.IO.Path.Combine(AppContext.BaseDirectory, "..", "..", "Fuzz", "Fixtures");
            if (!System.IO.Directory.Exists(testDataDir))
            {
                return null;
            }

            string[] fileTypes = ["*.mdb", "*.accdb"];

            var files = fileTypes
                .SelectMany(pattern => System.IO.Directory.GetFiles(testDataDir, pattern))
                .ToArray();

            if (files.Length == 0)
            {
                return null;
            }

            int idx = random?.Next(0, files.Length) ?? new Random().Next(files.Length);
            string chosen = files[idx];
            return await System.IO.File.ReadAllBytesAsync(chosen);
        }
        catch
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
