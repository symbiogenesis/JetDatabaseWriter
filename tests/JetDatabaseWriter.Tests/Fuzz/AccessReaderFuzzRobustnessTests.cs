namespace JetDatabaseWriter.Tests.Fuzz;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.TestSupport;
using Xunit;

#pragma warning disable CA5394 // System.Random is fine — no security context here, just deterministic fuzz seeds.

/// <summary>
/// <para>
/// Robustness tests that mirror the mdbtools libFuzzer harness
/// (https://github.com/mdbtools/mdbtools/blob/dev/src/fuzz/fuzz_mdb.c):
/// open arbitrary bytes as a database, walk the catalog, read every column, and
/// stream every row. The harness asserts only that the library never crashes the
/// process — every failure mode must surface as a documented exception.
/// </para>
/// <para>
/// We seed the corpus with deterministic byte patterns, single-byte mutations of
/// a real header, and pseudo-random buffers (fixed seed for reproducibility).
/// </para>
/// </summary>
public sealed class AccessReaderFuzzRobustnessTests
{
    public static TheoryData<byte[]> EmptyAndTinyBuffers =>
    [
        Array.Empty<byte>(),
        new byte[1],
        new byte[16],
        new byte[2048], // smaller than a single Jet4 page
        new byte[4096], // exactly one Jet4 page, all zeros
    ];

    [Theory]
    [MemberData(nameof(EmptyAndTinyBuffers))]
    public async Task OpenAsync_TinyOrZeroedBuffer_ThrowsDocumentedException(byte[] data)
    {
        await using var ms = new MemoryStream(data, writable: false);

        Exception? exception = await Record.ExceptionAsync(async () =>
        {
            await using AccessReader reader = await AccessReader.OpenAsync(
                ms,
                new AccessReaderOptions { UseLockFile = false, ValidateOnOpen = true },
                leaveOpen: true,
                cancellationToken: TestContext.Current.CancellationToken);
        });

        Assert.NotNull(exception);
        Assert.True(AccessReaderFuzzExercise.IsExpectedInputException(exception), exception.ToString());
    }

    [Fact]
    public async Task OpenAsync_RandomBytes_NeverCrashes()
    {
        // Fixed seed so failures reproduce. 64 trials of varying sizes covering
        // sub-page, page-aligned, and multi-page payloads.
        var rng = new Random(0xACCD8);
        int[] sizes = [4, 64, 512, 2048, 4096, 4097, 8192, 16384];

        for (int trial = 0; trial < 64; trial++)
        {
            int size = sizes[trial % sizes.Length];
            byte[] data = new byte[size];
            rng.NextBytes(data);

            await TryParseAsync(data, TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task OpenAsync_MutatedRealHeader_NeverCrashes()
    {
        // Build a real, known-good empty Jet4 database, then flip one byte in the
        // first page at every offset and verify the failure (or success) is
        // bounded by the documented exception set. This exercises the same parser
        // surface that fuzz_mdb.c hits via mdb_open_buffer.
        byte[] golden;
        {
            await using var seed = new MemoryStream();
            await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
                seed,
                DatabaseFormat.Jet4Mdb,
                leaveOpen: true,
                cancellationToken: TestContext.Current.CancellationToken))
            {
                // empty database is enough — fuzzer cares about the header parser
            }

            golden = seed.ToArray();
        }

        Assert.True(golden.Length >= 4096, "expected at least one full Jet4 page");

        // Sample offsets across the header rather than exhaustively flipping —
        // exhaustive would be slow and the parser surface is well-covered by
        // strided coverage. Step chosen so we hit the magic, version, codepage,
        // db-key, encryption flag, and several catalog pointers.
        for (int offset = 0; offset < 4096; offset += 7)
        {
            byte[] mutant = (byte[])golden.Clone();
            mutant[offset] ^= 0xFF;
            await TryParseAsync(mutant, TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task OpenAsync_TruncatedRealDatabase_NeverCrashes()
    {
        // Build a real database with one populated table, then re-open progressively
        // truncated copies. Each truncation must surface as a documented exception.
        byte[] golden;
        {
            await using var seed = new MemoryStream();
            await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
                seed,
                DatabaseFormat.Jet4Mdb,
                leaveOpen: true,
                cancellationToken: TestContext.Current.CancellationToken))
            {
                await writer.CreateTableAsync(
                    "T",
                    [
                        new("Id", typeof(int)),
                        new("Label", typeof(string), maxLength: 32),
                    ],
                    TestContext.Current.CancellationToken);

                var rows = new List<object[]>(5);
                for (int i = 0; i < 5; i++)
                {
                    rows.Add([i, $"row-{i}"]);
                }

                await writer.InsertRowsAsync("T", rows, TestContext.Current.CancellationToken);
            }

            golden = seed.ToArray();
        }

        // Truncate at 1/4, 1/2, 3/4, and one byte short of full length.
        foreach (int cut in (int[])[golden.Length / 4, golden.Length / 2, golden.Length * 3 / 4, golden.Length - 1])
        {
            if (cut <= 0)
            {
                continue;
            }

            byte[] truncated = new byte[cut];
            Array.Copy(golden, truncated, cut);
            await TryParseAsync(truncated, TestContext.Current.CancellationToken);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Schema_TDefContinuationBeyondFile_ThrowsEndOfStream(DatabaseFormat format)
    {
        await using var seed = new MemoryStream();
        long definitionPage;
        int pageSize;
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            seed, format, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int))], TestContext.Current.CancellationToken);
            CatalogEntry? entry = await writer.Services.Catalog.GetCatalogEntryAsync("T", TestContext.Current.CancellationToken);
            Assert.NotNull(entry);
            definitionPage = entry.TDefPage;
            pageSize = writer.Database.Format.PageSize;
        }

        await using (var valid = new PageTraceStream(seed, pageSize))
        await using (AccessReader reader = await AccessReader.OpenAsync(
            valid, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Single(await reader.GetColumnMetadataAsync("T", TestContext.Current.CancellationToken));
        }

        byte[] mutated = seed.ToArray();
        BinaryPrimitives.WriteUInt32LittleEndian(mutated.AsSpan(checked((int)(definitionPage * pageSize)) + 4, 4), uint.MaxValue);
        await using var memory = new MemoryStream(mutated, writable: false);
        await using var source = new PageTraceStream(memory, pageSize);
        await Assert.ThrowsAsync<EndOfStreamException>(async () =>
        {
            await using AccessReader reader = await AccessReader.OpenAsync(
                source, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
            _ = await reader.GetColumnMetadataAsync("T", TestContext.Current.CancellationToken);
        });
    }

    /// <summary>
    /// Open <paramref name="data"/>, walk every table's columns and rows, and
    /// swallow only the documented failure types. Anything else escapes and
    /// fails the test (the harness contract).
    /// </summary>
    /// <param name="data">The data bytes or values.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private static async Task TryParseAsync(byte[] data, System.Threading.CancellationToken cancellationToken)
    {
        await using var ms = new MemoryStream(data, writable: false);
        try
        {
            await using AccessReader reader = await AccessReader.OpenAsync(
                ms,
                new AccessReaderOptions { UseLockFile = false, ValidateOnOpen = true },
                leaveOpen: true,
                cancellationToken: cancellationToken);

            IReadOnlyList<string> tables = await reader.ListTablesAsync(cancellationToken);
            foreach (string t in tables)
            {
                try
                {
                    _ = await reader.GetColumnMetadataAsync(t, cancellationToken);
                    await foreach (object[] row in reader.Rows(t, cancellationToken: cancellationToken))
                    {
                        // drain
                        _ = row;
                    }
                }
                catch (Exception exception) when (AccessReaderFuzzExercise.IsExpectedInputException(exception))
                {
                }
            }
        }
        catch (Exception exception) when (AccessReaderFuzzExercise.IsExpectedInputException(exception))
        {
        }
    }
}
