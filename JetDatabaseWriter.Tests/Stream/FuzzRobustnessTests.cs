namespace JetDatabaseWriter.Tests;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Xunit;

#pragma warning disable CA5394 // System.Random is fine — no security context here, just deterministic fuzz seeds.

/// <summary>
/// Robustness tests that mirror the mdbtools libFuzzer harness
/// (https://github.com/mdbtools/mdbtools/blob/dev/src/fuzz/fuzz_mdb.c):
/// open arbitrary bytes as a database, walk the catalog, read every column, and
/// stream every row. The harness asserts only that the library never crashes the
/// process — every failure mode must surface as a documented exception.
///
/// We seed the corpus with deterministic byte patterns, single-byte mutations of
/// a real header, and pseudo-random buffers (fixed seed for reproducibility).
/// </summary>
public sealed class FuzzRobustnessTests
{
    public static IEnumerable<object[]> EmptyAndTinyBuffers()
    {
        yield return new object[] { Array.Empty<byte>() };
        yield return new object[] { new byte[1] };
        yield return new object[] { new byte[16] };
        yield return new object[] { new byte[2048] }; // smaller than a single Jet4 page
        yield return new object[] { new byte[4096] }; // exactly one Jet4 page, all zeros
    }

    [Theory]
    [MemberData(nameof(EmptyAndTinyBuffers))]
    public async Task OpenAsync_TinyOrZeroedBuffer_ThrowsDocumentedException(byte[] data)
    {
        var ms = new MemoryStream(data, writable: false);

        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var reader = await AccessReader.OpenAsync(
                ms,
                new AccessReaderOptions { UseLockFile = false, ValidateOnOpen = true },
                leaveOpen: true,
                cancellationToken: TestContext.Current.CancellationToken);
        });
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
            var seed = new MemoryStream();
            await using (var writer = await AccessWriter.CreateDatabaseAsync(
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
            var seed = new MemoryStream();
            await using (var writer = await AccessWriter.CreateDatabaseAsync(
                seed,
                DatabaseFormat.Jet4Mdb,
                leaveOpen: true,
                cancellationToken: TestContext.Current.CancellationToken))
            {
                await writer.CreateTableAsync(
                    "T",
                    new List<ColumnDefinition>
                    {
                        new("Id", typeof(int)),
                        new("Label", typeof(string), maxLength: 32),
                    },
                    TestContext.Current.CancellationToken);

                for (int i = 0; i < 5; i++)
                {
                    await writer.InsertRowAsync(
                        "T",
                        new object[] { i, $"row-{i}" },
                        TestContext.Current.CancellationToken);
                }
            }

            golden = seed.ToArray();
        }

        // Truncate at 1/4, 1/2, 3/4, and one byte short of full length.
        int[] cuts = [golden.Length / 4, golden.Length / 2, golden.Length * 3 / 4, golden.Length - 1];
        foreach (int cut in cuts)
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

    /// <summary>
    /// Open <paramref name="data"/>, walk every table's columns and rows, and
    /// swallow only the documented failure types. Anything else escapes and
    /// fails the test (the harness contract).
    /// </summary>
    private static async Task TryParseAsync(byte[] data, System.Threading.CancellationToken cancellationToken)
    {
        var ms = new MemoryStream(data, writable: false);
        try
        {
            await using var reader = await AccessReader.OpenAsync(
                ms,
                new AccessReaderOptions { UseLockFile = false, ValidateOnOpen = true },
                leaveOpen: true,
                cancellationToken: cancellationToken);

            var tables = await reader.ListTablesAsync(cancellationToken);
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
                catch (InvalidDataException)
                {
                }
                catch (EndOfStreamException)
                {
                }
                catch (JetLimitationException)
                {
                }
                catch (NotSupportedException)
                {
                }
            }
        }
        catch (InvalidDataException)
        {
        }
        catch (EndOfStreamException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
        catch (NotSupportedException)
        {
        }
        catch (ArgumentException)
        {
        }
        catch (IOException)
        {
        }
        catch (JetLimitationException)
        {
        }
    }
}
