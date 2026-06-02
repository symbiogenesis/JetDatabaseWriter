namespace JetDatabaseWriter.Tests.Fuzz;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Deterministic truncation-matrix robustness coverage. Lops the last
/// <c>N</c> bytes off a real database file and asserts that
/// <see cref="AccessReader.OpenAsync(Stream, AccessReaderOptions?, bool, CancellationToken)"/>
/// (and any subsequent table-list / row-iterate calls) either succeed
/// against the surviving prefix or fail with a <em>controlled</em>
/// exception type — never with an
/// <see cref="OutOfMemoryException"/>,
/// <see cref="StackOverflowException"/>,
/// <see cref="AccessViolationException"/>, or any unmanaged crash.
/// <para>
/// Complements <see cref="AccessReaderFuzzTests"/> (random-byte SharpFuzz)
/// by exercising a deterministic, reproducible mutation pattern that
/// targets the chain-of-trust running from the database header through
/// page-allocation pages, the catalog, and finally the data pages.
/// </para>
/// </summary>
/// <param name="db">The database input.</param>
public sealed class AccessReaderTruncationMatrixTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    /// <summary>
    /// Gets the truncation lengths in bytes. Each entry strips the last <c>N</c>
    /// bytes off the source file.
    /// <list type="bullet">
    ///   <item><description><c>1</c>, <c>16</c>, <c>256</c>: minor tail damage.</description></item>
    ///   <item><description><c>4096</c>, <c>8192</c>: drops the final 1–2 pages (4 KB-page databases).</description></item>
    ///   <item><description><c>65536</c>: drops the final 16 pages.</description></item>
    /// </list>
    /// Larger values are deliberately omitted so the test stays fast and so
    /// every case still leaves a meaningful catalog prefix.
    /// </summary>
    public static TheoryData<int> TruncationLengths =>
    [
        1,
        16,
        256,
        4096,
        8192,
        65536,
    ];

    [Theory]
    [MemberData(nameof(TruncationLengths))]
    public async Task TruncatedTail_OpenAndIterate_NeverThrowsUnmanagedOrOom(int truncateBytes)
    {
        string path = TestDatabases.NorthwindTraders;
        if (!File.Exists(path))
        {
            return;
        }

        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] full = await db.GetFileAsync(path, ct);
        if (truncateBytes >= full.Length)
        {
            return;
        }

        byte[] truncated = new byte[full.Length - truncateBytes];
        Buffer.BlockCopy(full, 0, truncated, 0, truncated.Length);

        await using var stream = new MemoryStream(truncated, writable: false);

        try
        {
            await using AccessReader reader = await AccessReader.OpenAsync(
                stream,
                new AccessReaderOptions { UseLockFile = false },
                leaveOpen: true,
                ct);

            // Best-effort table walk. Per-table failures are tolerated as
            // long as they surface as managed exceptions.
            IReadOnlyList<string> tables = await reader.ListTablesAsync(ct);
            foreach (string t in tables)
            {
                try
                {
                    int rows = 0;
                    await foreach (object[] row in reader.Rows(t, cancellationToken: ct))
                    {
                        _ = row;
                        if (++rows >= 1024)
                        {
                            break;
                        }
                    }
                }
                catch (IOException)
                {
                }
                catch (InvalidDataException)
                {
                }
                catch (InvalidOperationException)
                {
                }
                catch (NotSupportedException)
                {
                }
                catch (ArgumentException)
                {
                }
                catch (FormatException)
                {
                }
                catch (OverflowException)
                {
                }
                catch (IndexOutOfRangeException)
                {
                }
                catch (KeyNotFoundException)
                {
                }
                catch (JetLimitationException)
                {
                }
            }
        }
        catch (IOException)
        {
            // Open-time failure on a heavily-truncated file is acceptable
            // as long as the exception type is managed and well-known.
        }
        catch (InvalidDataException)
        {
            // Open-time failure on a heavily-truncated file is acceptable
            // as long as the exception type is managed and well-known.
        }
        catch (InvalidOperationException)
        {
            // Open-time failure on a heavily-truncated file is acceptable
            // as long as the exception type is managed and well-known.
        }
        catch (NotSupportedException)
        {
            // Open-time failure on a heavily-truncated file is acceptable
            // as long as the exception type is managed and well-known.
        }
        catch (ArgumentException)
        {
            // Open-time failure on a heavily-truncated file is acceptable
            // as long as the exception type is managed and well-known.
        }
        catch (FormatException)
        {
            // Open-time failure on a heavily-truncated file is acceptable
            // as long as the exception type is managed and well-known.
        }
        catch (OverflowException)
        {
            // Open-time failure on a heavily-truncated file is acceptable
            // as long as the exception type is managed and well-known.
        }
        catch (IndexOutOfRangeException)
        {
            // Open-time failure on a heavily-truncated file is acceptable
            // as long as the exception type is managed and well-known.
        }
        catch (KeyNotFoundException)
        {
            // Open-time failure on a heavily-truncated file is acceptable
            // as long as the exception type is managed and well-known.
        }
        catch (JetLimitationException)
        {
            // Open-time failure on a heavily-truncated file is acceptable
            // as long as the exception type is managed and well-known.
        }
    }
}
