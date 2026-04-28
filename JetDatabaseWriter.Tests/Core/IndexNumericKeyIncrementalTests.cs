namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention.

/// <summary>
/// Tests for T_NUMERIC incremental — drop the <c>T_NUMERIC</c> bail from the
/// incremental B-tree fast paths. The fast paths now use the column's DECLARED
/// scale (persisted at TDEF column-descriptor offsets 11/12 — the same
/// bytes Microsoft Access uses) as the canonical sort-key scale; values
/// whose natural scale exceeds the declared scale are rounded
/// half-to-even, mirroring Access semantics.
/// </summary>
public sealed class IndexNumericKeyIncrementalTests
{
    [Fact]
    public async Task ColumnMetadata_DecimalColumn_RoundTripsDeclaredPrecisionAndScale()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Amount", typeof(decimal)) { NumericPrecision = 18, NumericScale = 4 },
                ],
                ct);
        }

        await using var reader = await OpenReaderAsync(stream);
        IReadOnlyList<ColumnMetadata> cols = await reader.GetColumnMetadataAsync("T", ct);
        ColumnMetadata amount = cols.Single(c => c.Name == "Amount");
        Assert.Equal((byte)18, amount.NumericPrecision);
        Assert.Equal((byte)4, amount.NumericScale);
    }

    [Fact]
    public async Task ColumnMetadata_DecimalColumn_DefaultsMatchAccessUiDefaults()
    {
        // Access "Number → Decimal" UI default is precision=18, scale=0.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Amount", typeof(decimal))],
                ct);
        }

        await using var reader = await OpenReaderAsync(stream);
        ColumnMetadata amount = (await reader.GetColumnMetadataAsync("T", ct)).Single();
        Assert.Equal((byte)18, amount.NumericPrecision);
        Assert.Equal((byte)0, amount.NumericScale);
    }

    [Fact]
    public async Task IncrementalSingleLeafSplice_NumericKey_DoesNotAppendFreshTree()
    {
        // surgical single-leaf splice on a T_NUMERIC-keyed index. Before T_NUMERIC incremental
        // this bailed to the bulk path which always allocates a fresh
        // leaf (and possibly intermediates) at the end of the file.
        // After T_NUMERIC incremental the path participates: one new leaf is appended per
        // splice (same shape as the surgical non-numeric fast path).
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Amount", typeof(decimal)) { NumericScale = 2 }],
                [new IndexDefinition("IX_Amount", "Amount")],
                ct);

            await writer.InsertRowsAsync(
                "T",
                [
                    [1.00m],
                    [3.00m],
                    [5.00m],
                ],
                ct);
        }

        long lengthAfterBulk = stream.Length;

        await using (var writer = await OpenWriterAsync(stream))
        {
            // One row insert that lands between two existing keys ⇒
            // single-leaf splice.
            await writer.InsertRowAsync("T", [2.00m], ct);
        }

        long lengthAfterSplice = stream.Length;

        // The splice must be cheap: one extra leaf page (4096 bytes for
        // ACE) plus possibly a few small page-allocation bookkeeping
        // pages — definitely under one full page-table extent (8 pages
        // = 32 KB). A regression to the bulk bulk rebuild rebuild on every
        // mutation would re-emit the entire tree from a fresh
        // page-number range.
        Assert.True(
            lengthAfterSplice - lengthAfterBulk <= 8 * 4096,
            $"Splice grew file by {lengthAfterSplice - lengthAfterBulk} bytes; expected ≤ 32 KB.");

        // Round-trip verification.
        await using var reader = await OpenReaderAsync(stream);
        List<object[]> rows = new();
        await foreach (object[] row in reader.Rows("T", cancellationToken: ct))
        {
            rows.Add(row);
        }

        Assert.Equal(4, rows.Count);
        Assert.Contains(rows, r => (decimal)r[0] == 2.00m);
    }

    [Fact]
    public async Task IncrementalTailAppend_NumericKey_RewritesTailLeafInPlace()
    {
        // append-only tail rewrite on a T_NUMERIC-keyed index. With
        // declared scale=2 every value is canonical at scale 2; the
        // append fast path engages because every new key sorts strictly
        // after the current tail max.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Seq", typeof(decimal)) { NumericScale = 2 }],
                [new IndexDefinition("IX_Seq", "Seq")],
                ct);

            // Bulk-load enough rows to force a multi-level tree so the
            // append optimisation is meaningfully different from the
            // single-leaf splice path.
            object[][] bulk = Enumerable.Range(1, 400)
                .Select(i => new object[] { (decimal)i + 0.50m })
                .ToArray();
            await writer.InsertRowsAsync("T", bulk, ct);
        }

        long beforeAppend = stream.Length;

        await using (var writer = await OpenWriterAsync(stream))
        {
            // Pure append — sorts after every existing key.
            await writer.InsertRowAsync("T", [9999.00m], ct);
        }

        long afterAppend = stream.Length;

        // Tail-leaf rewrite is in-place: at most a handful of overhead
        // pages should be added. A bail to bulk rebuild would re-emit the whole
        // tree (~50+ leaves at this size).
        long delta = afterAppend - beforeAppend;
        Assert.True(
            delta <= 8 * 4096,
            $"Append grew file by {delta} bytes; expected ≤ 32 KB (in-place tail rewrite).");

        await using var reader = await OpenReaderAsync(stream);
        long count = await reader.Rows("T", cancellationToken: ct).CountAsync(cancellationToken: ct);
        Assert.Equal(401, count);
    }

    [Fact]
    public async Task IncrementalDelete_NumericKey_DoesNotBailToBulk()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("Amount", typeof(decimal)) { NumericScale = 2 },
                ],
                [new IndexDefinition("IX_Amount", "Amount")],
                ct);

            await writer.InsertRowsAsync(
                "T",
                [
                    [1, 10.00m],
                    [2, 20.00m],
                    [3, 30.00m],
                ],
                ct);
        }

        long beforeDelete = stream.Length;

        await using (var writer = await OpenWriterAsync(stream))
        {
            int deleted = await writer.DeleteRowsAsync("T", "Id", 2, ct);
            Assert.Equal(1, deleted);
        }

        long afterDelete = stream.Length;
        long delta = afterDelete - beforeDelete;

        Assert.True(
            delta <= 8 * 4096,
            $"Delete grew file by {delta} bytes; expected ≤ 32 KB (incremental rewrite).");

        await using var reader = await OpenReaderAsync(stream);
        long count = await reader.Rows("T", cancellationToken: ct).CountAsync(cancellationToken: ct);
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task UniqueNumericIndex_DuplicateAtDeclaredScale_ThrowsPreInsert()
    {
        // 1.50 and 1.5 are the same numeric value at any scale ≥ 1.
        // With declared scale=2 (the column's canonical scale), the
        // unique-index pre-insert check must catch the duplicate
        // before the row hits disk.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using var writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            "T",
            [new ColumnDefinition("Amount", typeof(decimal)) { NumericScale = 2 }],
            [new IndexDefinition("UQ_Amount", "Amount") { IsUnique = true }],
            ct);

        await writer.InsertRowAsync("T", [1.50m], ct);
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync("T", [1.5m], ct));
    }

    [Fact]
    public async Task UniqueNumericIndex_RoundsAboveDeclaredScale_ToCanonicalCollision()
    {
        // Per Access semantics: a Decimal column with declared scale=0
        // stores every cell as an integer. The index sort key is encoded
        // at scale=0, so 1.4 (rounds to 1) and 1.6 (rounds to 2) are
        // distinct, but 1.6 and 2.0 collide.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using var writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            "T",
            [new ColumnDefinition("N", typeof(decimal))],
            [new IndexDefinition("UQ_N", "N") { IsUnique = true }],
            ct);

        await writer.InsertRowAsync("T", [1.4m], ct); // → 1 at scale 0
        await writer.InsertRowAsync("T", [1.6m], ct); // → 2 at scale 0
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync("T", [2.0m], ct));
    }

    [Fact]
    public async Task NumericPrecisionOutOfRange_ThrowsArgumentOutOfRange()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using var writer = await OpenWriterAsync(stream);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("N", typeof(decimal)) { NumericPrecision = 30 }],
                ct));
    }

    [Fact]
    public async Task NumericScaleAboveDeclaredPrecision_ThrowsArgumentOutOfRange()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using var writer = await OpenWriterAsync(stream);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("N", typeof(decimal)) { NumericPrecision = 5, NumericScale = 6 }],
                ct));
    }

    private static async ValueTask<MemoryStream> CreateFreshAccdbStreamAsync()
    {
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
        }

        ms.Position = 0;
        return ms;
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(
            stream,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
    }
}
