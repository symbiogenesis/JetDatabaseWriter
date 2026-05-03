namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Internal;
using JetDatabaseWriter.Models;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Verifies the TDEF-page invariant that every per-real-idx
/// <c>num_idx_rows</c> counter (4-byte LE u32 at offset
/// <c>BlockEnd + i*RealIdxEntrySz + 4</c>) tracks the table-level
/// <c>row_count</c> field at <see cref="Constants.TableDefinition.RowCountOffset"/>.
///
/// This invariant is required for DAO Compact &amp; Repair: when the two
/// disagree DAO aborts compact with "could not find the object 'MSysDb'".
/// See <c>docs/design/round-trip-test-failures.md</c> hypothesis #2
/// and the corresponding fix in <c>AccessWriter.UpdateRowCountAsync</c>.
///
/// The invariant is exercised in two paths:
/// <list type="bullet">
///   <item><c>CreateTableAsync</c> writes a new <c>MSysObjects</c> row → the
///   MSysObjects TDEF must bump both <c>row_count</c> and the per-real-idx
///   counters in lock-step.</item>
///   <item><c>InsertRowAsync</c> writes a user-table row → the user-table
///   TDEF must do the same.</item>
/// </list>
/// </summary>
public sealed class TdefRowCountSyncTests
{
    private readonly CancellationToken ct = TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateTable_BumpsMSysObjectsPerRealIdxCounters_InLockStepWithRowCount(DatabaseFormat format)
    {
        var ms = new MemoryStream();

        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync(
                "T1", [new ColumnDefinition("A", typeof(int))], ct);
            await writer.CreateTableAsync(
                "T2", [new ColumnDefinition("B", typeof(int))], ct);
            await writer.CreateTableAsync(
                "T3", [new ColumnDefinition("C", typeof(int))], ct);
        }

        AssertAllTdefsHaveSyncedCounters(ms.ToArray(), format);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task InsertRows_BumpUserTablePerRealIdxCounters_InLockStepWithRowCount(DatabaseFormat format)
    {
        var ms = new MemoryStream();

        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                ct);

            for (int i = 1; i <= 7; i++)
            {
                await writer.InsertRowAsync("T", [i], ct);
            }
        }

        AssertAllTdefsHaveSyncedCounters(ms.ToArray(), format);
    }

    /// <summary>
    /// Walks every head-of-chain TDEF page in <paramref name="fileBytes"/>
    /// (page-type 0x02 with <c>tdef_id @ 4 == 0</c>) and asserts the
    /// per-real-idx <c>num_idx_rows</c> field equals <c>row_count</c> for
    /// every real-idx slot. Indexes contain every live row, so the two
    /// counters must always agree.
    /// </summary>
    private static void AssertAllTdefsHaveSyncedCounters(byte[] fileBytes, DatabaseFormat format)
    {
        int pageSize = format == DatabaseFormat.Jet3Mdb
            ? Constants.PageSizes.Jet3
            : Constants.PageSizes.Jet4;

        var layout = TDefHeaderLayout.For(format);

        int tdefsChecked = 0;
        for (int p = 0; p < fileBytes.Length / pageSize; p++)
        {
            int o = p * pageSize;

            // Head-of-chain TDEF: page_type == 0x02 and tdef_id (the
            // chain-continuation pointer at +4) == 0.
            if (fileBytes[o] != 0x02 || BinaryPrimitives.ReadUInt32LittleEndian(
                    fileBytes.AsSpan(o + 4, 4)) != 0)
            {
                continue;
            }

            uint rowCount = BinaryPrimitives.ReadUInt32LittleEndian(
                fileBytes.AsSpan(o + Constants.TableDefinition.RowCountOffset, 4));

            int numRealIdx = BinaryPrimitives.ReadInt32LittleEndian(
                fileBytes.AsSpan(o + layout.NumRealIdx, 4));

            // Sanity bound — Jet caps real-idx slots well below this.
            Assert.InRange(numRealIdx, 0, 1000);

            for (int i = 0; i < numRealIdx; i++)
            {
                int countOff = o + layout.BlockEnd + (i * layout.RealIdxEntrySz) + 4;
                uint perIdxCount = BinaryPrimitives.ReadUInt32LittleEndian(
                    fileBytes.AsSpan(countOff, 4));

                Assert.True(
                    perIdxCount == rowCount,
                    FormattableString.Invariant(
                        $"TDEF page {p}, real-idx {i}: num_idx_rows={perIdxCount} but row_count={rowCount} (format={format}). Indexes always cover every live row, so per-real-idx counters must track row_count exactly. See AccessWriter.UpdateRowCountAsync and docs/design/round-trip-test-failures.md hypothesis #2."));
            }

            tdefsChecked++;
        }

        // Sanity: a freshly-created database always has at least the
        // MSysObjects + MSysAccessStorage / etc. TDEFs plus our user table.
        Assert.True(tdefsChecked >= 2, $"Expected to find at least 2 TDEF pages; found {tdefsChecked}.");
    }
}
