namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// Tests for column-level constraints: DefaultValue, IsNullable, and
/// ValidationRule behaviour during insert and across writer reopens.
/// Tests run against both Jet3 and ACE formats via <c>[Theory]</c> parameters.
/// </summary>
public sealed class ColumnConstraintTests
{
    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task DefaultValue_IsAppliedOnInsert(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        const string table = "Defaults";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)),
                    new("Score", typeof(int)) { DefaultValue = 42 },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(table, [1, DBNull.Value], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(table, [2, 7], TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(2, dt.Rows.Count);
        Assert.Equal(42, dt.Rows[0]["Score"]);
        Assert.Equal(7, dt.Rows[1]["Score"]);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task NullablePublicValueShapes_TreatNullAsDatabaseNull(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        const string table = "NullableShapes";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)),
                    new("Name", typeof(string), maxLength: 50),
                    new("Score", typeof(int)) { DefaultValue = 42 },
                ],
                TestContext.Current.CancellationToken);

            object?[] single = [1, null, null];
            await writer.InsertRowAsync(table, single, TestContext.Current.CancellationToken);

            List<object?[]> rows =
            [
                [2, "Bob", 12],
                [3, null, 7],
            ];
            await writer.InsertRowsAsync(table, rows, TestContext.Current.CancellationToken);

            IReadOnlyDictionary<string, object?> updates = new Dictionary<string, object?>
            {
                ["Name"] = null,
                ["Score"] = null,
            };
            int updated = await writer.UpdateRowsAsync(table, "Id", 2, updates, TestContext.Current.CancellationToken);
            Assert.Equal(1, updated);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(3, dt.Rows.Count);

        DataRow first = Assert.Single(dt.AsEnumerable(), row => (int)row["Id"] == 1);
        DataRow second = Assert.Single(dt.AsEnumerable(), row => (int)row["Id"] == 2);
        DataRow third = Assert.Single(dt.AsEnumerable(), row => (int)row["Id"] == 3);

        Assert.Equal(DBNull.Value, first["Name"]);
        Assert.Equal(42, first["Score"]);

        Assert.Equal(DBNull.Value, second["Name"]);
        Assert.Equal(DBNull.Value, second["Score"]);

        Assert.Equal(DBNull.Value, third["Name"]);
        Assert.Equal(7, third["Score"]);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task NotNull_RejectsMissingValue(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        const string table = "Required";

        await using AccessWriter writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            table,
            [
                new("Id", typeof(int)),
                new("Name", typeof(string), maxLength: 50) { IsNullable = false },
            ],
            TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync(table, [1, DBNull.Value], TestContext.Current.CancellationToken));

        await writer.InsertRowAsync(table, [2, "Alice"], TestContext.Current.CancellationToken);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task NotNull_PersistsAcrossWriterReopen(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        const string table = "Required";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)),
                    new("Name", typeof(string), maxLength: 50) { IsNullable = false },
                ],
                TestContext.Current.CancellationToken);
        }

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.InsertRowAsync(table, [1, DBNull.Value], TestContext.Current.CancellationToken));

            await writer.InsertRowAsync(table, [2, "Alice"], TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        List<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);
        Assert.True(meta[0].IsNullable);
        Assert.False(meta[1].IsNullable);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task ValidationRule_RejectsBadValues(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        const string table = "Validated";

        await using AccessWriter writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            table,
            [
                new("Id", typeof(int)),
                new("Score", typeof(int)) { ValidationRule = v => v is int i && i is >= 0 and <= 100 },
            ],
            TestContext.Current.CancellationToken);

        await writer.InsertRowAsync(table, [1, 50], TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.InsertRowAsync(table, [2, 250], TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ValidationRule_IsNotPersistedAcrossReopen()
    {
        // ValidationRule is a Func<> delegate — it cannot be serialized to the
        // database. On reopen, no validation fires for previously-guarded columns.
        // This documents the gap; when persisted expression-based validation is
        // implemented, this test should start failing.
        await using MemoryStream stream = await CreateFreshStreamAsync(DatabaseFormat.AceAccdb);
        const string table = "ValidNoPersist";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)),
                    new("Score", typeof(int)) { ValidationRule = v => v is int i && i is >= 0 and <= 100 },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(table, [1, 50], TestContext.Current.CancellationToken);
        }

        // Reopen: the delegate-based rule is lost; out-of-range values are accepted.
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.InsertRowAsync(table, [2, 250], TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(2, dt.Rows.Count);
        Assert.Equal(250, dt.Rows[1]["Score"]);
    }

    /// <summary>
    /// Regression guard for the 2026-05-05 nullability fix: the writer must NOT
    /// stamp the legacy private 0x08 NOT-NULL bit (or any unknown bit) into the
    /// TDEF column-flags byte. DAO refuses to open tables whose column flags
    /// carry unknown bits with "Unrecognized database format". Nullability is
    /// now persisted via the Boolean <c>Required</c> property in
    /// <c>MSysObjects.LvProp</c> instead. Allowed bits: 0x01 (FIXED), 0x02
    /// (UNKNOWN_FF — always set on non-complex cols), 0x04 (AUTO_LONG),
    /// 0x07 (complex cols), 0x80 (HYPERLINK).
    /// </summary>
    [Fact]
    public async Task NotNull_DoesNotStampPrivate0x08BitInTdefColumnFlags()
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(DatabaseFormat.AceAccdb);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "FlagGuard",
                [
                    new("Id", typeof(int)) { IsNullable = false },
                    new("Name", typeof(string), maxLength: 50) { IsNullable = false },
                    new("Optional", typeof(int)) { IsNullable = true },
                ],
                TestContext.Current.CancellationToken);
        }

        byte[] disk = stream.ToArray();
        const byte AllowedFlagsMask = Constants.ColumnDescriptorFlags.Fixed
            | Constants.ColumnDescriptorFlags.Unknown
            | Constants.ColumnDescriptorFlags.AutoNumber
            | Constants.ColumnDescriptorFlags.Hyperlink;
        bool foundTable = false;

        for (int p = 1; p < disk.Length / Constants.PageSizes.Jet4; p++)
        {
            int off = p * Constants.PageSizes.Jet4;
            if (disk[off] != 0x02)
            {
                continue;
            }

            int numCols = disk[off + 45] | (disk[off + 46] << 8);
            if (numCols != 3)
            {
                continue;
            }

            int numRealIdx = disk[off + 51] | (disk[off + 52] << 8) | (disk[off + 53] << 16) | (disk[off + 54] << 24);
            int colStart = off + 63 + (numRealIdx * 12);

            for (int c = 0; c < numCols; c++)
            {
                int co = colStart + (c * 25);
                byte flags = disk[co + 15]; // descriptor-relative flags offset
                Assert.Equal(0, flags & ~AllowedFlagsMask);
                Assert.Equal(0, flags & 0x08); // explicit guard against the removed NOT-NULL bit
            }

            foundTable = true;
        }

        Assert.True(foundTable, "Did not find the FlagGuard TDEF page in the writer-produced file.");
    }

    // ── Helpers ───────────────────────────────────────────────────────

    private static async ValueTask<MemoryStream> CreateFreshStreamAsync(DatabaseFormat format)
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            format,
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
