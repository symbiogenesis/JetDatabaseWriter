namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Updates, cascades and schema rewrites delete each affected row and insert
/// it again from the writer's row snapshot. These tests pin that the snapshot
/// carries MEMO / OLE values exactly, OLE values as their stored bytes, which
/// the public read API also returns, and that a MEMO / OLE value whose LVAL data
/// cannot be read is refused instead of being rewritten as placeholder text
/// such as <c>"(memo on LVAL page)"</c>.
/// </summary>
public sealed class LongValueWriteBackTests
{
    private const string TableName = "Items";
    private const int LargeMemoRowCount = 30;

    /// <summary>
    /// 1,500,000 characters of a-z: at least 370 4 KB LVAL pages even with
    /// Unicode compression, and about 750 2 KB pages on Jet3.
    /// </summary>
    private static readonly string LargeMemo = string.Create(1_500_000, 0, static (text, _) =>
    {
        for (int i = 0; i < text.Length; i++)
        {
            text[i] = (char)('a' + (i % 26));
        }
    });

    private enum OlePayload
    {
        /// <summary>Inline value starting 00 01 followed by the "BM" bitmap signature.</summary>
        InlineBitmapSignature = 0,

        /// <summary>Inline OLE 1.0 package whose embedded file has a "%PDF" signature after a prefix.</summary>
        InlinePackage = 1,

        /// <summary>Single-page LVAL value with a "%PDF" signature after a 56-byte prefix.</summary>
        SinglePagePdfAfterPrefix = 2,

        /// <summary>Multi-page LVAL chain with a "%PDF" signature after a 56-byte prefix.</summary>
        ChainedPdfAfterPrefix = 3,

        /// <summary>Multi-page LVAL chain holding an OLE 1.0 package.</summary>
        ChainedPackage = 4,
    }

    private enum SchemaRewrite
    {
        /// <summary>Add an unrelated column.</summary>
        AddColumn = 0,

        /// <summary>Rename the OLE column.</summary>
        RenameColumn = 1,

        /// <summary>Drop an unrelated column.</summary>
        DropColumn = 2,
    }

    public static TheoryData<DatabaseFormat, string> FormatsAndPayloads()
    {
        var data = new TheoryData<DatabaseFormat, string>();
        foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
        {
            foreach (OlePayload payload in Enum.GetValues<OlePayload>())
            {
                data.Add(format, payload.ToString());
            }
        }

        return data;
    }

    public static TheoryData<DatabaseFormat, string, string> FormatsPayloadsAndRewrites()
    {
        var data = new TheoryData<DatabaseFormat, string, string>();
        foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
        {
            foreach (OlePayload payload in Enum.GetValues<OlePayload>())
            {
                foreach (SchemaRewrite rewrite in Enum.GetValues<SchemaRewrite>())
                {
                    data.Add(format, payload.ToString(), rewrite.ToString());
                }
            }
        }

        return data;
    }

    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndWriteModes()
    {
        var data = new TheoryData<DatabaseFormat, WriteMode>();
        foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
        {
            foreach (WriteMode mode in new[] { WriteMode.Direct, WriteMode.AutoCommit, WriteMode.ExplicitCommit })
            {
                data.Add(format, mode);
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(FormatsAndPayloads))]
    public async Task UpdateRowsAsync_OtherColumn_KeepsOleBytesExactly(DatabaseFormat format, string payloadKind)
    {
        byte[] payload = BuildPayload(Enum.Parse<OlePayload>(payloadKind));
        await using MemoryStream ms = await CreateDatabaseAsync(format, [[1, "a", payload], [2, "b", DBNull.Value]]);
        Assert.Equal(payload, await ReadPublicOleAsync(ms, id: 1));

        await using (AccessWriter writer = await OpenWriterAsync(ms))
        {
            int updated = await writer.UpdateRowsAsync(TableName, "Id", 1, new Dictionary<string, object?> { ["Note"] = "changed" }, TestContext.Current.CancellationToken);
            Assert.Equal(1, updated);
        }

        // The public read returns the stored bytes, before and after the update.
        Assert.Equal(payload, await ReadPublicOleAsync(ms, id: 1));

        DataRow row = await ReadSnapshotRowAsync(ms, id: 1);
        Assert.Equal("changed", row["Note"]);
        Assert.Equal(payload, Assert.IsType<byte[]>(row["Blob"]));
    }

    [Theory]
    [MemberData(nameof(FormatsPayloadsAndRewrites))]
    public async Task SchemaRewrite_KeepsOleBytesExactly(DatabaseFormat format, string payloadKind, string rewrite)
    {
        byte[] payload = BuildPayload(Enum.Parse<OlePayload>(payloadKind));
        await using MemoryStream ms = await CreateDatabaseAsync(format, [[1, "a", payload], [2, "b", DBNull.Value]]);
        Assert.Equal(payload, await ReadPublicOleAsync(ms, id: 1));

        string blobColumn = "Blob";
        await using (AccessWriter writer = await OpenWriterAsync(ms))
        {
            switch (Enum.Parse<SchemaRewrite>(rewrite))
            {
                case SchemaRewrite.AddColumn:
                    await writer.AddColumnAsync(TableName, new ColumnDefinition("Extra", typeof(int)), TestContext.Current.CancellationToken);
                    break;
                case SchemaRewrite.RenameColumn:
                    await writer.RenameColumnAsync(TableName, "Blob", "Picture", TestContext.Current.CancellationToken);
                    blobColumn = "Picture";
                    break;
                case SchemaRewrite.DropColumn:
                    await writer.DropColumnAsync(TableName, "Note", TestContext.Current.CancellationToken);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(rewrite));
            }
        }

        Assert.Equal(payload, await ReadPublicOleAsync(ms, id: 1, blobColumn));

        DataRow row = await ReadSnapshotRowAsync(ms, id: 1);
        Assert.Equal(payload, Assert.IsType<byte[]>(row[blobColumn]));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task UpdateRowsAsync_InExplicitTransaction_KeepsOleBytesExactly(bool commit)
    {
        byte[] payload = BuildPayload(OlePayload.ChainedPdfAfterPrefix);
        await using MemoryStream ms = await CreateDatabaseAsync(DatabaseFormat.AceAccdb, [[1, "a", payload]]);

        await using (AccessWriter writer = await OpenWriterAsync(ms))
        {
            await using JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            _ = await writer.UpdateRowsAsync(TableName, "Id", 1, new Dictionary<string, object?> { ["Note"] = "changed" }, TestContext.Current.CancellationToken);
            if (commit)
            {
                await tx.CommitAsync(TestContext.Current.CancellationToken);
            }
            else
            {
                await tx.RollbackAsync(TestContext.Current.CancellationToken);
            }
        }

        DataRow row = await ReadSnapshotRowAsync(ms, id: 1);
        Assert.Equal(commit ? "changed" : "a", row["Note"]);
        Assert.Equal(payload, Assert.IsType<byte[]>(row["Blob"]));
    }

    [Fact]
    public async Task UpdateRowsAsync_WithTransactionalWrites_KeepsOleBytesExactly()
    {
        byte[] payload = BuildPayload(OlePayload.InlinePackage);
        await using MemoryStream ms = await CreateDatabaseAsync(DatabaseFormat.Jet4Mdb, [[1, "a", payload]]);

        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            ms,
            new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = true },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            _ = await writer.UpdateRowsAsync(TableName, "Id", 1, new Dictionary<string, object?> { ["Note"] = "changed" }, TestContext.Current.CancellationToken);
        }

        DataRow row = await ReadSnapshotRowAsync(ms, id: 1);
        Assert.Equal(payload, Assert.IsType<byte[]>(row["Blob"]));
    }

    /// <summary>
    /// An update deletes each row and inserts it again in scan order, so the first
    /// row's 1.5M-character MEMO, a chain longer than the reader's default 256-page
    /// cache, goes through the snapshot read, the long-value release and a new
    /// chain, and is again followed by the other 29 rows. Read back with the
    /// default reader options, the MEMO and every row after it must be intact.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the update is driven.</param>
    [Theory]
    [MemberData(nameof(FormatsAndWriteModes))]
    public async Task UpdateRowsAsync_MemoChainLongerThanDefaultCache_KeepsMemo(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await CreateLargeMemoDatabaseAsync(format);

        ms.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            ms,
            new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = mode == WriteMode.AutoCommit },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await using JetTransaction? tx = mode == WriteMode.ExplicitCommit
                ? await writer.BeginTransactionAsync(TestContext.Current.CancellationToken)
                : null;
            int updated = await writer.UpdateRowsAsync(TableName, RowCriteria.All(), new RowValues { ["Note"] = "changed" }, TestContext.Current.CancellationToken);
            Assert.Equal(LargeMemoRowCount, updated);
            if (tx is not null)
            {
                await tx.CommitAsync(TestContext.Current.CancellationToken);
            }
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        var actual = new SortedDictionary<int, string>();
        await foreach (object[] row in reader.Rows(TableName, cancellationToken: TestContext.Current.CancellationToken))
        {
            int id = (int)row[0];
            Assert.True(actual.TryAdd(id, LargeMemoSignature(id, row[1] as string, row[2] as string)), $"Row {id} was read twice.");
        }

        List<string> expected = [.. Enumerable.Range(1, LargeMemoRowCount).Select(id => LargeMemoSignature(id, "changed", LargeMemoBody(id)))];
        Assert.Equal(expected, actual.Values);
        Assert.Equal(LargeMemoRowCount, await reader.GetRealRowCountAsync(TableName, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task UpdateRowsAsync_RowsOnSeveralDataPages_KeepsEveryOleValueExactly()
    {
        // 60 rows with ~200-byte inline OLE values fill several 4 KB data pages.
        var rows = new List<object[]>();
        for (int id = 1; id <= 60; id++)
        {
            byte[] value = new byte[200];
            value[0] = 0x00;
            value[1] = 0x01;
            value[2] = 0x42;
            value[3] = 0x4D;
            unchecked
            {
                for (int i = 4; i < value.Length; i++)
                {
                    value[i] = (byte)((id * 13) + i);
                }
            }

            rows.Add([id, "n" + id, value]);
        }

        await using MemoryStream ms = await CreateDatabaseAsync(DatabaseFormat.AceAccdb, rows);

        await using (AccessWriter writer = await OpenWriterAsync(ms))
        {
            int updated = await writer.UpdateRowsAsync(TableName, RowCriteria.All(), new RowValues { ["Note"] = "changed" }, TestContext.Current.CancellationToken);
            Assert.Equal(60, updated);
        }

        ms.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: TestContext.Current.CancellationToken);
        using DataTable snapshot = await harness.Services.Snapshots.ReadTableSnapshotAsync(TableName, TestContext.Current.CancellationToken);
        Assert.Equal(60, snapshot.Rows.Count);
        foreach (DataRow row in snapshot.Rows)
        {
            int id = (int)row["Id"];
            Assert.Equal("changed", row["Note"]);
            Assert.Equal((byte[])rows[id - 1][2], Assert.IsType<byte[]>(row["Blob"]));
        }
    }

    [Fact]
    public async Task UpdateRowsAsync_CascadedToChild_KeepsChildOleBytesExactly()
    {
        byte[] inline = BuildPayload(OlePayload.InlinePackage);
        byte[] chained = BuildPayload(OlePayload.ChainedPdfAfterPrefix);
        var ms = new MemoryStream();

        // CreateRelationshipAsync needs the MSysRelationships table that only ACCDB output has.
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Parent", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true, IsNullable = false }], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(
                "Child",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true, IsNullable = false },
                    new ColumnDefinition("ParentId", typeof(int)),
                    new ColumnDefinition("Blob", typeof(byte[])),
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Parent", [1], TestContext.Current.CancellationToken);
            await writer.InsertRowsAsync("Child", [[10, 1, inline], [11, 1, chained]], TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("ParentChild", "Parent", "Id", "Child", "ParentId")
                {
                    EnforceReferentialIntegrity = true,
                    CascadeUpdates = true,
                },
                TestContext.Current.CancellationToken);

            int updated = await writer.UpdateRowsAsync("Parent", "Id", 1, new Dictionary<string, object?> { ["Id"] = 5 }, TestContext.Current.CancellationToken);
            Assert.Equal(1, updated);
        }

        ms.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: TestContext.Current.CancellationToken);
        using DataTable child = await harness.Services.Snapshots.ReadTableSnapshotAsync("Child", TestContext.Current.CancellationToken);
        DataRow first = child.Rows.Cast<DataRow>().Single(r => (int)r["Id"] == 10);
        DataRow second = child.Rows.Cast<DataRow>().Single(r => (int)r["Id"] == 11);
        Assert.Equal(5, first["ParentId"]);
        Assert.Equal(5, second["ParentId"]);
        Assert.Equal(inline, Assert.IsType<byte[]>(first["Blob"]));
        Assert.Equal(chained, Assert.IsType<byte[]>(second["Blob"]));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task UpdateRowsAsync_UnreadableMemo_RefusesRowAndLeavesItUntouched(DatabaseFormat format)
    {
        await using MemoryStream ms = await CreateDatabaseWithUnreadableMemoAsync(format);

        await using (AccessWriter writer = await OpenWriterAsync(ms))
        {
            InvalidDataException ex = await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await writer.UpdateRowsAsync(TableName, "Id", 1, new Dictionary<string, object?> { ["Note"] = "changed" }, TestContext.Current.CancellationToken));
            Assert.Contains("Body", ex.Message, StringComparison.Ordinal);

            // Rows whose MEMO can be read still update.
            int updated = await writer.UpdateRowsAsync(TableName, "Id", 2, new Dictionary<string, object?> { ["Note"] = "changed" }, TestContext.Current.CancellationToken);
            Assert.Equal(1, updated);
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false, StrictParsing = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        DataTable table = await reader.ReadTableAsync(TableName, cancellationToken: TestContext.Current.CancellationToken);
        DataRow first = table.Rows.Cast<DataRow>().Single(r => (int)r["Id"] == 1);
        DataRow second = table.Rows.Cast<DataRow>().Single(r => (int)r["Id"] == 2);
        Assert.Equal(DBNull.Value, first["Body"]);
        Assert.Equal("a", first["Note"]);
        Assert.Equal("changed", second["Note"]);
        Assert.Equal("short memo", second["Body"]);
    }

    [Fact]
    public async Task UpdateRowsAsync_ReplacingUnreadableMemo_Succeeds()
    {
        await using MemoryStream ms = await CreateDatabaseWithUnreadableMemoAsync(DatabaseFormat.AceAccdb);

        await using (AccessWriter writer = await OpenWriterAsync(ms))
        {
            int updated = await writer.UpdateRowsAsync(TableName, "Id", 1, new Dictionary<string, object?> { ["Body"] = "replacement" }, TestContext.Current.CancellationToken);
            Assert.Equal(1, updated);
        }

        DataRow row = await ReadSnapshotRowAsync(ms, id: 1);
        Assert.Equal("replacement", row["Body"]);
    }

    [Fact]
    public async Task AddColumnAsync_UnreadableMemo_RefusesRewriteBeforeChangingSchema()
    {
        await using MemoryStream ms = await CreateDatabaseWithUnreadableMemoAsync(DatabaseFormat.AceAccdb);

        await using (AccessWriter writer = await OpenWriterAsync(ms))
        {
            _ = await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await writer.AddColumnAsync(TableName, new ColumnDefinition("Extra", typeof(int)), TestContext.Current.CancellationToken));

            // Dropping the unreadable column itself loses nothing else and is allowed.
            await writer.DropColumnAsync(TableName, "Body", TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        IReadOnlyList<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.Equal([TableName], tables);
        IReadOnlyList<ColumnMetadata> columns = await reader.GetColumnMetadataAsync(TableName, TestContext.Current.CancellationToken);
        Assert.Equal(["Id", "Note"], columns.Select(c => c.Name));
        Assert.Equal(2, await reader.GetRealRowCountAsync(TableName, TestContext.Current.CancellationToken));
    }

    private static byte[] BuildPayload(OlePayload kind)
    {
        byte[] prefix = [.. Encoding.ASCII.GetBytes("JUNKMARKERPREFIX"), .. new byte[40]];
        return kind switch
        {
            OlePayload.InlineBitmapSignature => [0x00, 0x01, 0x42, 0x4D, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A],
            OlePayload.InlinePackage => BuildOlePackage([.. "zz"u8, .. "%PDF-1.4 PACKAGEDTAIL"u8]),
            OlePayload.SinglePagePdfAfterPrefix => [.. prefix, .. "%PDF-1.4 "u8, .. Filler(1200)],
            OlePayload.ChainedPdfAfterPrefix => [.. prefix, .. "%PDF-1.4 "u8, .. Filler(9000)],
            OlePayload.ChainedPackage => BuildOlePackage([.. "zz"u8, .. "%PDF-1.4 "u8, .. Filler(9000)]),
            _ => throw new ArgumentOutOfRangeException(nameof(kind)),
        };
    }

    private static byte[] Filler(int length)
    {
        byte[] bytes = new byte[length];
        for (int i = 0; i < bytes.Length; i++)
        {
            bytes[i] = (byte)('a' + (i % 26));
        }

        return bytes;
    }

    /// <summary>
    /// Builds an OLE 1.0 "Package" object wrapping <paramref name="embedded"/>,
    /// in the layout <see cref="OleObjectValue"/> unwraps.
    /// </summary>
    /// <param name="embedded">The embedded file bytes.</param>
    /// <returns>The package bytes.</returns>
    private static byte[] BuildOlePackage(byte[] embedded)
    {
        using var dataBlock = new MemoryStream();
        using (var block = new BinaryWriter(dataBlock, Encoding.ASCII, leaveOpen: true))
        {
            byte[] localPath = Encoding.ASCII.GetBytes("C:\\a.pdf\0");
            block.Write((ushort)0x0002);
            block.Write(Encoding.ASCII.GetBytes("a.pdf\0"));
            block.Write(Encoding.ASCII.GetBytes("C:\\a.pdf\0"));
            block.Write(0x00030000);
            block.Write(localPath.Length);
            block.Write(localPath);
            block.Write(embedded.Length);
            block.Write(embedded);
        }

        using var package = new MemoryStream();
        using (var writer = new BinaryWriter(package, Encoding.ASCII, leaveOpen: true))
        {
            byte[] typeName = Encoding.ASCII.GetBytes("Package\0");
            writer.Write((ushort)0x1C15);
            writer.Write((ushort)20);
            writer.Write(new byte[16]);
            writer.Write(0x0501);
            writer.Write(2);
            writer.Write(typeName.Length);
            writer.Write(typeName);
            writer.Write(new byte[8]);
            writer.Write((int)dataBlock.Length);
            writer.Write(dataBlock.ToArray());
        }

        return package.ToArray();
    }

    private static async Task<MemoryStream> CreateDatabaseAsync(DatabaseFormat format, IEnumerable<object[]> rows)
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(ms, format, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Note", typeof(string), maxLength: 50),
                    new ColumnDefinition("Blob", typeof(byte[])),
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowsAsync(TableName, rows, TestContext.Current.CancellationToken);
        }

        return ms;
    }

    /// <summary>
    /// Creates a table whose row 1 holds a single-page LVAL MEMO and whose row 2
    /// holds an inline MEMO, then breaks the LVAL page's page-type byte so row 1's
    /// MEMO can no longer be read.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>The damaged database.</returns>
    private static async Task<MemoryStream> CreateDatabaseWithUnreadableMemoAsync(DatabaseFormat format)
    {
        const string marker = "UNREADABLEMEMOMARKER";
        string longMemo = marker + new string('q', 1500);
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(ms, format, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Note", typeof(string), maxLength: 50),
                    new ColumnDefinition("Body", typeof(string)),
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowsAsync(TableName, [[1, "a", longMemo], [2, "b", "short memo"]], TestContext.Current.CancellationToken);
        }

        byte[] file = ms.ToArray();
        int pageSize = format == DatabaseFormat.Jet3Mdb ? 2048 : 4096;
        int offset = IndexOf(file, Encoding.ASCII.GetBytes(marker));
        if (offset < 0)
        {
            offset = IndexOf(file, Encoding.Unicode.GetBytes(marker));
        }

        Assert.True(offset >= 0, "The MEMO's LVAL page was not found.");
        int lvalPageStart = offset / pageSize * pageSize;
        Assert.Equal(0x01, file[lvalPageStart]);
        ms.Position = lvalPageStart;
        ms.WriteByte(0x7F);

        // The default public read refuses corrupt stored content.
        ms.Position = 0;
        await using (AccessReader reader = await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await reader.ReadTableAsync(TableName, cancellationToken: TestContext.Current.CancellationToken));
        }

        return ms;
    }

    private static async Task<MemoryStream> CreateLargeMemoDatabaseAsync(DatabaseFormat format)
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(ms, format, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Note", typeof(string), maxLength: 50),
                    new ColumnDefinition("Body", typeof(string)),
                ],
                TestContext.Current.CancellationToken);
            _ = await writer.InsertRowsAsync(
                TableName,
                Enumerable.Range(1, LargeMemoRowCount).Select(id => new object[] { id, LargeMemoNote(id), LargeMemoBody(id) }),
                TestContext.Current.CancellationToken);
        }

        return ms;
    }

    private static string LargeMemoNote(int id) => "n" + id.ToString(CultureInfo.InvariantCulture);

    private static string LargeMemoBody(int id) => id == 1 ? LargeMemo : "small-" + id.ToString(CultureInfo.InvariantCulture);

    /// <summary>
    /// Describes a row without copying the 1.5M-character MEMO into assertion
    /// messages: the id, the note, then "intact" or the length of the wrong MEMO.
    /// </summary>
    /// <param name="id">The row id.</param>
    /// <param name="note">The Note value read back.</param>
    /// <param name="body">The MEMO value read back.</param>
    private static string LargeMemoSignature(int id, string? note, string? body) =>
        id.ToString(CultureInfo.InvariantCulture) + "|" + note + "|"
        + (string.Equals(body, LargeMemoBody(id), StringComparison.Ordinal)
            ? "intact"
            : "wrong, " + (body?.Length ?? -1).ToString(CultureInfo.InvariantCulture) + " chars");

    private static int IndexOf(byte[] haystack, byte[] needle) => haystack.AsSpan().IndexOf(needle);

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream ms)
    {
        ms.Position = 0;
        return AccessWriter.OpenAsync(ms, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
    }

    /// <summary>Reads row <paramref name="id"/> through the writer's own snapshot reader, which returns stored OLE bytes.</summary>
    /// <param name="ms">The database stream.</param>
    /// <param name="id">The row id.</param>
    /// <returns>The snapshot row.</returns>
    private static async Task<DataRow> ReadSnapshotRowAsync(MemoryStream ms, int id)
    {
        ms.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: TestContext.Current.CancellationToken);
        DataTable snapshot = await harness.Services.Snapshots.ReadTableSnapshotAsync(TableName, TestContext.Current.CancellationToken);
        return snapshot.Rows.Cast<DataRow>().Single(r => (int)r["Id"] == id);
    }

    private static async Task<byte[]> ReadPublicOleAsync(MemoryStream ms, int id, string column = "Blob")
    {
        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        DataTable table = await reader.ReadTableAsync(TableName, cancellationToken: TestContext.Current.CancellationToken);
        return Assert.IsType<byte[]>(table.Rows.Cast<DataRow>().Single(r => (int)r["Id"] == id)[column]);
    }
}
