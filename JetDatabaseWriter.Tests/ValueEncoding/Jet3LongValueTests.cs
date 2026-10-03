namespace JetDatabaseWriter.Tests.ValueEncoding;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding.Models;
using Xunit;

/// <summary>
/// MEMO and OLE values the writer moves to LVAL pages in a Jet3 (Access 97)
/// file. Jet3 data pages keep the row count at offset 8 and the row-offset
/// table at 10, and Access 97 packs each LVAL row at the end of its page with
/// no token in the page or the descriptor. The writer used to emit the Jet4
/// header on Jet3 too, so its own reader saw the token as the row count and
/// the first row offset, and values over the inline caps read back as
/// placeholders or as other bytes.
/// </summary>
public sealed class Jet3LongValueTests
{
    private const int Jet3PageSize = 2048;
    private const string TableName = "T";

    /// <summary>MEMO characters / OLE bytes per row: inline, single-page, single-page at capacity, two pages, two full chunks, chained.</summary>
    private static readonly (int Memo, int Ole)[] Sizes =
    [
        (100, 50),
        (1500, 1000),
        (2036, 2036),
        (2037, 2037),
        (4064, 4064),
        (6000, 9000),
    ];

    private enum WriteMode
    {
        /// <summary>Each write commits on its own.</summary>
        AutoCommit = 0,

        /// <summary>The writer journals each operation (<see cref="AccessWriterOptions.UseTransactionalWrites"/>).</summary>
        TransactionalWrites = 1,

        /// <summary>One explicit transaction holds the inserts and an update of the chained row.</summary>
        ExplicitTransaction = 2,
    }

    [Theory]
    [InlineData(nameof(WriteMode.AutoCommit))]
    [InlineData(nameof(WriteMode.TransactionalWrites))]
    [InlineData(nameof(WriteMode.ExplicitTransaction))]
    public async Task RoundTrip_EveryStorageForm_ReadsBackThroughEveryReadApi(string mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        WriteMode writeMode = Enum.Parse<WriteMode>(mode);
        await using MemoryStream ms = await CreateDatabaseAsync(ct);

        var options = new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = writeMode == WriteMode.TransactionalWrites };
        string lastNote = "n" + Sizes.Length;
        await using (AccessWriter writer = await OpenWriterAsync(ms, options, ct))
        {
            JetTransaction? transaction = writeMode == WriteMode.ExplicitTransaction ? await writer.BeginTransactionAsync(ct) : null;
            for (int id = 1; id <= Sizes.Length; id++)
            {
                await writer.InsertRowAsync(TableName, [id, "n" + id, Memo(id), Ole(id)], ct);
            }

            if (transaction is not null)
            {
                // The update reads the chained row's LVAL pages back from the journal.
                lastNote = "in-tx";
                int updated = await writer.UpdateRowsAsync(TableName, "Id", Sizes.Length, new Dictionary<string, object?> { ["Note"] = lastNote }, ct);
                Assert.Equal(1, updated);
                await transaction.CommitAsync(ct);
                await transaction.DisposeAsync();
            }
        }

        string NoteOf(int id) => id == Sizes.Length ? lastNote : "n" + id;

        ms.Position = 0;
        await using (AccessReader reader = await OpenReaderAsync(ms, ct))
        {
            List<object[]> rows = await reader.Rows(TableName, cancellationToken: ct).ToListAsync(ct);
            Assert.Equal(Sizes.Length, rows.Count);
            foreach (object[] row in rows)
            {
                int id = (int)row[0];
                Assert.Equal(NoteOf(id), row[1]);
                Assert.Equal(Memo(id), row[2]);
                Assert.Equal(Ole(id), row[3]);
            }

            List<Jet3Row> mapped = await reader.Rows<Jet3Row>(TableName, cancellationToken: ct).ToListAsync(ct);
            AssertMapped(mapped);
            AssertMapped(await reader.ReadTableAsync<Jet3Row>(TableName, cancellationToken: ct));

            using DataTable table = await reader.ReadTableAsync(TableName, cancellationToken: ct);
            Assert.Equal(Sizes.Length, table.Rows.Count);
            foreach (DataRow row in table.Rows)
            {
                int id = (int)row["Id"];
                Assert.Equal(Memo(id), row["Body"]);
                Assert.Equal(Ole(id), row["Blob"]);
            }

            List<string[]> strings = await reader.RowsAsStrings(TableName, cancellationToken: ct).ToListAsync(ct);
            Assert.Equal(Sizes.Length, strings.Count);
            foreach (string[] row in strings)
            {
                int id = int.Parse(row[0], System.Globalization.CultureInfo.InvariantCulture);
                Assert.Equal(Memo(id), row[2]);
                Assert.Equal(OleDataUri(id), row[3]);
            }

            using DataTable stringTable = await reader.ReadTableAsStringsAsync(TableName, cancellationToken: ct);
            Assert.Equal(Sizes.Length, stringTable.Rows.Count);
            foreach (DataRow row in stringTable.Rows)
            {
                int id = int.Parse((string)row["Id"], System.Globalization.CultureInfo.InvariantCulture);
                Assert.Equal(Memo(id), row["Body"]);
                Assert.Equal(OleDataUri(id), row["Blob"]);
            }
        }

        // The writer's own snapshot, which updates and schema rewrites copy rows from.
        ms.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: ct))
        {
            using DataTable snapshot = await harness.Services.Snapshots.ReadTableSnapshotAsync(TableName, ct);
            Assert.Equal(Sizes.Length, snapshot.Rows.Count);
            foreach (DataRow row in snapshot.Rows)
            {
                int id = (int)row["Id"];
                Assert.Equal(Memo(id), row["Body"]);
                Assert.Equal(Ole(id), Assert.IsType<byte[]>(row["Blob"]));
            }
        }

        void AssertMapped(IReadOnlyList<Jet3Row> mappedRows)
        {
            Assert.Equal(Sizes.Length, mappedRows.Count);
            foreach (Jet3Row row in mappedRows)
            {
                Assert.Equal(NoteOf(row.Id), row.Note);
                Assert.Equal(Memo(row.Id), row.Body);
                Assert.Equal(Ole(row.Id), row.Blob);
            }
        }
    }

    [Fact]
    public async Task InsertInTransaction_RolledBack_LeavesNoRowsOrLvalPages()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream ms = await CreateDatabaseAsync(ct);
        long lengthBefore = ms.Length;

        await using (AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false }, ct))
        {
            await using JetTransaction transaction = await writer.BeginTransactionAsync(ct);
            await writer.InsertRowAsync(TableName, [6, "n6", Memo(6), Ole(6)], ct);
            await transaction.RollbackAsync(ct);
        }

        Assert.Equal(lengthBefore, ms.Length);
        Assert.Equal(0, CountLvalPages(ms.ToArray()));

        ms.Position = 0;
        await using AccessReader reader = await OpenReaderAsync(ms, ct);
        Assert.Equal(0, await reader.GetRealRowCountAsync(TableName, ct));
    }

    /// <summary>
    /// Access 97 wrote <c>MSP_PROJECTS</c> in test2V1997.mdb with a 348-byte
    /// single-page MEMO and a 22,970-byte chained OLE value. Writing the same
    /// bytes into a fresh Jet3 file must produce the same LVAL pages: the
    /// single page byte for byte, and each chain page apart from its 4-byte
    /// next-row pointer, since page numbers differ between the files.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task LvalPages_MatchAccess97PagesByteForByte()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        if (!File.Exists(TestDatabases.Test2V1997))
        {
            Assert.Skip("test2V1997.mdb is unavailable on this machine.");
        }

        byte[] access = await File.ReadAllBytesAsync(TestDatabases.Test2V1997, ct);
        LongValueDescriptor accessSingle;
        LongValueDescriptor accessChained;
        await using (var copy = new MemoryStream())
        {
            copy.Write(access);
            copy.Position = 0;
            await using WriterHarness harness = await WriterHarness.OpenAsync(copy, cancellationToken: ct);
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.Services.Catalog.GetCatalogEntryAsync("MSP_PROJECTS", ct));
            accessSingle = Assert.Single(await ReadDescriptorsAsync(harness.Database, entry.TDefPage, "PROJ_PROP_AUTHOR", ct), d => d.IsExternal);
            accessChained = Assert.Single(await ReadDescriptorsAsync(harness.Database, entry.TDefPage, "RESERVED_BINARY_DATA", ct), d => d.IsExternal);
        }

        Assert.True(accessSingle.IsSinglePage);
        Assert.Equal(348, accessSingle.Length);
        Assert.True(accessChained.UsesChainedPages);
        Assert.Equal(22970, accessChained.Length);

        byte[] singlePayload = ReadJet3Payload(access, accessSingle);
        byte[] chainedPayload = ReadJet3Payload(access, accessChained);

        await using MemoryStream ms = new();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync(TableName, [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Blob", typeof(byte[]))], ct);
            await writer.InsertRowsAsync(TableName, [[1, singlePayload], [2, chainedPayload]], ct);
        }

        List<LongValueDescriptor> written;
        ms.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: ct))
        {
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.Services.Catalog.GetCatalogEntryAsync(TableName, ct));
            written = await ReadDescriptorsAsync(harness.Database, entry.TDefPage, "Blob", ct);
        }

        LongValueDescriptor mySingle = Assert.Single(written, d => d.Length == singlePayload.Length);
        LongValueDescriptor myChained = Assert.Single(written, d => d.Length == chainedPayload.Length);
        Assert.Equal(accessSingle.StorageMode, mySingle.StorageMode);
        Assert.Equal(accessSingle.Token, mySingle.Token);
        Assert.Equal(0u, mySingle.Token);
        Assert.Equal(accessChained.StorageMode, myChained.StorageMode);
        Assert.Equal(accessChained.Token, myChained.Token);

        byte[] mine = ms.ToArray();
        Assert.Equal(
            access.AsSpan(LongValueStore.PageNumber(accessSingle.FirstDp) * Jet3PageSize, Jet3PageSize).ToArray(),
            mine.AsSpan(LongValueStore.PageNumber(mySingle.FirstDp) * Jet3PageSize, Jet3PageSize).ToArray());

        uint accessDp = accessChained.FirstDp;
        uint myDp = myChained.FirstDp;
        int pages = 0;
        while (accessDp != 0 || myDp != 0)
        {
            Assert.True(accessDp != 0 && myDp != 0, $"The chains end at different lengths after {pages} pages.");
            byte[] accessPage = PageWithoutNextPointer(access, accessDp, out uint accessNext);
            byte[] myPage = PageWithoutNextPointer(mine, myDp, out uint myNext);
            Assert.Equal(accessPage, myPage);
            accessDp = accessNext;
            myDp = myNext;
            pages++;
        }

        Assert.Equal(12, pages);
    }

    [Fact]
    public async Task AccessAuthoredJet3LongValues_ReadExactly()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        if (!File.Exists(TestDatabases.Test2V1997) || !File.Exists(TestDatabases.Jet3Test))
        {
            Assert.Skip("The Jet3 fixtures are unavailable on this machine.");
        }

        Encoding cp1252 = CodePagesEncodingProvider.Instance.GetEncoding(1252)!;
        await using (AccessReader reader = await AccessReader.OpenAsync(TestDatabases.Test2V1997, new AccessReaderOptions { UseLockFile = false }, ct))
        {
            int author = await OrdinalAsync(reader, "MSP_PROJECTS", "PROJ_PROP_AUTHOR", ct);
            List<object[]> rows = await reader.Rows("MSP_PROJECTS", cancellationToken: ct).ToListAsync(ct);
            string authorText = Assert.IsType<string>(Assert.Single(rows, r => r[author] is string { Length: 348 })[author]);
            Assert.Equal("1B6D3223F06BEBFB9183FF14E45E5ED0D0B1662A3209E0635C5B4DD2095CFA89", Sha256(cp1252.GetBytes(authorText)));

            List<string[]> strings = await reader.RowsAsStrings("MSP_PROJECTS", cancellationToken: ct).ToListAsync(ct);
            Assert.Contains(strings, r => r[author] == authorText);
        }

        await using (var copy = new MemoryStream(await File.ReadAllBytesAsync(TestDatabases.Test2V1997, ct)))
        await using (WriterHarness harness = await WriterHarness.OpenAsync(copy, cancellationToken: ct))
        {
            using DataTable snapshot = await harness.Services.Snapshots.ReadTableSnapshotAsync("MSP_PROJECTS", ct);
            byte[] binary = Assert.IsType<byte[]>(Assert.Single(snapshot.Rows.Cast<DataRow>(), r => r["RESERVED_BINARY_DATA"] is byte[] { Length: 22970 })["RESERVED_BINARY_DATA"]);
            Assert.Equal("4444AA8E05096187493AEF56724710E8841DE604196E58089D48AB801338CEB0", Sha256(binary));
        }

        await using (AccessReader reader = await AccessReader.OpenAsync(TestDatabases.Jet3Test, new AccessReaderOptions { UseLockFile = false }, ct))
        {
            using DataTable categories = await reader.ReadTableAsync("Categories", cancellationToken: ct);
            DataRow other = categories.Rows.Cast<DataRow>().Single(r => Convert.ToInt32(r["CategoryID"], System.Globalization.CultureInfo.InvariantCulture) == 3);
            Assert.Equal("Everything else that does not fit in other categories", other["Description"]);
        }
    }

    /// <summary>
    /// mdbtools' nwind.mdb is an Access 97 Northwind that packs several long
    /// values onto many of its LVAL pages. Its Employees and Categories tables
    /// are listed only since the catalog scan follows overflow rows, so their
    /// long values are checked here: a single-page MEMO and two chained OLE
    /// pictures, against SHA-256 hashes taken from an independent parse of the
    /// Jet3 pages.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task AccessAuthoredJet3LongValues_Nwind_ReadExactly()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        if (!File.Exists(TestDatabases.MdbtoolsNwind))
        {
            Assert.Skip("nwind.mdb is unavailable on this machine.");
        }

        Encoding cp1252 = CodePagesEncodingProvider.Instance.GetEncoding(1252)!;
        await using (AccessReader reader = await AccessReader.OpenAsync(TestDatabases.MdbtoolsNwind, new AccessReaderOptions { UseLockFile = false }, ct))
        {
            int id = await OrdinalAsync(reader, "Employees", "EmployeeID", ct);
            int notes = await OrdinalAsync(reader, "Employees", "Notes", ct);
            List<object[]> rows = await reader.Rows("Employees", cancellationToken: ct).ToListAsync(ct);
            string notesText = Assert.IsType<string>(Assert.Single(rows, r => Convert.ToInt32(r[id], System.Globalization.CultureInfo.InvariantCulture) == 2)[notes]);
            Assert.Equal(448, notesText.Length);
            Assert.Equal("3082CCEC23B35826522F0034576478A357CA4EC1FC6359E522FFE5789F1D6D05", Sha256(cp1252.GetBytes(notesText)));

            List<string[]> strings = await reader.RowsAsStrings("Employees", cancellationToken: ct).ToListAsync(ct);
            Assert.Contains(strings, r => r[notes] == notesText);
        }

        // Public OLE reads unwrap the OLE package, so the exact stored bytes come
        // from the writer's snapshot, which updates and schema rewrites copy.
        await using (var copy = new MemoryStream(await File.ReadAllBytesAsync(TestDatabases.MdbtoolsNwind, ct)))
        await using (WriterHarness harness = await WriterHarness.OpenAsync(copy, cancellationToken: ct))
        {
            using DataTable categories = await harness.Services.Snapshots.ReadTableSnapshotAsync("Categories", ct);
            byte[] picture = Assert.IsType<byte[]>(categories.Rows.Cast<DataRow>().Single(r => Convert.ToInt32(r["CategoryID"], System.Globalization.CultureInfo.InvariantCulture) == 1)["Picture"]);
            Assert.Equal(10746, picture.Length);
            Assert.Equal("94CE40D8F8D1294F02CA7101B7A8C393140FD3F617947C81EA7C8ADB70BCE007", Sha256(picture));

            using DataTable employees = await harness.Services.Snapshots.ReadTableSnapshotAsync("Employees", ct);
            byte[] photo = Assert.IsType<byte[]>(employees.Rows.Cast<DataRow>().Single(r => Convert.ToInt32(r["EmployeeID"], System.Globalization.CultureInfo.InvariantCulture) == 1)["Photo"]);
            Assert.Equal(21626, photo.Length);
            Assert.Equal("0FEFEA1C00180A14F01278E124445C03578914CAAC1EDE2318D3A328A7A11FE9", Sha256(photo));
        }
    }

    [Fact]
    public async Task CreateTable_LvPropOver256Bytes_PropertiesRoundTrip()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var columns = new List<ColumnDefinition> { new("Id", typeof(int)) };
        for (int i = 0; i < 6; i++)
        {
            columns.Add(new ColumnDefinition($"C{i}", typeof(int))
            {
                DefaultValueExpression = (i * 11).ToString(System.Globalization.CultureInfo.InvariantCulture),
                Description = $"Column number {i} with a fairly long description text",
            });
        }

        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("P", columns, ct);
        }

        ms.Position = 0;
        await using AccessReader reader = await OpenReaderAsync(ms, ct);
        IReadOnlyList<ColumnMetadata> metadata = await reader.GetColumnMetadataAsync("P", ct);
        for (int i = 0; i < 6; i++)
        {
            ColumnMetadata column = metadata.Single(c => c.Name == $"C{i}");
            Assert.Equal((i * 11).ToString(System.Globalization.CultureInfo.InvariantCulture), column.DefaultValueExpression);
            Assert.Equal($"Column number {i} with a fairly long description text", column.Description);
        }
    }

    /// <summary>
    /// Dropping a table frees its long values' LVAL pages by following each
    /// value's chain. The value here fills its first chunk with row pointers
    /// to the Keep table's data page, and a trailing salt picks bytes whose
    /// Jet4-style token, which the writer used to store at offset 8 of every
    /// Jet3 LVAL page, made a Jet3 reader see a live slot 0 inside that chunk.
    /// The chain walk then read one of those pointers and freed Keep's page.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task DropTable_ChainedLongValue_FreesOnlyItsLvalPages()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("Keep", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Name", typeof(string), maxLength: 50)], ct);
            await writer.InsertRowsAsync("Keep", [[1, "one"], [2, "two"]], ct);
        }

        long keepPage;
        ms.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: ct))
        {
            CatalogEntry keep = Assert.IsType<CatalogEntry>(await harness.Services.Catalog.GetCatalogEntryAsync("Keep", ct));
            keepPage = (await harness.Database.GetLiveRowLocationsAsync(keep.TDefPage, ct))[0].PageNumber;
        }

        Assert.InRange(keepPage, 3, 255);
        byte[] before = ms.ToArray();
        byte[] payload = PointerFilledPayload((byte)keepPage);

        await using (AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false }, ct))
        {
            await writer.CreateTableAsync(TableName, [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Blob", typeof(byte[]))], ct);
            await writer.InsertRowAsync(TableName, [1, payload], ct);
            await writer.DropTableAsync(TableName, ct);
        }

        byte[] after = ms.ToArray();
        for (int page = 0; page < before.Length / Jet3PageSize; page++)
        {
            Assert.True(
                before[page * Jet3PageSize] == after[page * Jet3PageSize],
                $"Page {page} changed type from 0x{before[page * Jet3PageSize]:X2} to 0x{after[page * Jet3PageSize]:X2}.");
        }

        ms.Position = 0;
        await using AccessReader reader = await OpenReaderAsync(ms, ct);
        List<object[]> rows = await reader.Rows("Keep", cancellationToken: ct).ToListAsync(ct);
        Assert.Equal(["one", "two"], rows.OrderBy(r => (int)r[0]).Select(r => (string)r[1]));
    }

    private static string Memo(int id)
    {
        int length = Sizes[id - 1].Memo;
        var text = new StringBuilder(length);
        for (int i = 0; i < length; i++)
        {
            text.Append((char)('a' + ((i + id) % 26)));
        }

        return text.ToString();
    }

    /// <summary>
    /// OLE bytes that start <c>11 22</c> and carry no file signature, so the
    /// read API returns them unchanged.
    /// </summary>
    /// <param name="id">The row id.</param>
    /// <returns>The OLE bytes.</returns>
    private static byte[] Ole(int id)
    {
        byte[] bytes = new byte[Sizes[id - 1].Ole];
        for (int i = 0; i < bytes.Length; i++)
        {
            bytes[i] = unchecked((byte)((i * 7) + id));
        }

        bytes[0] = 0x11;
        bytes[1] = 0x22;
        return bytes;
    }

    private static string OleDataUri(int id) => "data:application/octet-stream;base64," + Convert.ToBase64String(Ole(id));

    private static byte[] PointerFilledPayload(byte targetPage)
    {
        byte[] data = new byte[5000];

        // The old layout put the first chunk at offset 24 of its page, after the
        // 20-byte header area and the 4-byte next-row pointer.
        const int oldFirstChunkStart = 24;
        for (int i = 0; i + 4 <= 2024; i += 4)
        {
            data[i + 1] = targetPage;
        }

        for (int salt = 0; ; salt++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(data.AsSpan(4990), salt);
            uint token = LongValueStore.ComputeToken(data);
            int slot0 = (int)(token >> 16);
            int rowStart = slot0 & 0x1FFF;
            if ((slot0 & 0xC000) == 0 && (token & 0xFFFF) != 0 && rowStart >= oldFirstChunkStart && rowStart <= Jet3PageSize - 8 && rowStart % 4 == 0)
            {
                return data;
            }
        }
    }

    private static async Task<List<LongValueDescriptor>> ReadDescriptorsAsync(DatabaseFile db, long tdefPage, string columnName, CancellationToken cancellationToken)
    {
        TableDef tableDef = Assert.IsType<TableDef>(await db.ReadTableDefAsync(tdefPage, cancellationToken));
        ColumnInfo column = tableDef.Columns.Single(c => c.Name == columnName);
        bool hasVarColumns = tableDef.Columns.Any(c => !c.IsFixed);
        var descriptors = new List<LongValueDescriptor>();
        await db.ForEachLiveTableRowAsync(
            tdefPage,
            (row, _) =>
            {
                RowLocation location = row.Location;
                if (db.TryParseRowLayout(row.Page, location.RowStart, location.RowSize, hasVarColumns, out RowLayout layout))
                {
                    ColumnSlice slice = db.ResolveColumnSlice(row.Page, location.RowStart, location.RowSize, layout, column);
                    if (slice.Kind is ColumnSliceKind.Fixed or ColumnSliceKind.Var
                        && LongValueDescriptor.TryRead(row.Page.AsSpan(location.RowStart + slice.DataStart, slice.DataLen), out LongValueDescriptor descriptor))
                    {
                        descriptors.Add(descriptor);
                    }
                }

                return new ValueTask<bool>(true);
            },
            cancellationToken);

        return descriptors;
    }

    /// <summary>
    /// Reads a long value straight from Jet3 page bytes: the row count at 8,
    /// row offsets from 10, and each row ending where the next higher row starts.
    /// </summary>
    /// <param name="file">The database bytes.</param>
    /// <param name="descriptor">The value's descriptor.</param>
    /// <returns>The value's bytes.</returns>
    private static byte[] ReadJet3Payload(byte[] file, LongValueDescriptor descriptor)
    {
        var payload = new List<byte>(descriptor.Length);
        uint lvalDp = descriptor.FirstDp;
        while (lvalDp != 0 && payload.Count < descriptor.Length)
        {
            (int start, int end) = Jet3RowBounds(file, lvalDp);
            int pageStart = LongValueStore.PageNumber(lvalDp) * Jet3PageSize;
            if (descriptor.IsSinglePage)
            {
                payload.AddRange(file.AsSpan(pageStart + start, Math.Min(end - start, descriptor.Length)).ToArray());
                break;
            }

            lvalDp = BinaryPrimitives.ReadUInt32LittleEndian(file.AsSpan(pageStart + start, 4));
            int take = Math.Min(end - start - 4, descriptor.Length - payload.Count);
            payload.AddRange(file.AsSpan(pageStart + start + 4, take).ToArray());
        }

        Assert.Equal(descriptor.Length, payload.Count);
        return [.. payload];
    }

    private static (int Start, int End) Jet3RowBounds(byte[] file, uint lvalDp)
    {
        int pageStart = LongValueStore.PageNumber(lvalDp) * Jet3PageSize;
        int rowIndex = LongValueStore.RowIndex(lvalDp);
        Assert.Equal(0x01, file[pageStart]);
        Assert.Equal("LVAL"u8.ToArray(), file.AsSpan(pageStart + 4, 4).ToArray());
        int numRows = BinaryPrimitives.ReadUInt16LittleEndian(file.AsSpan(pageStart + 8, 2));
        Assert.InRange(rowIndex, 0, numRows - 1);
        int start = BinaryPrimitives.ReadUInt16LittleEndian(file.AsSpan(pageStart + 10 + (rowIndex * 2), 2)) & 0x1FFF;
        int end = Jet3PageSize;
        for (int slot = 0; slot < numRows; slot++)
        {
            int other = BinaryPrimitives.ReadUInt16LittleEndian(file.AsSpan(pageStart + 10 + (slot * 2), 2)) & 0x1FFF;
            if (other > start && other < end)
            {
                end = other;
            }
        }

        return (start, end);
    }

    private static byte[] PageWithoutNextPointer(byte[] file, uint lvalDp, out uint nextDp)
    {
        (int start, _) = Jet3RowBounds(file, lvalDp);
        byte[] page = file.AsSpan(LongValueStore.PageNumber(lvalDp) * Jet3PageSize, Jet3PageSize).ToArray();
        nextDp = BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(start, 4));
        Array.Clear(page, start, 4);
        return page;
    }

    private static int CountLvalPages(byte[] file)
    {
        int count = 0;
        for (int page = 1; page < file.Length / Jet3PageSize; page++)
        {
            ReadOnlySpan<byte> bytes = file.AsSpan(page * Jet3PageSize, Jet3PageSize);
            if (bytes[0] == 0x01 && bytes.Slice(4, 4).SequenceEqual("LVAL"u8))
            {
                count++;
            }
        }

        return count;
    }

    private static string Sha256(byte[] bytes) => Convert.ToHexString(SHA256.HashData(bytes));

    private static async Task<int> OrdinalAsync(AccessReader reader, string table, string column, CancellationToken cancellationToken)
    {
        IReadOnlyList<ColumnMetadata> metadata = await reader.GetColumnMetadataAsync(table, cancellationToken);
        for (int i = 0; i < metadata.Count; i++)
        {
            if (metadata[i].Name == column)
            {
                return i;
            }
        }

        throw new InvalidOperationException($"{table} has no column {column}.");
    }

    private static async Task<MemoryStream> CreateDatabaseAsync(CancellationToken cancellationToken)
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Note", typeof(string), maxLength: 50),
                    new ColumnDefinition("Body", typeof(string)),
                    new ColumnDefinition("Blob", typeof(byte[])),
                ],
                cancellationToken);
        }

        return ms;
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream ms, AccessWriterOptions options, CancellationToken cancellationToken)
    {
        ms.Position = 0;
        return AccessWriter.OpenAsync(ms, options, leaveOpen: true, cancellationToken);
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream ms, CancellationToken cancellationToken)
    {
        ms.Position = 0;
        return AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken);
    }

    private sealed class Jet3Row
    {
        public int Id { get; set; }

        public string? Note { get; set; }

        public string? Body { get; set; }

        public byte[]? Blob { get; set; }
    }
}
