namespace JetDatabaseWriter.Benchmarks.Infrastructure;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;

/// <summary>
/// Builds (and caches by file existence) the synthetic .accdb files used
/// by the read-decode benchmarks. Files are written under
/// <see cref="Path.GetTempPath"/> so repeated benchmark runs reuse them.
/// Delete the files manually to force a rebuild.
/// </summary>
internal static class SyntheticDatabases
{
    /// <summary>Numeric/date-heavy table name (5 ints, currency, datetime).</summary>
    public const string NumericTable = "Numeric";

    /// <summary>Text-heavy table name (5 short-text columns).</summary>
    public const string TextTable = "TextHeavy";

    /// <summary>Wide table name (40 mixed columns).</summary>
    public const string WideTable = "Wide";

    /// <summary>MEMO-heavy table name (one int + one MEMO column with mixed payload sizes).</summary>
    public const string MemoTable = "Memos";

    /// <summary>MEMO table whose payloads stay inline in the row.</summary>
    public const string MemoInlineTable = "MemoInline";

    /// <summary>MEMO table whose payloads fit on a single LVAL page.</summary>
    public const string MemoSinglePageTable = "MemoSinglePage";

    /// <summary>MEMO table whose payloads require chained LVAL pages.</summary>
    public const string MemoChainedTable = "MemoChained";

    /// <summary>OLE table whose payloads stay inline in the row.</summary>
    public const string OleInlineTable = "OleInline";

    /// <summary>OLE table whose payloads fit on a single LVAL page.</summary>
    public const string OleSinglePageTable = "OleSinglePage";

    /// <summary>OLE table whose payloads require chained LVAL pages.</summary>
    public const string OleChainedTable = "OleChained";

    /// <summary>Small table used to isolate owned-page discovery cost.</summary>
    public const string OwnedPageDiscoveryTargetTable = "OwnedMapTarget";

    /// <summary>
    /// Table whose large MEMO and OLE values each span more LVAL pages than the
    /// default 256-page reader cache (Id, Body MEMO, Blob OLE).
    /// </summary>
    public const string LargeLongValueTable = "LargeLongValues";

    /// <summary>Rows in <see cref="LargeLongValueTable"/>.</summary>
    public const int LargeLongValueRows = 30;

    /// <summary>Characters in each large MEMO: the 3 rows whose Id is a multiple of 10.</summary>
    public const int LargeMemoLength = 1_500_000;

    /// <summary>Bytes in each large OLE value: the 3 rows whose Id ends in 5.</summary>
    public const int LargeOleLength = 2_000_000;

    /// <summary>Characters in each of the other rows' MEMO values, and bytes in their OLE values.</summary>
    public const int SmallLongValueLength = 100;

    /// <summary>Password of <see cref="AesNumericDbPath"/>.</summary>
    public const string AesPassword = "JetBench";

    /// <summary>Relational parent table (CustomerId primary key, Name).</summary>
    public const string CustomersTable = "Customers";

    /// <summary>
    /// Relational child table (OrderId primary key, CustomerId, OrderDate, Amount),
    /// related to <see cref="CustomersTable"/> by <c>FK_Orders_Customers</c>.
    /// </summary>
    public const string OrdersTable = "Orders";

    /// <summary>Name of the index Access gives a primary key.</summary>
    public const string PrimaryKeyIndex = "PrimaryKey";

    /// <summary>Non-unique index on <c>Orders.OrderDate</c>.</summary>
    public const string OrderDateIndex = "IX_Orders_OrderDate";

    /// <summary>Rows in <see cref="CustomersTable"/>.</summary>
    public const int RelationalCustomers = 1_000;

    /// <summary>
    /// Rows in <see cref="OrdersTable"/>: 10 per customer, fewer than planned
    /// (see <c>docs/design/read-performance-bottlenecks.md</c>; raising it is
    /// open work in <c>docs/todo.md</c>).
    /// </summary>
    public const int RelationalOrders = 10_000;

    /// <summary>Attachment table (Id primary key, Files attachment).</summary>
    public const string DocumentsTable = "Documents";

    /// <summary>The Attachment column of <see cref="DocumentsTable"/>.</summary>
    public const string DocumentsAttachmentColumn = "Files";

    /// <summary>
    /// Rows in <see cref="DocumentsTable"/>, each with one attachment. Kept small
    /// because each <c>AddAttachmentAsync</c> rebuilds the hidden flat table's
    /// indexes (see <c>docs/design/read-performance-bottlenecks.md</c>).
    /// </summary>
    public const int AttachmentDocuments = 150;

    /// <summary>Bytes in each attachment of <see cref="DocumentsTable"/>.</summary>
    public const int AttachmentBytes = 16 * 1024;

    /// <summary>Rows in <see cref="NumericTable"/>.</summary>
    public const int NumericRows = 25_000;

    /// <summary>
    /// Small query table (Id primary key, Name, Price) of <see cref="QuerySmallRows"/> rows,
    /// for the per-call cost of the LINQ and typed-read benchmarks.
    /// </summary>
    public const string QuerySmallTable = "QuerySmall";

    /// <summary>Rows in <see cref="QuerySmallTable"/>.</summary>
    public const int QuerySmallRows = 6;

    /// <summary>
    /// Large query table (Id primary key, Score, Name) of <see cref="QueryLargeRows"/> rows,
    /// with the non-unique <see cref="QueryScoreIndex"/> on Score. Row <c>n</c> (0-based)
    /// has Id <c>n</c> and Score <c>n % 1000</c>.
    /// </summary>
    public const string QueryLargeTable = "QueryLarge";

    /// <summary>Non-unique index on <c>QueryLarge.Score</c>.</summary>
    public const string QueryScoreIndex = "IX_QueryLarge_Score";

    /// <summary>Rows in <see cref="QueryLargeTable"/>.</summary>
    public const int QueryLargeRows = 25_000;

    private const int TextRows = 25_000;
    private const int WideRows = 10_000;
    private const int WideColumnCount = 40;
    private const int MemoRows = 5_000;
    private const int LongValueSubmodeRows = 2_000;
    private const int InlineLongValueLength = 32;
    private const int SinglePageLongValueLength = 2_000;
    private const int ChainedLongValueLength = 16_000;
    private const int OwnedPageDiscoveryTargetRows = 128;
    private const int OwnedPageDiscoveryFillerRows = 20_000;
    private const string OwnedPageDiscoveryFillerTable = "OwnedMapFiller";

    private static readonly string TempRoot = Path.Combine(Path.GetTempPath(), "JetBench");

    public static string NumericDbPath => Path.Combine(TempRoot, $"Numeric_{NumericRows}.accdb");

    public static string TextDbPath => Path.Combine(TempRoot, $"Text_{TextRows}.accdb");

    public static string WideDbPath => Path.Combine(TempRoot, $"Wide_{WideColumnCount}c_{WideRows}.accdb");

    public static string MemoDbPath => Path.Combine(TempRoot, $"Memo_{MemoRows}_lv2.accdb");

    public static string OwnedPageDiscoveryMappedDbPath => Path.Combine(
        TempRoot,
        $"OwnedPageDiscovery_{OwnedPageDiscoveryTargetRows}_{OwnedPageDiscoveryFillerRows}_mapped_v1.accdb");

    public static string OwnedPageDiscoveryFallbackDbPath => Path.Combine(
        TempRoot,
        $"OwnedPageDiscovery_{OwnedPageDiscoveryTargetRows}_{OwnedPageDiscoveryFillerRows}_fallback_v1.accdb");

    public static string LargeLongValueDbPath => Path.Combine(TempRoot, "LargeLongValue_v1.accdb");

    /// <summary>Gets the path of an <c>AccdbAgileCfb</c>-encrypted copy of <see cref="NumericDbPath"/>.</summary>
    public static string AesNumericDbPath => Path.Combine(TempRoot, $"Numeric_{NumericRows}_agile_cfb_v1.accdb");

    public static string RelationalDbPath => Path.Combine(TempRoot, $"Relational_{RelationalCustomers}_{RelationalOrders}_v1.accdb");

    public static string AttachmentDbPath => Path.Combine(TempRoot, $"Attachments_{AttachmentDocuments}_v1.accdb");

    public static string QueryDbPath => Path.Combine(TempRoot, $"Query_{QuerySmallRows}_{QueryLargeRows}_v1.accdb");

    /// <summary>Gets the total MEMO characters in <see cref="LargeLongValueTable"/>.</summary>
    public static long LargeLongValueMemoChars { get; } = SumLargeLongValueLengths(IsLargeMemoRow, LargeMemoLength);

    /// <summary>Gets the total OLE bytes in <see cref="LargeLongValueTable"/>.</summary>
    public static long LargeLongValueOleBytes { get; } = SumLargeLongValueLengths(IsLargeOleRow, LargeOleLength);

    /// <summary>
    /// Ensures all synthetic DBs exist on disk. Skips files that already
    /// exist (cache by path). Safe to call from <c>[GlobalSetup]</c>.
    /// </summary>
    public static async Task EnsureAllAsync()
    {
        Directory.CreateDirectory(TempRoot);
        await EnsureNumericAsync().ConfigureAwait(false);
        await EnsureTextAsync().ConfigureAwait(false);
        await EnsureWideAsync().ConfigureAwait(false);
        await EnsureMemoAsync().ConfigureAwait(false);
    }

    public static async Task EnsureOwnedPageDiscoveryAsync()
    {
        Directory.CreateDirectory(TempRoot);
        if (!File.Exists(OwnedPageDiscoveryMappedDbPath))
        {
            await CreateOwnedPageDiscoveryDatabaseAsync(OwnedPageDiscoveryMappedDbPath).ConfigureAwait(false);
        }

        if (!File.Exists(OwnedPageDiscoveryFallbackDbPath))
        {
            File.Copy(OwnedPageDiscoveryMappedDbPath, OwnedPageDiscoveryFallbackDbPath, overwrite: true);
            await PatchOwnedUsageMapToUnknownTypeAsync(
                OwnedPageDiscoveryFallbackDbPath,
                OwnedPageDiscoveryTargetTable).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Ensures <see cref="LargeLongValueDbPath"/> exists: 30 rows whose large MEMO
    /// and OLE values each span more LVAL pages than the default 256-page reader
    /// cache. The other rows hold short values, so a scan that loses the rows
    /// after a large value (as the page-cache eviction bug did) is visible.
    /// </summary>
    /// <returns>A task that completes when the file exists.</returns>
    public static async Task EnsureLargeLongValueAsync()
    {
        Directory.CreateDirectory(TempRoot);
        if (File.Exists(LargeLongValueDbPath))
        {
            return;
        }

        string building = Path.ChangeExtension(LargeLongValueDbPath, ".building.accdb");
        File.Delete(building);
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(building, DatabaseFormat.AceAccdb).ConfigureAwait(false))
        {
            await writer.CreateTableAsync(
                LargeLongValueTable,
                [
                    new("Id", typeof(int)),
                    new("Body", typeof(string)),
                    new("Blob", typeof(byte[])),
                ]).ConfigureAwait(false);

            // One row per insert keeps at most one large value in memory.
            for (int id = 1; id <= LargeLongValueRows; id++)
            {
                await writer.InsertRowAsync(
                    LargeLongValueTable,
                    [
                        id,
                        MakeMemoBody(id, IsLargeMemoRow(id) ? LargeMemoLength : SmallLongValueLength),
                        MakeOlePayload(id, IsLargeOleRow(id) ? LargeOleLength : SmallLongValueLength),
                    ]).ConfigureAwait(false);
            }
        }

        File.Move(building, LargeLongValueDbPath);
    }

    /// <summary>
    /// Ensures <see cref="AesNumericDbPath"/> exists: a copy of the numeric
    /// database encrypted as <see cref="AccessEncryptionFormat.AccdbAgileCfb"/>
    /// with <see cref="AesPassword"/>.
    /// </summary>
    /// <returns>A task that completes when the file exists.</returns>
    public static async Task EnsureAesNumericAsync()
    {
        Directory.CreateDirectory(TempRoot);
        await EnsureNumericAsync().ConfigureAwait(false);
        if (File.Exists(AesNumericDbPath))
        {
            return;
        }

        string building = Path.ChangeExtension(AesNumericDbPath, ".building.accdb");
        File.Copy(NumericDbPath, building, overwrite: true);
        await AccessWriter.EncryptAsync(building, AesPassword.AsMemory(), AccessEncryptionFormat.AccdbAgileCfb).ConfigureAwait(false);
        File.Move(building, AesNumericDbPath);
    }

    /// <summary>
    /// Ensures <see cref="RelationalDbPath"/> exists: <see cref="RelationalCustomers"/>
    /// customers and <see cref="RelationalOrders"/> orders (order <c>n</c> belongs to
    /// customer <c>((n - 1) % 1000) + 1</c>, is dated <see cref="OrderDate"/>(n), and
    /// each customer's k-th order has Amount <c>20 * k</c>), with
    /// <see cref="OrderDateIndex"/> and the <c>FK_Orders_Customers</c> relationship,
    /// created after the inserts so they skip the per-row referential check.
    /// </summary>
    /// <returns>A task that completes when the file exists.</returns>
    public static async Task EnsureRelationalAsync()
    {
        Directory.CreateDirectory(TempRoot);
        if (File.Exists(RelationalDbPath))
        {
            return;
        }

        string building = Path.ChangeExtension(RelationalDbPath, ".building.accdb");
        File.Delete(building);
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(building, DatabaseFormat.AceAccdb).ConfigureAwait(false))
        {
            await writer.CreateTableAsync(
                CustomersTable,
                [
                    new("CustomerId", typeof(int)) { IsPrimaryKey = true },
                    new("Name", typeof(string), 64),
                ]).ConfigureAwait(false);
            await writer.CreateTableAsync(
                OrdersTable,
                [
                    new("OrderId", typeof(int)) { IsPrimaryKey = true },
                    new("CustomerId", typeof(int)),
                    new("OrderDate", typeof(DateTime)),
                    new("Amount", typeof(decimal)),
                ],
                [new IndexDefinition(OrderDateIndex, "OrderDate")]).ConfigureAwait(false);

            var customers = new List<object[]>(RelationalCustomers);
            for (int id = 1; id <= RelationalCustomers; id++)
            {
                customers.Add([id, "Customer " + id.ToString(CultureInfo.InvariantCulture)]);
            }

            await writer.InsertRowsAsync(CustomersTable, customers).ConfigureAwait(false);

            var orders = new List<object[]>(RelationalOrders);
            for (int id = 1; id <= RelationalOrders; id++)
            {
                orders.Add([id, ((id - 1) % RelationalCustomers) + 1, OrderDate(id), (decimal)((((id - 1) / RelationalCustomers) + 1) * 20)]);
            }

            await writer.InsertRowsAsync(OrdersTable, orders).ConfigureAwait(false);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_Orders_Customers", CustomersTable, "CustomerId", OrdersTable, "CustomerId")).ConfigureAwait(false);
        }

        File.Move(building, RelationalDbPath);
    }

    /// <summary>
    /// Ensures <see cref="AttachmentDbPath"/> exists: <see cref="AttachmentDocuments"/>
    /// rows, each with one <see cref="AttachmentBytes"/>-byte attachment added through
    /// <c>AddAttachmentAsync</c>. The file names end in <c>.dat</c>, so the writer
    /// stores the data deflate-compressed, as Access does for types it does not
    /// keep raw; the bytes are pseudo-random, so they barely compress.
    /// </summary>
    /// <returns>A task that completes when the file exists.</returns>
    public static async Task EnsureAttachmentsAsync()
    {
        Directory.CreateDirectory(TempRoot);
        if (File.Exists(AttachmentDbPath))
        {
            return;
        }

        string building = Path.ChangeExtension(AttachmentDbPath, ".building.accdb");
        File.Delete(building);
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(building, DatabaseFormat.AceAccdb).ConfigureAwait(false))
        {
            await writer.CreateTableAsync(
                DocumentsTable,
                [
                    new("Id", typeof(int)) { IsPrimaryKey = true },
                    new(DocumentsAttachmentColumn, typeof(byte[])) { IsAttachment = true },
                ]).ConfigureAwait(false);

            var rows = new List<object[]>(AttachmentDocuments);
            for (int id = 1; id <= AttachmentDocuments; id++)
            {
                rows.Add([id, DBNull.Value]);
            }

            await writer.InsertRowsAsync(DocumentsTable, rows).ConfigureAwait(false);
            for (int id = 1; id <= AttachmentDocuments; id++)
            {
                await writer.AddAttachmentAsync(
                    DocumentsTable,
                    DocumentsAttachmentColumn,
                    new Dictionary<string, object?> { ["Id"] = id },
                    new AttachmentInput("document" + id.ToString(CultureInfo.InvariantCulture) + ".dat", MakeRandomBytes(id, AttachmentBytes))).ConfigureAwait(false);
            }
        }

        File.Move(building, AttachmentDbPath);
    }

    /// <summary>
    /// Ensures <see cref="QueryDbPath"/> exists: <see cref="QuerySmallTable"/> with
    /// <see cref="QuerySmallRows"/> rows (Id 1..6, Price <c>10 * Id</c>), and
    /// <see cref="QueryLargeTable"/> with <see cref="QueryLargeRows"/> rows and
    /// <see cref="QueryScoreIndex"/>.
    /// </summary>
    /// <returns>A task that completes when the file exists.</returns>
    public static async Task EnsureQueryAsync()
    {
        Directory.CreateDirectory(TempRoot);
        if (File.Exists(QueryDbPath))
        {
            return;
        }

        string building = Path.ChangeExtension(QueryDbPath, ".building.accdb");
        File.Delete(building);
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(building, DatabaseFormat.AceAccdb).ConfigureAwait(false))
        {
            await writer.CreateTableAsync(
                QuerySmallTable,
                [
                    new("Id", typeof(int)) { IsPrimaryKey = true },
                    new("Name", typeof(string), 32),
                    new("Price", typeof(decimal)),
                ]).ConfigureAwait(false);
            await writer.CreateTableAsync(
                QueryLargeTable,
                [
                    new("Id", typeof(int)) { IsPrimaryKey = true },
                    new("Score", typeof(int)),
                    new("Name", typeof(string), 32),
                ],
                [new IndexDefinition(QueryScoreIndex, "Score")]).ConfigureAwait(false);

            var small = new List<object[]>(QuerySmallRows);
            for (int id = 1; id <= QuerySmallRows; id++)
            {
                small.Add([id, "Item " + id.ToString(CultureInfo.InvariantCulture), (decimal)(10 * id)]);
            }

            await writer.InsertRowsAsync(QuerySmallTable, small).ConfigureAwait(false);

            var large = new List<object[]>(QueryLargeRows);
            for (int id = 0; id < QueryLargeRows; id++)
            {
                large.Add([id, id % 1000, "Row " + id.ToString(CultureInfo.InvariantCulture)]);
            }

            await writer.InsertRowsAsync(QueryLargeTable, large).ConfigureAwait(false);
        }

        File.Move(building, QueryDbPath);
    }

    /// <summary>Gets the OrderDate of order <paramref name="orderId"/>: one hour after the previous order, from 2020-01-01.</summary>
    /// <param name="orderId">The order id.</param>
    /// <returns>The order's date.</returns>
    public static DateTime OrderDate(int orderId) => new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Unspecified).AddHours(orderId);

    public static bool IsLargeMemoRow(int id) => id % 10 == 0;

    public static bool IsLargeOleRow(int id) => id % 10 == 5;

    private static long SumLargeLongValueLengths(Func<int, bool> isLargeRow, int largeLength)
    {
        long total = 0;
        for (int id = 1; id <= LargeLongValueRows; id++)
        {
            total += isLargeRow(id) ? largeLength : SmallLongValueLength;
        }

        return total;
    }

    private static async Task EnsureNumericAsync()
    {
        if (File.Exists(NumericDbPath))
        {
            return;
        }

        await using AccessWriter w = await AccessWriter.CreateDatabaseAsync(NumericDbPath, DatabaseFormat.AceAccdb).ConfigureAwait(false);
        await w.CreateTableAsync(
            NumericTable,
            [
                new("Id", typeof(int)),
                new("OrderId", typeof(int)),
                new("ProductId", typeof(int)),
                new("Quantity", typeof(short)),
                new("UnitPrice", typeof(decimal)),
                new("Discount", typeof(float)),
                new("StatusId", typeof(int)),
                new("AddedOn", typeof(DateTime)),
                new("ModifiedOn", typeof(DateTime)),
            ]).ConfigureAwait(false);

        var rows = new List<object[]>(NumericRows);
        var baseDate = new DateTime(2020, 1, 1);
        for (int i = 0; i < NumericRows; i++)
        {
            int discountBucket = i % 10;
            rows.Add(
            [
                i,
                i / 5,
                (i % 200) + 1,
                (short)((i % 50) + 1),
                (decimal)(1.99 + (i % 100)),
                (float)(discountBucket * 0.05),
                (i % 5) + 1,
                baseDate.AddMinutes(i),
                baseDate.AddMinutes(i + 30),
            ]);
        }

        await w.InsertRowsAsync(NumericTable, rows).ConfigureAwait(false);
    }

    private static async Task EnsureTextAsync()
    {
        if (File.Exists(TextDbPath))
        {
            return;
        }

        await using AccessWriter w = await AccessWriter.CreateDatabaseAsync(TextDbPath, DatabaseFormat.AceAccdb).ConfigureAwait(false);
        await w.CreateTableAsync(
            TextTable,
            [
                new("Id", typeof(int)),
                new("FirstName", typeof(string), 64),
                new("LastName", typeof(string), 64),
                new("Email", typeof(string), 128),
                new("City", typeof(string), 64),
                new("Notes", typeof(string), 255),
            ]).ConfigureAwait(false);

        var rows = new List<object[]>(TextRows);
        for (int i = 0; i < TextRows; i++)
        {
            rows.Add(
            [
                i,
                "First" + i,
                "Last" + i,
                "user" + i + "@example.com",
                "City" + (i % 100),
                "Note for row " + i + " — sample sentence with a few words to fill space.",
            ]);
        }

        await w.InsertRowsAsync(TextTable, rows).ConfigureAwait(false);
    }

    private static async Task EnsureWideAsync()
    {
        if (File.Exists(WideDbPath))
        {
            return;
        }

        var defs = new List<ColumnDefinition>(WideColumnCount)
        {
            new("Id", typeof(int)),
        };

        // 20 numeric, 19 text columns to round out the 40 total.
        for (int i = 0; i < 20; i++)
        {
            defs.Add(new ColumnDefinition("N" + i, typeof(int)));
        }

        for (int i = 0; i < 19; i++)
        {
            defs.Add(new ColumnDefinition("S" + i, typeof(string), 32));
        }

        await using AccessWriter w = await AccessWriter.CreateDatabaseAsync(WideDbPath, DatabaseFormat.AceAccdb).ConfigureAwait(false);
        await w.CreateTableAsync(WideTable, defs).ConfigureAwait(false);

        var rows = new List<object[]>(WideRows);
        for (int r = 0; r < WideRows; r++)
        {
            object[] row = new object[WideColumnCount];
            row[0] = r;
            for (int c = 1; c <= 20; c++)
            {
                row[c] = r * c;
            }

            for (int c = 21; c < WideColumnCount; c++)
            {
                row[c] = "v" + r + "_" + c;
            }

            rows.Add(row);
        }

        await w.InsertRowsAsync(WideTable, rows).ConfigureAwait(false);
    }

    private static async Task EnsureMemoAsync()
    {
        if (File.Exists(MemoDbPath))
        {
            return;
        }

        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(MemoDbPath, DatabaseFormat.AceAccdb).ConfigureAwait(false);
        await CreateMemoTableAsync(
            writer,
            MemoTable,
            MemoRows,
            static rowIndex => rowIndex % 3 switch
            {
                0 => InlineLongValueLength,
                1 => SinglePageLongValueLength,
                _ => ChainedLongValueLength,
            }).ConfigureAwait(false);

        await CreateMemoTableAsync(writer, MemoInlineTable, LongValueSubmodeRows, static _ => InlineLongValueLength).ConfigureAwait(false);
        await CreateMemoTableAsync(writer, MemoSinglePageTable, LongValueSubmodeRows, static _ => SinglePageLongValueLength).ConfigureAwait(false);
        await CreateMemoTableAsync(writer, MemoChainedTable, LongValueSubmodeRows, static _ => ChainedLongValueLength).ConfigureAwait(false);
        await CreateOleTableAsync(writer, OleInlineTable, LongValueSubmodeRows, InlineLongValueLength).ConfigureAwait(false);
        await CreateOleTableAsync(writer, OleSinglePageTable, LongValueSubmodeRows, SinglePageLongValueLength).ConfigureAwait(false);
        await CreateOleTableAsync(writer, OleChainedTable, LongValueSubmodeRows, ChainedLongValueLength).ConfigureAwait(false);
    }

    private static async Task CreateMemoTableAsync(AccessWriter writer, string tableName, int rowCount, Func<int, int> getLength)
    {
        await writer.CreateTableAsync(
            tableName,
            [
                new("Id", typeof(int)),
                new("Body", typeof(string)),
            ]).ConfigureAwait(false);

        var rows = new List<object[]>(rowCount);
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
        {
            int length = getLength(rowIndex);
            rows.Add([rowIndex, MakeMemoBody(rowIndex, length)]);
        }

        await writer.InsertRowsAsync(tableName, rows).ConfigureAwait(false);
    }

    private static async Task CreateOleTableAsync(AccessWriter writer, string tableName, int rowCount, int payloadLength)
    {
        await writer.CreateTableAsync(
            tableName,
            [
                new("Id", typeof(int)),
                new("Blob", typeof(byte[])),
            ]).ConfigureAwait(false);

        var rows = new List<object[]>(rowCount);
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
        {
            rows.Add([rowIndex, MakeOlePayload(rowIndex, payloadLength)]);
        }

        await writer.InsertRowsAsync(tableName, rows).ConfigureAwait(false);
    }

    private static async Task CreateOwnedPageDiscoveryDatabaseAsync(string databasePath)
    {
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(databasePath, DatabaseFormat.AceAccdb).ConfigureAwait(false);
        await writer.CreateTableAsync(OwnedPageDiscoveryTargetTable, OwnedPageDiscoverySchema()).ConfigureAwait(false);
        await writer.InsertRowsAsync(
            OwnedPageDiscoveryTargetTable,
            CreateOwnedPageDiscoveryRows(OwnedPageDiscoveryTargetRows, "T")).ConfigureAwait(false);

        await writer.CreateTableAsync(OwnedPageDiscoveryFillerTable, OwnedPageDiscoverySchema()).ConfigureAwait(false);
        await writer.InsertRowsAsync(
            OwnedPageDiscoveryFillerTable,
            CreateOwnedPageDiscoveryRows(OwnedPageDiscoveryFillerRows, "F")).ConfigureAwait(false);
    }

    private static ColumnDefinition[] OwnedPageDiscoverySchema() =>
    [
        new("Id", typeof(int)),
        new("Payload", typeof(string), maxLength: 240),
    ];

    private static List<object[]> CreateOwnedPageDiscoveryRows(int rowCount, string prefix)
    {
        var rows = new List<object[]>(rowCount);
        for (int rowNumber = 0; rowNumber < rowCount; rowNumber++)
        {
            rows.Add([rowNumber, prefix + "-" + rowNumber.ToString("D5", CultureInfo.InvariantCulture) + "-" + new string('x', 200)]);
        }

        return rows;
    }

    private static async Task PatchOwnedUsageMapToUnknownTypeAsync(string databasePath, string tableName)
    {
        int pageSize;
        long tdefPage;
        await using (AccessReader reader = await AccessReader.OpenAsync(
            databasePath,
            new AccessReaderOptions { UseLockFile = false }).ConfigureAwait(false))
        {
            pageSize = reader.PageSize;
            tdefPage = await ResolveTdefPageAsync(reader, tableName).ConfigureAwait(false);
        }

        byte[] fileBytes = await File.ReadAllBytesAsync(databasePath).ConfigureAwait(false);
        int rowAbsoluteStart = FindOwnedUsageMapRowStart(fileBytes, pageSize, tdefPage);
        if (fileBytes[rowAbsoluteStart] != 0x00)
        {
            throw new InvalidDataException("Expected an INLINE owned-pages usage-map row.");
        }

        fileBytes[rowAbsoluteStart] = 0x7F;
        await File.WriteAllBytesAsync(databasePath, fileBytes).ConfigureAwait(false);
    }

    private static async Task<long> ResolveTdefPageAsync(AccessReader reader, string tableName)
    {
        IReadOnlyList<ColumnMetadata> metadata = await reader.GetColumnMetadataAsync("MSysObjects").ConfigureAwait(false);
        int idIndex = FindColumnIndex(metadata, "Id");
        int nameIndex = FindColumnIndex(metadata, "Name");
        if (idIndex < 0 || nameIndex < 0)
        {
            throw new InvalidDataException("MSysObjects is missing Id or Name metadata.");
        }

        await foreach (object[] row in reader.Rows("MSysObjects").ConfigureAwait(false))
        {
            if (row[nameIndex] is string name && string.Equals(name, tableName, StringComparison.OrdinalIgnoreCase))
            {
                return Convert.ToInt64(row[idIndex], CultureInfo.InvariantCulture);
            }
        }

        throw new InvalidDataException($"Could not resolve the TDEF page for table '{tableName}'.");
    }

    private static int FindColumnIndex(IReadOnlyList<ColumnMetadata> metadata, string name)
    {
        for (int index = 0; index < metadata.Count; index++)
        {
            if (string.Equals(metadata[index].Name, name, StringComparison.OrdinalIgnoreCase))
            {
                return index;
            }
        }

        return -1;
    }

    private static int FindOwnedUsageMapRowStart(byte[] fileBytes, int pageSize, long tdefPage)
    {
        const int dataPageRowsStart = 14;
        const int ownedPagesPointerOffset = 0x37;
        int tdefOffset = checked((int)(tdefPage * pageSize));
        int usageMapRow = fileBytes[tdefOffset + ownedPagesPointerOffset];
        int usageMapPage = ReadUInt24(fileBytes, tdefOffset + ownedPagesPointerOffset + 1);
        int usageMapOffset = checked(usageMapPage * pageSize);
        int rowOffsetPosition = usageMapOffset + dataPageRowsStart + (usageMapRow * 2);
        int rowStart = BinaryPrimitives.ReadUInt16LittleEndian(fileBytes.AsSpan(rowOffsetPosition, 2)) & 0x1FFF;
        int rowAbsoluteStart = usageMapOffset + rowStart;

        if (usageMapPage <= 0 || rowAbsoluteStart < usageMapOffset || rowAbsoluteStart >= usageMapOffset + pageSize)
        {
            throw new InvalidDataException("The owned-pages usage-map pointer is outside the file bounds.");
        }

        return rowAbsoluteStart;
    }

    private static int ReadUInt24(byte[] buffer, int offset)
        => buffer[offset] | (buffer[offset + 1] << 8) | (buffer[offset + 2] << 16);

    private static string MakeMemoBody(int seed, int length)
    {
        char[] buffer = new char[length];
        string prefix = "row" + seed + ":";
        int prefixLength = Math.Min(prefix.Length, length);
        prefix.AsSpan(0, prefixLength).CopyTo(buffer);
        for (int index = prefixLength; index < length; index++)
        {
            buffer[index] = (char)('a' + ((index + seed) % 26));
        }

        return new string(buffer);
    }

    private static byte[] MakeRandomBytes(int seed, int length)
    {
        byte[] bytes = new byte[length];
#pragma warning disable CA5394 // A fixed seed makes the fixture reproducible; nothing here is security-sensitive.
        new Random(seed).NextBytes(bytes);
#pragma warning restore CA5394
        return bytes;
    }

    private static byte[] MakeOlePayload(int seed, int length)
    {
        byte[] payload = new byte[length];
        for (int index = 0; index < payload.Length; index++)
        {
            payload[index] = (byte)(((index + seed) % 251) + 1);
        }

        return payload;
    }
}
