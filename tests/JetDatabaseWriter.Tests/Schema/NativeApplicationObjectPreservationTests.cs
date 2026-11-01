namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Public mutations preserve frozen native application objects without invoking their content.</summary>
public sealed class NativeApplicationObjectPreservationTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets both native specimens, every public write mode and the independent mutation paths.</summary>
    public static TheoryData<bool, WriteMode, string> Mutations()
    {
        var cases = new TheoryData<bool, WriteMode, string>();
        foreach (bool codeObjects in new[] { false, true })
        {
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                foreach (string operation in new[] { "rows", "columns", "table" })
                {
                    cases.Add(codeObjects, mode, operation);
                }
            }
        }

        return cases;
    }

    [Theory]
    [MemberData(nameof(Mutations))]
    public async Task PublicMutation_PreservesCompleteApplicationStorage(bool codeObjects, WriteMode mode, string operation)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.ApplicationPreservation");
        await SeedAsync(session.SourcePath, codeObjects);
        Snapshot original = await ReadSnapshotAsync(session.SourcePath);
        Assert.Contains(original.Catalog, row => row.Contains(Hex("EvidenceForm"), StringComparison.Ordinal));
        Assert.Contains(original.Catalog, row => row.Contains(Hex("EvidenceReport"), StringComparison.Ordinal));
        Assert.True(original.Pages.Count > 0);
        Assert.True(original.Tables["MSysAccessStorage"].Length > 0);
        Assert.NotEmpty(original.Permissions);
        Assert.True(original.SharedPermissionPages > 0);
        Assert.NotEmpty(original.PermissionRows);
        Assert.NotEmpty(original.LongValueRows);
        if (codeObjects)
        {
            Assert.Contains(original.Catalog, row => row.Contains(Hex("EvidenceMacro"), StringComparison.Ordinal));
            Assert.Contains(original.Catalog, row => row.Contains(Hex("EvidenceModule"), StringComparison.Ordinal));
        }

        await MutateAsync(session.SourcePath, mode, operation);
        Snapshot actual = await ReadSnapshotAsync(session.SourcePath);
        Assert.Equal(original.Catalog, actual.Catalog);
        Assert.Equal(original.Tables.Keys, actual.Tables.Keys);
        foreach (KeyValuePair<string, string[]> table in original.Tables)
        {
            Assert.Equal(table.Value, actual.Tables[table.Key]);
        }

        Assert.Equal(original.Pages.Keys, actual.Pages.Keys);
        foreach (KeyValuePair<long, byte[]> page in original.Pages)
        {
            Assert.Equal(page.Value, actual.Pages[page.Key]);
        }

        Assert.Equal(original.Permissions, actual.Permissions);
        Assert.Equal(original.PermissionRows, actual.PermissionRows);
        if (operation == "rows")
        {
            Assert.Equal(original.PermissionPages, actual.PermissionPages);
        }
        Assert.Equal(original.LongValueDescriptors, actual.LongValueDescriptors);
        Assert.Equal(original.LongValueRows, actual.LongValueRows);

        await AssertItemsAsync(session.SourcePath, operation);
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData(false, "rows")]
    [InlineData(false, "columns")]
    [InlineData(false, "table")]
    [InlineData(true, "rows")]
    [InlineData(true, "columns")]
    [InlineData(true, "table")]
    public async Task Dao120_PublicMutationPreservesInertDefinitionsThroughCompact(bool codeObjects, string operation)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.ApplicationPreservation.Native");
        await SeedAsync(session.SourcePath, codeObjects);
        string control = session.CreateDatabasePath("control");
        string compactedControl = session.CreateDatabasePath("control-compact");
        File.Copy(session.SourcePath, control);
        await MutateAsync(session.SourcePath, WriteMode.AutoCommit, operation);
        string codeChecks = codeObjects
            ? "if ($db.Containers('Scripts').Documents('EvidenceMacro').Name -cne 'EvidenceMacro') { throw 'Macro identity changed' }; if ($db.Containers('Modules').Documents('EvidenceModule').Name -cne 'EvidenceModule') { throw 'Module identity changed' }"
            : string.Empty;
        string expectedLabel = operation == "rows" ? "updated" : "retained";
        string script = $$"""
            function Read-Definitions([string]$path, [string]$label) {
                $db = $engine.OpenDatabase($path, $false, $true)
                try {
                    if ($db.Containers('Forms').Documents('EvidenceForm').Name -cne 'EvidenceForm') { throw 'Form identity changed' }
                    if ($db.Containers('Reports').Documents('EvidenceReport').Name -cne 'EvidenceReport') { throw 'Report identity changed' }
                    {{codeChecks}}
                    $sql = $db.QueryDefs('EvidenceQuery').SQL
                    $rs = $db.OpenRecordset('SELECT [Id],[Label] FROM [Items]', 4)
                    try {
                        if ($rs.EOF -or $rs.Fields('Id').Value -ne 1 -or $rs.Fields('Label').Value -cne $label) { throw 'Expected mutation was lost' }
                        $rs.MoveNext()
                        if (!$rs.EOF) { throw 'Unexpected row' }
                    } finally { $rs.Close() }
                    return $sql
                } finally { $db.Close() }
            }
            $expected = Read-Definitions {{Literal(control)}} 'retained'
            $engine.CompactDatabase({{Literal(control)}}, {{Literal(compactedControl)}})
            $nativeCompacted = Read-Definitions {{Literal(compactedControl)}} 'retained'
            if ($nativeCompacted -cne $expected) { throw 'Native control compact changed the saved-query definition' }
            $actual = Read-Definitions {{Literal(session.SourcePath)}} '{{expectedLabel}}'
            if ($actual -cne $expected) { throw 'Saved-query definition changed' }
            $engine.CompactDatabase({{Literal(session.SourcePath)}}, {{Literal(session.CompactedPath)}})
            $compacted = Read-Definitions {{Literal(session.CompactedPath)}} '{{expectedLabel}}'
            if ($compacted -cne $expected) { throw 'Compacted saved-query definition changed' }
            Write-Output 'INERT_APPLICATION_PUBLIC_MUTATION=OK'
            """;
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0, $"Native application preservation failed: {result.StdOut}\n{result.StdErr}");
        Assert.Contains("INERT_APPLICATION_PUBLIC_MUTATION=OK", result.StdOut, StringComparison.Ordinal);
        foreach (string name in new[] { "MSysAccessStorage", "MSysQueries", "f_C54F32B8B3CB4285928325593DE12252_Data" })
        {
            NamedTableSnapshot original = await ReadNamedTableAsync(compactedControl, name);
            NamedTableSnapshot compacted = await ReadNamedTableAsync(session.CompactedPath, name);
            Assert.Equal(original.Columns, compacted.Columns);
            Assert.Equal(original.Rows, compacted.Rows);
        }
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Dao120_NativeCompactPreservesApplicationValuesByColumnName(bool codeObjects)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.ApplicationPreservation.NativeControl");
        await SeedAsync(session.SourcePath, codeObjects);
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(
            $$"""
            $engine.CompactDatabase({{Literal(session.SourcePath)}}, {{Literal(session.CompactedPath)}})
            """,
            TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0, $"Native control compact failed: {result.StdOut}\n{result.StdErr}");
        foreach (string name in new[] { "MSysAccessStorage", "MSysQueries", "f_C54F32B8B3CB4285928325593DE12252_Data" })
        {
            NamedTableSnapshot original = await ReadNamedTableAsync(session.SourcePath, name);
            NamedTableSnapshot compacted = await ReadNamedTableAsync(session.CompactedPath, name);
            Assert.Equal(original.Columns, compacted.Columns);
            Assert.Equal(original.Rows, compacted.Rows);
        }
    }

    private static async Task SeedAsync(string path, bool codeObjects)
    {
        string name = codeObjects ? "NativeInertCodeObjects.accdb" : "NativeInertApplicationObjects.accdb";
        string hash = codeObjects
            ? "EA1899DD97A8492C30B2F720201DD3A13046BEC23915C21A7DD938A12041FEE4"
            : "C809AA9371DF43F0901F6AA27C5ADDEEAAA7BEDA07C37578F2863F3CCF77364F";
        byte[] bytes = await File.ReadAllBytesAsync(Path.Combine(AppContext.BaseDirectory, "Databases", "ApplicationObjects", name), Ct);
        Assert.Equal(hash, Convert.ToHexString(SHA256.HashData(bytes)));
        await File.WriteAllBytesAsync(path, bytes, Ct);
    }

    private static async Task MutateAsync(string path, WriteMode mode, string operation)
    {
        await using AccessWriter writer = await AccessWriter.OpenAsync(path, WriteModes.WriterOptions(mode), Ct);
        await WriteModes.RunAsync(
            writer,
            mode,
            async () =>
            {
                switch (operation)
                {
                    case "rows":
                        await writer.InsertRowAsync("Items", [2, "temporary"], Ct);
                        Assert.Equal(1, await writer.UpdateRowsAsync("Items", "Id", 1, new Dictionary<string, object?> { ["Label"] = "updated" }, Ct));
                        Assert.Equal(1, await writer.DeleteRowsAsync("Items", "Id", 2, Ct));
                        break;
                    case "columns":
                        await writer.AddColumnAsync("Items", new ColumnDefinition("Extra", typeof(int)), Ct);
                        await writer.RenameColumnAsync("Items", "Extra", "Renamed", Ct);
                        await writer.DropColumnAsync("Items", "Renamed", Ct);
                        break;
                    case "table":
                        await writer.CreateTableAsync("Scratch", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
                        await writer.InsertRowAsync("Scratch", [1], Ct);
                        await writer.DropTableAsync("Scratch", Ct);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(operation));
                }
            },
            Ct);
    }

    private static async Task AssertItemsAsync(string path, string operation)
    {
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(path, cancellationToken: Ct);
        using DataTable table = await reader.ReadTableAsync("Items", cancellationToken: Ct);
        Assert.Equal(["Id", "Label"], table.Columns.Cast<DataColumn>().Select(column => column.ColumnName));
        DataRow row = Assert.Single(table.AsEnumerable());
        Assert.Equal(1, row["Id"]);
        Assert.Equal(operation == "rows" ? "updated" : "retained", row["Label"]);
        Assert.DoesNotContain(await reader.ListTablesAsync(Ct), name => name == "Scratch");
    }

    private static async Task<Snapshot> ReadSnapshotAsync(string path)
    {
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(path, cancellationToken: Ct);
        using DataTable catalog = await reader.ReadTableAsync("MSysObjects", cancellationToken: Ct);
        string[] identities = catalog.AsEnumerable().Where(row => (string)row["Name"] != "Items")
            .Select(EncodeRow).Order(StringComparer.Ordinal).ToArray();
        var tables = new SortedDictionary<string, string[]>(StringComparer.Ordinal);
        var owners = new HashSet<int>();
        var longValues = new SortedDictionary<uint, string>();
        var descriptors = new List<string>();
        byte[] bytes = await File.ReadAllBytesAsync(path, Ct);
        int itemId = (int)Assert.Single(catalog.AsEnumerable(), row => (string)row["Name"] == "Items")["Id"];
        using DataTable permissions = await reader.ReadTableAsync("MSysACEs", cancellationToken: Ct);
        string[] retainedPermissions = permissions.AsEnumerable().Select(EncodeRow).Order(StringComparer.Ordinal).ToArray();
        PermissionStorage permissionStorage = await SnapshotPermissionsAsync(reader);
        foreach (DataRow row in catalog.AsEnumerable().Where(row => (short)row["Type"] == Constants.SystemObjects.UserTableType))
        {
            string name = (string)row["Name"];
            if (name is "Items" or "MSysObjects" or "MSysACEs")
            {
                continue;
            }

            owners.Add((int)row["Id"]);
            using DataTable data = await reader.ReadTableAsync(name, cancellationToken: Ct);
            tables.Add(name, data.AsEnumerable().Select(EncodeRow).Order(StringComparer.Ordinal).ToArray());
        }

        // Catalog rows can move while the edited table's properties change. Select
        // retained objects by Id, then pin each external LVAL bookmark and full row,
        // including the native token, directory entry, chain pointer and padding.
        foreach (int owner in owners.Append(2))
        {
            await SnapshotLongValuesAsync(reader, bytes, owner, owner == 2 ? itemId : null, descriptors, longValues);
        }

        int pageSize = reader.Database.Format.PageSize;
        int ownerOffset = reader.Database.Format.DataPage.TDefOff;
        var pages = new SortedDictionary<long, byte[]>();
        for (int offset = pageSize; offset < bytes.Length; offset += pageSize)
        {
            ReadOnlySpan<byte> page = bytes.AsSpan(offset, pageSize);
            if (owners.Contains(offset / pageSize)
                || (page[0] is Constants.PageTypes.Data or Constants.PageTypes.IndexLeaf or Constants.PageTypes.IndexIntermediate
                    && owners.Contains(BinaryPrimitives.ReadInt32LittleEndian(page.Slice(ownerOffset, sizeof(int))))))
            {
                pages.Add(offset / pageSize, page.ToArray());
            }
        }

        return new(identities, tables, pages, retainedPermissions, permissionStorage.Rows, permissionStorage.Pages, permissionStorage.SharedPages, descriptors.Order(StringComparer.Ordinal).ToArray(), longValues);
    }

    private static async Task<PermissionStorage> SnapshotPermissionsAsync(AccessReader reader)
    {
        using DataTable catalog = await reader.ReadTableAsync("MSysObjects", cancellationToken: Ct);
        int owner = (int)Assert.Single(catalog.AsEnumerable(), row => (string)row["Name"] == "MSysACEs")["Id"];
        TDefImage definition = Assert.IsType<TDefImage>(await reader.Database.TableDefs.ReadImageAsync(owner, Ct));
        var rows = new SortedDictionary<string, string>(StringComparer.Ordinal);
        var pages = new SortedDictionary<long, string>();
        var objectsByPage = new Dictionary<long, HashSet<int>>();
        await reader.Database.OwnedPages.ForEachLiveTableRowAsync(
            owner,
            async (row, token) =>
            {
                int? objectId = null;
                await CompactRawFieldReader.ScanAsync(
                    reader.Database.Format,
                    definition,
                    row.Location,
                    row.Page.AsMemory(row.Location.RowStart, row.Location.RowSize),
                    (field, _) =>
                    {
                        if (field.Column.Name == "ObjectId")
                        {
                            objectId = BinaryPrimitives.ReadInt32LittleEndian(field.RawBytes.Span);
                        }

                        return ValueTask.CompletedTask;
                    },
                    token);
                Assert.NotNull(objectId);
                long number = row.Location.PageNumber;
                if (!objectsByPage.TryGetValue(number, out HashSet<int>? objectIds))
                {
                    objectIds = [];
                    objectsByPage.Add(number, objectIds);
                    pages.Add(number, Convert.ToHexString(row.Page.AsSpan(0, reader.PageSize)));
                }

                objectIds.Add(objectId.Value);
                int slotOffset = reader.Database.Format.DataPage.RowsStart + (row.Location.RowIndex * sizeof(ushort));
                rows.Add(
                    $"{number}:{row.Location.RowIndex}",
                    Convert.ToHexString(row.Page.AsSpan(4, 8)) + "|"
                        + Convert.ToHexString(row.Page.AsSpan(slotOffset, sizeof(ushort))) + "|"
                        + Convert.ToHexString(row.Page.AsSpan(row.Location.RowStart, row.Location.RowSize)));
                return true;
            },
            Ct,
            requireCompleteRows: true);
        return new(rows, pages, objectsByPage.Values.Count(objects => objects.Count > 1));
    }

    private static async Task SnapshotLongValuesAsync(AccessReader reader, byte[] bytes, int owner, int? excludedId, List<string> descriptors, SortedDictionary<uint, string> rows)
    {
        TDefImage definition = Assert.IsType<TDefImage>(await reader.Database.TableDefs.ReadImageAsync(owner, Ct));
        await reader.Database.OwnedPages.ForEachLiveTableRowAsync(
            owner,
            async (row, token) =>
            {
                var fields = new List<CompactRawField>();
                await CompactRawFieldReader.ScanAsync(
                    reader.Database.Format,
                    definition,
                    row.Location,
                    row.Page.AsMemory(row.Location.RowStart, row.Location.RowSize),
                    (field, _) =>
                    {
                        fields.Add(field);
                        return ValueTask.CompletedTask;
                    },
                    token);
                int? catalogId = excludedId.HasValue
                    ? BinaryPrimitives.ReadInt32LittleEndian(Assert.Single(fields, field => field.Column.Name == "Id").RawBytes.Span)
                    : null;
                if (catalogId == excludedId && excludedId.HasValue)
                {
                    return true;
                }

                foreach (CompactRawField field in fields.Where(field => field.Kind == CompactRawFieldKind.Stored && field.IsLongValue))
                {
                    string identity = catalogId.HasValue ? $"id:{catalogId}" : $"row:{row.Location.PageNumber}:{row.Location.RowIndex}";
                    descriptors.Add($"{owner}|{identity}|{field.Column.Name}|{Convert.ToHexString(field.RawBytes.Span)}");
                    SnapshotLongValueRows(bytes, reader.Database.Format, field.RawBytes.Span, rows);
                }

                return true;
            },
            Ct,
            requireCompleteRows: true);
    }

    private static void SnapshotLongValueRows(byte[] bytes, JetFormat format, ReadOnlySpan<byte> descriptor, SortedDictionary<uint, string> rows)
    {
        Assert.True(descriptor.Length >= Constants.LongValue.HeaderSize);
        uint lengthAndMode = BinaryPrimitives.ReadUInt32LittleEndian(descriptor);
        uint mode = lengthAndMode >> 30;
        if (mode == 2)
        {
            return;
        }

        Assert.True(mode is 0 or 1);
        int remaining = checked((int)(lengthAndMode & 0x3FFFFFFFu));
        uint pointer = BinaryPrimitives.ReadUInt32LittleEndian(descriptor[4..]);
        var visited = new HashSet<uint>();
        while (remaining > 0)
        {
            Ct.ThrowIfCancellationRequested();
            Assert.True(pointer != 0 && visited.Add(pointer), "Native LVAL chain must have unique nonzero bookmarks.");
            int number = checked((int)(pointer >> 8));
            int index = checked((int)(pointer & 0xFFu));
            ReadOnlySpan<byte> page = bytes.AsSpan(checked(number * format.PageSize), format.PageSize);
            Assert.Equal(Constants.PageTypes.Data, page[0]);
            Assert.Equal("4C56414C", Convert.ToHexString(page.Slice(4, 4)));
            int count = BinaryPrimitives.ReadUInt16LittleEndian(page[format.DataPage.NumRows..]);
            Assert.InRange(index, 0, count - 1);
            int position = format.DataPage.RowsStart + (index * sizeof(ushort));
            ushort slot = BinaryPrimitives.ReadUInt16LittleEndian(page[position..]);
            Assert.Equal(0, slot & Constants.DataPage.NonLiveRowFlags);
            int start = slot & Constants.DataPage.RowOffsetMask;
            Assert.InRange(start, format.DataPage.RowsStart + (count * sizeof(ushort)), format.PageSize - 1);
            int end = format.PageSize;
            for (int neighbor = 0; neighbor < count; neighbor++)
            {
                int candidate = BinaryPrimitives.ReadUInt16LittleEndian(page[(format.DataPage.RowsStart + (neighbor * sizeof(ushort)))..]) & Constants.DataPage.RowOffsetMask;
                if (candidate > start && candidate < end)
                {
                    end = candidate;
                }
            }

            ReadOnlySpan<byte> payload = page[start..end];
            string snapshot = $"{Convert.ToHexString(page.Slice(8, 4))}|{slot}|{Convert.ToHexString(payload)}";
            if (rows.TryGetValue(pointer, out string? previous))
            {
                Assert.Equal(previous, snapshot);
            }
            else
            {
                rows.Add(pointer, snapshot);
            }

            int prefix = mode == 0 ? sizeof(uint) : 0;
            Assert.True(payload.Length > prefix);
            remaining -= Math.Min(remaining, payload.Length - prefix);
            pointer = prefix == 0 ? 0 : BinaryPrimitives.ReadUInt32LittleEndian(payload);
        }

        Assert.Equal(0u, pointer);
    }

    private static async Task<NamedTableSnapshot> ReadNamedTableAsync(string path, string name)
    {
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(path, cancellationToken: Ct);
        using DataTable table = await reader.ReadTableAsync(name, cancellationToken: Ct);
        Dictionary<int, string>? queryNames = null;
        if (name == "MSysQueries")
        {
            using DataTable catalog = await reader.ReadTableAsync("MSysObjects", cancellationToken: Ct);
            queryNames = catalog.AsEnumerable().Where(row => (short)row["Type"] == 5)
                .ToDictionary(row => (int)row["Id"], row => (string)row["Name"]);
        }

        DataColumn[] columns = table.Columns.Cast<DataColumn>().OrderBy(column => column.ColumnName, StringComparer.Ordinal).ToArray();
        string[] schema = columns.Select(column => Hex(column.ColumnName) + ":" + column.DataType.FullName).ToArray();

        // Native compact may reorder physical descriptors and renumber query
        // catalog identities. Compare each exact named value, resolving only
        // the query definition's catalog foreign key to its stable query name.
        string[] rows = table.AsEnumerable()
            .Select(row => string.Join("|", columns.Select(column => Hex(column.ColumnName) + "="
                + (queryNames is not null && column.ColumnName == "ObjectId"
                    ? "query:" + Hex(queryNames[(int)row[column]])
                    : EncodeValue(row[column])))))
            .Order(StringComparer.Ordinal).ToArray();
        return new(schema, rows);
    }

    private static string EncodeRow(DataRow row) => string.Join("|", row.ItemArray.Select(EncodeValue));

    private static string EncodeValue(object? value) => value switch
    {
        null or DBNull => "null",
        byte[] bytes => "bytes:" + Convert.ToHexString(bytes),
        DateTime date => "date:" + date.ToBinary().ToString(CultureInfo.InvariantCulture),
        _ => value.GetType().Name + ":" + Hex(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty),
    };

    private static string Hex(string text) => Convert.ToHexString(Encoding.UTF8.GetBytes(text));

    private static string Literal(string path) => AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(path);

    private sealed record NamedTableSnapshot(string[] Columns, string[] Rows);

    private sealed record PermissionStorage(SortedDictionary<string, string> Rows, SortedDictionary<long, string> Pages, int SharedPages);

    private sealed record Snapshot(
        string[] Catalog,
        SortedDictionary<string, string[]> Tables,
        SortedDictionary<long, byte[]> Pages,
        string[] Permissions,
        SortedDictionary<string, string> PermissionRows,
        SortedDictionary<long, string> PermissionPages,
        int SharedPermissionPages,
        string[] LongValueDescriptors,
        SortedDictionary<uint, string> LongValueRows);
}
