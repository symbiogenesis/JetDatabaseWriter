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
using JetDatabaseWriter.Models;
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
        Snapshot original = await ReadSnapshotAsync(control);
        Snapshot compacted = await ReadSnapshotAsync(session.CompactedPath);
        foreach (string name in new[] { "MSysAccessStorage", "MSysQueries", "f_C54F32B8B3CB4285928325593DE12252_Data" })
        {
            Assert.Equal(original.Tables[name], compacted.Tables[name]);
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

        // LVAL pages use their own header rather than this owner field. Their complete
        // payloads are covered by the table values above, while this comparison pins
        // the selected TDEF/data/index pages, including unused bytes and row framing.
        byte[] bytes = await File.ReadAllBytesAsync(path, Ct);
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

        return new(identities, tables, pages);
    }

    private static string EncodeRow(DataRow row) => string.Join("|", row.ItemArray.Select(value => value switch
    {
        null or DBNull => "null",
        byte[] bytes => "bytes:" + Convert.ToHexString(bytes),
        DateTime date => "date:" + date.ToBinary().ToString(CultureInfo.InvariantCulture),
        _ => value.GetType().Name + ":" + Hex(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty),
    }));

    private static string Hex(string text) => Convert.ToHexString(Encoding.UTF8.GetBytes(text));

    private static string Literal(string path) => AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(path);

    private sealed record Snapshot(string[] Catalog, SortedDictionary<string, string[]> Tables, SortedDictionary<long, byte[]> Pages);
}
