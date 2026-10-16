namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Native Jet3 column descriptors and page-zero user slots survive writer mutations.</summary>
/// <param name="output">Captures independent native descriptor comparisons.</param>
public sealed class Jet3DescriptorPreservationTests(ITestOutputHelper output)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static string FixturePath => Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "Jet3", "NativeDescriptorMetadata.mdb");

    /// <summary>Gets every schema operation and write mode.</summary>
    public static TheoryData<string, WriteMode> Operations()
    {
        var cases = new TheoryData<string, WriteMode>();
        foreach (string operation in new[] { "add", "drop", "rename" })
        {
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                cases.Add(operation, mode);
            }
        }

        return cases;
    }

    [Fact]
    public async Task FrozenNativeDescriptors_EstablishTypeMetadataAndHeaderSlots()
    {
        byte[] fixture = await File.ReadAllBytesAsync(FixturePath, Ct);
        Assert.Equal("5D5396738DFCB7EC5BA12F0186F5691C0E845370C9C4BDB7EECBFED5EB9A265C", Convert.ToHexString(SHA256.HashData(fixture)));
        ColumnInfo[] columns = await ReadColumnsAsync(FixturePath, "Native");
        Assert.Equal<ColumnType>(
            [ColumnType.LongIntegerType, ColumnType.BooleanType, ColumnType.ByteType, ColumnType.IntegerType, ColumnType.MoneyType, ColumnType.FloatType, ColumnType.DoubleType, ColumnType.DateTimeType, ColumnType.TextType, ColumnType.MemoType, ColumnType.OleType, ColumnType.GuidType],
            columns.Select(column => column.Type));
        foreach (ColumnInfo column in columns)
        {
            Assert.Equal(new byte[] { 1, 0, 9, 4, 228, 4 }, MetadataBytes(column));
        }

        for (int offset = 0x601; offset < 0x800; offset += 2)
        {
            Assert.Equal(1, fixture[offset]);
        }
    }

    [Fact]
    public async Task NewDescriptors_MatchFrozenNativeTypeMetadata()
    {
        ColumnInfo[] native = await ReadColumnsAsync(FixturePath, "Native");
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("Writer", CreateColumns(), Ct);
        }

        ColumnInfo[] written = await ReadColumnsAsync(stream, "Writer");
        AssertMetadata(native, written);
    }

    [Fact]
    public void Bootstrap_UserSlotAvailabilityMatchesNativeJet3()
    {
        byte[] header = TDefPageBuilder.BuildEmptyDatabase(DatabaseFormat.Jet3Mdb);
        for (int offset = 0x601; offset < 0x800; offset += 2)
        {
            Assert.Equal(1, header[offset]);
        }
    }

    [Theory]
    [MemberData(nameof(Operations))]
    public async Task FrozenNative_RewriteAndDmlPreserveMetadataAndHeader(string operation, WriteMode mode)
    {
        byte[] fixture = await File.ReadAllBytesAsync(FixturePath, Ct);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(fixture, Ct);
        ColumnInfo[] original = await ReadColumnsAsync(stream, "Native");
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, Ct))
        {
            await RewriteAndMutateAsync(writer, operation, mode);
        }

        Assert.Equal(fixture.AsSpan(0x600, 0x200).ToArray(), stream.ToArray().AsSpan(0x600, 0x200).ToArray());
        ColumnInfo[] rewritten = await ReadColumnsAsync(stream, "Native");
        foreach (ColumnInfo expected in original)
        {
            if (operation == "drop" && expected.Name == "Small")
            {
                continue;
            }

            string name = operation == "rename" && expected.Name == "Label" ? "Caption" : expected.Name;
            ColumnInfo actual = Assert.Single(rewritten, column => column.Name == name);
            Assert.Equal(MetadataBytes(expected), MetadataBytes(actual));
        }

        stream.Position = 0;
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        Assert.Equal("Native", Assert.Single(await reader.ListTablesAsync(Ct)));
        using DataTable data = await reader.ReadTableAsync("Native", cancellationToken: Ct);
        Assert.Equal(3, data.Rows.Count);
        DataRow row = Assert.Single(data.Rows.Cast<DataRow>(), candidate => Equals(candidate["Id"], 1));
        Assert.Equal(1, row["Id"]);
        Assert.Equal(12.3456m, row["Amount"]);
        Assert.Equal("native text", row[operation == "rename" ? "Caption" : "Label"]);
        Assert.Equal("native memo", row["Notes"]);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task NewDescriptors_MatchDao36TypeMetadata()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.Jet3Descriptors", databaseExtension: ".mdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        PowerShellRunResult created = Jet3RoundTripEnvironment.RunEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{source}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', 32)
            try {
                $db.Execute('CREATE TABLE [Native] ([Id] LONG,[Flag] BIT,[Tiny] BYTE,[Small] SHORT,[Amount] CURRENCY,[Ratio] SINGLE,[Weight] DOUBLE,[Occurred] DATETIME,[Label] TEXT(40),[Notes] MEMO,[Payload] OLEOBJECT,[Identity] GUID)', 128)
            } finally { $db.Close() }
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(1));
        Assert.True(created.ExitCode == 0, $"DAO36 create failed: {created.StandardOutput}\n{created.StandardError}");
        ColumnInfo[] native = await ReadColumnsAsync(session.SourcePath, "Native");
        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            await writer.CreateTableAsync("Writer", CreateColumns(), Ct);
        }

        ColumnInfo[] written = await ReadColumnsAsync(session.SourcePath, "Writer");
        foreach (ColumnInfo column in native)
        {
            ColumnInfo actual = Assert.Single(written, candidate => candidate.Name == column.Name);
            output.WriteLine($"{column.Name} ({column.Type}): native={Convert.ToHexString(MetadataBytes(column))} writer={Convert.ToHexString(MetadataBytes(actual))}");
        }

        AssertMetadata(native, written);
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [MemberData(nameof(Operations))]
    public async Task FrozenNative_RewriteSurvivesDao36ReadWriteAndCompact(string operation, WriteMode mode)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.Jet3DescriptorRewrite", databaseExtension: ".mdb");
        await File.WriteAllBytesAsync(session.SourcePath, await File.ReadAllBytesAsync(FixturePath, Ct), Ct);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(session.SourcePath, WriteModes.WriterOptions(mode), Ct))
        {
            await RewriteAndMutateAsync(writer, operation, mode);
        }

        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        string label = operation == "rename" ? "Caption" : "Label";
        int columns = operation switch { "add" => 13, "drop" => 11, _ => 12 };
        string read = $$"""
            if ($db.TableDefs('Native').Fields.Count -ne {{columns}}) { throw 'Lost rewritten descriptors.' }
            $rs = $db.OpenRecordset('SELECT * FROM [Native] WHERE [Id]=1')
            try {
                if ($rs.Fields('Amount').Value -ne [decimal]12.3456 -or $rs.Fields('{{label}}').Value -ne 'native text' -or $rs.Fields('Notes').Value -ne 'native memo') { throw 'Lost surviving native values.' }
            } finally { $rs.Close() }
            """;
        PowerShellRunResult verified = Jet3RoundTripEnvironment.RunEngineScript(
            $$"""
            $db = $engine.OpenDatabase({{source}})
            try {
                {{read}}
                $db.Execute('INSERT INTO [Native] ([Id],[Amount]) VALUES (4,-12.3456)', 128)
                $db.Execute('UPDATE [Native] SET [Amount]=22.2222 WHERE [Id]=4', 128)
                $db.Execute('DELETE FROM [Native] WHERE [Id]=4', 128)
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}}, {{compacted}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', 32)
            $db = $engine.OpenDatabase({{compacted}}, $false, $true)
            try {
                {{read}}
            } finally { $db.Close() }
            Write-Output 'NATIVE_READ_WRITE_COMPACT_OK'
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(2));
        Assert.True(verified.ExitCode == 0, $"DAO36 rewrite failed: {verified.StandardOutput}\n{verified.StandardError}");
        Assert.Contains("NATIVE_READ_WRITE_COMPACT_OK", verified.StandardOutput, StringComparison.Ordinal);
    }

    private static byte[] MetadataBytes(ColumnInfo column) => column.RawDescriptor.Skip(7).Take(6).ToArray();

    private static void AssertMetadata(ColumnInfo[] native, ColumnInfo[] written)
    {
        Assert.Equal(native.Length, written.Length);
        foreach (ColumnInfo expected in native)
        {
            ColumnInfo actual = Assert.Single(written, column => column.Name == expected.Name);
            Assert.Equal(expected.Type, actual.Type);
            Assert.Equal(MetadataBytes(expected), MetadataBytes(actual));
        }
    }

    private static Task RewriteAndMutateAsync(AccessWriter writer, string operation, WriteMode mode)
        => WriteModes.RunAsync(
            writer,
            mode,
            async () =>
            {
                if (operation == "add")
                {
                    await writer.AddColumnAsync("Native", new ColumnDefinition("Added", typeof(int)), Ct);
                }
                else if (operation == "drop")
                {
                    await writer.DropColumnAsync("Native", "Small", Ct);
                }
                else
                {
                    await writer.RenameColumnAsync("Native", "Label", "Caption", Ct);
                }

                string label = operation == "rename" ? "Caption" : "Label";
                await writer.InsertRowAsync("Native", new RowValues { ["Id"] = 4, ["Amount"] = -12.3456m, [label] = "temporary" }, Ct);
                Assert.Equal(1, await writer.UpdateRowsAsync("Native", "Id", 4, new Dictionary<string, object?> { ["Amount"] = 22.2222m }, Ct));
                Assert.Equal(1, await writer.DeleteRowsAsync("Native", "Id", 4, Ct));
            },
            Ct);

    private static ColumnDefinition[] CreateColumns() =>
    [
        new("Id", typeof(int)),
        new("Flag", typeof(bool)),
        new("Tiny", typeof(byte)),
        new("Small", typeof(short)),
        new("Amount", typeof(decimal)) { IsCurrency = true },
        new("Ratio", typeof(float)),
        new("Weight", typeof(double)),
        new("Occurred", typeof(DateTime)),
        new("Label", typeof(string), 40),
        new("Notes", typeof(string)),
        new("Payload", typeof(byte[])),
        new("Identity", typeof(Guid)),
    ];

    private static async Task<ColumnInfo[]> ReadColumnsAsync(string path, string table)
    {
        await using FileStream stream = File.OpenRead(path);
        return await ReadColumnsAsync(stream, table);
    }

    private static async Task<ColumnInfo[]> ReadColumnsAsync(Stream stream, string table)
    {
        stream.Position = 0;
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        long root = Assert.IsType<JetDatabaseWriter.Catalog.Models.CatalogEntry>(await reader.GetCatalogEntryAsync(table, Ct)).TDefPage;
        return Assert.IsType<JetDatabaseWriter.Catalog.Models.TableDef>(await reader.ReadTableDefAsync(root, Ct)).Columns.ToArray();
    }
}
