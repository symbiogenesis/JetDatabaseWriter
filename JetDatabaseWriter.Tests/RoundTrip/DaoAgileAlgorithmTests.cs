namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Encryption;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Microsoft DAO acceptance checks for synthetic full-file Agile vectors; these files are not native producer oracles.</summary>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class DaoAgileAlgorithmTests
{
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData("SHA256", "SHA384", 192, 256, "ChainingModeCBC")]
    [InlineData("SHA384", "SHA512", 128, 192, "ChainingModeCFB")]
    public async Task SyntheticAgileMutation_DaoReadsWritesAndCompacts(string passwordHash, string dataHash, int passwordBits, int dataBits, string chaining)
    {
        await using var session = AccessRoundTripSession.CreateEmpty();
        byte[] variant = await NativeAgileAlgorithmTests.CreateVariantAsync(passwordHash, dataHash, passwordBits, dataBits, chaining);
        await File.WriteAllBytesAsync(session.SourcePath, variant, TestContext.Current.CancellationToken);
        var options = new AccessWriterOptions("vector") { UseLockFile = false };
        await using (AccessWriter writer = await AccessWriter.OpenAsync(session.SourcePath, options, TestContext.Current.CancellationToken))
        {
            await writer.InsertRowAsync("T", [8, "Library inserted"], TestContext.Current.CancellationToken);
            Assert.Equal(1, await writer.UpdateRowsAsync("T", "Id", 8, new Dictionary<string, object?> { ["Label"] = "Library updated" }, TestContext.Current.CancellationToken));
            await writer.InsertRowAsync("T", [9, "To delete"], TestContext.Current.CancellationToken);
            Assert.Equal(1, await writer.DeleteRowsAsync("T", "Id", 9, TestContext.Current.CancellationToken));
            await writer.CreateTableAsync("Added", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Added", [1], TestContext.Current.CancellationToken);
            await using (JetTransaction transaction = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken))
            {
                await writer.InsertRowAsync("Added", [2], TestContext.Current.CancellationToken);
                await transaction.RollbackAsync(TestContext.Current.CancellationToken);
            }

            await writer.CreateTableAsync("Maintenance", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Payload", typeof(byte[]))], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Maintenance", [1, new byte[32_000]], TestContext.Current.CancellationToken);
            long populatedLength = new FileInfo(session.SourcePath).Length;
            await writer.DropTableAsync("Maintenance", TestContext.Current.CancellationToken);
            Assert.True(await writer.ScrubFreePagesAsync(TestContext.Current.CancellationToken) > 0);
            Assert.True(await writer.ShrinkDatabaseAsync(TestContext.Current.CancellationToken) > 0);
            Assert.True(new FileInfo(session.SourcePath).Length < populatedLength);
        }

        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string destination = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        string script = $$"""
            $db = $engine.OpenDatabase({{source}}, $false, $false, ';PWD=vector')
            try {
                $rs = $db.OpenRecordset('SELECT Count(*) AS N FROM [T]')
                try { Write-Output "ROWS=$($rs.Fields('N').Value)" } finally { $rs.Close() }
                $rs = $db.OpenRecordset('SELECT Count(*) AS N FROM [Added]')
                try { Write-Output "ADDED=$($rs.Fields('N').Value)" } finally { $rs.Close() }
                $db.Execute("UPDATE [T] SET [Label]='DAO updated' WHERE [Id]=8", 128)
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}}, {{destination}}, ';LANGID=0x0409;CP=1252;COUNTRY=0;PWD=vector', 0, ';PWD=vector')
            """;
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0, $"DAO failed: {result.StdOut}\n{result.StdErr}");
        Assert.Contains("ROWS=2", result.StdOut, StringComparison.Ordinal);
        Assert.Contains("ADDED=1", result.StdOut, StringComparison.Ordinal);
        byte[] nativeWritten = await File.ReadAllBytesAsync(session.SourcePath, TestContext.Current.CancellationToken);
        int descriptorLength = BinaryPrimitives.ReadUInt16LittleEndian(variant.AsSpan(Constants.AgileEncryption.FlatEncryptionInfoLengthOffset));
        Assert.Equal(variant.AsSpan(Constants.AgileEncryption.FlatEncryptionInfoOffset, descriptorLength).ToArray(), nativeWritten.AsSpan(Constants.AgileEncryption.FlatEncryptionInfoOffset, descriptorLength).ToArray());
        byte[] compacted = await File.ReadAllBytesAsync(session.CompactedPath, TestContext.Current.CancellationToken);
        AssertCompactedProvider(compacted, passwordHash, dataHash, passwordBits, dataBits, chaining);

        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions("vector") { UseLockFile = false }, TestContext.Current.CancellationToken);
        using DataTable rows = await reader.ReadTableAsync("T", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Rows.Count);
        DataRow original = Assert.Single(rows.Select("Id = 7"));
        Assert.Equal("Native encrypted row", original["Label"]);
        DataRow updated = Assert.Single(rows.Select("Id = 8"));
        Assert.Equal("DAO updated", updated["Label"]);
        using DataTable added = await reader.ReadTableAsync("Added", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(1, Assert.Single(added.Select())["Id"]);
    }

    private static void AssertCompactedProvider(byte[] compacted, string passwordHash, string dataHash, int passwordBits, int dataBits, string chaining)
    {
        int length = BinaryPrimitives.ReadUInt16LittleEndian(compacted.AsSpan(Constants.AgileEncryption.FlatEncryptionInfoLengthOffset));
        var descriptor = XElement.Parse(Encoding.UTF8.GetString(compacted, Constants.AgileEncryption.FlatEncryptionInfoOffset + 8, length - 8));
        XElement keyData = Assert.Single(descriptor.Elements(XName.Get("keyData", "http://schemas.microsoft.com/office/2006/encryption")));
        XElement encryptedKey = Assert.Single(descriptor.Descendants(XName.Get("encryptedKey", "http://schemas.microsoft.com/office/2006/keyEncryptor/password")));
        Assert.Equal(dataBits.ToString(CultureInfo.InvariantCulture), (string?)keyData.Attribute("keyBits"));
        Assert.Equal(dataHash, (string?)keyData.Attribute("hashAlgorithm"));
        Assert.Equal(chaining, (string?)keyData.Attribute("cipherChaining"));
        Assert.Equal(passwordBits.ToString(CultureInfo.InvariantCulture), (string?)encryptedKey.Attribute("keyBits"));
        Assert.Equal(passwordHash, (string?)encryptedKey.Attribute("hashAlgorithm"));
        Assert.Equal(chaining, (string?)encryptedKey.Attribute("cipherChaining"));
        Assert.Equal("7", (string?)encryptedKey.Attribute("spinCount"));
    }
}
