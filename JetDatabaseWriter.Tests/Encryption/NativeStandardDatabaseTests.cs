namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Buffers.Binary;
using System.Data;
using System.IO;
using System.Security.Cryptography;
using System.Threading.Tasks;
using JetDatabaseWriter.CompoundFile;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Native-engine acceptance of a synthetic flat Standard provider database.</summary>
public sealed class NativeStandardDatabaseTests
{
    /// <summary>DAO authenticates, updates and compacts a flat 50,000-iteration Standard database.</summary>
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task StandardProvider_DaoReadsWritesAndCompacts()
    {
        await using var session = AccessRoundTripSession.CreateEmpty();
        byte[] bytes = await CreateStandardDatabaseAsync();
        await File.WriteAllBytesAsync(session.SourcePath, bytes, TestContext.Current.CancellationToken);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(session.SourcePath, new AccessWriterOptions("Password1234_") { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            await writer.InsertRowAsync("T", [8, "Library Standard row"], TestContext.Current.CancellationToken);
        }

        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        string script = $$"""
            $db = $engine.OpenDatabase({{source}}, $false, $false, ';PWD=Password1234_')
            try {
                $rs = $db.OpenRecordset('SELECT Count(*) AS N FROM [T]')
                try { Write-Output "ROWS=$($rs.Fields('N').Value)" } finally { $rs.Close() }
                $db.Execute("UPDATE [T] SET [Label]='DAO Standard row' WHERE [Id]=8", 128)
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}}, {{compacted}}, ';LANGID=0x0409;CP=1252;COUNTRY=0;PWD=Password1234_', 0, ';PWD=Password1234_')
            """;
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0, $"DAO Standard failed: {result.StdOut}\n{result.StdErr}");
        Assert.Contains("ROWS=2", result.StdOut, StringComparison.Ordinal);
        byte[] nativeOutput = await File.ReadAllBytesAsync(session.CompactedPath, TestContext.Current.CancellationToken);
        Assert.Equal(0x24u, BinaryPrimitives.ReadUInt32LittleEndian(nativeOutput.AsSpan(0x29F)));
        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions("Password1234_") { UseLockFile = false }, TestContext.Current.CancellationToken);
        using DataTable rows = await reader.ReadTableAsync("T", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Rows.Count);
        Assert.Equal("Native encrypted row", Assert.Single(rows.Select("Id = 7"))["Label"]);
        Assert.Equal("DAO Standard row", Assert.Single(rows.Select("Id = 8"))["Label"]);
    }

    /// <summary>Microsoft-compacted Standard pages retain the 50,000-iteration provider on updates.</summary>
    [Fact]
    public async Task NativeStandardFixture_ReadsAndUpdatesOriginalProvider()
    {
        byte[] original = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeAceStandard.accdb"), TestContext.Current.CancellationToken);
        Assert.Equal("349745691382E10D8F5E5946378FAFD2BA20FF33B616082E65742F45B288E1BA", Convert.ToHexString(SHA256.HashData(original)));
        Assert.Equal(0x24u, BinaryPrimitives.ReadUInt32LittleEndian(original.AsSpan(0x29F)));
        await using var stream = new MemoryStream();
        await stream.WriteAsync(original, TestContext.Current.CancellationToken);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions("Password1234_") { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.InsertRowAsync("T", [9, "Retained Standard provider"], TestContext.Current.CancellationToken);
        }

        Assert.Equal(original.AsSpan(0, 4096).ToArray(), stream.ToArray().AsSpan(0, 4096).ToArray());
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions("Password1234_") { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        using DataTable rows = await reader.ReadTableAsync("T", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(3, rows.Rows.Count);
        Assert.Equal("Native encrypted row", Assert.Single(rows.Select("Id = 7"))["Label"]);
        Assert.Equal("DAO Standard row", Assert.Single(rows.Select("Id = 8"))["Label"]);
        Assert.Equal("Retained Standard provider", Assert.Single(rows.Select("Id = 9"))["Label"]);
    }

    /// <summary>The native Standard fixture rejects a wrong password and an inadequate spin budget.</summary>
    [Fact]
    public async Task NativeStandardFixture_AuthenticatesAndEnforcesSpinBudget()
    {
        byte[] original = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeAceStandard.accdb"), TestContext.Current.CancellationToken);
        byte[] header = original.AsSpan(0, 4096).ToArray();
        Assert.Throws<UnauthorizedAccessException>(() => NativeStandardPageCodec.Open(header, "wrong", 50_000, 4096, cancellationToken: TestContext.Current.CancellationToken));
        Assert.Throws<JetDatabaseWriter.Exceptions.JetLimitationException>(() => NativeStandardPageCodec.Open(header, "Password1234_", 49_999, 4096, cancellationToken: TestContext.Current.CancellationToken));
    }

    private static async Task<byte[]> CreateStandardDatabaseAsync()
    {
        byte[] database = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeAceAgile.accdb"), TestContext.Current.CancellationToken);
        byte[] originalHeader = database.AsSpan(0, 4096).ToArray();
        using IPageCodec originalCodec = PageCodecFactory.Open(originalHeader, DatabaseFormat.AceAccdb, "Native123".AsMemory(), "password", cancellationToken: TestContext.Current.CancellationToken);
        await using FileStream package = File.OpenRead(Path.Combine(TestDatabases.EncryptedRoot, "Upstream-OfficeStandard.docx"));
        System.Collections.Generic.Dictionary<string, byte[]> streams = await CompoundFileReader.ReadStreamsAsync(package, TestContext.Current.CancellationToken);
        byte[] descriptor = streams["EncryptionInfo"];
        byte[] header = (byte[])originalHeader.Clone();
        BinaryPrimitives.WriteUInt16LittleEndian(header.AsSpan(0x299), checked((ushort)descriptor.Length));
        descriptor.CopyTo(header, 0x29B);
        using var codec = NativeStandardPageCodec.Open(header, "Password1234_", 50_000, 4096, cancellationToken: TestContext.Current.CancellationToken);
        header.CopyTo(database, 0);
        for (int page = 1; page < database.Length / 4096; page++)
        {
            originalCodec.Decode(database, page * 4096, page, 4096);
            codec.Encode(database, page * 4096, page, 4096);
        }

        return database;
    }
}
