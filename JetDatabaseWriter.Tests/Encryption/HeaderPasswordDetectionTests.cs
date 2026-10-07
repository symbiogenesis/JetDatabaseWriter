namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// <para>
/// Tests that an unencrypted database is never taken for an encrypted one.
/// The library's Jet4 RC4 and ACCDB legacy password schemes mark a file
/// with a value in raw header byte <c>0x62</c>. That byte lies in the header
/// password area, which Access masks with a fixed RC4 keystream; on a file
/// without a password the area holds the creation date's whole days
/// repeated, so the raw byte is a creation-date byte, not a flag.
/// </para>
/// <para>
/// Detection and the open path therefore treat the byte as a flag only when
/// the unmasked password area holds a password. Writer-created Jet4 files
/// (creation day 46163, raw byte <c>0x67</c>), about half of the
/// Access-authored Jet4 fixtures, and any file created on a day whose raw
/// byte reads <c>0x01</c>–<c>0x03</c> (Jet4) or <c>0x07</c> (ACCDB) were
/// misreported or could not be opened at all.
/// </para>
/// </summary>
public sealed class HeaderPasswordDetectionTests : IDisposable
{
    private const string FirstPassword = "Native123";
    private const string TableName = "T";

    private static readonly AccessWriterOptions NoLockOptions = new() { UseLockFile = false };

    private readonly List<string> tempFiles = [];

    /// <summary>
    /// Gets every <c>.mdb</c> and <c>.accdb</c> fixture under <c>Databases/</c>, as a path relative to it,
    /// except the library-encrypted ones under <c>Databases/Encrypted/</c>.
    /// </summary>
    public static TheoryData<string> AccessAuthoredFixtures
    {
        get
        {
            string root = Path.Combine(AppContext.BaseDirectory, "Databases");
            var data = new TheoryData<string>();
            if (!Directory.Exists(root))
            {
                return data;
            }

            foreach (string path in Directory.EnumerateFiles(root, "*", SearchOption.AllDirectories)
                .Where(static p => (p.EndsWith(".mdb", StringComparison.OrdinalIgnoreCase) || p.EndsWith(".accdb", StringComparison.OrdinalIgnoreCase))
                    && !p.StartsWith(TestDatabases.EncryptedRoot + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase))
                .OrderBy(static p => p, StringComparer.Ordinal))
            {
                data.Add(Path.GetRelativePath(root, path));
            }

            return data;
        }
    }

    /// <summary>Gets each header-password format with each write mode.</summary>
    public static TheoryData<DatabaseFormat, AccessEncryptionFormat, WriteMode> HeaderPasswordFormatsAndModes
    {
        get
        {
            var data = new TheoryData<DatabaseFormat, AccessEncryptionFormat, WriteMode>();
            foreach (WriteMode mode in new[] { WriteMode.Direct, WriteMode.AutoCommit, WriteMode.ExplicitCommit })
            {
                data.Add(DatabaseFormat.AceAccdb, AccessEncryptionFormat.AccdbAgile, mode);
            }

            return data;
        }
    }

    [Theory]
    [MemberData(nameof(HeaderPasswordFormatsAndModes))]
    public async Task NativeEncryptedDatabase_InsertRow_RoundTrips(DatabaseFormat format, AccessEncryptionFormat encryption, WriteMode mode)
    {
        Assert.Equal(DatabaseFormat.AceAccdb, format);
        byte[] fixture = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeAceAgile.accdb"), Ct);
        string path = this.WriteTemp(fixture, ".accdb");
        var options = new AccessWriterOptions(FirstPassword) { UseLockFile = false, UseTransactionalWrites = mode == WriteMode.AutoCommit };
        await using (AccessWriter writer = await AccessWriter.OpenAsync(path, options, Ct))
        {
            if (mode == WriteMode.ExplicitCommit)
            {
                await using JetTransaction transaction = await writer.BeginTransactionAsync(Ct);
                await writer.InsertRowAsync(TableName, [8, "two"], Ct);
                await transaction.CommitAsync(Ct);
            }
            else
            {
                await writer.InsertRowAsync(TableName, [8, "two"], Ct);
            }
        }

        Assert.Equal(encryption, await AccessWriter.DetectEncryptionFormatAsync(path, Ct));
        await AssertRefusedAsync(path, password: null);
        Assert.Equal(["7|Native encrypted row", "8|two"], await ReadRowsAsync(path, FirstPassword));
    }
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public void Dispose()
    {
        foreach (string path in this.tempFiles)
        {
            try
            {
                File.Delete(path);
            }
            catch (IOException)
            {
                // best-effort cleanup
            }
        }
    }

    // ───── Writer-created databases ──────────────────────────────────

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DetectEncryptionFormat_WriterCreatedDatabase_ReturnsNone(DatabaseFormat format)
    {
        string path = await this.CreateDatabaseWithRowAsync(format);

        Assert.Equal(AccessEncryptionFormat.None, await AccessWriter.DetectEncryptionFormatAsync(path, Ct));

        await using var stream = new MemoryStream(await File.ReadAllBytesAsync(path, Ct));
        Assert.Equal(AccessEncryptionFormat.None, await AccessWriter.DetectEncryptionFormatAsync(stream, Ct));
    }

    [Theory]
    [MemberData(nameof(AccessAuthoredFixtures))]
    public async Task DetectEncryptionFormat_AccessAuthoredFixture_ReturnsNone(string relativePath)
    {
        // No fixture carries a password: every header password area holds
        // Access's empty-password pattern. The 22 Jackcess Jet4 fixtures with
        // bit 0x02 set in raw byte 0x62, ComplexFields.accdb and
        // AesEncrypted.accdb (raw 0x07) were reported as encrypted.
        string path = Path.Combine(AppContext.BaseDirectory, "Databases", relativePath);

        Assert.Equal(AccessEncryptionFormat.None, await AccessWriter.DetectEncryptionFormatAsync(path, Ct));
    }

    [Theory]
    [InlineData(nameof(TestDatabases.AdoxJet4), 46133, (byte)0x01)]
    [InlineData(nameof(TestDatabases.AdoxJet4), 46134, (byte)0x02)]
    [InlineData(nameof(TestDatabases.AdoxJet4), 46135, (byte)0x03)]
    [InlineData(nameof(TestDatabases.AdventureWorks), 46133, (byte)0x01)]
    [InlineData(nameof(TestDatabases.AdventureWorks), 46134, (byte)0x02)]
    [InlineData(nameof(TestDatabases.AdventureWorks), 46135, (byte)0x03)]
    public async Task UnencryptedJet4_CreationDayLooksLikeFlag_OpensWithoutPassword(string fixture, int creationDay, byte rawFlagByte)
    {
        // Days 46133..46135 (0xB435..0xB437) make raw byte 0x62 read 0x01,
        // 0x02 and 0x03: values the open path took for a password flag.
        string source = FixturePath(fixture);
        IReadOnlyList<string> tables = await ListTablesAsync(source);
        byte[] bytes = await File.ReadAllBytesAsync(source, Ct);
        SetCreationDay(bytes, creationDay);
        Assert.Equal(rawFlagByte, bytes[0x62]);
        string path = this.WriteTemp(bytes, ".mdb");

        await AssertUnencryptedAndOpensAsync(path, tables);

        byte[] original = await File.ReadAllBytesAsync(path, Ct);
        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await AccessWriter.EncryptAsync(path, FirstPassword.AsMemory(), options: NoLockOptions, cancellationToken: Ct));
        Assert.Equal(original, await File.ReadAllBytesAsync(path, Ct));
    }

    [Theory]
    [InlineData(nameof(TestDatabases.TestV2010))]
    [InlineData(nameof(TestDatabases.TestV2007))]
    [InlineData("Writer")]
    public async Task UnencryptedAccdb_CreationDay46131_OpensWithoutPassword(string fixture)
    {
        // Day 46131 (0xB433) makes raw byte 0x62 read 0x07, the ACCDB legacy
        // password value. testV2010 is ACE version 3, where the open path
        // demanded a password; testV2007 and writer-created files are version 2.
        string source = fixture == "Writer"
            ? await this.CreateDatabaseWithRowAsync(DatabaseFormat.AceAccdb)
            : FixturePath(fixture);
        IReadOnlyList<string> tables = await ListTablesAsync(source);
        byte[] bytes = await File.ReadAllBytesAsync(source, Ct);
        SetCreationDay(bytes, 46131);
        Assert.Equal(0x07, bytes[0x62]);
        string path = this.WriteTemp(bytes, ".accdb");

        await AssertUnencryptedAndOpensAsync(path, tables);
    }

    // ───── HasHeaderPassword ─────────────────────────────────────────

    [Theory]
    [InlineData(nameof(TestDatabases.Jet3Test), DatabaseFormat.Jet3Mdb)]
    [InlineData(nameof(TestDatabases.TestV1997), DatabaseFormat.Jet3Mdb)]
    [InlineData(nameof(TestDatabases.AdventureWorks), DatabaseFormat.Jet4Mdb)]
    [InlineData(nameof(TestDatabases.AdoxJet4), DatabaseFormat.Jet4Mdb)]
    [InlineData(nameof(TestDatabases.NorthwindTraders), DatabaseFormat.AceAccdb)]
    [InlineData(nameof(TestDatabases.AesEncrypted), DatabaseFormat.AceAccdb)]
    public async Task HasHeaderPassword_AccessEmptyPasswordPattern_ReturnsFalse(string fixture, DatabaseFormat format)
    {
        byte[] header = await ReadHeaderAsync(FixturePath(fixture));

        Assert.Equal(format, JetFormat.DetectFormat(header));
        Assert.False(EncryptionManager.HasHeaderPassword(header, format));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void HasHeaderPassword_ZeroPasswordArea_ReturnsFalse(DatabaseFormat format)
    {
        byte[] header = UnmaskedEmptyHeader(format);
        header.AsSpan(0x42, 40).Clear();
        EncryptionManager.TransformHeaderMask(header);

        Assert.False(EncryptionManager.HasHeaderPassword(header, format));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void HasHeaderPassword_OtherPasswordAreaContent_ReturnsTrue(DatabaseFormat format)
    {
        byte[] header = UnmaskedEmptyHeader(format);
        header[0x42] ^= 0x41;
        EncryptionManager.TransformHeaderMask(header);

        Assert.True(EncryptionManager.HasHeaderPassword(header, format));
    }

    [Fact]
    public async Task HasHeaderPassword_NativeEncryptedDatabase_ReturnsTrue()
    {
        byte[] bytes = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeJet4Rc4.mdb"), Ct);
        Assert.True(EncryptionManager.HasHeaderPassword(bytes, DatabaseFormat.Jet4Mdb));
    }

    [Fact]
    public void HasHeaderPassword_CreationDateNotADayNumber_ReturnsTrueUnlessAreaIsZero()
    {
        byte[] unmasked = UnmaskedEmptyHeader(DatabaseFormat.Jet4Mdb);
        BinaryPrimitives.WriteInt64LittleEndian(unmasked.AsSpan(0x72), BitConverter.DoubleToInt64Bits(double.NaN));
        byte[] header = (byte[])unmasked.Clone();
        EncryptionManager.TransformHeaderMask(header);
        Assert.True(EncryptionManager.HasHeaderPassword(header, DatabaseFormat.Jet4Mdb));

        // With no day number there is no pattern to write, so the empty
        // password is written as zeros.
        EncryptionManager.WriteEmptyHeaderPassword(unmasked, DatabaseFormat.Jet4Mdb);
        Assert.True(unmasked.AsSpan(0x42, 40).IndexOfAnyExcept((byte)0) < 0);
        EncryptionManager.TransformHeaderMask(unmasked);
        Assert.False(EncryptionManager.HasHeaderPassword(unmasked, DatabaseFormat.Jet4Mdb));
    }

    [Fact]
    public void HasHeaderPassword_HeaderShorterThanCreationDate_ReturnsFalse()
    {
        byte[] header = UnmaskedEmptyHeader(DatabaseFormat.Jet4Mdb);
        header[0x42] ^= 0x41;
        EncryptionManager.TransformHeaderMask(header);

        Assert.False(EncryptionManager.HasHeaderPassword(header.AsSpan(0, 0x79).ToArray(), DatabaseFormat.Jet4Mdb));
    }

    [Fact]
    public void WriteEmptyHeaderPassword_WritesCreationDayPattern()
    {
        byte[] unmasked = UnmaskedEmptyHeader(DatabaseFormat.AceAccdb);
        BinaryPrimitives.WriteInt64LittleEndian(unmasked.AsSpan(0x72), BitConverter.DoubleToInt64Bits(46131.75));
        unmasked.AsSpan(0x42, 40).Fill(0xAA);

        EncryptionManager.WriteEmptyHeaderPassword(unmasked, DatabaseFormat.AceAccdb);

        for (int i = 0; i < 40; i += 4)
        {
            Assert.Equal(new byte[] { 0x33, 0xB4, 0x00, 0x00 }, unmasked.AsSpan(0x42 + i, 4).ToArray());
        }

        EncryptionManager.TransformHeaderMask(unmasked);
        Assert.Equal(0x07, unmasked[0x62]);
        Assert.False(EncryptionManager.HasHeaderPassword(unmasked, DatabaseFormat.AceAccdb));
    }

    // ───── Helpers ───────────────────────────────────────────────────

    /// <summary>Returns the unmasked header page of a new writer-created database of <paramref name="format"/>.</summary>
    /// <param name="format">The database format.</param>
    private static byte[] UnmaskedEmptyHeader(DatabaseFormat format)
    {
        byte[] header = TDefPageBuilder.BuildEmptyDatabase(format, fullCatalogSchema: true).AsSpan(0, Constants.PageSizes.Jet3).ToArray();
        EncryptionManager.TransformHeaderMask(header);
        return header;
    }

    private static async Task<byte[]> ReadHeaderAsync(string path)
    {
        byte[] bytes = await File.ReadAllBytesAsync(path, Ct);
        return bytes.AsSpan(0, Constants.PageSizes.Jet3).ToArray();
    }

    private static string FixturePath(string fixture) =>
        (string)typeof(TestDatabases).GetField(fixture)!.GetValue(null)!;

    /// <summary>
    /// Rewrites the creation date of an unencrypted Jet4 / ACE file to
    /// <paramref name="creationDay"/> (keeping the time of day) and writes the
    /// matching empty-password pattern, as Access would for a file created
    /// that day. Both live in the masked header region.
    /// </summary>
    /// <param name="file">The whole database file.</param>
    /// <param name="creationDay">The whole-day part of the new creation date.</param>
    private static void SetCreationDay(byte[] file, int creationDay)
    {
        byte[] header = file.AsSpan(0, 0x98).ToArray();
        EncryptionManager.TransformHeaderMask(header);

        double old = BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(header.AsSpan(0x72)));
        double date = creationDay + (old - Math.Floor(old));
        BinaryPrimitives.WriteInt64LittleEndian(header.AsSpan(0x72), BitConverter.DoubleToInt64Bits(date));

        Span<byte> pattern = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(pattern, creationDay);
        for (int i = 0; i < 40; i++)
        {
            header[0x42 + i] = pattern[i % 4];
        }

        EncryptionManager.TransformHeaderMask(header);
        header.CopyTo(file, 0);
    }

    private static async Task AssertUnencryptedAndOpensAsync(string path, IReadOnlyList<string> expectedTables)
    {
        Assert.Equal(AccessEncryptionFormat.None, await AccessWriter.DetectEncryptionFormatAsync(path, Ct));
        Assert.Equal(expectedTables, await ListTablesAsync(path));

        await using (AccessWriter writer = await AccessWriter.OpenAsync(path, NoLockOptions, Ct))
        {
            Assert.NotNull(writer);
        }

        await using MemoryStream stream = await CopyToStreamAsync(path);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, NoLockOptions, leaveOpen: true, Ct))
        {
            Assert.NotNull(writer);
        }
    }

    private static async Task AssertRefusedAsync(string path, string? password)
    {
        var options = new AccessReaderOptions { UseLockFile = false, Password = password.AsMemory() };
        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
        {
            await using AccessReader reader = await AccessReader.OpenAsync(path, options, Ct);
        });
    }

    private static async Task<IReadOnlyList<string>> ListTablesAsync(string path, string? password = null)
    {
        var options = new AccessReaderOptions { UseLockFile = false, Password = password.AsMemory() };
        await using AccessReader reader = await AccessReader.OpenAsync(path, options, Ct);
        return await reader.ListTablesAsync(Ct);
    }

    private static async Task<List<string>> ReadRowsAsync(string path, string? password)
    {
        var options = new AccessReaderOptions { UseLockFile = false, Password = password.AsMemory() };
        await using AccessReader reader = await AccessReader.OpenAsync(path, options, Ct);
        return await ReadRowsAsync(reader);
    }

    private static async Task<List<string>> ReadRowsAsync(AccessReader reader)
    {
        var rows = new List<string>();
        await foreach (object[] row in reader.Rows(TableName, cancellationToken: Ct))
        {
            rows.Add(string.Join("|", row));
        }

        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    private static async Task<MemoryStream> CopyToStreamAsync(string path)
    {
        var stream = new MemoryStream();
        await stream.WriteAsync(await File.ReadAllBytesAsync(path, Ct), Ct);
        stream.Position = 0;
        return stream;
    }

    private async Task<string> CreateDatabaseWithRowAsync(DatabaseFormat format)
    {
        string path = this.NewTempPath(format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(path, format, NoLockOptions, Ct))
        {
            await writer.CreateTableAsync(
                TableName,
                [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Name", typeof(string), maxLength: 50)],
                Ct);
            await writer.InsertRowAsync(TableName, [1, "one"], Ct);
        }

        return path;
    }

    private string WriteTemp(byte[] bytes, string extension)
    {
        string path = this.NewTempPath(extension);
        File.WriteAllBytes(path, bytes);
        return path;
    }

    private string NewTempPath(string extension)
    {
        string path = Path.Combine(Path.GetTempPath(), $"jdwhdrpwd_{Guid.NewGuid():N}{extension}");
        this.tempFiles.Add(path);
        return path;
    }
}
