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
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.Transactions;
using Xunit;

/// <summary>
/// Tests for the encryption probe that runs when a database is opened.
/// Opening a reader or a writer must look only at page 0 to decide whether
/// the file is Access-native flat Agile; the whole file is read only once
/// flat Agile is actually detected. The writer cannot edit flat Agile in
/// place, so it must refuse such a file before writing anything.
/// </summary>
/// <param name="db">The database input.</param>
public sealed class EncryptionOpenProbeTests(DatabaseCache db) : IClassFixture<DatabaseCache>, IDisposable
{
    private const string Password = "Probe1!Pa$";

    /// <summary>Source of a writer-created Jet3 .mdb.</summary>
    private const string Jet3 = "Jet3";

    /// <summary>Source of a writer-created Jet4 .mdb.</summary>
    private const string Jet4 = "Jet4";

    /// <summary>Source of a writer-created ACE .accdb.</summary>
    private const string Ace = "Ace";

    /// <summary>Source of the Access-authored AdventureWorks Jet4 .mdb fixture.</summary>
    private const string AdventureWorks = "AdventureWorks";

    /// <summary>Source of the Access-authored Northwind .accdb fixture.</summary>
    private const string Northwind = "Northwind";

    /// <summary>
    /// Upper bound for the bytes an open may read: the header, the page-0
    /// probe and the JET magic check together stay within a few pages.
    /// </summary>
    private const long MaxOpenBytes = 4L * Constants.PageSizes.Jet4;

    /// <summary>Lower bound for the test databases, so a whole-file read is far above <see cref="MaxOpenBytes"/>.</summary>
    private const long MinDatabaseBytes = 64L * Constants.PageSizes.Jet4;

    private static readonly AccessWriterOptions NoLockOptions = new() { UseLockFile = false };

    private readonly List<string> tempFiles = [];

    /// <summary>Gets each password format the writer can open, by path and by stream.</summary>
    /// <returns>The format and by-path pairs.</returns>
    public static TheoryData<AccessEncryptionFormat, bool> PasswordFormatsByPathAndStream()
    {
        var data = new TheoryData<AccessEncryptionFormat, bool>();
        foreach (AccessEncryptionFormat format in new[]
        {
            AccessEncryptionFormat.AccdbLegacyPassword,
            AccessEncryptionFormat.AccdbAgileCfb,
            AccessEncryptionFormat.AccdbStandard,
        })
        {
            data.Add(format, true);
            data.Add(format, false);
        }

        return data;
    }

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

    // ───── Open reads only page 0 ────────────────────────────────────

    [Theory]
    [InlineData(Jet3, AccessEncryptionFormat.None)]
    [InlineData(Jet4, AccessEncryptionFormat.None)]
    [InlineData(Ace, AccessEncryptionFormat.None)]
    [InlineData(AdventureWorks, AccessEncryptionFormat.None)]
    [InlineData(Northwind, AccessEncryptionFormat.None)]
    [InlineData(Ace, AccessEncryptionFormat.AccdbLegacyPassword)]
    public async Task ReaderOpen_ReadsOnlyTheFirstPages(string source, AccessEncryptionFormat encryption)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream backing = await this.BuildDatabaseAsync(source, encryption, ct);
        Assert.True(backing.Length >= MinDatabaseBytes, $"Fixture is only {backing.Length} bytes.");

        await using var counting = new CountingStream(backing);
        await using (AccessReader reader = await AccessReader.OpenAsync(counting, ReaderOptions(encryption), leaveOpen: true, ct))
        {
            Assert.True(
                counting.BytesRead <= MaxOpenBytes,
                $"AccessReader.OpenAsync read {counting.BytesRead} bytes of a {backing.Length}-byte file.");

            Assert.NotEmpty(await reader.ListTablesAsync(ct));
        }
    }

    [Theory]
    [InlineData(Jet3, AccessEncryptionFormat.None)]
    [InlineData(Jet4, AccessEncryptionFormat.None)]
    [InlineData(Ace, AccessEncryptionFormat.None)]
    [InlineData(Ace, AccessEncryptionFormat.AccdbLegacyPassword)]
    public async Task WriterOpen_ReadsOnlyTheFirstPages(string source, AccessEncryptionFormat encryption)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream backing = await this.BuildDatabaseAsync(source, encryption, ct);

        // Page 0 is read once; the header and the flat-Agile probe share it.
        const long maxBytes = Constants.PageSizes.Jet4;
        await using var counting = new CountingStream(backing);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(counting, WriterOptions(encryption), leaveOpen: true, ct))
        {
            Assert.True(
                counting.BytesRead <= maxBytes,
                $"AccessWriter.OpenAsync read {counting.BytesRead} bytes of a {backing.Length}-byte file.");

            await writer.CreateTableAsync("Probe", [new ColumnDefinition("Id", typeof(int))], ct);
            await writer.InsertRowAsync("Probe", [1], ct);
        }

        backing.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(backing, ReaderOptions(encryption), leaveOpen: true, ct);
        Assert.Equal(1L, await CountRowsAsync(reader, "Probe", ct));
    }

    [Fact]
    public async Task ReaderOpen_FlatAgile_StillDecryptsTheWholeFile()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream backing = await BuildLargeDatabaseAsync(DatabaseFormat.AceAccdb, AccessEncryptionFormat.AccdbAgile, ct);

        await using AccessReader reader = await AccessReader.OpenAsync(backing, ReaderOptions(AccessEncryptionFormat.AccdbAgile), leaveOpen: true, ct);
        Assert.Equal(3L, await CountRowsAsync(reader, ct));
    }

    // ───── Flat-Agile detection works from page 0 ────────────────────

    [Fact]
    public async Task IsFlatAgileEncrypted_RecognisesPageZeroAlone()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream backing = await BuildLargeDatabaseAsync(DatabaseFormat.AceAccdb, AccessEncryptionFormat.AccdbAgile, ct);
        byte[] file = backing.ToArray();
        byte[] pageZero = file.AsSpan(0, Constants.PageSizes.Jet4).ToArray();

        Assert.True(OfficeCryptoAgile.IsFlatAgileEncrypted(file));
        Assert.True(OfficeCryptoAgile.IsFlatAgileEncrypted(pageZero));
    }

    [Fact]
    public async Task IsFlatAgileEncrypted_DescriptorLengthPastPageZero_ReturnsFalse()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream backing = await BuildLargeDatabaseAsync(DatabaseFormat.AceAccdb, AccessEncryptionFormat.AccdbAgile, ct);
        byte[] file = backing.ToArray();

        // The descriptor must fit in page 0. A length that runs past the page
        // (but not past the file) is not a flat-Agile header, so detection
        // must say so instead of throwing from the page-0 copy.
        BinaryPrimitives.WriteUInt16LittleEndian(
            file.AsSpan(Constants.AgileEncryption.FlatEncryptionInfoLengthOffset, 2),
            Constants.PageSizes.Jet4);

        Assert.False(OfficeCryptoAgile.IsFlatAgileEncrypted(file));
    }

    // ───── Writer refuses flat Agile ─────────────────────────────────

    [Theory]
    [InlineData(Password)]
    [InlineData(null)]
    [InlineData("wrong-password")]
    public async Task WriterOpen_FlatAgilePath_ThrowsNotSupportedAndLeavesFileUnchanged(string? password)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = await this.CreateFlatAgileFileAsync(ct);
        byte[] before = await File.ReadAllBytesAsync(path, ct);
        string lockPath = LockFileSlotWriter.GetLockFilePath(path);

        var options = new AccessWriterOptions { Password = password.AsMemory() };
        NotSupportedException ex;
        using (FileStreamFactory.OpenTracker opens = FileStreamFactory.TrackOpens())
        {
            ex = await Assert.ThrowsAsync<NotSupportedException>(async () =>
            {
                await using AccessWriter writer = await AccessWriter.OpenAsync(path, options, ct);
            });

            Assert.Equal(1, CountOpens(opens, path));
        }

        Assert.Contains("Agile", ex.Message, StringComparison.Ordinal);
        Assert.Equal(before, await File.ReadAllBytesAsync(path, ct));
        Assert.False(File.Exists(lockPath), "The refused open left a lock file behind.");

        // The file is still a readable flat-Agile database.
        await using AccessReader reader = await AccessReader.OpenAsync(path, ReaderOptions(AccessEncryptionFormat.AccdbAgile), ct);
        Assert.Equal(3L, await CountRowsAsync(reader, ct));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task WriterOpen_FlatAgileStream_ThrowsNotSupportedAndLeavesStreamUnchanged(bool leaveOpen)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream source = await BuildLargeDatabaseAsync(DatabaseFormat.AceAccdb, AccessEncryptionFormat.AccdbAgile, ct);
        byte[] before = source.ToArray();

        var backing = new MemoryStream();
        await backing.WriteAsync(before.AsMemory(), ct);
        backing.Position = 0;
        var counting = new CountingStream(backing);

        NotSupportedException ex = await Assert.ThrowsAsync<NotSupportedException>(async () =>
        {
            await using AccessWriter writer = await AccessWriter.OpenAsync(counting, WriterOptions(AccessEncryptionFormat.AccdbAgile), leaveOpen, ct);
        });

        Assert.Contains("Agile", ex.Message, StringComparison.Ordinal);
        Assert.Equal(0L, counting.BytesWritten);
        Assert.True(
            counting.BytesRead <= MaxOpenBytes,
            $"The refused open read {counting.BytesRead} bytes of a {before.Length}-byte file.");
        Assert.Equal(before, backing.ToArray());
        Assert.Equal(!leaveOpen, counting.IsDisposed);

        await counting.DisposeAsync();
        await backing.DisposeAsync();
    }

    [Fact]
    public async Task WriterOpen_FlatAgileStream_RefusesBeforeDecryptingWithWrongPassword()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream backing = await BuildLargeDatabaseAsync(DatabaseFormat.AceAccdb, AccessEncryptionFormat.AccdbAgile, ct);
        byte[] before = backing.ToArray();

        var options = new AccessWriterOptions { UseLockFile = false, Password = "wrong-password".AsMemory() };
        _ = await Assert.ThrowsAsync<NotSupportedException>(async () =>
        {
            await using AccessWriter writer = await AccessWriter.OpenAsync(backing, options, leaveOpen: true, ct);
        });

        Assert.Equal(before, backing.ToArray());
    }

    // ───── The path overload opens the file once ─────────────────────

    [Theory]
    [InlineData(Jet3, AccessEncryptionFormat.None)]
    [InlineData(Jet4, AccessEncryptionFormat.None)]
    [InlineData(Ace, AccessEncryptionFormat.None)]
    [InlineData(Ace, AccessEncryptionFormat.AccdbLegacyPassword)]
    [InlineData(Ace, AccessEncryptionFormat.AccdbAgileCfb)]
    [InlineData(Ace, AccessEncryptionFormat.AccdbStandard)]
    public async Task WriterOpen_PathOverload_OpensDatabaseFileOnce(string source, AccessEncryptionFormat encryption)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = await this.WriteTempDatabaseAsync(source, encryption, ct);

        using (FileStreamFactory.OpenTracker opens = FileStreamFactory.TrackOpens())
        {
            await using AccessWriter writer = await AccessWriter.OpenAsync(path, WriterOptions(encryption), ct);
            Assert.Equal(1, CountOpens(opens, path));

            await writer.CreateTableAsync("Probe", [new ColumnDefinition("Id", typeof(int))], ct);
            await writer.InsertRowAsync("Probe", [1], ct);
        }

        await using AccessReader reader = await AccessReader.OpenAsync(path, ReaderOptions(encryption), ct);
        Assert.Equal(1L, await CountRowsAsync(reader, "Probe", ct));
    }

    [Theory]
    [InlineData(AccessEncryptionFormat.AccdbLegacyPassword)]
    [InlineData(AccessEncryptionFormat.AccdbAgileCfb)]
    public async Task WriterOpen_PathOverload_WrongPassword_LeavesFileAndLockUnchanged(AccessEncryptionFormat encryption)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = await this.WriteTempDatabaseAsync(encryption == AccessEncryptionFormat.Jet4Rc4 ? Jet4 : Ace, encryption, ct);
        byte[] before = await File.ReadAllBytesAsync(path, ct);

        var options = new AccessWriterOptions { Password = "wrong-password".AsMemory() };
        UnauthorizedAccessException ex = await Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
        {
            await using AccessWriter writer = await AccessWriter.OpenAsync(path, options, ct);
        });

        Assert.Contains("password", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(before, await File.ReadAllBytesAsync(path, ct));
        Assert.False(File.Exists(LockFileSlotWriter.GetLockFilePath(path)), "The refused open left a lock file behind.");
    }

    // ───── Password errors name the caller's options type ────────────

    [Theory]
    [MemberData(nameof(PasswordFormatsByPathAndStream))]
    public async Task WriterOpen_MissingPassword_NamesWriterOptions(AccessEncryptionFormat encryption, bool byPath)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = await this.WriteTempDatabaseAsync(encryption == AccessEncryptionFormat.Jet4Rc4 ? Jet4 : Ace, encryption, ct);

        UnauthorizedAccessException ex = await Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
        {
            if (byPath)
            {
                await using AccessWriter writer = await AccessWriter.OpenAsync(path, NoLockOptions, ct);
            }
            else
            {
                await using var stream = new MemoryStream();
                await stream.WriteAsync(await File.ReadAllBytesAsync(path, ct), ct);
                stream.Position = 0;
                await using AccessWriter writer = await AccessWriter.OpenAsync(stream, NoLockOptions, leaveOpen: true, ct);
            }
        });

        Assert.Contains("AccessWriterOptions.Password", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("AccessReaderOptions", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [MemberData(nameof(PasswordFormatsByPathAndStream))]
    [InlineData(AccessEncryptionFormat.AccdbAgile, true)]
    [InlineData(AccessEncryptionFormat.AccdbAgile, false)]
    public async Task ReaderOpen_MissingPassword_NamesReaderOptions(AccessEncryptionFormat encryption, bool byPath)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = await this.WriteTempDatabaseAsync(encryption == AccessEncryptionFormat.Jet4Rc4 ? Jet4 : Ace, encryption, ct);
        var options = new AccessReaderOptions { UseLockFile = false };

        UnauthorizedAccessException ex = await Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
        {
            if (byPath)
            {
                await using AccessReader reader = await AccessReader.OpenAsync(path, options, ct);
            }
            else
            {
                await using var stream = new MemoryStream(await File.ReadAllBytesAsync(path, ct), writable: false);
                await using AccessReader reader = await AccessReader.OpenAsync(stream, options, leaveOpen: true, ct);
            }
        });

        Assert.Contains("AccessReaderOptions.Password", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("AccessWriterOptions", ex.Message, StringComparison.Ordinal);
    }

    // ───── Helpers ───────────────────────────────────────────────────

    private static int CountOpens(FileStreamFactory.OpenTracker opens, string path) =>
        opens.OpenedPaths.Count(p => string.Equals(Path.GetFullPath(p), Path.GetFullPath(path), StringComparison.OrdinalIgnoreCase));

    private static AccessReaderOptions ReaderOptions(AccessEncryptionFormat encryption) => new()
    {
        UseLockFile = false,
        Password = encryption == AccessEncryptionFormat.None ? ReadOnlyMemory<char>.Empty : Password.AsMemory(),
    };

    private static AccessWriterOptions WriterOptions(AccessEncryptionFormat encryption) => new()
    {
        UseLockFile = false,
        Password = encryption == AccessEncryptionFormat.None ? ReadOnlyMemory<char>.Empty : Password.AsMemory(),
    };

    /// <summary>
    /// Builds a database of well over <see cref="MinDatabaseBytes"/> (three rows,
    /// one carrying a large MEMO), optionally encrypted in place.
    /// </summary>
    /// <param name="format">The database format to create.</param>
    /// <param name="encryption">The encryption to apply, or <see cref="AccessEncryptionFormat.None"/>.</param>
    /// <param name="ct">A token used to cancel the operation.</param>
    private static async Task<MemoryStream> BuildLargeDatabaseAsync(
        DatabaseFormat format,
        AccessEncryptionFormat encryption,
        CancellationToken ct)
    {
        var backing = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(backing, format, NoLockOptions, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Body", typeof(string))],
                ct);
            await writer.InsertRowAsync("T", [1, new string('x', 400_000)], ct);
            await writer.InsertRowAsync("T", [2, "small"], ct);
            await writer.InsertRowAsync("T", [3, "small too"], ct);
        }

        return await EncryptInPlaceAsync(backing, encryption, ct);
    }

    private static async Task<MemoryStream> EncryptInPlaceAsync(MemoryStream backing, AccessEncryptionFormat encryption, CancellationToken ct)
    {
        if (encryption != AccessEncryptionFormat.None)
        {
            backing.Position = 0;
            await AccessWriter.EncryptAsync(backing, Password.AsMemory(), encryption, ct);
            Assert.Equal(encryption, await AccessWriter.DetectEncryptionFormatAsync(backing, ct));
        }

        backing.Position = 0;
        return backing;
    }

    private static Task<long> CountRowsAsync(AccessReader reader, CancellationToken ct) => CountRowsAsync(reader, "T", ct);

    private static async Task<long> CountRowsAsync(AccessReader reader, string table, CancellationToken ct)
    {
        long count = 0;
        await foreach (object[] row in reader.Rows(table, cancellationToken: ct))
        {
            _ = row;
            count++;
        }

        return count;
    }

    /// <summary>
    /// Copies an Access-authored fixture, or builds a writer-created database,
    /// then optionally encrypts it in place.
    /// </summary>
    /// <param name="source">One of the source constants (<see cref="Jet3"/>, <see cref="AdventureWorks"/>, ...).</param>
    /// <param name="encryption">The encryption to apply, or <see cref="AccessEncryptionFormat.None"/>.</param>
    /// <param name="ct">A token used to cancel the operation.</param>
    private async Task<MemoryStream> BuildDatabaseAsync(string source, AccessEncryptionFormat encryption, CancellationToken ct)
    {
        string? fixture = source switch
        {
            AdventureWorks => TestDatabases.AdventureWorks,
            Northwind => TestDatabases.NorthwindTraders,
            _ => null,
        };

        if (fixture is null)
        {
            DatabaseFormat format = source switch
            {
                Jet3 => DatabaseFormat.Jet3Mdb,
                Jet4 => DatabaseFormat.Jet4Mdb,
                _ => DatabaseFormat.AceAccdb,
            };

            return await BuildLargeDatabaseAsync(format, encryption, ct);
        }

        byte[] bytes = await db.GetFileAsync(fixture, ct);
        var backing = new MemoryStream();
        await backing.WriteAsync(bytes.AsMemory(), ct);
        return await EncryptInPlaceAsync(backing, encryption, ct);
    }

    private async Task<string> WriteTempDatabaseAsync(string source, AccessEncryptionFormat encryption, CancellationToken ct)
    {
        await using MemoryStream database = await this.BuildDatabaseAsync(source, encryption, ct);
        string path = Path.Combine(Path.GetTempPath(), $"jdwprobe_{Guid.NewGuid():N}{(source is Jet3 or Jet4 or AdventureWorks ? ".mdb" : ".accdb")}");
        this.tempFiles.Add(path);
        this.tempFiles.Add(LockFileSlotWriter.GetLockFilePath(path));
        await File.WriteAllBytesAsync(path, database.ToArray(), ct);
        return path;
    }

    private async Task<string> CreateFlatAgileFileAsync(CancellationToken ct)
    {
        await using MemoryStream source = await BuildLargeDatabaseAsync(DatabaseFormat.AceAccdb, AccessEncryptionFormat.AccdbAgile, ct);
        string path = Path.Combine(Path.GetTempPath(), $"jdwprobe_{Guid.NewGuid():N}.accdb");
        this.tempFiles.Add(path);
        this.tempFiles.Add(LockFileSlotWriter.GetLockFilePath(path));
        await File.WriteAllBytesAsync(path, source.ToArray(), ct);
        return path;
    }
}
