namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Buffers.Binary;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.CompoundFile;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Independent flat ACE encryption oracles pinned to Jackcess Encrypt.</summary>
public sealed class NativeAceProviderTests
{
    /// <summary>Historical RC4 and SHA1 Agile providers decode independently authored rows.</summary>
    /// <param name="name">The fixture filename.</param>
    [Theory]
    [InlineData("Upstream-db2007-oldenc.accdb")]
    [InlineData("Upstream-db2007-enc.accdb")]
    public async Task HistoricalProvidersReadAndWrite(string name)
    {
        byte[] original = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, name), TestContext.Current.CancellationToken);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(original, TestContext.Current.CancellationToken);
        stream.Position = 0;
        await using (AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions("Test123") { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            DataTable rows = await reader.ReadTableAsync("Table1", cancellationToken: TestContext.Current.CancellationToken);
            Assert.Single(rows.Rows.Cast<DataRow>());
            Assert.Equal("foo", rows.Rows[0]["Field1"]);
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions("Test123") { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.InsertRowAsync("Table1", [2, "provider write"], TestContext.Current.CancellationToken);
        }

        Assert.Equal(original.AsSpan(0, 4096).ToArray(), stream.ToArray().AsSpan(0, 4096).ToArray());
        stream.Position = 0;
        await using AccessReader reopened = await AccessReader.OpenAsync(stream, new AccessReaderOptions("Test123") { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        DataTable result = await reopened.ReadTableAsync("Table1", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(2, result.Rows.Count);
    }

    /// <summary>A mismatched password is rejected during header authentication.</summary>
    /// <param name="name">The fixture filename.</param>
    [Theory]
    [InlineData("Upstream-db2007-oldenc.accdb")]
    [InlineData("Upstream-db2007-enc.accdb")]
    public async Task HistoricalProvidersRejectWrongPassword(string name)
        => await Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
        {
            await using AccessReader reader = await AccessReader.OpenAsync(Path.Combine(TestDatabases.EncryptedRoot, name), new AccessReaderOptions("wrong") { UseLockFile = false }, TestContext.Current.CancellationToken);
        });

    /// <summary>Canceling an expensive native password derivation interrupts its hashing loop.</summary>
    [Fact]
    public async Task CancellationInterruptsPasswordHashLoop()
    {
        byte[] database = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "Upstream-db2007-enc.accdb"), TestContext.Current.CancellationToken);
        byte[] header = database.AsSpan(0, 4096).ToArray();
        int length = BinaryPrimitives.ReadUInt16LittleEndian(header.AsSpan(0x299));
        string xml = Encoding.UTF8.GetString(header, 0x2A3, length - 8).Replace("spinCount=\"100000\"", "spinCount=\"10000000\"", StringComparison.Ordinal);
        byte[] changed = Encoding.UTF8.GetBytes(xml);
        changed.CopyTo(header, 0x2A3);
        BinaryPrimitives.WriteUInt16LittleEndian(header.AsSpan(0x299), checked((ushort)(changed.Length + 8)));
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        var timer = Stopwatch.StartNew();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Task.Run(
            () =>
            {
                using NativeAgilePageCodec codec = OfficeCryptoAgile.CreateFlatPageCodec(header, "Test123", 10_000_000, 4096, cancellationToken: cancellation.Token);
            },
            TestContext.Current.CancellationToken));
        Assert.True(timer.Elapsed < TimeSpan.FromSeconds(5), $"Cancellation took {timer.Elapsed}.");
    }

    /// <summary>The independent compatibility AES provider supports page updates.</summary>
    [Fact]
    public async Task CompatibilityAesProviderReadsAndWrites()
    {
        byte[] original = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "Upstream-db-nonstandard.accdb"), TestContext.Current.CancellationToken);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(original, TestContext.Current.CancellationToken);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions("password") { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.InsertRowAsync("Table_One", [null, "provider write"], TestContext.Current.CancellationToken);
        }

        Assert.Equal(original.AsSpan(0, 4096).ToArray(), stream.ToArray().AsSpan(0, 4096).ToArray());
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions("password") { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        DataTable rows = await reader.ReadTableAsync("Table_One", cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(rows.Rows.Count > 0);
    }

    /// <summary>Contradictory binary provider fields fail before password hashing.</summary>
    /// <param name="relativeOffset">The descriptor field offset.</param>
    /// <param name="value">The malformed field value.</param>
    [Theory]
    [InlineData(4, 36)]
    [InlineData(12, 36)]
    [InlineData(28, 129)]
    [InlineData(8, int.MaxValue)]
    public async Task BinaryProviderRejectsMalformedFields(int relativeOffset, int value)
    {
        byte[] bytes = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "Upstream-db2007-oldenc.accdb"), TestContext.Current.CancellationToken);
        byte[] header = bytes.AsSpan(0, 4096).ToArray();
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(0x29B + relativeOffset), value);
        Assert.Throws<InvalidDataException>(() => NativeStandardPageCodec.Open(header, "Test123", 1_000_000, 4096, cancellationToken: TestContext.Current.CancellationToken));
    }

    /// <summary>A package-authored verifier and published key validate native Standard derivation; the assembled header is a synthetic vector.</summary>
    [Fact]
    public async Task StandardProviderUsesIndependentPasswordVerifier()
    {
        await using FileStream stream = File.OpenRead(Path.Combine(TestDatabases.EncryptedRoot, "Upstream-OfficeStandard.docx"));
        System.Collections.Generic.Dictionary<string, byte[]> streams = await CompoundFileReader.ReadStreamsAsync(stream, TestContext.Current.CancellationToken);
        byte[] native = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeAceAgile.accdb"), TestContext.Current.CancellationToken);
        byte[] header = native.AsSpan(0, 4096).ToArray();
        BinaryPrimitives.WriteUInt16LittleEndian(header.AsSpan(0x299), checked((ushort)streams["EncryptionInfo"].Length));
        streams["EncryptionInfo"].CopyTo(header, 0x29B);
        byte[] unmasked = (byte[])header.Clone();
        EncryptionManager.TransformHeaderMask(unmasked);
        uint blockZeroPage = BinaryPrimitives.ReadUInt32LittleEndian(unmasked.AsSpan(Constants.DatabaseHeader.EncodingKey));
        byte[] plaintext = new byte[4096];
        plaintext.AsSpan().Fill(0xA5);
        using var aes = Aes.Create();
#pragma warning disable CA5358, RS0030 // Independent ECMA-376 Standard vector mandates ECB.
        aes.Mode = CipherMode.ECB;
#pragma warning restore CA5358, RS0030 // Independent ECMA-376 Standard vector mandates ECB.
        aes.Padding = PaddingMode.None;
        aes.Key = Convert.FromHexString("40B13A71F90B966E375408F2D181A1AA");
        using ICryptoTransform encryptor = aes.CreateEncryptor();
        byte[] cipher = encryptor.TransformFinalBlock(plaintext, 0, plaintext.Length);
        using var codec = NativeStandardPageCodec.Open(header, "Password1234_", 50_000, 4096, cancellationToken: TestContext.Current.CancellationToken);
        codec.Decode(cipher, 0, blockZeroPage, 4096);
        Assert.Equal(plaintext, cipher);
    }
}
