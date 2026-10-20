namespace JetDatabaseWriter.Tests;

using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.CompoundFile;
using JetDatabaseWriter.DelimitedText;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>Format failures are structured while real source failures retain their identity.</summary>
public sealed class StructuredFormatFailuresTests
{
    [Theory]
    [InlineData("Encrypt", 0)]
    [InlineData("Encrypt", 1024)]
    [InlineData("Decrypt", 128)]
    [InlineData("Decrypt", 1024)]
    [InlineData("ChangePassword", 128)]
    [InlineData("ChangePassword", 1024)]
    public async Task EncryptionMaintenance_TruncatedHeader_HasHeaderCodeAndPreservesSource(string operation, int length)
    {
        string directory = Path.Combine(Path.GetTempPath(), $"StructuredMaintenanceHeader_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        string path = Path.Combine(directory, "database.mdb");
        byte[] header = new byte[4096];
        header[1] = 1;
        header[0x14] = 1;
        byte[] original = header.AsSpan(0, length).ToArray();
        var options = new AccessWriterOptions { UseLockFile = false };
        try
        {
            await File.WriteAllBytesAsync(path, original, TestContext.Current.CancellationToken);
            JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            {
                if (operation == "Encrypt")
                {
                    await AccessDatabaseEncryption.EncryptAsync(path, "password".AsMemory(), options: options, cancellationToken: TestContext.Current.CancellationToken);
                }
                else if (operation == "Decrypt")
                {
                    await AccessDatabaseEncryption.DecryptAsync(path, "password".AsMemory(), options, TestContext.Current.CancellationToken);
                }
                else
                {
                    await AccessDatabaseEncryption.ChangePasswordAsync(path, "password".AsMemory(), "new password".AsMemory(), options, TestContext.Current.CancellationToken);
                }
            });
            Assert.Equal(JetErrorCode.CorruptDatabaseHeader, error.ErrorCode);
            Assert.Equal(0, error.ErrorInfo.PageNumber);
            Assert.Equal(original, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
            Assert.Equal([path], Directory.GetFiles(directory));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PublicOpen_InvalidSignature_HasHeaderCodeBeforeCodePage(bool writer)
    {
        await using var stream = new MemoryStream(new byte[4096]);
        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
        {
            if (writer)
            {
                await using AccessWriter opened = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
            }
            else
            {
                await using AccessReader opened = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
            }
        });
        Assert.Equal(JetErrorCode.CorruptDatabaseHeader, error.ErrorCode);
        Assert.Equal(0, error.ErrorInfo.PageNumber);
    }

    [Fact]
    public void EncryptionTarget_TruncatedHeader_HasHeaderCode()
    {
        JetCorruptDataException error = Assert.Throws<JetCorruptDataException>(() => EncryptionConverter.ResolveBestTargetFormat([]));
        Assert.Equal(JetErrorCode.CorruptDatabaseHeader, error.ErrorCode);
        Assert.Equal(0, error.ErrorInfo.PageNumber);
        Assert.Null(error.ErrorInfo.ObjectName);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task EncryptionHeaderRead_PropagatesActualSourceFailure(bool invalidData)
    {
        Exception expected = invalidData ? new InvalidDataException("Synthetic source failure") : new IOException("Synthetic source failure");
        await using var stream = new FailingReadStream(expected);
        Exception? actual = await Record.ExceptionAsync(async () => await EncryptionManager.ReadOpenHeaderPageAsync(stream, TestContext.Current.CancellationToken));
        Assert.Same(expected, actual);
    }

    [Fact]
    public async Task CompoundSignatureFailure_HasStructuredCode()
    {
        await using var stream = new MemoryStream(new byte[512]);
        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await CompoundFileReader.ReadStreamsAsync(stream, TestContext.Current.CancellationToken));
        Assert.Equal(JetErrorCode.CorruptCompoundFile, error.ErrorCode);
        Assert.Equal("CompoundFile", error.ErrorInfo.ObjectName);
    }

    [Fact]
    public void InvalidComplexCell_HasStructuredCode()
    {
        JetCorruptDataException error = Assert.Throws<JetCorruptDataException>(() => ComplexCellValue.ReadMultiValueItems([]));
        Assert.Equal(JetErrorCode.CorruptComplexColumn, error.ErrorCode);
    }

    [Fact]
    public void InvalidAgileDescriptor_HasStructuredCode()
    {
        JetCorruptDataException error = Assert.Throws<JetCorruptDataException>(() =>
            OfficeCryptoAgile.Decrypt([], [], "password"));
        Assert.Equal(JetErrorCode.CorruptEncryptionDescriptor, error.ErrorCode);
        Assert.Equal("EncryptionInfo", error.ErrorInfo.ObjectName);
    }

    [Theory]
    [InlineData("<")]
    [InlineData("<encryption xmlns=\"http://schemas.microsoft.com/office/2006/encryption\"><keyData saltSize=\"16\" blockSize=\"16\" keyBits=\"256\" hashSize=\"64\" saltValue=\"?\"/></encryption>")]
    public void MalformedAgileXml_HasStructuredCode(string xml)
    {
        byte[] descriptor = [4, 0, 4, 0, 0x40, 0, 0, 0, .. Encoding.UTF8.GetBytes(xml)];
        JetCorruptDataException error = Assert.Throws<JetCorruptDataException>(() =>
            OfficeCryptoAgile.Decrypt(descriptor, [], "password"));
        Assert.Equal(JetErrorCode.CorruptEncryptionDescriptor, error.ErrorCode);
        Assert.NotNull(error.InnerException);
    }

    [Fact]
    public async Task TruncatedCompoundHeader_HasStructuredCode()
    {
        await using var stream = new MemoryStream([1]);
        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await CompoundFileReader.ReadStreamsAsync(stream, TestContext.Current.CancellationToken));
        Assert.Equal(JetErrorCode.CorruptCompoundFile, error.ErrorCode);
    }

    [Fact]
    public async Task DelimitedTextBudget_HasStructuredCodeAndOption()
    {
        using var text = new StringReader("too long");
        using var reader = new DelimitedTextReader(text, new DelimitedTextFormat(false, ',', false), new DelimitedTextLimits(20, 3, 2, "RecordLimit", "FieldLimit", "ColumnLimit"));
        JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () =>
            await reader.ReadRecordAsync(TestContext.Current.CancellationToken));
        Assert.Equal(JetErrorCode.ValueTooLarge, error.ErrorCode);
        Assert.Equal("FieldLimit", error.ErrorInfo.ObjectName);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CompoundRead_PropagatesActualIoFailure(bool endOfStream)
    {
        IOException expected = endOfStream ? new EndOfStreamException("Synthetic physical EOF failure") : new IOException("Synthetic physical read failure");
        await using var stream = new FailingReadStream(expected);
        Exception? actual = await Record.ExceptionAsync(async () =>
            await CompoundFileReader.ReadStreamsAsync(stream, TestContext.Current.CancellationToken));
        Assert.Same(expected, actual);
    }

    [Theory]
    [InlineData("prepared")]
    [InlineData("renamed")]
    [InlineData("committed")]
    public async Task ReplacementCommit_PropagatesActualIoFailureAndRetainedPaths(string boundary)
    {
        string directory = Path.Combine(Path.GetTempPath(), $"StructuredFormatFailuresTests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        string path = Path.Combine(directory, "database.mdb");
        string temporary = Path.Combine(directory, "replacement.tmp");
        var expected = new IOException("Synthetic commit I/O failure");
        try
        {
            await File.WriteAllBytesAsync(path, [1, 2, 3], TestContext.Current.CancellationToken);
            await File.WriteAllBytesAsync(temporary, [4, 5, 6], TestContext.Current.CancellationToken);
            IOException actual = Assert.Throws<IOException>(() => EncryptionReplacementCommit.Commit(temporary, path, observed =>
            {
                if (observed == boundary)
                {
                    throw expected;
                }
            }));
            Assert.Same(expected, actual);
            string original = Assert.IsType<string>(actual.Data[EncryptionReplacementCommit.OriginalFileDataKey]);
            Assert.Equal([1, 2, 3], await File.ReadAllBytesAsync(original, TestContext.Current.CancellationToken));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private sealed class FailingReadStream(Exception error) : MemoryStream(new byte[512])
    {
        public override int Read(byte[] buffer, int offset, int count) => throw error;

        public override int Read(Span<byte> buffer) => throw error;

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) => throw error;

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => throw error;
    }
}
