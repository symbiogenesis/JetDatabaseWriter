namespace JetDatabaseWriter.Tests.Encryption;

using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.CompoundFile;
using JetDatabaseWriter.Exceptions;
using Xunit;

/// <summary>Office packages are refused as database inputs, while compound parsing remains bounded.</summary>
public sealed class CompoundEncryptionCorruptionTests
{
    [Fact]
    public async Task CompoundReader_RejectsTruncatedHeader()
    {
        byte[] bytes = new byte[512];
        Constants.CompoundFile.Signature.CopyTo(bytes);
        await using var stream = new MemoryStream(bytes);
        await StructuredFormatAssert.ThrowsAsync<JetCorruptDataException>(JetErrorCode.CorruptCompoundFile, () => CompoundFileReader.ReadStreamsAsync(stream, TestContext.Current.CancellationToken).AsTask());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PublicOpen_RefusesOfficePackageAsDatabase(bool writer)
    {
        byte[] bytes = CompoundFileWriter.Build([new KeyValuePair<string, byte[]>("Unexpected", [1, 2, 3])]);
        await using var stream = new MemoryStream(bytes);
        await StructuredFormatAssert.ThrowsAsync<JetNotSupportedException>(JetErrorCode.FeatureNotSupported, async () =>
        {
            if (writer)
            {
                await using AccessWriter database = await AccessWriter.OpenAsync(stream, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
            }
            else
            {
                await using AccessReader database = await AccessReader.OpenAsync(stream, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
            }
        });
        Assert.Equal(bytes, stream.ToArray());
    }
}
