namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Rejects corrupt physical-index counts before they can rebase column descriptors.</summary>
public sealed class TDefCountCorruptionTests
{
    [Theory]
    [InlineData(-1, true)]
    [InlineData(-1, false)]
    [InlineData(0, true)]
    [InlineData(0, false)]
    [InlineData(1, true)]
    [InlineData(1, false)]
    [InlineData(1000, true)]
    [InlineData(1000, false)]
    [InlineData(int.MaxValue, true)]
    [InlineData(int.MaxValue, false)]
    [InlineData(1001, true)]
    [InlineData(1001, false)]
    public async Task Relationships_InvalidPhysicalIndexCount_RefusesDefinitionWithPageContext(int count, bool strictParsing)
    {
        byte[] bytes = await File.ReadAllBytesAsync(TestDatabases.ComplexDataTestV2010, TestContext.Current.CancellationToken);
        const int page = 5;
        const int countOffset = (page * 4096) + 51;
        Assert.Equal(Constants.PageTypes.TableDefinition, bytes[page * 4096]);
        Assert.Equal(3, BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(countOffset, sizeof(int))));
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(countOffset, sizeof(int)), count);
        await using var stream = new MemoryStream(bytes, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false, StrictParsing = strictParsing },
            cancellationToken: TestContext.Current.CancellationToken);

        JetCorruptDataException exception = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await reader.ListRelationshipsAsync(TestContext.Current.CancellationToken));

        Assert.Equal(JetErrorCode.CorruptTableDefinition, exception.ErrorCode);
        Assert.Equal(page, exception.ErrorInfo.PageNumber);
    }

    [Fact]
    public async Task Relationships_NativePhysicalIndexCount_RemainsReadable()
    {
        byte[] bytes = await File.ReadAllBytesAsync(TestDatabases.ComplexDataTestV2010, TestContext.Current.CancellationToken);
        await using var stream = new MemoryStream(bytes, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, cancellationToken: TestContext.Current.CancellationToken);

        Assert.NotEmpty(await reader.ListRelationshipsAsync(TestContext.Current.CancellationToken));
    }
}
