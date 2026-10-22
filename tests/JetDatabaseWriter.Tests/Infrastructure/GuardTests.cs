namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Infrastructure;
using Xunit;

/// <summary>
/// Pins the exception contract of <see cref="Guard"/>. The library forks Guard on
/// <c>NETSTANDARD2_1</c>; the standard-asset lane runs these against that branch and
/// the native-asset lanes against the BCL throw helpers, with the same exception types.
/// </summary>
public sealed class GuardTests
{
    [Fact]
    public void NotNullOrEmpty_Null_ThrowsArgumentNullException()
    {
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => Guard.NotNullOrEmpty(null, "name"));
        Assert.Equal("name", ex.ParamName);
    }

    [Fact]
    public void NotNullOrEmpty_Empty_ThrowsArgumentException()
    {
        ArgumentException ex = Assert.Throws<ArgumentException>(() => Guard.NotNullOrEmpty(string.Empty, "name"));
        Assert.Equal("name", ex.ParamName);
    }

    [Theory]
    [InlineData(" ")]
    [InlineData("\t")]
    [InlineData("Orders")]
    public void NotNullOrEmpty_WhitespaceOrText_DoesNotThrow(string value)
    {
        Exception? ex = Record.Exception(() => Guard.NotNullOrEmpty(value, "name"));
        Assert.Null(ex);
    }

    [Fact]
    public void NotNull_Null_ThrowsArgumentNullException()
    {
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => Guard.NotNull<object>(null, "value"));
        Assert.Equal("value", ex.ParamName);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Positive_ZeroOrNegative_ThrowsArgumentOutOfRangeException(int value)
    {
        ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(() => Guard.Positive(value, "count"));
        Assert.Equal("count", ex.ParamName);
    }

    [Fact]
    public void ThrowIfDisposed_Instance_NamesRuntimeType()
    {
        ObjectDisposedException ex = Assert.Throws<ObjectDisposedException>(() => Guard.ThrowIfDisposed(disposed: true, new Uri("https://example.org/")));
        Assert.Equal(typeof(Uri).FullName, ex.ObjectName);
    }

    [Fact]
    public void ThrowIfDisposed_OwnerType_NamesOwnerType()
    {
        ObjectDisposedException ex = Assert.Throws<ObjectDisposedException>(() => Guard.ThrowIfDisposed(disposed: true, typeof(AccessWriter)));
        Assert.Equal(typeof(AccessWriter).FullName, ex.ObjectName);
    }

    [Theory]
    [InlineData("   ")]
    [InlineData("\t")]
    [InlineData("\r\n")]
    public async Task OpenAsync_WhitespacePath_ThrowsFileNotFoundException(string path)
    {
        // A whitespace-only path passes the null-or-empty guard on every target and is
        // then reported as a missing file, not as an invalid argument.
        await Assert.ThrowsAsync<FileNotFoundException>(async () =>
            await AccessWriter.OpenAsync(path, cancellationToken: TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<FileNotFoundException>(async () =>
            await AccessReader.OpenAsync(path, cancellationToken: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task OpenAsync_InvalidPathWithoutRecoveryEvidence_RetainsMissingFileContract()
    {
        const string path = "invalid\0database.accdb";
        FileNotFoundException writer = await Assert.ThrowsAsync<FileNotFoundException>(async () =>
            await AccessWriter.OpenAsync(path, cancellationToken: TestContext.Current.CancellationToken));
        FileNotFoundException reader = await Assert.ThrowsAsync<FileNotFoundException>(async () =>
            await AccessReader.OpenAsync(path, cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal(path, writer.FileName);
        Assert.Equal(path, reader.FileName);
    }
}
