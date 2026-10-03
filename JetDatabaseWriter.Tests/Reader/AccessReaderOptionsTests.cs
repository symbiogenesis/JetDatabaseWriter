namespace JetDatabaseWriter.Tests.Reader;

using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Pins the reader's page-read rule, <see cref="AccessReaderOptions.UsesPositionalPageReads"/>,
/// which <see cref="AccessReader.OpenAsync(string, AccessReaderOptions?, System.Threading.CancellationToken)"/>
/// and <see cref="ReaderHarness"/> share: positional <c>RandomAccess</c> reads
/// only for a file the reader opened by path, unless the mode is
/// <see cref="PageReadOptimizationMode.Disabled"/>, and never in the
/// netstandard2.1 build, which has no <c>RandomAccess</c>.
/// </summary>
public sealed class AccessReaderOptionsTests
{
    [Theory]
    [InlineData(PageReadOptimizationMode.Auto, true, true)]
    [InlineData(PageReadOptimizationMode.Enabled, true, true)]
    [InlineData(PageReadOptimizationMode.Disabled, true, false)]
    [InlineData(PageReadOptimizationMode.Auto, false, false)]
    [InlineData(PageReadOptimizationMode.Enabled, false, false)]
    [InlineData(PageReadOptimizationMode.Disabled, false, false)]
    public void UsesPositionalPageReads_PerMode(PageReadOptimizationMode mode, bool openedFromPath, bool expectedOnNet)
    {
        var options = new AccessReaderOptions { PageReadOptimizationMode = mode };

        Assert.Equal(expectedOnNet && !LibraryTarget.IsNetStandard, options.UsesPositionalPageReads(openedFromPath));
    }
}
