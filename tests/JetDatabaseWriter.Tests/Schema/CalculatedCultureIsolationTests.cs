namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>A writer retains its own regional settings throughout calculated evaluation.</summary>
public sealed class CalculatedCultureIsolationTests
{
    [Fact]
    public async Task ExplicitCulture_IsClonedBeforeCallerMutatesIt()
    {
        var culture = new CultureInfo("de-DE", useUserOverride: false);
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(stream, culture))
        {
            culture.NumberFormat.NumberDecimalSeparator = ":";
            culture.DateTimeFormat.MonthNames = Enumerable.Repeat("Changed", 13).ToArray();
            await writer.InsertRowAsync("T", [1.5d, null, null, DbDefault.Value], TestContext.Current.CancellationToken);
        }

        Assert.Equal<object>([1.5d, "1,5", "März", 1.5d], await ReadRowAsync(stream));
    }

    [Fact]
    public async Task DefaultCulture_IsCapturedBeforeLaterCurrentCultureChanges()
    {
        CultureInfo previous = CultureInfo.CurrentCulture;
        await using var stream = new MemoryStream();
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo("fr-FR", useUserOverride: false);
            await using AccessWriter writer = await CreateWriterAsync(stream, null);
            CultureInfo.CurrentCulture = new CultureInfo("en-US", useUserOverride: false);
            await writer.InsertRowAsync("T", [1.5d, null, null, DbDefault.Value], TestContext.Current.CancellationToken);
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }

        Assert.Equal<object>([1.5d, "1,5", "mars", 1.5d], await ReadRowAsync(stream));
    }

    [Fact]
    public async Task DefaultInvariantCulture_RetainsItsDateProfile()
    {
        CultureInfo previous = CultureInfo.CurrentCulture;
        await using var stream = new MemoryStream();
        var date = new DateTime(2020, 3, 4, 13, 45, 0);
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
            await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(
                "T",
                [
                    new("D", typeof(DateTime)),
                    new("Text", typeof(string)) { IsCalculated = true, CalculationExpression = "[D]&\"\"" },
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [date, null], TestContext.Current.CancellationToken);
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }

        Assert.Equal<object>([date, "03/04/2020 13:45:00"], await ReadRowAsync(stream));
    }

    [Fact]
    public async Task GregorianYearWindow_IsCapturedWithRegionalSettings()
    {
        var culture = new CultureInfo("en-US", useUserOverride: false);
        culture.DateTimeFormat.Calendar.TwoDigitYearMax = 2029;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false, CalculatedExpressionCulture = culture }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            culture.DateTimeFormat.Calendar.TwoDigitYearMax = 2099;
            await writer.CreateTableAsync(
                "T",
                [
                    new("Year", typeof(int)),
                    new("CalculatedYear", typeof(int)) { IsCalculated = true, CalculationExpression = "Year(DateSerial([Year],1,1))" },
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [30, null], TestContext.Current.CancellationToken);
        }

        Assert.Equal<object>([30, 1930], await ReadRowAsync(stream));
    }

    [Fact]
    public async Task ConcurrentWriters_KeepIndependentRegionalAndSharedDefaultResults()
    {
        Task<object[]> english = CalculateAsync("en-US");
        Task<object[]> german = CalculateAsync("de-DE");
        Task<object[]> french = CalculateAsync("fr-FR");
        object[][] results = await Task.WhenAll(english, german, french);
        Assert.Equal<object>([1.5d, "1.5", "March", 1.5d], results[0]);
        Assert.Equal<object>([1.5d, "1,5", "März", 1.5d], results[1]);
        Assert.Equal<object>([1.5d, "1,5", "mars", 1.5d], results[2]);
    }

    private static async Task<object[]> CalculateAsync(string culture)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(stream, new CultureInfo(culture, useUserOverride: false)))
        {
            await Task.Yield();
            await writer.InsertRowAsync("T", [1.5d, null, null, DbDefault.Value], TestContext.Current.CancellationToken);
        }

        return await ReadRowAsync(stream);
    }

    private static async Task<AccessWriter> CreateWriterAsync(MemoryStream stream, CultureInfo? culture)
    {
        AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false, CalculatedExpressionCulture = culture }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        try
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new("N", typeof(double)),
                    new("NumberText", typeof(string)) { IsCalculated = true, CalculationExpression = "[N]&\"\"" },
                    new("MonthName", typeof(string)) { IsCalculated = true, CalculationExpression = "MonthName(3)" },
                    new("SharedDefault", typeof(double)) { DefaultValueExpression = "CDbl(\"1.5\")" },
                ],
                TestContext.Current.CancellationToken);
            return writer;
        }
        catch
        {
            await writer.DisposeAsync();
            throw;
        }
    }

    private static async Task<object[]> ReadRowAsync(MemoryStream stream)
    {
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        return Assert.Single(await reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken));
    }
}
