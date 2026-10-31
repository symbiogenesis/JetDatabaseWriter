namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>Appending calculations populates existing rows without replacing surviving caches.</summary>
public sealed class CalculatedColumnAddTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("native-add.accdb", "60E0C0781C8B3309EE7449F860547C95AD9A71F13B34706DAFCEA8B810C5C48C")]
    [InlineData("native-add-compacted.accdb", "6F7EDFD5EC052D9B79BD33E191F676E10593F8674E98A298F475267CF5DE9438")]
    [InlineData("native-add-extended.accdb", "F235D6FA08494C91B1CC54DEAF4B22E215CE2D439EF397547EB4D8D48E00E892")]
    [InlineData("native-add-extended-compacted.accdb", "404EAAB9B16D295F706AAFCF6FD15E9F5552024FC1CDD8F29442ADAA31C68268")]
    [InlineData("native-add-rule.accdb", "EF49A9B43AEE953291CB43C73A68BD772F67CC049A2C8C9AE62126384D7C62A4")]
    [InlineData("native-stale-dependent.accdb", "78A3973E94792FFF53B4D0B7D82B7950E93DCB192EFE83432E2FF431EE4EE3D4")]
    [InlineData("native-stale-independent.accdb", "DE3EC2DE49D1D45BB50F8ED4213F831113267AA124D1D00213A4569BB06414A7")]
    [InlineData("native-stale-plain.accdb", "84E8692EA06BC78967E6BA208FE6FDBD64B529530270A71C3056203FC74B98AF")]
    public async Task NativeSpecimen_RetainsReviewedHash(string fixture, string hash)
    {
        byte[] bytes = await File.ReadAllBytesAsync(Path.Combine(AppContext.BaseDirectory, "Databases", "CalculatedAdd", fixture), Ct);
        Assert.Equal(hash, Convert.ToHexString(SHA256.HashData(bytes)));
    }

    [Fact]
    public async Task AddCalculatedColumn_TableRuleRefusalPreservesBytesAndCallerTransaction()
    {
        byte[] original = await File.ReadAllBytesAsync(Path.Combine(AppContext.BaseDirectory, "Databases", "CalculatedAdd", "native-add-rule.accdb"), Ct);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(original, Ct);
        stream.Position = 0;
        await using AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        await using JetTransaction transaction = await writer.BeginTransactionAsync(Ct);
        byte[] before = stream.ToArray();
        await Assert.ThrowsAsync<JetValidationRuleException>(async () => await writer.AddColumnAsync("T", new("Added", typeof(int)) { IsCalculated = true, CalculationExpression = "[N]*2" }, Ct));
        Assert.Equal(before, stream.ToArray());
        await writer.UpdateRowsAsync("T", RowCriteria.Where("Id", 1), new RowValues { ["N"] = 20 }, Ct);
        await writer.UpdateRowsAsync("T", RowCriteria.Where("Id", 2), new RowValues { ["N"] = 30 }, Ct);
        await writer.AddColumnAsync("T", new("Added", typeof(int)) { IsCalculated = true, CalculationExpression = "[N]*2" }, Ct);
        await transaction.RollbackAsync(Ct);
        Assert.Equal(original, stream.ToArray());
    }

    [Fact]
    public async Task AddCalculatedColumn_UsesWriterRegionalSnapshot()
    {
        await using var stream = new MemoryStream();
        var culture = (CultureInfo)CultureInfo.GetCultureInfo("fr-FR").Clone();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false, CalculatedExpressionCulture = culture }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync("T", [new("N", typeof(double))], Ct);
            await writer.InsertRowAsync("T", [1.5d], Ct);
            culture.NumberFormat.NumberDecimalSeparator = ":";
            await writer.AddColumnAsync("T", new("Added", typeof(string), 243) { IsCalculated = true, CalculationExpression = "[N]&\"\"" }, Ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        Assert.Equal("1,5", Assert.Single(await reader.Rows("T", cancellationToken: Ct).ToArrayAsync(Ct))[1]);
    }

    [Theory]
    [InlineData("plain")]
    [InlineData("independent")]
    [InlineData("dependent")]
    public async Task AddColumn_PreservesStaleCachesAndUsesNativeNewFieldBinding(string mode)
    {
        string originalPath = Path.Combine(AppContext.BaseDirectory, "Databases", "CalculatedStale", "native-before.accdb");
        byte[] original = await File.ReadAllBytesAsync(originalPath, Ct);
        object[][] preserved;
        await using (AccessReader source = await AccessReader.OpenAsync(originalPath, new AccessReaderOptions { UseLockFile = false }, Ct))
        {
            preserved = (await source.Rows("Renamed", cancellationToken: Ct).ToArrayAsync(Ct)).OrderBy(static row => row[0]).ToArray();
        }

        object[][] expected;
        await using (AccessReader native = await AccessReader.OpenAsync(Path.Combine(AppContext.BaseDirectory, "Databases", "CalculatedAdd", "native-stale-" + mode + ".accdb"), new AccessReaderOptions { UseLockFile = false }, Ct))
        {
            expected = (await native.Rows("Renamed", cancellationToken: Ct).ToArrayAsync(Ct)).OrderBy(static row => row[0]).ToArray();
        }

        await using var stream = new MemoryStream();
        await stream.WriteAsync(original, Ct);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.AddColumnAsync("Renamed", new("Added", typeof(int)) { IsCalculated = mode != "plain", CalculationExpression = mode switch { "independent" => "42", "dependent" => "[V]+1", _ => null } }, Ct);
        }

        stream.Position = 0;
        await using AccessReader actual = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        object[][] rows = (await actual.Rows("Renamed", cancellationToken: Ct).ToArrayAsync(Ct)).OrderBy(static row => row[0]).ToArray();
        Assert.Equal(expected.Length, rows.Length);
        for (int row = 0; row < rows.Length; row++)
        {
            // Native DDL zeroes surviving stale caches even for an ordinary field.
            // Retain the source caches while matching the new field's native binding.
            Assert.Equal(preserved[row], rows[row][..^1]);
            Assert.Equal(expected[row][^1], rows[row][^1]);
        }
    }

    [Theory]
    [InlineData("Normal", false)]
    [InlineData("Error", false)]
    [InlineData("Required", false)]
    [InlineData("ExistingRequired", false)]
    [InlineData("Dependent", false)]
    [InlineData("ErrorText", false)]
    [InlineData("ErrorMemo", false)]
    [InlineData("FieldRule", false)]
    [InlineData("ErrorRequired", false)]
    [InlineData("Normal", true)]
    [InlineData("Error", true)]
    [InlineData("Required", true)]
    [InlineData("ExistingRequired", true)]
    [InlineData("Dependent", true)]
    [InlineData("ErrorText", true)]
    [InlineData("ErrorMemo", true)]
    [InlineData("FieldRule", true)]
    [InlineData("ErrorRequired", true)]
    public async Task AddCalculatedColumn_PopulatesExistingRowsLikeNative(string table, bool compacted)
    {
        bool extended = table is "Dependent" or "ErrorText" or "ErrorMemo" or "FieldRule" or "ErrorRequired";
        string specimen = "native-add" + (extended ? "-extended" : string.Empty) + (compacted ? "-compacted" : string.Empty) + ".accdb";
        object[][] expected;
        await using (AccessReader native = await AccessReader.OpenAsync(Path.Combine(AppContext.BaseDirectory, "Databases", "CalculatedAdd", specimen), new AccessReaderOptions { UseLockFile = false }, Ct))
        {
            expected = (await native.Rows(table, cancellationToken: Ct).ToArrayAsync(Ct)).OrderBy(static row => row[0]).ToArray();
        }

        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync(table, [new("Id", typeof(int)), new("N", typeof(int))], Ct);
            await writer.InsertRowsAsync(table, (object?[][])[[1, 2], [2, 0], [3, null]], Ct);
            if (table == "Dependent")
            {
                await writer.AddColumnAsync(table, new("C", typeof(int)) { IsCalculated = true, CalculationExpression = "[N]*2" }, Ct);
            }

            Type type = table switch { "Error" or "ErrorRequired" => typeof(double), "ErrorText" or "ErrorMemo" => typeof(string), _ => typeof(int) };
            string expression = table switch { "Error" or "ErrorText" or "ErrorMemo" or "ErrorRequired" => "1/[N]", "Dependent" => "[C]+1", "ExistingRequired" => "[N]", _ => "[N]*2" };
            int width = table == "ErrorText" ? 243 : 0;
            var column = new ColumnDefinition(extended ? "Added" : "C", type, width)
            {
                IsCalculated = true,
                CalculationExpression = expression,
                IsNullable = table is not ("Required" or "ExistingRequired" or "ErrorRequired"),
                ValidationRuleExpression = table == "FieldRule" ? ">100" : null,
            };
            await writer.AddColumnAsync(table, column, Ct);
        }

        stream.Position = 0;
        await using AccessReader actual = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        object[][] rows = (await actual.Rows(table, cancellationToken: Ct).ToArrayAsync(Ct)).OrderBy(static row => row[0]).ToArray();
        Assert.Equal(expected.Length, rows.Length);
        for (int row = 0; row < rows.Length; row++)
        {
            Assert.Equal(expected[row], rows[row]);
        }
    }
}
