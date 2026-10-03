namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// <c>CreateTableAsync</c> and <c>AddColumnAsync</c> check that the format can
/// hold each declared column before any catalog I/O, and report a definition
/// they reject with the public parameter that carried it: <c>columns</c> for
/// <c>CreateTableAsync</c> and <c>column</c> for <c>AddColumnAsync</c>.
/// </summary>
public sealed class ColumnDefinitionArgumentTests
{
    private const string BadColumnName = "Bad";

    private static readonly DatabaseFormat[] AllFormats = [DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets each rejected definition kind, in every format that reaches its check.</summary>
    public static TheoryData<DatabaseFormat, string> InvalidDefinitionCases
    {
        get
        {
            var data = new TheoryData<DatabaseFormat, string>();
            foreach (DatabaseFormat format in AllFormats)
            {
                data.Add(format, "AttachmentAndMultiValue");
                data.Add(format, "CurrencyMultiValue");
                data.Add(format, "CurrencyNotDecimal");
                data.Add(format, "ExtendedDateNotDateTime");
                data.Add(format, "HyperlinkNotMemo");
                data.Add(format, "PrecisionAbove28");
                data.Add(format, "ScaleAbovePrecision");
            }

            // On an .mdb these are refused first as calculated or multi-value
            // columns, with NotSupportedException.
            data.Add(DatabaseFormat.AceAccdb, "CalculatedWithoutExpression");
            data.Add(DatabaseFormat.AceAccdb, "MultiValueDecimalPrecisionAbove28");
            return data;
        }
    }

    /// <summary>
    /// A definition the format check rejects with <see cref="ArgumentException"/>
    /// names the public parameter. The checks run before the catalog is read:
    /// <c>CreateTableAsync</c> reports the column although the table name is
    /// taken, and <c>AddColumnAsync</c> reports it although the table does not
    /// exist. The file is unchanged.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="kind">The rejected definition; see <see cref="BadColumn"/>.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(InvalidDefinitionCases))]
    public async Task CreateTableAndAddColumn_InvalidDefinition_ReportPublicParameterBeforeCatalogIo(DatabaseFormat format, string kind)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync("Existing", [new("Id", typeof(int))], Ct);
            await writer.InsertRowAsync("Existing", [1], Ct);
        }

        byte[] before = stream.ToArray();

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            ArgumentException create = await Assert.ThrowsAnyAsync<ArgumentException>(async () =>
                await writer.CreateTableAsync("Existing", [new("Id", typeof(int)), BadColumn(kind)], Ct));
            AssertRejected(create, kind, "columns");

            ArgumentException add = await Assert.ThrowsAnyAsync<ArgumentException>(async () =>
                await writer.AddColumnAsync("NoSuchTable", BadColumn(kind), Ct));
            AssertRejected(add, kind, "column");

            ArgumentException addExisting = await Assert.ThrowsAnyAsync<ArgumentException>(async () =>
                await writer.AddColumnAsync("Existing", BadColumn(kind), Ct));
            AssertRejected(addExisting, kind, "column");
        }

        Assert.Equal(before, stream.ToArray());

        await using AccessReader reader = await OpenReaderAsync(stream);
        Assert.Equal(["Existing"], await reader.ListTablesAsync(Ct));
        DataTable table = await reader.ReadDataTableAsync("Existing", cancellationToken: Ct);
        Assert.Equal("Id", Assert.Single(table.Columns.Cast<DataColumn>()).ColumnName);
        Assert.Equal(1, Assert.Single(table.AsEnumerable())["Id"]);
    }

    private static ColumnDefinition BadColumn(string kind) => kind switch
    {
        "AttachmentAndMultiValue" => new(BadColumnName, typeof(object)) { IsAttachment = true, IsMultiValue = true, MultiValueElementType = typeof(int) },
        "CurrencyMultiValue" => new(BadColumnName, typeof(object)) { IsCurrency = true, IsMultiValue = true, MultiValueElementType = typeof(decimal) },
        "CurrencyNotDecimal" => new(BadColumnName, typeof(int)) { IsCurrency = true },
        "ExtendedDateNotDateTime" => new(BadColumnName, typeof(string), maxLength: 20) { IsDateTimeExtended = true },
        "HyperlinkNotMemo" => new(BadColumnName, typeof(string), maxLength: 50) { IsHyperlink = true },
        "PrecisionAbove28" => new(BadColumnName, typeof(decimal)) { NumericPrecision = 29 },
        "ScaleAbovePrecision" => new(BadColumnName, typeof(decimal)) { NumericPrecision = 5, NumericScale = 6 },
        "CalculatedWithoutExpression" => new(BadColumnName, typeof(int)) { IsCalculated = true },
        "MultiValueDecimalPrecisionAbove28" => new(BadColumnName, typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(decimal), NumericPrecision = 29 },
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unknown definition kind."),
    };

    private static void AssertRejected(ArgumentException exception, string kind, string paramName)
    {
        // A precision or scale out of range is ArgumentOutOfRangeException.
        bool outOfRange = kind is "PrecisionAbove28" or "ScaleAbovePrecision" or "MultiValueDecimalPrecisionAbove28";
        Assert.Equal(outOfRange ? typeof(ArgumentOutOfRangeException) : typeof(ArgumentException), exception.GetType());
        Assert.Equal(paramName, exception.ParamName);
        Assert.Contains($"'{BadColumnName}'", exception.Message, StringComparison.Ordinal);
    }

    private static async ValueTask<MemoryStream> CreateFreshStreamAsync(DatabaseFormat format)
    {
        var stream = new MemoryStream();
        await using (await AccessWriter.CreateDatabaseAsync(
            stream,
            format,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            Ct))
        {
        }

        stream.Position = 0;
        return stream;
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, Ct);
    }
}
