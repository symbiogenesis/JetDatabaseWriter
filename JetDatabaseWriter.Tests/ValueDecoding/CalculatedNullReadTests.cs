namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

#pragma warning disable CA1812 // Public row materialization constructs this projection target.

/// <summary>Access stores a valid null calculated scalar as an envelope with no payload.</summary>
public sealed class CalculatedNullReadTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task NativeNullCachedDate_PublicReadsPreserveMissingValue(bool strict)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using AccessReader reader = await AccessReader.OpenAsync(TestDatabases.ExtDateTestV2019, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, ct);
        IReadOnlyList<ColumnMetadata> columns = await reader.GetColumnMetadataAsync("Table1", ct);
        ColumnMetadata calculated = Assert.Single(columns, column => column.Name == "DateNormalCalc");
        Assert.True(calculated.IsCalculated);
        List<object[]> typed = await CollectAsync(reader.Rows("Table1", cancellationToken: ct));
        List<string[]> strings = await CollectAsync(reader.RowsAsStrings("Table1", cancellationToken: ct));
        List<DateRow> pocos = await CollectAsync(reader.Rows<DateRow>("Table1", cancellationToken: ct));
        Assert.Equal(typed.Count, strings.Count);
        Assert.Equal(typed.Count, pocos.Count);
        int nullCount = 0;
        for (int i = 0; i < typed.Count; i++)
        {
            object value = typed[i][calculated.Ordinal];
            if (value is DBNull)
            {
                nullCount++;
                Assert.Equal(string.Empty, strings[i][calculated.Ordinal]);
                Assert.Null(pocos[i].DateNormalCalc);
            }
            else
            {
                DateTime date = Assert.IsType<DateTime>(value);
                Assert.Equal(date, pocos[i].DateNormalCalc);
                Assert.Equal(date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture), strings[i][calculated.Ordinal]);
            }
        }

        Assert.True(nullCount > 0, "The native fixture must exercise a null cached calculation.");
    }

    private static async Task<List<T>> CollectAsync<T>(IAsyncEnumerable<T> rows)
    {
        var result = new List<T>();
        await foreach (T row in rows)
        {
            result.Add(row);
        }

        return result;
    }

    private sealed class DateRow
    {
        public DateTime? DateNormalCalc { get; set; }
    }
}
