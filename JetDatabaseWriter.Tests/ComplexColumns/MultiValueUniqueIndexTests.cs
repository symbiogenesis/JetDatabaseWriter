namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Tests.ComplexColumns.ComplexColumnTestSupport;

/// <summary>Native multi-value unique keys apply to each parent separately before mutation.</summary>
public sealed class MultiValueUniqueIndexTests
{
    /// <summary>Gets every supported native element type in each write mode.</summary>
    public static TheoryData<string, WriteMode> Cases()
    {
        var data = new TheoryData<string, WriteMode>();
        foreach (string type in new[] { "Byte", "Short", "Long", "Single", "Double", "Guid", "Decimal", "Text" })
        {
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                data.Add(type, mode);
            }
        }

        return data;
    }

    /// <summary>Duplicate refusal keeps bytes and counters intact, and distinct parents may share a value.</summary>
    /// <param name="element">The native multi-value type.</param>
    /// <param name="mode">The write mode.</param>
    [Theory]
    [MemberData(nameof(Cases))]
    public async Task DuplicateItem_IsRefusedBeforeMutationAndCounterConsumption(string element, WriteMode mode)
    {
        object value = element switch
        {
            "Byte" => (byte)12,
            "Short" => (short)12,
            "Long" => 12,
            "Single" => 12f,
            "Double" => 12d,
            "Guid" => new Guid("00000001-0002-0003-0405-060708090a0b"),
            "Decimal" => 12m,
            _ => "native-value",
        };
        object next = element switch
        {
            "Byte" => (byte)13,
            "Short" => (short)13,
            "Long" => 13,
            "Single" => 13f,
            "Double" => 13d,
            "Guid" => new Guid("00000002-0002-0003-0405-060708090a0b"),
            "Decimal" => 13m,
            _ => "distinct-value",
        };
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(stream))
        {
            await writer.CreateTableAsync("Parents", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Items", typeof(object)) { IsMultiValue = true, MultiValueElementType = value.GetType() }], Ct);
            await writer.InsertRowsAsync("Parents", [[1, DBNull.Value], [2, DBNull.Value]], Ct);
            await writer.AddMultiValueItemAsync("Parents", "Items", Key(1), value, Ct);
            await writer.AddMultiValueItemAsync("Parents", "Items", Key(2), value, Ct);
        }

        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await OpenWriterAsync(stream, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                JetConstraintException error = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                    await writer.AddMultiValueItemAsync("Parents", "Items", Key(1), value, Ct));
                Assert.Equal(JetErrorCode.UniqueViolation, error.ErrorCode);
                Assert.Equal(before, stream.ToArray());
            });
            Assert.Equal(before, stream.ToArray());
            await RunAsync(writer, mode, async () => await writer.AddMultiValueItemAsync("Parents", "Items", Key(1), next, Ct));
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        ComplexColumnInfo info = Assert.Single(await reader.GetComplexColumnsAsync("Parents", Ct));
        IndexMetadata unique = Assert.Single(await reader.ListIndexesAsync(info.FlatTableName, Ct), index => index.Name == "IdxFKPrimaryScalar");
        Assert.True(unique.HasUniqueFlag);
        Assert.False(unique.IsRequired);
        Assert.Equal(["_Items", "Value"], unique.Columns.Select(column => column.Name));
        using DataTable rows = await reader.ReadTableAsync(info.FlatTableName, cancellationToken: Ct);
        Assert.Equal([1, 2, 3], rows.Rows.Cast<DataRow>().Select(row => Assert.IsType<int>(row["Parents_Items"])).Order());
    }

    private static Dictionary<string, object?> Key(int id) => new() { ["Id"] = id };
}
