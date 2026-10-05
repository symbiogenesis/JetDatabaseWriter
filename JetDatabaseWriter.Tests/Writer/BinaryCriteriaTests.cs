namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Binary and OLE criteria compare stored bytes by content.</summary>
public sealed class BinaryCriteriaTests
{
    /// <summary>Gets every supported format and write mode.</summary>
    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndModes =>
        WriteModes.Combine(DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb);

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task UpdateAndDelete_ByteCriteriaMatchContent(DatabaseFormat format, WriteMode mode)
    {
        foreach (bool ole in new[] { false, true })
        {
            foreach (string operation in new[] { "Equal", "NotEqual", "In", "SingleColumn" })
            {
                await VerifyMutationAsync(format, mode, ole, operation, delete: false);
                await VerifyMutationAsync(format, mode, ole, operation, delete: true);
            }
        }
    }

    private static async Task VerifyMutationAsync(DatabaseFormat format, WriteMode mode, bool ole, string operation, bool delete)
    {
        var ct = TestContext.Current.CancellationToken;
        byte[] value = Enumerable.Repeat((byte)128, ole ? 600 : 3).ToArray();
        value[0] = 255;
        byte[] unequal = (byte[])value.Clone();
        unequal[^1] = 127;
        int[] expected = operation switch
        {
            "NotEqual" => [2, 3, 4, 5],
            "In" => [1, 4],
            _ => [1],
        };

        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(mode), leaveOpen: true, cancellationToken: ct))
        {
            await writer.CreateTableAsync("Bytes", [new("Id", typeof(int)), new("Value", typeof(byte[]), ole ? 0 : 20), new("Changed", typeof(bool))], ct);
            await writer.InsertRowsAsync("Bytes", new object?[][]
            {
                [1, value, false],
                [2, unequal, false],
                [3, value[..^1], false],
                [4, Array.Empty<byte>(), false],
                [5, null, false],
            }, ct);

            await WriteModes.RunAsync(writer, mode, async () =>
            {
                // Each operand is a different array instance from the inserted value.
                byte[] operand = (byte[])value.Clone();
                var updated = new RowValues { ["Changed"] = true };
                int changed;
                if (operation == "SingleColumn")
                {
                    changed = delete
                        ? await writer.DeleteRowsAsync("Bytes", "Value", operand, ct)
                        : await writer.UpdateRowsAsync("Bytes", "Value", operand, new Dictionary<string, object?> { ["Changed"] = true }, ct);
                }
                else
                {
                    ColumnPredicate predicate = operation switch
                    {
                        "NotEqual" => ColumnPredicate.NotEqualTo("Value", operand),
                        "In" => ColumnPredicate.In("Value", operand, Array.Empty<byte>()),
                        _ => ColumnPredicate.EqualTo("Value", operand),
                    };
                    RowCriteria criteria = RowCriteria.Where(predicate);
                    changed = delete
                        ? await writer.DeleteRowsAsync("Bytes", criteria, ct)
                        : await writer.UpdateRowsAsync("Bytes", criteria, updated, ct);
                }

                Assert.Equal(expected.Length, changed);
            }, ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct);
        DataTable table = await reader.ReadDataTableAsync("Bytes", cancellationToken: ct);
        int[] actual = table.AsEnumerable().Where(row => delete || row.Field<bool>("Changed")).Select(row => row.Field<int>("Id")).Order().ToArray();
        Assert.Equal(delete ? Enumerable.Range(1, 5).Except(expected).ToArray() : expected, actual);
    }
}
