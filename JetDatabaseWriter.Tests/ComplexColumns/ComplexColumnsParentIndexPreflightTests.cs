namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Tests.ComplexColumns.ComplexColumnTestSupport;

/// <summary>Legacy null complex references must remain unchanged when parent indexes cannot be maintained.</summary>
public sealed class ComplexColumnsParentIndexPreflightTests
{
    /// <summary>Gets both complex item kinds in every write mode.</summary>
    public static TheoryData<bool, WriteMode> ItemKindsAndModes => new()
    {
        { false, WriteMode.Direct },
        { true, WriteMode.Direct },
        { false, WriteMode.AutoCommit },
        { true, WriteMode.AutoCommit },
        { false, WriteMode.ExplicitCommit },
        { true, WriteMode.ExplicitCommit },
    };

    [Theory]
    [MemberData(nameof(ItemKindsAndModes))]
    public async Task AddItem_LegacyNullReferenceWithDamagedParentIndex_RefusesBeforeMutation(bool attachment, WriteMode mode)
    {
        await using MemoryStream stream = await CopyFixtureAsync(TestDatabases.ComplexDataTestV2007);
        await ClearComplexReferencesAsync(stream, "Table1", r => (string)r[0] == "row4", clearCounter: false);
        string columnName = attachment ? "attach-data" : "multi-value-data";
        int realIndexNumber;
        await using (AccessReader reader = await OpenReaderAsync(stream))
        {
            realIndexNumber = Assert.Single(
                await reader.ListIndexesAsync("Table1", Ct),
                i => i.Columns.Count == 1 && i.Columns[0].Name == columnName).RealIndexNumber;
        }

        stream.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: Ct))
        {
            var table = await harness.Services.Catalog.ResolveRequiredTableAsync("Table1", Ct);
            await LegacyDamageInjector.SetPhantomKeyColumnAsync(harness, table.Entry.TDefPage, realIndexNumber, Ct);
        }

        RawTable beforeTable = await ReadRawTableAsync(stream, "Table1");
        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await OpenWriterAsync(stream, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                var key = new Dictionary<string, object?> { ["id"] = "row4" };
                JetLimitationException ex = await Assert.ThrowsAsync<JetLimitationException>(async () =>
                {
                    if (attachment)
                    {
                        await writer.AddAttachmentAsync("Table1", columnName, key, new AttachmentInput("refused.txt", Encoding.UTF8.GetBytes("refused")), Ct);
                    }
                    else
                    {
                        await writer.AddMultiValueItemAsync("Table1", columnName, key, "refused", Ct);
                    }
                });
                Assert.Contains("The indexes of table 'Table1' cannot be maintained", ex.Message, StringComparison.Ordinal);
            });
        }

        Assert.Equal(before, stream.ToArray());
        RawTable afterTable = await ReadRawTableAsync(stream, "Table1");
        Assert.Equal(beforeTable.ComplexAutoNumber, afterTable.ComplexAutoNumber);
        object[] parent = Assert.Single(afterTable.Rows, r => (string)r[0] == "row4");
        Assert.Null(Slot(afterTable, parent, "attach-data"));
        Assert.Null(Slot(afterTable, parent, "multi-value-data"));
    }
}
