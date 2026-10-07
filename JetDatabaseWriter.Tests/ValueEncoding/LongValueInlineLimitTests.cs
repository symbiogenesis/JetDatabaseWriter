namespace JetDatabaseWriter.Tests.ValueEncoding;

using System;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueEncoding;
using JetDatabaseWriter.ValueEncoding.Models;
using Xunit;

/// <summary>Native JET/ACE long values store at most 64 payload bytes inline.</summary>
public sealed class LongValueInlineLimitTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, ColumnType.MemoType)]
    [InlineData(DatabaseFormat.Jet4Mdb, ColumnType.MemoType)]
    [InlineData(DatabaseFormat.AceAccdb, ColumnType.MemoType)]
    [InlineData(DatabaseFormat.Jet3Mdb, ColumnType.OleType)]
    [InlineData(DatabaseFormat.Jet4Mdb, ColumnType.OleType)]
    [InlineData(DatabaseFormat.AceAccdb, ColumnType.OleType)]
    public async Task NativeInlineBoundary_PlansLvalWithoutWriting(DatabaseFormat databaseFormat, ColumnType columnType)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, databaseFormat, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            Assert.True(stream.Length > 0);
        }

        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: TestContext.Current.CancellationToken);
        var encoder = new LongValueEncoder(harness.Database.Format, harness.Pager, harness.Services.PageAllocator, new AccessWriterOptions());
        var definition = new TableDef { Columns = [new ColumnInfo { Name = "Value", Type = columnType }] };
        int[] payloadSizes = [64, 66];
        foreach (int payloadSize in payloadSizes)
        {
            object value = columnType == ColumnType.MemoType
                ? new string('A', databaseFormat == DatabaseFormat.Jet3Mdb ? payloadSize : payloadSize / 2)
                : new byte[payloadSize];
            object[] values = [value];
            byte[] before = stream.ToArray();
            object[] planned = encoder.PrepareLongValues(definition, values);
            if (payloadSize > 64)
            {
                var pending = Assert.IsType<PreEncodedLongValue>(planned[0]);
                Assert.Equal(payloadSize, pending.PendingPayload!.Length);
                Assert.NotSame(values, planned);
            }
            else
            {
                Assert.Same(values, planned);
            }

            Assert.Equal(before, stream.ToArray());
            Assert.Same(value, values[0]);
        }
    }
}
