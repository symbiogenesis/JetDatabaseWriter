namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.Buffers.Binary;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

/// <summary>Odd declared plain chained-Memo bytes remain observable but cannot silently become truncated text.</summary>
public sealed class PlainChainedMemoDecodePolicyTests
{
    private static readonly string[] Surfaces = ["typed", "strings", "poco", "table", "string-table"];

    /// <summary>Gets independent single-byte mutations of pinned DAO-authored plain Memo chains.</summary>
    public static TheoryData<string, int, string> NativeCases => new()
    {
        { "native.mdb", 6, "967643A8DE722A68665D768CE7CF48C2A32691BE751DBAC1EBD688D2CB705740" },
        { "native.mdb", 30, "2B09D9A0D0DFE4BE22A9B2A7ACB6BBACCF042F7082E950AFC284B01651534CB0" },
        { "native.accdb", 6, "BEB4CE081039601EEA0CFCE5A0336DEF5EC3C575699477CCC13F931F9F8AF402" },
        { "native.accdb", 30, "AB59FEB6D3A3010A05B252E236EB8A957F177C8E8E9B4BBBB62B8FB107352178" },
    };

    /// <summary>Gets both parsing modes and every public materialization surface.</summary>
    public static TheoryData<string, int, string, bool, string> ReadCases()
    {
        var cases = new TheoryData<string, int, string, bool, string>();
        foreach (TheoryDataRow<string, int, string> item in NativeCases)
        {
            foreach (bool strict in new[] { false, true })
            {
                foreach (string surface in Surfaces)
                {
                    cases.Add(item.Data.Item1, item.Data.Item2, item.Data.Item3, strict, surface);
                }
            }
        }

        return cases;
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [MemberData(nameof(ReadCases))]
    public async Task OddPlainChain_PublicReadsDiagnoseIncompleteCodeUnit(string fixture, int id, string hash, bool strict, string surface)
    {
        (byte[] bytes, _) = await DamagedAsync(fixture, id, hash);
        await using var messages = new StringWriter(CultureInfo.InvariantCulture);
        using var listener = new TextWriterTraceListener(messages);
        using var trace = new TestTraceScope(listener);
        await using var stream = new MemoryStream(bytes, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: Ct);
        if (strict)
        {
            JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ReadSurfaceAsync(reader, surface, id, Ct));
            Assert.Equal(JetErrorCode.UnreadableLongValue, failure.ErrorCode);
            Assert.Equal("Content", failure.ErrorInfo.ColumnName);
            JetCorruptDataException cause = Assert.IsType<JetCorruptDataException>(failure.InnerException);
            Assert.Contains("UTF-16", cause.Message, StringComparison.Ordinal);
        }
        else
        {
            object? value = await ReadSurfaceAsync(reader, surface, id, Ct);
            if (surface is "strings" or "poco")
            {
                Assert.Null(value);
            }
            else
            {
                Assert.Equal(DBNull.Value, value);
            }

            listener.Flush();
            Assert.Contains("column 'Content' is unreadable and was returned as a missing value", messages.ToString(), StringComparison.Ordinal);
            Assert.DoesNotContain(new string('b', 10), messages.ToString(), StringComparison.Ordinal);
        }

        Assert.Equal(bytes, stream.ToArray());
    }

    [Theory]
    [MemberData(nameof(NativeCases))]
    public async Task OddAssignedPlainChain_UnrelatedUpdateRefusesWithoutChangingBytes(string fixture, int id, string hash)
    {
        (byte[] bytes, _) = await DamagedAsync(fixture, id, hash);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(bytes, Ct);
        stream.Position = 0;
        await using AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await writer.UpdateRowsAsync("Items", RowCriteria.Where("Id", id), new RowValues { ["Id"] = id + 100 }, Ct));
        Assert.Equal(bytes, stream.ToArray());
    }

    [Theory]
    [MemberData(nameof(NativeCases))]
    public async Task OddPlainChain_RawReadsAndFullAuditPreserveDeclaredAndTrailingBytes(string fixture, int id, string hash)
    {
        (byte[] bytes, StoredMemo stored) = await DamagedAsync(fixture, id, hash);
        await using var stream = new MemoryStream(bytes, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        using var cache = new ReaderPageCache(reader.Database.Format, reader.Database.Pages, capacity: 0);
        var decoder = new LongValueDecoder(reader.Database.Format, cache);
        byte[] column = bytes.AsSpan(stored.ColumnOffset, 12).ToArray();
        byte[] raw = await decoder.ReadLongValueRawBytesAsync(column, 0, column.Length, Ct);
        byte[] expected = Encoding.Unicode.GetBytes((id == 30 ? "\uFEFF" : string.Empty) + new string('b', id == 30 ? 2048 : 2000));
        Assert.Equal(expected[..^1], raw);
        await using var copy = new MemoryStream();
        await decoder.CopyLongValueRawBytesAsync(column, 0, column.Length, copy, 100, Ct);
        Assert.Equal(raw, copy.ToArray());
        int trailing = 0;
        await CompactLongValueAudit.AuditAsync(
            reader.Database.Pages,
            reader.Database.Format,
            column,
            new(),
            (record, _) =>
            {
                trailing += record.TrailingBytes;
                return default;
            },
            Ct);
        Assert.True(trailing > 0);
        Assert.Equal(bytes, stream.ToArray());
    }

    [Theory]
    [InlineData(false, false, false)]
    [InlineData(false, false, true)]
    [InlineData(false, true, false)]
    [InlineData(false, true, true)]
    [InlineData(true, false, false)]
    [InlineData(true, false, true)]
    [InlineData(true, true, false)]
    [InlineData(true, true, true)]
    public async Task PayloadIoAndCancellation_PropagateBeforeOddLengthDiagnosis(bool strict, bool cancel, bool odd)
    {
        byte[] bytes;
        StoredMemo stored;
        if (odd)
        {
            (bytes, stored) = await DamagedAsync("native.accdb", 6, "BEB4CE081039601EEA0CFCE5A0336DEF5EC3C575699477CCC13F931F9F8AF402");
        }
        else
        {
            bytes = await ReadFixtureAsync("native.accdb");
            stored = await LocateAsync(bytes, 6);
        }

        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var failure = new IOException("Memo payload I/O failure");
        await using var stream = new FaultingStream(bytes, failure, cancellation);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict, PageCacheSize = 0, PageReadOptimizationMode = PageReadOptimizationMode.Disabled }, leaveOpen: true, cancellationToken: Ct);
        stream.FailureOffset = (stored.Descriptor.FirstDp >> 8) * reader.Database.Format.PageSize;
        stream.Cancel = cancel;
        if (cancel)
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await reader.Rows("Items", cancellationToken: cancellation.Token).ToListAsync(cancellation.Token));
        }
        else
        {
            IOException observed = await Assert.ThrowsAsync<IOException>(async () => await reader.Rows("Items", cancellationToken: Ct).ToListAsync(Ct));
            Assert.Same(failure, observed);
        }
    }

    private static async Task<object?> ReadSurfaceAsync(AccessReader reader, string surface, int id, CancellationToken token)
    {
        switch (surface)
        {
            case "typed":
                return (await reader.Rows("Items", cancellationToken: token).ToListAsync(token)).Single(row => (int)row[0] == id)[1];
            case "strings":
                return (await reader.RowsAsStrings("Items", cancellationToken: token).ToListAsync(token)).Single(row => int.Parse(row[0], CultureInfo.InvariantCulture) == id)[1];
            case "poco":
                return (await reader.Rows<MemoRow>("Items", cancellationToken: token).ToListAsync(token)).Single(row => row.Id == id).Content;
            case "table":
                using (DataTable table = await reader.ReadTableAsync("Items", cancellationToken: token))
                {
                    return table.Rows.Cast<DataRow>().Single(row => (int)row["Id"] == id)["Content"];
                }

            case "string-table":
                using (DataTable table = await reader.ReadTableAsStringsAsync("Items", cancellationToken: token))
                {
                    return table.Rows.Cast<DataRow>().Single(row => int.Parse((string)row["Id"], CultureInfo.InvariantCulture) == id)["Content"];
                }

            default:
                throw new ArgumentOutOfRangeException(nameof(surface));
        }
    }

    private static async Task<(byte[] Bytes, StoredMemo Stored)> DamagedAsync(string fixture, int id, string hash)
    {
        byte[] bytes = await ReadFixtureAsync(fixture);
        StoredMemo stored = await LocateAsync(bytes, id);
        Assert.True(stored.Descriptor.UsesChainedPages);
        Assert.Equal(id == 6 ? 4000 : 4098, stored.Descriptor.Length);
        Assert.Equal(id == 6 ? 85835 : 85219, stored.ColumnOffset);
        byte before = bytes[stored.ColumnOffset];
        bytes[stored.ColumnOffset]--;
        Assert.Equal((byte)(before - 1), bytes[stored.ColumnOffset]);
        Assert.Equal(hash, Convert.ToHexString(SHA256.HashData(bytes)));
        return (bytes, stored);
    }

    private static async Task<byte[]> ReadFixtureAsync(string fixture)
    {
        byte[] bytes = await File.ReadAllBytesAsync(Path.Combine(AppContext.BaseDirectory, "Databases", "NativeMemo", fixture), Ct);
        string expected = fixture == "native.mdb" ? "F1D9A911859401DB5CAD80D40F637CE0819DA273318F37A388890FD16A8A4FC2" : "6CAC20346F956E10297D0D0EA994DA84BB28B4BDBAC2FC206689EBF4B6446C95";
        Assert.Equal(expected, Convert.ToHexString(SHA256.HashData(bytes)));
        return bytes;
    }

    private static async Task<StoredMemo> LocateAsync(byte[] bytes, int id)
    {
        await using var stream = new MemoryStream(bytes, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        long owner = Assert.IsType<JetDatabaseWriter.Catalog.Models.CatalogEntry>(await reader.GetCatalogEntryAsync("Items", Ct)).TDefPage;
        TDefImage image = Assert.IsType<TDefImage>(await reader.Database.TableDefs.ReadImageAsync(owner, Ct));
        StoredMemo? found = null;
        await CompactRawRowReader.ScanAsync(
            reader.Database.Pages,
            reader.Database.Format,
            owner,
            new(),
            async (location, payload, token) =>
            {
                int current = -1;
                await CompactRawFieldReader.ScanAsync(
                    reader.Database.Format,
                    image,
                    location,
                    payload,
                    (field, _) =>
                    {
                        if (field.Column.Name == "Id")
                        {
                            current = BinaryPrimitives.ReadInt32LittleEndian(field.RawBytes.Span);
                        }
                        else if (current == id)
                        {
                            Assert.True(LongValueDescriptor.TryRead(field.RawBytes.Span, out LongValueDescriptor descriptor));
                            int offset = checked((int)(location.DataPageNumber * reader.Database.Format.PageSize) + location.RowStart + field.RowOffset);
                            found = new(offset, descriptor);
                        }

                        return default;
                    },
                    token);
            },
            Ct);
        return Assert.IsType<StoredMemo>(found);
    }

    private sealed record StoredMemo(int ColumnOffset, LongValueDescriptor Descriptor);

#pragma warning disable CA1812 // Public row materialization constructs this projection target.
    private sealed class MemoRow
    {
        public int Id { get; set; }

        public string? Content { get; set; }
    }
#pragma warning restore CA1812

    private sealed class FaultingStream(byte[] bytes, IOException failure, CancellationTokenSource cancellation) : MemoryStream(bytes, writable: false)
    {
        internal long FailureOffset { get; set; } = -1;

        internal bool Cancel { get; set; }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (this.Position != this.FailureOffset)
            {
                return await base.ReadAsync(buffer, cancellationToken);
            }

            if (this.Cancel)
            {
                await cancellation.CancelAsync();
                cancellation.Token.ThrowIfCancellationRequested();
            }

            throw failure;
        }
    }
}
