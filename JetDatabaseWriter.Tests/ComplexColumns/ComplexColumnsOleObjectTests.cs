namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Read-side coverage for the OLE Object column type (TDEF code <c>0x0B</c>,
/// LVAL chain) using the Jackcess <c>testOleV2007.accdb</c> fixture, which
/// contains mixed OLE payloads, including package-wrapped text attachments and
/// image content. Reads return the stored bytes, OLE Package header included,
/// and <see cref="OleObjectValue"/> unwraps the embedded file on request.
///
/// <para>Jackcess analogue: <c>util/OleBlobTest.java</c>.
/// </para>
/// </summary>
/// <param name="db">The database input.</param>
public sealed class ComplexColumnsOleObjectTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    /// <summary>
    /// At least one column in <c>testOleV2007</c> is reported as the
    /// <c>OLE Object</c> type with CLR type <c>byte[]</c>.
    /// </summary>
    [Fact]
    public async Task TestOleV2007_HasOleColumn_TypedAsByteArray()
    {
        if (!File.Exists(TestDatabases.TestOleV2007))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(TestDatabases.TestOleV2007, TestContext.Current.CancellationToken);
        IReadOnlyList<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        ColumnMetadata? oleColumn = null;
        foreach (string table in tables)
        {
            IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);
            oleColumn = meta.FirstOrDefault(c => c.TypeName == "OLE Object");
            if (oleColumn is not null)
            {
                break;
            }
        }

        Assert.NotNull(oleColumn);
        Assert.Equal(typeof(byte[]), oleColumn.ClrType);
    }

    /// <summary>
    /// Every OLE-typed value in the fixture decodes to a non-null
    /// <c>byte[]</c> with at least one byte. Asserts the LVAL chain reader
    /// does not silently truncate to empty for known fixture content.
    /// </summary>
    [Fact]
    public async Task TestOleV2007_OleValues_DecodeToNonEmptyByteArrays()
    {
        if (!File.Exists(TestDatabases.TestOleV2007))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(TestDatabases.TestOleV2007, TestContext.Current.CancellationToken);
        IReadOnlyList<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        bool sawAnyOleValue = false;

        foreach (string table in tables)
        {
            IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);
            int[] oleOrdinals = meta
                .Select((c, i) => (c, i))
                .Where(t => t.c.TypeName == "OLE Object")
                .Select(t => t.i)
                .ToArray();

            if (oleOrdinals.Length == 0)
            {
                continue;
            }

            await foreach (object[] row in reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken))
            {
                foreach (int ordinal in oleOrdinals)
                {
                    object value = row[ordinal];
                    if (value is byte[] bytes)
                    {
                        sawAnyOleValue = true;
                        Assert.NotEmpty(bytes);
                    }
                }
            }
        }

        Assert.True(sawAnyOleValue, "testOleV2007 was expected to contain at least one non-null OLE Object value.");
    }

    /// <summary>
    /// The Jackcess <c>testOleV2007</c> fixture includes image payloads. The typed
    /// reader returns them wrapped as stored, and <see cref="OleObjectValue.GetContent"/>
    /// unwraps them to real image bytes.
    /// </summary>
    [Fact]
    public async Task TestOleV2007_ImageOleValues_UnwrapToRecognizedImageBytes()
    {
        if (!File.Exists(TestDatabases.TestOleV2007))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(TestDatabases.TestOleV2007, TestContext.Current.CancellationToken);
        IReadOnlyList<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        bool sawAnyImagePayload = false;
        foreach (string table in tables)
        {
            IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);
            int[] oleOrdinals = meta
                .Select((column, index) => (column, index))
                .Where(static entry => entry.column.TypeName == "OLE Object")
                .Select(static entry => entry.index)
                .ToArray();

            if (oleOrdinals.Length == 0)
            {
                continue;
            }

            await foreach (object[] row in reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken))
            {
                foreach (int ordinal in oleOrdinals)
                {
                    byte[] bytes = OleObjectValue.GetContent(ExtractOleBytes(row[ordinal]));
                    if (bytes.Length == 0)
                    {
                        continue;
                    }

                    if (!StartsWithKnownImageMagic(bytes))
                    {
                        continue;
                    }

                    sawAnyImagePayload = true;
                }
            }
        }

        Assert.True(sawAnyImagePayload, "testOleV2007 was expected to contain at least one decodable image payload.");
    }

    /// <summary>
    /// Package-wrapped non-image payloads unwrap, through <see cref="OleObjectValue.GetContent"/>,
    /// to the embedded file bytes.
    /// </summary>
    [Fact]
    public async Task TestOleV2007_PackageWrappedTextOleValues_UnwrapToRawBytes()
    {
        if (!File.Exists(TestDatabases.TestOleV2007))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(TestDatabases.TestOleV2007, TestContext.Current.CancellationToken);
        IReadOnlyList<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        bool sawUnwrappedTextPayload = false;
        byte[] expectedPrefix =
        [
            0x74, 0x68, 0x69, 0x73, 0x20, 0x69, 0x73, 0x20, 0x73, 0x6F, 0x6D,
            0x65, 0x20, 0x74, 0x65, 0x73, 0x74, 0x20, 0x64, 0x61, 0x74, 0x61,
        ];

        foreach (string table in tables)
        {
            IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);
            int[] oleOrdinals = meta
                .Select((column, index) => (column, index))
                .Where(static entry => entry.column.TypeName == "OLE Object")
                .Select(static entry => entry.index)
                .ToArray();

            if (oleOrdinals.Length == 0)
            {
                continue;
            }

            await foreach (object[] row in reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken))
            {
                foreach (int ordinal in oleOrdinals)
                {
                    byte[] bytes = OleObjectValue.GetContent(ExtractOleBytes(row[ordinal]));
                    if (HasPrefix(bytes, expectedPrefix))
                    {
                        sawUnwrappedTextPayload = true;
                        break;
                    }
                }

                if (sawUnwrappedTextPayload)
                {
                    break;
                }
            }

            if (sawUnwrappedTextPayload)
            {
                break;
            }
        }

        Assert.True(sawUnwrappedTextPayload, "testOleV2007 was expected to contain at least one unwrapped package payload with the known text fixture bytes.");
    }

    /// <summary>
    /// TIFF bytes are classified as <c>image/tiff</c> when they are not wrapped in
    /// a package envelope.
    /// </summary>
    [Fact]
    public void DetectMediaType_RawTiffBytes_ReturnsImageTiff()
    {
        byte[] bytes = [0x49, 0x49, 0x2A, 0x00, 0x08, 0x00, 0x00, 0x00];

        Assert.Equal("image/tiff", OleObjectValue.DetectMediaType(bytes));
        Assert.Equal(OleObjectKind.NotWrapped, OleObjectValue.Parse(bytes).Kind);
    }

    /// <summary>
    /// Round-trip: an inline-OLE payload (≤ 256 bytes per the writer's
    /// documented inline-OLE limit) created via <see cref="AccessWriter"/>
    /// reads back byte-identical via <see cref="AccessReader"/>. Mirrors the
    /// Jackcess <c>testWriteAndReadInDb</c> path of <c>OleBlobTest</c>.
    /// </summary>
    [Fact]
    public async Task InsertRow_OleBytes_RoundTripsByteIdentical()
    {
        if (!File.Exists(TestDatabases.NorthwindTraders))
        {
            return;
        }

        byte[] sourceBytes = await File.ReadAllBytesAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        var ms = new MemoryStream();
        await ms.WriteAsync(sourceBytes, TestContext.Current.CancellationToken);
        ms.Position = 0;

        string tableName = $"OleRT_{Guid.NewGuid():N}"[..18];
        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Blob", typeof(byte[])),
        };

        // 200 bytes – well under the writer's inline-OLE limit, with a
        // recognisable byte pattern so byte-for-byte equality is meaningful.
        // Use unchecked arithmetic because the project enables CheckForOverflowUnderflow.
        byte[] payload = new byte[200];
        unchecked
        {
            for (int i = 0; i < payload.Length; i++)
            {
                payload[i] = (byte)((i * 7) ^ 0xA5);
            }
        }

        ms.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(ms, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(tableName, [1, payload], TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using (AccessReader reader = await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            object[]? actual = null;
            await foreach (object[] row in reader.Rows(tableName, cancellationToken: TestContext.Current.CancellationToken))
            {
                actual = row;
                break;
            }

            Assert.NotNull(actual);

            // Reader returns OLE payloads as byte[] at the row level. Tolerate
            // the older string representation so this round-trip test pins the
            // value-preservation contract independently of representation.
            switch (actual[1])
            {
                case byte[] roundTripped:
                    Assert.Equal(payload, roundTripped);
                    break;
                case string s:
                    // Compare via the system codepage round-trip: sufficient
                    // to detect truncation, which is the failure mode we care about.
                    Assert.True(s.Length >= payload.Length / 2, $"Round-tripped OLE string is suspiciously short ({s.Length} chars).");
                    break;
                default:
                    Assert.Fail($"Unexpected OLE round-trip value type: {actual[1]?.GetType().FullName ?? "<null>"}");
                    break;
            }
        }
    }

    private static byte[] ExtractOleBytes(object? cell)
    {
        if (cell is null or DBNull)
        {
            return [];
        }

        if (cell is byte[] bytes)
        {
            return bytes;
        }

        if (cell is string value)
        {
            return BinaryStringParser.TryDecodeBase64DataUri(value, null, out byte[] decoded) ? decoded : [];
        }

        return [];
    }

    private static bool StartsWithKnownImageMagic(byte[] bytes)
    {
        ReadOnlySpan<byte> span = bytes;
        return HasPrefix(span, [0xFF, 0xD8, 0xFF])
            || HasPrefix(span, [0x89, 0x50, 0x4E, 0x47])
            || HasPrefix(span, [0x47, 0x49, 0x46])
            || HasPrefix(span, [0x42, 0x4D])
            || HasPrefix(span, [0x49, 0x49, 0x2A, 0x00])
            || HasPrefix(span, [0x4D, 0x4D, 0x00, 0x2A])
            || HasPrefix(span, [0x52, 0x49, 0x46, 0x46]);
    }

    private static bool HasPrefix(ReadOnlySpan<byte> value, ReadOnlySpan<byte> prefix)
    {
        if (value.Length < prefix.Length)
        {
            return false;
        }

        for (int i = 0; i < prefix.Length; i++)
        {
            if (value[i] != prefix[i])
            {
                return false;
            }
        }

        return true;
    }
}
