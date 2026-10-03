namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using Xunit;

/// <summary>
/// Every read API returns an OLE Object value as its stored bytes, inline, on one
/// LVAL page or in an LVAL chain, as DAO, ADO and Jackcess do. The string APIs
/// render the stored bytes as a <c>data:</c> URI whose media type comes only from a
/// file signature at the first byte.
/// </summary>
public sealed class OleObjectReadTests
{
    private static readonly byte[] PngSignature = [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];

    /// <summary>The OLE values a test stores.</summary>
    [SuppressMessage("Design", "CA1515:Consider making public types internal", Justification = "Theory parameters of public xUnit test methods must be public.")]
    public enum Payload
    {
        /// <summary>An inline value starting 00 01 and then the "BM" bitmap signature.</summary>
        PrefixedBitmapInline = 0,

        /// <summary>An inline OLE Package embedding a small text file.</summary>
        PackageInline = 1,

        /// <summary>A "%PDF" signature after a 56-byte prefix, on one LVAL page.</summary>
        PdfAfterPrefixSinglePage = 2,

        /// <summary>A JPEG signature at offset 100, in an LVAL chain.</summary>
        JpegAtOffset100Chained = 3,

        /// <summary>An OLE Package embedding a 9,000-byte file, in an LVAL chain.</summary>
        PackageChained = 4,
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every writer-created format with every payload.</summary>
    /// <returns>The format and payload pairs.</returns>
    public static TheoryData<DatabaseFormat, Payload> FormatsAndPayloads()
    {
        var data = new TheoryData<DatabaseFormat, Payload>();
        foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
        {
            foreach (Payload payload in Enum.GetValues<Payload>())
            {
                data.Add(format, payload);
            }
        }

        return data;
    }

    /// <summary>
    /// Builds an OLE Package as Access stores a file inserted into an OLE Object
    /// field: Access's OLE header, an MS-OLEDS embedded object of class
    /// <c>Package</c>, the package (type 3), an empty presentation object and a
    /// 4-byte trailer.
    /// </summary>
    /// <param name="fileName">The embedded file's name.</param>
    /// <param name="data">The embedded file.</param>
    /// <returns>The stored bytes.</returns>
    internal static byte[] BuildPackage(string fileName, byte[] data)
    {
        using var native = new MemoryStream();
        using (var writer = new BinaryWriter(native, Encoding.ASCII, leaveOpen: true))
        {
            byte[] tempPath = Encoding.ASCII.GetBytes("C:\\Temp\\" + fileName + "\0");
            writer.Write((ushort)2);
            writer.Write(Encoding.ASCII.GetBytes(fileName + "\0"));
            writer.Write(Encoding.ASCII.GetBytes("C:\\Source\\" + fileName + "\0"));
            writer.Write((ushort)0);
            writer.Write((ushort)3);
            writer.Write(tempPath.Length);
            writer.Write(tempPath);
            writer.Write(data.Length);
            writer.Write(data);
        }

        return BuildEmbeddedObject("Packager Shell Object", "Package", native.ToArray());
    }

    /// <summary>
    /// Builds Access's OLE header and an MS-OLEDS embedded object of class
    /// <paramref name="className"/> holding <paramref name="native"/>, followed by an
    /// empty presentation object and a 4-byte trailer.
    /// </summary>
    /// <param name="displayName">The display name in Access's header.</param>
    /// <param name="className">The OLE class.</param>
    /// <param name="native">The native data.</param>
    /// <returns>The stored bytes.</returns>
    internal static byte[] BuildEmbeddedObject(string displayName, string className, byte[] native)
    {
        byte[] display = Encoding.ASCII.GetBytes(displayName + "\0");
        byte[] cls = Encoding.ASCII.GetBytes(className + "\0");
        using var ms = new MemoryStream();
        using (var writer = new BinaryWriter(ms, Encoding.ASCII, leaveOpen: true))
        {
            writer.Write((ushort)0x1C15);
            writer.Write((ushort)(20 + display.Length + cls.Length));
            writer.Write(2);
            writer.Write((ushort)display.Length);
            writer.Write((ushort)cls.Length);
            writer.Write((ushort)20);
            writer.Write((ushort)(20 + display.Length));
            writer.Write(uint.MaxValue);
            writer.Write(display);
            writer.Write(cls);
            writer.Write(0x0501);
            writer.Write(2);
            writer.Write(cls.Length);
            writer.Write(cls);
            writer.Write(0);
            writer.Write(0);
            writer.Write(native.Length);
            writer.Write(native);
            writer.Write(0x0501);
            writer.Write(0);
            writer.Write([0x00, 0xAD, 0x05, 0xFE]);
        }

        return ms.ToArray();
    }

    /// <summary>
    /// Every typed read API returns the stored bytes. Before, they unwrapped OLE
    /// Packages and cut off everything before the first file signature in the
    /// value's first 512 bytes.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="payload">The stored value.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndPayloads))]
    public async Task ReadApis_OleValue_ReturnStoredBytes(DatabaseFormat format, Payload payload)
    {
        byte[] stored = Build(payload);
        await using MemoryStream ms = await CreateDatabaseAsync(format, [[1, stored], [2, DBNull.Value]]);
        await using AccessReader reader = await OpenReaderAsync(ms);

        var reads = new Dictionary<string, byte[]?>(StringComparer.Ordinal)
        {
            ["Rows"] = (byte[]?)(await FirstAsync(reader.Rows("T", cancellationToken: Ct)))[1],
            ["Rows<T>"] = (await FirstAsync(reader.Rows<OleRow>("T", cancellationToken: Ct))).Blob,
            ["Rows<T>(predicate)"] = (await FirstAsync(reader.Rows<OleRow>("T", r => r.Id == 1, cancellationToken: Ct))).Blob,
            ["ReadTableAsync<T>"] = (await reader.ReadTableAsync<OleRow>("T", cancellationToken: Ct)).Single(r => r.Id == 1).Blob,
            ["Query<T>"] = (await reader.Query<OleRow>("T").Where(r => r.Id == 1).ToListAsync(Ct)).Single().Blob,
        };

        using (DataTable table = await reader.ReadTableAsync("T", cancellationToken: Ct))
        {
            reads["ReadTableAsync"] = (byte[])table.Rows.Cast<DataRow>().Single(r => (int)r["Id"] == 1)["Blob"];
        }

        using (DataTable table = await reader.ReadDataTableAsync("T", cancellationToken: Ct))
        {
            reads["ReadDataTableAsync"] = (byte[])table.Rows.Cast<DataRow>().Single(r => (int)r["Id"] == 1)["Blob"];
        }

        // Jet3 index seeks are not supported yet.
        if (format != DatabaseFormat.Jet3Mdb)
        {
            reads["FromIndex"] = (byte[]?)(await FirstAsync(reader.FromIndex("T", "PrimaryKey").WhereEquals(1).ToRowsAsync(Ct)))[1];
            reads["SeekRowsAsync"] = (byte[]?)(await FirstAsync(reader.SeekRowsAsync("T", "PrimaryKey", [1], Ct)))[1];
        }

        Assert.All(reads, read => Assert.True(stored.AsSpan().SequenceEqual(read.Value), $"{read.Key} returned {read.Value?.Length} bytes, not the {stored.Length} stored."));
    }

    /// <summary>
    /// The string APIs render the stored bytes as a data URI. The media type comes
    /// from a signature at the first byte only, so a PNG gets <c>image/png</c> and a
    /// value with "BM" after two other bytes gets <c>application/octet-stream</c>;
    /// both decode back to the stored bytes.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task StringApis_OleValue_ReturnDataUriOfStoredBytes(DatabaseFormat format)
    {
        byte[] png = [.. PngSignature, .. Enumerable.Range(0, 600).Select(i => unchecked((byte)(i * 7)))];
        byte[] prefixedBitmap = Build(Payload.PrefixedBitmapInline);
        await using MemoryStream ms = await CreateDatabaseAsync(format, [[1, png], [2, prefixedBitmap]]);
        await using AccessReader reader = await OpenReaderAsync(ms);

        string[] expected =
        [
            "1|data:image/png;base64," + Convert.ToBase64String(png),
            "2|data:application/octet-stream;base64," + Convert.ToBase64String(prefixedBitmap),
        ];

        var rows = new List<string>();
        await foreach (string[] row in reader.RowsAsStrings("T", cancellationToken: Ct))
        {
            rows.Add(row[0] + "|" + row[1]);
        }

        Assert.Equal(expected, rows.Order(StringComparer.Ordinal));

        using DataTable strings = await reader.ReadTableAsStringsAsync("T", cancellationToken: Ct);
        Assert.Equal(expected, strings.Rows.Cast<DataRow>().Select(r => r["Id"] + "|" + r["Blob"]).Order(StringComparer.Ordinal));

        using DataTable first = await reader.ReadFirstTableAsStringsAsync(cancellationToken: Ct);
        Assert.Equal(expected, first.Rows.Cast<DataRow>().Select(r => r["Id"] + "|" + r["Blob"]).Order(StringComparer.Ordinal));
    }

    private static byte[] Build(Payload payload)
    {
        byte[] prefix = [.. Encoding.ASCII.GetBytes("PREFIXPREFIXPREF"), .. new byte[40]];
        return payload switch
        {
            Payload.PrefixedBitmapInline => [0x00, 0x01, 0x42, 0x4D, .. Filler(36)],
            Payload.PackageInline => BuildPackage("note.txt", Encoding.ASCII.GetBytes("this is the embedded note")),
            Payload.PdfAfterPrefixSinglePage => [.. prefix, .. "%PDF-1.4 "u8, .. Filler(1000)],
            Payload.JpegAtOffset100Chained => [.. Filler(100), 0xFF, 0xD8, 0xFF, 0xE0, .. Filler(10_000)],
            Payload.PackageChained => BuildPackage("big.bin", Filler(9000)),
            _ => throw new ArgumentOutOfRangeException(nameof(payload)),
        };
    }

    private static byte[] Filler(int length)
    {
        byte[] bytes = new byte[length];
        for (int i = 0; i < bytes.Length; i++)
        {
            bytes[i] = (byte)('a' + (i % 26));
        }

        return bytes;
    }

    private static async Task<T> FirstAsync<T>(IAsyncEnumerable<T> source)
    {
        await foreach (T item in source)
        {
            return item;
        }

        throw new InvalidOperationException("The sequence is empty.");
    }

    private static async Task<MemoryStream> CreateDatabaseAsync(DatabaseFormat format, IEnumerable<object[]> rows)
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(ms, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync("T", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Blob", typeof(byte[]))], Ct);
            await writer.InsertRowsAsync("T", rows, Ct);
        }

        return ms;
    }

    private static async Task<AccessReader> OpenReaderAsync(MemoryStream ms)
    {
        ms.Position = 0;
        return await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
    }

    /// <summary>A row of <c>T</c>.</summary>
    [SuppressMessage("Performance", "CA1819:Properties should not return arrays", Justification = "The OLE column maps to a byte[].")]
    private sealed class OleRow
    {
        /// <summary>Gets or sets the key.</summary>
        public int Id { get; set; }

        /// <summary>Gets or sets the OLE value.</summary>
        public byte[]? Blob { get; set; }
    }
}
