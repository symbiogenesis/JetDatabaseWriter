namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Catalog mutations refuse an incomplete leaf payload instead of discarding its unreadable suffix.</summary>
public sealed class CatalogLeafPayloadPreservationTests
{
    /// <summary>Gets each schema operation, malformed payload and writer transaction mode.</summary>
    public static TheoryData<string, string, WriteMode> Cases
    {
        get
        {
            var cases = new TheoryData<string, string, WriteMode>();
            foreach (string operation in new[] { "add", "drop", "rename" })
            {
                foreach (string fault in new[] { "short-tail", "split-trailer", "invalid-prefix", "missing-sentinel", "past-end-boundary" })
                {
                    foreach (WriteMode mode in Enum.GetValues<WriteMode>())
                    {
                        cases.Add(operation, fault, mode);
                    }
                }
            }

            return cases;
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task NativeJet3Catalog_IncompleteLeafRefusesSchemaRewriteWithoutChangingBytes(string operation, string fault, WriteMode mode)
    {
        byte[] image = await File.ReadAllBytesAsync(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "FixedText", "NativeFixedTextJet3Cp1252.mdb"), TestContext.Current.CancellationToken);
        var format = JetFormat.FromHeader(image);
        byte[] definition = image.AsSpan(2 * format.PageSize, format.PageSize).ToArray();
        TDefCounts counts = TDefCodec.ReadCounts(format, definition);
        int descriptorStart = IndexCatalogReader.LocateRealIdxDescStart(format, definition, counts.ColumnCount, counts.RealIndexCount);
        Assert.True(descriptorStart >= 0);
        int root = JetTypeInfo.Ri32(definition, format.Index.FirstDpAbsoluteOffset(format.Index.RealIdxPhysOffset(descriptorStart, 0)));
        byte[] leaf = image.AsSpan(root * format.PageSize, format.PageSize).ToArray();
        List<IndexEntry> entries = IndexPageCodec.DecodeLeafEntries(format.IndexPage, leaf, format.PageSize);
        Assert.NotEmpty(entries);
        leaf = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, entries, enablePrefixCompression: false);
        int payloadEnd = format.PageSize - JetTypeInfo.Ru16(leaf, 2);
        switch (fault)
        {
            case "short-tail":
                leaf[payloadEnd] = 0xAA;
                leaf[payloadEnd + 1] = 0xBB;
                leaf[payloadEnd + 2] = 0xCC;
                SetBoundary(format.IndexPage, leaf, payloadEnd + 3);
                JetTypeInfo.Wu16(leaf, 2, format.PageSize - payloadEnd - 3);
                break;
            case "split-trailer":
                leaf[payloadEnd] = 0xAA;
                leaf[payloadEnd + 1] = 0xBB;
                leaf[payloadEnd + 2] = 0xCC;
                leaf[payloadEnd + 3] = 0xDD;
                leaf[payloadEnd + 4] = 0xEE;
                leaf[payloadEnd + 5] = 0xFF;
                leaf[payloadEnd + 6] = 0xAA;
                int bit = payloadEnd + 5 - format.IndexPage.FirstEntryOffset;
                leaf[format.IndexPage.BitmaskOffset + (bit / 8)] |= checked((byte)(1 << (bit % 8)));
                SetBoundary(format.IndexPage, leaf, payloadEnd + 7);
                JetTypeInfo.Wu16(leaf, 2, format.PageSize - payloadEnd - 7);
                break;
            case "missing-sentinel":
                int sentinel = payloadEnd - format.IndexPage.FirstEntryOffset;
                leaf[format.IndexPage.BitmaskOffset + (sentinel / 8)] &= unchecked((byte)~(1 << (sentinel % 8)));
                break;
            case "past-end-boundary":
                SetBoundary(format.IndexPage, leaf, payloadEnd + 1);
                break;
            default:
                leaf[format.IndexPage.PrefLenOffset] = byte.MaxValue;
                break;
        }

        leaf.CopyTo(image, root * format.PageSize);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(image, TestContext.Current.CancellationToken);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, TestContext.Current.CancellationToken))
        {
            JetCorruptDataException exception = await Assert.ThrowsAsync<JetCorruptDataException>(() => WriteModes.RunAsync(
                writer,
                mode,
                async () =>
                {
                    switch (operation)
                    {
                        case "add":
                            await writer.AddColumnAsync("FixedText", new ColumnDefinition("Added", typeof(int)), TestContext.Current.CancellationToken);
                            break;
                        case "drop":
                            await writer.DropColumnAsync("FixedText", "Spare", TestContext.Current.CancellationToken);
                            break;
                        default:
                            await writer.RenameColumnAsync("FixedText", "Spare", "Renamed", TestContext.Current.CancellationToken);
                            break;
                    }
                },
                TestContext.Current.CancellationToken));
            Assert.Equal("MSysObjects", exception.ErrorInfo.TableName);
            Assert.Equal(root, exception.ErrorInfo.PageNumber);
        }

        Assert.Equal(image, stream.ToArray());
    }

    private static void SetBoundary(IndexPageLayout layout, byte[] page, int offset)
    {
        int bit = offset - layout.FirstEntryOffset;
        page[layout.BitmaskOffset + (bit / 8)] |= checked((byte)(1 << (bit % 8)));
    }
}
