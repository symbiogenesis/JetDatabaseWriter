namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.ComplexColumns.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Attachment wrapper variants characterized through native DAO creation and extraction.</summary>
public sealed class AttachmentWrapperDaoTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task NativeAttachmentWrappers_ExtensionlessAndEmptyFilesSurviveWriterAndCompactExtraction()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.AttachmentWrappers");
        AttachmentCase[] cases =
        [
            new("extensionless", [1, 2, 3], 1),
            new("empty", [], 1),
            new("empty.txt", [], 1),
            new("empty.jpg", [], 0),
            new("raw.jpg", [1, 2, 3], 0),
            new("upper.JPG", [1, 2, 3], 0),
            new("compressed.txt", Encoding.UTF8.GetBytes(new string('t', 1024)), 1),
            new("long.abcdefghij", [1, 2, 3], 1),
            new("upper.TXT", [1, 2, 3], 1),
            new("mixed.aBcD", [1, 2, 3], 1, "aBcD"),
        ];
        foreach (AttachmentCase item in cases)
        {
            await File.WriteAllBytesAsync(Path.Combine(session.WorkDir, item.FileName), item.Payload, Ct);
        }

        string inputPaths = string.Join(",", cases.Select(item => Quote(Path.Combine(session.WorkDir, item.FileName))));
        string create = $$"""
            $table = $db.CreateTableDef('Documents')
            $table.Fields.Append($table.CreateField('Id', 4))
            $table.Fields.Append($table.CreateField('Files', 101))
            $db.TableDefs.Append($table)
            $db.Execute('INSERT INTO [Documents] ([Id]) VALUES (1)', 128)
            $rs = $db.OpenRecordset('Documents')
            try {
                $rs.Edit()
                $child = $rs.Fields('Files').Value
                try {
                    foreach ($inputPath in @({{inputPaths}})) {
                        $child.AddNew()
                        $child.Fields('FileData').LoadFromFile($inputPath)
                        $child.Update()
                    }
                } finally { $child.Close() }
                $rs.Update()
            } finally { $rs.Close() }
            Write-Output 'NATIVE_ATTACHMENTS_CREATED'
            """;
        AssertDaoSuccess(session.RunDaoCreateDatabaseScript(session.SourcePath, ";LANGID=0x0409;CP=1252;COUNTRY=0", create, TimeSpan.FromMinutes(2)));
        await AssertStoredWrappersAsync(session.SourcePath, cases, writerCopies: false, session.WorkDir);

        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            var key = new Dictionary<string, object?> { ["Id"] = 1 };
            foreach (AttachmentCase item in cases)
            {
                await writer.AddAttachmentAsync("Documents", "Files", key, new AttachmentInput("writer-" + item.FileName, item.Payload) { FileType = item.ExplicitFileType }, Ct);
            }
        }

        await AssertStoredWrappersAsync(session.SourcePath, cases, writerCopies: true, session.WorkDir);
        await AssertExtractionAsync(session, session.SourcePath, "before-", cases);
        session.RunDaoCompact();
        await AssertStoredWrappersAsync(session.CompactedPath, cases, writerCopies: true, session.WorkDir);
        await AssertExtractionAsync(session, session.CompactedPath, "after-", cases);
    }

    [Theory]
    [InlineData("010000000E000000785EE363606060846206060000E20011", "", "")]
    [InlineData("0100000011000000785EE363606060846220C9C40C00011F0017", "", "010203")]
    [InlineData("0100000014000000785E1361606060046216202E61A800620606000A24017A", "txt", "")]
    [InlineData("00000000140000001400000001000000040000006A00700067000000", "jpg", "")]
    public void CapturedNativeWrappers_DecodeEmptyAndExtensionlessFiles(string wrapperHex, string extension, string payloadHex)
    {
        // Captured from DAO.DBEngine.120 (ACE 16.0) LoadFromFile; the guarded
        // native test independently regenerates these header/content variants.
        Assert.True(AttachmentWrapper.TryDecode(Convert.FromHexString(wrapperHex), out string actualExtension, out byte[] payload, cancellationToken: Ct));
        Assert.Equal(extension, actualExtension);
        Assert.Equal(Convert.FromHexString(payloadHex), payload);
    }

    private static async Task AssertExtractionAsync(AccessRoundTripSession session, string databasePath, string prefix, AttachmentCase[] cases)
    {
        string extract = $$"""
            $rs = $db.OpenRecordset('Documents')
            try {
                $child = $rs.Fields('Files').Value
                try {
                    while (!$child.EOF) {
                        $fileName = [string]$child.Fields('FileName').Value
                        $child.Fields('FileData').SaveToFile([IO.Path]::Combine({{Quote(session.WorkDir)}}, {{Quote(prefix)}} + $fileName))
                        $child.MoveNext()
                    }
                } finally { $child.Close() }
            } finally { $rs.Close() }
            Write-Output 'NATIVE_EXTRACTION_OK'
            """;
        AssertDaoSuccess(session.RunDaoDatabaseScript(databasePath, extract, TimeSpan.FromMinutes(2)));
        foreach (AttachmentCase item in cases)
        {
            Assert.Equal(item.Payload, await File.ReadAllBytesAsync(Path.Combine(session.WorkDir, prefix + item.FileName), Ct));
            Assert.Equal(item.Payload, await File.ReadAllBytesAsync(Path.Combine(session.WorkDir, prefix + "writer-" + item.FileName), Ct));
        }
    }

    private static async Task AssertStoredWrappersAsync(string path, AttachmentCase[] cases, bool writerCopies, string evidenceDirectory)
    {
        int flatId;
        await using (AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, Ct))
        {
            flatId = Assert.Single(await reader.GetComplexColumnsAsync("Documents", Ct)).FlatTableId;
            IReadOnlyList<AttachmentRecord> attachments = await reader.GetAttachmentsAsync("Documents", "Files", Ct);
            Assert.Equal(cases.Length * (writerCopies ? 2 : 1), attachments.Count);
            foreach (AttachmentCase item in cases)
            {
                AttachmentRecord native = Assert.Single(attachments, attachment => attachment.FileName == item.FileName);
                Assert.Equal(item.Payload, native.FileData);
                Assert.Equal(Path.GetExtension(item.FileName).TrimStart('.'), native.FileType);
                if (writerCopies)
                {
                    AttachmentRecord written = Assert.Single(attachments, attachment => attachment.FileName == "writer-" + item.FileName);
                    Assert.Equal(item.Payload, written.FileData);
                    Assert.Equal(native.FileType, written.FileType);
                }
            }
        }

        await using WriterHarness harness = await WriterHarness.OpenAsync(path, cancellationToken: Ct);
        long flatTdefPage = CatalogValueReader.TdefPageFromId(flatId);
        TableDef flat = Assert.IsType<TableDef>(await harness.Database.TableDefs.ReadTableDefAsync(flatTdefPage, Ct));
        int fileName = flat.FindColumnIndex("FileName");
        int fileData = flat.FindColumnIndex("FileData");
        foreach (LocatedRow row in await harness.Services.Snapshots.ReadRowsAsync(flatTdefPage, Ct))
        {
            string name = Assert.IsType<string>(row.Values[fileName]);
            AttachmentCase item = Assert.Single(cases, item => name == item.FileName || name == "writer-" + item.FileName);
            byte[] wrapped = Assert.IsType<byte[]>(row.Values[fileData]);
            AssertWrapper(wrapped, item);
            if (!writerCopies)
            {
                await File.WriteAllBytesAsync(Path.Combine(evidenceDirectory, "native-" + name + ".wrapper"), wrapped, Ct);
            }
        }
    }

    private static void AssertWrapper(byte[] wrapped, AttachmentCase item)
    {
        string extension = Path.GetExtension(item.FileName).TrimStart('.');
        Assert.Equal(item.TypeFlag, BinaryPrimitives.ReadInt32LittleEndian(wrapped));
        byte[] content = ReadContent(wrapped);
        int expectedHeaderLength = 12 + ((extension.Length + 1) * 2);
        Assert.Equal(expectedHeaderLength + item.Payload.Length, BinaryPrimitives.ReadInt32LittleEndian(wrapped.AsSpan(4)));
        Assert.Equal(expectedHeaderLength, BinaryPrimitives.ReadInt32LittleEndian(content));
        Assert.Equal(1, BinaryPrimitives.ReadInt32LittleEndian(content.AsSpan(4)));
        Assert.Equal(extension.Length + 1, BinaryPrimitives.ReadInt32LittleEndian(content.AsSpan(8)));
        Assert.Equal(extension + '\0', Encoding.Unicode.GetString(content, 12, expectedHeaderLength - 12));
        Assert.Equal(item.Payload, content[expectedHeaderLength..]);
        Assert.True(AttachmentWrapper.TryDecode(wrapped, out string decodedExtension, out byte[] payload, cancellationToken: Ct));
        Assert.Equal(extension, decodedExtension);
        Assert.Equal(item.Payload, payload);

        byte[] reencoded = AttachmentWrapper.Encode(extension, item.Payload);
        if (item.TypeFlag == 0)
        {
            Assert.Equal(wrapped, reencoded);
        }
        else
        {
            Assert.Equal(wrapped[..10], reencoded[..10]);
            Assert.Equal(wrapped[^4..], reencoded[^4..]);
            Assert.Equal(content, ReadContent(reencoded));
        }
    }

    private static byte[] ReadContent(byte[] wrapped)
    {
        if (BinaryPrimitives.ReadInt32LittleEndian(wrapped) == 0)
        {
            return wrapped[8..];
        }

        using var input = new MemoryStream(wrapped, 8, wrapped.Length - 8);
        using var zlib = new ZLibStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        zlib.CopyTo(output);
        return output.ToArray();
    }

    private static string Quote(string value) => AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(value);

    private static void AssertDaoSuccess(AccessRoundTripEnvironment.CompactResult result)
        => Assert.True(result.ExitCode == 0, $"DAO failed: {result.StdOut}\n{result.StdErr}");

    private sealed record AttachmentCase(string FileName, byte[] Payload, int TypeFlag, string? ExplicitFileType = null);
}
