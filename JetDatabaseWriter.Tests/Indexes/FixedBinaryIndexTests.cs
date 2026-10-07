namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Fixed Binary uniqueness, writes and seeks use persisted zero padding.</summary>
public sealed class FixedBinaryIndexTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task FixedBinary_InsertUpdateDeleteAndSeekNormalizeKeys(DatabaseFormat databaseFormat)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, databaseFormat, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await CreateSamplesAsync(writer);
            await writer.InsertRowAsync("Samples", [1, new byte[] { 1 }], TestContext.Current.CancellationToken);
            byte[] snapshot = stream.ToArray();
            JetConstraintException duplicate = await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowAsync("Samples", [2, new byte[] { 1, 0, 0, 0 }], TestContext.Current.CancellationToken));
            Assert.Equal(JetErrorCode.UniqueViolation, duplicate.ErrorCode);
            Assert.Equal(snapshot, stream.ToArray());
            Assert.Equal(1, await writer.UpdateRowsAsync("Samples", "Data", new byte[] { 1 }, new Dictionary<string, object?> { ["Data"] = new byte[] { 2 } }, TestContext.Current.CancellationToken));
        }

        stream.Position = 0;
        await using (AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            Assert.Empty(await reader.SeekRowsAsync("Samples", "UX_Data", [new byte[] { 1 }], TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken));
            Assert.Single(await reader.SeekRowsAsync("Samples", "UX_Data", [new byte[] { 2 }], TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken));
            Assert.Single(await reader.SeekRowsAsync("Samples", "UX_Data", [new byte[] { 2, 0, 0, 0 }], TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken));
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            Assert.Equal(1, await writer.DeleteRowsAsync("Samples", "Data", new byte[] { 2 }, TestContext.Current.CancellationToken));
        }

        stream.Position = 0;
        await using AccessReader empty = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        Assert.Empty(await empty.SeekRowsAsync("Samples", "UX_Data", [new byte[] { 2 }], TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken));
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task FixedBinary_DaoSeeksPersistedPaddedKeyAndRefusesDuplicate()
    {
        await using AccessRoundTripSession session = await AccessRoundTripSession.CreateDaoAccdbAsync(TestContext.Current.CancellationToken);
        await using (AccessWriter writer = await session.OpenWriterAsync(TestContext.Current.CancellationToken))
        {
            await CreateSamplesAsync(writer);
            await writer.InsertRowAsync("Samples", [1, new byte[] { 1 }], TestContext.Current.CancellationToken);
        }

        const string script =
            """
            $rs = $db.OpenRecordset('Samples')
            try {
                $rs.Index = 'UX_Data'
                $short = [byte[]](1)
                $full = [byte[]](1,0,0,0)
                $rs.Seek('=', $short)
                if (!$rs.NoMatch) { throw 'DAO unexpectedly normalized a raw short Binary seek key.' }
                $rs.Seek('=', $full)
                if ($rs.NoMatch) { throw 'Padded Binary seek failed.' }
                $rs.AddNew()
                $rs.Fields('Id').Value = 2
                $name = 'Data'
                $rs.Fields($name).Value = $full
                $accepted = $false
                try { $rs.Update(); $accepted = $true }
                catch { if ($engine.Errors.Item(0).Number -ne 3022) { throw }; Write-Output 'DUPLICATE=REFUSED'; $rs.CancelUpdate() }
                if ($accepted) { throw 'Duplicate Binary was accepted.' }
            } finally { $rs.Close() }
            """;
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoDatabaseScriptThenCompact(script, TimeSpan.FromMinutes(1));
        Assert.True(result.ExitCode == 0, $"DAO failed: {result.StdOut}\n{result.StdErr}");
        Assert.Contains("DUPLICATE=REFUSED", result.StdOut, StringComparison.Ordinal);
        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        Assert.Single(await reader.SeekRowsAsync("Samples", "UX_Data", [new byte[] { 1 }], TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken));
    }

    private static async Task CreateSamplesAsync(AccessWriter writer)
        => await writer.CreateTableAsync("Samples", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Data", typeof(byte[]), 4)], [new IndexDefinition("UX_Data", ["Data"]) { IsUnique = true }], TestContext.Current.CancellationToken);
}
