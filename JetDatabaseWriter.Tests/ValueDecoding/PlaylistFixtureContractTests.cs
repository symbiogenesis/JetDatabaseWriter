namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Validates the benchmark's playlist contract without running any measurements.</summary>
public sealed class PlaylistFixtureContractTests
{
    private static readonly int[] Ids = [0, 7, 10, 833, 10_115];

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Verification_UsesRowIdentityAndAcceptsStoredOrder(bool largeSortOnly)
    {
        SnapshotPlaylistRow[] rows = Ids.AsEnumerable().Reverse().Select(id => Expected(id, largeSortOnly)).ToArray();

        PlaylistFixture.Verify(rows, largeSortOnly, Ids);

        rows[0].Title = "Changed playlist";
        InvalidOperationException mismatch = Assert.Throws<InvalidOperationException>(() => PlaylistFixture.Verify(rows, largeSortOnly, Ids));
        Assert.Contains("PlaylistID=10115", mismatch.Message, StringComparison.Ordinal);
        Assert.Contains("Title", mismatch.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Verification_RejectsMissingDuplicateAndCorruptDefinitions(bool largeSortOnly)
    {
        SnapshotPlaylistRow[] rows = Ids.Select(id => Expected(id, largeSortOnly)).ToArray();
        Assert.Throws<InvalidOperationException>(() => PlaylistFixture.Verify(rows[..^1], largeSortOnly, Ids));
        rows[^1] = rows[0];
        Assert.Throws<InvalidOperationException>(() => PlaylistFixture.Verify(rows, largeSortOnly, Ids));

        rows = Ids.Select(id => Expected(id, largeSortOnly)).ToArray();
        rows[1].Filter![0] ^= 1;
        InvalidOperationException mismatch = Assert.Throws<InvalidOperationException>(() => PlaylistFixture.Verify(rows, largeSortOnly, Ids));
        Assert.Contains("Filter", mismatch.Message, StringComparison.Ordinal);
        Assert.Contains("exactBytes=false", mismatch.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SparseAndLargeSortOnly_RoundTripEveryFieldThroughBothReaderPaths(bool largeSortOnly)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Playlists",
                [
                    new("PlaylistID", typeof(int)),
                    new("CreationDate", typeof(string), 40),
                    new("Favorite", typeof(bool)),
                    new("ArtistSort", typeof(string), 100),
                    new("Title", typeof(string), 100),
                    new("Filter", typeof(byte[])),
                    new("SortOrder", typeof(byte[])),
                    new("Random", typeof(bool)),
                    new("Max", typeof(int)),
                    new("Shuffle", typeof(bool)),
                    new("Reload", typeof(bool)),
                    new("PlayingTime", typeof(int)),
                    new("NumberOfTracks", typeof(short)),
                    new("Notes", typeof(string)),
                ],
                TestContext.Current.CancellationToken);
            var rows = Ids.Select(id =>
            {
                SnapshotPlaylistRow row = Expected(id, largeSortOnly);
                return new object[]
                {
                    id, row.CreationDate!, row.Favorite!, row.ArtistSort!, row.Title!, row.Filter!, row.SortOrder!,
                    row.Random!, row.Max!, row.Shuffle!, row.Reload!, row.PlayingTime!, row.NumberOfTracks!, new string('N', 4096),
                };
            }).ToList();
            await writer.InsertRowsAsync("Playlists", rows, TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
        var typed = new List<SnapshotPlaylistRow>();
        await foreach (SnapshotPlaylistRow row in reader.Rows<SnapshotPlaylistRow>("Playlists", cancellationToken: TestContext.Current.CancellationToken))
        {
            typed.Add(row);
        }

        var projected = new List<SnapshotPlaylistRow>();
        await foreach (SnapshotPlaylistRow row in reader.ReadPrivateField<ReaderServices>("services").Tables.RowsWithDecoder<SnapshotPlaylistRow>(
            "Playlists", forceProjection: true, progress: null, cancellationToken: TestContext.Current.CancellationToken))
        {
            projected.Add(row);
        }

        PlaylistFixture.Verify(typed, largeSortOnly, Ids);
        PlaylistFixture.Verify(projected, largeSortOnly, Ids);
        Assert.Equal(projected.Select(row => row.PlaylistID), typed.Select(row => row.PlaylistID));
        SnapshotPlaylistRow sortOnly = typed.Single(row => row.PlaylistID == 10);
        Assert.Null(sortOnly.Filter);
        byte[] bytes = Assert.IsType<byte[]>(sortOnly.SortOrder);
        Assert.Equal(largeSortOnly ? 4096 : 120, bytes.Length);
        Assert.Equal(largeSortOnly ? 10 : 11, bytes[0]);
        Assert.Equal(largeSortOnly ? 249 : 242, bytes[^1]);
    }

    private static SnapshotPlaylistRow Expected(int id, bool largeSortOnly)
    {
        SnapshotPlaylistRow row = PlaylistFixture.Metadata(id);
        (row.Filter, row.SortOrder) = PlaylistFixture.Definitions(id, largeSortOnly);
        return row;
    }
}
