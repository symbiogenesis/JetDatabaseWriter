namespace JetDatabaseWriter.TestSupport;

using System;
using System.Collections.Generic;
using System.Globalization;

/// <summary>Pure playlist data and correctness checks shared by ordinary tests and hosted benchmarks.</summary>
internal static class PlaylistFixture
{
    /// <summary>Creates the expected scalar metadata for a stored identity.</summary>
    /// <param name="id">The stored identity.</param>
    /// <returns>The expected metadata.</returns>
    internal static SnapshotPlaylistRow Metadata(int id) => new()
    {
        PlaylistID = id,
        CreationDate = id % 13 == 0 ? null : "2026-09-" + ((id % 28) + 1).ToString("D2", CultureInfo.InvariantCulture),
        Favorite = id % 5 == 0,
        ArtistSort = id % 11 == 0 ? null : "Artist " + (id % 347).ToString(CultureInfo.InvariantCulture),
        Title = "Playlist " + id.ToString(CultureInfo.InvariantCulture),
        Random = id % 3 == 0,
        Max = id % 17 == 0 ? null : id % 500,
        Shuffle = id % 4 == 0,
        Reload = id % 6 == 0,
        PlayingTime = id % 19 == 0 ? null : id * 123,
        NumberOfTracks = id % 23 == 0 ? null : (short)(id % 1000),
    };

    /// <summary>Creates the exact opaque definition bytes for a stored identity.</summary>
    /// <param name="id">The stored identity.</param>
    /// <param name="largeSortOnly">Whether every playlist retains a 4 KiB sort definition.</param>
    /// <returns>The nullable filter and sort definitions.</returns>
    internal static (byte[]? Filter, byte[]? SortOrder) Definitions(int id, bool largeSortOnly)
    {
        if (largeSortOnly)
        {
            return (id % 7 == 0 ? Payload(id, 37) : null, Payload(id, 4096));
        }

        byte[]? filter = id % 7 == 0 ? Payload(id, 37) : null;
        byte[]? sort = id % 10 == 0 ? Payload(id + 1, 120) : null;
        return (filter, sort);
    }

    /// <summary>Verifies every identity, scalar and definition without assuming identity order equals stored order.</summary>
    /// <param name="rows">The retained snapshot in stored order.</param>
    /// <param name="largeSortOnly">The fixture's definition distribution.</param>
    /// <param name="expectedIds">The exact identities the fixture contains.</param>
    /// <exception cref="InvalidOperationException">An identity, count, scalar or definition differs.</exception>
    internal static void Verify(IReadOnlyList<SnapshotPlaylistRow> rows, bool largeSortOnly, IReadOnlyCollection<int> expectedIds)
    {
        if (rows.Count != expectedIds.Count)
        {
            throw new InvalidOperationException($"Expected {expectedIds.Count} playlists; got {rows.Count} (largeSortOnly={largeSortOnly}).");
        }

        var remaining = new HashSet<int>(expectedIds);
        for (int position = 0; position < rows.Count; position++)
        {
            SnapshotPlaylistRow actual = rows[position];
            int id = actual.PlaylistID;
            if (!remaining.Remove(id))
            {
                throw new InvalidOperationException($"Invalid or duplicate PlaylistID {id} at stored position {position} (largeSortOnly={largeSortOnly}).");
            }

            SnapshotPlaylistRow expected = Metadata(id);
            (byte[]? filter, byte[]? sort) = Definitions(id, largeSortOnly);
            var differences = new List<string>();
            Check(nameof(actual.CreationDate), actual.CreationDate, expected.CreationDate);
            Check(nameof(actual.Favorite), actual.Favorite, expected.Favorite);
            Check(nameof(actual.ArtistSort), actual.ArtistSort, expected.ArtistSort);
            Check(nameof(actual.Title), actual.Title, expected.Title);
            Check(nameof(actual.Random), actual.Random, expected.Random);
            Check(nameof(actual.Max), actual.Max, expected.Max);
            Check(nameof(actual.Shuffle), actual.Shuffle, expected.Shuffle);
            Check(nameof(actual.Reload), actual.Reload, expected.Reload);
            Check(nameof(actual.PlayingTime), actual.PlayingTime, expected.PlayingTime);
            Check(nameof(actual.NumberOfTracks), actual.NumberOfTracks, expected.NumberOfTracks);
            CheckBytes(nameof(actual.Filter), actual.Filter, filter);
            CheckBytes(nameof(actual.SortOrder), actual.SortOrder, sort);
            if (differences.Count != 0)
            {
                throw new InvalidOperationException($"PlaylistID={id}, stored position={position}, largeSortOnly={largeSortOnly}: {string.Join("; ", differences)}");
            }

            void Check<T>(string field, T value, T wanted)
            {
                if (!EqualityComparer<T>.Default.Equals(value, wanted))
                {
                    differences.Add($"{field}: actual={(object?)value ?? "<null>"}, expected={(object?)wanted ?? "<null>"}");
                }
            }

            void CheckBytes(string field, byte[]? value, byte[]? wanted)
            {
                bool equal = value is null ? wanted is null : wanted is not null && value.AsSpan().SequenceEqual(wanted);
                if (!equal)
                {
                    differences.Add($"{field}: actualLength={value?.Length.ToString(CultureInfo.InvariantCulture) ?? "null"}, expectedLength={wanted?.Length.ToString(CultureInfo.InvariantCulture) ?? "null"}, exactBytes=false");
                }
            }
        }
    }

    private static byte[] Payload(int id, int length)
    {
        byte[] bytes = new byte[length];
        for (int index = 0; index < bytes.Length; index++)
        {
            bytes[index] = (byte)((id + (index * 17)) & 255);
        }

        return bytes;
    }
}
