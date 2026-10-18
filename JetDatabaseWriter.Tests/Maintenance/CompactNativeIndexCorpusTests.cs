namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Maintenance;
using Xunit;

/// <summary>Checks compact discovery against stored native indexes beyond the catalog roots.</summary>
public sealed class CompactNativeIndexCorpusTests
{
    [Theory]
    [InlineData("FixedText/NativeFixedTextJet3Cp1252.mdb")]
    [InlineData("FixedText/NativeFixedTextJet4.mdb")]
    [InlineData("NorthwindTraders.accdb")]
    [InlineData("ComplexFields.accdb")]
    [InlineData("mdbtools/nwind.mdb")]
    public async Task NativeInventory_AllDiscoveredPhysicalIndexesCompleteRawAudit(string fixture)
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        byte[] image = await File.ReadAllBytesAsync(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", fixture), cancellationToken);
        await using var stream = new MemoryStream(image, writable: false);
        await using var file = DatabaseFile.ForReader(stream, image, ReadOnlyMemory<char>.Empty, string.Empty, leaveOpen: true, cancellationToken: cancellationToken);
        var roots = new HashSet<(long Owner, long Root)>();
        await CompactStorageInventory.ScanAsync(
            file.Pages,
            file.Format,
            file.TableDefs,
            new(),
            (record, _) =>
            {
                if (record.Role == "index-root")
                {
                    Assert.NotNull(record.OwnerTDefPage);
                    Assert.NotNull(record.TargetPageNumber);
                    roots.Add((record.OwnerTDefPage.Value, record.TargetPageNumber.Value));
                }

                return ValueTask.CompletedTask;
            },
            cancellationToken);
        Assert.NotEmpty(roots);
        int terminals = 0;
        foreach ((long owner, long root) in roots)
        {
            await CompactIndexAudit.ScanAsync(
                file.Pages,
                file.Format,
                owner,
                root,
                new(),
                (record, _) =>
                {
                    if (record.Kind == "index-terminal")
                    {
                        Assert.True(record.NavigableGraphAcyclic);
                        Assert.True(record.EntryEdgeFencesValidated);
                        Assert.False(record.TreeCompletenessProven);
                        terminals++;
                    }

                    Assert.True(record.IsPinned);
                    return ValueTask.CompletedTask;
                },
                cancellationToken);
        }

        Assert.Equal(roots.Count, terminals);
    }
}
