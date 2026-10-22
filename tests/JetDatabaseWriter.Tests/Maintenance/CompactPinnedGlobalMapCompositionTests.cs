namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Prefix preservation accepts only concretely validated native global-map changes under shared read admission.</summary>
public sealed class CompactPinnedGlobalMapCompositionTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task PrefixComparison_IncludesEveryGlobalMapReadInItsAggregateAllowance(DatabaseFormat kind, bool reference)
    {
        byte[] original = await CreateAsync(kind);
        await using var immutable = new MemoryStream(original, writable: false);
        await using AccessReader source = await TestDatabase.OpenReaderAsync(immutable, cancellationToken: Ct);
        JetFormat format = source.Database.Format;
        await using var destination = new MemoryStream();
        await destination.WriteAsync(original, Ct);
        long appendPoint = source.Database.Pages.PageCount;
        if (reference)
        {
            byte[] page = await source.Database.Pages.ReadPageAsync(1, Ct);
            try
            {
                Assert.True(UsageMap.TryGetFirstRowBound(page, format.DataPage, format.PageSize, out RowBound row));
                appendPoint = Math.Max(appendPoint, (8L * (row.RowSize - Constants.UsageMap.InlineMapHeaderSize)) + 8);
            }
            finally
            {
                PageBuffers.Return(page);
            }

            destination.SetLength(appendPoint * format.PageSize);
        }

        await CompactGlobalMapInstaller.InstallAsync(source.Database.Pages, format, appendPoint, destination, new(), Ct);
        await using var reopened = new MemoryStream(destination.ToArray(), writable: false);
        CompactGlobalMapResult map = await CompactGlobalMapValidator.ValidateAsync(source.Database.Pages, format, appendPoint, reopened, new(), Ct);
        Assert.Equal(reference, map.BitmapPages > 0);
        long expectedReads = map.ReadBytes + (2 * (source.Database.Pages.PageCount - 1) * format.PageSize);
        var context = new CompactPinnedGlobalMapValidation(appendPoint, new());
        CompactPinnedPrefixValidatorResult result = await CompactPinnedPrefixValidator.ValidateAsync(
            source.Database.Pages, format, [], reopened, new() { MaxReadBytes = expectedReads }, Ct, context);
        Assert.Equal(expectedReads, result.ReadBytes);
        Assert.False(result.DatabaseValidated);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactPinnedPrefixValidator.ValidateAsync(
            source.Database.Pages, format, [], reopened, new() { MaxReadBytes = expectedReads - 1 }, Ct, context));
        Assert.Equal(original, immutable.ToArray());
    }

    [Theory]
    [InlineData("row")]
    [InlineData("surround")]
    [InlineData("append-point")]
    public async Task NativeMapContext_NeverExemptsUncheckedPageOneBytes(string damage)
    {
        byte[] original = await CreateAsync(DatabaseFormat.Jet4Mdb);
        await using var immutable = new MemoryStream(original, writable: false);
        await using AccessReader source = await TestDatabase.OpenReaderAsync(immutable, cancellationToken: Ct);
        JetFormat format = source.Database.Format;
        await using var destination = new MemoryStream();
        await destination.WriteAsync(original, Ct);
        long appendPoint = source.Database.Pages.PageCount;
        CompactGlobalMapResult installed = await CompactGlobalMapInstaller.InstallAsync(source.Database.Pages, format, appendPoint, destination, new(), Ct);
        byte[] bytes = destination.ToArray();
        if (damage == "row")
        {
            bytes[format.PageSize + installed.MapRowStart] ^= 1;
        }
        else if (damage == "surround")
        {
            bytes[format.PageSize + 8] ^= 1;
        }
        else if (damage != "append-point")
        {
            throw new ArgumentException("Unknown damage.", nameof(damage));
        }

        await using var reopened = new MemoryStream(bytes, writable: false);
        var context = new CompactPinnedGlobalMapValidation(damage == "append-point" ? appendPoint + 1 : appendPoint, new());
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactPinnedPrefixValidator.ValidateAsync(
            source.Database.Pages, format, [], reopened, new(), Ct, context));
    }

    private static async Task<byte[]> CreateAsync(DatabaseFormat kind)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, kind, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int))], cancellationToken: Ct);
        }

        return stream.ToArray();
    }
}
