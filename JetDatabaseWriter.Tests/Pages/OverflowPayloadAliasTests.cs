namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Distinct slot identities cannot authorize destroying payload bytes still used by another live row.</summary>
public sealed class OverflowPayloadAliasTests
{
    private const string TableName = "Items";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task DeleteOverflow_PayloadSharedThroughDistinctSlot_RefusesWithoutChangingBytes(DatabaseFormat format, bool ordinaryAlias)
    {
        byte[] image = await SyntheticOverflowRows.CreateTableAsync(format, TableName, 300, primaryKey: false, Ct);
        SyntheticOverflowRow moved = await SyntheticOverflowRows.MoveRowAsync(image, TableName, OverflowRowLayout.CrossPage, Ct);
        var profile = JetFormat.ForNewDatabase(format);
        int dataOffset = checked((int)(moved.DataPage * profile.PageSize));
        int aliases = Ru16(image, dataOffset + profile.DataPage.NumRows);
        Wu16(image, dataOffset + profile.DataPage.RowsStart + (aliases * 2), moved.DataStart | (ordinaryAlias ? 0 : Constants.DataPage.DeletedRowFlag));
        Wu16(image, dataOffset + profile.DataPage.NumRows, aliases + 1);
        Wu16(image, dataOffset + 2, moved.DataStart - profile.DataPage.RowsStart - ((aliases + 1) * 2));
        if (ordinaryAlias)
        {
            int definitionOffset = checked(Ri32(image, dataOffset + profile.DataPage.TDefOff) * profile.PageSize);
            Wi32(image, definitionOffset + profile.TDef.NumRows, Ri32(image, definitionOffset + profile.TDef.NumRows) + 1);
        }

        if (!ordinaryAlias)
        {
            int headerOffset = checked((int)(moved.HeaderPage * profile.PageSize));
            byte[] headerPage = image.AsSpan(headerOffset, profile.PageSize).ToArray();
            RowBound secondHeader = DataPageRows.EnumerateLiveRowBounds(profile, headerPage).First();
            Array.Clear(image, headerOffset + secondHeader.RowStart, secondHeader.RowSize);
            UsageMap.WritePointer(image, headerOffset + secondHeader.RowStart, aliases, moved.DataPage);
            Wu16(image, headerOffset + profile.DataPage.RowsStart + (secondHeader.RowIndex * 2), secondHeader.RowStart | Constants.DataPage.OverflowRowFlag);
        }

        await using var stream = new MemoryStream();
        await stream.WriteAsync(image, Ct);
        await using AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        CatalogEntry? table = await writer.Services.Catalog.GetCatalogEntryAsync(TableName, Ct);
        Assert.NotNull(table);
        LocatedRow row = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(table.TDefPage, Ct), candidate => candidate.Location.PageNumber == moved.HeaderPage && candidate.Location.RowIndex == moved.HeaderRow);
        int id = Assert.IsType<int>(row.Values[0]);
        Assert.Equal(2, (await writer.Services.Snapshots.ReadRowsAsync(table.TDefPage, Ct)).Count(candidate => (int)candidate.Values[0] == id));
        byte[] original = stream.ToArray();
        await Assert.ThrowsAsync<JetCorruptDataException>(() => writer.DeleteRowsAsync(TableName, "Id", id, Ct).AsTask());
        Assert.Equal(original, stream.ToArray());
    }
}
