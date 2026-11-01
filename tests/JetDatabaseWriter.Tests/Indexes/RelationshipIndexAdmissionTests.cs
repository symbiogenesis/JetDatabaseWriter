namespace JetDatabaseWriter.Tests.Indexes;

using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Verification;
using Xunit;

/// <summary>Boundary controls for the generated relationship index admission.</summary>
public sealed class RelationshipIndexAdmissionTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task TextRelationship_RetainsConservativeRebuild(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        await writer.CreateTableAsync("P", [new ColumnDefinition("Code", typeof(string), maxLength: 20)], [new IndexDefinition("PK_P", "Code") { IsPrimaryKey = true }], ct);
        await writer.CreateTableAsync("C", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Code", typeof(string), maxLength: 20)], [new IndexDefinition("PK_C", "Id") { IsPrimaryKey = true }], ct);
        await writer.InsertRowAsync("P", ["parent"], ct);
        await writer.CreateRelationshipAsync(new RelationshipDefinition("FK", "P", "Code", "C", "Code"), ct);
        await writer.InsertRowAsync("C", [1, "parent"], ct);
        Assert.Equal("C1c unsupported foreign-key index form", writer.Services.Indexes.LastIncrementalBail);
        await writer.InsertRowAsync("C", [2, "parent"], ct);
        Assert.Equal("C1c unsupported foreign-key index form", writer.Services.Indexes.LastIncrementalBail);
        Assert.Equal(2, (await writer.Services.Snapshots.ReadRowsAsync((await writer.Services.Catalog.GetRequiredCatalogEntryAsync("C", ct)).TDefPage, ct)).Count);
    }

    [Fact]
    public void PageMembership_RefusesMissingRootStaleMapDuplicatesAndNonpositivePages()
    {
        Assert.True(IndexMutationKernel.ValidateIndexPageSet(12, [4, 12, 19], [4, 12, 19]));
        Assert.False(IndexMutationKernel.ValidateIndexPageSet(13, [4, 12, 19], [4, 12, 19]));
        Assert.False(IndexMutationKernel.ValidateIndexPageSet(12, [4, 12, 19], [4, 12, 20]));
        Assert.False(IndexMutationKernel.ValidateIndexPageSet(12, [4, 12, 19], [4, 12]));
        Assert.False(IndexMutationKernel.ValidateIndexPageSet(12, [4, 12, 12], [4, 12, 12]));
        Assert.False(IndexMutationKernel.ValidateIndexPageSet(12, [0, 12], [0, 12]));
        Assert.False(IndexMutationKernel.ValidateIndexPageSet(12, [12, 4], [12, 4]));
        Assert.False(IndexMutationKernel.ValidateIndexPageSet(12, [], []));
    }

    [Fact]
    public void RelationshipKey_AdmitsNativeInt32FlagsAndRefusesUnqualifiedForms()
    {
        Assert.True(IndexMutationKernel.AdmitRelationshipKey(4, 1, true, true, false, 0));
        Assert.True(IndexMutationKernel.AdmitRelationshipKey(4, 1, true, true, false, 0x89));
        Assert.False(IndexMutationKernel.AdmitRelationshipKey(10, 1, true, false, false, 0x80));
        Assert.False(IndexMutationKernel.AdmitRelationshipKey(4, 2, true, true, false, 0x80));
        Assert.False(IndexMutationKernel.AdmitRelationshipKey(4, 1, false, true, false, 0x80));
        Assert.False(IndexMutationKernel.AdmitRelationshipKey(4, 1, true, true, true, 0x80));
        Assert.False(IndexMutationKernel.AdmitRelationshipKey(4, 1, true, true, false, 0x84));
        Assert.False(IndexMutationKernel.AdmitRelationshipKey(4, 1, true, true, false, 0x90));
    }
}
