namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Creating enforced relationships validates existing rows before writing.</summary>
/// <param name="db">Caches the fixture files.</param>
public sealed class CreateRelationshipExistingRowsTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every format in every write mode.</summary>
    /// <returns>The format and write-mode pairs.</returns>
    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndModes() => ForeignKeyTestDatabase.FormatsAndModes();

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task ExistingOrphan_IsRejectedWithoutChangingBytes(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateAsync(db, format, [[1, "one"]], [[1, 1, "valid"], [2, 99, "orphan"]]);
        byte[] before = ms.ToArray();
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_C_P", "P", "Id", "C", "ParentId"), Ct));
                Assert.Contains("FK_C_P", exception.Message, StringComparison.Ordinal);
                Assert.Equal(before, ms.ToArray());
            });
        }

        Assert.Equal(before, ms.ToArray());
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task UnenforcedRelationship_AllowsExistingOrphan(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateAsync(db, format, [], [[1, 99, "orphan"]]);
        await using AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode);
        await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_C_P", "P", "Id", "C", "ParentId") { EnforceReferentialIntegrity = false }, Ct));
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task ExistingNullAndMatchingKeys_AreAccepted(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateAsync(db, format, [[1, "one"]], [[1, 1, "valid"], [2, null, "null"]]);
        await using AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode);
        await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_C_P", "P", "Id", "C", "ParentId"), Ct));
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task ExistingTextKeys_UseDatabaseCollationAndCompositeBoundaries(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync("P", [new ColumnDefinition("First", typeof(string), 40), new ColumnDefinition("Second", typeof(string), 40)], Ct);
            await writer.CreateTableAsync("C", [new ColumnDefinition("First", typeof(string), 40), new ColumnDefinition("Second", typeof(string), 40)], Ct);
            await writer.InsertRowAsync("P", ["ABC", "a|S:b"], Ct);
            await writer.InsertRowsAsync("C", [["abc   ", "A|s:B"], [null, "absent"]], Ct);
        }

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_C_P", "P", ["First", "Second"], "C", ["First", "Second"]), Ct));
        }
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task CompositeKeys_WithAmbiguousTextSeparators_AreRejected(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync("P", [new ColumnDefinition("First", typeof(string), 40), new ColumnDefinition("Second", typeof(string), 40)], Ct);
            await writer.CreateTableAsync("C", [new ColumnDefinition("First", typeof(string), 40), new ColumnDefinition("Second", typeof(string), 40)], Ct);
            await writer.InsertRowAsync("P", ["a|S:b", "c"], Ct);
            await writer.InsertRowAsync("C", ["a", "b|S:c"], Ct);
        }

        byte[] before = ms.ToArray();
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_C_P", "P", ["First", "Second"], "C", ["First", "Second"]), Ct)));
        }

        Assert.Equal(before, ms.ToArray());
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task ExistingKeysInsertedInCurrentTransaction_AreVisible(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateAsync(db, format, [], []);
        await using AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode);
        await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
        {
            await writer.InsertRowAsync("P", [1, "one"], Ct);
            await writer.InsertRowAsync("C", [1, 1, "valid"], Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_C_P", "P", "Id", "C", "ParentId"), Ct);
        });
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task SelfRelationship_ExistingMatchingAndNullKeys_AreAccepted(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateAsync(db, format, [], [[1, null, "root"], [2, 1, "child"], [3, 3, "self"]]);
        await using AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode);
        await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_C_C", "C", "Id", "C", "ParentId"), Ct));
    }
}
