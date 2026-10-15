namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Compares relationship TDEF metadata with independently authored native output.</summary>
/// <param name="output">Captures native and writer descriptor statistics.</param>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class DaoRelationshipMetadataTests(ITestOutputHelper output)
{
    private static readonly TimeSpan DaoTimeout = TimeSpan.FromMinutes(2);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>DAO120 verifies Jet4 and ACE relationship emission and rename metadata.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="attributes">The native cascade flags.</param>
    /// <returns>The asynchronous comparison.</returns>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet4Mdb, 0)]
    [InlineData(DatabaseFormat.Jet4Mdb, 256)]
    [InlineData(DatabaseFormat.Jet4Mdb, 4096)]
    [InlineData(DatabaseFormat.Jet4Mdb, 4352)]
    [InlineData(DatabaseFormat.AceAccdb, 0)]
    [InlineData(DatabaseFormat.AceAccdb, 256)]
    [InlineData(DatabaseFormat.AceAccdb, 4096)]
    [InlineData(DatabaseFormat.AceAccdb, 4352)]
    public Task Dao120_CreateAndRename_RetainNativeMetadata(DatabaseFormat format, int attributes) =>
        this.CompareAsync(format, attributes);

    /// <summary>DAO3.6 verifies Access97 relationship emission and rename metadata.</summary>
    /// <param name="attributes">The native cascade flags.</param>
    /// <returns>The asynchronous comparison.</returns>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [InlineData(0)]
    [InlineData(256)]
    [InlineData(4096)]
    [InlineData(4352)]
    public Task Dao36_CreateAndRename_RetainNativeMetadata(int attributes) =>
        this.CompareAsync(DatabaseFormat.Jet3Mdb, attributes);

    /// <summary>DAO120 identifies the preferred parent when an earlier unique index also covers the key.</summary>
    /// <param name="format">The database format.</param>
    /// <returns>The asynchronous comparison.</returns>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public Task Dao120_EarlierUniqueParentIndex_RetainsNativeChoice(DatabaseFormat format) =>
        this.CompareAsync(format, 4352, uniqueParentIndex: true);

    /// <summary>DAO3.6 identifies the preferred parent when an earlier unique index also covers the key.</summary>
    /// <returns>The asynchronous comparison.</returns>
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    public Task Dao36_EarlierUniqueParentIndex_RetainsNativeChoice() =>
        this.CompareAsync(DatabaseFormat.Jet3Mdb, 4352, uniqueParentIndex: true);

    private static string RunNative(AccessRoundTripSession session, DatabaseFormat format, string script)
    {
        if (format == DatabaseFormat.Jet3Mdb)
        {
            PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, session.WorkDir, DaoTimeout);
            Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, result.StandardOutput + result.StandardError);
            return result.StandardOutput;
        }

        AccessRoundTripEnvironment.CompactResult native = session.RunDaoEngineScript(script, DaoTimeout);
        Assert.True(native.ExitCode == 0, native.StdOut + native.StdErr);
        return native.StdOut;
    }

    private static async Task<TableSnapshot> ReadSnapshotAsync(string path, string table)
    {
        await using AccessReader harness = await TestDatabase.OpenReaderAsync(path, cancellationToken: Ct);
        CatalogEntry entry = await harness.GetCatalogEntryAsync(table, Ct) ?? throw new InvalidOperationException(table);
        TableDef definition = await harness.ReadTableDefAsync(entry.TDefPage, Ct) ?? throw new InvalidOperationException(table);
        byte[] td = (await harness.Database.TableDefs.ReadTDefBytesAsync(entry.TDefPage, Ct))!;
        JetFormat format = harness.Database.Format;
        int count = Ri32(td, format.TDef.NumRealIdx);
        int start = IndexCatalogReader.LocateRealIdxDescStart(format, td, definition.Columns.Count, count);
        IndexSectionAnchors anchors = format.Index.GetIndexSection(start, count, Ri32(td, format.TDef.NumIdx));
        List<IndexMetadata> indexes = IndexCatalogReader.ReadMetadata(format, td, definition.Columns);
        var snapshots = new List<IndexSnapshot>();
        for (int i = 0; i < indexes.Count; i++)
        {
            IndexMetadata index = indexes[i];
            Assert.True(format.Index.TryReadLogicalEntry(td, anchors.LogIdxStart, i, out LogicalIdxEntry logical));
            Assert.True(format.Index.TryReadRealIdxSlot(td, start, index.RealIndexNumber, out RealIdxSlot physical));
            int statistics = format.TDef.BlockEnd + (index.RealIndexNumber * format.TDef.RealIdxEntrySz);
            snapshots.Add(new IndexSnapshot(
                index.Name,
                index.IndexNumber,
                index.RealIndexNumber,
                index.Kind,
                index.RelatedIndexNumber,
                index.RelatedTablePage,
                td[logical.FieldsOffset + 8],
                logical.CascadeUps,
                logical.CascadeDels,
                physical.Flags,
                Convert.ToHexString(td.AsSpan(statistics, format.TDef.RealIdxEntrySz)),
                string.Join(",", index.Columns.Select(column => column.Name + (column.IsAscending ? "+" : "-")))));
        }

        return new TableSnapshot(checked((int)entry.TDefPage), snapshots);
    }

    private static void AssertPair(TableSnapshot parent, TableSnapshot child, int attributes, string relationship)
    {
        IndexSnapshot parentFk = Assert.Single(parent.Indexes, index => index.Kind == IndexKind.ForeignKey);
        IndexSnapshot childFk = Assert.Single(child.Indexes, index => index.Kind == IndexKind.ForeignKey);
        Assert.StartsWith(".r", parentFk.Name, StringComparison.Ordinal);
        Assert.Equal(relationship, childFk.Name);
        Assert.Equal(child.Page, parentFk.RelatedPage);
        Assert.Equal(parent.Page, childFk.RelatedPage);
        Assert.Equal(childFk.Number, parentFk.RelatedNumber);
        Assert.Equal(parentFk.Number, childFk.RelatedNumber);
        Assert.Equal(1, parentFk.Role);
        Assert.Equal(2, childFk.Role);
        Assert.Contains(parent.Indexes, index => index.RealNumber == parentFk.RealNumber
            && (index.Kind == IndexKind.PrimaryKey || (index.Flags & 1) != 0));
        foreach (IndexSnapshot index in new[] { parentFk, childFk })
        {
            Assert.Equal((attributes & 256) != 0, (index.CascadeUpdates & 1) != 0);
            Assert.Equal((attributes & 4096) != 0, (index.CascadeDeletes & 1) != 0);
        }
    }

    private static void AssertEquivalent(TableSnapshot expected, TableSnapshot actual)
    {
        Assert.Equal(expected.Indexes.Select(index => index.Name), actual.Indexes.Select(index => index.Name));
        Assert.Equal(
            expected.Indexes.Select(index => index with { Statistics = string.Empty, RelatedPage = 0 }),
            actual.Indexes.Select(index => index with { Statistics = string.Empty, RelatedPage = 0 }));
    }

    private static void AssertPreservedStatistics(TableSnapshot original, TableSnapshot actual)
    {
        foreach (IndexSnapshot index in original.Indexes)
        {
            Assert.Equal(index.Statistics, Assert.Single(actual.Indexes, candidate => candidate.Name == index.Name).Statistics);
        }
    }

    private async Task CompareAsync(DatabaseFormat format, int attributes, bool uniqueParentIndex = false)
    {
        await using var session = AccessRoundTripSession.CreateEmpty(databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        int version = format switch
        {
            DatabaseFormat.Jet3Mdb => 32,
            DatabaseFormat.Jet4Mdb => 64,
            DatabaseFormat.AceAccdb => 128,
            _ => throw new ArgumentOutOfRangeException(nameof(format)),
        };
        string initializeRelationshipStorage = string.Empty;
        if (format == DatabaseFormat.Jet3Mdb)
        {
            // Keep native mapped relationship storage populated with an unenforced
            // relation. First allocation of Jet3 system owned maps is a separate gap.
            initializeRelationshipStorage = """
                $seed=$db.CreateRelation('StorageSeed','P','C',2)
                $field=$seed.CreateField('Id'); $field.ForeignName='ParentId'; $seed.Fields.Append($field)
                $db.Relations.Append($seed)
                """;
        }

        string setup = $$"""
            $db = $engine.CreateDatabase({{source}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{version}})
            try {
                $db.Execute('CREATE TABLE P (Id LONG, [Note] LONG)',128)
                $db.Execute('CREATE {{(uniqueParentIndex ? "UNIQUE " : string.Empty)}}INDEX A_P_Id ON P (Id)',128)
                $db.Execute('CREATE UNIQUE INDEX PK_P ON P (Id) WITH PRIMARY',128)
                $db.Execute('CREATE TABLE C (Id LONG CONSTRAINT PK_C PRIMARY KEY, ParentId LONG, [Note] LONG)',128)
                $db.Execute('CREATE INDEX A_C_Note ON C ([Note])',128)
                $db.Execute('CREATE INDEX Z_C_Note ON C ([Note],Id)',128)
                foreach($row in @('1,10','2,20','3,30')) { $db.Execute(('INSERT INTO P VALUES ('+$row+')'),128) }
                foreach($row in @('10,1,100','11,1,101','12,2,102','13,3,103')) { $db.Execute(('INSERT INTO C VALUES ('+$row+')'),128) }
                {{initializeRelationshipStorage}}
            } finally { $db.Close() }
            """;
        RunNative(session, format, setup);
        string writerPath = session.CreateDatabasePath("writer");
        File.Copy(session.SourcePath, writerPath);
        string nativeZzPath = session.CreateDatabasePath("native_ZZ_Rel");
        string nativeAaPath = session.CreateDatabasePath("native_AA_Rel");
        File.Copy(session.SourcePath, nativeZzPath);
        File.Copy(session.SourcePath, nativeAaPath);
        TableSnapshot originalParent = await ReadSnapshotAsync(writerPath, "P");
        TableSnapshot originalChild = await ReadSnapshotAsync(writerPath, "C");
        Assert.Equal(2, originalParent.Indexes.Count);
        Assert.Equal(3, originalChild.Indexes.Count);
        string createRelationship = $$"""
            foreach($case in @(
                @({{source}},'Rel'),
                @({{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(nativeZzPath)}},'ZZ_Rel'),
                @({{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(nativeAaPath)}},'AA_Rel'))) {
                $db=$engine.OpenDatabase($case[0])
                try {
                    $rel=$db.CreateRelation($case[1],'P','C',{{attributes}})
                    $field=$rel.CreateField('Id'); $field.ForeignName='ParentId'; $rel.Fields.Append($field)
                    $db.Relations.Append($rel)
                } finally { $db.Close() }
            }
            """;
        RunNative(session, format, createRelationship);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(writerPath, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("Rel", "P", "Id", "C", "ParentId")
                {
                    CascadeUpdates = (attributes & 256) != 0,
                    CascadeDeletes = (attributes & 4096) != 0,
                },
                Ct);
        }

        TableSnapshot nativeParent = await ReadSnapshotAsync(session.SourcePath, "P");
        TableSnapshot nativeChild = await ReadSnapshotAsync(session.SourcePath, "C");
        TableSnapshot writerParent = await ReadSnapshotAsync(writerPath, "P");
        TableSnapshot writerChild = await ReadSnapshotAsync(writerPath, "C");
        foreach ((string label, TableSnapshot snapshot) in new[] { ("native-parent", nativeParent), ("native-child", nativeChild), ("writer-parent", writerParent), ("writer-child", writerChild) })
        {
            output.WriteLine(label + ": " + string.Join("; ", snapshot.Indexes));
        }

        AssertPair(nativeParent, nativeChild, attributes, "Rel");
        AssertPair(writerParent, writerChild, attributes, "Rel");
        AssertEquivalent(nativeParent, writerParent);
        AssertEquivalent(nativeChild, writerChild);
        AssertPreservedStatistics(originalParent, writerParent);
        AssertPreservedStatistics(originalChild, writerChild);
        IndexSnapshot nativeFk = Assert.Single(nativeChild.Indexes, index => index.Kind == IndexKind.ForeignKey);
        IndexSnapshot writerFk = Assert.Single(writerChild.Indexes, index => index.Kind == IndexKind.ForeignKey);
        string nativeStatistics = format == DatabaseFormat.Jet3Mdb ? "0400000003000000" : "040000000300000000000000";
        Assert.Equal(nativeStatistics, nativeFk.Statistics);
        Assert.Equal(new string('0', nativeStatistics.Length), writerFk.Statistics);
        foreach (string renamed in new[] { "ZZ_Rel", "AA_Rel" })
        {
            string oldName = renamed == "ZZ_Rel" ? "Rel" : "ZZ_Rel";

            // DAO refuses changing an appended Relation.Name. Independently
            // authored oracles establish the native ordering of each new name,
            // including A_C_Note before AA_Rel, which differs from ordinal order.
            await using (AccessWriter writer = await AccessWriter.OpenAsync(writerPath, new AccessWriterOptions { UseLockFile = false }, Ct))
            {
                await writer.RenameRelationshipAsync(oldName, renamed, Ct);
            }

            TableSnapshot renamedParent = await ReadSnapshotAsync(writerPath, "P");
            TableSnapshot renamedChild = await ReadSnapshotAsync(writerPath, "C");
            AssertPair(renamedParent, renamedChild, attributes, renamed);
            Assert.Equal(writerParent.Indexes, renamedParent.Indexes);
            Assert.Equal(writerChild.Indexes.OrderBy(index => index.Number).Select(index => index.Statistics), renamedChild.Indexes.OrderBy(index => index.Number).Select(index => index.Statistics));
            AssertEquivalent(await ReadSnapshotAsync(session.SourcePath, "P"), renamedParent);
            TableSnapshot expectedChild = await ReadSnapshotAsync(renamed == "ZZ_Rel" ? nativeZzPath : nativeAaPath, "C");
            AssertEquivalent(expectedChild, renamedChild);
        }

        string writerLiteral = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(writerPath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        string nativeCompactedPath = session.CreateDatabasePath("native_compacted");
        string nativeCompacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(nativeCompactedPath);
        string verifyAndCompact = $$"""
            $db=$engine.OpenDatabase({{writerLiteral}})
            try {
                if($db.Relations.Item('AA_Rel').Attributes -ne {{attributes}}){throw 'Relationship flags changed.'}
                $rs=$db.OpenRecordset('SELECT Id FROM C ORDER BY Id',4)
                try {
                    $ids=@(while(-not $rs.EOF){$rs.Fields('Id').Value;$rs.MoveNext()})
                    if(($ids -join ',') -ne '10,11,12,13'){throw 'Native ordered read lost existing children.'}
                } finally {$rs.Close()}
                $refused=$false
                try { $db.Execute('INSERT INTO C VALUES (99,999,999)',128) } catch {
                    if(@($engine.Errors | ForEach-Object {$_.Number}) -notcontains 3201){throw}
                    $refused=$true
                }
                if(-not $refused){throw 'Native engine accepted an orphan.'}
                $db.Execute('INSERT INTO C VALUES (99,1,999)',128)
                $db.Execute('DELETE FROM C WHERE Id=99',128)
                if(({{attributes}} -band 256) -ne 0) {
                    $db.Execute('UPDATE P SET Id=5 WHERE Id=1',128)
                    $rs=$db.OpenRecordset('SELECT COUNT(*) AS N FROM C WHERE ParentId=5',4)
                    try {if($rs.Fields('N').Value -ne 2){throw 'Native cascade update lost children.'} } finally {$rs.Close()}
                    $db.Execute('UPDATE P SET Id=1 WHERE Id=5',128)
                }
                if(({{attributes}} -band 4096) -ne 0) {
                    $workspace=$engine.Workspaces.Item(0)
                    $workspace.BeginTrans()
                    try {
                        $db.Execute('DELETE FROM P WHERE Id=1',128)
                        $rs=$db.OpenRecordset('SELECT COUNT(*) AS N FROM C',4)
                        try {if($rs.Fields('N').Value -ne 2){throw 'Native cascade delete lost unrelated children.'} } finally {$rs.Close()}
                    } finally {$workspace.Rollback()}
                }
                $rs=$db.OpenRecordset('SELECT COUNT(*) AS N FROM C',4)
                try { if($rs.Fields('N').Value -ne 4){throw 'Child count changed.'} } finally {$rs.Close()}
            } finally {$db.Close()}
            $engine.CompactDatabase({{writerLiteral}},{{compacted}})
            $engine.CompactDatabase({{source}},{{nativeCompacted}})
            Write-Output 'NATIVE_METADATA=ACCEPTED'
            """;
        string readback = RunNative(session, format, verifyAndCompact);
        Assert.Contains("NATIVE_METADATA=ACCEPTED", readback, StringComparison.Ordinal);
        TableSnapshot compactParent = await ReadSnapshotAsync(session.CompactedPath, "P");
        TableSnapshot compactChild = await ReadSnapshotAsync(session.CompactedPath, "C");
        AssertPair(compactParent, compactChild, attributes, "AA_Rel");
        Assert.Equal(
            writerParent.Indexes.Select(index => index.Name).OrderBy(name => name, StringComparer.OrdinalIgnoreCase),
            compactParent.Indexes.Select(index => index.Name).OrderBy(name => name, StringComparer.OrdinalIgnoreCase));
        Assert.Equal(
            (await ReadSnapshotAsync(writerPath, "C")).Indexes.Select(index => index.Name).OrderBy(name => name, StringComparer.OrdinalIgnoreCase),
            compactChild.Indexes.Select(index => index.Name).OrderBy(name => name, StringComparer.OrdinalIgnoreCase));
        Assert.Equal(
            (await ReadSnapshotAsync(nativeCompactedPath, "P")).Indexes.Select(index => (index.Name, index.Statistics))
                .OrderBy(index => index.Name, StringComparer.OrdinalIgnoreCase),
            compactParent.Indexes.Select(index => (index.Name, index.Statistics))
                .OrderBy(index => index.Name, StringComparer.OrdinalIgnoreCase));
        Assert.Equal(
            (await ReadSnapshotAsync(nativeCompactedPath, "C")).Indexes
                .Select(index => (Name: index.Name == "Rel" ? "AA_Rel" : index.Name, index.Statistics))
                .OrderBy(index => index.Name, StringComparer.OrdinalIgnoreCase),
            compactChild.Indexes.Select(index => (index.Name, index.Statistics))
                .OrderBy(index => index.Name, StringComparer.OrdinalIgnoreCase));
    }

    private sealed record TableSnapshot(int Page, List<IndexSnapshot> Indexes);

    private sealed record IndexSnapshot(
        string Name,
        int Number,
        int RealNumber,
        IndexKind Kind,
        int RelatedNumber,
        int RelatedPage,
        byte Role,
        byte CascadeUpdates,
        byte CascadeDeletes,
        byte Flags,
        string Statistics,
        string Columns);
}
