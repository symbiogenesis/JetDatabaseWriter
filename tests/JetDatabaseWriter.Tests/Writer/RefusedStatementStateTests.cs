namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

/// <summary>
/// A statement refused by validation, a constraint or its row producer must
/// leave the file bytes and the writer's auxiliary state exactly as before it
/// ran: the next successful statements produce the same bytes, AutoNumber
/// values, complex-item identities and page placement as when the refused
/// statement never ran.
/// </summary>
public sealed class RefusedStatementStateTests
{
    private static readonly string LongNote = new('n', 5000);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every refusal on every format that supports it, with and without an enclosing explicit transaction.</summary>
    public static TheoryData<DatabaseFormat, string, bool> Cases
    {
        get
        {
            var cases = new TheoryData<DatabaseFormat, string, bool>();
            foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
            {
                foreach (string refusal in Refusals)
                {
                    if (format != DatabaseFormat.AceAccdb && refusal.StartsWith("Complex", StringComparison.Ordinal))
                    {
                        continue;
                    }

                    cases.Add(format, refusal, false);
                    cases.Add(format, refusal, true);
                }
            }

            return cases;
        }
    }

    private static string[] Refusals =>
    [
        "InsertUniqueMidBatch",
        "InsertForeignKeyMidBatch",
        "InsertValidationMidBatch",
        "InsertRequiredMidBatch",
        "InsertProducerFailure",
        "InsertProducerCancelled",
        "UpdateUniqueConflict",
        "UpdateAutoNumberUniqueConflict",
        "UpdateForeignKey",
        "UpdateValidation",
        "DeleteRestricted",
        "CreateUniqueIndexOverDuplicates",
        "CreateRelationshipOverOrphans",
        "SetTableRuleViolated",
        "AddRequiredColumn",
        "DropRelatedColumn",
        "RenameTableOverExisting",
        "ComplexAttachmentMissingParent",
        "ComplexMultiValueWrongType",
        "ComplexMultiValueMissingItem",
        "ComplexInsertProducerFailure",
    ];

    /// <summary>Gets savepoint and outer-transaction rollback workloads across every format.</summary>
    public static TheoryData<DatabaseFormat, string, bool> RollbackScopeCases
    {
        get
        {
            var cases = new TheoryData<DatabaseFormat, string, bool>();
            string[] workloads =
            [
                "UpdateAndCascade",
                "DeleteAndCascade",
                "SchemaRewriteColumns",
                "SchemaRewriteIndexesAndRules",
                "ComplexSchemaAndItems",
            ];
            foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
            {
                foreach (string workload in workloads)
                {
                    if (format != DatabaseFormat.AceAccdb && workload.StartsWith("Complex", StringComparison.Ordinal))
                    {
                        continue;
                    }

                    cases.Add(format, workload, false);
                    cases.Add(format, workload, true);
                }
            }

            return cases;
        }
    }

    /// <summary>
    /// A refused statement followed by successful work produces exactly the
    /// bytes of the same successful work without the refusal.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="refusal">The refused statement.</param>
    /// <param name="explicitTransaction">Whether both runs execute inside an explicit transaction.</param>
    /// <returns>The test completion.</returns>
    [Theory]
    [MemberData(nameof(Cases))]
    public async Task RefusedStatement_LeavesNoTrace(DatabaseFormat format, string refusal, bool explicitTransaction)
    {
        byte[] seed = await CreateSeedAsync(format);
        byte[] expected = await RunAsync(seed, format, refusal: null, explicitTransaction);
        byte[] actual = await RunAsync(seed, format, refusal, explicitTransaction);
        AssertSameBytes(expected, actual, format);
    }

    /// <summary>
    /// Rolling back an outer transaction or a nested savepoint that executed
    /// updates, cascade deletes, schema rewrites or complex-column mutations
    /// leaves the exact database bytes and auxiliary state as when that scope
    /// never ran.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="workload">The rolled-back mutation workload.</param>
    /// <param name="nestedSavepoint">Whether to roll back a nested savepoint inside an outer transaction.</param>
    /// <returns>The test completion.</returns>
    [Theory]
    [MemberData(nameof(RollbackScopeCases))]
    public async Task RolledBackScope_LeavesNoTrace(DatabaseFormat format, string workload, bool nestedSavepoint)
    {
        byte[] seed = await CreateSeedAsync(format);
        byte[] expected = await RunScopeAsync(seed, format, workload: null, nestedSavepoint);
        byte[] actual = await RunScopeAsync(seed, format, workload, nestedSavepoint);
        AssertSameBytes(expected, actual, format);
    }

    /// <summary>
    /// Updating an AutoNumber column (directly or through a cascaded key update)
    /// advances the session's next generated AutoNumber when committed and
    /// restores the exact prior sequence when rolled back.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>The test completion.</returns>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task AutoNumberUpdate_CommitAdvancesAndRollbackRestoresSequence(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync("Parent", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
            _ = await writer.InsertRowsAsync("Parent", [[1], [2]], Ct);
            await writer.CreateTableAsync(
                "Child",
                [
                    new ColumnDefinition("ChildKey", typeof(int)) { IsAutoIncrement = true, IsPrimaryKey = true },
                    new ColumnDefinition("Label", typeof(string), 40),
                ],
                Ct);
            await writer.InsertRowAsync("Child", [DBNull.Value, "c1"], Ct);
            await writer.InsertRowAsync("Child", [DBNull.Value, "c2"], Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_Child_Parent", "Parent", "Id", "Child", "ChildKey") { CascadeUpdates = true }, Ct);

            await using JetTransaction outer = await writer.BeginTransactionAsync(Ct);
            await using (JetTransaction savepoint = await writer.BeginTransactionAsync(Ct))
            {
                _ = await writer.UpdateRowsAsync("Parent", RowCriteria.Where("Id", 2), new RowValues { ["Id"] = 40 }, Ct);
                await savepoint.RollbackAsync(Ct);
            }

            await writer.InsertRowAsync("Parent", [3], Ct);
            await writer.InsertRowAsync("Child", [DBNull.Value, "c3"], Ct);

            _ = await writer.UpdateRowsAsync("Parent", RowCriteria.Where("Id", 3), new RowValues { ["Id"] = 25 }, Ct);
            await writer.InsertRowAsync("Parent", [26], Ct);
            await writer.InsertRowAsync("Child", [DBNull.Value, "c26"], Ct);
            await outer.CommitAsync(Ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        using DataTable table = await reader.ReadTableAsync("Child", cancellationToken: Ct);
        string[] actual = [.. table.Rows.Cast<DataRow>().OrderBy(r => Convert.ToInt32(r["ChildKey"], System.Globalization.CultureInfo.InvariantCulture)).Select(r => $"{r["ChildKey"]}:{r["Label"]}")];
        Assert.Equal(["1:c1", "2:c2", "25:c3", "26:c26"], actual);
    }

    /// <summary>
    /// Encrypting, changing the password of, and decrypting a database after
    /// updates, deletes and schema rewrites preserves all state and refuses
    /// invalid conversions without altering bytes or leaving sidecars.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>The test completion.</returns>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task EncryptionConversionLifecycle_PreservesMutatedDatabaseAndRollbackState(DatabaseFormat format)
    {
        string directory = Path.Combine(Path.GetTempPath(), $"EncryptionLifecycle_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        string path = Path.Combine(directory, format == DatabaseFormat.AceAccdb ? "db.accdb" : "db.mdb");
        try
        {
            byte[] seed = await CreateSeedAsync(format);
            await File.WriteAllBytesAsync(path, seed, Ct);
            var writerOptions = new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false };

            await using (AccessWriter writer = await AccessWriter.OpenAsync(path, writerOptions, Ct))
            {
                await writer.DeleteRowsAsync("O", RowCriteria.Where("Id", 2), Ct);
                await writer.CreateIndexAsync("O", new IndexDefinition("PK_O", "Id") { IsPrimaryKey = true }, Ct);
                await writer.AddColumnAsync("O", new ColumnDefinition("Extra", typeof(int)) { DefaultValue = 10 }, Ct);
                await writer.UpdateRowsAsync("T", RowCriteria.Where("Code", 2), new RowValues { ["Note"] = "before-encrypt", ["Score"] = 12 }, Ct);
            }

            await AccessDatabaseEncryption.EncryptAsync(path, "FirstPass123".AsMemory(), options: writerOptions, cancellationToken: Ct);
            byte[] encryptedBytes = await File.ReadAllBytesAsync(path, Ct);

            await Assert.ThrowsAsync<JetOperationException>(async () =>
                await AccessDatabaseEncryption.EncryptAsync(path, "SecondPass123".AsMemory(), options: writerOptions, cancellationToken: Ct));
            await Assert.ThrowsAsync<JetAccessDeniedException>(async () =>
                await AccessDatabaseEncryption.ChangePasswordAsync(path, "WrongPass".AsMemory(), "SecondPass123".AsMemory(), writerOptions, Ct));
            await Assert.ThrowsAsync<JetAccessDeniedException>(async () =>
                await AccessDatabaseEncryption.DecryptAsync(path, "WrongPass".AsMemory(), writerOptions, Ct));
            Assert.Equal(encryptedBytes, await File.ReadAllBytesAsync(path, Ct));

            await AccessDatabaseEncryption.ChangePasswordAsync(path, "FirstPass123".AsMemory(), "SecondPass123".AsMemory(), writerOptions, Ct);
            var secondOptions = new AccessWriterOptions("SecondPass123") { UseLockFile = false, UseByteRangeLocks = false };

            await using (AccessWriter writer = await AccessWriter.OpenAsync(path, secondOptions, Ct))
            {
                await using JetTransaction tx = await writer.BeginTransactionAsync(Ct);
                await Assert.ThrowsAnyAsync<Exception>(async () => await RefuseAsync(writer, "UpdateUniqueConflict"));
                await using (JetTransaction savepoint = await writer.BeginTransactionAsync(Ct))
                {
                    await MutateScopeAsync(writer, "UpdateAndCascade");
                    await savepoint.RollbackAsync(Ct);
                }

                await FollowAsync(writer, format);
                await tx.CommitAsync(Ct);
            }

            await AccessDatabaseEncryption.DecryptAsync(path, "SecondPass123".AsMemory(), writerOptions, Ct);
            byte[] decryptedBytes = await File.ReadAllBytesAsync(path, Ct);
            await Assert.ThrowsAsync<JetOperationException>(async () =>
                await AccessDatabaseEncryption.DecryptAsync(path, "SecondPass123".AsMemory(), writerOptions, Ct));
            Assert.Equal(decryptedBytes, await File.ReadAllBytesAsync(path, Ct));
            Assert.Single(Directory.GetFiles(directory));

            await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, Ct);
            Assert.Equal(5, await reader.GetRealRowCountAsync("T", Ct));
            Assert.Equal(1, await reader.GetRealRowCountAsync("Later", Ct));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private static void AssertSameBytes(byte[] expected, byte[] actual, DatabaseFormat format)
    {
        Assert.Equal(expected.Length, actual.Length);
        int pageSize = JetFormat.ForNewDatabase(format).PageSize;
        var differing = new List<string>();
        for (int offset = 0; offset < expected.Length; offset += pageSize)
        {
            int length = Math.Min(pageSize, expected.Length - offset);
            if (!expected.AsSpan(offset, length).SequenceEqual(actual.AsSpan(offset, length)))
            {
                int first = 0;
                while (expected[offset + first] == actual[offset + first])
                {
                    first++;
                }

                differing.Add($"page {offset / pageSize} (first byte 0x{first:X})");
            }
        }

        Assert.True(differing.Count == 0, "Pages differ: " + string.Join(", ", differing));
    }

    private static async Task<byte[]> RunAsync(byte[] seed, DatabaseFormat format, string? refusal, bool explicitTransaction)
    {
        await using var stream = new MemoryStream();
        await stream.WriteAsync(seed, Ct);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false }, leaveOpen: true, cancellationToken: Ct))
        {
            JetTransaction? transaction = explicitTransaction ? await writer.BeginTransactionAsync(Ct) : null;
            await using (transaction)
            {
                // Warm every cache the refusal could disturb before it runs.
                await writer.UpdateRowsAsync("T", RowCriteria.Where("Code", 2), new RowValues { ["Note"] = "warm" }, Ct);
                await writer.InsertRowAsync("C", [DBNull.Value, 1, 50], Ct);

                if (refusal is not null)
                {
                    Exception error = await Assert.ThrowsAnyAsync<Exception>(async () => await RefuseAsync(writer, refusal));
                    Assert.True(error is IJetException or OperationCanceledException or InvalidDataException or FormatException, error.ToString());
                    Assert.False(transaction is { IsRolledBack: true } or { IsCommitted: true });
                }

                await FollowAsync(writer, format);
                if (transaction is not null)
                {
                    await transaction.CommitAsync(Ct);
                }
            }
        }

        byte[] bytes = stream.ToArray();
        await NormalizeLaterCatalogDatesAsync(bytes);
        return bytes;
    }

    private static async Task<byte[]> RunScopeAsync(byte[] seed, DatabaseFormat format, string? workload, bool nestedSavepoint)
    {
        await using var stream = new MemoryStream();
        await stream.WriteAsync(seed, Ct);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false }, leaveOpen: true, cancellationToken: Ct))
        {
            if (nestedSavepoint)
            {
                await using JetTransaction outer = await writer.BeginTransactionAsync(Ct);
                await writer.UpdateRowsAsync("T", RowCriteria.Where("Code", 2), new RowValues { ["Note"] = "warm" }, Ct);
                await writer.InsertRowAsync("C", [DBNull.Value, 1, 50], Ct);

                if (workload is not null)
                {
                    await using JetTransaction inner = await writer.BeginTransactionAsync(Ct);
                    await MutateScopeAsync(writer, workload);
                    await inner.RollbackAsync(Ct);
                    Assert.True(inner.IsRolledBack);
                    Assert.False(outer.IsRolledBack);
                }

                await FollowAsync(writer, format);
                await outer.CommitAsync(Ct);
            }
            else
            {
                await writer.UpdateRowsAsync("T", RowCriteria.Where("Code", 2), new RowValues { ["Note"] = "warm" }, Ct);
                await writer.InsertRowAsync("C", [DBNull.Value, 1, 50], Ct);

                if (workload is not null)
                {
                    await using JetTransaction tx = await writer.BeginTransactionAsync(Ct);
                    await MutateScopeAsync(writer, workload);
                    await tx.RollbackAsync(Ct);
                    Assert.True(tx.IsRolledBack);
                }

                await FollowAsync(writer, format);
            }
        }

        byte[] bytes = stream.ToArray();
        await NormalizeLaterCatalogDatesAsync(bytes);
        return bytes;
    }

    private static async ValueTask MutateScopeAsync(AccessWriter writer, string workload)
    {
        var first = new Dictionary<string, object?> { ["Id"] = 1 };
        switch (workload)
        {
            case "UpdateAndCascade":
                _ = await writer.UpdateRowsAsync("T", RowCriteria.Where("Code", 1), new RowValues { ["Note"] = "temp-short", ["Score"] = 77 }, Ct);
                _ = await writer.UpdateRowsAsync("T", RowCriteria.Where("Code", 2), new RowValues { ["Note"] = LongNote + "temp" }, Ct);
                _ = await writer.UpdateRowsAsync("T", RowCriteria.Where("Code", 3), new RowValues { ["Id"] = 500 }, Ct);
                _ = await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 3), new RowValues { ["Id"] = 30 }, Ct);
                break;
            case "DeleteAndCascade":
                await writer.DropRelationshipAsync("FK_C_P", Ct);
                await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_C_P", "P", "Id", "C", "ParentId") { CascadeUpdates = true, CascadeDeletes = true }, Ct);
                _ = await writer.DeleteRowsAsync("P", RowCriteria.Where("Id", 1), Ct);
                _ = await writer.DeleteRowsAsync("T", RowCriteria.Where("Code", 3), Ct);
                break;
            case "SchemaRewriteColumns":
                await writer.AddColumnAsync("T", new ColumnDefinition("Extra", typeof(int)) { DefaultValue = 42, ValidationRule = static v => v is int i && i > 0 }, Ct);
                await writer.RenameColumnAsync("T", "Score", "Points", Ct);
                await writer.DropColumnAsync("O", "ParentId", Ct);
                await writer.InsertRowAsync("T", [DBNull.Value, 90, "rewritten", 5, DbDefault.Value], Ct);
                break;
            case "SchemaRewriteIndexesAndRules":
                await writer.CreateIndexAsync("T", new IndexDefinition("IX_Temp", "Score"), Ct);
                await writer.RenameIndexAsync("T", "IX_Temp", "IX_Renamed", Ct);
                await writer.DropIndexAsync("T", "IX_Renamed", Ct);
                await writer.SetTableValidationRuleAsync("T", new TableValidationRule("[Score] >= 0") { ValidationText = "nonnegative" }, Ct);
                await writer.RenameTableAsync("O", "Orphans", Ct);
                await writer.RenameRelationshipAsync("FK_C_P", "FK_Renamed", Ct);
                break;
            case "ComplexSchemaAndItems":
                await writer.AddColumnAsync("D", new ColumnDefinition("MoreFiles", typeof(byte[])) { IsAttachment = true }, Ct);
                await writer.RenameColumnAsync("D", "Tags", "Labels", Ct);
                await writer.DropColumnAsync("D", "Files", Ct);
                await writer.AddMultiValueItemAsync("D", "Labels", first, 99, Ct);
                _ = await writer.DeleteRowsAsync("D", RowCriteria.Where("Id", 1), Ct);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(workload), workload, "Unknown rollback workload.");
        }
    }

    private static async Task NormalizeLaterCatalogDatesAsync(byte[] bytes)
    {
        await using var stream = new MemoryStream(bytes, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        TableDef? msys = await reader.Database.TableDefs.ReadTableDefAsync(2, Ct);
        Assert.NotNull(msys);
        ColumnInfo? nameCol = msys.FindColumn("Name");
        ColumnInfo? dateCreateCol = msys.FindColumn("DateCreate");
        ColumnInfo? dateUpdateCol = msys.FindColumn("DateUpdate");
        Assert.NotNull(nameCol);
        Assert.NotNull(dateCreateCol);
        Assert.NotNull(dateUpdateCol);

        int matched = 0;
        await reader.Database.OwnedPages.ForEachLiveTableRowAsync(
            2,
            (row, _) =>
            {
                string name = ScalarColumnReader.DecodeSimpleColumnValue(reader.Database.Format, row.Page, row.Location.RowStart, row.Location.RowSize, nameCol);
                if (string.Equals(name, "Later", StringComparison.Ordinal))
                {
                    int pageOffset = checked((int)row.Location.PageNumber * reader.Database.Format.PageSize);
                    int fixedStart = pageOffset + row.Location.RowStart + reader.Database.Format.RowFields.NumCols;
                    bytes.AsSpan(fixedStart + dateCreateCol.FixedOff, 8).Clear();
                    bytes.AsSpan(fixedStart + dateUpdateCol.FixedOff, 8).Clear();
                    matched++;
                }

                return new ValueTask<bool>(true);
            },
            Ct);
        Assert.Equal(1, matched);
    }

    private static async Task FollowAsync(AccessWriter writer, DatabaseFormat format)
    {
        _ = await writer.InsertRowsAsync("T", [[DBNull.Value, 20, LongNote, 4], [DBNull.Value, 21, "short", 5]], Ct);
        await writer.InsertRowAsync("C", [DBNull.Value, 2, 60], Ct);
        _ = await writer.UpdateRowsAsync("T", RowCriteria.Where("Code", 1), new RowValues { ["Note"] = LongNote + "x", ["Score"] = 9 }, Ct);
        _ = await writer.DeleteRowsAsync("C", RowCriteria.Where("Tag", 50), Ct);
        _ = await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 3), new RowValues { ["Id"] = 4 }, Ct);
        await writer.CreateTableAsync("Later", [new ColumnDefinition("Id", typeof(int)) { IsAutoIncrement = true, IsPrimaryKey = true }, new ColumnDefinition("Name", typeof(string), 40)], Ct);
        await writer.InsertRowAsync("Later", [DBNull.Value, "later"], Ct);
        await writer.CreateIndexAsync("T", new IndexDefinition("IX_Score", "Score"), Ct);
        if (format == DatabaseFormat.AceAccdb)
        {
            var key = new Dictionary<string, object?> { ["Id"] = 1 };
            await writer.AddAttachmentAsync("D", "Files", key, new AttachmentInput("b.txt", [4, 5, 6]), Ct);
            await writer.AddMultiValueItemAsync("D", "Tags", key, 7, Ct);
            await writer.InsertRowAsync("D", [2, DBNull.Value, DBNull.Value], Ct);
            await writer.AddMultiValueItemAsync("D", "Tags", new Dictionary<string, object?> { ["Id"] = 2 }, 8, Ct);
        }
    }

    private static async ValueTask RefuseAsync(AccessWriter writer, string refusal)
    {
        var missing = new Dictionary<string, object?> { ["Id"] = 99 };
        var first = new Dictionary<string, object?> { ["Id"] = 1 };
        switch (refusal)
        {
            case "InsertUniqueMidBatch":
                _ = await writer.InsertRowsAsync("T", [[DBNull.Value, 10, LongNote, 1], [DBNull.Value, 11, LongNote, 1], [DBNull.Value, 1, LongNote, 1]], Ct);
                break;
            case "InsertForeignKeyMidBatch":
                _ = await writer.InsertRowsAsync("C", [[DBNull.Value, 1, 70], [DBNull.Value, 2, 71], [DBNull.Value, 99, 72]], Ct);
                break;
            case "InsertValidationMidBatch":
                _ = await writer.InsertRowsAsync("T", [[DBNull.Value, 10, LongNote, 1], [DBNull.Value, 11, LongNote, 1], [DBNull.Value, 12, LongNote, -1]], Ct);
                break;
            case "InsertRequiredMidBatch":
                _ = await writer.InsertRowsAsync("T", [[DBNull.Value, 10, LongNote, 1], [DBNull.Value, DBNull.Value, LongNote, 1]], Ct);
                break;
            case "InsertProducerFailure":
                _ = await writer.InsertRowsAsync("T", FailingRows(), Ct);
                break;
            case "InsertProducerCancelled":
                using (var cancellation = new CancellationTokenSource())
                {
                    _ = await writer.InsertRowsAsync("T", CancellingRows(cancellation), cancellation.Token);
                }

                break;
            case "UpdateUniqueConflict":
                _ = await writer.UpdateRowsAsync("T", RowCriteria.All(), new RowValues { ["Code"] = 7, ["Note"] = LongNote }, Ct);
                break;
            case "UpdateAutoNumberUniqueConflict":
                _ = await writer.UpdateRowsAsync("T", RowCriteria.All(), new RowValues { ["Id"] = 500 }, Ct);
                break;
            case "UpdateForeignKey":
                _ = await writer.UpdateRowsAsync("C", RowCriteria.All(), new RowValues { ["ParentId"] = 99 }, Ct);
                break;
            case "UpdateValidation":
                _ = await writer.UpdateRowsAsync("T", RowCriteria.All(), new RowValues { ["Score"] = -5, ["Note"] = LongNote }, Ct);
                break;
            case "DeleteRestricted":
                _ = await writer.DeleteRowsAsync("P", RowCriteria.All(), Ct);
                break;
            case "CreateUniqueIndexOverDuplicates":
                await writer.CreateIndexAsync("T", new IndexDefinition("UX_Score", "Score") { IsUnique = true }, Ct);
                break;
            case "CreateRelationshipOverOrphans":
                await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_O_P", "P", "Id", "O", "ParentId"), Ct);
                break;
            case "SetTableRuleViolated":
                await writer.SetTableValidationRuleAsync("T", new TableValidationRule("[Score] > 1"), Ct);
                break;
            case "AddRequiredColumn":
                await writer.AddColumnAsync("O", new ColumnDefinition("Needed", typeof(int)) { IsPrimaryKey = true }, Ct);
                break;
            case "DropRelatedColumn":
                await writer.DropColumnAsync("C", "ParentId", Ct);
                break;
            case "RenameTableOverExisting":
                await writer.RenameTableAsync("O", "P", Ct);
                break;
            case "ComplexAttachmentMissingParent":
                await writer.AddAttachmentAsync("D", "Files", missing, new AttachmentInput("x.txt", [1]), Ct);
                break;
            case "ComplexMultiValueWrongType":
                await writer.AddMultiValueItemAsync("D", "Tags", first, "not a number", Ct);
                break;
            case "ComplexMultiValueMissingItem":
                await writer.ReplaceMultiValueItemAsync("D", "Tags", first, 999, 5, Ct);
                break;
            case "ComplexInsertProducerFailure":
                _ = await writer.InsertRowsAsync("D", FailingComplexRows(), Ct);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(refusal), refusal, "Unknown refusal.");
        }
    }

    private static IEnumerable<object?[]> FailingRows()
    {
        yield return [DBNull.Value, 10, LongNote, 1];
        yield return [DBNull.Value, 11, LongNote, 1];
        throw new InvalidDataException("Producer failed after two rows.");
    }

    private static IEnumerable<object?[]> CancellingRows(CancellationTokenSource cancellation)
    {
        yield return [DBNull.Value, 10, LongNote, 1];
        yield return [DBNull.Value, 11, LongNote, 1];
        cancellation.Cancel();
        yield return [DBNull.Value, 12, LongNote, 1];
    }

    private static IEnumerable<object?[]> FailingComplexRows()
    {
        yield return [10, DBNull.Value, DBNull.Value];
        yield return [11, DBNull.Value, DBNull.Value];
        throw new InvalidDataException("Producer failed after two complex rows.");
    }

    private static async Task<byte[]> CreateSeedAsync(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsAutoIncrement = true, IsPrimaryKey = true },
                    new ColumnDefinition("Code", typeof(int)) { IsNullable = false },
                    new ColumnDefinition("Note", typeof(string)),
                    new ColumnDefinition("Score", typeof(int)) { ValidationRuleExpression = ">= 0" },
                ],
                [new IndexDefinition("UX_Code", "Code") { IsUnique = true }],
                Ct);
            _ = await writer.InsertRowsAsync("T", [[DBNull.Value, 1, LongNote, 1], [DBNull.Value, 2, "two", 1], [DBNull.Value, 3, LongNote, 2]], Ct);
            await writer.CreateTableAsync("P", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
            _ = await writer.InsertRowsAsync("P", [[1], [2], [3]], Ct);
            await writer.CreateTableAsync(
                "C",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsAutoIncrement = true, IsPrimaryKey = true },
                    new ColumnDefinition("ParentId", typeof(int)),
                    new ColumnDefinition("Tag", typeof(int)),
                ],
                [new IndexDefinition("UX_Tag", "Tag") { IsUnique = true }],
                Ct);
            _ = await writer.InsertRowsAsync("C", [[DBNull.Value, 1, 1], [DBNull.Value, 2, 2]], Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_C_P", "P", "Id", "C", "ParentId") { CascadeUpdates = true }, Ct);
            await writer.CreateTableAsync("O", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("ParentId", typeof(int))], Ct);
            _ = await writer.InsertRowsAsync("O", [[1, 1], [2, 42]], Ct);
            if (format == DatabaseFormat.AceAccdb)
            {
                await writer.CreateTableAsync(
                    "D",
                    [
                        new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                        new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true },
                        new ColumnDefinition("Tags", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(int) },
                    ],
                    Ct);
                await writer.InsertRowAsync("D", [1, DBNull.Value, DBNull.Value], Ct);
                var key = new Dictionary<string, object?> { ["Id"] = 1 };
                await writer.AddAttachmentAsync("D", "Files", key, new AttachmentInput("a.txt", [1, 2, 3]), Ct);
                await writer.AddMultiValueItemAsync("D", "Tags", key, 3, Ct);
            }
        }

        return stream.ToArray();
    }
}
