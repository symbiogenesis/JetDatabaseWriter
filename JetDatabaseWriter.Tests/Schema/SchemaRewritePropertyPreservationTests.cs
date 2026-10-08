namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// AddColumn, DropColumn and RenameColumn rebuild a table, and with it the
/// table's <c>MSysObjects.LvProp</c> blob. The rebuilt blob keeps every
/// property the original holds, byte for byte, apart from the target of a
/// dropped column, the new name of a renamed one, the writer-modelled
/// properties the rewrite changes (an expression that names a renamed
/// column), and the table-level <c>NameMap</c>, which is dropped.
/// </summary>
public sealed class SchemaRewritePropertyPreservationTests
{
    private const string NameMap = "NameMap";

    /// <summary>The schema rewrite a test runs.</summary>
    [SuppressMessage("Design", "CA1515:Consider making public types internal", Justification = "Theory parameters of public xUnit test methods must be public.")]
    public enum Rewrite
    {
        /// <summary><c>AddColumnAsync</c> of a new column.</summary>
        AddColumn = 0,

        /// <summary><c>DropColumnAsync</c> of a column.</summary>
        DropColumn = 1,

        /// <summary><c>RenameColumnAsync</c> of a column.</summary>
        RenameColumn = 2,
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>
    /// Gets the Access-authored tables, each with a column to rename and one to
    /// drop that no expression, rule or relationship names, in every write mode.
    /// </summary>
    /// <returns>The fixture, table, column and write-mode tuples.</returns>
    public static TheoryData<string, string, string, WriteMode> AccessAuthoredTables()
    {
        var data = new TheoryData<string, string, string, WriteMode>();
        (string Fixture, string Table, string Column)[] tables =
        [
            (TestDatabases.CalcFieldTestV2010, "Table1", "City"),
            (TestDatabases.MdbtoolsNwind, "Order Details", "Discount"),
            (TestDatabases.TestV2003, "Table2", "column89"),
            (TestDatabases.TestV1997, "Table1", "H"),
            (TestDatabases.NorthwindTraders, "Strings", "ModifiedOn"),
        ];
        foreach ((string fixture, string table, string column) in tables)
        {
            foreach (WriteMode mode in new[] { WriteMode.Direct, WriteMode.AutoCommit, WriteMode.ExplicitCommit })
            {
                data.Add(fixture, table, column, mode);
            }
        }

        return data;
    }

    /// <summary>Gets the Access-authored tables with table-level properties, with every rewrite.</summary>
    /// <returns>The fixture, table, column and rewrite tuples.</returns>
    public static TheoryData<string, string, string, Rewrite> TablesWithTableProperties()
    {
        var data = new TheoryData<string, string, string, Rewrite>();
        foreach (Rewrite rewrite in Enum.GetValues<Rewrite>())
        {
            data.Add(TestDatabases.NorthwindTraders, "Strings", "ModifiedOn", rewrite);
            data.Add(TestDatabases.TestV2000, "Table1", "H", rewrite);
        }

        return data;
    }

    /// <summary>Gets every rewrite in every write mode.</summary>
    /// <returns>The rewrite and write-mode pairs.</returns>
    public static TheoryData<Rewrite, WriteMode> RewritesAndModes()
    {
        var data = new TheoryData<Rewrite, WriteMode>();
        foreach (Rewrite rewrite in Enum.GetValues<Rewrite>())
        {
            foreach (WriteMode mode in new[] { WriteMode.Direct, WriteMode.AutoCommit, WriteMode.ExplicitCommit })
            {
                data.Add(rewrite, mode);
            }
        }

        return data;
    }

    /// <summary>Gets every writer-created format with every rewrite.</summary>
    /// <returns>The format and rewrite pairs.</returns>
    public static TheoryData<DatabaseFormat, Rewrite> FormatsAndRewrites()
    {
        var data = new TheoryData<DatabaseFormat, Rewrite>();
        foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
        {
            foreach (Rewrite rewrite in Enum.GetValues<Rewrite>())
            {
                data.Add(format, rewrite);
            }
        }

        return data;
    }

    /// <summary>Gets display-property refusals across formats, mutations and modes.</summary>
    /// <returns>The focused refusal cases.</returns>
    public static TheoryData<DatabaseFormat, Rewrite, WriteMode, string> DisplayPropertyRefusals()
    {
        var data = new TheoryData<DatabaseFormat, Rewrite, WriteMode, string>();
        foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
        {
            foreach (Rewrite rewrite in new[] { Rewrite.DropColumn, Rewrite.RenameColumn })
            {
                foreach (WriteMode mode in new[] { WriteMode.Direct, WriteMode.AutoCommit, WriteMode.ExplicitCommit })
                {
                    foreach (string property in new[] { "Filter", "OrderBy" })
                    {
                        data.Add(format, rewrite, mode, property);
                    }
                }
            }
        }

        return data;
    }

    /// <summary>
    /// An added column gets the writer's properties for its definition, and
    /// every target and entry of the Access-authored table stays as stored.
    /// </summary>
    /// <param name="fixture">The fixture path.</param>
    /// <param name="table">The table.</param>
    /// <param name="column">A column of the table (unused by this rewrite).</param>
    /// <param name="mode">How the writer runs the rewrite.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(AccessAuthoredTables))]
    public async Task AddColumn_AccessAuthoredTable_KeepsEveryPersistedProperty(string fixture, string table, string column, WriteMode mode)
    {
        _ = column;
        await using MemoryStream ms = await CopyFixtureAsync(fixture);
        List<TargetView> before = await ReadTargetsAsync(ms, table);

        await RunRewriteAsync(ms, mode, writer => writer.AddColumnAsync(table, new ColumnDefinition("Added", typeof(string), maxLength: 10) { Description = "added column" }, Ct));

        List<TargetView> after = await ReadTargetsAsync(ms, table);
        TargetView added = Assert.Single(after, t => t.Name == "Added");
        Assert.Contains(added.Entries, e => e.StartsWith("Description|", StringComparison.Ordinal));
        Assert.Contains(added.Entries, e => e.StartsWith("AllowZeroLength|", StringComparison.Ordinal));
        AssertSameTargets(WithoutNameMap(before), [.. after.Where(t => t.Name != "Added")]);
    }

    /// <summary>
    /// A renamed column's target takes the new name and keeps every entry, and
    /// every other target and entry stays as stored. Tables with nonempty display properties refuse the edit.
    /// </summary>
    /// <param name="fixture">The fixture path.</param>
    /// <param name="table">The table.</param>
    /// <param name="column">The column to rename.</param>
    /// <param name="mode">How the writer runs the rewrite.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(AccessAuthoredTables))]
    public async Task RenameColumn_AccessAuthoredTable_MovesTargetAndKeepsEntries(string fixture, string table, string column, WriteMode mode)
    {
        await using MemoryStream ms = await CopyFixtureAsync(fixture);
        List<TargetView> before = await ReadTargetsAsync(ms, table);
        Assert.Contains(before, t => t.Name == column);

        if (table == "Strings")
        {
            await AssertDisplayPropertyRefusalAsync(ms, mode, writer => writer.RenameColumnAsync(table, column, "Renamed", Ct).AsTask());
            AssertSameTargets(before, await ReadTargetsAsync(ms, table));
            return;
        }

        await RunRewriteAsync(ms, mode, writer => writer.RenameColumnAsync(table, column, "Renamed", Ct));

        List<TargetView> expected = [.. WithoutNameMap(before).Select(t => t.Name == column ? t with { Name = "Renamed" } : t)];
        AssertSameTargets(expected, await ReadTargetsAsync(ms, table));
    }

    /// <summary>
    /// A dropped column's target goes, and every other target and entry stays
    /// as stored.
    /// </summary>
    /// <param name="fixture">The fixture path.</param>
    /// <param name="table">The table.</param>
    /// <param name="column">The column to drop.</param>
    /// <param name="mode">How the writer runs the rewrite.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(AccessAuthoredTables))]
    public async Task DropColumn_AccessAuthoredTable_RemovesOnlyDroppedTarget(string fixture, string table, string column, WriteMode mode)
    {
        await using MemoryStream ms = await CopyFixtureAsync(fixture);
        List<TargetView> before = await ReadTargetsAsync(ms, table);
        Assert.Contains(before, t => t.Name == column);

        if (table == "Strings")
        {
            await AssertDisplayPropertyRefusalAsync(ms, mode, writer => writer.DropColumnAsync(table, column, Ct).AsTask());
            AssertSameTargets(before, await ReadTargetsAsync(ms, table));
            return;
        }

        await RunRewriteAsync(ms, mode, writer => writer.DropColumnAsync(table, column, Ct));

        AssertSameTargets([.. WithoutNameMap(before).Where(t => t.Name != column)], await ReadTargetsAsync(ms, table));
    }

    /// <summary>
    /// The table-level target (the empty-named property block) keeps every
    /// entry but <c>NameMap</c>, wherever it is in the blob: last in
    /// NorthwindTraders' Strings, whose Filter names a column, first in
    /// testV2000's Table1.
    /// </summary>
    /// <param name="fixture">The fixture path.</param>
    /// <param name="table">The table.</param>
    /// <param name="column">The column to drop or rename.</param>
    /// <param name="rewrite">The schema rewrite.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(TablesWithTableProperties))]
    public async Task SchemaRewrite_KeepsAccessAuthoredTableProperties(string fixture, string table, string column, Rewrite rewrite)
    {
        await using MemoryStream ms = await CopyFixtureAsync(fixture);
        TargetView before = Assert.Single(await ReadTargetsAsync(ms, table), t => t.Name.Length == 0);
        Assert.Contains(before.Entries, e => e.StartsWith("GUID|", StringComparison.Ordinal));

        if (table == "Strings" && rewrite != Rewrite.AddColumn)
        {
            await AssertDisplayPropertyRefusalAsync(ms, WriteMode.Direct, writer => RunAsync(writer, rewrite, table, column));
            Assert.Equal(before, Assert.Single(await ReadTargetsAsync(ms, table), target => target.Name.Length == 0));
            return;
        }

        await RunRewriteAsync(ms, WriteMode.Direct, writer => rewrite switch
        {
            Rewrite.AddColumn => writer.AddColumnAsync(table, new ColumnDefinition("Added", typeof(int)), Ct),
            Rewrite.DropColumn => writer.DropColumnAsync(table, column, Ct),
            Rewrite.RenameColumn => writer.RenameColumnAsync(table, column, "Renamed", Ct),
            _ => throw new ArgumentOutOfRangeException(nameof(rewrite)),
        });

        TargetView after = Assert.Single(await ReadTargetsAsync(ms, table), t => t.Name.Length == 0);
        Assert.Equal(0, after.ChunkType);
        Assert.Equal([.. before.Entries.Where(e => !e.StartsWith(NameMap + "|", StringComparison.Ordinal))], after.Entries);
    }

    /// <summary>
    /// A table-level Description and GUID planted on a writer-created table
    /// survive every rewrite, in every write mode.
    /// </summary>
    /// <param name="rewrite">The schema rewrite.</param>
    /// <param name="mode">How the writer runs the rewrite.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(RewritesAndModes))]
    public async Task SchemaRewrite_KeepsTableLevelTarget(Rewrite rewrite, WriteMode mode)
    {
        await using MemoryStream ms = await CreateDatabaseAsync(DatabaseFormat.Jet4Mdb);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync("T", [new("Id", typeof(int)), new("Name", typeof(string), maxLength: 20), new("Note", typeof(string), maxLength: 20)], Ct);
            await writer.InsertRowAsync("T", [1, "a", "n"], Ct);
        }

        await PlantPropertiesAsync(ms, "T", AddTableTarget);
        List<TargetView> before = await ReadTargetsAsync(ms, "T");
        Assert.Equal(0, before.FindIndex(t => t.Name.Length == 0));

        await RunRewriteAsync(ms, mode, writer => RunAsync(writer, rewrite, "T", "Note"));

        List<TargetView> after = await ReadTargetsAsync(ms, "T");
        Assert.Equal(Assert.Single(WithoutNameMap(before), t => t.Name.Length == 0), Assert.Single(after, t => t.Name.Length == 0));
        Assert.Equal(0, after.FindIndex(t => t.Name.Length == 0));
    }

    /// <summary>
    /// A rewrite rolled back inside an explicit transaction leaves the stored
    /// properties as they were, <c>NameMap</c> included.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task SchemaRewrite_ExplicitRollback_LeavesPropertiesUnchanged()
    {
        await using MemoryStream ms = await CopyFixtureAsync(TestDatabases.CalcFieldTestV2010);
        List<TargetView> before = await ReadTargetsAsync(ms, "Table1");

        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await using JetTransaction transaction = await writer.BeginTransactionAsync(Ct);
            await writer.RenameColumnAsync("Table1", "City", "Town", Ct);
            await writer.DropColumnAsync("Table1", "BoolTest", Ct);
            await transaction.RollbackAsync(Ct);
        }

        AssertSameTargets(before, await ReadTargetsAsync(ms, "Table1"));
    }

    /// <summary>
    /// A table with an attachment column is rebuilt onto its original TDEF page
    /// (the transplant path), which writes the catalog row's properties
    /// separately; they are kept there too.
    /// </summary>
    /// <param name="rewrite">The schema rewrite.</param>
    /// <param name="mode">How the writer runs the rewrite.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(RewritesAndModes))]
    public async Task SchemaRewrite_TableWithAttachment_KeepsProperties(Rewrite rewrite, WriteMode mode)
    {
        await using MemoryStream ms = await CreateDatabaseAsync(DatabaseFormat.AceAccdb);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync(
                "Docs",
                [
                    new("Id", typeof(int)) { IsPrimaryKey = true },
                    new("Title", typeof(string), maxLength: 40) { Description = "the title" },
                    new("Note", typeof(string), maxLength: 40),
                    new("Files", typeof(byte[])) { IsAttachment = true },
                ],
                Ct);
            await writer.InsertRowAsync("Docs", new RowValues { ["Id"] = 1, ["Title"] = "t", ["Note"] = "n" }, Ct);
            await writer.AddAttachmentAsync("Docs", "Files", new Dictionary<string, object?> { ["Id"] = 1 }, new AttachmentInput("a.txt", Encoding.ASCII.GetBytes("attached")), Ct);
        }

        await PlantPropertiesAsync(ms, "Docs", builder =>
        {
            AddTableTarget(builder);
            AddCaption(builder, "Title", "Document title");
        });
        List<TargetView> before = await ReadTargetsAsync(ms, "Docs");

        await RunRewriteAsync(ms, mode, writer => rewrite == Rewrite.RenameColumn
            ? writer.RenameColumnAsync("Docs", "Title", "Heading", Ct).AsTask()
            : RunAsync(writer, rewrite, "Docs", "Note"));

        List<TargetView> expected = [.. WithoutNameMap(before).Where(t => rewrite != Rewrite.DropColumn || t.Name != "Note").Select(t => rewrite == Rewrite.RenameColumn && t.Name == "Title" ? t with { Name = "Heading" } : t)];
        List<TargetView> after = await ReadTargetsAsync(ms, "Docs");
        AssertSameTargets(expected, [.. after.Where(t => t.Name != "Added")]);

        await using AccessReader reader = await OpenReaderAsync(ms);
        AttachmentRecord attachment = Assert.Single(await reader.GetAttachmentsAsync("Docs", "Files", Ct));
        Assert.Equal("attached", Encoding.ASCII.GetString(attachment.FileData));
    }

    /// <summary>
    /// A table the writer created rebuilds to the same properties: every
    /// surviving column's target is unchanged.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="rewrite">The schema rewrite.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndRewrites))]
    public async Task WriterCreatedTable_RewriteProducesSamePropertiesAsBefore(DatabaseFormat format, Rewrite rewrite)
    {
        await using MemoryStream ms = await CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            List<ColumnDefinition> columns =
            [
                new("Id", typeof(int)) { IsAutoIncrement = true, IsNullable = false },
                new("Name", typeof(string), maxLength: 20) { IsNullable = false, Description = "the name" },
                new("Qty", typeof(int)) { DefaultValueExpression = "1", ValidationRuleExpression = ">0", ValidationText = "positive" },
                new("Note", typeof(string), maxLength: 20),
            ];
            if (format == DatabaseFormat.AceAccdb)
            {
                columns.Add(new("Total", typeof(int)) { IsCalculated = true, CalculationExpression = "[Qty]*2" });
            }

            await writer.CreateTableAsync("T", columns, Ct);
        }

        List<TargetView> before = await ReadTargetsAsync(ms, "T");
        await RunRewriteAsync(ms, WriteMode.Direct, writer => RunAsync(writer, rewrite, "T", "Note"));

        List<TargetView> expected = [.. before.Where(t => rewrite != Rewrite.DropColumn || t.Name != "Note").Select(t => rewrite == Rewrite.RenameColumn && t.Name == "Note" ? t with { Name = "Renamed" } : t)];
        AssertSameTargets(expected, [.. (await ReadTargetsAsync(ms, "T")).Where(t => t.Name != "Added")]);
    }

    /// <summary>
    /// Renaming a column rewrites the stored expressions that name it, and only
    /// those entries: each keeps its data type, and every other entry of the
    /// calculated columns (Format, GUID, DecimalPlaces and the rest) stays as
    /// stored.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task RenameColumn_ExpressionNamingColumn_PersistsRewrittenExpression()
    {
        await using MemoryStream ms = await CopyFixtureAsync(TestDatabases.CalcFieldTestV2010);
        List<TargetView> before = await ReadTargetsAsync(ms, "Table1");

        await RunRewriteAsync(ms, WriteMode.Direct, writer => writer.RenameColumnAsync("Table1", "Salary", "Pay", Ct));

        List<TargetView> after = await ReadTargetsAsync(ms, "Table1");
        var expected = new List<TargetView>();
        foreach (TargetView target in WithoutNameMap(before))
        {
            List<string> entries = [.. target.Entries.Select(e => e.StartsWith("Expression|", StringComparison.Ordinal) ? RewriteSalary(e) : e)];
            expected.Add(target with { Name = target.Name == "Salary" ? "Pay" : target.Name, Entries = entries });
        }

        AssertSameTargets(expected, after);
        TargetView monthly = Assert.Single(after, t => t.Name == "MonthlySalary");
        Assert.Contains($"Expression|12|1|{Hex("[Pay]/12")}", monthly.Entries);
    }

    /// <summary>Editing and removing a table rule keeps opaque target headers.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The write mode.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(JetDatabaseWriter.Tests.Catalog.LvPropReadTests.FormatsAndModes), MemberType = typeof(JetDatabaseWriter.Tests.Catalog.LvPropReadTests))]
    public async Task TableRuleEdit_PreservesOpaqueHeaders(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await CreateDatabaseAsync(format);
        await using (AccessWriter initial = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await initial.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int))], Ct);
        }

        await PlantPropertiesAsync(ms, "T", builder =>
        {
            ColumnPropertyTargetBuilder table = builder.GetOrAddTableTarget();
            table.SourceHeader = 0xCDAB80FF;
            table.SourceHeaderIsNameLength = false;
            table.AddText("Caption", "Keep table", JetFormat.ForNewDatabase(format));
            ColumnPropertyTargetBuilder column = builder.GetOrAddTarget("Id");
            column.SourceHeader = 0xFEDC1234;
            column.SourceHeaderIsNameLength = false;
            column.AddText("Caption", "Keep column", JetFormat.ForNewDatabase(format));
        });
        List<TargetView> before = await ReadTargetsAsync(ms, "T");
        Assert.Equal(0xCDAB80FFu, Assert.Single(before, target => target.Name.Length == 0).SourceHeader);
        Assert.Equal(0xFEDC1234u, Assert.Single(before, target => target.Name == "Id").SourceHeader);
        static async Task EditRuleAsync(AccessWriter writer)
        {
            await writer.SetTableValidationRuleAsync("T", new TableValidationRule("[Id] > 0", "Positive"), Ct);
            await writer.SetTableValidationRuleAsync("T", null, Ct);
        }

        await RunRewriteAsync(ms, mode, EditRuleAsync);

        AssertSameTargets(before, await ReadTargetsAsync(ms, "T"));
    }

    /// <summary>Display dependencies refuse column removal or renaming before mutation.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="rewrite">The mutation.</param>
    /// <param name="mode">The write mode.</param>
    /// <param name="property">The stored property.</param>
    /// <returns>The asynchronous check.</returns>
    [Theory]
    [MemberData(nameof(DisplayPropertyRefusals))]
    public async Task SchemaRewrite_DisplayProperty_RefusesWithoutChangingBytes(DatabaseFormat format, Rewrite rewrite, WriteMode mode, string property)
    {
        await using MemoryStream ms = await CreateDatabaseAsync(format);
        await using (AccessWriter initial = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await initial.CreateTableAsync("T", [new("Id", typeof(int)), new("Note", typeof(int))], Ct);
        }

        await PlantPropertiesAsync(ms, "T", builder => builder.GetOrAddTableTarget().AddText(property, "[T].[Id] DESC, Note ASC", JetFormat.ForNewDatabase(format)));
        byte[] before = ms.ToArray();
        async Task CheckRefusalAsync(AccessWriter writer)
        {
            JetOperationException failure = await Assert.ThrowsAsync<JetOperationException>(() => RunAsync(writer, rewrite, "T", "Note"));
            Assert.Equal(JetErrorCode.FeatureNotSupported, failure.ErrorCode);
            Assert.Equal("T", failure.ErrorInfo.TableName);
            Assert.Equal("Note", failure.ErrorInfo.ColumnName);
            Assert.Contains(property, failure.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("DESC", failure.Message, StringComparison.Ordinal);
            Assert.Equal(before, ms.ToArray());
            await writer.InsertRowAsync("T", [1, 2], Ct);
        }

        await RunRewriteAsync(ms, mode, CheckRefusalAsync);
        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Equal(1, await reader.GetRealRowCountAsync("T", Ct));
    }

    /// <summary>Adding a column and editing tables with empty display properties remains allowed.</summary>
    /// <param name="rewrite">The mutation.</param>
    /// <param name="mode">The write mode.</param>
    /// <returns>The asynchronous check.</returns>
    [Theory]
    [MemberData(nameof(RewritesAndModes))]
    public async Task SchemaRewrite_EmptyDisplayPropertiesOrAdd_PreservesProperties(Rewrite rewrite, WriteMode mode)
    {
        await using MemoryStream ms = await CreateDatabaseAsync(DatabaseFormat.Jet4Mdb);
        await using (AccessWriter initial = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await initial.CreateTableAsync("T", [new("Id", typeof(int)), new("Note", typeof(int))], Ct);
        }

        await PlantPropertiesAsync(ms, "T", builder =>
        {
            ColumnPropertyTargetBuilder table = builder.GetOrAddTableTarget();
            table.AddText("Filter", rewrite == Rewrite.AddColumn ? "[Id] > 0" : string.Empty, JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb));
            table.AddText("OrderBy", rewrite == Rewrite.AddColumn ? "Id DESC" : "   ", JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb));
        });
        TargetView before = Assert.Single(await ReadTargetsAsync(ms, "T"), target => target.Name.Length == 0);
        await RunRewriteAsync(ms, mode, writer => RunAsync(writer, rewrite, "T", "Note"));
        Assert.Equal(before, Assert.Single(await ReadTargetsAsync(ms, "T"), target => target.Name.Length == 0));
    }

    /// <summary>Case-only renames obey the same conservative display-property refusal.</summary>
    /// <param name="mode">The write mode.</param>
    /// <returns>The asynchronous check.</returns>
    [Theory]
    [InlineData(WriteMode.Direct)]
    [InlineData(WriteMode.AutoCommit)]
    [InlineData(WriteMode.ExplicitCommit)]
    public async Task RenameColumn_CaseOnlyWithDisplayProperty_RefusesWithoutChangingBytes(WriteMode mode)
    {
        await using MemoryStream ms = await CreateDatabaseAsync(DatabaseFormat.Jet4Mdb);
        await using (AccessWriter initial = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await initial.CreateTableAsync("T", [new("Id", typeof(int)), new("Note", typeof(int))], Ct);
        }

        await PlantPropertiesAsync(ms, "T", builder => builder.GetOrAddTableTarget().AddText("Filter", "[Note] > 0", JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb)));
        await AssertDisplayPropertyRefusalAsync(ms, mode, writer => writer.RenameColumnAsync("T", "Note", "note", Ct).AsTask());
    }

    private static async Task AssertDisplayPropertyRefusalAsync(MemoryStream ms, WriteMode mode, Func<AccessWriter, Task> rewrite)
    {
        byte[] before = ms.ToArray();
        async Task CheckAsync(AccessWriter writer)
        {
            JetOperationException failure = await Assert.ThrowsAsync<JetOperationException>(() => rewrite(writer));
            Assert.Equal(JetErrorCode.FeatureNotSupported, failure.ErrorCode);
            Assert.Equal(before, ms.ToArray());
        }

        await RunRewriteAsync(ms, mode, CheckAsync);
        Assert.Equal(before, ms.ToArray());
    }

    private static string RewriteSalary(string entry)
    {
        string[] parts = entry.Split('|');
        string text = Encoding.Unicode.GetString(Convert.FromHexString(parts[3]));
        return $"{parts[0]}|{parts[1]}|{parts[2]}|{Hex(text.Replace("[Salary]", "[Pay]", StringComparison.Ordinal))}";
    }

    private static string Hex(string text) => Convert.ToHexString(Encoding.Unicode.GetBytes(text));

    private static Task RunAsync(AccessWriter writer, Rewrite rewrite, string table, string column) => rewrite switch
    {
        Rewrite.AddColumn => writer.AddColumnAsync(table, new ColumnDefinition("Added", typeof(int)), Ct).AsTask(),
        Rewrite.DropColumn => writer.DropColumnAsync(table, column, Ct).AsTask(),
        Rewrite.RenameColumn => writer.RenameColumnAsync(table, column, "Renamed", Ct).AsTask(),
        _ => throw new ArgumentOutOfRangeException(nameof(rewrite)),
    };

    private static void AddTableTarget(ColumnPropertyBlockBuilder builder)
    {
        var target = new ColumnPropertyTargetBuilder { Name = string.Empty, ChunkType = ColumnPropertyChunkType.PropertyBlock };
        target.AddText(Constants.ColumnPropertyNames.Description, "the table", JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb));
        target.Entries.Add(new ColumnPropertyEntryBuilder { Name = "GUID", DataType = ColumnType.BinaryType, DdlFlag = 1, Value = Guid.Parse("0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0").ToByteArray() });
        target.Entries.Add(new ColumnPropertyEntryBuilder { Name = NameMap, DataType = ColumnType.OleType, DdlFlag = 0, Value = [1, 2, 3, 4] });
        builder.Targets.Insert(0, target);

        // This synthetic fixture intentionally plants the table target first.
        // Clear the source positions so serialization uses this constructed order;
        // the fresh reader below then verifies the order actually stored on disk.
        foreach (ColumnPropertyTargetBuilder planted in builder.Targets)
        {
            planted.SourceChunkIndex = -1;
        }
    }

    private static void AddCaption(ColumnPropertyBlockBuilder builder, string column, string caption)
    {
        ColumnPropertyTargetBuilder target = builder.GetOrAddTarget(column);
        target.Entries.Add(new ColumnPropertyEntryBuilder { Name = "Caption", DataType = ColumnType.MemoType, DdlFlag = 1, Value = Encoding.Unicode.GetBytes(caption) });
        target.Entries.Add(new ColumnPropertyEntryBuilder { Name = "ColumnWidth", DataType = ColumnType.IntegerType, DdlFlag = 0, Value = [0x10, 0x0E] });
    }

    /// <summary>
    /// Replaces a table's stored properties through the writer's internal
    /// catalog path, as another tool would have written them.
    /// </summary>
    /// <param name="ms">The database.</param>
    /// <param name="table">The table.</param>
    /// <param name="edit">Edits a builder seeded with the stored properties.</param>
    private static async Task PlantPropertiesAsync(MemoryStream ms, string table, Action<ColumnPropertyBlockBuilder> edit)
    {
        ms.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: Ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.Services.Catalog.GetCatalogEntryAsync(table, Ct));
        ColumnPropertyBlock? stored = await harness.Services.Snapshots.ReadLvPropBlockAsync(entry.TDefPage, Ct);
        ColumnPropertyBlockBuilder builder = stored is null ? new ColumnPropertyBlockBuilder() : ColumnPropertyBlockBuilder.FromBlock(stored);
        edit(builder);
        await harness.Services.CatalogArtifacts.ExecutePlanAsync(
            new CatalogArtifactPlan([], [])
            {
                CatalogReplacements = [new UserTableCatalogReplacementArtifact(table, table, entry.TDefPage, builder.ToBytes(harness.Database.Format))],
            },
            Ct);
    }

    private static List<TargetView> WithoutNameMap(List<TargetView> targets)
        => [.. targets.Select(t => t.Name.Length == 0 ? t with { Entries = [.. t.Entries.Where(e => !e.StartsWith(NameMap + "|", StringComparison.Ordinal))] } : t)];

    private static void AssertSameTargets(List<TargetView> expected, List<TargetView> actual)
    {
        Assert.Equal(expected.Select(t => t.Name), actual.Select(t => t.Name));
        for (int i = 0; i < expected.Count; i++)
        {
            Assert.Equal(expected[i].ChunkType, actual[i].ChunkType);
            Assert.Equal(expected[i].SourceHeader, actual[i].SourceHeader);
            Assert.True(expected[i].Entries.SequenceEqual(actual[i].Entries), $"Target '{expected[i].Name}': expected [{string.Join("; ", expected[i].Entries)}], got [{string.Join("; ", actual[i].Entries)}].");
        }
    }

    /// <summary>Reads a table's stored properties with a fresh reader.</summary>
    /// <param name="ms">The database.</param>
    /// <param name="table">The table.</param>
    private static async Task<List<TargetView>> ReadTargetsAsync(MemoryStream ms, string table)
    {
        ms.Position = 0;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(ms, cancellationToken: Ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync(table, Ct));
        ColumnPropertyBlock? block = await harness.Services.Catalog.ReadLvPropForTableAsync(entry.TDefPage, Ct);
        return block is null
            ? []
            : [.. block.Targets.Select(t => new TargetView(t.Name, (int)t.ChunkType, t.SourceHeaderIsNameLength ? null : t.SourceHeader, [.. t.Entries.Select(e => $"{e.Name}|{(int)e.DataType}|{e.DdlFlag}|{Convert.ToHexString(e.Value)}")]))];
    }

    private static async Task RunRewriteAsync(MemoryStream ms, WriteMode mode, Func<AccessWriter, Task> rewrite)
    {
        await using AccessWriter writer = await OpenWriterAsync(ms, mode);
        if (mode != WriteMode.ExplicitCommit)
        {
            await rewrite(writer);
            return;
        }

        await using JetTransaction transaction = await writer.BeginTransactionAsync(Ct);
        await rewrite(writer);
        await transaction.CommitAsync(Ct);
    }

    private static Task RunRewriteAsync(MemoryStream ms, WriteMode mode, Func<AccessWriter, ValueTask> rewrite)
        => RunRewriteAsync(ms, mode, writer => rewrite(writer).AsTask());

    private static async Task<MemoryStream> CreateDatabaseAsync(DatabaseFormat format)
    {
        var ms = new MemoryStream();
        await using (await AccessWriter.CreateDatabaseAsync(ms, format, WriterOptions(WriteMode.Direct), leaveOpen: true, cancellationToken: Ct))
        {
        }

        ms.Position = 0;
        return ms;
    }

    private static async Task<MemoryStream> CopyFixtureAsync(string path)
    {
        var ms = new MemoryStream();
        byte[] bytes = await File.ReadAllBytesAsync(path, Ct);
        await ms.WriteAsync(bytes, Ct);
        ms.Position = 0;
        return ms;
    }

    private static AccessWriterOptions WriterOptions(WriteMode mode) => new()
    {
        UseLockFile = false,
        UseTransactionalWrites = mode == WriteMode.AutoCommit,
    };

    private static async Task<AccessWriter> OpenWriterAsync(MemoryStream ms, WriteMode mode)
    {
        ms.Position = 0;
        return await AccessWriter.OpenAsync(ms, WriterOptions(mode), leaveOpen: true, cancellationToken: Ct);
    }

    private static async Task<AccessReader> OpenReaderAsync(MemoryStream ms)
    {
        ms.Position = 0;
        return await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
    }

    /// <summary>One property target as its name, chunk type and entry lines.</summary>
    /// <param name="Name">The target name; empty for the table.</param>
    /// <param name="ChunkType">The property-block chunk type.</param>
    /// <param name="SourceHeader">The preserved opaque inner target header, or null for a recognized native name length.</param>
    /// <param name="Entries">One line per entry: name, data type, DDL flag and value bytes in hex.</param>
    private sealed record TargetView(string Name, int ChunkType, uint? SourceHeader, List<string> Entries)
    {
        /// <inheritdoc/>
        public bool Equals(TargetView? other)
            => other is not null && this.Name == other.Name && this.ChunkType == other.ChunkType && this.SourceHeader == other.SourceHeader && this.Entries.SequenceEqual(other.Entries);

        /// <inheritdoc/>
        public override int GetHashCode() => HashCode.Combine(this.Name, this.ChunkType, this.SourceHeader, this.Entries.Count);
    }
}
