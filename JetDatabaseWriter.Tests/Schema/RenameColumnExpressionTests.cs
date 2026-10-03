namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// <c>RenameColumnAsync</c> carries the new name into every calculated
/// expression, validation rule and default that names the column, so they
/// keep evaluating; <c>DropColumnAsync</c> refuses to drop a column one of
/// them still names.
/// </summary>
public sealed class RenameColumnExpressionTests
{
    /// <summary>How each test drives the writer.</summary>
    [SuppressMessage("Design", "CA1515:Consider making public types internal", Justification = "Theory parameters of public xUnit test methods must be public.")]
    public enum WriteMode
    {
        /// <summary>No transaction; every page write goes straight to the stream.</summary>
        Direct = 0,

        /// <summary><see cref="AccessWriterOptions.UseTransactionalWrites"/> wraps each call in its own transaction.</summary>
        AutoCommit = 1,

        /// <summary>The schema change and the writes after it run inside one explicit transaction that is committed.</summary>
        ExplicitCommit = 2,
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every write mode.</summary>
    /// <returns>The write modes.</returns>
    public static TheoryData<WriteMode> WriteModes() => [.. Enum.GetValues<WriteMode>()];

    /// <summary>Gets every writer-created format (Jet3, Jet4, ACCDB) in every write mode.</summary>
    /// <returns>The format and write-mode pairs.</returns>
    public static TheoryData<DatabaseFormat, WriteMode> AllFormatsAndModes()
    {
        var data = new TheoryData<DatabaseFormat, WriteMode>();
        foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
        {
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                data.Add(format, mode);
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(WriteModes))]
    public async Task RenameColumn_RewritesCalculatedExpressionsThatNameIt(WriteMode mode)
    {
        await using MemoryStream ms = await CreatePriceTableAsync();

        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                await writer.RenameColumnAsync("T", "Price", "Unit Price", Ct);
                await writer.InsertRowAsync("T", [2, 3d, 2, null, null, null, null], Ct);
                Assert.Equal(1, await writer.UpdateRowsAsync("T", "Id", 1, new Dictionary<string, object?> { ["Qty"] = 5 }, Ct));
            });
        }

        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await writer.InsertRowAsync("T", [3, 1.5d, 4, null, null, null, null], Ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync("T", Ct);
        Assert.Equal(["Id", "Unit Price", "Qty", "Total", "Label", "Bare", "Twice"], meta.Select(c => c.Name));
        Assert.Equal("[Unit Price]*[Qty]", Expression(meta, "Total"));
        Assert.Equal("\"[Price] is \" & [Unit Price]", Expression(meta, "Label"));
        Assert.Equal("[Unit Price] + 1", Expression(meta, "Bare"));
        Assert.Equal("[Total]*2", Expression(meta, "Twice"));

        DataTable rows = await reader.ReadDataTableAsync("T", cancellationToken: Ct);
        AssertPriceRow(rows, 1, total: 12.5, label: "[Price] is 2.5", bare: 3.5, twice: 25);
        AssertPriceRow(rows, 2, total: 6, label: "[Price] is 3", bare: 4, twice: 12);
        AssertPriceRow(rows, 3, total: 6, label: "[Price] is 1.5", bare: 2.5, twice: 12);
    }

    [Fact]
    public async Task RenameColumn_OfCalculatedColumnAnotherUses_RewritesTheDependent()
    {
        await using MemoryStream ms = await CreatePriceTableAsync();

        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.RenameColumnAsync("T", "Total", "Subtotal", Ct);
            await writer.InsertRowAsync("T", [2, 3d, 2, null, null, null, null], Ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync("T", Ct);
        Assert.Equal("[Price]*[Qty]", Expression(meta, "Subtotal"));
        Assert.Equal("[Subtotal]*2", Expression(meta, "Twice"));

        DataRow row = FindRow(await reader.ReadDataTableAsync("T", cancellationToken: Ct), 2);
        Assert.Equal(6d, Convert.ToDouble(row["Subtotal"], CultureInfo.InvariantCulture));
        Assert.Equal(12d, Convert.ToDouble(row["Twice"], CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// A rule and a default that name the renamed column keep working: the
    /// rule still rejects values, in the renaming session and a later one,
    /// rather than silently accepting everything, and the default still
    /// fills the column.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the rename and the inserts after it.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(AllFormatsAndModes))]
    public async Task RenameColumn_RewritesValidationRuleAndDefaultThatNameIt(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync(
                "R",
                [
                    new("Id", typeof(int)),
                    new("Code", typeof(string), maxLength: 10) { ValidationRuleExpression = "Len([Code]) = 3", ValidationText = "3 chars" },
                    new("Note", typeof(string), maxLength: 20) { DefaultValueExpression = "[Code] & \"-n\"" },
                ],
                Ct);
            await writer.InsertRowAsync("R", [1, "ABC", DbDefault.Value], Ct);
        }

        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                await writer.RenameColumnAsync("R", "Code", "Sku", Ct);
                ArgumentException rejected = await Assert.ThrowsAsync<ArgumentException>(async () => await writer.InsertRowAsync("R", [2, "ABCD", DbDefault.Value], Ct));
                Assert.Contains("3 chars", rejected.Message, StringComparison.Ordinal);
                await writer.InsertRowAsync("R", [3, "XYZ", DbDefault.Value], Ct);
            });
        }

        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            ArgumentException rejected = await Assert.ThrowsAsync<ArgumentException>(async () => await writer.InsertRowAsync("R", [4, "WXYZ", DbDefault.Value], Ct));
            Assert.Contains("Len([Sku]) = 3", rejected.Message, StringComparison.Ordinal);
            await writer.InsertRowAsync("R", [5, "UVW", DbDefault.Value], Ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync("R", Ct);
        Assert.Equal("Len([Sku]) = 3", Assert.Single(meta, c => c.Name == "Sku").ValidationRuleExpression);
        Assert.Equal("3 chars", Assert.Single(meta, c => c.Name == "Sku").ValidationText);
        Assert.Equal("[Sku] & \"-n\"", Assert.Single(meta, c => c.Name == "Note").DefaultValueExpression);

        DataTable rows = await reader.ReadDataTableAsync("R", cancellationToken: Ct);
        Assert.Equal(
            ["1|ABC|ABC-n", "3|XYZ|XYZ-n", "5|UVW|UVW-n"],
            rows.AsEnumerable().Select(r => $"{r["Id"]}|{r["Sku"]}|{r["Note"]}").Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task RenameColumn_InExplicitTransaction_RolledBack_KeepsOriginalExpressions()
    {
        await using MemoryStream ms = await CreatePriceTableAsync();

        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await using (JetTransaction transaction = await writer.BeginTransactionAsync(Ct))
            {
                await writer.RenameColumnAsync("T", "Price", "Cost", Ct);
                await transaction.RollbackAsync(Ct);
            }

            await writer.InsertRowAsync("T", [2, 3d, 2, null, null, null, null], Ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync("T", Ct);
        Assert.Equal("Price", meta[1].Name);
        Assert.Equal("[Price]*[Qty]", Expression(meta, "Total"));
        Assert.Equal("Price + 1", Expression(meta, "Bare"));
        AssertPriceRow(await reader.ReadDataTableAsync("T", cancellationToken: Ct), 2, total: 6, label: "[Price] is 3", bare: 4, twice: 12);
    }

    /// <summary>
    /// Renames two fields of the Access-authored calcFieldTestV2010 table that
    /// eight of its calculated columns use, then has every row re-evaluated by
    /// an update: each value is still what Access cached.
    /// </summary>
    /// <param name="mode">How the writer runs the renames and the update.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(WriteModes))]
    public async Task RenameColumn_OnAccessAuthoredCalculatedTable_RewritesEveryExpression(WriteMode mode)
    {
        await using MemoryStream ms = await CopyFixtureAsync(TestDatabases.CalcFieldTestV2010);
        DataTable before;
        await using (AccessReader reader = await OpenReaderAsync(ms))
        {
            before = await reader.ReadDataTableAsync("Table1", cancellationToken: Ct);
        }

        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                await writer.RenameColumnAsync("Table1", "Salary", "Base Pay", Ct);
                await writer.RenameColumnAsync("Table1", "LastFirst", "Full Name", Ct);
                foreach (DataRow row in before.Rows)
                {
                    Assert.Equal(1, await writer.UpdateRowsAsync("Table1", RowCriteria.Where("ID", row["ID"]), new RowValues { ["City"] = row["City"] }, Ct));
                }
            });
        }

        await using (AccessReader reader = await OpenReaderAsync(ms))
        {
            IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync("Table1", Ct);
            Assert.Equal("[LastName] & \", \" & [FirstName]", Expression(meta, "Full Name"));
            Assert.Equal("Len([Full Name])", Expression(meta, "LastFirstLen"));
            Assert.Equal("[Base Pay]/12", Expression(meta, "MonthlySalary"));
            Assert.Equal("[Base Pay]>100000", Expression(meta, "IsRich"));
            Assert.Equal("[LastName] & \", \" & [FirstName] & \"=\" & [Full Name]", Expression(meta, "AllNames"));
            Assert.Equal("[Base Pay]/52", Expression(meta, "WeeklySalary"));
            Assert.Equal("[Base Pay]", Expression(meta, "SalaryTest"));
            Assert.Equal("[Base Pay]*0.13/[DecimalTest]", Expression(meta, "FloatTest"));
            Assert.Equal("([Base Pay]*[MonthlySalary]/[DecimalTest])*34.12342134", Expression(meta, "BigNumTest"));

            DataTable after = await reader.ReadDataTableAsync("Table1", cancellationToken: Ct);
            var mismatches = new List<string>();
            foreach (DataRow original in before.Rows)
            {
                DataRow updated = Assert.Single(after.AsEnumerable(), r => Equals(r["ID"], original["ID"]));
                foreach (ColumnMetadata column in meta.Where(c => c.IsCalculated))
                {
                    string originalName = column.Name switch
                    {
                        "Full Name" => "LastFirst",
                        _ => column.Name,
                    };
                    CompareCalculated(mismatches, $"ID {original["ID"]} {column.Name}", original[originalName], updated[column.Name]);
                }

                Assert.Equal(original["Salary"], updated["Base Pay"]);
            }

            Assert.True(mismatches.Count == 0, string.Join(Environment.NewLine, mismatches));
        }
    }

    [Fact]
    public async Task RenameColumn_StoredExpressionTheEngineCannotParse_IsRewrittenTextually()
    {
        await using MemoryStream ms = await CreateDatabaseAsync(DatabaseFormat.AceAccdb);

        // Only an earlier version of this library could have stored '%'; plant it
        // through the internal schema service, which skips the definition check.
        ms.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: Ct))
        {
            await harness.Services.Schema.CreateTableAsync(
                "CalcLegacyPercent",
                [
                    new("Id", typeof(int)),
                    new("R", typeof(int)),
                    new("C", typeof(double)) { IsCalculated = true, CalculationExpression = "[R]%" },
                ],
                [],
                Ct);
        }

        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.RenameColumnAsync("CalcLegacyPercent", "R", "Rate", Ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Equal("[Rate]%", Expression(await reader.GetColumnMetadataAsync("CalcLegacyPercent", Ct), "C"));
    }

    [Fact]
    public async Task RenameColumn_ToNameWithClosingBracket_WhenReferenced_ThrowsAndLeavesTableUnchanged()
    {
        await using MemoryStream ms = await CreatePriceTableAsync();

        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            ArgumentException exception = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.RenameColumnAsync("T", "Price", "Pr]ice", Ct));

            Assert.Equal("newColumnName", exception.ParamName);
            Assert.Contains("'Total'", exception.Message, StringComparison.Ordinal);
            Assert.Contains("[Price]*[Qty]", exception.Message, StringComparison.Ordinal);

            await writer.InsertRowAsync("T", [2, 3d, 2, null, null, null, null], Ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync("T", Ct);
        Assert.Equal(["Id", "Price", "Qty", "Total", "Label", "Bare", "Twice"], meta.Select(c => c.Name));
        Assert.Equal("[Price]*[Qty]", Expression(meta, "Total"));
        DataTable rows = await reader.ReadDataTableAsync("T", cancellationToken: Ct);
        AssertPriceRow(rows, 1, total: 5, label: "[Price] is 2.5", bare: 3.5, twice: 10);
        AssertPriceRow(rows, 2, total: 6, label: "[Price] is 3", bare: 4, twice: 12);
    }

    [Fact]
    public async Task RenameColumn_ReferenceOnlyInsideStringLiteral_LeavesExpressionUnchanged()
    {
        await using MemoryStream ms = await CreateDatabaseAsync(DatabaseFormat.AceAccdb);

        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync(
                "S",
                [
                    new("Price", typeof(int)),
                    new("Qty", typeof(int)),
                    new("Label", typeof(string), maxLength: 40) { IsCalculated = true, CalculationExpression = "\"[Price]\" & 'Price' & [Qty]" },
                ],
                Ct);
            await writer.RenameColumnAsync("S", "Price", "Cost", Ct);
            await writer.InsertRowAsync("S", [1, 2, null], Ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Equal("\"[Price]\" & 'Price' & [Qty]", Expression(await reader.GetColumnMetadataAsync("S", Ct), "Label"));
        DataRow row = Assert.Single((await reader.ReadDataTableAsync("S", cancellationToken: Ct)).AsEnumerable());
        Assert.Equal("[Price]Price2", row["Label"]);
    }

    /// <summary>
    /// Dropping a column a calculated expression names is refused before
    /// anything is written, and the writer, or the explicit transaction, stays
    /// usable: the next insert still evaluates the expression.
    /// </summary>
    /// <param name="mode">How the writer runs the drop and the insert after it.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(WriteModes))]
    public async Task DropColumn_NamedByCalculatedExpression_ThrowsAndLeavesTableUnchanged(WriteMode mode)
    {
        await using MemoryStream ms = await CreatePriceTableAsync();

        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    await writer.DropColumnAsync("T", "Price", Ct));

                Assert.Contains("'Price'", exception.Message, StringComparison.Ordinal);
                Assert.Contains("'Total'", exception.Message, StringComparison.Ordinal);
                Assert.Contains("[Price]*[Qty]", exception.Message, StringComparison.Ordinal);

                await writer.InsertRowAsync("T", [2, 3d, 2, null, null, null, null], Ct);
            });
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync("T", Ct);
        Assert.Equal(["Id", "Price", "Qty", "Total", "Label", "Bare", "Twice"], meta.Select(c => c.Name));
        Assert.Equal("[Price]*[Qty]", Expression(meta, "Total"));
        DataTable rows = await reader.ReadDataTableAsync("T", cancellationToken: Ct);
        AssertPriceRow(rows, 1, total: 5, label: "[Price] is 2.5", bare: 3.5, twice: 10);
        AssertPriceRow(rows, 2, total: 6, label: "[Price] is 3", bare: 4, twice: 12);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, "validation rule")]
    [InlineData(DatabaseFormat.Jet3Mdb, "default value")]
    [InlineData(DatabaseFormat.Jet4Mdb, "validation rule")]
    [InlineData(DatabaseFormat.Jet4Mdb, "default value")]
    [InlineData(DatabaseFormat.AceAccdb, "validation rule")]
    [InlineData(DatabaseFormat.AceAccdb, "default value")]
    public async Task DropColumn_NamedByAnotherColumnsValidationRuleOrDefault_Throws(DatabaseFormat format, string property)
    {
        await using MemoryStream ms = await CreateDatabaseAsync(format);
        string expression = property == "validation rule" ? "Len([Code]) > 0" : "[Code] & \"-n\"";
        ColumnDefinition other = property == "validation rule"
            ? new("Other", typeof(string), maxLength: 20) { ValidationRuleExpression = expression }
            : new("Other", typeof(string), maxLength: 20) { DefaultValueExpression = expression };

        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync("D", [new("Id", typeof(int)), new("Code", typeof(string), maxLength: 10), other], Ct);
            await writer.InsertRowAsync("D", [1, "ABC", "x"], Ct);

            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.DropColumnAsync("D", "code", Ct));

            Assert.Contains($"the {property} of column 'Other'", exception.Message, StringComparison.Ordinal);
            Assert.Contains(expression, exception.Message, StringComparison.Ordinal);
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Equal(["Id", "Code", "Other"], (await reader.GetColumnMetadataAsync("D", Ct)).Select(c => c.Name));
        Assert.Equal("1|ABC|x", string.Join("|", Assert.Single((await reader.ReadDataTableAsync("D", cancellationToken: Ct)).AsEnumerable()).ItemArray));
    }

    /// <summary>
    /// A column named only inside another expression's string literals, or
    /// only by its own rule, can be dropped.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DropColumn_NamedOnlyInStringLiteralOrByItsOwnRule_Succeeds(DatabaseFormat format)
    {
        await using MemoryStream ms = await CreateDatabaseAsync(format);

        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync(
                "D",
                [
                    new("Id", typeof(int)),
                    new("Code", typeof(string), maxLength: 10) { ValidationRuleExpression = "Len([Code]) = 3" },
                    new("Note", typeof(string), maxLength: 20) { DefaultValueExpression = "\"[Code]\" & 'Code'" },
                ],
                Ct);
            await writer.InsertRowAsync("D", [1, "ABC", DbDefault.Value], Ct);
            await writer.DropColumnAsync("D", "Code", Ct);
            await writer.InsertRowAsync("D", [2, DbDefault.Value], Ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync("D", Ct);
        Assert.Equal(["Id", "Note"], meta.Select(c => c.Name));
        Assert.Equal("\"[Code]\" & 'Code'", meta[1].DefaultValueExpression);
        Assert.Equal(
            ["1|[Code]Code", "2|[Code]Code"],
            (await reader.ReadDataTableAsync("D", cancellationToken: Ct)).AsEnumerable().Select(r => $"{r["Id"]}|{r["Note"]}").Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task DropColumn_TheCalculatedColumnItselfOrOneNoLongerNamed_Succeeds()
    {
        await using MemoryStream ms = await CreatePriceTableAsync();

        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            // Twice names Total, so Total can go only after Twice.
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.DropColumnAsync("T", "Total", Ct));
            await writer.DropColumnAsync("T", "Twice", Ct);
            await writer.DropColumnAsync("T", "Total", Ct);
            await writer.InsertRowAsync("T", [2, 3d, 2, null, null], Ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Equal(["Id", "Price", "Qty", "Label", "Bare"], (await reader.GetColumnMetadataAsync("T", Ct)).Select(c => c.Name));
        DataRow row = FindRow(await reader.ReadDataTableAsync("T", cancellationToken: Ct), 2);
        Assert.Equal("[Price] is 3", row["Label"]);
        Assert.Equal(4d, Convert.ToDouble(row["Bare"], CultureInfo.InvariantCulture));
    }

    private static string? Expression(IReadOnlyList<ColumnMetadata> meta, string column)
        => Assert.Single(meta, c => c.Name == column).CalculationExpression;

    private static DataRow FindRow(DataTable rows, int id)
        => Assert.Single(rows.AsEnumerable(), r => Convert.ToInt32(r["Id"], CultureInfo.InvariantCulture) == id);

    private static void AssertPriceRow(DataTable rows, int id, double total, string label, double bare, double twice)
    {
        DataRow row = FindRow(rows, id);
        Assert.Equal(total, Convert.ToDouble(row["Total"], CultureInfo.InvariantCulture));
        Assert.Equal(label, row["Label"]);
        Assert.Equal(bare, Convert.ToDouble(row["Bare"], CultureInfo.InvariantCulture));
        Assert.Equal(twice, Convert.ToDouble(row["Twice"], CultureInfo.InvariantCulture));
    }

    private static void CompareCalculated(List<string> mismatches, string label, object expected, object actual)
    {
        if (expected is DBNull || actual is DBNull)
        {
            if (!(expected is DBNull && actual is DBNull))
            {
                mismatches.Add($"{label}: expected {expected}, got {actual}");
            }
        }
        else if (expected is float or double or decimal)
        {
            double e = Convert.ToDouble(expected, CultureInfo.InvariantCulture);
            double a = Convert.ToDouble(actual, CultureInfo.InvariantCulture);
            if (Math.Abs(e - a) > Math.Max(1e-4, Math.Abs(e) * 1e-6))
            {
                mismatches.Add($"{label}: expected {e}, got {a}");
            }
        }
        else if (!Equals(expected, actual))
        {
            mismatches.Add($"{label}: expected {expected}, got {actual}");
        }
    }

    /// <summary>
    /// Creates an ACCDB with table <c>T</c>: Id, Price, Qty, and calculated
    /// Total (<c>[Price]*[Qty]</c>), Label (<c>"[Price] is " &amp; [price]</c>),
    /// Bare (<c>Price + 1</c>) and Twice (<c>[Total]*2</c>), holding row
    /// 1 with Price 2.5 and Qty 2.
    /// </summary>
    /// <returns>The database.</returns>
    private static async Task<MemoryStream> CreatePriceTableAsync()
    {
        MemoryStream ms = await CreateDatabaseAsync(DatabaseFormat.AceAccdb);
        await using AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct);
        await writer.CreateTableAsync(
            "T",
            [
                new("Id", typeof(int)),
                new("Price", typeof(double)),
                new("Qty", typeof(int)),
                new("Total", typeof(double)) { IsCalculated = true, CalculationExpression = "[Price]*[Qty]" },
                new("Label", typeof(string), maxLength: 60) { IsCalculated = true, CalculationExpression = "\"[Price] is \" & [price]" },
                new("Bare", typeof(double)) { IsCalculated = true, CalculationExpression = "Price + 1" },
                new("Twice", typeof(double)) { IsCalculated = true, CalculationExpression = "[Total]*2" },
            ],
            Ct);
        await writer.InsertRowAsync("T", [1, 2.5d, 2, null, null, null, null], Ct);
        return ms;
    }

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

    private static async Task RunAsync(AccessWriter writer, WriteMode mode, Func<Task> work)
    {
        if (mode != WriteMode.ExplicitCommit)
        {
            await work();
            return;
        }

        await using JetTransaction transaction = await writer.BeginTransactionAsync(Ct);
        await work();
        await transaction.CommitAsync(Ct);
    }
}
