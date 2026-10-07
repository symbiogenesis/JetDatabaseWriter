namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// <c>RenameColumnAsync</c> to a name that differs from the column's only in
/// letter case, which Microsoft Access is believed to allow (not checked; see
/// <c>DaoRenameColumnCaseTests</c>): the column takes the new
/// spelling, and so does every expression and index that names it. A rename
/// onto another column's name, in any case, is still refused, and a rename to
/// the name the column already has changes nothing.
/// </summary>
public sealed class RenameColumnCaseTests
{
    private static readonly string[] TakenNames = ["Note", "NOTE", "id"];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every writer-created format (Jet3, Jet4, ACCDB) in every write mode.</summary>
    /// <returns>The format and write-mode pairs.</returns>
    public static TheoryData<DatabaseFormat, WriteMode> AllFormatsAndModes()
        => WriteModes.Combine(DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb);

    /// <summary>Gets every write mode.</summary>
    /// <returns>The write modes.</returns>
    public static TheoryData<WriteMode> AllModes() => [.. Enum.GetValues<WriteMode>()];

    /// <summary>
    /// A case-only rename keeps the rows, and the validation rule, the default
    /// and the unique index that name the column take the new spelling and keep
    /// working, in the renaming session and a later one.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the rename and the insert after it.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(AllFormatsAndModes))]
    public async Task RenameColumn_CaseOnly_TakesTheNewSpellingEverywhere(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await CreateCodeTableAsync(format);

        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await WriteModes.RunAsync(
                writer,
                mode,
                async () =>
                {
                    await writer.RenameColumnAsync("T", "Code", "CODE", Ct);
                    await writer.InsertRowAsync("T", [2, "XYZ", DbDefault.Value], Ct);
                },
                Ct);
        }

        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            JetValidationRuleException rejected = await Assert.ThrowsAsync<JetValidationRuleException>(async () => await writer.InsertRowAsync("T", [3, "WXYZ", DbDefault.Value], Ct));
            Assert.Contains("Len([CODE]) = 3", rejected.Message, StringComparison.Ordinal);
            await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowAsync("T", [4, "ABC", DbDefault.Value], Ct));
            await writer.InsertRowAsync("T", [5, "UVW", DbDefault.Value], Ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync("T", Ct);
        Assert.Equal(["Id", "CODE", "Note"], meta.Select(c => c.Name));
        Assert.Equal("Len([CODE]) = 3", meta[1].ValidationRuleExpression);
        Assert.Equal("3 chars", meta[1].ValidationText);
        Assert.Equal("[CODE] & \"-n\"", meta[2].DefaultValueExpression);

        IndexMetadata index = Assert.Single(await reader.ListIndexesAsync("T", Ct));
        Assert.Equal("IX_Code", index.Name);
        Assert.Equal("CODE", Assert.Single(index.Columns).Name);

        DataTable rows = await reader.ReadTableAsync("T", cancellationToken: Ct);
        Assert.Equal("CODE", rows.Columns[1].ColumnName);
        Assert.Equal(
            ["1|ABC|ABC-n", "2|XYZ|XYZ-n", "5|UVW|UVW-n"],
            rows.AsEnumerable().Select(r => $"{r[0]}|{r[1]}|{r[2]}").Order(StringComparer.Ordinal));
    }

    /// <summary>
    /// A case-only rename rewrites the calculated expressions that name the
    /// column, bracketed or bare, to the new spelling, and they keep evaluating.
    /// </summary>
    /// <param name="mode">How the writer runs the rename and the insert after it.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(AllModes))]
    public async Task RenameColumn_CaseOnly_RewritesCalculatedExpressions(WriteMode mode)
    {
        await using MemoryStream ms = await CreateDatabaseAsync(DatabaseFormat.AceAccdb);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync(
                "C",
                [
                    new("Id", typeof(int)),
                    new("Price", typeof(double)),
                    new("Twice", typeof(double)) { IsCalculated = true, CalculationExpression = "[Price]*2" },
                    new("Bare", typeof(double)) { IsCalculated = true, CalculationExpression = "price + 1" },
                ],
                Ct);
            await writer.InsertRowAsync("C", [1, 2.5d, null, null], Ct);
        }

        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await WriteModes.RunAsync(
                writer,
                mode,
                async () =>
                {
                    await writer.RenameColumnAsync("C", "Price", "PRICE", Ct);
                    await writer.InsertRowAsync("C", [2, 3d, null, null], Ct);
                },
                Ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync("C", Ct);
        Assert.Equal(["Id", "PRICE", "Twice", "Bare"], meta.Select(c => c.Name));
        Assert.Equal("[PRICE]*2", meta[2].CalculationExpression);
        Assert.Equal("[PRICE] + 1", meta[3].CalculationExpression);

        DataTable rows = await reader.ReadTableAsync("C", cancellationToken: Ct);
        Assert.Equal(
            ["1|2.5|5|3.5", "2|3|6|4"],
            rows.AsEnumerable().Select(r => $"{r["Id"]}|{Number(r["PRICE"])}|{Number(r["Twice"])}|{Number(r["Bare"])}").Order(StringComparer.Ordinal));
    }

    /// <summary>
    /// The renamed column is left out of the duplicate-name check, but every
    /// other column is still in it: a rename onto another column's name is
    /// refused whatever its case, and the table is unchanged.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task RenameColumn_OntoAnotherColumnsNameInAnyCase_Throws(DatabaseFormat format)
    {
        await using MemoryStream ms = await CreateCodeTableAsync(format);

        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.RenameColumnAsync("T", "Code", "code", Ct);
            foreach (string taken in TakenNames)
            {
                JetObjectExistsException exception = await Assert.ThrowsAsync<JetObjectExistsException>(async () =>
                    await writer.RenameColumnAsync("T", "code", taken, Ct));
                Assert.Equal($"Column '{taken}' already exists in table 'T'.", exception.Message);
            }
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Equal(["Id", "code", "Note"], (await reader.GetColumnMetadataAsync("T", Ct)).Select(c => c.Name));
        DataRow row = Assert.Single((await reader.ReadTableAsync("T", cancellationToken: Ct)).AsEnumerable());
        Assert.Equal("1|ABC|ABC-n", string.Join("|", row.ItemArray));
    }

    /// <summary>
    /// A rename to the name the column already has, spelled the same, changes
    /// nothing and leaves the table where it is, after checking that the table
    /// and the column exist. The stored spelling decides: naming the column
    /// in another case on both sides is a case-only rename.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task RenameColumn_ToTheNameItAlreadyHas_ChangesNothing(DatabaseFormat format)
    {
        await using MemoryStream ms = await CreateCodeTableAsync(format);
        long tdefPage = await GetTDefPageAsync(ms, "T");

        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.RenameColumnAsync("T", "Code", "Code", Ct);

            JetObjectNotFoundException missingColumn = await Assert.ThrowsAsync<JetObjectNotFoundException>(async () =>
                await writer.RenameColumnAsync("T", "Missing", "Missing", Ct));
            Assert.Equal("oldColumnName", missingColumn.ParamName);

            JetOperationException missingTable = await Assert.ThrowsAsync<JetOperationException>(async () =>
                await writer.RenameColumnAsync("Nope", "Code", "Code", Ct));
            Assert.Contains("'Nope'", missingTable.Message, StringComparison.Ordinal);
        }

        Assert.Equal(tdefPage, await GetTDefPageAsync(ms, "T"));
        await using (AccessReader reader = await OpenReaderAsync(ms))
        {
            Assert.Equal(["Id", "Code", "Note"], (await reader.GetColumnMetadataAsync("T", Ct)).Select(c => c.Name));
        }

        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.RenameColumnAsync("T", "code", "code", Ct);
        }

        await using (AccessReader reader = await OpenReaderAsync(ms))
        {
            IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync("T", Ct);
            Assert.Equal(["Id", "code", "Note"], meta.Select(c => c.Name));
            Assert.Equal("Len([code]) = 3", meta[1].ValidationRuleExpression);
            DataRow row = Assert.Single((await reader.ReadTableAsync("T", cancellationToken: Ct)).AsEnumerable());
            Assert.Equal("1|ABC|ABC-n", string.Join("|", row.ItemArray));
        }
    }

    /// <summary>
    /// Creates a database of <paramref name="format"/> with table <c>T</c>: Id,
    /// Code (rule <c>Len([Code]) = 3</c>, text "3 chars", unique index
    /// <c>IX_Code</c>) and Note (default <c>[Code] &amp; "-n"</c>), holding the
    /// row 1, ABC, ABC-n.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>The database.</returns>
    private static async Task<MemoryStream> CreateCodeTableAsync(DatabaseFormat format)
    {
        MemoryStream ms = await CreateDatabaseAsync(format);
        await using AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct);
        await writer.CreateTableAsync(
            "T",
            [
                new("Id", typeof(int)),
                new("Code", typeof(string), maxLength: 10) { ValidationRuleExpression = "Len([Code]) = 3", ValidationText = "3 chars" },
                new("Note", typeof(string), maxLength: 20) { DefaultValueExpression = "[Code] & \"-n\"" },
            ],
            [new IndexDefinition("IX_Code", "Code") { IsUnique = true }],
            Ct);
        await writer.InsertRowAsync("T", [1, "ABC", DbDefault.Value], Ct);
        return ms;
    }

    private static string Number(object value)
        => Convert.ToDouble(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture);

    private static async Task<MemoryStream> CreateDatabaseAsync(DatabaseFormat format)
    {
        var ms = new MemoryStream();
        await using (await AccessWriter.CreateDatabaseAsync(ms, format, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, cancellationToken: Ct))
        {
        }

        ms.Position = 0;
        return ms;
    }

    private static async Task<long> GetTDefPageAsync(MemoryStream ms, string tableName)
    {
        ms.Position = 0;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(ms, cancellationToken: Ct);
        CatalogEntry? entry = await harness.GetCatalogEntryAsync(tableName, Ct);
        Assert.NotNull(entry);
        return entry.TDefPage;
    }

    private static async Task<AccessWriter> OpenWriterAsync(MemoryStream ms, WriteMode mode)
    {
        ms.Position = 0;
        return await AccessWriter.OpenAsync(ms, WriteModes.WriterOptions(mode), leaveOpen: true, cancellationToken: Ct);
    }

    private static async Task<AccessReader> OpenReaderAsync(MemoryStream ms)
    {
        ms.Position = 0;
        return await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
    }
}
