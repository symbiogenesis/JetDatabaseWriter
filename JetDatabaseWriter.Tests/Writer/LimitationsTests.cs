namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// Pinning tests for the limitations documented in README "Limitations".
/// Each test asserts a documented limitation is in effect today. They are
/// expected to FAIL the day a limitation is lifted — at which point the
/// README, the test, and (for behavioural pins) the relevant write path
/// must be updated together.
/// </summary>
public sealed class LimitationsTests
{
    // ── Schema evolution ──────────────────────────────────────────────

    [Fact]
    public void SchemaEvolution_IAccessWriter_DoesNotExposeIndexOrConstraintApis()
    {
        // README: "No index, primary-key, or foreign-key/cascade enforcement creation."
        // PK creation and MSysRelationships row emission via
        // CreateRelationshipAsync have been lifted. The remaining pinned
        // restrictions: no methods exposing "Index" or "PrimaryKey" or
        // "ForeignKey" — those concepts are configured via property-bearing
        // model types (IndexDefinition, RelationshipDefinition), not via
        // methods on the writer itself.
        AssertNoMethodMatching(typeof(IAccessWriter), "Index");
        AssertNoMethodMatching(typeof(IAccessWriter), "PrimaryKey");
        AssertNoMethodMatching(typeof(IAccessWriter), "ForeignKey");
        AssertNoMethodMatching(typeof(AccessWriter), "Index");
        AssertNoMethodMatching(typeof(AccessWriter), "PrimaryKey");
        AssertNoMethodMatching(typeof(AccessWriter), "ForeignKey");
    }

    // ── Specialized column kinds ──────────────────────────────────────

    [Fact]
    public void SpecializedColumns_NoCalculatedColumnWriteApi()
    {
        // Phase 1A status (see docs/design/calculated-columns-format-notes.md):
        // calc-column metadata round-trips on read (ColumnMetadata.IsCalculated /
        // .CalculationExpression / .CalculatedResultType), but writing calc
        // columns (Phase 1B) and evaluating expressions client-side (Phase 2+)
        // are not yet implemented. CreateTableAsync rejects IsCalculated=true
        // with NotSupportedException; this pin asserts that contract until
        // Phase 1B lifts it.
        Assert.NotNull(typeof(ColumnDefinition).GetProperty("IsCalculated"));
        Assert.NotNull(typeof(ColumnMetadata).GetProperty("IsCalculated"));
        Assert.NotNull(typeof(ColumnMetadata).GetProperty("CalculationExpression"));
    }

    [Fact]
    public async Task SpecializedColumns_CreateTableAsync_RejectsIsCalculated()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        await using var writer = await OpenWriterAsync(stream);

        var ex = await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await writer.CreateTableAsync(
                "CalcRejection",
                [
                    new("Id", typeof(int)) { IsAutoIncrement = true, IsPrimaryKey = true },
                    new("Computed", typeof(string), maxLength: 100)
                    {
                        IsCalculated = true,
                        CalculationExpression = "[Id] & \" row\"",
                        CalculatedResultType = 0x0A, // T_TEXT
                    },
                ],
                TestContext.Current.CancellationToken));

        Assert.Contains("writing calculated columns is not yet implemented", ex.Message, StringComparison.Ordinal);
    }

    // ── Forms, reports, macros, queries, VBA ──────────────────────────

    [Fact]
    public void FormsReportsMacrosQueriesVba_NoPublicWriteApi()
    {
        // README: "Out of scope. The library targets the JET storage layer only.
        //          MSysObjects entries of type Form, Report, Macro, Module, or
        //          Query are preserved on disk but are neither parsed nor editable."
        //
        // Pin write/edit-style methods only — we use verb+noun pairs so legitimate
        // members like 'DatabaseFormat' (contains 'Form') don't false-positive.
        string[] verbs = ["Create", "Insert", "Add", "Update", "Drop", "Delete", "Remove", "Edit", "Save", "Compile", "Execute", "Run"];
        string[] nouns = ["Form", "Report", "Macro", "Module", "Query", "Vba"];
        foreach (string verb in verbs)
        {
            foreach (string noun in nouns)
            {
                AssertNoMethodMatching(typeof(IAccessWriter), verb + noun);
                AssertNoMethodMatching(typeof(AccessWriter), verb + noun);
            }
        }
    }

    // ── Concurrency ───────────────────────────────────────────────────

    [Fact]
    public void Concurrency_AccessWriter_DoesNotExposeRowLockingApi()
    {
        // Page-level byte-range locks (Phase 2) and explicit transactions
        // (Phase 3) are now shipped — only row-level locking remains absent.
        AssertNoMethodMatching(typeof(IAccessWriter), "Lock");
        AssertNoMethodMatching(typeof(AccessWriter), "AcquirePageLock");
        AssertNoMethodMatching(typeof(AccessWriter), "AcquireRowLock");
    }

    // ── Helpers ───────────────────────────────────────────────────────

    private static void AssertNoMethodMatching(Type type, string substring)
    {
        var matches = type
            .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static)
            .Where(m => m.Name.Contains(substring, StringComparison.OrdinalIgnoreCase))
            .Select(m => m.Name)
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        string message = $"Type '{type.FullName}' must not expose any public method whose name contains '{substring}'. Found: {string.Join(", ", matches)}";
        Assert.True(matches.Length == 0, message);
    }

    private static async ValueTask<MemoryStream> CreateFreshAccdbStreamAsync()
    {
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
        }

        ms.Position = 0;
        return ms;
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(
            stream,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
    }
}
