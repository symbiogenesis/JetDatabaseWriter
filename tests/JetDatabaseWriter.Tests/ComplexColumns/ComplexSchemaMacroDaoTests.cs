namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Native application dependencies in the untouched Northwind complex-table fixture.</summary>
public sealed class ComplexSchemaMacroDaoTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>
    /// The fixture's parent data macro requires an Access application function that DAO cannot evaluate.
    /// ComplexSchemaDaoTests therefore verifies native read/compact for this fixture while its other
    /// fixtures retain native complex writes; preserving application metadata must preserve this refusal.
    /// </summary>
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task OriginalNorthwindMacro_RefusesParentUpdateButReadsAndCompacts()
    {
        await using AccessRoundTripSession session = await AccessRoundTripSession.CreateFromNorthwindAsync(Ct, "JetDatabaseWriter.Tests.ComplexSchemaMacro");
        (int Id, string Name)[] originalRows = await ReadRowsAsync(session.SourcePath);
        Assert.NotEmpty(originalRows);
        const string nativeControl = """
            $rs = $db.OpenRecordset('ProductCategories')
            try {
                $rs.Edit()
                $refused = $false
                try {
                    $rs.Update()
                } catch {
                    $expected = @($engine.Errors | Where-Object {
                        $_.Number -eq 3892 -and $_.Description.Contains("'GetAuditFieldsUserName'")
                    })
                    if ($expected.Count -ne 1 -or !$_.Exception.Message.Contains("'GetAuditFieldsUserName'")) { throw }
                    $refused = $true
                    $rs.CancelUpdate()
                }
                if (!$refused) { throw 'The untouched fixture unexpectedly accepted its application-dependent parent data macro.' }
                Write-Output 'ORIGINAL_MACRO_REFUSAL=3892:GetAuditFieldsUserName'
            } finally { $rs.Close() }
            """;
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoDatabaseScriptThenCompact(nativeControl, TimeSpan.FromMinutes(2));
        AssertDaoSuccess(result, "ORIGINAL_MACRO_REFUSAL=3892:GetAuditFieldsUserName");
        Assert.Equal(originalRows, await ReadRowsAsync(session.CompactedPath));

        string nativeReadback = $$"""
            $rs = $db.OpenRecordset('SELECT ProductCategoryID, ProductCategoryName FROM ProductCategories ORDER BY ProductCategoryID')
            try {
                $count = 0
                while (!$rs.EOF) {
                    if ([string]::IsNullOrEmpty([string]$rs.Fields('ProductCategoryName').Value)) { throw 'The native category name could not be read.' }
                    $count++
                    $rs.MoveNext()
                }
                if ($count -ne {{originalRows.Length}}) { throw 'Native compact changed the category count.' }
                Write-Output 'ORIGINAL_MACRO_NATIVE_READBACK_OK'
            } finally { $rs.Close() }
            """;
        AssertDaoSuccess(session.RunDaoDatabaseScript(session.CompactedPath, nativeReadback, TimeSpan.FromMinutes(2)), "ORIGINAL_MACRO_NATIVE_READBACK_OK");
    }

    private static async Task<(int Id, string Name)[]> ReadRowsAsync(string path)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, Ct);
        using DataTable rows = await reader.ReadTableAsync("ProductCategories", cancellationToken: Ct);
        return [.. rows.AsEnumerable().Select(row => (Id: Assert.IsType<int>(row["ProductCategoryID"]), Name: Assert.IsType<string>(row["ProductCategoryName"]))).OrderBy(row => row.Id)];
    }

    private static void AssertDaoSuccess(AccessRoundTripEnvironment.CompactResult result, string marker)
    {
        Assert.True(result.ExitCode == 0, $"DAO failed with exit {result.ExitCode}.{Environment.NewLine}{result.StdOut}{Environment.NewLine}{result.StdErr}");
        Assert.Contains(marker, result.StdOut, StringComparison.Ordinal);
    }
}
