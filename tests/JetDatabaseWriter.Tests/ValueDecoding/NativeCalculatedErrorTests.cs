namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Native calculated errors remain values distinct from ordinary Null.</summary>
public sealed class NativeCalculatedErrorTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("native-calculated-errors.accdb", "FC789B7ECD75FE9E371F69C2432D077660143BB3BD4C801C75DF838D265097EE")]
    [InlineData("native-calculated-errors-compacted.accdb", "21CFBDD1801B32F852E8D9FCBC49A4BC71668979C78BA7A4FADA558D609E4D4F")]
    public async Task NativeErrors_PublicRowsKeepDistinctErrorValues(string fixture, string hash)
    {
        ArgumentNullException.ThrowIfNull(fixture);
        string path = Path.Combine(AppContext.BaseDirectory, "Databases", "CalculatedErrors", fixture);
        Assert.Equal(hash, Convert.ToHexString(SHA256.HashData(await File.ReadAllBytesAsync(path, Ct))));
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, Ct);
        object[][] rows = (await reader.Rows("Overflow", cancellationToken: Ct).ToArrayAsync(Ct)).OrderBy(row => row[0]).ToArray();
        CalculatedError error = Assert.IsType<CalculatedError>(rows[0][4]);
        Assert.Equal(6, error.NativeCode);
        Assert.Equal("#Error(6)", error.ToString());
        Assert.Equal(DBNull.Value, rows[1][4]);
        Assert.Equal((byte)1, rows[2][4]);
        Assert.Equal(13, Assert.IsType<CalculatedError>(rows[0][14]).NativeCode);
        string[][] strings = await reader.RowsAsStrings("Overflow", cancellationToken: Ct).ToArrayAsync(Ct);
        Assert.Equal("#Error(6)", Assert.Single(strings, row => row[0] == "1")[4]);
        Assert.Equal(string.Empty, Assert.Single(strings, row => row[0] == "2")[4]);
        using DataTable table = await reader.ReadTableAsync("Overflow", cancellationToken: Ct);
        Assert.Equal(typeof(object), table.Columns[4].DataType);
        Assert.Equal(error, Assert.Single(table.Rows.Cast<DataRow>(), row => (int)row[0] == 1)[4]);
        ErrorRow[] mapped = await reader.Rows<ErrorRow>("Overflow", cancellationToken: Ct).ToArrayAsync(Ct);
        Assert.Equal(error, Assert.Single(mapped, row => row.Id == 1).Byte);
        Assert.Null(Assert.Single(mapped, row => row.Id == 2).Byte);
        InvalidCastException incompatible = await Assert.ThrowsAsync<InvalidCastException>(async () => await reader.Rows<ByteRow>("Overflow", cancellationToken: Ct).ToArrayAsync(Ct));
        Assert.Contains("ByteRow.Byte", incompatible.Message, StringComparison.Ordinal);

        object[][] propagation = (await reader.Rows("Propagation", cancellationToken: Ct).ToArrayAsync(Ct)).OrderBy(row => row[0]).ToArray();
        foreach (int ordinal in new[] { 5, 12, 13 })
        {
            // Native CompactDatabase clears a dependency's -3030 cache while
            // retaining the original source field's overflow error.
            if (fixture.Contains("compacted", StringComparison.Ordinal))
            {
                Assert.Equal(DBNull.Value, propagation[0][ordinal]);
            }
            else
            {
                Assert.Equal(-3030, Assert.IsType<CalculatedError>(propagation[0][ordinal]).NativeCode);
            }
        }

        object[][] functions = await reader.Rows("FunctionNull", cancellationToken: Ct).ToArrayAsync(Ct);
        Assert.All(functions[0][1..8], value => Assert.Equal(13, Assert.IsType<CalculatedError>(value).NativeCode));
        Assert.Equal(94, Assert.IsType<CalculatedError>(functions[0][8]).NativeCode);
    }

    [Fact]
    public async Task WriterErrors_RecomputeDependenciesRepairAndRefuseInvalidStorageAtomically()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, Enums.DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync("T", [new("Id", typeof(int)), new("N", typeof(double)), new("Bad", typeof(byte)) { IsCalculated = true, CalculationExpression = "[N]" }, new("Dependent", typeof(int)) { IsCalculated = true, CalculationExpression = "[Bad]+1" }, new("Ignore", typeof(int)) { IsCalculated = true, CalculationExpression = "IIf(False,[Bad],42)" }, new("AndFalse", typeof(bool)) { IsCalculated = true, CalculationExpression = "False And [Bad]" }], Ct);
            await writer.InsertRowAsync("T", new RowValues { ["Id"] = 1, ["N"] = 300d }, Ct);
            await AssertRowAsync(stream, 6, -3030);
            await writer.UpdateRowsAsync("T", RowCriteria.Where("Id", 1), new RowValues { ["N"] = 2d }, Ct);
            await AssertRowAsync(stream, 0, 0);
            await writer.UpdateRowsAsync("T", RowCriteria.Where("Id", 1), new RowValues { ["N"] = 300d }, Ct);
            await writer.CreateTableAsync("Required", [new("N", typeof(double)), new("Bad", typeof(byte)) { IsCalculated = true, CalculationExpression = "[N]", IsNullable = false }], Ct);
            byte[] before = stream.ToArray();
            await Assert.ThrowsAsync<JetValidationRuleException>(async () => await writer.InsertRowAsync("Required", new RowValues { ["N"] = 300d }, Ct));
            Assert.Equal(before, stream.ToArray());
        }
    }

    [Fact(Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason, SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable), SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task WriterErrors_NativeReadRepairAndCompactAcceptEmittedStatuses()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.CalculatedErrors");
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(session.SourcePath, Enums.DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            await writer.CreateTableAsync("T", [new("Id", typeof(int)), new("N", typeof(double)), new("Bad", typeof(byte)) { IsCalculated = true, CalculationExpression = "[N]" }, new("Dependent", typeof(int)) { IsCalculated = true, CalculationExpression = "[Bad]+1" }, new("NullTest", typeof(bool)) { IsCalculated = true, CalculationExpression = "IsNull([Bad])" }], Ct);
            await writer.InsertRowAsync("T", new RowValues { ["Id"] = 1, ["N"] = 300d }, Ct);
            await writer.InsertRowAsync("T", new RowValues { ["Id"] = 2, ["N"] = 2d }, Ct);
        }

        AccessRoundTripEnvironment.CompactResult result = session.RunDaoDatabaseScriptThenCompact(
            """
            $rs=$db.OpenRecordset('SELECT * FROM [T] ORDER BY [Id]')
            try {
                foreach($name in @('Bad','Dependent','NullTest')) {if($null -ne $rs.Fields($name).Value){throw ('Expected calculation error in '+$name)}}
                $rs.MoveNext()
                if($rs.Fields('Bad').Value -ne 2 -or $rs.Fields('Dependent').Value -ne 3){throw 'Valid control changed'}
            } finally {$rs.Close()}
            $db.Execute('UPDATE [T] SET [N]=3 WHERE [Id]=1',128)
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, Ct);
        object[][] rows = await reader.Rows("T", cancellationToken: Ct).ToArrayAsync(Ct);
        Assert.Equal((byte)3, rows[0][2]);
        Assert.Equal(4, rows[0][3]);
    }

    [Fact(Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason, SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable), SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task WriterErrorCodes_NativeRecalculationAndCompactRetainStatuses()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.CalculatedErrorCodes");
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(session.SourcePath, Enums.DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            await writer.CreateTableAsync("T", [new("Id", typeof(int)), new("InvalidCall", typeof(double)) { IsCalculated = true, CalculationExpression = "Asc(\"\")" }, new("Overflow", typeof(byte)) { IsCalculated = true, CalculationExpression = "300" }, new("Division", typeof(double)) { IsCalculated = true, CalculationExpression = "1/0" }, new("Mismatch", typeof(Guid)) { IsCalculated = true, CalculationExpression = "1" }, new("InvalidNull", typeof(double)) { IsCalculated = true, CalculationExpression = "CDbl(Null)" }, new("Automation", typeof(double)) { IsCalculated = true, CalculationExpression = "(-1)^.5" }], Ct);
            await writer.InsertRowAsync("T", new RowValues { ["Id"] = 1 }, Ct);
        }

        AccessRoundTripEnvironment.CompactResult result = session.RunDaoDatabaseScriptThenCompact(
            """
            $rs=$db.OpenRecordset('SELECT * FROM [T]')
            try {foreach($field in $rs.Fields){if($field.Name -ne 'Id' -and $null -ne $field.Value){throw ('Expected calculation error in '+$field.Name)}}} finally {$rs.Close()}
            $db.Execute('UPDATE [T] SET [Id]=2',128)
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, Ct);
        object[] row = Assert.Single(await reader.Rows("T", cancellationToken: Ct).ToArrayAsync(Ct));
        Assert.Equal(2, row[0]);
        int[] expected = [5, 6, 11, 13, 94, 440];
        for (int index = 0; index < expected.Length; index++)
        {
            Assert.Equal(expected[index], Assert.IsType<CalculatedError>(row[index + 1]).NativeCode);
        }
    }

    private static async Task AssertRowAsync(MemoryStream stream, int errorCode, int dependencyCode)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        object[] row = Assert.Single(await reader.Rows("T", cancellationToken: Ct).ToArrayAsync(Ct));
        if (errorCode == 0)
        {
            Assert.Equal((byte)2, row[2]);
            Assert.Equal(3, row[3]);
        }
        else
        {
            Assert.Equal(errorCode, Assert.IsType<CalculatedError>(row[2]).NativeCode);
            Assert.Equal(dependencyCode, Assert.IsType<CalculatedError>(row[3]).NativeCode);
        }

        Assert.Equal(42, row[4]);
        Assert.False(Assert.IsType<bool>(row[5]));
    }

#pragma warning disable CA1812 // Public materialization constructs these projection targets.
    private sealed class ErrorRow
    {
        public int Id { get; set; }

        public object? Byte { get; set; }
    }

    private sealed class ByteRow
    {
        public byte? Byte { get; set; }
    }
#pragma warning restore CA1812
}
