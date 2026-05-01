#if DIAG_ROUNDTRIP
namespace JetDatabaseWriter.Tests.Core;

using System;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Narrow bisection of the round-trip failure trigger.</summary>
public sealed class AccessRoundTripDiagNarrow
{
    public static bool Available => AccessRoundTripEnvironment.IsAvailable;

    private static async Task RunAsync(string suffix, Func<AccessWriter, Task> setup)
    {
        string work = Path.Combine(Path.GetTempPath(), "diagN_" + suffix + "_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(work);
        string src = Path.Combine(work, "src.accdb");
        string dst = Path.Combine(work, "dst.accdb");
        File.Copy(TestDatabases.NorthwindTraders, src, overwrite: true);
        File.SetAttributes(src, File.GetAttributes(src) & ~FileAttributes.ReadOnly);
        var ct = TestContext.Current.CancellationToken;
        await using (var w = await AccessWriter.OpenAsync(src, new AccessWriterOptions { UseLockFile = false }, ct))
        {
            await setup(w);
        }

        var r = AccessRoundTripEnvironment.RunDaoCompact(src, dst, TimeSpan.FromMinutes(2));
        Assert.True(r.ExitCode == 0, $"compact failed: ec={r.ExitCode}\n{r.StdErr}");
    }

    // Failing original shape:
    [Fact(SkipUnless = nameof(Available), Skip = "diag", SkipType = typeof(AccessRoundTripDiagNarrow))]
    public Task N1_Original_Failing_Shape() => RunAsync("N1", async w =>
    {
        await w.CreateTableAsync("RT_Customers", [new("CustomerID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false }, new("Name", typeof(string), maxLength: 100) { IsNullable = false }], TestContext.Current.CancellationToken);
        await w.CreateTableAsync("RT_Orders", [new("OrderID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false }, new("CustomerID", typeof(int)) { IsNullable = false }, new("OrderDate", typeof(DateTime))], TestContext.Current.CancellationToken);
    });

    // Same as N1 but Name nullable:
    [Fact(SkipUnless = nameof(Available), Skip = "diag", SkipType = typeof(AccessRoundTripDiagNarrow))]
    public Task N2_NameNullable() => RunAsync("N2", async w =>
    {
        await w.CreateTableAsync("RT_Customers", [new("CustomerID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false }, new("Name", typeof(string), maxLength: 100) { IsNullable = true }], TestContext.Current.CancellationToken);
        await w.CreateTableAsync("RT_Orders", [new("OrderID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false }, new("CustomerID", typeof(int)) { IsNullable = false }, new("OrderDate", typeof(DateTime))], TestContext.Current.CancellationToken);
    });

    // Same as N1 but only the parent table:
    [Fact(SkipUnless = nameof(Available), Skip = "diag", SkipType = typeof(AccessRoundTripDiagNarrow))]
    public Task N3_OnlyParent() => RunAsync("N3", async w =>
    {
        await w.CreateTableAsync("RT_Customers", [new("CustomerID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false }, new("Name", typeof(string), maxLength: 100) { IsNullable = false }], TestContext.Current.CancellationToken);
    });

    // Same as N1 but only the child table:
    [Fact(SkipUnless = nameof(Available), Skip = "diag", SkipType = typeof(AccessRoundTripDiagNarrow))]
    public Task N4_OnlyChild() => RunAsync("N4", async w =>
    {
        await w.CreateTableAsync("RT_Orders", [new("OrderID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false }, new("CustomerID", typeof(int)) { IsNullable = false }, new("OrderDate", typeof(DateTime))], TestContext.Current.CancellationToken);
    });

    // Same as N1 but child table FIRST then parent:
    [Fact(SkipUnless = nameof(Available), Skip = "diag", SkipType = typeof(AccessRoundTripDiagNarrow))]
    public Task N5_ReverseOrder() => RunAsync("N5", async w =>
    {
        await w.CreateTableAsync("RT_Orders", [new("OrderID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false }, new("CustomerID", typeof(int)) { IsNullable = false }, new("OrderDate", typeof(DateTime))], TestContext.Current.CancellationToken);
        await w.CreateTableAsync("RT_Customers", [new("CustomerID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false }, new("Name", typeof(string), maxLength: 100) { IsNullable = false }], TestContext.Current.CancellationToken);
    });

    // Same as N1 but no NotNull anywhere:
    [Fact(SkipUnless = nameof(Available), Skip = "diag", SkipType = typeof(AccessRoundTripDiagNarrow))]
    public Task N6_NoNotNull() => RunAsync("N6", async w =>
    {
        await w.CreateTableAsync("RT_Customers", [new("CustomerID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true }, new("Name", typeof(string), maxLength: 100)], TestContext.Current.CancellationToken);
        await w.CreateTableAsync("RT_Orders", [new("OrderID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true }, new("CustomerID", typeof(int)), new("OrderDate", typeof(DateTime))], TestContext.Current.CancellationToken);
    });

    // Same as N1 but two completely unrelated tables (no name overlap with Northwind):
    [Fact(SkipUnless = nameof(Available), Skip = "diag", SkipType = typeof(AccessRoundTripDiagNarrow))]
    public Task N7_NoNameCollision() => RunAsync("N7", async w =>
    {
        await w.CreateTableAsync("ZZ_Foobars", [new("CustomerID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false }, new("Name", typeof(string), maxLength: 100) { IsNullable = false }], TestContext.Current.CancellationToken);
        await w.CreateTableAsync("ZZ_Bazquxxxx", [new("OrderID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false }, new("CustomerID", typeof(int)) { IsNullable = false }, new("OrderDate", typeof(DateTime))], TestContext.Current.CancellationToken);
    });

    // Just RT_Customers + a benign second table:
    [Fact(SkipUnless = nameof(Available), Skip = "diag", SkipType = typeof(AccessRoundTripDiagNarrow))]
    public Task N8_RTCustomers_thenZZ() => RunAsync("N8", async w =>
    {
        await w.CreateTableAsync("RT_Customers", [new("CustomerID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false }, new("Name", typeof(string), maxLength: 100) { IsNullable = false }], TestContext.Current.CancellationToken);
        await w.CreateTableAsync("ZZ_Bazquxxxx", [new("OrderID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false }, new("CustomerID", typeof(int)) { IsNullable = false }, new("OrderDate", typeof(DateTime))], TestContext.Current.CancellationToken);
    });

    // Just ZZ + RT_Orders:
    [Fact(SkipUnless = nameof(Available), Skip = "diag", SkipType = typeof(AccessRoundTripDiagNarrow))]
    public Task N9_ZZ_thenRTOrders() => RunAsync("N9", async w =>
    {
        await w.CreateTableAsync("ZZ_Foobars", [new("CustomerID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false }, new("Name", typeof(string), maxLength: 100) { IsNullable = false }], TestContext.Current.CancellationToken);
        await w.CreateTableAsync("RT_Orders", [new("OrderID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false }, new("CustomerID", typeof(int)) { IsNullable = false }, new("OrderDate", typeof(DateTime))], TestContext.Current.CancellationToken);
    });

    // Same as N1 but unique alphabetical order: aRT_xx, bRT_yy:
    [Fact(SkipUnless = nameof(Available), Skip = "diag", SkipType = typeof(AccessRoundTripDiagNarrow))]
    public Task N10_DifferentNamesSameLengths() => RunAsync("N10", async w =>
    {
        await w.CreateTableAsync("RT_Cusxxxxxx", [new("CustomerID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false }, new("Name", typeof(string), maxLength: 100) { IsNullable = false }], TestContext.Current.CancellationToken);
        await w.CreateTableAsync("RT_Ordxxxxx", [new("OrderID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false }, new("CustomerID", typeof(int)) { IsNullable = false }, new("OrderDate", typeof(DateTime))], TestContext.Current.CancellationToken);
    });

    // Length boundaries on second name
    [Fact(SkipUnless = nameof(Available), Skip = "diag", SkipType = typeof(AccessRoundTripDiagNarrow))]
    public Task N11_Second11chars() => RunAsync("N11", async w =>
    {
        await w.CreateTableAsync("RT_Customers", [new("CustomerID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false }, new("Name", typeof(string), maxLength: 100) { IsNullable = false }], TestContext.Current.CancellationToken);
        await w.CreateTableAsync("RT_Ord12345", [new("OrderID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false }, new("CustomerID", typeof(int)) { IsNullable = false }, new("OrderDate", typeof(DateTime))], TestContext.Current.CancellationToken);
    });

    [Fact(SkipUnless = nameof(Available), Skip = "diag", SkipType = typeof(AccessRoundTripDiagNarrow))]
    public Task N12_Second12chars() => RunAsync("N12", async w =>
    {
        await w.CreateTableAsync("RT_Customers", [new("CustomerID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false }, new("Name", typeof(string), maxLength: 100) { IsNullable = false }], TestContext.Current.CancellationToken);
        await w.CreateTableAsync("RT_Ord123456", [new("OrderID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false }, new("CustomerID", typeof(int)) { IsNullable = false }, new("OrderDate", typeof(DateTime))], TestContext.Current.CancellationToken);
    });

    [Fact(SkipUnless = nameof(Available), Skip = "diag", SkipType = typeof(AccessRoundTripDiagNarrow))]
    public Task N13_Second10chars() => RunAsync("N13", async w =>
    {
        await w.CreateTableAsync("RT_Customers", [new("CustomerID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false }, new("Name", typeof(string), maxLength: 100) { IsNullable = false }], TestContext.Current.CancellationToken);
        await w.CreateTableAsync("RT_Ord1234", [new("OrderID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false }, new("CustomerID", typeof(int)) { IsNullable = false }, new("OrderDate", typeof(DateTime))], TestContext.Current.CancellationToken);
    });

    [Fact(SkipUnless = nameof(Available), Skip = "diag", SkipType = typeof(AccessRoundTripDiagNarrow))]
    public Task N14_Both6chars() => RunAsync("N14", async w =>
    {
        await w.CreateTableAsync("RTCust", [new("CustomerID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false }, new("Name", typeof(string), maxLength: 100) { IsNullable = false }], TestContext.Current.CancellationToken);
        await w.CreateTableAsync("RTOrds", [new("OrderID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false }, new("CustomerID", typeof(int)) { IsNullable = false }, new("OrderDate", typeof(DateTime))], TestContext.Current.CancellationToken);
    });
}
#endif
