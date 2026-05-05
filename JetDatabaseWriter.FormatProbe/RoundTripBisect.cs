// Bisection probe for round-trip failures. Runs a sequence of escalating
// AccessWriter operations on copies of NorthwindTraders.accdb, then invokes
// DAO compact on each via a bitness-matched PowerShell host, and reports which
// step first breaks DAO.
//
// USAGE
//   $env:DIAG_RT_BISECT = "1"
//   dotnet run --project JetDatabaseWriter.FormatProbe

namespace JetDatabaseWriter.FormatProbe;

using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;

internal static class RoundTripBisect
{
    public static async Task<int> RunAsync(string baselinePath, string workRoot)
    {
        if (!File.Exists(baselinePath))
        {
            await Console.Error.WriteLineAsync($"[bisect] baseline not found: {baselinePath}");
            return 1;
        }

        DaoPowerShellHostResolver.DaoPowerShellHostProbeResult hostProbe = DaoPowerShellHostResolver.Probe();
        if (hostProbe.HostPath is null)
        {
            await Console.Error.WriteLineAsync($"[bisect] {hostProbe.FailureReason}");
            return 1;
        }

        Directory.CreateDirectory(workRoot);

        var steps = new (string Name, Func<AccessWriter, Task> Action)[]
        {
            ("N0_OpenClose",        async w => { await Task.CompletedTask; }),
            ("N1_CreateOneTable",   async w =>
            {
                await w.CreateTableAsync(
                    "RT_Customers",
                    [
                        new("CustomerID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                        new("Name", typeof(string), maxLength: 100) { IsNullable = false },
                    ]);
            }),
            ("N2_CreateTwoTables",  async w =>
            {
                await w.CreateTableAsync(
                    "RT_Customers",
                    [
                        new("CustomerID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                        new("Name", typeof(string), maxLength: 100) { IsNullable = false },
                    ]);
                await w.CreateTableAsync(
                    "RT_Orders",
                    [
                        new("OrderID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                        new("CustomerID", typeof(int)) { IsNullable = false },
                        new("OrderDate", typeof(DateTime)),
                    ]);
            }),
            ("N3_TwoTables_Relationship", async w =>
            {
                await w.CreateTableAsync(
                    "RT_Customers",
                    [
                        new("CustomerID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                        new("Name", typeof(string), maxLength: 100) { IsNullable = false },
                    ]);
                await w.CreateTableAsync(
                    "RT_Orders",
                    [
                        new("OrderID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                        new("CustomerID", typeof(int)) { IsNullable = false },
                        new("OrderDate", typeof(DateTime)),
                    ]);
                await w.CreateRelationshipAsync(new RelationshipDefinition(
                    "RT_FK_Orders_Customers",
                    primaryTable: "RT_Customers",
                    primaryColumn: "CustomerID",
                    foreignTable: "RT_Orders",
                    foreignColumn: "CustomerID")
                {
                    EnforceReferentialIntegrity = true,
                    CascadeDeletes = true,
                });
            }),
            ("N4_TwoTables_Rel_Rows", async w =>
            {
                await w.CreateTableAsync(
                    "RT_Customers",
                    [
                        new("CustomerID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                        new("Name", typeof(string), maxLength: 100) { IsNullable = false },
                    ]);
                await w.CreateTableAsync(
                    "RT_Orders",
                    [
                        new("OrderID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                        new("CustomerID", typeof(int)) { IsNullable = false },
                        new("OrderDate", typeof(DateTime)),
                    ]);
                await w.CreateRelationshipAsync(new RelationshipDefinition(
                    "RT_FK_Orders_Customers",
                    primaryTable: "RT_Customers",
                    primaryColumn: "CustomerID",
                    foreignTable: "RT_Orders",
                    foreignColumn: "CustomerID")
                {
                    EnforceReferentialIntegrity = true,
                    CascadeDeletes = true,
                });
                await w.InsertRowsAsync("RT_Customers", new[]
                {
                    new object[] { DBNull.Value, "Acme" },
                    [DBNull.Value, "Beta"],
                });
                await w.InsertRowsAsync("RT_Orders", new[]
                {
                    new object[] { DBNull.Value, 1, new DateTime(2025, 1, 15) },
                });
            }),
        };

        Console.WriteLine($"[bisect] baseline = {baselinePath}");
        Console.WriteLine($"[bisect] workRoot = {workRoot}");

        foreach (var (name, action) in steps)
        {
            string srcPath = Path.Combine(workRoot, $"{name}.accdb");
            string dstPath = Path.Combine(workRoot, $"{name}.compacted.accdb");
            File.Copy(baselinePath, srcPath, overwrite: true);

            try
            {
                await using (var writer = await AccessWriter.OpenAsync(srcPath, new AccessWriterOptions { UseLockFile = false }))
                {
                    await action(writer);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[bisect] {name}: WRITE FAILED — {ex.GetType().Name}: {ex.Message}");
                continue;
            }

            (int code, string err) = RunDaoCompact(hostProbe.HostPath, srcPath, dstPath);
            string verdict = code == 0 ? "✅ OK"
                : err.Contains("MSysDb", StringComparison.Ordinal) ? "❌ MSysDb"
                : err.Contains("Object invalid", StringComparison.Ordinal) ? "❌ ObjInvalid"
                : $"❌ exit={code}";
            Console.WriteLine($"[bisect] {name}: {verdict}");
            if (code != 0)
            {
                string firstLine = (err ?? string.Empty).Replace("\r\n", " ", StringComparison.Ordinal).Replace('\n', ' ');
                if (firstLine.Length > 250)
                {
                    firstLine = string.Concat(firstLine.AsSpan(0, 250), "…");
                }

                Console.WriteLine($"           stderr: {firstLine}");
            }
        }

        return 0;
    }

    private static (int Code, string StdErr) RunDaoCompact(string powerShellPath, string src, string dst)
    {
        if (File.Exists(dst))
        {
            File.Delete(dst);
        }

        string srcLit = src.Replace("'", "''", StringComparison.Ordinal);
        string dstLit = dst.Replace("'", "''", StringComparison.Ordinal);
        string script =
            "$ErrorActionPreference='Stop'\n" +
            $"$src='{srcLit}'\n$dst='{dstLit}'\n" +
            "$e=New-Object -ComObject DAO.DBEngine.120\n" +
            "try { $e.CompactDatabase($src,$dst); exit 0 } finally { [GC]::Collect() }\n";
        string scriptPath = Path.Combine(Path.GetDirectoryName(dst)!, "compact.ps1");
        File.WriteAllText(scriptPath, script);

        var psi = new ProcessStartInfo(powerShellPath)
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true,
        };
        psi.ArgumentList.Add("-NoProfile");
        psi.ArgumentList.Add("-ExecutionPolicy");
        psi.ArgumentList.Add("Bypass");
        psi.ArgumentList.Add("-File");
        psi.ArgumentList.Add(scriptPath);

        using var p = Process.Start(psi)!;
        string err = p.StandardError.ReadToEnd();
        _ = p.StandardOutput.ReadToEnd();
        p.WaitForExit(120_000);
        return (p.ExitCode, err);
    }
}
