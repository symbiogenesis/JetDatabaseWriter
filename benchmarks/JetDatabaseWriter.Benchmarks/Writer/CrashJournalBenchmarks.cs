namespace JetDatabaseWriter.Benchmarks.Writer;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Pages.Paging;

/// <summary>Compares grouped and individual file transactions over the same raw page updates.</summary>
[MemoryDiagnoser]
public class CrashJournalBenchmarks
{
    private const int PageSize = 4096;
    private const int Updates = 8;
    private readonly byte[] changedPage = new byte[PageSize];
    private string directory = string.Empty;
    private string baseline = string.Empty;
    private string database = string.Empty;

    /// <summary>Gets or sets the original file size in MiB.</summary>
    [Params(16, 128)]
    public int DatabaseMegabytes { get; set; }

    /// <summary>Builds an unmeasured raw-page baseline used by both transaction shapes.</summary>
    [GlobalSetup]
    public void Setup()
    {
        this.directory = Directory.CreateDirectory(Path.Combine(Path.GetTempPath(), "jdw-journal-benchmark-" + Guid.NewGuid().ToString("N"))).FullName;
        this.baseline = Path.Combine(this.directory, "baseline.mdb");
        this.database = Path.Combine(this.directory, "iteration.mdb");
        byte[] buffer = new byte[65536];
        Array.Fill(buffer, (byte)17);
        using FileStream file = File.Create(this.baseline);
        for (long remaining = (long)this.DatabaseMegabytes * 1024 * 1024; remaining > 0; remaining -= buffer.Length)
        {
            file.Write(buffer, 0, buffer.Length);
        }

        Array.Fill(this.changedPage, (byte)42);
    }

    /// <summary>Copies the baseline outside measurement so iterations never accumulate writes.</summary>
    [IterationSetup]
    public void Prepare() => File.Copy(this.baseline, this.database, overwrite: true);

    /// <summary>Measures eight independently committed one-page transactions.</summary>
    /// <returns>The mutation completion.</returns>
    [Benchmark]
    public Task IndividualTransactions() => this.UpdateAsync(grouped: false);

    /// <summary>Measures the same eight page updates committed together.</summary>
    /// <returns>The mutation completion.</returns>
    [Benchmark]
    public Task GroupedTransaction() => this.UpdateAsync(grouped: true);

    /// <summary>Verifies the committed pages and cleanup after every measured invocation.</summary>
    /// <exception cref="InvalidOperationException">The committed page bytes or journal cleanup are incorrect.</exception>
    [IterationCleanup]
    public void Verify()
    {
        using FileStream file = File.OpenRead(this.database);
        if (file.Length != (long)this.DatabaseMegabytes * 1024 * 1024 || File.Exists(PersistentRollbackJournal.JournalPath(this.database)))
        {
            throw new InvalidOperationException("The journal benchmark did not commit and clean up completely.");
        }

        byte[] page = new byte[PageSize];
        for (int index = 0; index <= Updates; index++)
        {
            file.ReadExactly(page);
            byte expected = index < Updates ? (byte)42 : (byte)17;
            foreach (byte value in page)
            {
                if (value != expected)
                {
                    throw new InvalidOperationException("The journal benchmark changed the wrong raw page bytes.");
                }
            }
        }
    }

    /// <summary>Removes this benchmark's isolated files.</summary>
    [GlobalCleanup]
    public void Cleanup() => Directory.Delete(this.directory, recursive: true);

    private async Task UpdateAsync(bool grouped)
    {
        await using var file = new FileStream(this.database, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        await using var store = new StreamPageStore(file, leaveOpen: true);
        if (grouped)
        {
            using var journal = PersistentRollbackJournal.Create(file, PageSize);
            store.RollbackJournal = journal;
            for (int index = 0; index < Updates; index++)
            {
                await store.WriteAsync((long)index * PageSize, this.changedPage, CancellationToken.None);
            }

            journal.Complete();
        }
        else
        {
            for (int index = 0; index < Updates; index++)
            {
                using var journal = PersistentRollbackJournal.Create(file, PageSize);
                store.RollbackJournal = journal;
                await store.WriteAsync((long)index * PageSize, this.changedPage, CancellationToken.None);
                journal.Complete();
            }
        }
    }
}
