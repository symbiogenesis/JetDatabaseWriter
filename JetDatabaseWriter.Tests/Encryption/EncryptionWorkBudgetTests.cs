namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Aggregate password work follows linked readers without mutating caller options.</summary>
public sealed class EncryptionWorkBudgetTests
{
    /// <summary>Separate linked reads cannot each spend the root reader's entire password budget.</summary>
    [Fact]
    public async Task LinkedReads_RejectCumulativePasswordWork()
    {
        string source = Path.Combine(TestDatabases.EncryptedRoot, "NativeAceAgile.accdb");
        await using var host = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(host, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateLinkedTableAsync("First", source, "T", TestContext.Current.CancellationToken);
            await writer.CreateLinkedTableAsync("Second", source, "T", TestContext.Current.CancellationToken);
        }

        host.Position = 0;
        var options = new AccessReaderOptions
        {
            UseLockFile = false,
            MaxTotalEncryptionSpinCount = 150_000,
            LinkedSourcePathAllowlist = [TestDatabases.EncryptedRoot],
            LinkedSourcePasswordResolver = static (_, _) => "Native123".AsMemory(),
        };
        await using AccessReader reader = await AccessReader.OpenAsync(host, options, leaveOpen: true, TestContext.Current.CancellationToken);
        Assert.Equal(1, await reader.GetRealRowCountAsync("First", TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<JetLimitationException>(async () => await reader.GetRealRowCountAsync("Second", TestContext.Current.CancellationToken));
        Assert.Equal(2, (await reader.ListLinkedTablesAsync(TestContext.Current.CancellationToken)).Count);
    }

    /// <summary>A refused reservation does not overflow or consume the remaining budget.</summary>
    [Fact]
    public void Charge_RefusesAggregateWorkWithoutOverflow()
    {
        var budget = new EncryptionWorkBudget(100_000);
        budget.Charge(60_000);
        Assert.Throws<JetLimitationException>(() => budget.Charge(int.MaxValue));
        budget.Charge(40_000);
        Assert.Throws<JetLimitationException>(() => budget.Charge(1));
    }

    /// <summary>The native password derivation is refused before encrypted pages are consumed.</summary>
    [Fact]
    public async Task Reader_TotalBudgetStopsAtPageZero()
    {
        byte[] fixture = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeAceAgile.accdb"), TestContext.Current.CancellationToken);
        await using var stream = new MemoryStream(fixture);
        await using var counting = new CountingStream(stream);
        var options = new AccessReaderOptions("Native123") { UseLockFile = false, MaxTotalEncryptionSpinCount = 99_999 };
        await Assert.ThrowsAsync<JetLimitationException>(async () =>
        {
            await using AccessReader reader = await AccessReader.OpenAsync(counting, options, leaveOpen: true, TestContext.Current.CancellationToken);
        });
        Assert.Equal(Constants.PageSizes.Jet4, counting.BytesRead);
    }

    /// <summary>Reusing the caller's options starts an independent budget for each root reader.</summary>
    [Fact]
    public async Task Reader_ReusedOptionsHaveIndependentBudgets()
    {
        string path = Path.Combine(TestDatabases.EncryptedRoot, "NativeAceAgile.accdb");
        var options = new AccessReaderOptions("Native123") { UseLockFile = false, MaxTotalEncryptionSpinCount = 100_000 };
        await using AccessReader first = await AccessReader.OpenAsync(path, options, TestContext.Current.CancellationToken);
        await using AccessReader second = await AccessReader.OpenAsync(path, options, TestContext.Current.CancellationToken);
        Assert.Null(options.EncryptionWorkBudget);
    }

    /// <summary>Linked options share one budget even across sibling and transitive opens.</summary>
    [Fact]
    public void LinkedOptions_ShareAggregateBudget()
    {
        var original = new AccessReaderOptions { MaxTotalEncryptionSpinCount = 100_000 };
        AccessReaderOptions root = original.WithEncryptionWorkBudget();
        AccessReaderOptions first = LinkedTableManager.CreateLinkedSourceOpenOptions(root, string.Empty);
        AccessReaderOptions sibling = LinkedTableManager.CreateLinkedSourceOpenOptions(root, string.Empty);
        AccessReaderOptions nested = LinkedTableManager.CreateLinkedSourceOpenOptions(first, string.Empty);
        Assert.Same(root.EncryptionWorkBudget, first.EncryptionWorkBudget);
        Assert.Same(first.EncryptionWorkBudget, nested.EncryptionWorkBudget);
        first.EncryptionWorkBudget!.Charge(60_000);
        Assert.Throws<JetLimitationException>(() => sibling.EncryptionWorkBudget!.Charge(60_000));
        nested.EncryptionWorkBudget!.Charge(40_000);
        Assert.Null(original.EncryptionWorkBudget);
        Assert.Equal(100_000, sibling.MaxTotalEncryptionSpinCount);
    }

    /// <summary>A negative aggregate budget is invalid even for an unencrypted source.</summary>
    [Fact]
    public void ReaderOptions_NegativeBudgetIsRejected()
    {
        var options = new AccessReaderOptions { MaxTotalEncryptionSpinCount = -1 };
        Assert.Throws<ArgumentOutOfRangeException>(options.WithEncryptionWorkBudget);
    }
}
