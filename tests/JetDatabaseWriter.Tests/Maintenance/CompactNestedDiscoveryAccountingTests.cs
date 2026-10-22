namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Maintenance;
using Xunit;

/// <summary>Nested authority work admits before execution and releases both working-storage accounts.</summary>
public sealed class CompactNestedDiscoveryAccountingTests
{
    [Fact]
    public void ParentWorkRefusal_RetainsChildAttemptWithoutExceedingParent()
    {
        var parent = new CompactDiscoveryAccounting(new() { MaxRows = 1 });
        var child = new CompactDiscoveryAccounting(new() { MaxRows = 10 }, parent);
        child.Charge(CompactDiscoveryCharge.Rows, 1);
        Assert.Throws<JetLimitationException>(() => child.Charge(CompactDiscoveryCharge.Rows, 1));
        Assert.Equal(2, child.Rows);
        Assert.Equal(1, parent.Rows);
    }

    [Fact]
    public void ParentStorageRefusal_UnwindsOnlyPendingChildReservation()
    {
        var parent = new CompactDiscoveryAccounting(new() { MaxWorkingBytes = 10 });
        using IDisposable existing = parent.ReserveWorkingBytes(7);
        var child = new CompactDiscoveryAccounting(new() { MaxWorkingBytes = 10 }, parent);
        Assert.Throws<JetLimitationException>(() => child.ReserveWorkingBytes(4));
        Assert.Equal(0, child.ActiveWorkingBytes);
        Assert.Equal(7, parent.ActiveWorkingBytes);
        using IDisposable admitted = child.ReserveWorkingBytes(3);
        Assert.Equal(3, child.ActiveWorkingBytes);
        Assert.Equal(10, parent.ActiveWorkingBytes);
    }

    [Fact]
    public void ConcurrentDisposal_ReleasesEveryLevelExactlyOnce()
    {
        var root = new CompactDiscoveryAccounting(new());
        var parent = new CompactDiscoveryAccounting(new(), root);
        var child = new CompactDiscoveryAccounting(new(), parent);
        IDisposable reservation = child.ReserveWorkingBytes(127);
        Assert.Equal(127, root.ActiveWorkingBytes);
        Parallel.For(0, 64, new ParallelOptions { CancellationToken = TestContext.Current.CancellationToken }, _ => reservation.Dispose());
        Assert.Equal(0, child.ActiveWorkingBytes);
        Assert.Equal(0, parent.ActiveWorkingBytes);
        Assert.Equal(0, root.ActiveWorkingBytes);
    }

    [Fact]
    public void ExcessiveParentDepth_RefusesBeforeRecursiveAccounting()
    {
        var parent = new CompactDiscoveryAccounting(new());
        for (int depth = 0; depth < 16; depth++)
        {
            parent = new(new(), parent);
        }

        Assert.Throws<ArgumentException>(() => new CompactDiscoveryAccounting(new(), parent));
        parent.Charge(CompactDiscoveryCharge.Rows, 1);
        using IDisposable reservation = parent.ReserveWorkingBytes(1);
    }
}
