namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Maintenance;
using Xunit;

public sealed class CompactDiscoveryAccountingTests
{
    [Fact]
    public void ConcurrentCharges_RespectLimitWithoutLostUpdates()
    {
        var accounting = new CompactDiscoveryAccounting(new() { MaxRows = 127 });
        Parallel.For(0, 1024, new ParallelOptions { CancellationToken = TestContext.Current.CancellationToken }, _ =>
        {
            try
            {
                accounting.Charge(CompactDiscoveryCharge.Rows, 1);
            }
            catch (JetLimitationException)
            {
                // Rejected charges leave the admitted total unchanged.
            }
        });

        Assert.Equal(127, accounting.Rows);
        Assert.Equal(0, accounting.PageReads);
    }

    [Fact]
    public void ConcurrentReservations_RespectLimitAndReleaseExactlyOnce()
    {
        var accounting = new CompactDiscoveryAccounting(new() { MaxWorkingBytes = 127 });
        var reservations = new IDisposable?[1024];
        var options = new ParallelOptions { CancellationToken = TestContext.Current.CancellationToken };
        Parallel.For(0, reservations.Length, options, index =>
        {
            try
            {
                reservations[index] = accounting.ReserveWorkingBytes(1);
            }
            catch (JetLimitationException)
            {
                // Keep successful reservations live until every admission finishes.
            }
        });

        Assert.Equal(127, accounting.ActiveWorkingBytes);
        Parallel.For(0, reservations.Length * 2, options, index => reservations[index / 2]?.Dispose());
        Assert.Equal(0, accounting.ActiveWorkingBytes);
        using IDisposable full = accounting.ReserveWorkingBytes(127);
        Assert.Equal(127, accounting.ActiveWorkingBytes);
    }

    [Fact]
    public void RejectedAdmission_DoesNotConsumeAllowanceOrOverflow()
    {
        var accounting = new CompactDiscoveryAccounting(new() { MaxRows = long.MaxValue, MaxWorkingBytes = long.MaxValue });
        accounting.Charge(CompactDiscoveryCharge.Rows, long.MaxValue - 1);
        Assert.Throws<JetLimitationException>(() => accounting.Charge(CompactDiscoveryCharge.Rows, 2));
        Assert.Equal(long.MaxValue - 1, accounting.Rows);
        accounting.Charge(CompactDiscoveryCharge.Rows, 1);
        accounting.Charge(CompactDiscoveryCharge.Rows, 0);
        Assert.Equal(long.MaxValue, accounting.Rows);

        using (IDisposable almostFull = accounting.ReserveWorkingBytes(long.MaxValue - 1))
        {
            Assert.Throws<JetLimitationException>(() => accounting.ReserveWorkingBytes(2));
            Assert.Equal(long.MaxValue - 1, accounting.ActiveWorkingBytes);
            using IDisposable last = accounting.ReserveWorkingBytes(1);
            Assert.Equal(long.MaxValue, accounting.ActiveWorkingBytes);
        }

        Assert.Equal(0, accounting.ActiveWorkingBytes);
    }
}
