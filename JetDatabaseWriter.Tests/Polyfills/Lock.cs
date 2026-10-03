#if !NET9_0_OR_GREATER
namespace System.Threading;

/// <summary>
/// Stand-in for the .NET 9 <c>System.Threading.Lock</c> on the net8.0 test leg, so tests
/// can declare <c>Lock</c> fields and <c>lock</c> on them on both target frameworks. The
/// compiler lowers <c>lock</c> on a type with this name to <see cref="EnterScope"/>, which
/// enters a private <see cref="Monitor"/> object.
/// </summary>
internal sealed class Lock
{
    private readonly object monitor = new();

    /// <summary>Enters the lock; disposing the returned scope exits it.</summary>
    /// <returns>The scope that exits the lock when disposed.</returns>
    public Scope EnterScope()
    {
        Monitor.Enter(this.monitor);
        return new Scope(this.monitor);
    }

    /// <summary>Holds the lock until disposed.</summary>
    /// <param name="monitor">The entered monitor.</param>
#pragma warning disable CA1515 // The compiler lowers lock only onto a public Lock.Scope.
    public ref struct Scope(object monitor)
#pragma warning restore CA1515
    {
        /// <summary>Exits the lock.</summary>
        public readonly void Dispose() => Monitor.Exit(monitor);
    }
}
#endif
