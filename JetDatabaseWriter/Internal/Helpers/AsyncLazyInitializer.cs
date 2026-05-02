namespace JetDatabaseWriter.Internal.Helpers;

using System;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Async-friendly single-shot lazy initializer. Replaces the
/// volatile-cache + <see cref="SemaphoreSlim"/> double-checked-lock pattern
/// for state that must be built exactly once on first access.
/// </summary>
/// <typeparam name="T">The cached value type. Must be a reference type so
/// the volatile read can observe a fully-published instance.</typeparam>
/// <remarks>
/// Initializes a new instance of the <see cref="AsyncLazyInitializer{T}"/> class.
/// </remarks>
/// <param name="factory">Factory invoked at most once to materialize the value.</param>
internal sealed class AsyncLazyInitializer<T>(Func<CancellationToken, ValueTask<T>> factory) : IDisposable
    where T : class
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private volatile T? _value;

    /// <summary>Returns the cached value, building it under a single-writer gate on first call.</summary>
    /// <param name="cancellationToken">A token used to cancel the asynchronous build.</param>
    public async ValueTask<T> GetAsync(CancellationToken cancellationToken)
    {
        T? cached = _value;
        if (cached != null)
        {
            return cached;
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            cached = _value;
            if (cached != null)
            {
                return cached;
            }

            cached = await factory(cancellationToken).ConfigureAwait(false);
            _value = cached;
            return cached;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>Drops the cached value and releases the underlying gate.</summary>
    public void Dispose()
    {
        _value = null;
        _gate.Dispose();
    }
}
