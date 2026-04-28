namespace JetDatabaseWriter;

using System;
using System.Runtime.CompilerServices;

/// <summary>
/// Helper methods for input validation and guard clauses.
/// </summary>
internal static class Guard
{
    /// <summary>
    /// Throws an <see cref="ArgumentNullException"/> when <paramref name="value"/> is
    /// <see langword="null"/>. Forwards to <c>ArgumentNullException.ThrowIfNull</c> on
    /// .NET 6+ for JIT-friendlier codegen and falls back to a manual check on older targets.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void NotNull<T>(T value, string paramName)
        where T : class
    {
#if NET6_0_OR_GREATER
        ArgumentNullException.ThrowIfNull(value, paramName);
#else
        if (value is null)
        {
            throw new ArgumentNullException(paramName);
        }
#endif
    }

    public static void NotNullOrEmpty(string value, string paramName)
    {
#if NET6_0_OR_GREATER
        ArgumentException.ThrowIfNullOrEmpty(value, paramName);
#else
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("Value cannot be null or empty", paramName);
        }
#endif
    }

    /// <summary>
    /// Validates that <paramref name="value"/> falls in the inclusive range
    /// <c>[min, max]</c>. On failure throws an <see cref="ArgumentOutOfRangeException"/>
    /// whose message is deterministically derived from <paramref name="paramName"/>
    /// and the bounds.
    /// </summary>
    public static void InRange<T>(T value, T min, T max, string paramName)
        where T : IComparable<T>
    {
        if (value.CompareTo(min) < 0 || value.CompareTo(max) > 0)
        {
            throw new ArgumentOutOfRangeException(
                paramName,
                value,
                $"{paramName} must be between {min} and {max}.");
        }
    }

    public static void Positive(int value, string paramName)
    {
        if (value <= 0)
        {
            throw new ArgumentOutOfRangeException(paramName, value, "Value must be positive.");
        }
    }

    /// <summary>
    /// Throws an <see cref="ObjectDisposedException"/> when <paramref name="disposed"/> is
    /// <see langword="true"/>, using the runtime type of <paramref name="instance"/> as the object
    /// name. Forwards to <c>ObjectDisposedException.ThrowIf</c> on .NET 7+ for JIT-friendlier codegen.
    /// </summary>
    /// <param name="disposed">The disposed flag of the calling instance.</param>
    /// <param name="instance">The instance being checked; typically <c>this</c>.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ThrowIfDisposed(bool disposed, object instance)
    {
#if NET7_0_OR_GREATER
        ObjectDisposedException.ThrowIf(disposed, instance);
#else
        if (disposed)
        {
            throw new ObjectDisposedException(instance?.GetType().FullName);
        }
#endif
    }
}
