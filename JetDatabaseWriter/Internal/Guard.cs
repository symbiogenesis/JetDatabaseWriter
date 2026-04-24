namespace JetDatabaseWriter;

using System;
using System.Runtime.CompilerServices;

/// <summary>
/// Helper methods for input validation and guard clauses.
/// </summary>
internal static class Guard
{
    public static void NotNull(object value, string paramName)
    {
        if (value == null)
        {
            throw new ArgumentNullException(paramName);
        }
    }

    public static void NotNullOrEmpty(string value, string paramName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("Value cannot be null or empty", paramName);
        }
    }

    public static void InRange(int value, int min, int max, string paramName)
    {
        if (value < min || value > max)
        {
            throw new ArgumentOutOfRangeException(
                paramName,
                $"Value must be between {min} and {max}");
        }
    }

    public static void Positive(int value, string paramName)
    {
        if (value <= 0)
        {
            throw new ArgumentOutOfRangeException(paramName, "Value must be positive");
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
