namespace JetDatabaseWriter.Infrastructure;

using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
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
    public static void NotNull<T>([NotNull] T? value, string paramName)
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

    public static void NotNullOrEmpty([NotNull] string? value, string paramName)
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
#if NET8_0_OR_GREATER
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(value, paramName);
#else
        if (value <= 0)
        {
            throw new ArgumentOutOfRangeException(paramName, value, "Value must be positive.");
        }
#endif
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

    /// <summary>
    /// Validates that <paramref name="path"/> is non-empty and refers to an existing file,
    /// throwing <see cref="FileNotFoundException"/> with a consistent "Database file not found"
    /// message when it does not exist.
    /// </summary>
    public static void RequireExistingDatabaseFile([NotNull] string? path, string paramName)
    {
        NotNullOrEmpty(path, paramName);
        if (!File.Exists(path))
        {
            throw new FileNotFoundException($"Database file not found: {path}", path);
        }
    }

    /// <summary>
    /// Validates that <paramref name="stream"/> is non-<see langword="null"/>, readable, and
    /// seekable, throwing <see cref="ArgumentException"/> for any unmet capability.
    /// </summary>
    public static void RequireReadableSeekableStream([NotNull] Stream? stream, string paramName)
    {
        NotNull(stream, paramName);
        if (!stream.CanRead)
        {
            throw new ArgumentException("Stream must be readable.", paramName);
        }

        if (!stream.CanSeek)
        {
            throw new ArgumentException("Stream must be seekable.", paramName);
        }
    }

    /// <summary>
    /// Validates that <paramref name="stream"/> is non-<see langword="null"/>, readable,
    /// writable, and seekable, throwing <see cref="ArgumentException"/> for any unmet capability.
    /// </summary>
    public static void RequireReadWriteSeekableStream([NotNull] Stream? stream, string paramName)
    {
        NotNull(stream, paramName);
        if (!stream.CanRead)
        {
            throw new ArgumentException("Stream must be readable.", paramName);
        }

        if (!stream.CanWrite)
        {
            throw new ArgumentException("Stream must be writable.", paramName);
        }

        if (!stream.CanSeek)
        {
            throw new ArgumentException("Stream must be seekable.", paramName);
        }
    }
}
