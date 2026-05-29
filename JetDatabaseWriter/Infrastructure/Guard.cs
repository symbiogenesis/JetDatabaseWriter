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
    /// <typeparam name="T">The reference type being validated.</typeparam>
    /// <param name="value">The value.</param>
    /// <param name="paramName">The param name.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="value"/> is <see langword="null"/>.</exception>
    [SuppressMessage("Roslynator", "RCS1256:Invalid argument null check", Justification = "Guard helper accepts nullable values to establish the NotNull postcondition.")]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void NotNull<T>([NotNull] T? value, string paramName)
        where T : class
    {
#if NETSTANDARD2_1
        if (value is null)
        {
            throw new ArgumentNullException(paramName);
        }
#else
        ArgumentNullException.ThrowIfNull(value, paramName);

        _ = value;
#endif
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void NotNullOrEmpty([NotNull] string? value, string paramName)
    {
#if NETSTANDARD2_1
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("Value cannot be null or empty", paramName);
        }
#else
        ArgumentException.ThrowIfNullOrEmpty(value, paramName);

        _ = value;
#endif
    }

    /// <summary>
    /// Validates that <paramref name="value"/> falls in the inclusive range
    /// <c>[min, max]</c>. On failure throws an <see cref="ArgumentOutOfRangeException"/>
    /// whose message is deterministically derived from <paramref name="paramName"/>
    /// and the bounds.
    /// </summary>
    /// <typeparam name="T">The comparable type of the value and bounds.</typeparam>
    /// <param name="value">The value.</param>
    /// <param name="min">The minimum allowed value.</param>
    /// <param name="max">The maximum allowed value.</param>
    /// <param name="paramName">The param name.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="value"/> falls outside the inclusive bounds.</exception>
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Positive(int value, string paramName)
    {
#if NETSTANDARD2_1
        if (value <= 0)
        {
            throw new ArgumentOutOfRangeException(paramName, value, "Value must be positive.");
        }
#else
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(value, paramName);

        _ = value;
#endif
    }

    /// <summary>
    /// Throws an <see cref="ObjectDisposedException"/> when <paramref name="disposed"/> is
    /// <see langword="true"/>, using the runtime type of <paramref name="instance"/> as the object
    /// name. Forwards to <c>ObjectDisposedException.ThrowIf</c> on .NET 7+ for JIT-friendlier codegen.
    /// </summary>
    /// <param name="disposed">The disposed flag of the calling instance.</param>
    /// <param name="instance">The instance being checked; typically <c>this</c>.</param>
    /// <exception cref="ObjectDisposedException">Thrown when <paramref name="disposed"/> is <see langword="true"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ThrowIfDisposed(bool disposed, object instance)
    {
#if NETSTANDARD2_1
        if (disposed)
        {
            throw new ObjectDisposedException(instance?.GetType().FullName);
        }
#else
        ObjectDisposedException.ThrowIf(disposed, instance);

        _ = instance;
#endif
    }

    /// <summary>
    /// Validates that <paramref name="path"/> is non-empty and refers to an existing file,
    /// throwing <see cref="FileNotFoundException"/> with a consistent "Database file not found"
    /// message when it does not exist.
    /// </summary>
    /// <param name="path">The file path.</param>
    /// <param name="paramName">The param name.</param>
    /// <exception cref="FileNotFoundException">Thrown when <paramref name="path"/> does not reference an existing file.</exception>
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
    /// <param name="stream">The stream.</param>
    /// <param name="paramName">The param name.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="stream"/> is not readable or not seekable.</exception>
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
    /// <param name="stream">The stream.</param>
    /// <param name="paramName">The param name.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="stream"/> is not readable, writable, or seekable.</exception>
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
