namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

/// <summary>Loads one bounded input for a process-isolated, awaited fuzz iteration.</summary>
internal static class FuzzInput
{
    private const int MaxInputBytes = 16 * 1024 * 1024;

    /// <summary>Reads the input selected by the hosted fuzz driver, or a deterministic standalone seed.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The owned input bytes.</returns>
    /// <exception cref="InvalidDataException">The selected input exceeds the fuzz input budget.</exception>
    internal static async Task<byte[]> ReadAsync(CancellationToken cancellationToken)
    {
        string? path = Environment.GetEnvironmentVariable("JETDATABASEWRITER_FUZZ_INPUT");
        if (string.IsNullOrEmpty(path))
        {
            return [0, 1, 2, 3, 4, 5, 6, 7];
        }

        await using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.Asynchronous);
        if (stream.Length > MaxInputBytes)
        {
            throw new InvalidDataException($"Fuzz input exceeds {MaxInputBytes} bytes.");
        }

        byte[] input = new byte[checked((int)stream.Length)];
        await stream.ReadExactlyAsync(input, cancellationToken);
        return input;
    }
}
