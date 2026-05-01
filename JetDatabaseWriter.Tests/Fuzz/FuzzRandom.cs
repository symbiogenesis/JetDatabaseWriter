namespace JetDatabaseWriter.Tests.Fuzz;

using System;

#pragma warning disable CA5394 // Using non-cryptographic random for fuzz testing is acceptable.

/// <summary>
/// A random number generator that uses fuzzed bytes as entropy and falls back to a standard Random instance if the fuzzed bytes are exhausted.
/// </summary>
internal sealed class FuzzRandom : Random
{
    private readonly byte[]? _bytes;
    private readonly Random? _fallback;
    private int _pos;

    public FuzzRandom(byte[] bytes)
    {
        _bytes = bytes;
        _pos = 0;
    }

    public FuzzRandom(Random fallback)
    {
        _fallback = fallback;
    }

    public static FuzzRandom Create(byte[]? fuzzedBytes)
        => fuzzedBytes?.Length > 0
            ? new FuzzRandom(fuzzedBytes)
            : new FuzzRandom(new Random());

    private int NextByte()
    {
        if (_bytes != null && _pos < _bytes.Length)
        {
            return _bytes[_pos++];
        }

        return _fallback?.Next(0, 256) ?? 0;
    }

    public override int Next()
    {
        // Use 4 bytes for int
        int b1 = NextByte();
        int b2 = NextByte();
        int b3 = NextByte();
        int b4 = NextByte();
        return (b1 << 24) | (b2 << 16) | (b3 << 8) | b4;
    }

    public override int Next(int minValue, int maxValue)
    {
        if (minValue >= maxValue)
        {
            return minValue;
        }

        int range = maxValue - minValue;
        int value = Math.Abs(Next()) % range;
        return minValue + value;
    }

    public override double NextDouble()
    {
        int value = Next();
        return (value & 0x7FFFFFFF) / (double)int.MaxValue;
    }

    public override void NextBytes(byte[] buffer)
    {
        for (int i = 0; i < buffer.Length; i++)
        {
            buffer[i] = (byte)NextByte();
        }
    }
}