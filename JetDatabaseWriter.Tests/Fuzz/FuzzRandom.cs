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

    private FuzzRandom(byte[] bytes)
    {
        _bytes = bytes;
        _pos = 0;
    }

    private FuzzRandom(Random fallback)
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

    private static readonly Type[] SupportedTypes =
    [
        typeof(int), typeof(long), typeof(short), typeof(byte), typeof(bool),
        typeof(string), typeof(DateTime), typeof(double), typeof(float), typeof(byte[]),
    ];

    public string RandomString(int length)
    {
        const string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        return string.Create(length, this, static (span, rng) =>
        {
            for (int i = 0; i < span.Length; i++)
            {
                span[i] = chars[rng.Next(chars.Length)];
            }
        });
    }

    public byte[] RandomBytes(int maxLength = 32)
    {
        var arr = new byte[Next(0, maxLength)];
        NextBytes(arr);
        return arr;
    }

    public Type RandomType() => SupportedTypes[Next(SupportedTypes.Length)];

    public object? RandomValue(Type type) => type switch
    {
        _ when type == typeof(int) => Next(),
        _ when type == typeof(long) => (long)Next() << 32 | (long)Next(),
        _ when type == typeof(short) => (short)Next(short.MinValue, short.MaxValue),
        _ when type == typeof(byte) => (byte)Next(byte.MinValue, byte.MaxValue),
        _ when type == typeof(bool) => NextDouble() < 0.5,
        _ when type == typeof(string) => RandomString(Next(0, 20)),
        _ when type == typeof(DateTime) => DateTime.UtcNow.AddDays(Next(-10000, 10000)),
        _ when type == typeof(double) => NextDouble() * Next(),
        _ when type == typeof(float) => (float)(NextDouble() * Next()),
        _ when type == typeof(byte[]) => RandomBytes(),
        _ => null,
    };
}
