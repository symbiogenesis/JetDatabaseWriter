namespace JetDatabaseWriter.Tests.Infrastructure;

using System;

#pragma warning disable CA5394 // Using non-cryptographic random for fuzz testing is acceptable.

/// <summary>
/// A random number generator that uses fuzzed bytes as entropy and falls back to a deterministic Random instance if the fuzzed bytes are exhausted.
/// </summary>
internal sealed class FuzzRandom : Random
{
    private readonly byte[]? bytes;
    private readonly Random? fallback;
    private int pos;

    private FuzzRandom(byte[] bytes)
    {
        this.bytes = bytes;
        this.fallback = new Random(CreateFallbackSeed(bytes));
        this.pos = 0;
    }

    private FuzzRandom(Random fallback) => this.fallback = fallback;

    public static FuzzRandom Create(byte[]? fuzzedBytes)
        => fuzzedBytes?.Length > 0
            ? new FuzzRandom(fuzzedBytes)
            : new FuzzRandom(new Random(0));

    private int NextByte()
    {
        if (this.bytes != null && this.pos < this.bytes.Length)
        {
            return this.bytes[this.pos++];
        }

        return this.fallback?.Next(0, 256) ?? 0;
    }

    public override int Next()
    {
        // Use 4 bytes for int
        int b1 = this.NextByte();
        int b2 = this.NextByte();
        int b3 = this.NextByte();
        int b4 = this.NextByte();
        return (b1 << 24) | (b2 << 16) | (b3 << 8) | b4;
    }

    public override int Next(int maxValue) => this.Next(0, maxValue);

    public override int Next(int minValue, int maxValue)
    {
        if (minValue >= maxValue)
        {
            return minValue;
        }

        long range = (long)maxValue - minValue;
        uint value = unchecked((uint)this.Next());
        return (int)(minValue + (long)(value % (ulong)range));
    }

    public override double NextDouble()
    {
        int value = this.Next();
        return (value & 0x7FFFFFFF) / (double)int.MaxValue;
    }

    public override void NextBytes(byte[] buffer)
    {
        for (int i = 0; i < buffer.Length; i++)
        {
            buffer[i] = (byte)this.NextByte();
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
        var arr = new byte[this.Next(0, maxLength)];
        this.NextBytes(arr);
        return arr;
    }

    public Type RandomType() => SupportedTypes[this.Next(SupportedTypes.Length)];

    public object? RandomValue(Type type) => type switch
    {
        _ when type == typeof(int) => this.Next(),
        _ when type == typeof(long) => ((long)this.Next() << 32) | (long)this.Next(),
        _ when type == typeof(short) => (short)this.Next(short.MinValue, short.MaxValue),
        _ when type == typeof(byte) => (byte)this.Next(byte.MinValue, byte.MaxValue),
        _ when type == typeof(bool) => this.NextDouble() < 0.5,
        _ when type == typeof(string) => this.RandomString(this.Next(0, 20)),
        _ when type == typeof(DateTime) => DateTime.UtcNow.AddDays(this.Next(-10000, 10000)),
        _ when type == typeof(double) => this.NextDouble() * this.Next(),
        _ when type == typeof(float) => (float)(this.NextDouble() * this.Next()),
        _ when type == typeof(byte[]) => this.RandomBytes(),
        _ => null,
    };

    private static int CreateFallbackSeed(byte[] bytes)
    {
        unchecked
        {
            uint hash = 2166136261u;
            foreach (byte value in bytes)
            {
                hash ^= value;
                hash *= 16777619u;
            }

            return (int)hash;
        }
    }
}
