namespace JetDatabaseWriter.Infrastructure;

using System;
using System.Collections.Generic;

internal sealed class ByteArrayEqualityComparer : IEqualityComparer<byte[]>
{
    public static readonly ByteArrayEqualityComparer Instance = new();

    public bool Equals(byte[]? x, byte[]? y)
    {
        if (ReferenceEquals(x, y))
        {
            return true;
        }

        if (x is null || y is null)
        {
            return false;
        }

        return x.AsSpan().SequenceEqual(y);
    }

    public int GetHashCode(byte[] obj)
    {
#if NET6_0_OR_GREATER
        HashCode hc = default;
        hc.AddBytes(obj);
        return hc.ToHashCode();
#else
        unchecked
        {
            int hash = (int)2166136261u;
            for (int i = 0; i < obj.Length; i++)
            {
                hash = (hash ^ obj[i]) * 16777619;
            }

            return hash;
        }
#endif
    }
}
