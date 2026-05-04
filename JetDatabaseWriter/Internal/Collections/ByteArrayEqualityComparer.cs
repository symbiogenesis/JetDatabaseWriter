namespace JetDatabaseWriter.Internal.Collections;

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

        if (x is null || y is null || x.Length != y.Length)
        {
            return false;
        }

        for (int i = 0; i < x.Length; i++)
        {
            if (x[i] != y[i])
            {
                return false;
            }
        }

        return true;
    }

    public int GetHashCode(byte[] obj)
    {
        unchecked
        {
            int hash = (int)2166136261u;
            for (int i = 0; i < obj.Length; i++)
            {
                hash = (hash ^ obj[i]) * 16777619;
            }

            return hash;
        }
    }
}