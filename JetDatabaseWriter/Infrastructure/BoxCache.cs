namespace JetDatabaseWriter.Infrastructure;

using System.Runtime.CompilerServices;

/// <summary>
/// Interned boxes for low-cardinality fixed-width cell values, so the untyped
/// object-array decode path does not allocate a fresh heap box for every
/// <c>bool</c>, <c>byte</c>, or small <c>short</c>/<c>int</c> cell.
/// <para>
/// Boxed value types are immutable, so a shared box can never be observed as
/// mutated by a caller; the only behavioural change versus a fresh box is that
/// two equal low-magnitude cells become reference-equal, which is benign (and
/// mirrors the long-standing singleton <see cref="System.DBNull.Value"/> already
/// shared across decoded row arrays). High-cardinality values (IDs, prices,
/// timestamps) fall back to a normal box.
/// </para>
/// </summary>
internal static class BoxCache
{
    /// <summary>Inclusive lower bound of the cached small-integer range.</summary>
    private const int SmallIntMin = -1;

    /// <summary>Inclusive upper bound of the cached small-integer range.</summary>
    private const int SmallIntMax = 256;

    private static readonly object BoxedTrue = true;
    private static readonly object BoxedFalse = false;

    private static readonly object[] ByteBoxes = CreateByteBoxes();
    private static readonly object[] Int16Boxes = CreateInt16Boxes();
    private static readonly object[] Int32Boxes = CreateInt32Boxes();

    /// <summary>Returns an interned box for <paramref name="value"/> (no allocation).</summary>
    /// <param name="value">The boolean cell value.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static object Bool(bool value) => value ? BoxedTrue : BoxedFalse;

    /// <summary>Returns an interned box for <paramref name="value"/> (no allocation).</summary>
    /// <param name="value">The byte cell value.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static object Byte(byte value) => ByteBoxes[value];

    /// <summary>
    /// Returns an interned box for <paramref name="value"/> when it falls in the
    /// cached range, otherwise a freshly boxed <see cref="short"/>.
    /// </summary>
    /// <param name="value">The 16-bit integer cell value.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static object Int16(short value) =>
        value is >= SmallIntMin and <= SmallIntMax ? Int16Boxes[value - SmallIntMin] : value;

    /// <summary>
    /// Returns an interned box for <paramref name="value"/> when it falls in the
    /// cached range, otherwise a freshly boxed <see cref="int"/>.
    /// </summary>
    /// <param name="value">The 32-bit integer cell value.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static object Int32(int value) =>
        value is >= SmallIntMin and <= SmallIntMax ? Int32Boxes[value - SmallIntMin] : value;

    private static object[] CreateByteBoxes()
    {
        object[] boxes = new object[256];
        for (int i = 0; i < boxes.Length; i++)
        {
            boxes[i] = (byte)i;
        }

        return boxes;
    }

    private static object[] CreateInt16Boxes()
    {
        object[] boxes = new object[SmallIntMax - SmallIntMin + 1];
        for (int i = 0; i < boxes.Length; i++)
        {
            // -1..256 is always within short range, so the checked narrowing cast never throws.
            boxes[i] = (short)(SmallIntMin + i);
        }

        return boxes;
    }

    private static object[] CreateInt32Boxes()
    {
        object[] boxes = new object[SmallIntMax - SmallIntMin + 1];
        for (int i = 0; i < boxes.Length; i++)
        {
            boxes[i] = SmallIntMin + i;
        }

        return boxes;
    }
}
