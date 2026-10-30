namespace JetDatabaseWriter.Models;

using System;
using System.Globalization;

/// <summary>Retains the stored OLE date serial alongside its public DateTime projection.</summary>
internal readonly struct NativeDate : IEquatable<NativeDate>, IComparable, IConvertible
{
    private NativeDate(double serial)
    {
        _ = DateTime.FromOADate(serial);
        this.Serial = serial;
    }

    internal double Serial { get; }

    internal DateTime Value => DateTime.FromOADate(this.Serial);

    internal static NativeDate FromSerial(double serial) => new(serial);

    internal static NativeDate FromDateTime(DateTime value) => FromSerial(value.ToOADate());

    public bool Equals(NativeDate other) => this.Serial.Equals(other.Serial);

    public override bool Equals(object? obj) => obj is NativeDate other && this.Equals(other);

    public override int GetHashCode() => this.Serial.GetHashCode();

    public int CompareTo(object? obj) => obj switch
    {
        null => 1,
        NativeDate other => this.Serial.CompareTo(other.Serial),
        _ => throw new ArgumentException("A native date can only be ordered against another native date.", nameof(obj)),
    };

    public override string ToString() => this.Value.ToString(CultureInfo.InvariantCulture);

    TypeCode IConvertible.GetTypeCode() => TypeCode.DateTime;

    bool IConvertible.ToBoolean(IFormatProvider? provider) => Convert.ToBoolean(this.Serial, provider);

    byte IConvertible.ToByte(IFormatProvider? provider) => Convert.ToByte(this.Serial, provider);

    char IConvertible.ToChar(IFormatProvider? provider) => ((IConvertible)this.Value).ToChar(provider);

    DateTime IConvertible.ToDateTime(IFormatProvider? provider) => this.Value;

    decimal IConvertible.ToDecimal(IFormatProvider? provider) => Convert.ToDecimal(this.Serial, provider);

    double IConvertible.ToDouble(IFormatProvider? provider) => this.Serial;

    short IConvertible.ToInt16(IFormatProvider? provider) => Convert.ToInt16(this.Serial, provider);

    int IConvertible.ToInt32(IFormatProvider? provider) => Convert.ToInt32(this.Serial, provider);

    long IConvertible.ToInt64(IFormatProvider? provider) => Convert.ToInt64(this.Serial, provider);

    sbyte IConvertible.ToSByte(IFormatProvider? provider) => Convert.ToSByte(this.Serial, provider);

    float IConvertible.ToSingle(IFormatProvider? provider) => Convert.ToSingle(this.Serial, provider);

    string IConvertible.ToString(IFormatProvider? provider) => this.Value.ToString(provider);

    object IConvertible.ToType(Type conversionType, IFormatProvider? provider)
    {
        if (conversionType == typeof(NativeDate) || conversionType == typeof(object))
        {
            return this;
        }

        if (conversionType == typeof(DateTime))
        {
            return this.Value;
        }

        return conversionType == typeof(string) ? this.Value.ToString(provider) : Convert.ChangeType(this.Serial, conversionType, provider);
    }

    ushort IConvertible.ToUInt16(IFormatProvider? provider) => Convert.ToUInt16(this.Serial, provider);

    uint IConvertible.ToUInt32(IFormatProvider? provider) => Convert.ToUInt32(this.Serial, provider);

    ulong IConvertible.ToUInt64(IFormatProvider? provider) => Convert.ToUInt64(this.Serial, provider);
}
