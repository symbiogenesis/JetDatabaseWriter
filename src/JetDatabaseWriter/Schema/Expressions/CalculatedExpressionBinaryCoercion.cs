namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Buffers.Binary;
using JetDatabaseWriter.Models;

/// <summary>Converts expression Variants to the bytes cached by native Binary and OLE results.</summary>
internal static class CalculatedExpressionBinaryCoercion
{
    internal static object ConvertValue(object value)
        => value switch
        {
            byte[] bytes => bytes.Length == 0 ? DBNull.Value : bytes,
            Guid guid => guid.ToByteArray(),
            string text => EncodeText(text),
            decimal => EncodeText(CalculatedExpressionCoercion.ToText(value)),
            bool boolean => EncodeInteger(boolean ? -1 : 0, sizeof(short)),
            byte number => EncodeInteger(number, sizeof(int)),
            short number => EncodeInteger(number, sizeof(short)),
            int number => EncodeInteger(number, sizeof(int)),
            long number => EncodeInteger(number, sizeof(long)),
            AccessCurrency currency => EncodeInteger(checked((long)(currency.Value * 10000m)), sizeof(long)),
            float number => EncodeInteger(BitConverter.SingleToInt32Bits(number), sizeof(int)),
            double number => EncodeInteger(BitConverter.DoubleToInt64Bits(number), sizeof(long)),
            NativeDate date => EncodeInteger(BitConverter.DoubleToInt64Bits(date.Serial), sizeof(long)),
            DateTime date => EncodeInteger(BitConverter.DoubleToInt64Bits(date.ToOADate()), sizeof(long)),
            _ => throw new InvalidCastException($"A value of type {value.GetType().Name} cannot be converted to a calculated Binary result."),
        };

    private static byte[] EncodeInteger(long value, int width)
    {
        byte[] result = new byte[width];
        switch (width)
        {
            case sizeof(short):
                BinaryPrimitives.WriteInt16LittleEndian(result, checked((short)value));
                break;
            case sizeof(int):
                BinaryPrimitives.WriteInt32LittleEndian(result, checked((int)value));
                break;
            default:
                BinaryPrimitives.WriteInt64LittleEndian(result, value);
                break;
        }

        return result;
    }

    private static object EncodeText(string text)
    {
        if (text.Length == 0)
        {
            return DBNull.Value;
        }

        byte[] result = new byte[checked(text.Length * sizeof(char))];
        for (int index = 0; index < text.Length; index++)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(result.AsSpan(index * sizeof(char), sizeof(char)), text[index]);
        }

        return result;
    }
}
