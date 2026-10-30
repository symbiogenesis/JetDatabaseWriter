namespace JetDatabaseWriter;

using System;

internal sealed partial class JetFormat
{
    /// <summary>Encodes calculated Text with the native leading-marker compression decision.</summary>
    /// <param name="value">The evaluated Text result.</param>
    /// <param name="maxBytes">The declared uncompressed payload limit.</param>
    /// <returns>The native cached payload, including native plain-marker conversion behavior.</returns>
    internal static byte[] EncodeCalculatedText(string value, int maxBytes)
    {
        int count = Math.Min(value.Length, maxBytes > 0 ? maxBytes / 2 : int.MaxValue);
        ReadOnlySpan<char> text = value.AsSpan(0, count);
        int plainLength = count * 2;
        int compressedLength = 2;
        bool compressed = true;
        if (text.IsEmpty || text[0] != '\uFEFF')
        {
            return EncodeCalculatedPlainText(text);
        }

        foreach (char character in text)
        {
            // A zero low byte cannot appear in the native mixed-mode codec.
            // Access falls back to plain UTF-16, even with a long Latin tail.
            if ((character & 0xFF) == 0)
            {
                return EncodeCalculatedPlainText(text);
            }

            bool nextCompressed = character <= '\u00FF';
            compressedLength += (nextCompressed ? 1 : 2) + (compressed == nextCompressed ? 0 : 1);
            compressed = nextCompressed;
        }

        // Calculated Text accepts ties; ordinary Text requires a byte saving.
        if (compressedLength > plainLength)
        {
            return EncodeCalculatedPlainText(text);
        }

        byte[] payload = new byte[compressedLength];
        payload[0] = 0xFF;
        payload[1] = 0xFE;
        int offset = 2;
        compressed = true;
        foreach (char character in text)
        {
            bool nextCompressed = character <= '\u00FF';
            if (compressed != nextCompressed)
            {
                payload[offset++] = 0;
                compressed = nextCompressed;
            }

            payload[offset++] = (byte)(character & 0xFF);
            if (!compressed)
            {
                payload[offset++] = (byte)(character >> 8);
            }
        }

        return payload;
    }

    /// <summary>Projects a calculated Text reference through its native cached representation.</summary>
    /// <param name="value">The evaluated Text result before storage conversion.</param>
    /// <param name="maxBytes">The declared uncompressed payload limit.</param>
    /// <returns>The value seen by native dependent expressions.</returns>
    internal static object NormalizeCalculatedTextReference(string value, int maxBytes)
    {
        if (value.Length == 0)
        {
            return DBNull.Value;
        }

        if (value[0] != '\uFEFF')
        {
            return maxBytes > 0 && value.Length > maxBytes / 2 ? value[..(maxBytes / 2)] : value;
        }

        byte[] payload = EncodeCalculatedText(value, maxBytes);
        string text = DecodeJet4TextCore(payload, 0, payload.Length, allowTerminalLowByte: true);
        return text.Length == 0 ? DBNull.Value : text;
    }

    private static byte[] EncodeCalculatedPlainText(ReadOnlySpan<char> text)
    {
        byte[] payload = new byte[text.Length * 2];
        EncodeUtf16LE(text, payload);
        return payload;
    }
}
