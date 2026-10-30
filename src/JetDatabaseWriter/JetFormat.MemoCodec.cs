namespace JetDatabaseWriter;

using System;
using JetDatabaseWriter.Exceptions;

internal sealed partial class JetFormat
{
    /// <summary>Encodes ordinary Memo text with bounded native compression and unambiguous literal U+FEFF storage.</summary>
    /// <param name="value">The complete text.</param>
    /// <param name="compress">Whether the column permits Unicode compression.</param>
    /// <returns>The complete native Memo payload.</returns>
    /// <exception cref="JetLimitationException">The short leading U+FEFF value has no supported representation for its column flags or character shape.</exception>
    internal byte[] EncodeMemoText(string value, bool compress)
    {
        if (this.IsJet3)
        {
            return this.EncodeAnsiText(value);
        }

        // DAO120 reads compressed single-row values through 2048 Latin-1 characters;
        // larger expansions can fail exact native reads; chained payloads are plain UTF-16.
        // Native writers can choose plain chained storage below this conservative cap.
        byte[] bytes = EncodeJet4Text(value, compress && value.Length <= 2048);
        if (value.Length == 0 || value[0] != '\uFEFF' || value.Length > 2048)
        {
            return bytes;
        }

        // Native compact chooses ambiguous plain bytes when the escape saves no
        // space. External escaped payloads also require the compression flag;
        // an inline value can spill when the containing row is too large.
        // The retained DAO120 controls use this exact escape for Latin-1 tails.
        if (compress && value.Length >= 6)
        {
            bool latinTail = true;
            for (int index = 1; index < value.Length; index++)
            {
                if (value[index] is '\0' or > '\u00FF')
                {
                    latinTail = false;
                    break;
                }
            }

            if (latinTail)
            {
                byte[] escaped = new byte[value.Length + 5];
                escaped[0] = 0xFF;
                escaped[1] = 0xFE;
                escaped[2] = 0;
                escaped[3] = 0xFF;
                escaped[4] = 0xFE;
                escaped[5] = 0;
                for (int index = 1; index < value.Length; index++)
                {
                    escaped[index + 5] = (byte)value[index];
                }

                return escaped;
            }
        }

        throw new JetLimitationException(JetErrorCode.FeatureNotSupported, "Short ordinary Memo values beginning with U+FEFF require Unicode compression and 6 through 2048 characters with a nonzero Latin-1 tail for verified writing.");
    }

    /// <summary>Decodes ordinary Memo using its descriptor's compression eligibility, or plain calculated Memo text.</summary>
    /// <param name="bytes">The complete payload or unwrapped calculated text.</param>
    /// <param name="start">The payload start.</param>
    /// <param name="length">The complete payload length.</param>
    /// <param name="compressedStorage">Whether an ordinary inline or single-row descriptor permits the marker.</param>
    /// <returns>The stored text, preserving literal leading U+FEFF in plain chained or calculated values.</returns>
    internal string DecodeMemoText(byte[] bytes, int start, int length, bool compressedStorage)
    {
        if (this.IsJet3)
        {
            return this.AnsiEncoding.GetString(bytes, start, length);
        }

        if (!compressedStorage || length < 2 || bytes[start] != 0xFF || bytes[start + 1] != 0xFE)
        {
            return DecodeUtf16LE(bytes.AsSpan(start, length & ~1));
        }

        return DecodeJet4Text(bytes, start, length);
    }
}
