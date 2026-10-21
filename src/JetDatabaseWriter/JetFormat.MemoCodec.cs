namespace JetDatabaseWriter;

using System;
using JetDatabaseWriter.Exceptions;

internal sealed partial class JetFormat
{
    /// <summary>Encodes ordinary Memo text with bounded native compression and unambiguous literal U+FEFF storage.</summary>
    /// <param name="value">The complete text.</param>
    /// <param name="compress">Whether the column permits Unicode compression.</param>
    /// <returns>The complete native Memo payload.</returns>
    /// <exception cref="JetLimitationException">The leading U+FEFF value has no supported unambiguous single-row representation.</exception>
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

        // FF FE is a marker in inline/single-row storage. Native escape behavior
        // also depends on column flags; no writer representation is verified for
        // every supported case. Refuse before allocating instead of losing FEFF.
        throw new JetLimitationException(JetErrorCode.FeatureNotSupported, "Ordinary Memo values of at most 2048 characters beginning with U+FEFF are not supported for writing.");
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
