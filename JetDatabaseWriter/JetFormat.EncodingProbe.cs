namespace JetDatabaseWriter;

using System.Text;

internal sealed partial class JetFormat
{
    /// <summary>Checks a native password's code-page representation without throwing on an absent character.</summary>
    /// <param name="value">The caller-owned password characters.</param>
    /// <param name="bytes">The encoded bytes, which the caller must clear even on refusal.</param>
    /// <returns>Whether every character can be represented.</returns>
    internal bool TryEncodeAnsiText(char[] value, out byte[] bytes)
    {
        var fallback = new CharacterProbeFallback();
        var encoding = (Encoding)this.AnsiEncoding.Clone();
        encoding.EncoderFallback = fallback;
        bytes = encoding.GetBytes(value);
        return !fallback.Unknown.HasValue;
    }

    private sealed class CharacterProbeFallback : EncoderFallback
    {
        public char? Unknown { get; private set; }

        public char? LowSurrogate { get; private set; }

        public override int MaxCharCount => 0;

        public override EncoderFallbackBuffer CreateFallbackBuffer() => new ProbeBuffer(this);

        private sealed class ProbeBuffer(CharacterProbeFallback owner) : EncoderFallbackBuffer
        {
            public override int Remaining => 0;

            public override bool Fallback(char charUnknown, int index)
            {
                owner.Unknown ??= charUnknown;
                return true;
            }

            public override bool Fallback(char charUnknownHigh, char charUnknownLow, int index)
            {
                if (!owner.Unknown.HasValue)
                {
                    owner.Unknown = charUnknownHigh;
                    owner.LowSurrogate = charUnknownLow;
                }

                return true;
            }

            public override char GetNextChar() => '\0';

            public override bool MovePrevious() => false;
        }
    }
}
