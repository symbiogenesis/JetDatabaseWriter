namespace JetDatabaseWriter.ValueEncoding.Models;

/// <summary>
/// Sentinel produced by <see cref="LongValueEncoder.PreEncodeLongValuesAsync"/>. The wrapped
/// 12-byte LVAL header already references LVAL pages allocated earlier in
/// the row-insert pipeline, so the row encoder just splices <see cref="HeaderBytes"/>
/// straight into the row body without any further encoding.
/// </summary>
/// <param name="headerBytes">The header bytes.</param>
internal sealed class PreEncodedLongValue(byte[] headerBytes)
{
    public byte[] HeaderBytes { get; } = headerBytes;
}
