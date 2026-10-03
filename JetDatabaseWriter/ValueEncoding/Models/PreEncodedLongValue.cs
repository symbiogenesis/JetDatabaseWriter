namespace JetDatabaseWriter.ValueEncoding.Models;

/// <summary>
/// Stands in a row's value array for a MEMO or OLE value stored on LVAL pages:
/// the row encoder splices <see cref="HeaderBytes"/> straight into the row body
/// without any further encoding. <see cref="LongValueEncoder.PrepareLongValues"/>
/// creates a pending one for each value over its inline cap, holding the payload
/// and a zeroed header of the final 12-byte size, so the row can be serialized
/// and measured before any page is written.
/// <see cref="LongValueEncoder.WriteLongValuesAsync"/> then writes the payload
/// and puts a finished sentinel, whose header names the LVAL pages, in its place.
/// </summary>
/// <param name="headerBytes">The 12-byte LVAL header.</param>
/// <param name="pendingPayload">The payload still to write to LVAL pages, or <see langword="null"/> once <paramref name="headerBytes"/> names them.</param>
internal sealed class PreEncodedLongValue(byte[] headerBytes, byte[]? pendingPayload = null)
{
    /// <summary>Gets the 12-byte LVAL header, zeroed while <see cref="PendingPayload"/> is set.</summary>
    public byte[] HeaderBytes { get; } = headerBytes;

    /// <summary>
    /// Gets the payload still to write to LVAL pages, or <see langword="null"/>
    /// once <see cref="HeaderBytes"/> names them.
    /// </summary>
    public byte[]? PendingPayload { get; } = pendingPayload;
}
