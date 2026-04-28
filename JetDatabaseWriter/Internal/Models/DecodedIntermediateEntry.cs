namespace JetDatabaseWriter.Internal.Models;

/// <summary>
/// One canonical entry on a JET intermediate (<c>page_type = 0x03</c>)
/// index page: the decoded summary key (with the §4.4 shared prefix
/// re-prepended on entries beyond the first), the (page, row) pointer to
/// the referenced child's last data row, and the absolute page number of
/// that child. Used by the surgical multi-level mutation path so the
/// writer can re-emit a single intermediate page in place after a
/// summary-key change or a leaf-split insert.
/// </summary>
internal readonly record struct DecodedIntermediateEntry(byte[] Key, long DataPage, byte DataRow, long ChildPage);
