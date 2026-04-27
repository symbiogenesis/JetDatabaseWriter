namespace JetDatabaseWriter;

using System;
using System.Text;

/// <summary>
/// Strongly-typed representation of a Microsoft Access Hyperlink field value.
/// </summary>
/// <remarks>
/// <para>
/// Access stores hyperlinks as a MEMO column whose TDEF column-flag bit
/// <c>HYPERLINK_FLAG_MASK = 0x80</c> is set. The persisted text is a
/// <c>#</c>-delimited record of up to four parts:
/// </para>
/// <code>
/// displaytext # address # subaddress # screentip
/// </code>
/// <para>
/// Empty parts are preserved (e.g. <c>"Click##anchor#"</c> → display=<c>"Click"</c>,
/// address=empty, subaddress=<c>"anchor"</c>, screentip=empty). Literal <c>#</c>
/// characters embedded inside any part are escaped as <c>%23</c> on serialization
/// and decoded on parse, matching Microsoft Access semantics.
/// </para>
/// <para>
/// To declare a hyperlink column, set <see cref="ColumnDefinition.IsHyperlink"/>
/// to <see langword="true"/> on a string/MEMO column, or use
/// <c>ColumnDefinition("Link", typeof(Hyperlink))</c>. Reader APIs auto-materialize
/// rows of hyperlink columns as <see cref="Hyperlink"/> instances; the writer
/// accepts <see cref="Hyperlink"/>, raw <see cref="string"/>, or any value whose
/// <c>ToString()</c> returns the encoded form.
/// </para>
/// </remarks>
public sealed record Hyperlink
{
    /// <summary>
    /// Initializes a new instance of the <see cref="Hyperlink"/> class.
    /// </summary>
    /// <param name="displayText">Optional human-readable label shown to the user. May be empty.</param>
    /// <param name="address">URL or file path the hyperlink targets. May be empty.</param>
    /// <param name="subAddress">Optional anchor / subaddress within <paramref name="address"/> (e.g. bookmark name, sheet name). May be empty.</param>
    /// <param name="screenTip">Optional tooltip text shown when hovering. May be empty.</param>
    public Hyperlink(string displayText = "", string address = "", string subAddress = "", string screenTip = "")
    {
        DisplayText = displayText ?? string.Empty;
        Address = address ?? string.Empty;
        SubAddress = subAddress ?? string.Empty;
        ScreenTip = screenTip ?? string.Empty;
    }

    /// <summary>Gets the display text shown in place of the raw address.</summary>
    public string DisplayText { get; init; }

    /// <summary>Gets the link target — typically a URL, UNC path, or local file path.</summary>
    public string Address { get; init; }

    /// <summary>Gets the optional sub-address (e.g. <c>#bookmark</c>, sheet name, query string).</summary>
    public string SubAddress { get; init; }

    /// <summary>Gets the optional tooltip text shown on hover in Microsoft Access.</summary>
    public string ScreenTip { get; init; }

    /// <summary>
    /// Parses a Microsoft Access hyperlink string (the on-disk MEMO payload of
    /// a hyperlink-flagged column) into a <see cref="Hyperlink"/> value.
    /// </summary>
    /// <param name="value">
    /// The encoded hyperlink string. <see langword="null"/> or empty returns
    /// <see langword="null"/>. A string with no <c>#</c> delimiters is treated
    /// as a bare address (<see cref="DisplayText"/> empty).
    /// </param>
    /// <returns>
    /// A populated <see cref="Hyperlink"/>, or <see langword="null"/> when
    /// <paramref name="value"/> is null or empty.
    /// </returns>
    public static Hyperlink? Parse(string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return null;
        }

        // Up to 4 parts separated by '#'. Extra '#'-delimited parts are joined
        // back into the screentip slot so no information is lost on round-trip.
        string[] parts = value!.Split('#');
        string display = parts.Length > 0 ? Decode(parts[0]) : string.Empty;
        string address = parts.Length > 1 ? Decode(parts[1]) : string.Empty;
        string subAddr = parts.Length > 2 ? Decode(parts[2]) : string.Empty;
        string tip = parts.Length > 3 ? Decode(string.Join("#", parts, 3, parts.Length - 3)) : string.Empty;

        // Bare address shortcut: a single token with no delimiters is the
        // address, not the display text. Matches DAO behaviour.
        if (parts.Length == 1)
        {
            return new Hyperlink(string.Empty, display);
        }

        return new Hyperlink(display, address, subAddr, tip);
    }

    /// <summary>
    /// Returns the encoded <c>displaytext#address#subaddress#screentip</c>
    /// representation suitable for storage in a hyperlink-flagged MEMO column.
    /// Trailing empty parts beyond <see cref="Address"/> are omitted to match
    /// the form Microsoft Access emits when those fields are blank.
    /// </summary>
    /// <returns>The encoded hyperlink string. Never <see langword="null"/>.</returns>
    public override string ToString()
    {
        var sb = new StringBuilder();
        _ = sb.Append(Encode(DisplayText));
        _ = sb.Append('#');
        _ = sb.Append(Encode(Address));

        bool hasTip = !string.IsNullOrEmpty(ScreenTip);
        bool hasSub = !string.IsNullOrEmpty(SubAddress);
        if (hasSub || hasTip)
        {
            _ = sb.Append('#');
            _ = sb.Append(Encode(SubAddress));
        }

        if (hasTip)
        {
            _ = sb.Append('#');
            _ = sb.Append(Encode(ScreenTip));
        }

        return sb.ToString();
    }

    private static string Encode(string s) => string.IsNullOrEmpty(s) ? string.Empty : s.Replace("#", "%23", StringComparison.Ordinal);

    private static string Decode(string s) => string.IsNullOrEmpty(s) ? string.Empty : s.Replace("%23", "#", StringComparison.Ordinal);
}
