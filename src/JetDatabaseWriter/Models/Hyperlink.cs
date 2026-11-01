namespace JetDatabaseWriter.Models;

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
    private StoredRepresentation? storedRepresentation;

    /// <summary>
    /// Initializes a new instance of the <see cref="Hyperlink"/> class.
    /// </summary>
    /// <param name="displayText">Optional human-readable label shown to the user. May be empty.</param>
    /// <param name="address">URL or file path the hyperlink targets. May be empty.</param>
    /// <param name="subAddress">Optional anchor / subaddress within <paramref name="address"/> (e.g. bookmark name, sheet name). May be empty.</param>
    /// <param name="screenTip">Optional tooltip text shown when hovering. May be empty.</param>
    public Hyperlink(string displayText = "", string address = "", string subAddress = "", string screenTip = "")
    {
        this.DisplayText = displayText ?? string.Empty;
        this.Address = address ?? string.Empty;
        this.SubAddress = subAddress ?? string.Empty;
        this.ScreenTip = screenTip ?? string.Empty;
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
    /// The encoded hyperlink string. <see langword="null"/> returns
    /// <see langword="null"/>; an empty string returns an empty hyperlink.
    /// A string with no <c>#</c> delimiters is treated
    /// as a bare address (<see cref="DisplayText"/> empty).
    /// </param>
    /// <returns>
    /// A <see cref="Hyperlink"/>, or <see langword="null"/> when
    /// <paramref name="value"/> is null.
    /// </returns>
    public static Hyperlink? Parse(string? value)
    {
        if (value is null)
        {
            return null;
        }

        // Up to 4 parts separated by '#'. Extra delimiters stay in the
        // screentip slot so no information is lost on round-trip.
#if NET8_0_OR_GREATER
        ReadOnlySpan<char> source = value.AsSpan();
        Span<Range> parts = stackalloc Range[4];
        int partCount = source.Split(parts, '#');
        string display = Decode(source[parts[0]].ToString());
        string address = partCount > 1 ? Decode(source[parts[1]].ToString()) : string.Empty;
        string subAddr = partCount > 2 ? Decode(source[parts[2]].ToString()) : string.Empty;
        string tip = partCount > 3 ? Decode(source[parts[3]].ToString()) : string.Empty;
#else
        string[] parts = value.Split('#');
        int partCount = parts.Length;
        string display = Decode(parts[0]);
        string address = parts.Length > 1 ? Decode(parts[1]) : string.Empty;
        string subAddr = parts.Length > 2 ? Decode(parts[2]) : string.Empty;
        string tip = parts.Length > 3 ? Decode(string.Join("#", parts, 3, parts.Length - 3)) : string.Empty;
#endif

        // Bare address shortcut: a single token with no delimiters is the
        // address, not the display text. Matches DAO behaviour.
        if (partCount == 1)
        {
            return PreserveRepresentation(new Hyperlink(string.Empty, display), value);
        }

        return PreserveRepresentation(new Hyperlink(display, address, subAddr, tip), value);
    }

    /// <inheritdoc/>
    public bool Equals(Hyperlink? other)
        => other is not null
            && string.Equals(this.DisplayText, other.DisplayText, StringComparison.Ordinal)
            && string.Equals(this.Address, other.Address, StringComparison.Ordinal)
            && string.Equals(this.SubAddress, other.SubAddress, StringComparison.Ordinal)
            && string.Equals(this.ScreenTip, other.ScreenTip, StringComparison.Ordinal);

    /// <inheritdoc/>
    public override int GetHashCode() => HashCode.Combine(this.DisplayText, this.Address, this.SubAddress, this.ScreenTip);

    /// <summary>
    /// Returns the encoded <c>displaytext#address#subaddress#screentip</c>
    /// representation suitable for storage in a hyperlink-flagged MEMO column.
    /// An unchanged parsed value retains its stored spelling. Newly constructed
    /// or edited values omit trailing empty parts beyond <see cref="Address"/>;
    /// a newly constructed empty link serializes as an empty string.
    /// </summary>
    /// <returns>The encoded hyperlink string. Never <see langword="null"/>.</returns>
    public override string ToString()
    {
        if (this.storedRepresentation is { } stored
            && string.Equals(this.DisplayText, stored.DisplayText, StringComparison.Ordinal)
            && string.Equals(this.Address, stored.Address, StringComparison.Ordinal)
            && string.Equals(this.SubAddress, stored.SubAddress, StringComparison.Ordinal)
            && string.Equals(this.ScreenTip, stored.ScreenTip, StringComparison.Ordinal))
        {
            return stored.Text;
        }

        if (string.IsNullOrEmpty(this.DisplayText) && string.IsNullOrEmpty(this.Address)
            && string.IsNullOrEmpty(this.SubAddress) && string.IsNullOrEmpty(this.ScreenTip))
        {
            return string.Empty;
        }

        var sb = new StringBuilder();
        _ = sb.Append(Encode(this.DisplayText));
        _ = sb.Append('#');
        _ = sb.Append(Encode(this.Address));

        bool hasTip = !string.IsNullOrEmpty(this.ScreenTip);
        bool hasSub = !string.IsNullOrEmpty(this.SubAddress);
        if (hasSub || hasTip)
        {
            _ = sb.Append('#');
            _ = sb.Append(Encode(this.SubAddress));
        }

        if (hasTip)
        {
            _ = sb.Append('#');
            _ = sb.Append(Encode(this.ScreenTip));
        }

        return sb.ToString();
    }

    private static string Encode(string s) => string.IsNullOrEmpty(s) ? string.Empty : s.Replace("#", "%23", StringComparison.Ordinal);

    private static string Decode(string s) => string.IsNullOrEmpty(s) ? string.Empty : s.Replace("%23", "#", StringComparison.Ordinal);

    private static Hyperlink PreserveRepresentation(Hyperlink value, string text)
    {
        value.storedRepresentation = new StoredRepresentation(text, value.DisplayText, value.Address, value.SubAddress, value.ScreenTip);
        return value;
    }

    private readonly record struct StoredRepresentation(string Text, string DisplayText, string Address, string SubAddress, string ScreenTip);
}
