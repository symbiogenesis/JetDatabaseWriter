namespace JetDatabaseWriter.Enums;

/// <summary>Supported native text sort orders for Text and Memo columns.</summary>
public enum TextCollation
{
    /// <summary>The General 97 sort order in Jet3 databases.</summary>
    General97 = 0,

    /// <summary>The General Legacy sort order (LCID 1033, version 0) in Jet4 and ACE databases.</summary>
    GeneralLegacy = 1,

    /// <summary>The General sort order (LCID 1033, version 1) in Jet4 and ACE databases.</summary>
    General = 2,
}
