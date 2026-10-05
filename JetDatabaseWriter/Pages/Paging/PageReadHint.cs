namespace JetDatabaseWriter.Pages.Paging;

/// <summary>Controls whether a page read participates in the writer frame cache.</summary>
internal enum PageReadHint
{
    /// <summary>Use the frame cache.</summary>
    Normal,

    /// <summary>Read without retaining a frame.</summary>
    NoCache,
}