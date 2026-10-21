namespace JetDatabaseWriter.Exceptions;

/// <summary>A database failure with a stable code and structured context.</summary>
#pragma warning disable CA1711 // The interface intentionally identifies exceptions implementing the database failure contract.
public interface IJetException
{
    /// <summary>Gets the stable failure code.</summary>
    public JetErrorCode ErrorCode { get; }

    /// <summary>Gets the context available at the failure site.</summary>
    public JetErrorInfo ErrorInfo { get; }
}
