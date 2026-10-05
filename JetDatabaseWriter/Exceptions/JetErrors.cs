namespace JetDatabaseWriter.Exceptions;

/// <summary>Creates structured failures at deliberate refusal sites.</summary>
internal static class JetErrors
{
    /// <summary>Creates an operation refusal.</summary>
    /// <param name="code">The failure code.</param>
    /// <param name="message">The unchanged failure message.</param>
    /// <param name="errorInfo">The available context.</param>
    public static JetOperationException Operation(JetErrorCode code, string message, JetErrorInfo? errorInfo = null)
        => new(code, message, errorInfo);

    /// <summary>Creates a constraint refusal.</summary>
    /// <param name="code">The failure code.</param>
    /// <param name="message">The unchanged failure message.</param>
    /// <param name="errorInfo">The available context.</param>
    public static JetConstraintException Constraint(JetErrorCode code, string message, JetErrorInfo? errorInfo = null)
        => new(code, message, errorInfo);

    /// <summary>Creates a validation-rule refusal.</summary>
    /// <param name="code">The failure code.</param>
    /// <param name="message">The unchanged failure message.</param>
    /// <param name="errorInfo">The available context.</param>
    public static JetValidationRuleException Validation(JetErrorCode code, string message, JetErrorInfo? errorInfo = null)
        => new(code, message, errorInfo);

    /// <summary>Creates a missing argument object refusal.</summary>
    /// <param name="code">The failure code.</param>
    /// <param name="message">The unchanged failure message.</param>
    /// <param name="paramName">The parameter name.</param>
    /// <param name="errorInfo">The available context.</param>
    public static JetObjectNotFoundException NotFound(JetErrorCode code, string message, string? paramName = null, JetErrorInfo? errorInfo = null)
        => new(code, message, paramName, errorInfo);

    /// <summary>Creates a limitation refusal.</summary>
    /// <param name="code">The failure code.</param>
    /// <param name="message">The unchanged failure message.</param>
    /// <param name="errorInfo">The available context.</param>
    public static JetLimitationException Limitation(JetErrorCode code, string message, JetErrorInfo? errorInfo = null)
        => new(code, message, errorInfo);
}
