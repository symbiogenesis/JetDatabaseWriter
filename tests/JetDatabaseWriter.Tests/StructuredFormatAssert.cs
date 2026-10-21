namespace JetDatabaseWriter.Tests;

using System;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using Xunit;

/// <summary>Checks exact exception types and stable format failure codes together.</summary>
internal static class StructuredFormatAssert
{
    /// <summary>Checks a synchronous structured failure.</summary>
    /// <typeparam name="TException">The exact expected exception type.</typeparam>
    /// <param name="code">The expected failure code.</param>
    /// <param name="action">The operation to invoke.</param>
    /// <returns>The captured failure.</returns>
    internal static TException Throws<TException>(JetErrorCode code, Action action)
        where TException : Exception, IJetException
    {
        TException error = Assert.Throws<TException>(action);
        Assert.Equal(code, error.ErrorCode);
        return error;
    }

    /// <summary>Checks an asynchronous structured failure.</summary>
    /// <typeparam name="TException">The exact expected exception type.</typeparam>
    /// <param name="code">The expected failure code.</param>
    /// <param name="action">The operation to invoke.</param>
    /// <returns>The captured failure.</returns>
    internal static async Task<TException> ThrowsAsync<TException>(JetErrorCode code, Func<Task> action)
        where TException : Exception, IJetException
    {
        TException error = await Assert.ThrowsAsync<TException>(action);
        Assert.Equal(code, error.ErrorCode);
        return error;
    }
}
