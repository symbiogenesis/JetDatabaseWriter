namespace JetDatabaseWriter.Tests.Fuzz;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using Xunit;

/// <summary>Checks physical refusal atomicity and readable output for generated writer inputs.</summary>
internal static class WriterFuzzSafetyOracle
{
    /// <summary>Runs a statement, accepting only documented input refusals.</summary>
    /// <param name="stream">The physical database image.</param>
    /// <param name="operation">The mutation.</param>
    /// <returns>The expected rejection, or null after success.</returns>
    internal static async Task<Exception?> ExecuteAsync(MemoryStream stream, Func<Task> operation)
    {
        byte[] before = stream.ToArray();
        try
        {
            await operation();
            return null;
        }
        catch (Exception exception) when (IsExpectedRejection(exception))
        {
            AssertUnchanged(stream, before);
            return exception;
        }
    }

    /// <summary>Checks the database bytes and length after a refusal or rollback.</summary>
    /// <param name="stream">The physical database image.</param>
    /// <param name="before">The image before the operation.</param>
    internal static void AssertUnchanged(MemoryStream stream, byte[] before) => Assert.Equal(before, stream.ToArray());

    /// <summary>Opens successful output with its actual password and consumes every bounded generated row.</summary>
    /// <param name="stream">The physical database image.</param>
    /// <param name="password">The password used at creation.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The verification completion.</returns>
    internal static async Task VerifyRoundTripAsync(MemoryStream stream, ReadOnlyMemory<char> password, CancellationToken cancellationToken)
    {
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { Password = password, UseLockFile = false }, leaveOpen: true, cancellationToken);
        foreach (string tableName in await reader.ListTablesAsync(cancellationToken))
        {
            await foreach (object[] row in reader.Rows(tableName, cancellationToken: cancellationToken))
            {
                _ = row.Length;
            }
        }
    }

    private static bool IsExpectedRejection(Exception exception) => exception is IJetException jet && jet.ErrorCode is
        JetErrorCode.TableNotFound or JetErrorCode.ColumnNotFound or JetErrorCode.TableExists or JetErrorCode.ColumnExists
        or JetErrorCode.UniqueViolation or JetErrorCode.NotNullViolation or JetErrorCode.ForeignKeyMissingParent
        or JetErrorCode.ForeignKeyRestrictDelete or JetErrorCode.ForeignKeyRestrictUpdate or JetErrorCode.LastColumn
        or JetErrorCode.InvalidObjectName or JetErrorCode.DefaultNotAllowed or JetErrorCode.InvalidExpression
        or JetErrorCode.UnsupportedExpression or JetErrorCode.ColumnReferencedByExpression or JetErrorCode.TableInRelationship
        or JetErrorCode.KeyColumnInRelationship or JetErrorCode.ValidationRuleViolation or JetErrorCode.TableValidationRuleViolation
        or JetErrorCode.ValueTooLarge or JetErrorCode.RowTooLarge or JetErrorCode.JournalBudgetExceeded
        or JetErrorCode.NumericOverflow or JetErrorCode.IndexEntryTooLarge or JetErrorCode.ColumnCountLimit;
}
