namespace JetDatabaseWriter.Tests;

using System;
using System.IO;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

/// <summary>Storage refusals expose stable context without reclassifying physical I/O failures.</summary>
public sealed class StructuredStorageFailuresTests
{
    /// <summary>The native long-value length bound has a size failure code.</summary>
    [Fact]
    public void LongValueLength_ReportsStructuredSizeLimit()
    {
        var descriptor = LongValueDescriptor.Inline(LongValueDescriptor.MaxLength + 1);
        JetLimitationException failure = Assert.Throws<JetLimitationException>(descriptor.ToHeaderBytes);
        Assert.Equal(JetErrorCode.ValueTooLarge, failure.ErrorCode);
    }

    /// <summary>A strict long-value read retains the column and underlying corruption.</summary>
    [Fact]
    public void StrictLongValue_ReportsColumnAndCause()
    {
        var cause = new JetCorruptDataException(JetErrorCode.UnreadableLongValue, "malformed descriptor");
        JetCorruptDataException failure = Assert.Throws<JetCorruptDataException>(() => LongValueReadPolicy.MissingValue("Notes", cause, strictParsing: true));
        Assert.Equal(JetErrorCode.UnreadableLongValue, failure.ErrorCode);
        Assert.Equal("Notes", failure.ErrorInfo.ColumnName);
        Assert.Same(cause, failure.InnerException);
    }

    /// <summary>Linked text byte budgets expose their table without masking the source failure.</summary>
    [Fact]
    public void LinkedTextBudget_ReportsStructuredSizeLimit()
    {
        using var bytes = new MemoryStream([1, 2]);
        using var bounded = new LinkedTextSourceStream(bytes, maxBytes: 1, "Import");
        byte[] buffer = new byte[2];
        JetLimitationException failure = Assert.Throws<JetLimitationException>(() => bounded.Read(buffer, 0, buffer.Length));
        Assert.Equal(JetErrorCode.LinkedSourceBudgetExceeded, failure.ErrorCode);
        Assert.Equal("Import", failure.ErrorInfo.TableName);
    }

    /// <summary>A real stream I/O failure reaches the caller unchanged.</summary>
    [Fact]
    public void LinkedTextSource_PreservesPhysicalIoFailure()
    {
        var expected = new IOException("physical read failure");
        using var source = new FailedReadStream(expected);
        using var bounded = new LinkedTextSourceStream(source, maxBytes: 1, "Import");
        byte[] buffer = new byte[1];
        Assert.Same(expected, Assert.Throws<IOException>(() => bounded.Read(buffer, 0, buffer.Length)));
    }

    /// <summary>A physical failure cannot become a lenient missing long value.</summary>
    /// <param name="invalidData">Whether the caller stream reports invalid data.</param>
    /// <param name="strict">Whether strict parsing was requested.</param>
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void LongValuePolicy_PreservesPhysicalIoFailure(bool invalidData, bool strict)
    {
        Exception expected = invalidData ? new InvalidDataException("physical read failure") : new IOException("physical read failure");
        Assert.Same(expected, Assert.Throws(expected.GetType(), () => LongValueReadPolicy.MissingValue("Notes", expected, strict)));
    }

    private sealed class FailedReadStream(IOException failure) : MemoryStream
    {
        public override int Read(Span<byte> buffer) => throw failure;
    }
}
