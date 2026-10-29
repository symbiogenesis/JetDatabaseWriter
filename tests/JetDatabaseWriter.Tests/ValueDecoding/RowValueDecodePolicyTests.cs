namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using Xunit;
using static JetDatabaseWriter.Enums.ColumnType;

public sealed class RowValueDecodePolicyTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void MalformedValueFilter_ExcludesIoAndCancellation(bool strict)
    {
        Assert.False(RowValueDecodePolicy.IsMalformedValueException(new IOException("disk failure")));
        Assert.False(RowValueDecodePolicy.IsMalformedValueException(new OperationCanceledException()));
        Assert.True(RowValueDecodePolicy.IsMalformedValueException(new ArgumentException("invalid bytes")));
#pragma warning disable CA2201 // The policy must recognize this runtime exception without swallowing I/O failures.
        Assert.True(RowValueDecodePolicy.IsMalformedValueException(new IndexOutOfRangeException()));
#pragma warning restore CA2201
        Assert.True(RowValueDecodePolicy.IsMalformedValueException(new OverflowException()));
        Assert.True(RowValueDecodePolicy.HasFixedPayload(new ColumnInfo { Type = MoneyType }, 8, strict));
    }

    [Fact]
    public void EmptyVariableValue_TextAndMemo_ReturnsEmptyString()
    {
        Assert.Equal(string.Empty, RowValueDecodePolicy.EmptyVariableValue(new ColumnInfo { Type = TextType }));
        Assert.Equal(string.Empty, RowValueDecodePolicy.EmptyVariableValue(new ColumnInfo { Type = MemoType }));
    }

    [Fact]
    public void EmptyVariableValue_BinaryAndOle_ReturnsEmptyByteArray()
    {
        Assert.Same(Array.Empty<byte>(), RowValueDecodePolicy.EmptyVariableValue(new ColumnInfo { Type = BinaryType }));
        Assert.Same(Array.Empty<byte>(), RowValueDecodePolicy.EmptyVariableValue(new ColumnInfo { Type = OleType }));
    }

    [Fact]
    public void FixedVariableSlotTooShort_NonStrict_ReturnsDBNull()
    {
        object value = RowValueDecodePolicy.FixedVariableSlotTooShort(
            new ColumnInfo { Name = "Amount", Type = MoneyType },
            actualLength: 3,
            requiredLength: 8,
            strictParsing: false);

        Assert.Equal(DBNull.Value, value);
    }

    [Fact]
    public void FixedVariableSlotTooShort_Strict_ThrowsCorruption()
    {
        JetCorruptDataException ex = Assert.Throws<JetCorruptDataException>(() =>
            RowValueDecodePolicy.FixedVariableSlotTooShort(
                new ColumnInfo { Name = "Amount", Type = MoneyType },
                actualLength: 3,
                requiredLength: 8,
                strictParsing: true));

        Assert.Equal(JetErrorCode.MalformedValue, ex.ErrorCode);
        Assert.Equal("Amount", ex.ErrorInfo.ColumnName);
        Assert.Contains("Amount", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void MalformedVariableValue_NonStrict_ReturnsDBNull()
    {
        object value = RowValueDecodePolicy.MalformedVariableValue(
            new ColumnInfo { Name = "When", Type = DateTimeType },
            new ArgumentException("bad date"),
            strictParsing: false);

        Assert.Equal(DBNull.Value, value);
    }

    [Fact]
    public void MalformedVariableDate_Strict_WrapsAsCorruption()
    {
        var inner = new ArgumentException("bad date");
        JetCorruptDataException ex = Assert.Throws<JetCorruptDataException>(() =>
            RowValueDecodePolicy.MalformedVariableValue(
                new ColumnInfo { Name = "When", Type = DateTimeType },
                inner,
                strictParsing: true));

        Assert.Equal(JetErrorCode.MalformedValue, ex.ErrorCode);
        Assert.Equal("When", ex.ErrorInfo.ColumnName);
        Assert.Same(inner, ex.InnerException);
        Assert.Contains("When", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void LenientScalarFault_EmitsDiagnosticAndToleratesListenerFailure(bool calculated)
    {
        var column = new ColumnInfo
        {
            Name = "Amount",
            Type = calculated ? MemoType : MoneyType,
            ExtraFlags = calculated ? Constants.CalculatedColumn.ExtFlagMask : (byte)0,
            CalculatedResultType = MoneyType,
        };
        using var output = new StringWriter(System.Globalization.CultureInfo.InvariantCulture);
        using var listener = new TextWriterTraceListener(output);
        var traceScope = new TestTraceScope(listener);
        try
        {
            Assert.Equal(DBNull.Value, RowValueDecodePolicy.MalformedVariableValue(column, new ArgumentException("secret payload"), strictParsing: false));
            Assert.Contains("Amount", output.ToString(), StringComparison.Ordinal);
            Assert.Contains("Currency", output.ToString(), StringComparison.Ordinal);
            Assert.Contains("missing value", output.ToString(), StringComparison.Ordinal);
            Assert.Contains(calculated ? "calculated" : "variable-area", output.ToString(), StringComparison.Ordinal);
            Assert.DoesNotContain("secret payload", output.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            traceScope.Dispose();
        }

        using var throwing = new ThrowingTraceListener();
        var throwingScope = new TestTraceScope(throwing);
        try
        {
            Assert.Equal(DBNull.Value, RowValueDecodePolicy.MalformedVariableValue(column, new ArgumentException("bad currency"), strictParsing: false));
        }
        finally
        {
            throwingScope.Dispose();
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void MalformedVariableValue_IoAndCancellation_RethrowsOriginalException(bool strict)
    {
        var column = new ColumnInfo { Name = "Amount", Type = MoneyType };
        var io = new IOException("disk failure");
        var cancellation = new OperationCanceledException();
        Assert.Same(io, Assert.Throws<IOException>(() => RowValueDecodePolicy.MalformedVariableValue(column, io, strict)));
        Assert.Same(cancellation, Assert.Throws<OperationCanceledException>(() => RowValueDecodePolicy.MalformedVariableValue(column, cancellation, strict)));
    }

    [Theory]
    [InlineData(20)]
    [InlineData(21)]
    [InlineData(22)]
    public void CalculatedEnvelope_TruncatedOverheadCannotBecomeNull(int length)
    {
        byte[] envelope = new byte[length];
        JetCorruptDataException exception = Assert.Throws<JetCorruptDataException>(() => RowValueDecodePolicy.UnwrapCalculatedPayload(envelope));
        Assert.Equal(JetErrorCode.MalformedValue, exception.ErrorCode);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void CalculatedEnvelope_PayloadCannotConsumeTrailingOverhead(int excess)
    {
        byte[] envelope = CalculatedColumnUtil.Wrap([1, 2, 3, 4]);
        BinaryPrimitives.WriteInt32LittleEndian(envelope.AsSpan(Constants.CalculatedColumn.DataLenOffset), 4 + excess);
        JetCorruptDataException exception = Assert.Throws<JetCorruptDataException>(() => RowValueDecodePolicy.UnwrapCalculatedPayload(envelope));
        Assert.Equal(JetErrorCode.MalformedValue, exception.ErrorCode);
    }

    [Fact]
    public void CalculatedEnvelope_CompleteNullAndPayloadKeepTheirValues()
    {
        Assert.Empty(RowValueDecodePolicy.UnwrapCalculatedPayload(CalculatedColumnUtil.Wrap([])));
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, RowValueDecodePolicy.UnwrapCalculatedPayload(CalculatedColumnUtil.Wrap([1, 2, 3, 4])));
    }

    private sealed class ThrowingTraceListener : TraceListener
    {
        public override void Write(string? message) => throw new IOException("diagnostic failure");

        public override void WriteLine(string? message) => throw new IOException("diagnostic failure");
    }

    [Fact]
    public void MalformedVariableValue_JetLimitationException_RethrowsOriginalException()
    {
        var limitation = new JetLimitationException("numeric overflow");

        JetLimitationException ex = Assert.Throws<JetLimitationException>(() =>
            RowValueDecodePolicy.MalformedVariableValue(
                new ColumnInfo { Name = "DecimalValue", Type = NumericType },
                limitation,
                strictParsing: false));

        Assert.Same(limitation, ex);
    }
}
