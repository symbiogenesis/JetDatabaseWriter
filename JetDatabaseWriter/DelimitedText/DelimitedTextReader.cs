namespace JetDatabaseWriter.DelimitedText;

using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

internal sealed class DelimitedTextReader : IDisposable
{
    private const char Quote = '"';
    private const int DefaultBufferLength = 16 * 1024;
    private const int MaxRetainedFieldBuilderCapacity = 64 * 1024;
    private readonly TextReader _reader;
    private readonly DelimitedTextFormat _format;
    private readonly DelimitedTextLimits _limits;
    private readonly List<DelimitedTextField> _fields = [];
#if NET8_0_OR_GREATER
    private readonly SearchValues<char> _unquotedSpecialChars;
#else
    private readonly char[] _unquotedSpecialChars;
#endif
    private char[] _buffer;
    private StringBuilder _fieldBuilder = new();
    private int _bufferIndex;
    private int _bufferLength;
    private int _lineNumber = 1;
    private int _recordVersion;
    private int _rowIndex = -1;

    private static int IncrementLength(int currentLength, int maxLength, string optionName)
        => IncrementLength(currentLength, 1, maxLength, optionName);

    private static int IncrementLength(int currentLength, int increment, int maxLength, string optionName)
    {
        if (currentLength > maxLength - increment)
        {
            throw new InvalidDataException($"Delimited text source exceeds {optionName} ({maxLength}).");
        }

        return currentLength + increment;
    }

    private static bool IsTrimCharacter(char ch) => ch == ' ' || ch == '\t';

    private static long CountRecord(long recordCount, bool skipFirstRecord, ref bool isFirstRecord)
    {
        if (isFirstRecord)
        {
            isFirstRecord = false;
            return skipFirstRecord ? recordCount : recordCount + 1;
        }

        return recordCount + 1;
    }

#if NET8_0_OR_GREATER
    private static SearchValues<char> CreateUnquotedSpecialChars(char delimiter) => SearchValues.Create([delimiter, '\r', '\n']);

    private static int IndexOfUnquotedSpecialChar(ReadOnlySpan<char> source, SearchValues<char> specialChars) => source.IndexOfAny(specialChars);
#else
    private static char[] CreateUnquotedSpecialChars(char delimiter) => [delimiter, '\r', '\n'];

    private static int IndexOfUnquotedSpecialChar(ReadOnlySpan<char> source, char[] specialChars) => source.IndexOfAny(specialChars);
#endif

    internal DelimitedTextReader(TextReader reader, DelimitedTextFormat format, DelimitedTextLimits limits)
        : this(reader, format, limits, DefaultBufferLength)
    {
    }

    internal DelimitedTextReader(TextReader reader, DelimitedTextFormat format, DelimitedTextLimits limits, int bufferLength)
    {
        if (bufferLength <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(bufferLength), bufferLength, "Buffer length must be positive.");
        }

        _reader = reader ?? throw new ArgumentNullException(nameof(reader));
        _format = format;
        _limits = limits;
        _unquotedSpecialChars = CreateUnquotedSpecialChars(format.Delimiter);
        _buffer = ArrayPool<char>.Shared.Rent(bufferLength);
    }

    internal async ValueTask<DelimitedTextRecord?> ReadRecordAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();

        _recordVersion++;
        List<DelimitedTextField> fields = _fields;
        fields.Clear();
        StringBuilder field = ResetFieldBuilder();
        int bufferedFieldStart = -1;
        int bufferedFieldLength = 0;
        int recordLength = 0;
        int fieldLength = 0;
        int lineNumberFrom = _lineNumber;
        bool inQuotes = false;
        bool atFieldStart = true;
        bool sawAnyCharacter = false;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int value = await ReadCharAsync(cancellationToken).ConfigureAwait(false);
            if (value < 0)
            {
                if (!sawAnyCharacter && fields.Count == 0 && field.Length == 0)
                {
                    return null;
                }

                if (inQuotes)
                {
                    throw new InvalidDataException("Delimited text source contains a quoted field without a closing quote.");
                }

                AddField(fields, field, bufferedFieldStart, bufferedFieldLength);
                _ = ResetFieldBuilder();
                return CreateRecord(fields, lineNumberFrom, _lineNumber + 1);
            }

            recordLength = IncrementLength(recordLength, _limits.MaxRecordLength, _limits.MaxRecordLengthOptionName);
            char ch = (char)value;
            sawAnyCharacter = true;

            if (inQuotes)
            {
                if (ch == Quote)
                {
                    if (await PeekCharAsync(cancellationToken).ConfigureAwait(false) == Quote)
                    {
                        _ = await ReadCharAsync(cancellationToken).ConfigureAwait(false);
                        recordLength = IncrementLength(recordLength, _limits.MaxRecordLength, _limits.MaxRecordLengthOptionName);
                        fieldLength = AppendFieldCharacter(field, Quote, fieldLength, ref bufferedFieldStart, ref bufferedFieldLength);
                    }
                    else
                    {
                        inQuotes = false;
                    }

                    continue;
                }

                if (ch == '\r')
                {
                    fieldLength = AppendFieldCharacter(field, ch, fieldLength, ref bufferedFieldStart, ref bufferedFieldLength);
                    if (await PeekCharAsync(cancellationToken).ConfigureAwait(false) == '\n')
                    {
                        _ = await ReadCharAsync(cancellationToken).ConfigureAwait(false);
                        recordLength = IncrementLength(recordLength, _limits.MaxRecordLength, _limits.MaxRecordLengthOptionName);
                        fieldLength = AppendFieldCharacter(field, '\n', fieldLength, ref bufferedFieldStart, ref bufferedFieldLength);
                    }

                    IncrementLineNumber();
                    continue;
                }

                if (ch == '\n')
                {
                    fieldLength = AppendFieldCharacter(field, ch, fieldLength, ref bufferedFieldStart, ref bufferedFieldLength);
                    IncrementLineNumber();
                    continue;
                }

                fieldLength = AppendFieldCharacter(field, ch, fieldLength, ref bufferedFieldStart, ref bufferedFieldLength);
                continue;
            }

            if (atFieldStart && ch == Quote)
            {
                inQuotes = true;
                atFieldStart = false;
                continue;
            }

            if (_format.TrimValues && atFieldStart && IsTrimCharacter(ch))
            {
                continue;
            }

            if (ch == _format.Delimiter)
            {
                AddField(fields, field, bufferedFieldStart, bufferedFieldLength);
                field = ResetFieldBuilder();
                bufferedFieldStart = -1;
                bufferedFieldLength = 0;
                fieldLength = 0;
                atFieldStart = true;
                continue;
            }

            if (ch == '\r')
            {
                SpillBufferedField(field, ref bufferedFieldStart, ref bufferedFieldLength);
                if (await PeekCharAsync(cancellationToken).ConfigureAwait(false) == '\n')
                {
                    _ = await ReadCharAsync(cancellationToken).ConfigureAwait(false);
                    recordLength = IncrementLength(recordLength, _limits.MaxRecordLength, _limits.MaxRecordLengthOptionName);
                }

                IncrementLineNumber();
                AddField(fields, field, bufferedFieldStart, bufferedFieldLength);
                _ = ResetFieldBuilder();
                return CreateRecord(fields, lineNumberFrom, _lineNumber);
            }

            if (ch == '\n')
            {
                IncrementLineNumber();
                AddField(fields, field, bufferedFieldStart, bufferedFieldLength);
                _ = ResetFieldBuilder();
                return CreateRecord(fields, lineNumberFrom, _lineNumber);
            }

            fieldLength = AppendBufferedFieldCharacter(
                field,
                ch,
                fieldLength,
                ref bufferedFieldStart,
                ref bufferedFieldLength);
            AppendUnquotedRunFromBuffer(field, ref fieldLength, ref recordLength, ref bufferedFieldStart, ref bufferedFieldLength);
            atFieldStart = false;
        }
    }

    internal async ValueTask<long> CountRecordsAsync(bool skipFirstRecord, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();

        long recordCount = 0;
        int recordLength = 0;
        int fieldLength = 0;
        int fieldCount = 0;
        bool inQuotes = false;
        bool atFieldStart = true;
        bool sawAnyCharacter = false;
        bool isFirstRecord = true;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int value = await ReadCharAsync(cancellationToken).ConfigureAwait(false);
            if (value < 0)
            {
                if (!sawAnyCharacter && fieldCount == 0 && fieldLength == 0)
                {
                    return recordCount;
                }

                if (inQuotes)
                {
                    throw new InvalidDataException("Delimited text source contains a quoted field without a closing quote.");
                }

                AddCountedField(ref fieldCount);
                return CountRecord(recordCount, skipFirstRecord, ref isFirstRecord);
            }

            recordLength = IncrementLength(recordLength, _limits.MaxRecordLength, _limits.MaxRecordLengthOptionName);
            char ch = (char)value;
            sawAnyCharacter = true;

            if (inQuotes)
            {
                if (ch == Quote)
                {
                    if (await PeekCharAsync(cancellationToken).ConfigureAwait(false) == Quote)
                    {
                        _ = await ReadCharAsync(cancellationToken).ConfigureAwait(false);
                        recordLength = IncrementLength(recordLength, _limits.MaxRecordLength, _limits.MaxRecordLengthOptionName);
                        fieldLength = IncrementLength(fieldLength, _limits.MaxFieldLength, _limits.MaxFieldLengthOptionName);
                    }
                    else
                    {
                        inQuotes = false;
                    }

                    continue;
                }

                if (ch == '\r')
                {
                    fieldLength = IncrementLength(fieldLength, _limits.MaxFieldLength, _limits.MaxFieldLengthOptionName);
                    if (await PeekCharAsync(cancellationToken).ConfigureAwait(false) == '\n')
                    {
                        _ = await ReadCharAsync(cancellationToken).ConfigureAwait(false);
                        recordLength = IncrementLength(recordLength, _limits.MaxRecordLength, _limits.MaxRecordLengthOptionName);
                        fieldLength = IncrementLength(fieldLength, _limits.MaxFieldLength, _limits.MaxFieldLengthOptionName);
                    }

                    IncrementLineNumber();
                    continue;
                }

                if (ch == '\n')
                {
                    fieldLength = IncrementLength(fieldLength, _limits.MaxFieldLength, _limits.MaxFieldLengthOptionName);
                    IncrementLineNumber();
                    continue;
                }

                fieldLength = IncrementLength(fieldLength, _limits.MaxFieldLength, _limits.MaxFieldLengthOptionName);
                continue;
            }

            if (atFieldStart && ch == Quote)
            {
                inQuotes = true;
                atFieldStart = false;
                continue;
            }

            if (_format.TrimValues && atFieldStart && IsTrimCharacter(ch))
            {
                continue;
            }

            if (ch == _format.Delimiter)
            {
                AddCountedField(ref fieldCount);
                fieldLength = 0;
                atFieldStart = true;
                continue;
            }

            if (ch == '\r')
            {
                if (await PeekCharAsync(cancellationToken).ConfigureAwait(false) == '\n')
                {
                    _ = await ReadCharAsync(cancellationToken).ConfigureAwait(false);
                    recordLength = IncrementLength(recordLength, _limits.MaxRecordLength, _limits.MaxRecordLengthOptionName);
                }

                IncrementLineNumber();
                AddCountedField(ref fieldCount);
                recordCount = CountRecord(recordCount, skipFirstRecord, ref isFirstRecord);
                recordLength = 0;
                fieldLength = 0;
                fieldCount = 0;
                atFieldStart = true;
                sawAnyCharacter = false;
                continue;
            }

            if (ch == '\n')
            {
                IncrementLineNumber();
                AddCountedField(ref fieldCount);
                recordCount = CountRecord(recordCount, skipFirstRecord, ref isFirstRecord);
                recordLength = 0;
                fieldLength = 0;
                fieldCount = 0;
                atFieldStart = true;
                sawAnyCharacter = false;
                continue;
            }

            fieldLength = IncrementLength(fieldLength, _limits.MaxFieldLength, _limits.MaxFieldLengthOptionName);
            SkipUnquotedRunFromBuffer(ref fieldLength, ref recordLength);
            atFieldStart = false;
        }
    }

    public void Dispose()
    {
        if (_buffer.Length != 0)
        {
            ArrayPool<char>.Shared.Return(_buffer);
            _buffer = [];
        }

        _fields.Clear();
        _ = _fieldBuilder.Clear();
    }

    internal string[] MaterializeFields(int fieldCount, int version)
    {
        if (version != _recordVersion)
        {
            throw new InvalidOperationException("Delimited text records must be materialized before reading the next record.");
        }

        var result = new string[fieldCount];
        for (int i = 0; i < result.Length; i++)
        {
            DelimitedTextField field = _fields[i];
            result[i] = field.IsBuffered
                ? MaterializeBufferedField(field.BufferStart, field.BufferLength)
                : field.Value!;
        }

        return result;
    }

    private DelimitedTextRecord CreateRecord(List<DelimitedTextField> fields, int lineNumberFrom, int lineNumberToExclusive)
    {
        _rowIndex++;
        return new DelimitedTextRecord(this, fields.Count, _rowIndex, lineNumberFrom, lineNumberToExclusive, _recordVersion);
    }

    private async ValueTask<int> ReadCharAsync(CancellationToken cancellationToken)
    {
        if (_bufferIndex >= _bufferLength && !await FillBufferAsync(cancellationToken).ConfigureAwait(false))
        {
            return -1;
        }

        int value = _buffer[_bufferIndex];
        _bufferIndex++;
        return value;
    }

    private async ValueTask<int> PeekCharAsync(CancellationToken cancellationToken)
    {
        if (_bufferIndex >= _bufferLength && !await FillBufferAsync(cancellationToken).ConfigureAwait(false))
        {
            return -1;
        }

        return _buffer[_bufferIndex];
    }

    private async ValueTask<bool> FillBufferAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        MaterializeBufferedFieldsBeforeFill();
        _bufferIndex = 0;
        _bufferLength = await _reader.ReadAsync(_buffer, 0, _buffer.Length).ConfigureAwait(false);
        return _bufferLength > 0;
    }

    private void MaterializeBufferedFieldsBeforeFill()
    {
        for (int i = 0; i < _fields.Count; i++)
        {
            DelimitedTextField field = _fields[i];
            if (field.IsBuffered)
            {
                _fields[i] = DelimitedTextField.FromString(MaterializeBufferedField(field.BufferStart, field.BufferLength));
            }
        }
    }

    private int GetUnquotedRunLengthFromBuffer()
    {
        if (_bufferIndex >= _bufferLength)
        {
            return 0;
        }

        ReadOnlySpan<char> remaining = _buffer.AsSpan(_bufferIndex, _bufferLength - _bufferIndex);
        int relativeIndex = IndexOfUnquotedSpecialChar(remaining, _unquotedSpecialChars);
        return relativeIndex < 0 ? remaining.Length : relativeIndex;
    }

    private void AppendUnquotedRunFromBuffer(
        StringBuilder field,
        ref int fieldLength,
        ref int recordLength,
        ref int bufferedFieldStart,
        ref int bufferedFieldLength)
    {
        int runLength = GetUnquotedRunLengthFromBuffer();
        if (runLength == 0)
        {
            return;
        }

        fieldLength = IncrementLength(fieldLength, runLength, _limits.MaxFieldLength, _limits.MaxFieldLengthOptionName);
        recordLength = IncrementLength(recordLength, runLength, _limits.MaxRecordLength, _limits.MaxRecordLengthOptionName);
        AppendBufferedFieldRun(field, _bufferIndex, runLength, ref bufferedFieldStart, ref bufferedFieldLength);
        _bufferIndex += runLength;
        SpillBufferedFieldAtEndOfBuffer(field, ref bufferedFieldStart, ref bufferedFieldLength);
    }

    private void SkipUnquotedRunFromBuffer(ref int fieldLength, ref int recordLength)
    {
        int runLength = GetUnquotedRunLengthFromBuffer();
        if (runLength == 0)
        {
            return;
        }

        fieldLength = IncrementLength(fieldLength, runLength, _limits.MaxFieldLength, _limits.MaxFieldLengthOptionName);
        recordLength = IncrementLength(recordLength, runLength, _limits.MaxRecordLength, _limits.MaxRecordLengthOptionName);
        _bufferIndex += runLength;
    }

    private int AppendFieldCharacter(
        StringBuilder field,
        char ch,
        int fieldLength,
        ref int bufferedFieldStart,
        ref int bufferedFieldLength)
    {
        int newLength = IncrementLength(fieldLength, _limits.MaxFieldLength, _limits.MaxFieldLengthOptionName);
        SpillBufferedField(field, ref bufferedFieldStart, ref bufferedFieldLength);
        field.Append(ch);
        return newLength;
    }

    private int AppendBufferedFieldCharacter(
        StringBuilder field,
        char ch,
        int fieldLength,
        ref int bufferedFieldStart,
        ref int bufferedFieldLength)
    {
        int newLength = IncrementLength(fieldLength, _limits.MaxFieldLength, _limits.MaxFieldLengthOptionName);
        AppendBufferedFieldRun(field, _bufferIndex - 1, 1, ref bufferedFieldStart, ref bufferedFieldLength);
        SpillBufferedFieldAtEndOfBuffer(field, ref bufferedFieldStart, ref bufferedFieldLength);
        return newLength;
    }

    private void AddField(List<DelimitedTextField> fields, StringBuilder field, int bufferedFieldStart, int bufferedFieldLength)
    {
        if (fields.Count >= _limits.MaxColumnCount)
        {
            throw new InvalidDataException($"Delimited text source exceeds {_limits.MaxColumnCountOptionName} ({_limits.MaxColumnCount}).");
        }

        if (field.Length == 0 && bufferedFieldStart >= 0)
        {
            fields.Add(DelimitedTextField.FromBuffer(bufferedFieldStart, GetTrimmedBufferedLength(bufferedFieldStart, bufferedFieldLength)));
            return;
        }

        fields.Add(DelimitedTextField.FromString(MaterializeBuiltField(field)));
    }

    private void AppendBufferedFieldRun(
        StringBuilder field,
        int runStart,
        int runLength,
        ref int bufferedFieldStart,
        ref int bufferedFieldLength)
    {
        if (field.Length == 0 && bufferedFieldStart < 0)
        {
            bufferedFieldStart = runStart;
            bufferedFieldLength = runLength;
            return;
        }

        if (field.Length == 0 && bufferedFieldStart + bufferedFieldLength == runStart)
        {
            bufferedFieldLength += runLength;
            return;
        }

        SpillBufferedField(field, ref bufferedFieldStart, ref bufferedFieldLength);
        field.Append(_buffer, runStart, runLength);
    }

    private void SpillBufferedField(StringBuilder field, ref int bufferedFieldStart, ref int bufferedFieldLength)
    {
        if (bufferedFieldStart < 0)
        {
            return;
        }

        field.Append(_buffer, bufferedFieldStart, bufferedFieldLength);
        bufferedFieldStart = -1;
        bufferedFieldLength = 0;
    }

    private void SpillBufferedFieldAtEndOfBuffer(StringBuilder field, ref int bufferedFieldStart, ref int bufferedFieldLength)
    {
        if (_bufferIndex >= _bufferLength)
        {
            SpillBufferedField(field, ref bufferedFieldStart, ref bufferedFieldLength);
        }
    }

    private string MaterializeBufferedField(int bufferStart, int bufferLength)
    {
        int length = GetTrimmedBufferedLength(bufferStart, bufferLength);
        return length == 0 ? string.Empty : new string(_buffer, bufferStart, length);
    }

    private string MaterializeBuiltField(StringBuilder field)
    {
        int length = GetTrimmedBuiltLength(field);
        if (length == 0)
        {
            return string.Empty;
        }

        return length == field.Length ? field.ToString() : field.ToString(0, length);
    }

    private int GetTrimmedBufferedLength(int bufferStart, int bufferLength)
    {
        if (!_format.TrimValues)
        {
            return bufferLength;
        }

        int length = bufferLength;
        while (length > 0 && IsTrimCharacter(_buffer[bufferStart + length - 1]))
        {
            length--;
        }

        return length;
    }

    private int GetTrimmedBuiltLength(StringBuilder field)
    {
        if (!_format.TrimValues)
        {
            return field.Length;
        }

        int length = field.Length;
        while (length > 0 && IsTrimCharacter(field[length - 1]))
        {
            length--;
        }

        return length;
    }

    private StringBuilder ResetFieldBuilder()
    {
        if (_fieldBuilder.Capacity > MaxRetainedFieldBuilderCapacity)
        {
            _fieldBuilder = new StringBuilder();
            return _fieldBuilder;
        }

        _ = _fieldBuilder.Clear();
        return _fieldBuilder;
    }

    private void AddCountedField(ref int fieldCount)
    {
        if (fieldCount >= _limits.MaxColumnCount)
        {
            throw new InvalidDataException($"Delimited text source exceeds {_limits.MaxColumnCountOptionName} ({_limits.MaxColumnCount}).");
        }

        fieldCount++;
    }

    private void IncrementLineNumber()
    {
        _lineNumber++;
    }

    private void ThrowIfDisposed()
    {
        if (_buffer.Length == 0)
        {
            throw new ObjectDisposedException(nameof(DelimitedTextReader));
        }
    }
}
