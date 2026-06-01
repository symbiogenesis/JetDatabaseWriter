namespace JetDatabaseWriter.DelimitedText;

using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Infrastructure;

internal sealed class DelimitedTextReader : IDisposable
{
    private const char Quote = '"';
    private const int DefaultBufferLength = 16 * 1024;
    private const int MaxRetainedFieldBuilderCapacity = 64 * 1024;
    private readonly TextReader reader;
    private readonly DelimitedTextFormat format;
    private readonly DelimitedTextLimits limits;
    private readonly List<DelimitedTextField> fields = [];
#if NET8_0_OR_GREATER
    private readonly SearchValues<char> unquotedSpecialChars;
#else
    private readonly char[] unquotedSpecialChars;
#endif
    private char[] buffer;
    private StringBuilder fieldBuilder = new();
    private int bufferIndex;
    private int bufferLength;
    private int lineNumber = 1;
    private int recordVersion;
    private int rowIndex = -1;

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

    private static bool IsTrimCharacter(char ch) => ch is ' ' or '\t';

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

        this.reader = reader ?? throw new ArgumentNullException(nameof(reader));
        this.format = format;
        this.limits = limits;
        this.unquotedSpecialChars = CreateUnquotedSpecialChars(format.Delimiter);
        this.buffer = ArrayPool<char>.Shared.Rent(bufferLength);
    }

    internal async ValueTask<DelimitedTextRecord?> ReadRecordAsync(CancellationToken cancellationToken)
    {
        this.ThrowIfDisposed();

        this.recordVersion++;
        List<DelimitedTextField> fields = this.fields;
        fields.Clear();
        StringBuilder field = this.ResetFieldBuilder();
        int bufferedFieldStart = -1;
        int bufferedFieldLength = 0;
        int recordLength = 0;
        int fieldLength = 0;
        int lineNumberFrom = this.lineNumber;
        bool inQuotes = false;
        bool atFieldStart = true;
        bool sawAnyCharacter = false;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int value = await this.ReadCharAsync(cancellationToken).ConfigureAwait(false);
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

                this.AddField(fields, field, bufferedFieldStart, bufferedFieldLength);
                _ = this.ResetFieldBuilder();
                return this.CreateRecord(fields, lineNumberFrom, this.lineNumber + 1);
            }

            recordLength = IncrementLength(recordLength, this.limits.MaxRecordLength, this.limits.MaxRecordLengthOptionName);
            char ch = (char)value;
            sawAnyCharacter = true;

            if (inQuotes)
            {
                if (ch == Quote)
                {
                    if (await this.PeekCharAsync(cancellationToken).ConfigureAwait(false) == Quote)
                    {
                        _ = await this.ReadCharAsync(cancellationToken).ConfigureAwait(false);
                        recordLength = IncrementLength(recordLength, this.limits.MaxRecordLength, this.limits.MaxRecordLengthOptionName);
                        fieldLength = this.AppendFieldCharacter(field, Quote, fieldLength, ref bufferedFieldStart, ref bufferedFieldLength);
                    }
                    else
                    {
                        inQuotes = false;
                    }

                    continue;
                }

                if (ch == '\r')
                {
                    fieldLength = this.AppendFieldCharacter(field, ch, fieldLength, ref bufferedFieldStart, ref bufferedFieldLength);
                    if (await this.PeekCharAsync(cancellationToken).ConfigureAwait(false) == '\n')
                    {
                        _ = await this.ReadCharAsync(cancellationToken).ConfigureAwait(false);
                        recordLength = IncrementLength(recordLength, this.limits.MaxRecordLength, this.limits.MaxRecordLengthOptionName);
                        fieldLength = this.AppendFieldCharacter(field, '\n', fieldLength, ref bufferedFieldStart, ref bufferedFieldLength);
                    }

                    this.IncrementLineNumber();
                    continue;
                }

                if (ch == '\n')
                {
                    fieldLength = this.AppendFieldCharacter(field, ch, fieldLength, ref bufferedFieldStart, ref bufferedFieldLength);
                    this.IncrementLineNumber();
                    continue;
                }

                fieldLength = this.AppendFieldCharacter(field, ch, fieldLength, ref bufferedFieldStart, ref bufferedFieldLength);
                continue;
            }

            if (atFieldStart && ch == Quote)
            {
                inQuotes = true;
                atFieldStart = false;
                continue;
            }

            if (this.format.TrimValues && atFieldStart && IsTrimCharacter(ch))
            {
                continue;
            }

            if (ch == this.format.Delimiter)
            {
                this.AddField(fields, field, bufferedFieldStart, bufferedFieldLength);
                field = this.ResetFieldBuilder();
                bufferedFieldStart = -1;
                bufferedFieldLength = 0;
                fieldLength = 0;
                atFieldStart = true;
                continue;
            }

            if (ch == '\r')
            {
                this.SpillBufferedField(field, ref bufferedFieldStart, ref bufferedFieldLength);
                if (await this.PeekCharAsync(cancellationToken).ConfigureAwait(false) == '\n')
                {
                    _ = await this.ReadCharAsync(cancellationToken).ConfigureAwait(false);
                    recordLength = IncrementLength(recordLength, this.limits.MaxRecordLength, this.limits.MaxRecordLengthOptionName);
                }

                this.IncrementLineNumber();
                this.AddField(fields, field, bufferedFieldStart, bufferedFieldLength);
                _ = this.ResetFieldBuilder();
                return this.CreateRecord(fields, lineNumberFrom, this.lineNumber);
            }

            if (ch == '\n')
            {
                this.IncrementLineNumber();
                this.AddField(fields, field, bufferedFieldStart, bufferedFieldLength);
                _ = this.ResetFieldBuilder();
                return this.CreateRecord(fields, lineNumberFrom, this.lineNumber);
            }

            fieldLength = this.AppendBufferedFieldCharacter(
                field,
                fieldLength,
                ref bufferedFieldStart,
                ref bufferedFieldLength);
            this.AppendUnquotedRunFromBuffer(field, ref fieldLength, ref recordLength, ref bufferedFieldStart, ref bufferedFieldLength);
            atFieldStart = false;
        }
    }

    internal async ValueTask<long> CountRecordsAsync(bool skipFirstRecord, CancellationToken cancellationToken)
    {
        this.ThrowIfDisposed();

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
            int value = await this.ReadCharAsync(cancellationToken).ConfigureAwait(false);
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

                this.AddCountedField(ref fieldCount);
                return CountRecord(recordCount, skipFirstRecord, ref isFirstRecord);
            }

            recordLength = IncrementLength(recordLength, this.limits.MaxRecordLength, this.limits.MaxRecordLengthOptionName);
            char ch = (char)value;
            sawAnyCharacter = true;

            if (inQuotes)
            {
                if (ch == Quote)
                {
                    if (await this.PeekCharAsync(cancellationToken).ConfigureAwait(false) == Quote)
                    {
                        _ = await this.ReadCharAsync(cancellationToken).ConfigureAwait(false);
                        recordLength = IncrementLength(recordLength, this.limits.MaxRecordLength, this.limits.MaxRecordLengthOptionName);
                        fieldLength = IncrementLength(fieldLength, this.limits.MaxFieldLength, this.limits.MaxFieldLengthOptionName);
                    }
                    else
                    {
                        inQuotes = false;
                    }

                    continue;
                }

                if (ch == '\r')
                {
                    fieldLength = IncrementLength(fieldLength, this.limits.MaxFieldLength, this.limits.MaxFieldLengthOptionName);
                    if (await this.PeekCharAsync(cancellationToken).ConfigureAwait(false) == '\n')
                    {
                        _ = await this.ReadCharAsync(cancellationToken).ConfigureAwait(false);
                        recordLength = IncrementLength(recordLength, this.limits.MaxRecordLength, this.limits.MaxRecordLengthOptionName);
                        fieldLength = IncrementLength(fieldLength, this.limits.MaxFieldLength, this.limits.MaxFieldLengthOptionName);
                    }

                    this.IncrementLineNumber();
                    continue;
                }

                if (ch == '\n')
                {
                    fieldLength = IncrementLength(fieldLength, this.limits.MaxFieldLength, this.limits.MaxFieldLengthOptionName);
                    this.IncrementLineNumber();
                    continue;
                }

                fieldLength = IncrementLength(fieldLength, this.limits.MaxFieldLength, this.limits.MaxFieldLengthOptionName);
                continue;
            }

            if (atFieldStart && ch == Quote)
            {
                inQuotes = true;
                atFieldStart = false;
                continue;
            }

            if (this.format.TrimValues && atFieldStart && IsTrimCharacter(ch))
            {
                continue;
            }

            if (ch == this.format.Delimiter)
            {
                this.AddCountedField(ref fieldCount);
                fieldLength = 0;
                atFieldStart = true;
                continue;
            }

            if (ch == '\r')
            {
                if (await this.PeekCharAsync(cancellationToken).ConfigureAwait(false) == '\n')
                {
                    _ = await this.ReadCharAsync(cancellationToken).ConfigureAwait(false);
                    recordLength = IncrementLength(recordLength, this.limits.MaxRecordLength, this.limits.MaxRecordLengthOptionName);
                }

                this.IncrementLineNumber();
                this.AddCountedField(ref fieldCount);
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
                this.IncrementLineNumber();
                this.AddCountedField(ref fieldCount);
                recordCount = CountRecord(recordCount, skipFirstRecord, ref isFirstRecord);
                recordLength = 0;
                fieldLength = 0;
                fieldCount = 0;
                atFieldStart = true;
                sawAnyCharacter = false;
                continue;
            }

            fieldLength = IncrementLength(fieldLength, this.limits.MaxFieldLength, this.limits.MaxFieldLengthOptionName);
            this.SkipUnquotedRunFromBuffer(ref fieldLength, ref recordLength);
            atFieldStart = false;
        }
    }

    public void Dispose()
    {
        if (this.buffer.Length != 0)
        {
            ArrayPool<char>.Shared.Return(this.buffer);
            this.buffer = [];
        }

        this.fields.Clear();
        _ = this.fieldBuilder.Clear();
    }

    internal string[] MaterializeFields(int fieldCount, int version)
    {
        if (version != this.recordVersion)
        {
            throw new InvalidOperationException("Delimited text records must be materialized before reading the next record.");
        }

        string[] result = new string[fieldCount];
        for (int i = 0; i < result.Length; i++)
        {
            DelimitedTextField field = this.fields[i];
            result[i] = field.IsBuffered
                ? this.MaterializeBufferedField(field.BufferStart, field.BufferLength)
                : field.Value!;
        }

        return result;
    }

    private DelimitedTextRecord CreateRecord(List<DelimitedTextField> fields, int lineNumberFrom, int lineNumberToExclusive)
    {
        this.rowIndex++;
        return new DelimitedTextRecord(this, fields.Count, this.rowIndex, lineNumberFrom, lineNumberToExclusive, this.recordVersion);
    }

    private async ValueTask<int> ReadCharAsync(CancellationToken cancellationToken)
    {
        if (this.bufferIndex >= this.bufferLength && !await this.FillBufferAsync(cancellationToken).ConfigureAwait(false))
        {
            return -1;
        }

        int value = this.buffer[this.bufferIndex];
        this.bufferIndex++;
        return value;
    }

    private async ValueTask<int> PeekCharAsync(CancellationToken cancellationToken)
    {
        if (this.bufferIndex >= this.bufferLength && !await this.FillBufferAsync(cancellationToken).ConfigureAwait(false))
        {
            return -1;
        }

        return this.buffer[this.bufferIndex];
    }

    private async ValueTask<bool> FillBufferAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        this.MaterializeBufferedFieldsBeforeFill();
        this.bufferIndex = 0;
        this.bufferLength = await this.reader.ReadAsync(this.buffer, 0, this.buffer.Length).ConfigureAwait(false);
        return this.bufferLength > 0;
    }

    private void MaterializeBufferedFieldsBeforeFill()
    {
        for (int i = 0; i < this.fields.Count; i++)
        {
            DelimitedTextField field = this.fields[i];
            if (field.IsBuffered)
            {
                this.fields[i] = DelimitedTextField.FromString(this.MaterializeBufferedField(field.BufferStart, field.BufferLength));
            }
        }
    }

    private int GetUnquotedRunLengthFromBuffer()
    {
        if (this.bufferIndex >= this.bufferLength)
        {
            return 0;
        }

        ReadOnlySpan<char> remaining = this.buffer.AsSpan(this.bufferIndex, this.bufferLength - this.bufferIndex);
        int relativeIndex = IndexOfUnquotedSpecialChar(remaining, this.unquotedSpecialChars);
        return relativeIndex < 0 ? remaining.Length : relativeIndex;
    }

    private void AppendUnquotedRunFromBuffer(
        StringBuilder field,
        ref int fieldLength,
        ref int recordLength,
        ref int bufferedFieldStart,
        ref int bufferedFieldLength)
    {
        int runLength = this.GetUnquotedRunLengthFromBuffer();
        if (runLength == 0)
        {
            return;
        }

        fieldLength = IncrementLength(fieldLength, runLength, this.limits.MaxFieldLength, this.limits.MaxFieldLengthOptionName);
        recordLength = IncrementLength(recordLength, runLength, this.limits.MaxRecordLength, this.limits.MaxRecordLengthOptionName);
        this.AppendBufferedFieldRun(field, this.bufferIndex, runLength, ref bufferedFieldStart, ref bufferedFieldLength);
        this.bufferIndex += runLength;
        this.SpillBufferedFieldAtEndOfBuffer(field, ref bufferedFieldStart, ref bufferedFieldLength);
    }

    private void SkipUnquotedRunFromBuffer(ref int fieldLength, ref int recordLength)
    {
        int runLength = this.GetUnquotedRunLengthFromBuffer();
        if (runLength == 0)
        {
            return;
        }

        fieldLength = IncrementLength(fieldLength, runLength, this.limits.MaxFieldLength, this.limits.MaxFieldLengthOptionName);
        recordLength = IncrementLength(recordLength, runLength, this.limits.MaxRecordLength, this.limits.MaxRecordLengthOptionName);
        this.bufferIndex += runLength;
    }

    private int AppendFieldCharacter(
        StringBuilder field,
        char ch,
        int fieldLength,
        ref int bufferedFieldStart,
        ref int bufferedFieldLength)
    {
        int newLength = IncrementLength(fieldLength, this.limits.MaxFieldLength, this.limits.MaxFieldLengthOptionName);
        this.SpillBufferedField(field, ref bufferedFieldStart, ref bufferedFieldLength);
        field.Append(ch);
        return newLength;
    }

    private int AppendBufferedFieldCharacter(
        StringBuilder field,
        int fieldLength,
        ref int bufferedFieldStart,
        ref int bufferedFieldLength)
    {
        int newLength = IncrementLength(fieldLength, this.limits.MaxFieldLength, this.limits.MaxFieldLengthOptionName);
        this.AppendBufferedFieldRun(field, this.bufferIndex - 1, 1, ref bufferedFieldStart, ref bufferedFieldLength);
        this.SpillBufferedFieldAtEndOfBuffer(field, ref bufferedFieldStart, ref bufferedFieldLength);
        return newLength;
    }

    private void AddField(List<DelimitedTextField> fields, StringBuilder field, int bufferedFieldStart, int bufferedFieldLength)
    {
        if (fields.Count >= this.limits.MaxColumnCount)
        {
            throw new InvalidDataException($"Delimited text source exceeds {this.limits.MaxColumnCountOptionName} ({this.limits.MaxColumnCount}).");
        }

        if (field.Length == 0 && bufferedFieldStart >= 0)
        {
            fields.Add(DelimitedTextField.FromBuffer(bufferedFieldStart, this.GetTrimmedBufferedLength(bufferedFieldStart, bufferedFieldLength)));
            return;
        }

        fields.Add(DelimitedTextField.FromString(this.MaterializeBuiltField(field)));
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

        this.SpillBufferedField(field, ref bufferedFieldStart, ref bufferedFieldLength);
        field.Append(this.buffer, runStart, runLength);
    }

    private void SpillBufferedField(StringBuilder field, ref int bufferedFieldStart, ref int bufferedFieldLength)
    {
        if (bufferedFieldStart < 0)
        {
            return;
        }

        field.Append(this.buffer, bufferedFieldStart, bufferedFieldLength);
        bufferedFieldStart = -1;
        bufferedFieldLength = 0;
    }

    private void SpillBufferedFieldAtEndOfBuffer(StringBuilder field, ref int bufferedFieldStart, ref int bufferedFieldLength)
    {
        if (this.bufferIndex >= this.bufferLength)
        {
            this.SpillBufferedField(field, ref bufferedFieldStart, ref bufferedFieldLength);
        }
    }

    private string MaterializeBufferedField(int bufferStart, int bufferLength)
    {
        int length = this.GetTrimmedBufferedLength(bufferStart, bufferLength);
        return length == 0 ? string.Empty : new string(this.buffer, bufferStart, length);
    }

    private string MaterializeBuiltField(StringBuilder field)
    {
        int length = this.GetTrimmedBuiltLength(field);
        if (length == 0)
        {
            return string.Empty;
        }

        return length == field.Length ? field.ToString() : field.ToString(0, length);
    }

    private int GetTrimmedBufferedLength(int bufferStart, int bufferLength)
    {
        if (!this.format.TrimValues)
        {
            return bufferLength;
        }

        int length = bufferLength;
        while (length > 0 && IsTrimCharacter(this.buffer[bufferStart + length - 1]))
        {
            length--;
        }

        return length;
    }

    private int GetTrimmedBuiltLength(StringBuilder field)
    {
        if (!this.format.TrimValues)
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
        if (this.fieldBuilder.Capacity > MaxRetainedFieldBuilderCapacity)
        {
            this.fieldBuilder = new StringBuilder();
            return this.fieldBuilder;
        }

        _ = this.fieldBuilder.Clear();
        return this.fieldBuilder;
    }

    private void AddCountedField(ref int fieldCount)
    {
        if (fieldCount >= this.limits.MaxColumnCount)
        {
            throw new InvalidDataException($"Delimited text source exceeds {this.limits.MaxColumnCountOptionName} ({this.limits.MaxColumnCount}).");
        }

        fieldCount++;
    }

    private void IncrementLineNumber() => this.lineNumber++;

    private void ThrowIfDisposed() => Guard.ThrowIfDisposed(this.buffer.Length == 0, this);
}
