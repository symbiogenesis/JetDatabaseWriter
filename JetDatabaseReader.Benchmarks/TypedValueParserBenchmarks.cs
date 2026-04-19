namespace JetDatabaseReader.Benchmarks;

using System;
using BenchmarkDotNet.Attributes;

[MemoryDiagnoser]
public class TypedValueParserBenchmarks
{
    [Benchmark]
    public object Parse_Int32() => TypedValueParser.ParseValue("123456", typeof(int));

    [Benchmark]
    public object Parse_Int64() => TypedValueParser.ParseValue("9876543210", typeof(long));

    [Benchmark]
    public object Parse_Double() => TypedValueParser.ParseValue("3.14159265", typeof(double));

    [Benchmark]
    public object Parse_Decimal() => TypedValueParser.ParseValue("12345.6789", typeof(decimal));

    [Benchmark]
    public object Parse_DateTime() => TypedValueParser.ParseValue("2025-06-15T10:30:00", typeof(DateTime));

    [Benchmark]
    public object Parse_Boolean_True() => TypedValueParser.ParseValue("True", typeof(bool));

    [Benchmark]
    public object Parse_Boolean_One() => TypedValueParser.ParseValue("1", typeof(bool));

    [Benchmark]
    public object Parse_String() => TypedValueParser.ParseValue("Hello World", typeof(string));

    [Benchmark]
    public object Parse_Guid() => TypedValueParser.ParseValue("d3b07384-d9a0-4e9b-8a0d-1e2f3a4b5c6d", typeof(Guid));

    [Benchmark]
    public object Parse_ByteArray() => TypedValueParser.ParseValue("DE-AD-BE-EF-CA-FE", typeof(byte[]));

    [Benchmark]
    public object Parse_Empty() => TypedValueParser.ParseValue(string.Empty, typeof(int));

    [Benchmark]
    public object Parse_Invalid() => TypedValueParser.ParseValue("not-a-number", typeof(int));
}
