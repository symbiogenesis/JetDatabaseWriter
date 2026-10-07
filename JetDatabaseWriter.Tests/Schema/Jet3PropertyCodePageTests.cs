namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Buffers.Binary;
using System.Text;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using Xunit;

/// <summary>Regression checks for database-specific Jet3 property encoding.</summary>
public sealed class Jet3PropertyCodePageTests
{
    [Fact]
    public void PropertyNamesAndValues_UseHeaderCodePage()
    {
        JetFormat format = CreateFormat(1251);
        var builder = new ColumnPropertyBlockBuilder();
        builder.GetOrAddTarget("Колонка").AddText("Описание", "Привет", format);
        builder.GetOrAddTableTarget().AddMemoText("Description", "Привет", format);
        byte[] blob = builder.ToBytes(format)!;

        ColumnPropertyBlock parsed = ColumnPropertyBlock.Parse(blob, format)!;
        Assert.Equal("Привет", parsed.FindTarget("Колонка")!.GetTextValue("Описание", format));
        Assert.Equal("Привет", parsed.FindTableTarget()!.GetTextValue("Description", format));
        Assert.Equal(blob, parsed.ToBytes(format));
    }

    [Theory]
    [InlineData("中文")]
    [InlineData("Łódź")]
    public void UnrepresentablePropertyText_IsRejected(string value)
    {
        JetFormat format = CreateFormat(1252);
        var builder = new ColumnPropertyBlockBuilder();
        Assert.Throws<EncoderFallbackException>(() => builder.GetOrAddTableTarget().AddText("Description", value, format));
        builder.GetOrAddTarget(value).AddBoolean("Required", true);
        Assert.Throws<EncoderFallbackException>(() => builder.ToBytes(format));
    }

    [Fact]
    public void LinkedOdbcNameMap_UsesHeaderCodePage()
    {
        JetFormat format = CreateFormat(1251);
        byte[] blob = LinkedOdbcLvPropBuilder.Build("dbo.Таблица", [new("Колонка", typeof(string), maxLength: 20)], format);
        ColumnPropertyBlock parsed = ColumnPropertyBlock.Parse(blob, format)!;
        Assert.NotNull(parsed.FindTarget("Колонка"));
        byte[] nameMap = parsed.FindTableTarget()!.Find("NameMap")!.Value;
        string names = format.AnsiEncoding.GetString(nameMap);
        Assert.Contains("Таблица", names, StringComparison.Ordinal);
        Assert.Contains("Колонка", names, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("中文")]
    [InlineData("Łódź")]
    public async Task CreateTableWithUnrepresentableProperty_LeavesDatabaseUnchanged(string description)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, DatabaseFormat.Jet3Mdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        byte[] before = stream.ToArray();
        await Assert.ThrowsAsync<EncoderFallbackException>(async () => await writer.CreateTableAsync(
            "T", [new ColumnDefinition("Id", typeof(int)) { Description = description }], TestContext.Current.CancellationToken));
        Assert.Equal(before, stream.ToArray());
    }
    [Fact]
    public void MissingHeaderCodePage_UsesWindows1252()
    {
        Assert.Equal(1252, CreateFormat(0).CodePage);
    }
    private static JetFormat CreateFormat(ushort codePage)
    {
        byte[] header = TDefPageBuilder.BuildEmptyDatabase(DatabaseFormat.Jet3Mdb, fullCatalogSchema: false);
        JetDatabaseWriter.Encryption.EncryptionManager.TransformHeaderMask(header, DatabaseFormat.Jet3Mdb);
        BinaryPrimitives.WriteUInt16LittleEndian(header.AsSpan(Constants.DatabaseHeader.CodePage), codePage);
        JetDatabaseWriter.Encryption.EncryptionManager.TransformHeaderMask(header, DatabaseFormat.Jet3Mdb);
        return JetFormat.FromHeader(header);
    }
}