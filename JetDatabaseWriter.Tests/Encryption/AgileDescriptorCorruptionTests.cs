namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;
using System.Xml;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Exceptions;
using Xunit;

public sealed class AgileDescriptorCorruptionTests
{
    [Fact]
    public void Decrypt_RejectsNoncanonicalReservedHeaderFlags()
    {
        byte[] info = Info(DescriptorXml());
        info[4] = 0x41;

        Assert.Throws<InvalidDataException>(() => OfficeCryptoAgile.Decrypt(info, new byte[8], "password"));
    }

    [Theory]
    [InlineData("spinCount=\"2147483647\"")]
    [InlineData("spinCount=\"-1\"")]
    [InlineData("blockSize=\"0\"")]
    [InlineData("keyBits=\"2147483640\"")]
    [InlineData("hashSize=\"0\"")]
    [InlineData("saltSize=\"65537\"")]
    public void Decrypt_RejectsInvalidParametersBeforePasswordWork(string replacement)
    {
        ArgumentNullException.ThrowIfNull(replacement);

        string name = replacement[..replacement.IndexOf('=', StringComparison.Ordinal)];
        string xml = DescriptorXml();
        string original = name switch
        {
            "spinCount" => "spinCount=\"0\"",
            "blockSize" => "blockSize=\"16\"",
            "keyBits" => "keyBits=\"256\"",
            "hashSize" => "hashSize=\"64\"",
            "saltSize" => "saltSize=\"16\"",
            _ => throw new InvalidOperationException(),
        };

        byte[] info = Info(xml.Replace(original, replacement, StringComparison.Ordinal));
        Assert.Throws<InvalidDataException>(() => OfficeCryptoAgile.Decrypt(info, [], "password"));
    }

    [Fact]
    public void Decrypt_RefusesConfiguredSpinBudgetBeforeHashing()
    {
        string xml = DescriptorXml().Replace("spinCount=\"0\"", "spinCount=\"100001\"", StringComparison.Ordinal);

        JetLimitationException failure = Assert.Throws<JetLimitationException>(() =>
            OfficeCryptoAgile.Decrypt(Info(xml), new byte[8], "password", maxSpinCount: 100000));

        Assert.Equal(JetErrorCode.ValueTooLarge, failure.ErrorCode);
    }

    [Fact]
    public void Decrypt_RejectsPlaintextSizeBeyondCiphertextBeforePasswordWork()
    {
        byte[] package = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(package, int.MaxValue);

        Assert.Throws<InvalidDataException>(() => OfficeCryptoAgile.Decrypt(Info(DescriptorXml()), package, "password"));
    }

    [Fact]
    public void Decrypt_ProhibitsExternalEntities()
    {
        byte[] info = Info("<!DOCTYPE encryption [<!ENTITY external SYSTEM 'file:///untrusted-secret'>]><encryption>&external;</encryption>");

        Assert.Throws<XmlException>(() => OfficeCryptoAgile.Decrypt(info, [], "password"));
    }

    [Fact]
    public void Decrypt_RejectsOversizedXmlBeforeParsing()
    {
        byte[] info = Info(new string(' ', (1024 * 1024) + 1));

        Assert.Throws<JetLimitationException>(() => OfficeCryptoAgile.Decrypt(info, [], "password"));
    }

    [Theory]
    [InlineData("<dataIntegrity/>")]
    [InlineData("<dataIntegrity encryptedHmacKey=\"AA==\"/>")]
    [InlineData("<dataIntegrity encryptedHmacValue=\"AA==\"/>")]
    [InlineData("<dataIntegrity encryptedHmacKey=\"\" encryptedHmacValue=\"\"/>")]
    public void Decrypt_RejectsIncompleteIntegrityBeforePasswordWork(string integrity)
    {
        string xml = DescriptorXml().Replace("<keyEncryptors>", integrity + "<keyEncryptors>", StringComparison.Ordinal);

        InvalidDataException failure = Assert.Throws<InvalidDataException>(() =>
            OfficeCryptoAgile.Decrypt(Info(xml), new byte[8], "password"));

        Assert.Contains("integrity", failure.Message, StringComparison.OrdinalIgnoreCase);
    }

    private static byte[] Info(string xml)
    {
        byte[] bytes = new byte[8 + Encoding.UTF8.GetByteCount(xml)];
        bytes[0] = 4;
        bytes[2] = 4;
        bytes[4] = 0x40;
        _ = Encoding.UTF8.GetBytes(xml, 0, xml.Length, bytes, 8);
        return bytes;
    }

    private static string DescriptorXml()
    {
        string salt = Convert.ToBase64String(new byte[16]);
        string verifier = Convert.ToBase64String(new byte[64]);
        string key = Convert.ToBase64String(new byte[32]);
        string attributes = $"saltSize=\"16\" blockSize=\"16\" keyBits=\"256\" hashSize=\"64\" cipherAlgorithm=\"AES\" cipherChaining=\"ChainingModeCBC\" hashAlgorithm=\"SHA512\" saltValue=\"{salt}\"";
        return $"<encryption><keyData {attributes}/><keyEncryptors><keyEncryptor uri=\"http://schemas.microsoft.com/office/2006/keyEncryptor/password\"><encryptedKey spinCount=\"0\" {attributes} encryptedVerifierHashInput=\"{salt}\" encryptedVerifierHashValue=\"{verifier}\" encryptedKeyValue=\"{key}\"/></keyEncryptor></keyEncryptors></encryption>";
    }
}
