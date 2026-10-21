namespace JetDatabaseWriter.Encryption.Models;

/// <summary>
/// Contains the two Office Crypto streams written into an encrypted CFB package.
/// </summary>
/// <param name="EncryptionInfo">The <c>EncryptionInfo</c> stream bytes.</param>
/// <param name="EncryptedPackage">The <c>EncryptedPackage</c> stream bytes.</param>
internal readonly record struct OfficeEncryptedPackage(byte[] EncryptionInfo, byte[] EncryptedPackage);
