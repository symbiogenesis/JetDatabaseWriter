using System;
using System.IO;
using System.Text;

// Jackcess BASE_HEADER_MASK (128 bytes) - applied XOR starting at file offset 0x18
byte[] headerMask = {
    0xB5, 0x6F, 0x03, 0x62, 0x61, 0x08, 0xC2, 0x55,
    0xEB, 0xA9, 0x67, 0x72, 0x43, 0x3F, 0x00, 0x9C,
    0x7A, 0x9F, 0x90, 0xFF, 0x80, 0x9A, 0x31, 0xC5,
    0x79, 0xBA, 0xED, 0x30, 0xBC, 0xDF, 0xCC, 0x9D,
    0x63, 0xD9, 0xE4, 0xC3, 0x7B, 0x42, 0xFB, 0x8A,
    0xBC, 0x4E,
    // offset 42 in mask = file offset 0x42 (password area)
    0x86, 0xFB, 0xEC, 0x37, 0x5D, 0x44, 0x9C, 0xFA,
    0xC6, 0x5E, 0x28, 0xE6, 0x13, 0xB6, 0x8A, 0x60,
    0x54, 0x94, 0x7B, 0x36, 0xF5, 0x72, 0xDF, 0xB1,
    0x77, 0xF4, 0x13, 0x43, 0xCF, 0xAF, 0xB1, 0x33,
    0x34, 0x61, 0x79, 0x5B, 0x92, 0xB5, 0x7C, 0x2A,
    // offset 82 in mask = file offset 0x6A
    0x05, 0xF1, 0x7C, 0x99, 0x01, 0x1B, 0x98, 0xFD,
    0x12, 0x4F, 0x4A, 0x94, 0x6C, 0x3E,
    // offset 96 in mask = file offset 0x78
    0x60, 0x26, 0x5F, 0x95, 0xF8, 0xD0, 0x89, 0x24,
    0x85, 0x67, 0xC6, 0x1F, 0x27, 0x44, 0xD2, 0xEE,
    0xCF, 0x65, 0xED, 0xFF, 0x07, 0xC7, 0x46, 0xA1,
    0x78, 0x16, 0x0C, 0xED, 0xE9, 0x2D, 0x62, 0xD4,
};

byte[] hdr = new byte[0x98]; // need up to 0x97
using var fs = File.OpenRead("JetDatabaseReader.Tests/Databases/AesEncrypted.accdb");
fs.Read(hdr, 0, hdr.Length);

Console.WriteLine($"Version: 0x{hdr[0x14]:X2}");
Console.WriteLine($"EncFlag: 0x{hdr[0x62]:X2}");
Console.WriteLine($"Raw password bytes: {BitConverter.ToString(hdr, 0x42, 20)}");
Console.WriteLine($"Raw date bytes: {BitConverter.ToString(hdr, 0x72, 8)}");

// Step 1: Deobfuscate header by XORing with mask starting at offset 0x18
byte[] deob = (byte[])hdr.Clone();
for (int i = 0; i < headerMask.Length && (0x18 + i) < deob.Length; i++)
    deob[0x18 + i] ^= headerMask[i];

Console.WriteLine($"Deob password bytes: {BitConverter.ToString(deob, 0x42, 20)}");
Console.WriteLine($"Deob date bytes 0x72: {BitConverter.ToString(deob, 0x72, 8)}");

// Step 2: Read 8 bytes at 0x72 as double, cast to int, get 4-byte BE mask
long rawLong = BitConverter.ToInt64(deob, 0x72);
double dateVal = BitConverter.Int64BitsToDouble(rawLong);
Console.WriteLine($"dateVal = {dateVal}");
int dateInt = (int)dateVal;
Console.WriteLine($"dateInt = {dateInt}");
// Jackcess uses big-endian putInt
byte[] dateMask = BitConverter.GetBytes(dateInt);
if (BitConverter.IsLittleEndian) Array.Reverse(dateMask);
Console.WriteLine($"Date mask (BE): {BitConverter.ToString(dateMask)}");

// Also try little-endian
byte[] dateMaskLE = BitConverter.GetBytes(dateInt);
Console.WriteLine($"Date mask (LE): {BitConverter.ToString(dateMaskLE)}");

// Step 3: XOR password area with date mask
byte[] pwd = new byte[40];
Array.Copy(deob, 0x42, pwd, 0, 40);
for (int i = 0; i < 40; i++) pwd[i] ^= dateMask[i % 4];
string pwdStr = Encoding.Unicode.GetString(pwd);
int nullIdx = pwdStr.IndexOf('\0');
if (nullIdx >= 0) pwdStr = pwdStr[..nullIdx];
Console.WriteLine($"Password (BE mask): '{pwdStr}'");

// Try LE
byte[] pwd2 = new byte[40];
Array.Copy(deob, 0x42, pwd2, 0, 40);
for (int i = 0; i < 40; i++) pwd2[i] ^= dateMaskLE[i % 4];
string pwdStr2 = Encoding.Unicode.GetString(pwd2);
nullIdx = pwdStr2.IndexOf('\0');
if (nullIdx >= 0) pwdStr2 = pwdStr2[..nullIdx];
Console.WriteLine($"Password (LE mask): '{pwdStr2}'");
