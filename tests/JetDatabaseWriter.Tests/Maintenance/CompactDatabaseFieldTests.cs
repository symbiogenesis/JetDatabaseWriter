namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;
using Xunit;

public sealed class CompactDatabaseFieldTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DatabaseFields_DiscoverStoredLongValuesAndRetainNullFields(DatabaseFormat format)
    {
        byte[] bytes = await CreateAsync(format);
        byte[] original = SHA256.HashData(bytes);
        await using DatabaseFile file = Open(bytes);
        var session = new CompactDiscoverySession(file.Pages, file.Format, new());
        long owner = 0;
        int fields = 0;
        int nullMemos = 0;
        int storedMemos = 0;
        int memoTerminals = 0;
        int blobTerminals = 0;
        int externalRows = 0;
        await session.ScanDatabaseFieldsAsync(
            (record, _) =>
            {
                if (record.Kind == "catalog-object" && record.Name == "Items")
                {
                    owner = record.OwnerTDefPage!.Value;
                }

                return ValueTask.CompletedTask;
            },
            (table, field, _) =>
            {
                fields++;
                Assert.True(session.Accounting.ActiveWorkingBytes > 0);
                if (table == owner && field.Column.Name == "Memo")
                {
                    nullMemos += field.Kind == CompactRawFieldKind.Null ? 1 : 0;
                    storedMemos += field.Kind == CompactRawFieldKind.Stored ? 1 : 0;
                }

                return ValueTask.CompletedTask;
            },
            (table, field, record, _) =>
            {
                Assert.Equal(CompactRawFieldKind.Stored, field.Kind);
                Assert.True(field.IsLongValue);
                if (table == owner)
                {
                    memoTerminals += record.Kind == "lval-terminal" && field.Column.Name == "Memo" ? 1 : 0;
                    blobTerminals += record.Kind == "lval-terminal" && field.Column.Name == "Blob" ? 1 : 0;
                    externalRows += record.Kind == "lval-row" ? 1 : 0;
                }

                return ValueTask.CompletedTask;
            },
            (_, _) => ValueTask.CompletedTask,
            Ct);
        Assert.True(owner > 2);
        Assert.Equal(1, nullMemos);
        Assert.Equal(1, storedMemos);
        Assert.Equal(1, memoTerminals);
        Assert.Equal(1, blobTerminals);
        Assert.True(externalRows > 0);
        Assert.Equal(fields, session.Accounting.ReadCount(CompactDiscoveryCharge.Fields));
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
        Assert.Equal(original, SHA256.HashData(bytes));
    }

    [Fact]
    public async Task DatabaseFields_FailedScansRetainTheCumulativeFieldAllowance()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        await using DatabaseFile file = Open(bytes);
        var session = new CompactDiscoverySession(file.Pages, file.Format, new() { MaxFields = 1 });
        int fields = 0;
        for (int attempt = 0; attempt < 2; attempt++)
        {
            await Assert.ThrowsAsync<JetLimitationException>(async () => await session.ScanDatabaseFieldsAsync(
                (_, _) => ValueTask.CompletedTask,
                (_, _, _) =>
                {
                    fields++;
                    return ValueTask.CompletedTask;
                },
                (_, _, _, _) => ValueTask.CompletedTask,
                (_, _) => ValueTask.CompletedTask,
                Ct));
            Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
        }

        Assert.Equal(1, fields);
        Assert.Equal(1, session.Accounting.ReadCount(CompactDiscoveryCharge.Fields));
    }

    [Fact]
    public async Task DatabaseFields_VisitorFailureReleasesBorrowedStorageAndPreservesException()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        await using DatabaseFile file = Open(bytes);
        var session = new CompactDiscoverySession(file.Pages, file.Format, new());
        var failure = new IOException("field consumer failed");
        IOException actual = await Assert.ThrowsAsync<IOException>(async () => await session.ScanDatabaseFieldsAsync(
            (_, _) => ValueTask.CompletedTask,
            (_, _, _) => ValueTask.FromException(failure),
            (_, _, _, _) => ValueTask.CompletedTask,
            (_, _) => ValueTask.CompletedTask,
            Ct));
        Assert.Same(failure, actual);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task DatabaseFields_CancellationFromAFieldVisitorStopsDiscovery()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        await using DatabaseFile file = Open(bytes);
        var session = new CompactDiscoverySession(file.Pages, file.Format, new());
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        int fields = 0;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await session.ScanDatabaseFieldsAsync(
            (_, _) => ValueTask.CompletedTask,
            async (_, _, _) =>
            {
                fields++;
                await cancellation.CancelAsync();
            },
            (_, _, _, _) => ValueTask.CompletedTask,
            (_, _) => ValueTask.CompletedTask,
            cancellation.Token));
        Assert.Equal(1, fields);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData("FixedText/NativeFixedTextJet3Cp1252.mdb")]
    [InlineData("FixedText/NativeFixedTextJet4.mdb")]
    [InlineData("ComplexFields.accdb")]
    public async Task DatabaseFields_NativeFixturesIncludeSystemFieldsAndKnownIndexOwnership(string fixture)
    {
        byte[] bytes = await File.ReadAllBytesAsync(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", fixture), Ct);
        byte[] original = SHA256.HashData(bytes);
        await using DatabaseFile file = Open(bytes);
        var session = new CompactDiscoverySession(file.Pages, file.Format, new());
        int fields = 0;
        int systemFields = 0;
        int ownershipTerminals = 0;
        await session.ScanDatabaseFieldsAsync(
            (_, _) => ValueTask.CompletedTask,
            (owner, _, _) =>
            {
                fields++;
                systemFields += owner == 2 ? 1 : 0;
                return ValueTask.CompletedTask;
            },
            (_, _, _, _) => ValueTask.CompletedTask,
            (_, _) => ValueTask.CompletedTask,
            Ct,
            (record, _) =>
            {
                Assert.False(record.ExclusiveOwnershipProven);
                Assert.True(record.IsPinned);
                if (record.Kind == "ownership-terminal")
                {
                    Assert.True(record.DeclaredRootsObserved);
                    ownershipTerminals++;
                }

                return ValueTask.CompletedTask;
            });
        Assert.True(fields > 0);
        Assert.True(systemFields > 0);
        Assert.Equal(1, ownershipTerminals);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
        Assert.Equal(original, SHA256.HashData(bytes));
    }

    [Fact]
    public async Task DatabaseFields_DanglingLongValueRefusesWithoutACompletionOrSourceMutation()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        int pointerOffset = -1;
        await using (DatabaseFile file = Open(bytes))
        {
            var session = new CompactDiscoverySession(file.Pages, file.Format, new());
            await session.ScanDatabaseFieldsAsync(
                (_, _) => ValueTask.CompletedTask,
                (_, field, _) =>
                {
                    if (field.Column.Name == "Memo" && field.Kind == CompactRawFieldKind.Stored)
                    {
                        pointerOffset = checked((int)(field.Location.DataPageNumber * file.Format.PageSize) + field.Location.RowStart + field.RowOffset + 4);
                    }

                    return ValueTask.CompletedTask;
                },
                (_, _, _, _) => ValueTask.CompletedTask,
                (_, _) => ValueTask.CompletedTask,
                Ct);
        }

        Assert.True(pointerOffset >= 0);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(pointerOffset, 4), 0xFFFFFF00);
        byte[] damaged = SHA256.HashData(bytes);
        await using DatabaseFile source = Open(bytes);
        var audit = new CompactDiscoverySession(source.Pages, source.Format, new());
        bool valueCompleted = false;
        bool ownershipCompleted = false;
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await audit.ScanDatabaseFieldsAsync(
            (_, _) => ValueTask.CompletedTask,
            (_, _, _) => ValueTask.CompletedTask,
            (_, field, record, _) =>
            {
                valueCompleted |= field.Column.Name == "Memo" && record.Kind == "lval-terminal";
                return ValueTask.CompletedTask;
            },
            (_, _) => ValueTask.CompletedTask,
            Ct,
            (record, _) =>
            {
                ownershipCompleted |= record.Kind == "ownership-terminal";
                return ValueTask.CompletedTask;
            }));
        Assert.False(valueCompleted);
        Assert.False(ownershipCompleted);
        Assert.Equal(0, audit.Accounting.ActiveWorkingBytes);
        Assert.Equal(damaged, SHA256.HashData(bytes));
    }

    [Theory]
    [InlineData(false, false, false, false)]
    [InlineData(true, false, false, false)]
    [InlineData(true, true, false, false)]
    [InlineData(true, false, true, false)]
    [InlineData(true, true, true, false)]
    [InlineData(true, false, true, true)]
    [InlineData(true, true, true, true)]
    public async Task DatabaseFields_CalculatedFixtureHydratesResultTypesAndDiscoversMemoStorage(bool recoverCatalog, bool conflictingOwner, bool moved, bool damagedMovedOwner)
    {
        string path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "Jackcess", "V2010", "calcFieldTestV2010.accdb");
        byte[] bytes = await File.ReadAllBytesAsync(path, Ct);
        if (recoverCatalog)
        {
            await using DatabaseFile intact = Open(bytes);
            TDefImage catalog = Assert.IsType<TDefImage>(await intact.TableDefs.ReadImageAsync(2, Ct));
            var decoder = new CatalogRowReader(intact.Format, intact.TableDefs, intact.OwnedPages);
            bool damaged = false;
            foreach (CatalogRow row in await decoder.GetCatalogRowsAsync(catalog.Definition, Ct, scanPhysicalPages: true))
            {
                if (row.ObjectType != Constants.SystemObjects.UserTableType || row.TDefPage <= 2)
                {
                    continue;
                }

                TDefImage image = Assert.IsType<TDefImage>(await intact.TableDefs.ReadImageAsync(row.TDefPage, Ct));
                foreach (ColumnInfo column in image.Columns)
                {
                    if (column.IsCalculated)
                    {
                        int offset = checked(((int)row.PageNumber * intact.Format.PageSize) + intact.Format.DataPage.TDefOff);
                        uint owner = conflictingOwner ? checked((uint)row.TDefPage) : 0;
                        if (moved)
                        {
                            byte[] payload = [];
                            RowLocation location = default;
                            var discovery = new CompactDiscoverySession(intact.Pages, intact.Format, new());
                            await discovery.ScanRowsAsync(
                                2,
                                (candidate, data, _) =>
                                {
                                    if (candidate.PageNumber == row.PageNumber && candidate.RowIndex == row.RowIndex)
                                    {
                                        location = candidate;
                                        payload = data.ToArray();
                                    }

                                    return ValueTask.CompletedTask;
                                },
                                Ct);
                            Assert.NotEmpty(payload);
                            Assert.False(location.IsOverflow);
                            int target = bytes.Length / intact.Format.PageSize;
                            Array.Resize(ref bytes, bytes.Length + intact.Format.PageSize);
                            int dataOffset = ((target + 1) * intact.Format.PageSize) - payload.Length;
                            payload.CopyTo(bytes, dataOffset);
                            bytes[target * intact.Format.PageSize] = Constants.PageTypes.Data;
                            BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan((target * intact.Format.PageSize) + intact.Format.DataPage.TDefOff, sizeof(uint)), damagedMovedOwner ? owner : 2);
                            BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan((target * intact.Format.PageSize) + intact.Format.DataPage.NumRows, sizeof(ushort)), 1);
                            BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan((target * intact.Format.PageSize) + intact.Format.DataPage.RowsStart, sizeof(ushort)), checked((ushort)((intact.Format.PageSize - payload.Length) | Constants.DataPage.DeletedRowFlag)));
                            int headerOffset = checked(((int)row.PageNumber * intact.Format.PageSize) + location.RowStart);
                            bytes[headerOffset] = 0;
                            bytes[headerOffset + 1] = unchecked((byte)target);
                            bytes[headerOffset + 2] = unchecked((byte)(target >> 8));
                            bytes[headerOffset + 3] = unchecked((byte)(target >> 16));
                            BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(checked(((int)row.PageNumber * intact.Format.PageSize) + intact.Format.DataPage.RowsStart + (row.RowIndex * 2)), sizeof(ushort)), checked((ushort)(location.RowStart | Constants.DataPage.OverflowRowFlag)));
                        }

                        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(offset, sizeof(uint)), owner);
                        damaged = true;
                        break;
                    }
                }

                if (damaged)
                {
                    break;
                }
            }

            Assert.True(damaged);
        }

        byte[] original = SHA256.HashData(bytes);
        await using DatabaseFile file = Open(bytes);
        var session = new CompactDiscoverySession(file.Pages, file.Format, new());
        int calculated = 0;
        int memoTerminals = 0;
        int memoExternalRows = 0;
        bool ownershipCompleted = false;
        int recoveredTargets = 0;
        int recoveredTerminals = 0;
        await session.ScanDatabaseFieldsAsync(
            (_, _) => ValueTask.CompletedTask,
            (_, field, _) =>
            {
                if (field.Column.IsCalculated && field.Kind == CompactRawFieldKind.Stored)
                {
                    calculated++;
                    Assert.True(field.IsStorageTypeKnown);
                    Assert.NotEmpty(field.RawBytes.ToArray());
                    if (field.Column.Name == "AllNames")
                    {
                        Assert.Equal(ColumnType.TextType, field.Column.Type);
                        Assert.Equal(ColumnType.MemoType, field.Column.CalculatedResultType);
                        Assert.True(field.IsLongValue);
                    }
                }

                return ValueTask.CompletedTask;
            },
            (_, field, record, _) =>
            {
                if (field.Column.Name == "AllNames")
                {
                    memoTerminals += record.Kind == "lval-terminal" ? 1 : 0;
                    memoExternalRows += record.Kind == "lval-row" ? 1 : 0;
                }

                return ValueTask.CompletedTask;
            },
            (record, _) =>
            {
                if (record.BookmarkTargetRecovered)
                {
                    recoveredTargets++;
                    Assert.False(record.BookmarkTargetValidated);
                }

                if (record.BookmarkTargetsRecovered)
                {
                    recoveredTerminals++;
                    Assert.False(record.BookmarkTargetsValidated);
                    Assert.Contains("same-owner proof is withheld", record.Coverage, StringComparison.Ordinal);
                }

                return ValueTask.CompletedTask;
            },
            Ct,
            (record, _) =>
            {
                ownershipCompleted |= record.Kind == "ownership-terminal";
                return ValueTask.CompletedTask;
            });
        Assert.Equal(recoverCatalog, recoveredTargets > 0);
        Assert.Equal(recoverCatalog, recoveredTerminals > 0);
        Assert.True(calculated > 4);
        Assert.Equal(4, memoTerminals);
        Assert.Equal(3, memoExternalRows);
        Assert.True(ownershipCompleted);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
        Assert.Equal(original, SHA256.HashData(bytes));
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static async Task<byte[]> CreateAsync(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync(
                "Items",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("Memo", typeof(string)) { ColumnTypeOverride = ColumnType.MemoType },
                    new ColumnDefinition("Blob", typeof(byte[])) { ColumnTypeOverride = ColumnType.OleType },
                ],
                Ct);
            await writer.InsertRowAsync("Items", [17, new string('x', 10000), new byte[] { 0xFF, 0x80, 0, 7 }], Ct);
            await writer.InsertRowAsync("Items", [18, null, null], Ct);
        }

        return stream.ToArray();
    }

    private static DatabaseFile Open(byte[] bytes)
        => DatabaseFile.ForReader(new MemoryStream(bytes, writable: false), bytes.AsSpan(0, Math.Min(4096, bytes.Length)).ToArray(), ReadOnlyMemory<char>.Empty, string.Empty, leaveOpen: false);
}
