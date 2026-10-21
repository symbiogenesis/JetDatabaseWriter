namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueEncoding;
using JetDatabaseWriter.ValueEncoding.Models;
using Xunit;

public sealed class CompactCalculatedColumnHydratorTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Hydrate_MatchesCompleteCatalogIdBeforeFollowingPropertiesAndPreservesDescriptors()
    {
        var fixture = new HydrationFixture();
        fixture.AddRow(unchecked((int)0x80000007), [1, 2, 3]);
        fixture.AddRow(7, fixture.Payload);
        fixture.AddRow(8, [1, 2, 3]);
        var accounting = new CompactDiscoveryAccounting(new());
        using (CompactCalculatedColumnHydrator hydrator = fixture.Create(new(), accounting))
        {
            await hydrator.WithColumnsAsync(
                7,
                fixture.Table,
                (columns, _) =>
                {
                    ColumnInfo column = Assert.Single(Assert.IsType<IReadOnlyList<ColumnInfo>>(columns, exactMatch: false));
                    Assert.Equal(ColumnType.TextType, column.Type);
                    Assert.Equal(ColumnType.MemoType, column.CalculatedResultType);
                    Assert.Equal(fixture.Table.Columns[0].RawDescriptor, column.RawDescriptor);
                    Assert.Equal(default, fixture.Table.Columns[0].CalculatedResultType);
                    Assert.True(accounting.ActiveWorkingBytes > 0);
                    return ValueTask.CompletedTask;
                },
                Ct);
        }

        Assert.Equal(1, fixture.Audits);
        Assert.Equal(fixture.Payload.Length, accounting.ReadCount(CompactDiscoveryCharge.SchemaBytes));
        Assert.Equal(fixture.Payload.Length, accounting.ReadCount(CompactDiscoveryCharge.LongValuePayloadBytes));
        Assert.Equal(0, accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task Hydrate_CumulativePropertyAllowancePersistsAcrossScansAndFailures()
    {
        var fixture = new HydrationFixture();
        fixture.AddRow(7, fixture.Payload);
        var budget = new CompactDiscoveryBudget { MaxSchemaBytes = fixture.Payload.Length };
        var accounting = new CompactDiscoveryAccounting(budget);
        using (CompactCalculatedColumnHydrator hydrator = fixture.Create(budget, accounting))
        {
            await hydrator.WithColumnsAsync(7, fixture.Table, (_, _) => ValueTask.CompletedTask, Ct);
            for (int attempt = 0; attempt < 2; attempt++)
            {
                await Assert.ThrowsAsync<JetLimitationException>(async () => await hydrator.WithColumnsAsync(7, fixture.Table, (_, _) => ValueTask.CompletedTask, Ct));
            }
        }

        Assert.Equal(1, fixture.Audits);
        Assert.Equal(fixture.Payload.Length, accounting.ReadCount(CompactDiscoveryCharge.SchemaBytes));
        Assert.Equal(0, accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task Hydrate_RefusesOversizedPayloadBeforeFollowingOrAllocatingIt()
    {
        var fixture = new HydrationFixture();
        fixture.AddRow(7, fixture.Payload);
        var budget = new CompactDiscoveryBudget { MaxValueBytes = fixture.Payload.Length - 1 };
        var accounting = new CompactDiscoveryAccounting(budget);
        using (CompactCalculatedColumnHydrator hydrator = fixture.Create(budget, accounting))
        {
            await Assert.ThrowsAsync<JetLimitationException>(async () => await hydrator.WithColumnsAsync(7, fixture.Table, (_, _) => ValueTask.CompletedTask, Ct));
        }

        Assert.Equal(0, fixture.Audits);
        Assert.Equal(0, accounting.ReadCount(CompactDiscoveryCharge.SchemaBytes));
        Assert.Equal(0, accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task Hydrate_MalformedMatchingPropertiesRefuseAndReleaseStorage()
    {
        var fixture = new HydrationFixture();
        fixture.AddRow(7, [1, 2, 3]);
        var accounting = new CompactDiscoveryAccounting(new());
        using (CompactCalculatedColumnHydrator hydrator = fixture.Create(new(), accounting))
        {
            await Assert.ThrowsAsync<JetCorruptDataException>(async () => await hydrator.WithColumnsAsync(7, fixture.Table, (_, _) => ValueTask.CompletedTask, Ct));
        }

        Assert.Equal(1, fixture.Audits);
        Assert.Equal(0, accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task Hydrate_MissingPropertiesLeaveTheStorageTypeUnknown()
    {
        var fixture = new HydrationFixture();
        fixture.AddRow(7, null);
        var accounting = new CompactDiscoveryAccounting(new());
        using (CompactCalculatedColumnHydrator hydrator = fixture.Create(new(), accounting))
        {
            await hydrator.WithColumnsAsync(
                7,
                fixture.Table,
                (columns, _) =>
                {
                    ColumnInfo column = Assert.Single(Assert.IsType<IReadOnlyList<ColumnInfo>>(columns, exactMatch: false));
                    Assert.False(new CompactRawField(column, default, CompactRawFieldKind.Stored, 0, default, false).IsStorageTypeKnown);
                    return ValueTask.CompletedTask;
                },
                Ct);
        }

        Assert.Equal(0, fixture.Audits);
        Assert.Equal(0, accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Hydrate_UnknownAndOutOfRangeResultTypesRemainUnknown(bool overflowing)
    {
        var fixture = new HydrationFixture(invalidResultType: true, overflowingResultType: overflowing);
        fixture.AddRow(7, fixture.Payload);
        var accounting = new CompactDiscoveryAccounting(new());
        using (CompactCalculatedColumnHydrator hydrator = fixture.Create(new(), accounting))
        {
            await hydrator.WithColumnsAsync(
                7,
                fixture.Table,
                (columns, _) =>
                {
                    ColumnInfo column = Assert.Single(Assert.IsType<IReadOnlyList<ColumnInfo>>(columns, exactMatch: false));
                    Assert.Equal(default, column.CalculatedResultType);
                    return ValueTask.CompletedTask;
                },
                Ct);
        }

        Assert.Equal(1, fixture.Audits);
        Assert.Equal(0, accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(1)]
    public async Task Hydrate_IncompleteOrOverlongPayloadRefusesWithinAdmittedStorage(int adjustment)
    {
        var fixture = new HydrationFixture { PayloadByteAdjustment = adjustment };
        fixture.AddRow(7, fixture.Payload);
        var accounting = new CompactDiscoveryAccounting(new());
        using (CompactCalculatedColumnHydrator hydrator = fixture.Create(new(), accounting))
        {
            await Assert.ThrowsAsync<JetCorruptDataException>(async () => await hydrator.WithColumnsAsync(7, fixture.Table, (_, _) => ValueTask.CompletedTask, Ct));
        }

        Assert.Equal(1, fixture.Audits);
        Assert.Equal(0, accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task Hydrate_AdmitsCatalogColumnsAndPropertyParserSimultaneouslyBeforeAllocation()
    {
        var fixture = new HydrationFixture();
        fixture.AddRow(7, fixture.Payload);
        var budget = new CompactDiscoveryBudget { MaxWorkingBytes = 150_000 };
        var accounting = new CompactDiscoveryAccounting(budget);
        using (CompactCalculatedColumnHydrator hydrator = fixture.Create(budget, accounting))
        {
            await Assert.ThrowsAsync<JetLimitationException>(async () => await hydrator.WithColumnsAsync(7, fixture.Table, (_, _) => ValueTask.CompletedTask, Ct));
        }

        Assert.Equal(0, fixture.Audits);
        Assert.Equal(fixture.Payload.Length, accounting.ReadCount(CompactDiscoveryCharge.SchemaBytes));
        Assert.Equal(0, accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Hydrate_RepeatedResultTypeDeclarationsRequireEquivalentSupportedTypes(bool repeatedTarget, bool equivalent)
    {
        var fixture = new HydrationFixture(customize: builder =>
        {
            ColumnPropertyTargetBuilder target = builder.Targets[0];
            target.Entries[0].Value = [(byte)(equivalent ? ColumnType.MemoType : ColumnType.TextType)];
            if (repeatedTarget)
            {
                target = new ColumnPropertyTargetBuilder { Name = "cAcHeD", ChunkType = target.ChunkType };
                builder.Targets.Add(target);
            }

            target.Entries.Add(new ColumnPropertyEntryBuilder { Name = "rEsUlTtYpE", DataType = ColumnType.IntegerType, Value = [(byte)ColumnType.MemoType, 0] });
        });
        fixture.AddRow(7, fixture.Payload);
        var accounting = new CompactDiscoveryAccounting(new());
        using (CompactCalculatedColumnHydrator hydrator = fixture.Create(new(), accounting))
        {
            await hydrator.WithColumnsAsync(
                7,
                fixture.Table,
                (columns, _) =>
                {
                    ColumnInfo column = Assert.Single(Assert.IsType<IReadOnlyList<ColumnInfo>>(columns, exactMatch: false));
                    Assert.Equal(equivalent ? ColumnType.MemoType : default, column.CalculatedResultType);
                    Assert.Equal(equivalent, new CompactRawField(column, default, CompactRawFieldKind.Stored, 0, default, false).IsStorageTypeKnown);
                    return ValueTask.CompletedTask;
                },
                Ct);
        }

        Assert.Equal(1, fixture.Audits);
        Assert.Equal(0, accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Hydrate_MalformedOrUnknownRepeatedResultTypesKeepStorageUnknown(bool repeatedTarget, bool unknownType)
    {
        var fixture = new HydrationFixture(customize: builder =>
        {
            ColumnPropertyTargetBuilder target = builder.Targets[0];
            if (repeatedTarget)
            {
                target = new ColumnPropertyTargetBuilder { Name = "cAcHeD", ChunkType = target.ChunkType };
                builder.Targets.Add(target);
            }

            target.Entries.Add(new ColumnPropertyEntryBuilder { Name = "rEsUlTtYpE", DataType = ColumnType.ByteType, Value = unknownType ? [255] : [(byte)ColumnType.MemoType, 0] });
        });
        fixture.AddRow(7, fixture.Payload);
        var accounting = new CompactDiscoveryAccounting(new());
        using (CompactCalculatedColumnHydrator hydrator = fixture.Create(new(), accounting))
        {
            await hydrator.WithColumnsAsync(
                7,
                fixture.Table,
                (columns, _) =>
                {
                    ColumnInfo column = Assert.Single(Assert.IsType<IReadOnlyList<ColumnInfo>>(columns, exactMatch: false));
                    Assert.Equal(default, column.CalculatedResultType);
                    Assert.False(new CompactRawField(column, default, CompactRawFieldKind.Stored, 0, default, false).IsStorageTypeKnown);
                    return ValueTask.CompletedTask;
                },
                Ct);
        }

        Assert.Equal(1, fixture.Audits);
        Assert.Equal(0, accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(2)]
    public async Task Hydrate_ObjectOrIndexMapCannotEstablishColumnStorage(int subtype)
    {
        var fixture = new HydrationFixture(customize: builder => builder.Targets[0].ChunkType = (ColumnPropertyChunkType)subtype);
        fixture.AddRow(7, fixture.Payload);
        var accounting = new CompactDiscoveryAccounting(new());
        using (CompactCalculatedColumnHydrator hydrator = fixture.Create(new(), accounting))
        {
            await hydrator.WithColumnsAsync(
                7,
                fixture.Table,
                (columns, _) =>
                {
                    ColumnInfo column = Assert.Single(Assert.IsType<IReadOnlyList<ColumnInfo>>(columns, exactMatch: false));
                    Assert.Equal(default, column.CalculatedResultType);
                    Assert.False(new CompactRawField(column, default, CompactRawFieldKind.Stored, 0, default, false).IsStorageTypeKnown);
                    return ValueTask.CompletedTask;
                },
                Ct);
        }

        Assert.Equal(0, accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData(0, false)]
    [InlineData(0, true)]
    [InlineData(2, false)]
    [InlineData(2, true)]
    public async Task Hydrate_SameNameObjectOrIndexMapCannotOverrideColumnResultType(int subtype, bool first)
    {
        var fixture = new HydrationFixture(customize: builder =>
        {
            var target = new ColumnPropertyTargetBuilder { Name = "cAcHeD", ChunkType = (ColumnPropertyChunkType)subtype };
            target.AddByte(Constants.ColumnPropertyNames.ResultType, (byte)ColumnType.TextType);
            builder.Targets.Insert(first ? 0 : builder.Targets.Count, target);
        });
        fixture.AddRow(7, fixture.Payload);
        var accounting = new CompactDiscoveryAccounting(new());
        using (CompactCalculatedColumnHydrator hydrator = fixture.Create(new(), accounting))
        {
            await hydrator.WithColumnsAsync(
                7,
                fixture.Table,
                (columns, _) =>
                {
                    ColumnInfo column = Assert.Single(Assert.IsType<IReadOnlyList<ColumnInfo>>(columns, exactMatch: false));
                    Assert.Equal(ColumnType.MemoType, column.CalculatedResultType);
                    Assert.True(new CompactRawField(column, default, CompactRawFieldKind.Stored, 0, default, false).IsStorageTypeKnown);
                    return ValueTask.CompletedTask;
                },
                Ct);
        }

        Assert.Equal(0, accounting.ActiveWorkingBytes);
    }

    private sealed class HydrationFixture
    {
        private readonly JetFormat format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        private readonly List<byte[]> rows = [];
        private readonly TDefImage catalog = Image(
            new ColumnInfo { Name = "Id", Type = ColumnType.LongIntegerType, ColNum = 0, Flags = Constants.ColumnDescriptorFlags.Fixed },
            new ColumnInfo { Name = "LvProp", Type = ColumnType.OleType, ColNum = 1, VarIdx = 0 });

        internal HydrationFixture(bool invalidResultType = false, bool overflowingResultType = false, Action<ColumnPropertyBlockBuilder>? customize = null)
        {
            var builder = new ColumnPropertyBlockBuilder();
            var target = new ColumnPropertyTargetBuilder { Name = "Cached", ChunkType = ColumnPropertyChunkType.PropertyBlockAlt1 };
            if (invalidResultType)
            {
                target.Entries.Add(new ColumnPropertyEntryBuilder
                {
                    Name = Constants.ColumnPropertyNames.ResultType,
                    DataType = ColumnType.IntegerType,
                    Value = overflowingResultType ? [(byte)ColumnType.MemoType, 1] : [255, 0],
                });
            }
            else
            {
                target.AddByte(Constants.ColumnPropertyNames.ResultType, (byte)ColumnType.MemoType);
            }

            target.AddText(Constants.ColumnPropertyNames.Expression, "UntrustedFunction()", this.format);
            builder.Targets.Add(target);
            customize?.Invoke(builder);
            this.Payload = Assert.IsType<byte[]>(builder.ToBytes(this.format));
        }

        internal byte[] Payload { get; }

        internal TDefImage Table { get; } = Image(new ColumnInfo([0x0A, 0x80]) { Name = "Cached", Type = ColumnType.TextType, ColNum = 0, VarIdx = 0, ExtraFlags = Constants.CalculatedColumn.ExtFlagMask });

        internal int Audits { get; private set; }

        internal int PayloadByteAdjustment { get; set; }

        internal void AddRow(int id, byte[]? payload)
            => this.rows.Add(new RowEncoder(this.format).SerializeRow(this.catalog.Definition, [id, payload is null ? DBNull.Value : new PreEncodedLongValue(Assert.IsType<byte[]>(LongValueStore.WrapInlineLongValue(payload)))]));

        internal CompactCalculatedColumnHydrator Create(CompactDiscoveryBudget budget, CompactDiscoveryAccounting accounting)
        {
            var hydrator = new CompactCalculatedColumnHydrator(this.format, budget, accounting, this.ScanAsync, this.AuditAsync);
            hydrator.SetCatalog(this.catalog);
            return hydrator;
        }

        private static TDefImage Image(params ColumnInfo[] columns)
            => new([], new(columns.Length, 0, 0, default), [.. columns], false, TDefParseIssues.None, null, [], []);

        private async ValueTask ScanAsync(long owner, Func<RowLocation, ReadOnlyMemory<byte>, CancellationToken, ValueTask> visitor, CancellationToken token)
        {
            Assert.Equal(2, owner);
            foreach (byte[] row in this.rows)
            {
                await visitor(new(9, 0, 0, row.Length), row, token);
            }
        }

        private ValueTask AuditAsync(ReadOnlyMemory<byte> column, Func<CompactLongValueRecord, CancellationToken, ValueTask> visitor, CancellationToken token)
        {
            this.Audits++;
            Assert.True(LongValueDescriptor.TryRead(column.Span, out LongValueDescriptor descriptor));
            return visitor(new CompactLongValueRecord { Kind = "lval-column", RawBytes = column, PayloadOffset = Constants.LongValue.HeaderSize, DeclaredPayloadBytes = descriptor.Length + this.PayloadByteAdjustment }, token);
        }
    }
}
