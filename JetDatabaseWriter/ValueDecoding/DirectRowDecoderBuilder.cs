namespace JetDatabaseWriter.ValueDecoding;

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding.Models;
using static JetDatabaseWriter.Enums.ColumnType;

/// <summary>
/// Builds <see cref="DirectRowDecoder{T}"/> delegates for the
/// <see cref="AccessReader.Rows{T}(string, IProgress{long}?, System.Threading.CancellationToken)"/>
/// fast path. The builder inspects the bound
/// columns and refuses (returns <see langword="null"/>) when any column
/// requires the slow path — calculated columns, Memo/Ole LVAL chains,
/// Complex/Attachment, or any property typed as
/// <see cref="Hyperlink"/>.
/// </summary>
internal static class DirectRowDecoderBuilder
{
    private const BindingFlags InstanceNonPublic = BindingFlags.Instance | BindingFlags.NonPublic;
    private const BindingFlags StaticNonPublic = BindingFlags.Static | BindingFlags.NonPublic;

    private static readonly MethodInfo TryParseRowLayoutMethod =
        GetRequiredMethod(typeof(RowDecodePlan), nameof(RowDecodePlan.TryParseLayoutForDirectDecode), InstanceNonPublic);

    private static readonly MethodInfo ResolveColumnSliceMethod =
        GetRequiredMethod(typeof(RowDecodePlan), nameof(RowDecodePlan.ResolveColumnSliceForDirectDecode), StaticNonPublic);

    private static readonly MethodInfo DecodeTextMethod =
        GetRequiredMethod(typeof(AccessReader), nameof(AccessReader.DecodeTextSliceForDirectDecode), InstanceNonPublic);

    private static readonly MethodInfo ReadBinarySliceMethod =
        GetRequiredMethod(typeof(BinaryBuffer), nameof(BinaryBuffer.CopySlice), StaticNonPublic);

    private static readonly MethodInfo ReadDateTimeExtendedMethod =
        GetRequiredMethod(typeof(JetTypeInfo), nameof(JetTypeInfo.ReadDateTimeExtendedAt), StaticNonPublic);

    private static readonly PropertyInfo ColumnSliceKindProperty =
        GetRequiredProperty(typeof(ColumnSlice), nameof(ColumnSlice.Kind));

    private static readonly PropertyInfo ColumnSliceDataStartProperty =
        GetRequiredProperty(typeof(ColumnSlice), nameof(ColumnSlice.DataStart));

    private static readonly PropertyInfo ColumnSliceDataLenProperty =
        GetRequiredProperty(typeof(ColumnSlice), nameof(ColumnSlice.DataLen));

    private static readonly PropertyInfo ColumnSliceBoolValueProperty =
        GetRequiredProperty(typeof(ColumnSlice), nameof(ColumnSlice.BoolValue));

    /// <summary>
    /// Builds a direct decoder for <typeparamref name="T"/> bound against
    /// <paramref name="headers"/>/<paramref name="columns"/>, or returns
    /// <see langword="null"/> when any bound column requires the slow path.
    /// </summary>
    /// <typeparam name="T">The target row type for the generated direct decoder.</typeparam>
    /// <param name="headers">The headers.</param>
    /// <param name="columns">The columns.</param>
    /// <param name="clrTypes">The clr types.</param>
    public static DirectRowDecoder<T>? TryBuild<T>(
        IReadOnlyList<string> headers,
        IReadOnlyList<ColumnInfo> columns,
        IReadOnlyList<Type> clrTypes)
        where T : class, new()
    {
        Guard.NotNull(headers, nameof(headers));
        Guard.NotNull(columns, nameof(columns));
        Guard.NotNull(clrTypes, nameof(clrTypes));

        int columnCount = headers.Count;
        if (columns.Count < columnCount || clrTypes.Count < columnCount)
        {
            return null;
        }

        var bound = new List<(int Index, RowMapper<T>.Accessor Accessor, ColumnInfo Col)>();
        for (int i = 0; i < columnCount; i++)
        {
            RowMapper<T>.Accessor? acc = RowMapper<T>.TryGetAccessor(headers[i]);
            if (acc == null)
            {
                continue;
            }

            // Reject hyperlink-typed targets — those need the post-processing
            // pass that runs only on the object?[] path.
            if (acc.TargetType == typeof(Hyperlink) || clrTypes[i] == typeof(Hyperlink))
            {
                return null;
            }

            ColumnInfo col = columns[i];
            if (col.IsCalculated)
            {
                return null;
            }

            if (!IsDirectlyDecodable(col.Type, acc.TargetType))
            {
                return null;
            }

            bound.Add((i, acc, col));
        }

        if (bound.Count == 0)
        {
            // Nothing bound — let the caller fall through to the slow
            // path (which is already a no-op for unbound rows).
            return null;
        }

        return Emit(bound);
    }

    private static DirectRowDecoder<T> Emit<T>(
        List<(int Index, RowMapper<T>.Accessor Accessor, ColumnInfo Col)> bound)
        where T : class, new()
    {
        ParameterExpression readerParam = Expression.Parameter(typeof(AccessReader), "reader");
        ParameterExpression decodePlanParam = Expression.Parameter(typeof(RowDecodePlan), "decodePlan");
        ParameterExpression pageParam = Expression.Parameter(typeof(byte[]), "page");
        ParameterExpression rowStartParam = Expression.Parameter(typeof(int), "rowStart");
        ParameterExpression rowSizeParam = Expression.Parameter(typeof(int), "rowSize");
        ParameterExpression targetParam = Expression.Parameter(typeof(T), "target");

        ParameterExpression layoutLocal = Expression.Variable(typeof(RowLayout), "layout");
        ParameterExpression sliceLocal = Expression.Variable(typeof(ColumnSlice), "slice");
        LabelTarget returnLabel = Expression.Label(typeof(bool), "ret");

        var statements = new List<Expression>(8 + (bound.Count * 3))
        {
            // Emit the row-layout preflight; malformed rows return false so
            // the caller can skip them without constructing an object?[] row.
            Expression.IfThen(
            Expression.Not(Expression.Call(
                decodePlanParam,
                TryParseRowLayoutMethod,
                readerParam,
                pageParam,
                rowStartParam,
                rowSizeParam,
                layoutLocal)),
            Expression.Return(returnLabel, Expression.Constant(false))),
        };

        foreach ((int Index, RowMapper<T>.Accessor Accessor, ColumnInfo Col) entry in bound)
        {
            ColumnInfo col = entry.Col;
            ConstantExpression colExpr = Expression.Constant(col, typeof(ColumnInfo));

            // Emit one plan-owned slice lookup per bound column; the kind
            // gate below leaves null/empty/malformed slices at defaults.
            statements.Add(Expression.Assign(
                sliceLocal,
                Expression.Call(
                    ResolveColumnSliceMethod,
                    readerParam,
                    pageParam,
                    rowStartParam,
                    rowSizeParam,
                    layoutLocal,
                    colExpr)));

            MemberExpression kindExpr = Expression.Property(sliceLocal, ColumnSliceKindProperty);
            MemberExpression dataStartExpr = Expression.Property(sliceLocal, ColumnSliceDataStartProperty);
            MemberExpression dataLenExpr = Expression.Property(sliceLocal, ColumnSliceDataLenProperty);
            MemberExpression boolValueExpr = Expression.Property(sliceLocal, ColumnSliceBoolValueProperty);

            // Compute the absolute offset once (rowStart + slice.DataStart).
            BinaryExpression offsetExpr = Expression.Add(rowStartParam, dataStartExpr);

            Expression readExpr = BuildReadExpression(
                col,
                pageParam,
                offsetExpr,
                dataLenExpr,
                boolValueExpr,
                readerParam);

            // target.Prop = (PropType)readExpr;
            // Compose the raw read — which yields the column's natural CLR type —
            // up to the property type in two steps so a lossless widening
            // (e.g. short→long) and a nullable lift (e.g. long→long?) combine
            // cleanly: first widen to the nullable-unwrapped target type, then
            // lift to the declared property type when it differs (Nullable<T>).
            // Exact-match columns skip both Convert nodes.
            Type propertyType = entry.Accessor.Property.PropertyType;
            Type targetUnderlying = entry.Accessor.TargetType;

            Expression widened = readExpr.Type == targetUnderlying
                ? readExpr
                : Expression.Convert(readExpr, targetUnderlying);

            Expression assignValue = propertyType == targetUnderlying
                ? widened
                : Expression.Convert(widened, propertyType);

            // Wrap with try/catch to swallow ArgumentException / OverflowException /
            // IndexOutOfRangeException — matches ReadFixedTyped's safety contract
            // (bad row → DBNull → mapper-skip → property keeps default).
            Expression assign = Expression.Assign(
                Expression.Property(targetParam, entry.Accessor.Property),
                assignValue);

            Expression safeAssign = Expression.TryCatch(
                Expression.Block(typeof(void), assign),
                Expression.Catch(typeof(ArgumentException), Expression.Empty()),
                Expression.Catch(typeof(OverflowException), Expression.Empty()),
                Expression.Catch(typeof(IndexOutOfRangeException), Expression.Empty()));

            // Gate by slice kind / size sanity to mimic the per-kind switch in
            // TryCrackRowSync. Empty/Null leave the property at its default.
            Expression kindGate = BuildKindGate(col.Type, kindExpr, dataLenExpr);

            statements.Add(Expression.IfThen(kindGate, safeAssign));
        }

        statements.Add(Expression.Return(returnLabel, Expression.Constant(true)));
        statements.Add(Expression.Label(returnLabel, Expression.Constant(false)));

        BlockExpression body = Expression.Block(
            typeof(bool),
            [layoutLocal, sliceLocal],
            statements);

        return Expression.Lambda<DirectRowDecoder<T>>(
            body,
            readerParam,
            decodePlanParam,
            pageParam,
            rowStartParam,
            rowSizeParam,
            targetParam).Compile();
    }

    private static BinaryExpression BuildKindGate(
        ColumnType colType,
        Expression kindExpr,
        Expression dataLenExpr)
    {
        if (colType == BooleanType)
        {
            return Expression.Equal(kindExpr, Expression.Constant(ColumnSliceKind.Bool));
        }

        if (colType is TextType or BinaryType)
        {
            return Expression.Equal(kindExpr, Expression.Constant(ColumnSliceKind.Var));
        }

        int expectedSize = JetTypeInfo.GetFixedSize(colType);
        BinaryExpression fixedOrVariableKind = Expression.OrElse(
            Expression.Equal(kindExpr, Expression.Constant(ColumnSliceKind.Fixed)),
            Expression.Equal(kindExpr, Expression.Constant(ColumnSliceKind.Var)));

        return Expression.AndAlso(
            fixedOrVariableKind,
            Expression.GreaterThanOrEqual(dataLenExpr, Expression.Constant(expectedSize)));
    }

    private static Expression BuildReadExpression(
        ColumnInfo column,
        ParameterExpression pageParam,
        Expression offsetExpr,
        Expression dataLenExpr,
        Expression boolValueExpr,
        ParameterExpression readerParam) => column.Type switch
        {
            BooleanType => boolValueExpr,
            ByteType => Expression.Call(GetRequiredMethod(typeof(JetTypeInfo), nameof(JetTypeInfo.ReadByteAt), StaticNonPublic), pageParam, offsetExpr),
            IntegerType => Expression.Call(GetRequiredMethod(typeof(JetTypeInfo), nameof(JetTypeInfo.ReadInt16LE), StaticNonPublic), pageParam, offsetExpr),
            LongIntegerType => Expression.Call(GetRequiredMethod(typeof(JetTypeInfo), nameof(JetTypeInfo.ReadInt32LE), StaticNonPublic), pageParam, offsetExpr),
            BigIntType => Expression.Call(GetRequiredMethod(typeof(JetTypeInfo), nameof(JetTypeInfo.ReadInt64LE), StaticNonPublic), pageParam, offsetExpr),
            MoneyType => Expression.Call(GetRequiredMethod(typeof(JetTypeInfo), nameof(JetTypeInfo.ReadMoneyLE), StaticNonPublic), pageParam, offsetExpr),
            FloatType => Expression.Call(GetRequiredMethod(typeof(JetTypeInfo), nameof(JetTypeInfo.ReadFloatLE), StaticNonPublic), pageParam, offsetExpr),
            DoubleType => Expression.Call(GetRequiredMethod(typeof(JetTypeInfo), nameof(JetTypeInfo.ReadDoubleLE), StaticNonPublic), pageParam, offsetExpr),
            DateTimeType => Expression.Call(GetRequiredMethod(typeof(JetTypeInfo), nameof(JetTypeInfo.ReadDateTimeLE), StaticNonPublic), pageParam, offsetExpr),
            GuidType => Expression.Call(GetRequiredMethod(typeof(JetTypeInfo), nameof(JetTypeInfo.ReadGuidAt), StaticNonPublic), pageParam, offsetExpr),
            NumericType => Expression.Call(GetRequiredMethod(typeof(JetTypeInfo), nameof(JetTypeInfo.ReadDecimalLE), StaticNonPublic), pageParam, offsetExpr, Expression.Constant((int)column.NumericScale)),
            TextType => Expression.Call(readerParam, DecodeTextMethod, pageParam, offsetExpr, dataLenExpr),
            BinaryType => Expression.Call(ReadBinarySliceMethod, pageParam, offsetExpr, dataLenExpr),
            DateTimeExtendedType => Expression.Call(ReadDateTimeExtendedMethod, pageParam, offsetExpr),
            OleType or
            MemoType or
            AttachmentType or
            ComplexType or
            _ => throw new InvalidOperationException($"BuildReadExpression invoked for unsupported type {JetTypeInfo.GetTypeDisplayName(column.Type)}."),
        };

    private static MethodInfo GetRequiredMethod(Type declaringType, string name, BindingFlags bindingAttr) =>
        declaringType.GetMethod(name, bindingAttr) ?? throw new MissingMethodException(declaringType.FullName ?? declaringType.Name, name);

    private static PropertyInfo GetRequiredProperty(Type declaringType, string name) =>
        declaringType.GetProperty(name) ?? throw new MissingMemberException(declaringType.FullName ?? declaringType.Name, name);

    private static bool IsDirectlyDecodable(ColumnType colType, Type targetUnderlying)
    {
        if (colType is OleType or MemoType or AttachmentType or ComplexType)
        {
            return false;
        }

        Type? naturalType = JetTypeInfo.GetClrType(colType);
        if (naturalType is null)
        {
            return false;
        }

        return naturalType == targetUnderlying || IsLosslessWidening(naturalType, targetUnderlying);
    }

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="target"/> is a
    /// lossless numeric widening of <paramref name="source"/> — a conversion
    /// that can never drop range or precision, so it is safe to emit on the
    /// zero-box direct path. The precision-losing implicit conversions C#
    /// otherwise permits (<c>int→float</c>, <c>long→float</c>,
    /// <c>long→double</c>) are deliberately excluded so the direct decoder
    /// never produces a value the boxing fallback would not.
    /// </summary>
    /// <param name="source">The column's natural CLR type.</param>
    /// <param name="target">The nullable-unwrapped target property type.</param>
    private static bool IsLosslessWidening(Type source, Type target)
    {
        if (source == typeof(byte))
        {
            return target == typeof(short)
                || target == typeof(int)
                || target == typeof(long)
                || target == typeof(float)
                || target == typeof(double)
                || target == typeof(decimal);
        }

        if (source == typeof(short))
        {
            return target == typeof(int)
                || target == typeof(long)
                || target == typeof(float)
                || target == typeof(double)
                || target == typeof(decimal);
        }

        if (source == typeof(int))
        {
            return target == typeof(long)
                || target == typeof(double)
                || target == typeof(decimal);
        }

        if (source == typeof(long))
        {
            return target == typeof(decimal);
        }

        if (source == typeof(float))
        {
            return target == typeof(double);
        }

        return false;
    }
}

/// <summary>
/// Compiled per-<typeparamref name="T"/> delegate that decodes a single row
/// straight off the page bytes into <paramref name="target"/>'s properties,
/// bypassing the per-row <c>object?[]</c> buffer and the box/unbox round-trip
/// that the projection-aware path still pays.
/// </summary>
/// <typeparam name="T">The target row type decoded into by the delegate.</typeparam>
/// <param name="reader">The reader.</param>
/// <param name="decodePlan">The decode plan.</param>
/// <param name="page">The page bytes.</param>
/// <param name="rowStart">The row start.</param>
/// <param name="rowSize">The row size.</param>
/// <param name="target">Target object whose mapped properties are populated.</param>
/// <returns>
/// <see langword="true"/> when the row was decoded; <see langword="false"/>
/// when the row should be skipped (empty / malformed trailer).
/// </returns>
internal delegate bool DirectRowDecoder<T>(
    AccessReader reader,
    RowDecodePlan decodePlan,
    byte[] page,
    int rowStart,
    int rowSize,
    T target);
