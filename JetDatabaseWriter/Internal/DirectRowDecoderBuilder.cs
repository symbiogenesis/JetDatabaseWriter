namespace JetDatabaseWriter.Internal;

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Internal.Helpers;
using JetDatabaseWriter.Internal.Models;
using JetDatabaseWriter.Models;
using static JetDatabaseWriter.Constants.ColumnTypes;

/// <summary>
/// Builds <see cref="DirectRowDecoder{T}"/> delegates for the
/// <see cref="AccessReader.Rows{T}(string, IProgress{long}?, System.Threading.CancellationToken)"/>
/// fast path. The builder inspects the bound
/// columns and refuses (returns <see langword="null"/>) when any column
/// requires the slow path — T_MEMO/T_OLE LVAL chains, T_BINARY, T_NUMERIC,
/// T_COMPLEX/T_ATTACHMENT, or any property typed as
/// <see cref="JetDatabaseWriter.Models.Hyperlink"/>.
/// </summary>
internal static class DirectRowDecoderBuilder
{
    private static readonly MethodInfo TryParseRowLayoutMethod =
        typeof(AccessReader).GetMethod(
            nameof(AccessReader.TryParseRowLayoutForDirectDecode),
            BindingFlags.Instance | BindingFlags.NonPublic)!;

    private static readonly MethodInfo ResolveColumnSliceMethod =
        typeof(AccessReader).GetMethod(
            nameof(AccessReader.ResolveColumnSliceForDirectDecode),
            BindingFlags.Instance | BindingFlags.NonPublic)!;

    private static readonly MethodInfo ReadRawNumColsMethod =
        typeof(AccessReader).GetMethod(
            nameof(AccessReader.ReadRawNumCols),
            BindingFlags.Instance | BindingFlags.NonPublic)!;

    private static readonly MethodInfo DecodeTextMethod =
        typeof(AccessReader).GetMethod(
            nameof(AccessReader.DecodeTextSliceForDirectDecode),
            BindingFlags.Instance | BindingFlags.NonPublic)!;

    private static readonly PropertyInfo NumColsFieldSizeProp =
        typeof(AccessReader).GetProperty(
            nameof(AccessReader.NumColsFieldSize),
            BindingFlags.Instance | BindingFlags.NonPublic)!;

    /// <summary>
    /// Builds a direct decoder for <typeparamref name="T"/> bound against
    /// <paramref name="headers"/>/<paramref name="columns"/>, or returns
    /// <see langword="null"/> when any bound column requires the slow path.
    /// </summary>
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

        return Emit<T>(bound);
    }

    private static DirectRowDecoder<T> Emit<T>(
        List<(int Index, RowMapper<T>.Accessor Accessor, ColumnInfo Col)> bound)
        where T : class, new()
    {
        var readerParam = Expression.Parameter(typeof(AccessReader), "reader");
        var pageParam = Expression.Parameter(typeof(byte[]), "page");
        var rowStartParam = Expression.Parameter(typeof(int), "rowStart");
        var rowSizeParam = Expression.Parameter(typeof(int), "rowSize");
        var hasVarParam = Expression.Parameter(typeof(bool), "hasVarColumns");
        var targetParam = Expression.Parameter(typeof(T), "target");

        var layoutLocal = Expression.Variable(typeof(AccessBase.RowLayout), "layout");
        var sliceLocal = Expression.Variable(typeof(AccessBase.ColumnSlice), "slice");
        var returnLabel = Expression.Label(typeof(bool), "ret");

        var statements = new List<Expression>(8 + (bound.Count * 3))
        {
            // if (rowSize < reader.NumColsFieldSize) return false;
            Expression.IfThen(
            Expression.LessThan(
                rowSizeParam,
                Expression.Property(readerParam, NumColsFieldSizeProp)),
            Expression.Return(returnLabel, Expression.Constant(false))),

            // if (reader.ReadRawNumCols(page, rowStart) == 0) return false;
            Expression.IfThen(
            Expression.Equal(
                Expression.Call(readerParam, ReadRawNumColsMethod, pageParam, rowStartParam),
                Expression.Constant(0)),
            Expression.Return(returnLabel, Expression.Constant(false))),

            // if (!reader.TryParseRowLayoutForDirectDecode(page, rowStart, rowSize, hasVarColumns, out layout))
            //     return false;
            Expression.IfThen(
            Expression.Not(Expression.Call(
                readerParam,
                TryParseRowLayoutMethod,
                pageParam,
                rowStartParam,
                rowSizeParam,
                hasVarParam,
                layoutLocal)),
            Expression.Return(returnLabel, Expression.Constant(false))),
        };

        var kindProp = typeof(AccessBase.ColumnSlice).GetProperty(nameof(AccessBase.ColumnSlice.Kind))!;
        var dataStartProp = typeof(AccessBase.ColumnSlice).GetProperty(nameof(AccessBase.ColumnSlice.DataStart))!;
        var dataLenProp = typeof(AccessBase.ColumnSlice).GetProperty(nameof(AccessBase.ColumnSlice.DataLen))!;
        var boolValueProp = typeof(AccessBase.ColumnSlice).GetProperty(nameof(AccessBase.ColumnSlice.BoolValue))!;

        foreach (var entry in bound)
        {
            ColumnInfo col = entry.Col;
            var colExpr = Expression.Constant(col, typeof(ColumnInfo));

            // slice = reader.ResolveColumnSliceForDirectDecode(page, rowStart, rowSize, layout, col);
            statements.Add(Expression.Assign(
                sliceLocal,
                Expression.Call(
                    readerParam,
                    ResolveColumnSliceMethod,
                    pageParam,
                    rowStartParam,
                    rowSizeParam,
                    layoutLocal,
                    colExpr)));

            var kindExpr = Expression.Property(sliceLocal, kindProp);
            var dataStartExpr = Expression.Property(sliceLocal, dataStartProp);
            var dataLenExpr = Expression.Property(sliceLocal, dataLenProp);
            var boolValueExpr = Expression.Property(sliceLocal, boolValueProp);

            // Compute the absolute offset once (rowStart + slice.DataStart).
            var offsetExpr = Expression.Add(rowStartParam, dataStartExpr);

            Expression readExpr = BuildReadExpression(
                col.Type,
                pageParam,
                offsetExpr,
                dataLenExpr,
                boolValueExpr,
                readerParam);

            // target.Prop = (PropType)readExpr;
            // Wrap with try/catch to swallow ArgumentException / OverflowException /
            // IndexOutOfRangeException — matches ReadFixedTyped's safety contract
            // (bad row → DBNull → mapper-skip → property keeps default).
            Expression assign = Expression.Assign(
                Expression.Property(targetParam, entry.Accessor.Property),
                Expression.Convert(readExpr, entry.Accessor.Property.PropertyType));

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

        var body = Expression.Block(
            typeof(bool),
            [layoutLocal, sliceLocal],
            statements);

        return Expression.Lambda<DirectRowDecoder<T>>(
            body,
            readerParam,
            pageParam,
            rowStartParam,
            rowSizeParam,
            hasVarParam,
            targetParam).Compile();
    }

    private static BinaryExpression BuildKindGate(
        byte colType,
        Expression kindExpr,
        Expression dataLenExpr)
    {
        if (colType == T_BOOL)
        {
            return Expression.Equal(kindExpr, Expression.Constant(AccessBase.ColumnSliceKind.Bool));
        }

        if (colType == T_TEXT)
        {
            return Expression.Equal(kindExpr, Expression.Constant(AccessBase.ColumnSliceKind.Var));
        }

        int expectedSize = JetTypeInfo.GetFixedSize(colType);
        return Expression.AndAlso(
            Expression.Equal(kindExpr, Expression.Constant(AccessBase.ColumnSliceKind.Fixed)),
            Expression.GreaterThanOrEqual(dataLenExpr, Expression.Constant(expectedSize)));
    }

    private static Expression BuildReadExpression(
        byte colType,
        ParameterExpression pageParam,
        Expression offsetExpr,
        Expression dataLenExpr,
        Expression boolValueExpr,
        ParameterExpression readerParam)
    {
        return colType switch
        {
            T_BOOL => boolValueExpr,
            T_BYTE => Expression.Call(typeof(JetTypeInfo).GetMethod(nameof(JetTypeInfo.ReadByteAt), BindingFlags.Static | BindingFlags.NonPublic)!, pageParam, offsetExpr),
            T_INT => Expression.Call(typeof(JetTypeInfo).GetMethod(nameof(JetTypeInfo.ReadInt16LE), BindingFlags.Static | BindingFlags.NonPublic)!, pageParam, offsetExpr),
            T_LONG => Expression.Call(typeof(JetTypeInfo).GetMethod(nameof(JetTypeInfo.ReadInt32LE), BindingFlags.Static | BindingFlags.NonPublic)!, pageParam, offsetExpr),
            T_MONEY => Expression.Call(typeof(JetTypeInfo).GetMethod(nameof(JetTypeInfo.ReadMoneyLE), BindingFlags.Static | BindingFlags.NonPublic)!, pageParam, offsetExpr),
            T_FLOAT => Expression.Call(typeof(JetTypeInfo).GetMethod(nameof(JetTypeInfo.ReadFloatLE), BindingFlags.Static | BindingFlags.NonPublic)!, pageParam, offsetExpr),
            T_DOUBLE => Expression.Call(typeof(JetTypeInfo).GetMethod(nameof(JetTypeInfo.ReadDoubleLE), BindingFlags.Static | BindingFlags.NonPublic)!, pageParam, offsetExpr),
            T_DATETIME => Expression.Call(typeof(JetTypeInfo).GetMethod(nameof(JetTypeInfo.ReadDateTimeLE), BindingFlags.Static | BindingFlags.NonPublic)!, pageParam, offsetExpr),
            T_GUID => Expression.Call(typeof(JetTypeInfo).GetMethod(nameof(JetTypeInfo.ReadGuidAt), BindingFlags.Static | BindingFlags.NonPublic)!, pageParam, offsetExpr),
            T_NUMERIC => Expression.Call(typeof(JetTypeInfo).GetMethod(nameof(JetTypeInfo.ReadDecimalLE), BindingFlags.Static | BindingFlags.NonPublic)!, pageParam, offsetExpr),
            T_TEXT => Expression.Call(readerParam, DecodeTextMethod, pageParam, offsetExpr, dataLenExpr),
            _ => throw new InvalidOperationException($"BuildReadExpression invoked for unsupported type 0x{colType:X2}."),
        };
    }

    private static bool IsDirectlyDecodable(byte colType, Type targetUnderlying)
    {
        return colType switch
        {
            T_BOOL => targetUnderlying == typeof(bool),
            T_BYTE => targetUnderlying == typeof(byte),
            T_INT => targetUnderlying == typeof(short),
            T_LONG => targetUnderlying == typeof(int),
            T_MONEY => targetUnderlying == typeof(decimal),
            T_FLOAT => targetUnderlying == typeof(float),
            T_DOUBLE => targetUnderlying == typeof(double),
            T_DATETIME => targetUnderlying == typeof(DateTime),
            T_GUID => targetUnderlying == typeof(Guid),
            T_NUMERIC => targetUnderlying == typeof(decimal),
            T_TEXT => targetUnderlying == typeof(string),
            _ => false,
        };
    }
}

/// <summary>
/// Compiled per-<typeparamref name="T"/> delegate that decodes a single row
/// straight off the page bytes into <paramref name="target"/>'s properties,
/// bypassing the per-row <c>object?[]</c> buffer and the box/unbox round-trip
/// that the projection-aware path still pays.
/// </summary>
/// <returns>
/// <see langword="true"/> when the row was decoded; <see langword="false"/>
/// when the row should be skipped (empty / malformed trailer).
/// </returns>
internal delegate bool DirectRowDecoder<T>(
    AccessReader reader,
    byte[] page,
    int rowStart,
    int rowSize,
    bool hasVarColumns,
    T target);
