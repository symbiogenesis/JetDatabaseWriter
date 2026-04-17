namespace JetDatabaseReader
{
    using System;

    /// <summary>
    /// Structured column size: a numeric <see cref="Value"/> paired with a <see cref="ColumnSizeUnit"/>.
    /// Use <see cref="ToString"/> for a human-readable description.
    /// </summary>
    public readonly struct ColumnSize : IEquatable<ColumnSize>
    {
        /// <summary>Variable-length with no declared maximum.</summary>
        public static readonly ColumnSize Variable = new ColumnSize(null, ColumnSizeUnit.Variable);

        /// <summary>Large-value data stored on LVAL pages (MEMO / OLE).</summary>
        public static readonly ColumnSize Lval = new ColumnSize(null, ColumnSizeUnit.Lval);

        private ColumnSize(int? value, ColumnSizeUnit unit)
        {
            Value = value;
            Unit = unit;
        }

        /// <summary>Gets the numeric count; <c>null</c> for <see cref="ColumnSizeUnit.Variable"/> and <see cref="ColumnSizeUnit.Lval"/>.</summary>
        public int? Value { get; }

        /// <summary>Gets the unit in which <see cref="Value"/> is expressed.</summary>
        public ColumnSizeUnit Unit { get; }

        /// <summary>Determines whether two <see cref="ColumnSize"/> values are equal.</summary>
        public static bool operator ==(ColumnSize left, ColumnSize right)
        {
            return left.Equals(right);
        }

        /// <summary>Determines whether two <see cref="ColumnSize"/> values are not equal.</summary>
        public static bool operator !=(ColumnSize left, ColumnSize right)
        {
            return !left.Equals(right);
        }

        /// <summary>Creates a fixed size expressed in bits.</summary>
        /// <returns></returns>
        public static ColumnSize FromBits(int count) => new ColumnSize(count, ColumnSizeUnit.Bits);

        /// <summary>Creates a fixed size expressed in bytes.</summary>
        /// <returns></returns>
        public static ColumnSize FromBytes(int count) => new ColumnSize(count, ColumnSizeUnit.Bytes);

        /// <summary>Creates a maximum character count for a text column.</summary>
        /// <returns></returns>
        public static ColumnSize FromChars(int count) => new ColumnSize(count, ColumnSizeUnit.Chars);

        /// <inheritdoc/>
        public override string ToString()
        {
            switch (Unit)
            {
                case ColumnSizeUnit.Bits: return Value == 1 ? "1 bit" : $"{Value} bits";
                case ColumnSizeUnit.Bytes: return Value == 1 ? "1 byte" : $"{Value} bytes";
                case ColumnSizeUnit.Chars: return $"{Value} chars";
                case ColumnSizeUnit.Variable: return "variable";
                case ColumnSizeUnit.Lval: return "LVAL";
                default: return string.Empty;
            }
        }

        /// <inheritdoc/>
        public bool Equals(ColumnSize other)
        {
            return Value == other.Value && Unit == other.Unit;
        }

        /// <inheritdoc/>
        public override bool Equals(object obj)
        {
            return obj is ColumnSize other && Equals(other);
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            return (Value.GetHashCode() * 397) ^ Unit.GetHashCode();
        }
    }
}
