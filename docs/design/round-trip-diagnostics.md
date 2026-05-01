# Round-trip diagnostics notes

This note preserves the useful parts of the temporary `DIAG_ROUNDTRIP`
scratch tests that were removed from `JetDatabaseWriter.Tests`. Those tests
were never part of normal builds or CI; they existed only to narrow one
round-trip failure and inspect raw page bytes.

## When to use this

Use these notes only when a Microsoft Access round-trip regression reappears
and the normal coverage in `AccessRoundTripTests` is not enough to isolate the
 failure mode.

The normal regression suite remains:

- `SinglePk_AndSingleColumnFk_SurviveCompactAndRepair`
- `CompositePk_AndMultiColumnFk_SurviveCompactAndRepair`

## Historical narrow bisection matrix

The removed `AccessRoundTripDiagNarrow` file explored variations of the same
minimal write pattern against a copied `NorthwindTraders.accdb` fixture, then
ran DAO `CompactDatabase(src, dst)` and treated any non-zero exit as failure.

Base shape:

- Create `RT_Customers` with `CustomerID` (`int`, PK, autonumber, not null)
  and `Name` (`string`, max length 100, not null).
- Create `RT_Orders` with `OrderID` (`int`, PK, autonumber, not null),
  `CustomerID` (`int`, not null), and `OrderDate` (`DateTime`).

Variants that were useful during bisection:

| Case | Variation |
| --- | --- |
| `N1` | Original failing shape: `RT_Customers` then `RT_Orders` |
| `N2` | Same as `N1`, but `RT_Customers.Name` is nullable |
| `N3` | Only `RT_Customers` |
| `N4` | Only `RT_Orders` |
| `N5` | Reverse creation order: child first, then parent |
| `N6` | Remove explicit `IsNullable = false` from non-string columns |
| `N7` | Use unrelated names: `ZZ_Foobars` and `ZZ_Bazquxxxx` |
| `N8` | `RT_Customers` plus benign second table `ZZ_Bazquxxxx` |
| `N9` | `ZZ_Foobars` plus `RT_Orders` |
| `N10` | Similar lengths, different names: `RT_Cusxxxxxx`, `RT_Ordxxxxx` |
| `N11` | Keep `RT_Customers`, use second table `RT_Ord12345` |
| `N12` | Keep `RT_Customers`, use second table `RT_Ord123456` |
| `N13` | Keep `RT_Customers`, use second table `RT_Ord1234` |
| `N14` | Short names: `RTCust` and `RTOrds` |

These cases are worth preserving because they encode the historical search
space: nullability, table independence, creation order, name collision, and
name-length effects.

If a similar regression returns, recreate the smallest relevant subset as
temporary local tests or a focused helper near `AccessRoundTripTests` rather
than restoring the old files wholesale.

## Historical page-dump probe

The removed `AccessRoundTripDiagPageBytes` file compared a known failing name
shape against a known passing name shape by dumping raw bytes from the copied
source database before compacting.

Historical cases:

- Failing-name case: create `RT_Customers` and `RT_Orders`
- Passing-name case: create `RT_C18` and `RT_O18`

Useful observations from that probe setup:

- The file size and the page count (`file_size / 4096`) were logged first.
- Page `2994` was the primary inspection target.
- The probe logged page header fields from page `2994`:
  `free_space`, `tdef_pg`, `num_rows`, then every row offset.
- It dumped three byte regions from page `2994`:
  `0x0000..0x0100`, the tail `4096 - 384 .. 4096`, and
  `0x0B40..0x0C00`.
- It also scanned original Northwind pages `2990..3007` and any appended pages
  `3008+` to compare header bytes and detect newly allocated pages.

If a future investigation needs raw bytes again, recreate a one-off probe that:

1. Copies `NorthwindTraders.accdb` to a temp working directory.
2. Clears the read-only attribute.
3. Applies one minimal writer mutation.
4. Dumps the relevant page headers and hex slices before compact.
5. Runs DAO compact through `AccessRoundTripEnvironment.RunDaoCompact`.

Do not treat page `2994` as a permanent invariant; it was an observed hot spot
in the historical Northwind-based investigation, not a guaranteed location in
all fixtures or after unrelated catalog changes.