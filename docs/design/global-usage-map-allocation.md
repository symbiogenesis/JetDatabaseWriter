# Global usage-map allocation

The global usage map on page 1 records free pages with set bits. Native global
maps treat pages beyond an inline window or a missing reference window as free.
Appending a live page therefore requires clearing its allocation bit even when
it extends beyond the old window. The bitmap pages themselves must also be
marked allocated.

A contiguous reservation first appends its complete run, then updates the map.
Map growth appends bitmap pages after that run. Inline promotion copies all old
bits into a native `05 01` bitmap page, initializes the newly covered range as
free, publishes the reference, and marks every new live page and bitmap used.
Map bitmap creation appends directly rather than reentering free-page reuse.

## Native deletion oracle

On the DAO-produced Northwind copy used by `DaoRelationshipDropTests`, an inline
map covered pages 0 through 3007 while the mutated database contained 3018 pages.
The child table definition at page 3008 lay in the implicitly free range. DAO's
parent deletion reused that page for a usage-map bitmap, leaving the child
unreadable. Both an isolated inline-window extension and a native reference-map
promotion, with the known live pages marked used, allowed parent deletion and
preserved the child's fields and recordset. The copied reference-map oracle also
marked its new backing page 3018 allocated. No table or relationship bytes were
changed in those controls.

`GlobalAllocationAppendTests` checks existing future-free bits, contiguous runs
crossing the inline boundary, backing-page allocation and malformed metadata.
The guarded DAO regression remains the native interoperability check for the
integrated writer; a manually corrected copy does not establish that verdict.

## Corruption policy

An established database's malformed global page is refused, never initialized
or cleared as a repair. Before free-page reuse or freeing a page, every reference
pointer is checked for range, duplicate references, page-one aliases and the
native bitmap page type. Every referenced bitmap must itself be covered and
marked allocated. Allocation ranges, including any new backing bitmaps, must
fit the map before writes begin. Creation supplies an initialized global map.

Table/index/LVAL usage maps, complete reachability auditing and interrupted-write
recovery remain separate requirements in the TODO.
