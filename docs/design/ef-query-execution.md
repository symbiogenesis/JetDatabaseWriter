# EF Core query execution

The provider compiles the original query through `JetAsyncQueryExecutor`.
Its plan records table aliases, required columns, safe scan predicates, join-key
lookups, ordering and row limits. EF materializers are built during query
compilation using the registered dependencies. The EF-required translating
visitor factory rejects shaped-query translation; it does not pretend to
translate operators that the provider's executor owns.

## Buffer execution and materialization

Scalar property projections, scalar grouping and supported joins operate on
value buffers without constructing intermediate entities. Unordered single-table
`Where`, `Select`, `Skip`, `Take` and scalar `Distinct` pipelines stream through
the connection. Plain entity enumeration constructs a returned entity when its
row is requested. Blocking operators retain buffers; compiled result shapes
materialize surviving entity references and honor tracking and identity
resolution. Scalar grouping retains only columns actually referenced by its
keys, aggregates, predicates and joins.

`Count` and `LongCount` visit rows with one reusable value buffer. Supported
single-table `Sum`, `Min`, `Max`, `Average` and `All` use the same visitor contract,
including nullable and empty-source behavior, checked integral accumulation and
early termination for `All`. A visitor cannot retain its buffer after returning.
Streaming consumers receive independent buffers and own a read lease until their
enumerator is disposed, including cancellation and early exit.

Synchronous enumeration advances the asynchronous pipeline once per requested
row and synchronously disposes that enumerator when the consumer stops or throws.
It adds no full-result buffer; blocking operators retain their own buffering
requirements. Synchronous scalar terminals use the same asynchronous execution
path and wait for their single result.

Global query filters bind context references to the context executing the query,
including cached and compiled queries. Index candidates and residual predicates
use the same bound values. Named `IgnoreQueryFilters` exclusions disable only
the requested filters; the parameterless call disables all filters. The last
tracking modifier in a query chain determines tracking and identity resolution.
`IgnoreAutoIncludes` suppresses configured eager loading while retaining owned
navigations, including nested owned references and collections. Repeated owned
include prefixes reuse each owner's instances without resolving identities
between separate no-tracking owners.

Entity equality, user methods that consume entities, constrained generic shapes,
inheritance, owned graphs and navigation-dependent shapes can require the general
entity execution path. These cases preserve CLR and EF behavior rather than
substituting buffer equality or incomplete entities. Complete entities include
all mapped properties, including at `First`, `Single` and `Last` terminals and
inside projected graphs. Result-graph promotion uses weakly cached compiled
accessors and constructors, including private DTO setters; it does not perform
member reflection for every result.

Supported single-table bulk mutations rent the writer before selecting buffers.
They read the key, filter and setter columns, evaluate every assignment against
the original buffer, deduplicate selected keys and apply atomic writer batches.
The general path remains available for richer entity graphs and setter shapes.

## Pushdown boundaries

A predicate outside `Skip` or `Take` cannot narrow the scan before that operator.
An outer match limit cannot become an unfiltered inner limit after a predicate
boundary. Unknown operators retain full evaluation when the planner cannot prove
a safe bound. Join provenance permits scalar predicates after inner joins and
key lookups for later joins in a chain. Inner predicates remain residuals after
join-key lookup. Outer joins retain null-extension behavior.
Comparer overloads and constructed join keys with nullable components retain
ordinary evaluation when exact-key candidate lookup cannot preserve equality.

Projected root and navigation reads retain key and relationship columns and skip
unreferenced long values when the result does not require a complete entity.
Ordered index reads support matching mixed directions and reverse traversal.
Residual scalar filters run before a matching-row limit. Native text/GUID order,
value converters, calculated ordering and legacy Numeric key order retain a sort
fallback where equivalence to the requested CLR order is unqualified.

## Connection and persistence ownership

File paths resolve to an absolute path when the scoped `JetClientConnection` is
constructed. Its `DatabasePath` property, existence checks, creation, reads,
writes and deletion all use that captured path, even if the working directory
later changes. EF peer identity compares absolute paths ordinally on every
platform, preserving distinct files on case-sensitive filesystems and Windows
case-sensitive directories. Callers must use the same absolute path spelling
for peer handoff; case aliases, symbolic links and hard links are not unified.

Connections with the same absolute path spelling or stream instance coordinate
lifecycle transitions and acquire per-connection operation leases. A writer rental holds its lease through
release, so peer invalidation and disposal wait for the operation. Idle file and
stream writers are reused; switching a peer connection retires idle writer
state. Reader peers may coexist. Explicit transactions prevent another
connection from taking over their writer. Schema creation releases its file
writer when finished.
The configured `leaveOpen` option applies to both direct streams and streams
returned by a factory; owned streams close after their readers and writers.

Cancellation before transaction completion enters the connection preserves the
EF transaction handle and its savepoints for retry or rollback. Once completion
has entered and ends the underlying transaction, the handle is invalidated even
when commit or rollback reports a failure.

`SaveChangesAsync` protects multiple row operations with a nested transaction.
At the native write-nesting limit it refuses before mutation if that protective
scope cannot be opened; releasing a savepoint permits retry. Table-split owned
values retain foreign keys other than their ownership keys. Update and delete
predicates include original concurrency tokens from the tracked entities sharing
the row, and successful saves propagate shared-column tokens across them.

Creation requires an empty destination stream and initializes it from offset
zero. `EnsureCreatedAsync` and `MigrateAsync` preserve nonempty unreadable streams
and throw, including when the configured password cannot open the database.

Creation and additive migration invoke `UseAsyncSeeding` after releasing the
schema writer, including when the schema is already current. Callbacks receive
the executing context, cancellation token and whether schema or model-seed
operations occurred. Schema creation commits before callback execution. Callback
writes use a separate transaction when the caller has none, and roll back on
failure or cancellation; an explicit caller transaction remains caller-owned.

`DeleteBehavior.NoAction` creates an enforcing relationship without cascading
deletes. Self-referencing save dependencies use each foreign key's actual
principal key, including composite alternate keys. Self-referencing deletes
execute in dependency order rather than in physical scan order.

Generated-value refresh first attempts exact indexed lookup, then evaluates the
same primary-key criteria through a row scan when the index cannot serve the
key type or is unavailable. Migration seeding converts model keys to provider
values, compares binary values structurally, and uses indexed or stop-first
streamed existence checks. Finalized metadata and CLR mapping attributes are
weakly cached; mutable model construction continues to observe edits.
Inheritance seed rows fill omitted discriminator values through the property's
provider conversion. Decimal precision defaults use enum underlying and converter
provider types, preserving full unsigned 64-bit values and integer-backed time
values while honoring explicit precision and scale.

The reverse cursor and EF adapters remain reviewed and tested C# components.
Their tests do not extend the generated recovery or index-kernel proof boundary.
