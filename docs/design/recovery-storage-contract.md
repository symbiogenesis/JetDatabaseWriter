# Recovery storage qualification

The recovery proofs assume exact successful bytes-and-length flush fences, restart quiescence, discoverable evidence and exclusive cooperating ownership. This document records the host obligations behind those assumptions. It does not qualify a filesystem or device for power-loss recovery. E4/F6/I15 remain open.

## Current implementation contract

`StreamPageStore.FlushAsync(true)` calls `FileStream.Flush(true)` for a `FileStream`. Other streams receive their own `FlushAsync`; the Boolean request does not give an arbitrary stream device-flush semantics. `PhysicalRollback.BeginWrite` creates persistent recovery evidence only for the file-stream path. In-memory captures and temporary spill files on other paths support rollback within that lifecycle; they do not provide a persistent identity, discovery or reopen protocol.

Journal preparation/records, replacement copies/decisions and ownership receipts flush file contents. Creation, movement, replacement and deletion also change names. The implementation has no explicit parent-directory persistence fence. A successful file-content flush therefore cannot discharge the separate name-discovery premise. A process kill exercises recovery code but leaves the host, its caches and outstanding device work alive.

A failed flush grants no successful fence. Retain the surviving evidence and distinguish an uncertain decision from a confirmed rollback. The buffered model permits finite reordered/torn persistence before failure; its restart transition additionally assumes that retired effects cannot later overwrite recovery. Tests that throw from a managed flush do not qualify this host contract. The buffered content refinement additionally requires authorization for zeros exposed by reordered reextension before a durable replay marker. An old write persisting after truncation can preserve exact undo evidence while leaving an unrecorded zero gap that production refuses without mutation. Qualification must establish this zero-exposure contract, or a separately justified protocol must handle such survivors; undo recoverability alone does not establish parser admission or recovery availability.

## Managed API and operating-system evidence

The public [`FileStream.Flush(Boolean)` contract](https://learn.microsoft.com/en-us/dotnet/api/system.io.filestream.flush?view=net-10.0) covers intermediate file buffers; it does not specify a parent-directory barrier. The following source audit is pinned to the .NET `v10.0.0` reference implementation. It is evidence about that implementation, not a guarantee for every runtime that can load the netstandard2.1 asset. Qualification must record the actual runtime build and repeat the relevant audit.

| Host boundary | Reference evidence | Remaining obligation |
|---|---|---|
| Windows file flush | [`FileStreamHelpers.Windows.FlushToDisk`](https://github.com/dotnet/runtime/blob/v10.0.0/src/libraries/System.Private.CoreLib/src/System/IO/Strategies/FileStreamHelpers.Windows.cs) invokes `FlushFileBuffers`; its access-denied handling also means an exception-free call alone is not a universal successful-fence witness. The [Win32 contract](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers) concerns the specified file handle. | Qualify writable regular-file handles, name persistence and ordering for the actual filesystem, volume, controller and device. Do not infer directory durability from this call. |
| Linux file flush | [`FileStreamHelpers.Unix.FlushToDisk`](https://github.com/dotnet/runtime/blob/v10.0.0/src/libraries/System.Private.CoreLib/src/System/IO/Strategies/FileStreamHelpers.Unix.cs) invokes the runtime synchronization shim. [`SystemNative_FSync`](https://github.com/dotnet/runtime/blob/v10.0.0/src/native/libs/System.Native/pal_io.c) uses `fsync` outside its macOS branch. Linux documents a [separate directory synchronization requirement](https://man7.org/linux/man-pages/man2/fsync.2.html). | Establish directory-entry ordering, successful regular-file synchronization and restart quiescence; qualify mount/storage configuration and error behavior. |
| macOS file flush | The same pinned [runtime shim](https://github.com/dotnet/runtime/blob/v10.0.0/src/native/libs/System.Native/pal_io.c) selects `F_FULLFSYNC` when built with `TARGET_OSX` and `HAVE_F_FULLFSYNC`. [Apple distinguishes this operation from ordinary `fsync`](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fsync.2.html). The managed Unix helper can ignore unsupported-synchronization errors. | Verify the deployed build selects the expected operation and the filesystem supports it. File synchronization alone does not establish this protocol's name lifecycle or cooperating lock semantics. |

The reviewed production calls do not implement a portable directory barrier. This is a conclusion about the current paths, not a proof that no future managed API or protocol can satisfy the requirement. Passing a native directory handle to a managed wrapper would still require platform-specific acquisition and a separate correctness/qualification argument under the unchanged-runtime constraint.

## Qualification receipt

No OS/filesystem/device combination currently has a power-loss qualification receipt in this repository. Ordinary CI supplies runtime, parser and injected-fault evidence. A future receipt must identify the OS/kernel, exact runtime, filesystem/version and mount options, local or remote transport, virtualization layers, controller/device firmware and cache/power protection. A filesystem family name alone is insufficient.

For that fixed configuration, record the following evidence before promoting a storage guarantee:

1. Establish writable regular-file flush semantics for exact bytes **and length**, including truncation, re-extension, errors and previously issued operations.
2. Establish discovery/order of preparation promotion, journal/intent/receipt creation, decisions, replacement and cleanup names. If initial provisioning is relied on, include its independent fence and prove reuse, torn slots and stale-generation rejection.
3. Exercise host/block/device interruptions at those boundaries and during repeated replay. Keep the original and selected images, surviving names/bytes, interruption mechanism and independent post-restart observations. A managed exception or process kill is a different fault model.
4. Establish that no pre-restart or retired-copy effect can resurface after recovery starts. Test exclusive acquisition and stale-generation cleanup across processes and name loss; include macOS coordination explicitly.
5. Qualify destination metadata when a missing name is recreated, including ACLs, timestamps and alternate streams where applicable. Bind any public guarantee to the configurations actually covered.

## Caller streams

The [caller-stream recovery contract](caller-stream-recovery.md) defines current stream classification, live rollback, disposal/failure ownership and exclusive-access obligations, together with the complete journal identity, discovery, fence, reopen, lease and evidence-retention capabilities required of a future persistent stream adapter. The current public stream overloads provide no persistent journal-stream API. A wrapper around a `FileStream` follows the custom-stream path; `CanSeek`, successful `FlushAsync` or an in-process semaphore cannot establish persistent recovery.

The library's current file sidecars are library recovery evidence. Microsoft Access does not replay them; a native open after an interrupted library mutation must follow library recovery. Format interoperability and physical persistence remain separate acceptance requirements.
