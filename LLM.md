# hanzoai/vfs — AI Engineering Guide

## What this is

A filesystem (a **Volume**) on an object-store prefix, sealed with a post-quantum
key. One writer at a time, fenced by a **Round**. The store is any S3-compatible
service or a directory. The engine knows nothing of who mounts it, what runs on
it, or what it costs: it exposes a small POSIX-shaped Go API, a FUSE front, and a
command line.

The `replica/` subpackage (HIP-0107) replicates SQLite snapshots and WAL to an object
store; it shares the Round fence with the Volume (`pkg/fence`).

## On-disk layout

There is one layout and no version number: nothing in an object says which
library wrote it, and no code path reads any other layout. Extension is by
tagged records (below), not by numbered revisions. A Volume is a
prefix holding three kinds of object. Every byte is sealed by the Volume's
`Cipher`; the backend sees ciphertext only.

| Key | What | Name |
|---|---|---|
| `head` | the one mutable object: root shard ref, next inode, Seq, Volume id, chunking seed; its fence header carries the Round | fixed |
| `meta/<2 hex>/<hash>` | metadata shard, immutable | blake3 of the sealed bytes |
| `pack/<2 hex>/<id>` | pack of sealed chunks, immutable | 128 random bits |

**Chunks** (`chunk.go`). File bytes are cut by FastCDC (gear hash, normalized
chunking) into chunks of 64 KiB to 4 MiB, mean 512 to 580 KiB. A cut depends only
on the bytes since the previous cut, so an edit rewrites the chunks it touches and
the cuts re-synchronize after it (a 100 byte overwrite, insertion or deletion in
24 MiB changes one chunk). The gear table is drawn per Volume from the seed in the
Head, so chunk lengths do not identify a known file across Volumes. A chunk id is
the blake3 of its plaintext and appears only inside sealed shards; equal ids in one
Volume mean equal bytes (dedupe). Volumes never share chunks.

**Packs** (`pack.go`). Each chunk is sealed on its own and appended to the open
pack, uploaded at 16 MiB or at Flush. A 150 GiB tree is ~9.6k pack PUTs. Where a
chunk lies is the chunk table, not a pack header.

**Small files** (at most `small` = 16 KiB) are not chunked: their bytes are a
record beside their inode. A file's form is a function of its size alone.

**Holes.** A file's size is its inode's. Only runs of bytes that were written have
chunks; a byte no chunk covers is a hole and reads as zero. Each run is cut on its
own from its first byte. So a truncate to 1 PiB, or a write at an offset of a
petabyte, costs the written runs and nothing for the gap: no chunks, no records, no
memory (`TestSparsePetabyte`). The stored chunk list is, after any sequence of
edits, exactly what a cut of the file's final bytes gives
(`TestChunkListIsTheWholeFileCut`). `MaxSize` is 2^62.

**Metadata** (`record.go`, `tree.go`, `shard.go`). All metadata is records in one
ordered Merkle search tree (a prolly tree) of immutable shards:

```
[1][ino]                 inode: tagged fields (mode, size, times; owner, group, link count, parent)
[2][dir ino][name]       directory entry: child ino, type
[3][ino]                 inline file bytes
[3][ino][offset]         chunk reference: chunk id, length
[4][chunk id]            chunk table: pack id, offset, sealed length
```

The inode record is a sequence of `[tag][length][value]` fields in ascending tag
order. A reader checks the tags it knows, carries the ones it does not, and writes
them back on rewrite. Tags 1 to 5 are always there; 6 and 7 (owner and group, both or
neither), 8 (link count of a non-directory, at least 2: the `links` bit of the Head announces
it) and 9 (the parent of a directory) are there when they differ from what absence says. A
symbolic link's target is its inline data record. A directory's link count is derived (2 and
its subdirectories), cached in memory, never stored. New record kinds take the next `[table]` byte.
The Head's Tail carries `links` (tag 2, under bit 2: the number of inodes with more than one
name; set by the first hard link, never cleared) and `usage` (tag 64, no bit: inodes and bytes
stored, kept by every commit; statfs reads it).

Node boundaries are decided by the entries (a keyed hash of the key under a
threshold proportional to the entry's size, the key derived from the Volume's seed),
average 64 KiB, so the tree's shape is a pure function of its records and the seed.
The key is what stops a guest who picks file names from picking names that each end
their own node (2,000 such names cost 2,007 shards instead of 4). A change rewrites the changed leaves and their ancestors
and nothing else; the new root equals the root built from scratch
(`TestTreeMatchesModel`, `TestTreeForcedCuts`).

**Sealing context.** Every object is sealed together with what it is: a 32-byte
digest of its kind and of the length-framed fields that name it (a Head: its place,
`Backend.String()`; a shard: its Volume id; a chunk: its Volume id and its own id).
The reader compares it for equality, so no context is a prefix of another. An object
from another Volume or place does not open even under a Cipher both share, and every
Seal checks that the Cipher did not return its input. Only the Head is bound to the
place: a Volume copied to another place does not open there until `vfs.Reseal`
rewrites its Head for it. The place string is canonical: absolute and symlink-free
for a directory, `s3://bucket/prefix` without slashes at the ends for a bucket.

## Commit, Flush, Fsync

`Flush` settles every written file into chunks, uploads the open pack, writes the
shards the changes touched, then replaces `head` with **one conditional PUT**
(`If-Match` on the version read; `If-None-Match: *` for the first). Until that
write lands `head` names the previous tree, which nothing touched: a crash at any
point leaves the Volume as the last Flush left it. Uploaded chunks, packs and
shards of a failed Flush are unreferenced garbage, never corruption.

The package documentation (`doc.go`) holds the **durability table**: what is
durable when each call returns, and what a later journal must keep. In short: only
`Flush` is durable; `Fsync` and the FUSE close are `Flush`; `Close` is not; `Attach`
loses no acknowledged commit. Objects are written once under hash or random names,
so repeating a write, or one that completes after its caller gave up, is harmless;
the Head compare-and-swap is the only mutation of the tree.

* On the mount no request is answered EINTR: an interrupt (a signal reaching a caller
  that waits) does not end the wait, and every call returns its own result. C callers
  do not restart EINTR: SQLite reports an fsync answered EINTR as an I/O error, a stat
  answered EINTR is an error to a C caller, a read answered EINTR reaches the caller
  as EIO, close is not restarted, a restarted O_EXCL create would meet its own file.
  Every handler therefore runs on `context.WithoutCancel(ctx)` (`TestEveryHandlerOutlivesItsRequest`
  reads the source for it). The cost, a known limit: store requests have no deadlines, so
  a stalled store stalls its callers and SIGKILL frees none of read, stat, readdir,
  create, mkdir, unlink, truncate, write or fsync (the kernel keeps a request the daemon
  holds), and the daemon holds 128 KiB for each abandoned request (1,600 reads: 225 MB;
  3,000 lookups: 386 MB). Deadlines on the store are the next change; SPECIFICATION 11.2.
  The one exception is a lookup, a getattr and a readdir, which change nothing: an
  interrupt ends them after 3 s (`patient`), so a killed caller of a cold lookup is
  freed. The details are in `pkg/mount/errno.go`; `TestFUSEInterruptsAreNotErrors`
  hammers the mount with signals.
* SIGTERM on a mount commits, asks for the unmount, detaches a mount in use after 5 s
  and returns within 3 s more (`pkg/mount/stop.go`); the commit at the end (`finish`) has a
  deadline of 3 s and exits non-zero saying what was not committed. A supervisor needs a lazy unmount
  at teardown and a store-latency watchdog (SPECIFICATION 11.2).
* A moved Volume is `vfs reseal --backend NEW --round N [--from OLD]` after copying its
  objects; it can be repeated at a higher round, and `--from` can be left out
  (SPECIFICATION 5.4 says what that trusts). The copy is always checked (the root shard
  opens and holds the root directory); `--verify` reads every shard and checks every pack and
  that every extent's chunk is in the chunk table.
* The layout bytes are pinned by `spec_test.go`, which rebuilds each from the
  specification text, and by `pin_test.go` (key shapes, sizes, a fixture of the default
  Cipher). Change a byte and one fails; change the text and the test together.
* The Head ends in `Journal` (the journal sequence it includes, 0 for none), `Needs`
  (feature bits: 0, or bit 2 `links` once a hard link exists) and the Tail: fields `[tag][length][value]`
  with strictly increasing tags. A build refuses a Head with a bit it does not know
  (`ErrNeeds`), refuses one whose Tail is not whole, in order and at most 256 fields or that
  lacks the field of a bit it sets (`ErrCorrupt`), skips the fields it does not know and
  carries every one unchanged. `snapshots`, `links` and `base` are the reserved tags and bits
  1 to 3 (SPECIFICATION 5.1); `links` (2) is defined and the others have no encoding: a bit is
  unusable until its encoding is in the text, and no build sets one of those. A change that must
  be honoured by every writer or every collector sets its bit and writes its field.
* Integers are in their shortest form and a value wider than its field is refused, not
  truncated (SPECIFICATION 1): `reader.uvarint`, `uvarint32`, `varint`.
* Objects beside the tree (a trace store, say) are extension objects (SPECIFICATION 3.1):
  `Volume.PutExt` and `GetExt` keep one at `ext/<name>/<64 hex digits of a BLAKE3 hash keyed
  from Head.Seed over the length-framed name and the key>` (`Volume.ExtKey` gives it), sealed
  to the Volume, the name and that segment, outside the Round's fence. `snapshots`, `links`
  and `base` are not extension names. Readers ignore keys they do not know and a collector
  never deletes them (11.1 says how collection will go).
* The `head` object is at most 65,536 bytes, read and written: `fence.Store` refuses (with
  `ErrTooLarge`, before any request) a record it would not read back, so a commit that would
  pass the bound fails and changes nothing. A Head is read in a fixed order, `ErrCorrupt` for
  a fixed field, `ErrNeeds` for a bit that is not known, `ErrCorrupt` for the Tail
  (`parseHead(b, known)` takes the known bits, so a table of them is tested before one is
  defined). Reseal reads the Head the same way on the branch where it is already sealed to
  the place.
* Every key a Volume writes is 1 to 511 bytes of `a-z`, `0-9`, `-` in segments of at most
  255, separated by `/`; every backend keeps those as written (a backend may keep more: the
  replica package's `orgs/acme/kv.db`), and `storetest.Strict` refuses any other from the
  Volume. `List` returns objects with size and modification time, by string prefix.
* On error the changes stay in memory for the next Flush and the outcome of that
  attempt is unknown. `ErrStaleRound` means this Volume was fenced; if the store's
  Head is exactly its tree, Flush says nil (the commit landed, the reply was lost).
* `Head.Seq` numbers commit attempts and never repeats; it can skip.
* A store that returns an older Head cannot be told from the current one by this
  library; a caller that must notice keeps the Seq it last saw.
* Nothing polls. A deadline timer belongs to the caller.
* Flush holds the Volume exclusively for its duration, including its store calls.
* Memory is bounded without a commit: past `Config.Dirty` bytes written (each
  separate write is charged a fixed overhead too) the files are chunked and
  uploaded; past `Config.Overlay` pending records they are written into shards.

## Fence

`pkg/fence` is the only fencing implementation: one object per key, a Round header
and a claimant token in front of the payload, admitted iff `round >= recorded`, in
one conditional write.

* `Attach(ctx, Config{Round: r})` calls `Claim`: it reads the last landed Head and
  re-seals it at `r` by compare-and-set. `r` must exceed the recorded Round
  (strict, `ErrStaleRound`); 0 and 2^64-1 are refused. No acknowledged commit is
  lost; every commit by a lower Round is then refused. A claim that landed with its
  reply lost is recognised by its token, byte for byte.
* A fenced Volume answers every write with `ErrStaleRound`.
* `Open` is read-only and claims nothing.
* The Round comes from the caller.

## Keys

`Cipher` (`cipher.go`): `Seal(plaintext) (sealed, keyID, err)`, `Open(sealed,
keyID)`. The engine frames every object as `[len keyID][keyID][sealed]`, so a
Cipher holding several keys keeps opening old objects while sealing new ones under
its current key. `Hybrid(current, retired...)` is the default: luxfi/age hybrid
X25519 + ML-KEM-768, one age file per chunk and shard. A Volume checks its Cipher
when configured and refuses one that returns its input, does not open what it
sealed, or opens altered bytes (`ErrBadCipher`). The engine never reads a key from
a file or the environment and never stores one. `Provider` is the seam a caller
implements to fetch a Cipher. The CLI's provider reads **standard input**: one
`AGE-SECRET-KEY-PQ-1...` identity per line, the first seals, the others are
retired. Why stdin: it cannot leave a key in argv, the environment or a file, and
it composes with any secret store that can print to a pipe.

## Go API

```go
vfs.Create(ctx, cfg)                 // empty Volume; refuses an existing one
v, _ := vfs.Open(ctx, cfg)           // read-only
v, _ := vfs.Attach(ctx, cfg)         // cfg.Round > recorded: read-write
v.Stat/Lookup/ReadDir/Mkdir/Create/Remove/Resolve  // by (parent inode, name)
f, _ := v.Open(ctx, ino)             // *File: ReadAt, WriteAt, Truncate, Stat, Close
v.Truncate(ctx, ino, size); v.Flush(ctx); v.Stats(); v.Close()
```

The API is by inode and name, as FUSE is. The namespace operations are `Rename(oldDir,
oldName, newDir, newName, flags)` (atomic; no-replace and exchange), `Link`, `Symlink`,
`Readlink`, `Mknod` (pipe, socket or regular), `SetAttr` (mode, owner and group, times, size),
`File.Punch`, `Parent`, `Usage` and `Check` (the namespace's invariants: names, link counts,
parents and the Head's counters). A creation takes `vfs.Owner(uid, gid)`; without it the inode has
no owner record and belongs to whoever runs the mount. `Config.Clock` is the time of every
change; `Config.Capacity` bounds the bytes (`ErrNoSpace`). An operation reads before it stages,
so an error changes nothing. A file removed while a handle holds it stays whole for the handle
(an orphan, in memory, never in a Head); the last close ends it.

## Layout

```
vfs/
├── doc.go                   # layout, commit, durability table, crash matrix, fence
├── volume.go head.go        # Create/Open/Attach/Flush; Head and sealing contexts
├── ops.go file.go           # names, links, symbolic links, removal; File, settle, recut, holes, orphans
├── rename.go attr.go        # Rename and its flags; SetAttr
├── namespace.go usage.go    # the Head's links and usage fields; directory link counts; Usage
├── check.go                 # Check: the namespace's invariants
├── chunk.go pack.go         # FastCDC; packs, putChunk/getChunk
├── record.go tree.go shard.go overlay.go codec.go lru.go   # metadata tree
├── cipher.go                # Cipher, Provider, Hybrid, the configuration check
├── cmd/vfs/                 # CLI: keygen init put get ls stats mount
├── cmd/vfs-cost/            # cost model (pkg/cost)
├── pkg/backend/             # Backend interface; file/ and s3/ implementations
├── pkg/fence/               # the Round fence
├── pkg/mount/               # FUSE front (build tag fuse): bazil on Linux, jacobsa on macOS
├── replica/                 # SQLite snapshot replication (HIP-0107)
├── internal/storetest/      # Mem store, S3 protocol server, real-server harness, conformance suite
├── tools/mutate/            # the mutation check: breaks the source, expects a test to fail
├── docs/SPECIFICATION.md    # the layout and protocols, normatively
└── docs/PRODUCTION-READINESS.md
```

## Backends

`backend.Backend`: `Get(ctx, key, max)` (bytes and a `Version`; an object larger
than `max` is `ErrTooLarge`), `GetRange`, `Put`, `PutIfAbsent`, `PutIfVersion`,
`Delete`, `List`, `Stat`. The two conditional writes decide and write atomically
(exactly one of N racers on a version wins) or are refused with `ErrTooLarge`;
never silently unconditional. `String()` names the place (bucket and prefix) and
never a credential or endpoint.

| URL | |
|---|---|
| `file:///dir` | atomic via one flock; version = blake3 of content; dev and tests |
| `s3://bucket/prefix?endpoint=host:port[&insecure=1][&region=r]` | S3 API through `hanzos3/go`; ETag as version; `If-Match`/`If-None-Match`; 412 is `ErrConflict`; no credential in the URL (refused); the prefix is validated (no empty, `.` or `..` segment, no control character) |

`s3.New(s3.Config{...})` takes the credential from the caller; the URL form uses
the SDK's standard chain. Objects over 64 MiB go in parts, so a conditional write
that large is refused. The SDK retries a failing request for about half a minute
and hides that from the caller: retries and deadlines are not built yet.

## Tests

```
go test ./...                                    # fast + scale tests
go test -short -race ./...                       # the race detector over the lot (CI runs it on the tests with goroutines)
go test -tags fuse ./pkg/mount                   # needs /dev/fuse + fusermount3
tools/ci/mount.sh ./pkg/mount run '^TestFUSEPOSIX$'   # the same with no helper and no privilege, in a user namespace
tools/ci/gitwork.sh DIR ORIGIN                   # git's work on DIR: the same hashes on a mount and on tmpfs
tools/ci/treehash.sh DIR FACTS                   # every fact about a tree, and one hash (an npm ci on both)
PATH=$DIR:$PATH go test ./...                    # $DIR holds the `s3` server (tools/ci/s3server.sh DIR fetches the published image)
go run ./tools/mutate                            # every mutation must be killed by a test
```

The real-server tests start `s3 server` (own-storage mode) in a temp dir with a
throwaway key pair, race 16 and 128 writers on one version, run whole Volumes on it,
and kill a writer at random moments. CI (`hanzo.yml`, `.github/workflows/cicd.yml`)
runs sixteen gates as parallel jobs, each under five minutes: `check`, `unit`, `race`,
`model`, `sparse`, `sparse-tiny`, `sqlite` (the only gate that builds the SQLite engine),
`realserver`, `realserver-cas`, `realserver-cas-large` and `realserver-kill` (against the
published image of the server, pinned by digest, which fail if a test did not pass),
`scale-a`, `scale-b`, `scale-c`, `fuzz` and `fuzz-head`. The tests on a kernel mount are not gates: they
assert what Linux's FUSE does, which a kernel of another kind answers differently
(`docs/PRODUCTION-READINESS.md` has the release checklist). They run on a Linux
kernel before a release, with `go test -tags fuse -count=1 ./pkg/mount ./internal/sqlitecheck`, or with
`tools/ci/mount.sh` where there is no `fusermount3` (it mounts in a user namespace
through `tools/fusemount`). Crash tests kill a
re-executed test binary with SIGKILL at every point of a commit (the key arrives on
its stdin). A mount test serves the Volume from a child process, always: a process
that mounts a Volume and also opens a file on that mount deadlocks. Go registers the
open file with its poller, the kernel asks the file system for readiness, and the
server needs the same poller to read its next request; nothing in the process can
answer, and without root the threads stay in D state until reboot.

## Not yet built (plain statement of fact)

* Extended attributes, device nodes, `O_TMPFILE`, the flags of `renameat2` on a mount
  (the FUSE library has no request for them: SPECIFICATION 11.2), and the sticky bit in
  `stat` (the library leaves it out of its answer).
* The node cache and journal (Fsync costs a commit), group commit, parallel
  uploads, read-ahead, partial-chunk reads.
* A server and agent process around the Volume.
* Garbage collection of chunks, packs and shards no Head names (a deposed writer
  can upload packs until its next commit), Snapshot, Clone, Usage, import, export.
* Retries with backoff, deadlines on store calls, a circuit breaker, metrics,
  handling of a store's slow-down answers. The Volume lock is held across the store
  calls of a commit.
* Paged directory listing: ReadDir returns a whole directory.
* Re-sealing a Volume's Head for a new place.
* Compression.

## Rules

1. Never store plaintext or a key. Everything crosses `Cipher`.
2. One way: one fence, one metadata tree, one commit (a single conditional PUT).
3. Orphans are garbage for a collector, never corruption: write order is chunks and
   packs, then shards, then `head`.
4. No polling. A deadline timer is the caller's.
5. No test-only environment variables or build tags.
6. LLM.md is the one project memory file; CLAUDE.md / AGENTS.md are local symlinks.
