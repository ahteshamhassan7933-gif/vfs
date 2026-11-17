// Package vfs is a filesystem on an object store: a Volume.
//
// # On-disk layout
//
// A Volume is a prefix of a bucket holding three kinds of object, all sealed by
// the Volume's Cipher:
//
//	head                    the one mutable object: names the root shard
//	meta/<2 hex>/<hash>     metadata shards, immutable, named by the hash of their bytes
//	pack/<2 hex>/<id>       packs of file chunks, immutable, named by a random id
//
// File bytes are cut into content-defined chunks of 64 KiB to 4 MiB (chunk.go).
// Each chunk is sealed on its own, and sealed chunks are packed together into
// objects of about 16 MiB, so a tree of 150 GiB is a few thousand PUTs rather
// than one per chunk. Files of at most 16 KiB are not chunked at all: their bytes
// are a record in the metadata shard, beside their inode. A hole in a file is no
// record at all: a file's size is its inode's, and only the runs of bytes that
// were written have chunks, so the cost of a file follows what was written and
// not how large it was declared.
//
// All metadata (inodes, directory entries, chunk references, the table saying
// where each chunk lives) is records in one ordered Merkle tree (tree.go), cut
// into shards. The Head holds the tree's root ref, the next inode number, the
// Volume's identity and the seed of its chunking table.
//
// Every object is sealed together with what it is: shards and chunks to their
// Volume, a Head to the Volume's place (Backend.String). An object copied in from
// another Volume, or a Volume copied to another place, does not open.
//
// Objects are written once, under names that are a hash of their content or a
// random id: a write that is repeated, or that completes after its caller gave
// up, puts the same bytes at the same name. The Head is the only object that
// changes, and only by compare-and-swap.
//
// # Commit
//
// Flush settles every written file into chunks, uploads the open pack, writes
// the shards the changes touched (and no others), and then replaces the Head with
// one conditional write. Until that write lands the Head names the previous
// tree, which was not touched: a crash anywhere before it leaves the Volume
// exactly as the last Flush left it. Chunks, packs and shards a crashed or
// failed Flush uploaded are unreferenced garbage, never corruption.
//
// # Durability
//
// What is durable, in the store, when a call returns. "Commit" is the Head
// advancing in the store: the conditional PUT of the Head has returned success.
//
//	call                                   durable when it returns
//	File.WriteAt, Truncate, Punch, Create, nothing. Held in memory; lost if the process dies.
//	  Mkdir, Remove, Rename, Link, Symlink,  Past Config.Dirty bytes the data is chunked and
//	  Mknod, SetAttr                         uploaded, still unreferenced, still not durable.
//	                                         Each is atomic in a commit: a Head holds all of
//	                                         it or none.
//	File.Close                             nothing. It releases the handle; it is not a flush.
//	Volume.Flush                           everything written before the call, committed. It
//	                                         returns only after the Head advanced.
//	Fsync (FUSE FSYNC)                     the same as Flush: a full commit. There is no
//	                                         cheaper durable point. A signal that reaches the
//	                                         caller does not end the wait.
//	Flush on the FUSE front (every close)  the same as Flush: a full commit.
//	Release on the FUSE front              nothing.
//	end of a mount                         one Flush, then the mount returns.
//	Volume.Close                           nothing. It releases the backend and does not
//	                                         flush: a caller that must keep its writes calls
//	                                         Flush first (the detach sequence is Flush, Close).
//	Attach (takeover)                      every commit the previous owner saw acknowledged is
//	                                         in the new owner's tree, and no commit by an older
//	                                         Round lands afterwards.
//
// Interrupts on the mount. A signal that reaches a caller while it waits on the mount
// (Go's runtime sends one every few milliseconds) makes the kernel interrupt the
// request, and no request is answered EINTR: every call waits and returns its own
// result. Callers do not restart it: SQLite reports an fsync answered EINTR as an I/O
// error, a stat answered EINTR is an error to a C caller whether or not it set
// SA_RESTART, a read answered EINTR reaches its caller as EIO, close is never
// restarted, and a restarted create(2) with O_EXCL would meet its own file. Nothing a
// caller's interrupt does cancels a commit or a fetch. The cost is that a call waits
// as long as the store takes: store requests have no deadline yet (docs/SPECIFICATION.md,
// section 11.2).
//
// What a failed Flush tells the caller. nil: committed. Any other error: the
// outcome of that attempt is unknown to the caller (a reply can be lost after the
// write landed), and the changes stay in memory for the next Flush. ErrStaleRound:
// this Volume was fenced and can never commit again; changes it had not committed
// will never land, but a commit whose acknowledgement was lost may already be in
// the successor's tree, and Flush reports nil when the store's Head is exactly this
// Volume's tree. To learn more, read the Volume from the successor's view.
//
// What a later change (a node journal for fast fsync, group commit) must keep:
// Flush and the detach sequence return only after the Head advanced in the store;
// a takeover loses no acknowledged commit; objects are written once under names
// that make repeating a write harmless; and the Head compare-and-swap stays the
// only mutation. Fsync is a full commit today; a journal may make a call cheaper
// only by making it durable somewhere else first, and then Flush must still be a
// commit in the store.
//
// The crash matrix the tests cover (a process killed by SIGKILL, or a call made
// to fail, at each point of a commit, then the store read from another process):
//
//	point                                  test
//	before the first pack upload           TestSigkill/pack
//	after the first pack upload            TestSigkill/packed
//	before the first shard upload          TestSigkill/shard
//	after the first shard upload           TestSigkill/sharded
//	before the Head write                  TestSigkill/head
//	after the Head write                   TestSigkill/landed
//	each write of a commit failing         TestFailureAtEveryWrite
//	Head landed, reply lost, then retry    TestLostAcknowledgementOfHead, TestLostAckNoLostUpdateNoDuplicate
//	Head landed, reply lost, takeover      TestLostAckThenTakeoverIsNotReportedLost
//	takeover during a stream of commits    TestTakeoverLosesNoAcknowledgedCommit
//	kill at a random moment, real server   TestKillOverRealServer
//
// In every row, a fresh view of the store is the last committed tree, whole.
//
// # Fence
//
// Attach claims the Volume at a Round, a number that only grows. The claim is a
// conditional write of the Head that also carries forward the previous owner's
// last committed Head, so no acknowledged commit is lost, and from then on a
// commit by any older Round is refused with ErrStaleRound. A claim whose write
// landed but whose reply was lost is recognised as the claimant's own. Round
// 2^64-1 is refused, since no Round could follow it.
//
// The store is trusted to return the Head it holds. A store that returns an older
// Head it once held cannot be told from the current one by this library: a caller
// that must notice a rollback keeps the Seq of the last Head it saw and compares.
//
// # Keys
//
// Every object goes through a Cipher (cipher.go); the library never reads a key
// from a file or the environment and never stores one.
package vfs
