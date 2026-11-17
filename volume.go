package vfs

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"sync"
	"time"

	"github.com/hanzoai/vfs/pkg/backend"
	"github.com/hanzoai/vfs/pkg/fence"
)

var (
	// ErrStaleRound is returned when the Round does not hold the Volume: a commit by
	// a Round below the recorded one (this writer was fenced, and changes it had not
	// committed will never land), or an Attach at a Round that does not exceed the
	// recorded one. A commit whose acknowledgement was lost may nevertheless be in
	// the successor's tree; see the package documentation.
	ErrStaleRound = fence.ErrStaleRound

	// ErrTooBig is returned for a file larger than MaxSize.
	ErrTooBig = errors.New("vfs: file too large")

	// ErrReadOnly is returned by a write on a Volume that was Opened, not Attached.
	ErrReadOnly = errors.New("vfs: volume is read-only")

	ErrNotDir   = errors.New("vfs: not a directory")
	ErrIsDir    = errors.New("vfs: is a directory")
	ErrNotEmpty = errors.New("vfs: directory not empty")
)

// Config says where a Volume is, how it is sealed, and how much memory it may use.
type Config struct {
	Backend backend.Backend // the Volume's prefix; the Volume owns it and Close closes it
	Cipher  Cipher          // seals and opens every object
	Round   uint64          // Attach, Reseal: the Round to claim; must exceed the recorded one, and be above 0
	Verify  bool            // Reseal: read every shard the Head reaches and check every pack it needs is there, not only the root

	Capacity int64            // bytes the Volume may hold: a write past it is refused with ErrNoSpace; 0 for no limit
	Clock    func() time.Time // the time every change is stamped with; default time.Now

	Cache   int64 // bytes of shards and chunks kept decoded; default 256 MiB
	Dirty   int64 // bytes of unsettled writes (each separate write counts a fixed overhead too) before they are chunked and uploaded; default 64 MiB
	Overlay int   // metadata changes held before they are written into shards; default 262144
}

func (c Config) valid() (Config, error) {
	if c.Backend == nil || c.Cipher == nil {
		return c, errors.New("vfs: config needs a Backend and a Cipher")
	}
	if c.Cache <= 0 {
		c.Cache = 256 << 20
	}
	if c.Dirty <= 0 {
		c.Dirty = 64 << 20
	}
	if c.Overlay <= 0 {
		c.Overlay = 1 << 18
	}
	if err := check(c.Cipher); err != nil {
		return c, err
	}
	return c, nil
}

// maxHead bounds what a read accepts for the Head: it is a few hundred bytes
// sealed under a post-quantum key.
const maxHead = 64 << 10

// Volume is a mounted filesystem. All methods are safe for concurrent use.
// A Volume that was Attached may write; one that was Opened may only read.
type Volume struct {
	be    backend.Backend
	c     Cipher
	fence *fence.Store
	cfg   Config
	round uint64 // 0 when read-only
	t     *tree

	chunks *lru[chunkID, []byte]
	id     volumeID
	gear   *gear

	// mu guards everything below. Flush holds it throughout: writers wait for a
	// commit, readers too. The upload pipeline that relaxes this comes with the
	// node cache.
	mu     sync.RWMutex
	head   Head   // the committed Head
	seq    uint64 // the last Seq this Volume wrote or read, landed or not
	root   ref    // the tree with every applied change, committed or not
	over   *overlay
	next   InodeID
	files  map[InodeID]*fileState
	dirty  int64
	pack   pack
	fenced bool
	ns     counters // usage and links, written into every Head
	dirn   dirCache // link counts of directories
}

func newVolume(cfg Config) *Volume {
	sh := &shards{be: cfg.Backend, c: cfg.Cipher, cache: newLRU[ref, *node](cfg.Cache / 2)}
	return &Volume{
		be:     cfg.Backend,
		c:      cfg.Cipher,
		fence:  fence.New(cfg.Backend, maxHead),
		cfg:    cfg,
		t:      newTree(sh, [32]byte{}),
		chunks: newLRU[chunkID, []byte](cfg.Cache / 2),
		over:   newOverlay(),
		files:  map[InodeID]*fileState{},
	}
}

// adopt makes h the state of the Volume: what the Head names, and the identity
// everything else is sealed to.
func (v *Volume) adopt(h Head) {
	v.head, v.seq = h, h.Seq
	v.root, v.next = h.Root, InodeID(h.Next)
	v.id, v.gear = h.ID, newGear(h.Seed)
	v.t = newTree(v.t.sh, h.Seed)
	v.t.sh.aad = shardAAD(h.ID)
}

// Create makes an empty Volume at the Backend's prefix: a tree holding the root
// directory, and a Head naming it, written only if no Head exists. It returns an
// error wrapping fs.ErrExist if the prefix already holds a Volume.
func Create(ctx context.Context, cfg Config) error {
	cfg, err := cfg.valid()
	if err != nil {
		return err
	}
	v := newVolume(cfg)
	h := Head{Next: uint64(RootInode) + 1}
	for _, b := range [][]byte{h.ID[:], h.Seed[:]} {
		if _, err := rand.Read(b); err != nil {
			return err
		}
	}
	v.adopt(h)
	now := time.Now()
	root := Inode{ID: RootInode, Mode: fs.ModeDir | 0o755, Mtime: now, Atime: now, Ctime: now}
	if h.Root, err = v.t.apply(ctx, none, []mut{{key: keyInode(RootInode), val: encodeInode(root)}}); err != nil {
		return err
	}
	sealed, err := seal(v.c, headAAD(cfg.Backend.String()), h.marshal())
	if err != nil {
		return err
	}
	if err := v.fence.Create(ctx, headKey, sealed); err != nil {
		if errors.Is(err, backend.ErrConflict) {
			return fmt.Errorf("vfs: %s already holds a volume: %w", cfg.Backend, fs.ErrExist)
		}
		return err
	}
	return nil
}

// Open returns a read-only Volume at the committed Head. It claims nothing and
// fences no one.
func Open(ctx context.Context, cfg Config) (*Volume, error) {
	cfg, err := cfg.valid()
	if err != nil {
		return nil, err
	}
	v := newVolume(cfg)
	payload, _, err := v.fence.Get(ctx, headKey)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return nil, fmt.Errorf("vfs: no volume at %s: %w", cfg.Backend, fs.ErrNotExist)
		}
		return nil, err
	}
	h, err := v.readHead(payload)
	if err != nil {
		return nil, err
	}
	v.adopt(h)
	if err := v.ns.load(h); err != nil {
		return nil, err
	}
	return v, nil
}

// Attach claims the Volume at cfg.Round and returns it ready to write. The claim
// is refused with ErrStaleRound unless cfg.Round exceeds the Round recorded; it
// seals the predecessor's last committed Head at the new Round, atomically, and
// opens the Volume at that Head, so nothing the predecessor committed is lost and
// nothing it tries to commit afterwards lands.
//
// After any error from Attach other than a refusal for a Round that is too low,
// the claim may or may not have landed (a reply can be lost). A caller that retries
// MUST use a higher Round: a retry at the same Round is told ErrStaleRound, since
// the Round is by then recorded, and only a claim whose own retry finds its own
// bytes within one call is recognised as landed.
func Attach(ctx context.Context, cfg Config) (*Volume, error) {
	cfg, err := cfg.valid()
	if err != nil {
		return nil, err
	}
	if cfg.Round == 0 || cfg.Round == math.MaxUint64 {
		return nil, fmt.Errorf("%w: Attach needs a Round above 0 and below 2^64-1, since no Round can follow the last", fs.ErrInvalid)
	}
	v := newVolume(cfg)
	var h Head
	err = v.fence.Claim(ctx, headKey, cfg.Round, func(payload []byte, recorded uint64) error {
		if payload == nil {
			return fmt.Errorf("vfs: no volume at %s: %w", cfg.Backend, fs.ErrNotExist)
		}
		if cfg.Round <= recorded {
			return fmt.Errorf("%w: round %d does not exceed the recorded %d", ErrStaleRound, cfg.Round, recorded)
		}
		var err error
		h, err = v.readHead(payload)
		return err
	})
	if err != nil {
		return nil, err
	}
	v.adopt(h)
	if err := v.ns.load(h); err != nil {
		return nil, err
	}
	v.round = cfg.Round
	return v, nil
}

// Reseal rewrites the Head of the Volume at cfg.Backend so that it is sealed to
// that place, and claims the Volume there at cfg.Round, which must exceed the
// recorded Round and be above 0 and below 2^64-1. A Head is bound to its place, and
// every other object to the Volume, so moving a Volume to another bucket or prefix
// is: copy every object, then Reseal. It fences the copy only; a writer still
// attached at the old place is not told, so stop it first (a writer that commits
// without pause can also make Reseal lose the race for the Head, which is then
// retried at a higher Round).
//
// from is the place the Head is sealed to now: the String of the Backend it was last
// written through. If the Head is already sealed to cfg.Backend, a Reseal whose
// reply was lost for instance, it is left as it is and only the Round is claimed, so
// a Reseal can always be repeated at a higher Round.
//
// from may be empty when the old place is not remembered. The Head is then opened
// with the key alone, whatever place it was sealed to. That trusts the key (only a
// holder can have written a Head that opens) and that the Head at cfg.Backend is the
// newest Head of the Volume the caller means: it cannot tell a Head from before the
// newest commit from the newest, as no one can without the place.
//
// However the Head was opened, the copy is checked before anything is written: the root
// shard the Head names must open at cfg.Backend sealed to the Volume the Head names and
// hold the root directory, which refuses a copy that lacks its objects, a Head of another
// Volume, and an object of another kind that a store writer put where the Head goes.
// cfg.Verify reads every shard the Head reaches and checks that every pack a chunk is in
// is there and long enough; chunks are checked when a file is read.
func Reseal(ctx context.Context, cfg Config, from string) error {
	cfg, err := cfg.valid()
	if err != nil {
		return err
	}
	if cfg.Round == 0 || cfg.Round == math.MaxUint64 {
		return fmt.Errorf("%w: Reseal needs a Round above 0 and below 2^64-1", fs.ErrInvalid)
	}
	v := newVolume(cfg)
	here := cfg.Backend.String()
	w := newVolume(cfg) // reads the copy: one cache for every attempt, which find mostly the same shards
	return v.fence.Rewrite(ctx, headKey, cfg.Round, func(payload []byte, recorded uint64) ([]byte, error) {
		if payload == nil {
			return nil, fmt.Errorf("vfs: no volume at %s: %w", cfg.Backend, fs.ErrNotExist)
		}
		if cfg.Round <= recorded {
			return nil, fmt.Errorf("%w: round %d does not exceed the recorded %d", ErrStaleRound, cfg.Round, recorded)
		}
		var plain []byte
		var err error
		var sealedHere bool // the Head is already sealed to this place
		if from == "" {
			plain, err = unsealAny(cfg.Cipher, payload)
		} else if plain, err = unseal(cfg.Cipher, headAAD(from), payload); errors.Is(err, errContext) {
			if plain, err = unseal(cfg.Cipher, headAAD(here), payload); err == nil {
				sealedHere = true
			} else {
				err = fmt.Errorf("%w (from %s)", errContext, from)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("vfs: open head: %w", err)
		}
		// Whichever way the Head was opened it is read like any other, so a Head that needs
		// a feature this build lacks is refused and nothing is claimed.
		h, err := unmarshalHead(plain)
		if err != nil {
			return nil, fmt.Errorf("vfs: head: %w", err)
		}
		// The copy is checked whichever way the Head was opened: a Head sealed to the old
		// place says nothing about whether the objects it names came along.
		w.adopt(h)
		if err := w.checkTree(ctx, cfg.Verify); err != nil {
			return nil, fmt.Errorf("vfs: the copy at %s is not whole: %w", here, err)
		}
		if sealedHere {
			return payload, nil // left as it is: only the Round is claimed
		}
		return seal(cfg.Cipher, headAAD(here), plain)
	})
}

// Stats is a snapshot of a Volume for observability.
type Stats struct {
	Backend    string
	Round      uint64 // 0 when read-only
	Seq        uint64 // the Seq of the committed Head: it counts commit attempts and can skip, so it is not a count of commits
	Inodes     uint64 // inode numbers handed out
	CacheBytes int64
	CacheMax   int64
	Pending    int // metadata changes not yet written into shards
	Dirty      int64
}

func (v *Volume) Stats() Stats {
	v.mu.RLock()
	defer v.mu.RUnlock()
	cs, cm := v.chunks.stats()
	ss, sm := v.t.sh.cache.stats()
	return Stats{
		Backend: v.be.String(), Round: v.round, Seq: v.head.Seq, Inodes: uint64(v.next) - 1,
		CacheBytes: cs + ss, CacheMax: cm + sm, Pending: v.over.n, Dirty: v.dirty,
	}
}

// Close releases the backend. It does not Flush: changes not flushed are lost.
func (v *Volume) Close() error { return v.be.Close() }

// writable reports why v cannot write, if it cannot. mu must be held.
func (v *Volume) writable() error {
	switch {
	case v.round == 0:
		return ErrReadOnly
	case v.fenced:
		return ErrStaleRound
	}
	return nil
}

// Flush makes everything written so far durable: it returns only after the Head
// naming it has advanced in the store. If it returns an error the outcome of that
// attempt is unknown to the caller (a reply can be lost after the write landed),
// the changes stay in memory for the next Flush, and the Volume is unchanged
// otherwise. ErrStaleRound means this Volume was fenced and can never commit
// again; see the package documentation for what that does and does not say about
// earlier attempts.
func (v *Volume) Flush(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.writable(); err != nil {
		return err
	}
	if err := v.sweep(ctx); err != nil {
		return err
	}
	if v.over.n == 0 && v.pack.empty() && v.root == v.head.Root {
		return nil // the Head already names everything
	}
	if err := v.pack.upload(ctx, v.be); err != nil {
		return err
	}
	if err := v.spill(ctx); err != nil {
		return err
	}
	v.seq++
	h := v.head
	h.Seq, h.Root, h.Next = v.seq, v.root, uint64(v.next)
	v.ns.stamp(&h)
	if err := v.writeHead(ctx, h); err != nil {
		if errors.Is(err, ErrStaleRound) {
			v.fenced = true
			// An earlier attempt of ours may have landed with its reply lost, and a
			// successor carried it forward: then the store holds exactly this tree.
			if cur, ok := v.landed(ctx); ok {
				v.head = cur
				return nil
			}
		}
		return fmt.Errorf("vfs: commit: %w", err)
	}
	v.head = h
	return nil
}

// landed reports the Head in the store if it names exactly this Volume's tree.
func (v *Volume) landed(ctx context.Context) (Head, bool) {
	payload, _, err := v.fence.Get(ctx, headKey)
	if err != nil {
		return Head{}, false
	}
	cur, err := v.readHead(payload)
	return cur, err == nil && cur.ID == v.id && cur.Root == v.root
}

// spill writes the pending metadata changes into shards, advancing the working
// root. The Head is untouched, so the shards stay garbage until a commit names them.
func (v *Volume) spill(ctx context.Context) error {
	if v.over.n == 0 {
		return nil
	}
	r, err := v.t.apply(ctx, v.root, v.over.drain())
	if err != nil {
		return err
	}
	v.root = r
	v.over.reset()
	return nil
}

// relieve bounds memory after a change: it chunks and uploads written file
// bytes, and writes pending metadata into shards, once either passes its limit.
func (v *Volume) relieve(ctx context.Context) error {
	if v.dirty >= v.cfg.Dirty {
		if err := v.sweep(ctx); err != nil {
			return err
		}
	}
	if v.over.n >= v.cfg.Overlay {
		return v.spill(ctx)
	}
	return nil
}

// get returns the current value of a record: a pending change, else the tree's.
func (v *Volume) get(ctx context.Context, k []byte) ([]byte, bool, error) {
	if n, ok := v.over.get(k); ok {
		return n.val, !n.del, nil
	}
	return v.t.get(ctx, v.root, k)
}

// scan returns the current records whose key starts with prefix, in key order.
func (v *Volume) scan(ctx context.Context, prefix []byte) ([]entry, error) {
	var base []entry
	if err := v.t.scan(ctx, v.root, prefix, func(k, val []byte) bool {
		base = append(base, entry{k, val})
		return true
	}); err != nil {
		return nil, err
	}
	out := make([]entry, 0, len(base))
	i := 0
	for n := v.over.seek(prefix); n != nil && bytes.HasPrefix(n.key, prefix); n = n.next[0] {
		for i < len(base) && bytes.Compare(base[i].key, n.key) < 0 {
			out = append(out, base[i])
			i++
		}
		if i < len(base) && bytes.Equal(base[i].key, n.key) {
			i++
		}
		if !n.del {
			out = append(out, entry{n.key, n.val})
		}
	}
	return append(out, base[i:]...), nil
}

// has reports whether any record whose key starts with prefix exists, reading no
// more than it takes to find one: a directory is checked for emptiness without
// listing it.
func (v *Volume) has(ctx context.Context, prefix []byte) (bool, error) {
	for n := v.over.seek(prefix); n != nil && bytes.HasPrefix(n.key, prefix); n = n.next[0] {
		if !n.del {
			return true, nil
		}
	}
	found := false
	err := v.t.scan(ctx, v.root, prefix, func(k, _ []byte) bool {
		if n, ok := v.over.get(k); ok && n.del {
			return true // removed by a pending change
		}
		found = true
		return false
	})
	return found, err
}
