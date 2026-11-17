package vfs

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"io/fs"
	"slices"
	"sort"
	"time"
)

// small is the largest file stored whole in a metadata record. Larger files are
// chunked. A file's form is a function of its size alone, so a given content has
// exactly one representation.
const small = 16 << 10

// MaxSize is the largest a file can be: 4 EiB, far past anything a store holds,
// chosen so offsets and lengths never overflow a signed 64-bit integer. A file
// may be declared much larger than the bytes written to it. What it costs follows
// the runs of bytes written, not its size: a hole has no chunks, no records and no
// memory.
const MaxSize = 1 << 62

// spanCost is what each separate unsettled write is charged beyond its bytes: the
// slice header, the list slot and the allocation it occupies. Without it a million
// one-byte writes would be accounted as a megabyte and cost a hundred.
const spanCost = 64

// fileState is one file's content as the Volume holds it: the settled bytes
// (inline, or a list of chunks) plus the writes made since, which are chunked,
// sealed and uploaded together at the next settle. All handles to a file share
// one fileState, so every handle sees every write at once.
type fileState struct {
	ino  InodeID
	refs int // open handles

	// orphan is a file that was unlinked while a handle held it: no name reaches it, the
	// tree holds nothing of it, and it lives in memory (attr is its inode) until the last
	// handle closes.
	orphan bool
	attr   Inode

	// The settled content: inline bytes, or chunks sorted by offset. Chunks need
	// not touch: where none covers a byte, the byte is a hole and reads as zero.
	inline []byte
	ext    []extent
	base   uint64

	// Writes since. size is the file's length now. limit is how much of the
	// settled content still counts: truncating below it voids the rest, so a
	// later extension reads zeros, not the old bytes. Bytes in [limit, size)
	// that no write covers are a hole too.
	size    uint64
	limit   uint64
	mtime   time.Time
	ctime   time.Time
	changed bool
	dirty   []span // sorted, disjoint, never touching
	dirtyN  int64  // bytes plus spanCost per span

	// void is the ranges of the settled content that were punched out: sorted, disjoint,
	// never touching. They read as zero and cost nothing once settled. A write over one
	// is a write: voids hide settled bytes, never written ones.
	void []run
}

// span is a run of bytes written at an offset.
type span struct {
	off  uint64
	data []byte
}

func (s span) end() uint64 { return s.off + uint64(len(s.data)) }

// File is an open file. Its methods are safe for concurrent use.
type File struct {
	v *Volume
	f *fileState
}

// Open opens a regular file for reading and writing. Writes are visible to every
// handle at once and durable at the next Flush.
func (v *Volume) Open(ctx context.Context, ino InodeID) (*File, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	f, err := v.state(ctx, ino)
	if err != nil {
		return nil, err
	}
	if f.orphan {
		return nil, fmt.Errorf("%w: inode %d was unlinked", fs.ErrNotExist, ino)
	}
	f.refs++
	return &File{v: v, f: f}, nil
}

// state returns the file's state, loading it from the tree on first use.
func (v *Volume) state(ctx context.Context, ino InodeID) (*fileState, error) {
	if f := v.files[ino]; f != nil {
		return f, nil
	}
	in, err := v.stat(ctx, ino)
	if err != nil {
		return nil, err
	}
	switch {
	case in.IsDir():
		return nil, fmt.Errorf("%w: inode %d", ErrIsDir, ino)
	case in.Mode&fs.ModeType != 0:
		return nil, fmt.Errorf("%w: inode %d is not a regular file", fs.ErrInvalid, ino)
	}
	recs, err := v.scan(ctx, key(tData, uint64(ino)))
	if err != nil {
		return nil, err
	}
	f := &fileState{ino: ino, base: in.Size, size: in.Size, limit: in.Size, mtime: in.Mtime, ctime: in.Ctime}
	for _, r := range recs {
		if len(r.key) == prefixLen {
			f.inline = r.val
			continue
		}
		e, err := decodeExtent(offsetOf(r.key), r.val)
		if err != nil {
			return nil, err
		}
		f.ext = append(f.ext, e)
	}
	v.files[ino] = f
	return f, nil
}

// Stat returns the file's attributes, including writes not yet flushed.
func (h *File) Stat(ctx context.Context) (Inode, error) {
	h.v.mu.RLock()
	defer h.v.mu.RUnlock()
	return h.v.stat(ctx, h.f.ino)
}

// ReadAt reads up to len(p) bytes at off. Like io.ReaderAt it returns io.EOF
// with the bytes it has when the file ends inside the range.
func (h *File) ReadAt(ctx context.Context, p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("%w: negative offset %d", fs.ErrInvalid, off)
	}
	h.v.mu.RLock()
	defer h.v.mu.RUnlock()
	return h.v.read(ctx, h.f, p, uint64(off))
}

// read fills p from the file at off: settled content below limit, zeros above
// it, then the unsettled writes over both. v.mu is held.
func (v *Volume) read(ctx context.Context, f *fileState, p []byte, off uint64) (int, error) {
	if off >= f.size {
		return 0, io.EOF
	}
	n := min(uint64(len(p)), f.size-off)
	end := off + n
	if lim := min(f.limit, end); off < lim {
		if err := v.readBase(ctx, f, p[:lim-off], off); err != nil {
			return 0, err
		}
		f.clearVoid(p[:lim-off], off)
		clear(p[lim-off : n])
	} else {
		clear(p[:n])
	}
	for i := sort.Search(len(f.dirty), func(i int) bool { return f.dirty[i].end() > off }); i < len(f.dirty) && f.dirty[i].off < end; i++ {
		s := f.dirty[i]
		lo, hi := max(s.off, off), min(s.end(), end)
		copy(p[lo-off:hi-off], s.data[lo-s.off:hi-s.off])
	}
	if n < uint64(len(p)) {
		return int(n), io.EOF
	}
	return int(n), nil
}

// readBase copies settled bytes [off, off+len(p)) into p; a byte no chunk covers
// is a hole and reads as zero.
func (v *Volume) readBase(ctx context.Context, f *fileState, p []byte, off uint64) error {
	if f.ext == nil {
		n := 0
		if off < uint64(len(f.inline)) {
			n = copy(p, f.inline[off:])
		}
		clear(p[n:])
		return nil
	}
	i := sort.Search(len(f.ext), func(i int) bool { return f.ext[i].end() > off })
	for len(p) > 0 {
		if i == len(f.ext) || f.ext[i].off > off {
			hole := uint64(len(p))
			if i < len(f.ext) {
				hole = min(hole, f.ext[i].off-off)
			}
			clear(p[:hole])
			p, off = p[hole:], off+hole
			continue
		}
		e := f.ext[i]
		plain, err := v.getChunk(ctx, e.id)
		if err != nil {
			return err
		}
		n := copy(p, plain[off-e.off:])
		p, off, i = p[n:], off+uint64(n), i+1
	}
	return nil
}

// WriteAt writes p at off, extending the file if needed. The bytes are held in
// memory until a settle chunks and uploads them; they are durable after Flush.
func (h *File) WriteAt(ctx context.Context, p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("%w: negative offset %d", fs.ErrInvalid, off)
	}
	if len(p) == 0 {
		return 0, nil
	}
	if uint64(off)+uint64(len(p)) > MaxSize {
		return 0, fmt.Errorf("%w: a write to %d", ErrTooBig, uint64(off)+uint64(len(p)))
	}
	v := h.v
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.writable(); err != nil {
		return 0, err
	}
	f := h.f
	if end := uint64(off) + uint64(len(p)); v.cfg.Capacity > 0 && end > f.size {
		if grow := end - max(uint64(off), f.size); v.used()+int64(grow) > v.cfg.Capacity {
			return 0, fmt.Errorf("%w: capacity %d", ErrNoSpace, v.cfg.Capacity)
		}
	}
	v.write(f, p, uint64(off))
	f.size = max(f.size, uint64(off)+uint64(len(p)))
	f.mtime, f.changed = v.Now(), true
	f.ctime = f.mtime
	return len(p), v.relieve(ctx)
}

// write adds p at off to the file's unsettled writes, merging with the writes it
// touches so the list stays sorted, disjoint and gap-separated.
func (v *Volume) write(f *fileState, p []byte, off uint64) {
	end := off + uint64(len(p))
	d := f.dirty
	i := sort.Search(len(d), func(i int) bool { return d[i].end() >= off })
	j := i
	for j < len(d) && d[j].off <= end {
		j++
	}
	var merged span
	switch {
	case i == j:
		merged = span{off, append([]byte(nil), p...)}
	case j == i+1 && off >= d[i].off && end <= d[i].end():
		copy(d[i].data[off-d[i].off:], p) // inside one write: overwrite in place
		return
	case j == i+1 && off == d[i].end():
		merged = span{d[i].off, append(d[i].data, p...)} // append, amortized
	default:
		lo, hi := min(off, d[i].off), max(end, d[j-1].end())
		buf := make([]byte, hi-lo)
		for _, s := range d[i:j] {
			copy(buf[s.off-lo:], s.data)
		}
		copy(buf[off-lo:], p)
		merged = span{lo, buf}
	}
	grown := int64(len(merged.data)) + spanCost
	for _, s := range d[i:j] {
		grown -= int64(len(s.data)) + spanCost
	}
	f.dirty = slices.Replace(d, i, j, merged)
	f.dirtyN += grown
	v.dirty += grown
}

// Truncate sets the file's length. Shrinking discards the bytes past it for
// good; growing adds zeros.
func (h *File) Truncate(ctx context.Context, size uint64) error {
	if size > MaxSize {
		return fmt.Errorf("%w: %d bytes", ErrTooBig, size)
	}
	h.v.mu.Lock()
	defer h.v.mu.Unlock()
	if err := h.v.writable(); err != nil {
		return err
	}
	h.v.truncate(h.f, size)
	return h.v.relieve(ctx)
}

// Truncate sets the length of the file with inode ino, as File.Truncate does.
func (v *Volume) Truncate(ctx context.Context, ino InodeID, size uint64) error {
	if size > MaxSize {
		return fmt.Errorf("%w: %d bytes", ErrTooBig, size)
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.writable(); err != nil {
		return err
	}
	f, err := v.state(ctx, ino)
	if err != nil {
		return err
	}
	v.truncate(f, size)
	return v.relieve(ctx)
}

func (v *Volume) truncate(f *fileState, size uint64) {
	if size < f.size {
		f.limit = min(f.limit, size)
		var kept []span
		var n int64
		for _, s := range f.dirty {
			if s.off >= size {
				break
			}
			if s.end() > size {
				s.data = s.data[:size-s.off]
			}
			kept = append(kept, s)
			n += int64(len(s.data)) + spanCost
		}
		v.dirty += n - f.dirtyN
		f.dirty, f.dirtyN = kept, n
	}
	f.size, f.mtime, f.changed = size, v.Now(), true
	f.ctime = f.mtime
	for len(f.void) > 0 && f.void[len(f.void)-1].b > size {
		last := &f.void[len(f.void)-1]
		if last.a >= size {
			f.void = f.void[:len(f.void)-1]
		} else {
			last.b = size
		}
	}
}

// Close releases the handle. It does not Flush. The last handle of a file that was
// unlinked takes the file with it.
func (h *File) Close() error {
	h.v.mu.Lock()
	defer h.v.mu.Unlock()
	h.f.refs--
	if h.f.refs == 0 && (!h.f.changed || h.f.orphan) {
		h.v.dirty -= h.f.dirtyN
		delete(h.v.files, h.f.ino)
	}
	return nil
}

// sweep settles every changed file, then forgets files nobody holds.
func (v *Volume) sweep(ctx context.Context) error {
	for ino, f := range v.files {
		if f.changed {
			if err := v.settle(ctx, f); err != nil {
				return err
			}
		}
		if f.refs == 0 {
			delete(v.files, ino)
		}
	}
	return nil
}

// touched reports whether an unsettled write overlaps [a, b).
func (f *fileState) touched(a, b uint64) bool {
	i := sort.Search(len(f.dirty), func(i int) bool { return f.dirty[i].end() > a })
	return i < len(f.dirty) && f.dirty[i].off < b
}

// settle turns the file's unsettled writes into settled content: it re-cuts the
// file into chunks, uploads the new ones, and records the file's new shape as
// pending metadata. If it fails nothing about the file has changed, and the
// writes are still held.
//
// Only the part of the file the writes touch is re-cut. Cutting is local (a cut
// depends on the bytes since the previous cut), so a settled chunk that is
// unchanged and begins where the new cutting stands is exactly a chunk the new
// cutting would produce again: it is kept, and cutting resumes after it. The
// result is the chunk list a cut of the whole file would give, found by reading
// and sealing only the chunks around the edit.
func (v *Volume) settle(ctx context.Context, f *fileState) error {
	var inline []byte
	var out []extent

	if f.size <= small {
		inline = make([]byte, f.size)
		if _, err := v.read(ctx, f, inline, 0); err != nil && err != io.EOF {
			return err
		}
	} else {
		var err error
		if out, err = v.recut(ctx, f); err != nil {
			return err
		}
	}

	if f.orphan {
		// Nothing of it is in the tree: its content settles into memory alone, which is
		// what bounds the writes it holds.
		f.inline, f.ext, f.base, f.limit, f.void = inline, out, f.size, f.size, nil
		v.dirty -= f.dirtyN
		f.dirty, f.dirtyN, f.changed = nil, 0, false
		if f.size <= small {
			f.ext = nil
		} else {
			f.inline = nil
		}
		return nil
	}
	in, err := v.stat(ctx, f.ino)
	if err != nil {
		return err
	}

	held := uint64(len(f.inline))
	for _, e := range f.ext {
		held += uint64(e.n)
	}
	now := uint64(len(inline))
	for _, e := range out {
		now += uint64(e.n)
	}
	v.ns.bytes = v.ns.bytes - min(v.ns.bytes, held) + now

	// Diff against the settled records and queue only what differs.
	if f.size <= small {
		if f.size > 0 {
			v.over.put(keyInline(f.ino), inline)
		} else {
			v.over.del(keyInline(f.ino))
		}
		for _, e := range f.ext {
			v.over.del(keyExtent(f.ino, e.off))
		}
	} else {
		v.over.del(keyInline(f.ino))
		old := make(map[uint64]extent, len(f.ext))
		for _, e := range f.ext {
			old[e.off] = e
		}
		for _, e := range out {
			if o, ok := old[e.off]; !ok || o != e {
				v.over.put(keyExtent(f.ino, e.off), encodeExtent(e))
			}
			delete(old, e.off)
		}
		for off := range old {
			v.over.del(keyExtent(f.ino, off))
		}
	}
	in.Size, in.Mtime, in.Ctime = f.size, f.mtime, f.ctime
	v.over.put(keyInode(f.ino), encodeInode(in))

	f.inline, f.ext, f.base, f.limit, f.void = inline, out, f.size, f.size, nil
	v.dirty -= f.dirtyN
	f.dirty, f.dirtyN, f.changed = nil, 0, false
	if f.size <= small {
		f.ext = nil
	} else {
		f.inline = nil
	}
	return nil
}

// run is a stretch of a file that holds written bytes: [a, b).
type run struct{ a, b uint64 }

// runs returns, in order, the stretches of the file that hold written bytes now:
// the settled chunks and inline bytes that still count, and the unsettled writes.
// Everything between them is a hole.
func (f *fileState) runs() []run {
	var rs []run
	lim := min(f.size, f.limit)
	add := func(a, b uint64) {
		if b = min(b, f.size); a < b {
			rs = append(rs, run{a, b})
		}
	}
	// settled adds a stretch of the settled content: what was punched out is not in it.
	settled := func(a, b uint64) {
		for _, r := range f.void {
			if r.b <= a {
				continue
			}
			if r.a >= b {
				break
			}
			if r.a > a {
				add(a, r.a)
			}
			if a = max(a, r.b); a >= b {
				return
			}
		}
		add(a, b)
	}
	for _, e := range f.ext {
		if e.off >= lim {
			break
		}
		settled(e.off, min(e.end(), lim))
	}
	if f.ext == nil {
		settled(0, min(uint64(len(f.inline)), lim))
	}
	for _, s := range f.dirty {
		add(s.off, s.end())
	}
	slices.SortFunc(rs, func(x, y run) int { return cmp.Compare(x.a, y.a) })
	merged := rs[:0]
	for _, r := range rs {
		if n := len(merged); n > 0 && r.a <= merged[n-1].b {
			merged[n-1].b = max(merged[n-1].b, r.b)
		} else {
			merged = append(merged, r)
		}
	}
	return merged
}

// recut returns the chunk list of the file's content now, uploading new chunks.
// It cuts each run of written bytes on its own, from the run's first byte, so
// chunks never span a hole and a hole costs nothing however long it is.
func (v *Volume) recut(ctx context.Context, f *fileState) ([]extent, error) {
	runs := f.runs()
	var widest uint64
	for _, r := range runs {
		widest = max(widest, r.b-r.a)
	}
	window := make([]byte, min(widest, maxChunk))
	old := f.ext
	var out []extent
	for _, r := range runs {
		// A settled chunk is kept if its bytes are unchanged and the cut that ended
		// it would still end it. A content cut does not depend on what follows. A
		// chunk that ended a run was cut by the run's end, so it is kept only if the
		// run still ends there.
		clean := func() bool {
			e := old[0]
			if e.end() > min(f.size, f.limit) || f.touched(e.off, e.end()) || f.voided(e.off, e.end()) {
				return false
			}
			endsRun := len(old) == 1 || old[1].off != e.end()
			return !endsRun || e.end() == r.b
		}
		for pos := r.a; pos < r.b; {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			for len(old) > 0 && old[0].off < pos {
				old = old[1:]
			}
			if len(old) > 0 && old[0].off == pos && clean() {
				out = append(out, old[0])
				pos = old[0].end()
				old = old[1:]
				continue
			}
			w := window[:min(uint64(len(window)), r.b-pos)]
			if _, err := v.read(ctx, f, w, pos); err != nil && err != io.EOF {
				return nil, err
			}
			piece := w[:cut(v.gear, w)]
			id, err := v.putChunk(ctx, piece)
			if err != nil {
				return nil, err
			}
			out = append(out, extent{off: pos, n: uint32(len(piece)), id: id})
			pos += uint64(len(piece))
		}
	}
	return out, nil
}

// voided reports whether a punched range overlaps [a, b).
func (f *fileState) voided(a, b uint64) bool {
	i := sort.Search(len(f.void), func(i int) bool { return f.void[i].b > a })
	return i < len(f.void) && f.void[i].a < b
}

// clearVoid zeroes the parts of p, the settled bytes at off, that were punched out.
func (f *fileState) clearVoid(p []byte, off uint64) {
	end := off + uint64(len(p))
	for i := sort.Search(len(f.void), func(i int) bool { return f.void[i].b > off }); i < len(f.void) && f.void[i].a < end; i++ {
		lo, hi := max(f.void[i].a, off), min(f.void[i].b, end)
		clear(p[lo-off : hi-off])
	}
}

// addVoid punches [a, b) out of the settled content.
func (f *fileState) addVoid(a, b uint64) {
	out := make([]run, 0, len(f.void)+1)
	placed := false
	for _, r := range f.void {
		switch {
		case r.b < a:
			out = append(out, r)
		case r.a > b:
			if !placed {
				out, placed = append(out, run{a, b}), true
			}
			out = append(out, r)
		default:
			a, b = min(a, r.a), max(b, r.b)
		}
	}
	if !placed {
		out = append(out, run{a, b})
	}
	f.void = out
}

// Punch deallocates [off, off+n) of the file: those bytes read as zero from now on, the
// size is unchanged, and once settled the range costs no chunk, record or object. A range
// past the end of the file is clipped to it.
func (h *File) Punch(ctx context.Context, off, n uint64) error {
	v, f := h.v, h.f
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.writable(); err != nil {
		return err
	}
	a, b := min(off, f.size), min(off+n, f.size)
	if off+n < off { // the range wraps: it reaches the end of the file
		b = f.size
	}
	if a >= b {
		return nil
	}
	var kept []span
	var bytes int64
	for _, s := range f.dirty {
		switch {
		case s.end() <= a || s.off >= b:
			kept = append(kept, s)
		default:
			if s.off < a {
				kept = append(kept, span{s.off, s.data[: a-s.off : a-s.off]})
			}
			if s.end() > b {
				kept = append(kept, span{b, s.data[b-s.off:]})
			}
		}
	}
	for _, s := range kept {
		bytes += int64(len(s.data)) + spanCost
	}
	v.dirty += bytes - f.dirtyN
	f.dirty, f.dirtyN = kept, bytes
	f.addVoid(a, b)
	f.mtime, f.changed = v.Now(), true
	f.ctime = f.mtime
	return v.relieve(ctx)
}
