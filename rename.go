package vfs

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
)

// RenameFlags are the flags of renameat2(2) that a Volume honours.
type RenameFlags uint32

const (
	// RenameNoReplace refuses a rename whose new name exists (fs.ErrExist).
	RenameNoReplace RenameFlags = 1 << 0
	// RenameExchange swaps two names that both exist; neither is removed.
	RenameExchange RenameFlags = 1 << 1
)

// Rename gives the inode named oldName in oldDir the name newName in newDir, in one atomic
// step: a crash leaves the old state or the new one, never both names, never neither.
//
// A new name that exists is replaced when both are directories (the new one must be empty)
// or neither is. Two names of the same inode are left as they are and Rename succeeds.
// A directory cannot move into itself or below itself (fs.ErrInvalid). The directories
// that gain and lose an entry take the time as their modification and change times, and so
// does the inode that moved.
func (v *Volume) Rename(ctx context.Context, oldDir InodeID, oldName string, newDir InodeID, newName string, flags RenameFlags) error {
	if err := validName(oldName); err != nil {
		return err
	}
	if err := validName(newName); err != nil {
		return err
	}
	if flags&^(RenameNoReplace|RenameExchange) != 0 || flags == RenameNoReplace|RenameExchange {
		return fmt.Errorf("%w: rename flags %#x", fs.ErrInvalid, uint32(flags))
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.writable(); err != nil {
		return err
	}
	srcID, _, err := v.entry(ctx, oldDir, oldName)
	if err != nil {
		return err
	}
	src, err := v.stat(ctx, srcID)
	if err != nil {
		return err
	}
	od, err := v.stat(ctx, oldDir)
	if err != nil {
		return err
	}
	nd := od
	if newDir != oldDir {
		if nd, err = v.stat(ctx, newDir); err != nil {
			return err
		}
	}
	if !od.IsDir() || !nd.IsDir() {
		return fmt.Errorf("%w: a rename between inodes %d and %d", ErrNotDir, oldDir, newDir)
	}
	dstID, _, err := v.entry(ctx, newDir, newName)
	switch {
	case err == nil:
	case flags&RenameExchange != 0:
		return err
	case errors.Is(err, fs.ErrNotExist):
		dstID = 0
	default:
		return err
	}
	if dstID != 0 && flags&RenameNoReplace != 0 {
		return fmt.Errorf("%w: %q in inode %d", fs.ErrExist, newName, newDir)
	}
	if dstID == srcID {
		return nil // two names of one inode: nothing to do
	}
	var dst Inode
	if dstID != 0 {
		if dst, err = v.stat(ctx, dstID); err != nil {
			return err
		}
	}
	// A directory must not become its own ancestor.
	if src.IsDir() {
		if in, err := v.within(ctx, newDir, srcID); err != nil || in {
			if err == nil {
				err = fmt.Errorf("%w: directory %d cannot move into itself", fs.ErrInvalid, srcID)
			}
			return err
		}
	}
	if flags&RenameExchange != 0 && dst.IsDir() {
		if in, err := v.within(ctx, oldDir, dstID); err != nil || in {
			if err == nil {
				err = fmt.Errorf("%w: directory %d cannot move into itself", fs.ErrInvalid, dstID)
			}
			return err
		}
	}
	now := v.Now()
	switch {
	case dstID == 0:
	case flags&RenameExchange != 0:
	case src.IsDir() && !dst.IsDir():
		return fmt.Errorf("%w: %q is not a directory", ErrNotDir, newName)
	case !src.IsDir() && dst.IsDir():
		return fmt.Errorf("%w: %q is a directory", ErrIsDir, newName)
	default:
		if err := v.drop(ctx, dst, now); err != nil { // fails, changing nothing, for a directory that is not empty
			return err
		}
	}

	// Everything is read; what follows only stages, so a rename is staged whole or not at all.
	v.over.put(keyEntry(newDir, newName), encodeEntry(srcID, src.Mode))
	if flags&RenameExchange != 0 {
		v.over.put(keyEntry(oldDir, oldName), encodeEntry(dstID, dst.Mode))
		dst.Ctime = now
		if dst.IsDir() {
			dst.Parent = oldDir
		}
		v.set(dst)
	} else {
		v.over.del(keyEntry(oldDir, oldName))
	}
	src.Ctime = now
	if src.IsDir() {
		src.Parent = newDir
	}
	v.set(src)
	od.Mtime, od.Ctime = now, now
	v.set(od)
	if newDir != oldDir {
		nd.Mtime, nd.Ctime = now, now
		v.set(nd)
	}
	// each directory's subdirectories: the new name gains the inode that moved, and loses
	// what it replaced; the old name loses the one and, in an exchange, gains the other
	if src.IsDir() {
		v.dirn.bump(oldDir, -1)
		v.dirn.bump(newDir, +1)
	}
	if dst.IsDir() {
		v.dirn.bump(newDir, -1)
		if flags&RenameExchange != 0 {
			v.dirn.bump(oldDir, +1)
		}
	}
	return v.relieve(ctx)
}

// within reports whether dir is the directory anc or lies below it, following the parent of
// each directory up to the root.
func (v *Volume) within(ctx context.Context, dir, anc InodeID) (bool, error) {
	for range 1 << 16 {
		if dir == anc {
			return true, nil
		}
		if dir == RootInode {
			return false, nil
		}
		in, err := v.stat(ctx, dir)
		if err != nil {
			return false, err
		}
		if in.Parent == 0 {
			return false, fmt.Errorf("%w: directory %d has no parent", ErrCorrupt, dir)
		}
		dir = in.Parent
	}
	return false, fmt.Errorf("%w: directories nest past %d levels, or in a loop", ErrCorrupt, 1<<16)
}
