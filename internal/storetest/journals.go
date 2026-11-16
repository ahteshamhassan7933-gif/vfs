package storetest

import (
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/hanzoai/vfs/journal"
)

var (
	journalsOnce sync.Once
	journalsDir  string
)

// Journals returns a directory for the journals of the Volumes of a test binary: the
// operating system's temporary directory, or, where that is memory (the journal refuses
// it), a directory beside the tests. StopReal removes it. A Volume's journal is removed
// when the Volume is closed with nothing unpublished; the rest is what a test left.
func Journals() string {
	journalsOnce.Do(func() {
		dir, err := os.MkdirTemp("", "vfs-journals-")
		if err != nil {
			panic(err)
		}
		probe, err := journal.Files(filepath.Join(dir, "probe"))
		if err == nil {
			probe.Close()
			os.Remove(filepath.Join(dir, "probe"))
			journalsDir = dir
			return
		}
		os.RemoveAll(dir)
		if !errors.Is(err, journal.ErrVolatile) {
			panic(err)
		}
		wd, err := os.Getwd()
		if err != nil {
			panic(err)
		}
		if journalsDir, err = os.MkdirTemp(wd, ".scratch-journals-"); err != nil {
			panic(err)
		}
	})
	return journalsDir
}
