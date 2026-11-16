//go:build fuse && linux

package mount

import "syscall"

// syncfs has the kernel write back every page it holds for the file system of dir. The
// directory is opened with the system call and not with os.Open: a process must not
// register a file of its own mount with its poller.
func syncfs(dir string) error {
	fd, err := syscall.Open(dir, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer syscall.Close(fd)
	if _, _, errno := syscall.Syscall(syscall.SYS_SYNCFS, uintptr(fd), 0, 0); errno != 0 {
		return errno
	}
	return nil
}
