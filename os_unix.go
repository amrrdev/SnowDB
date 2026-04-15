//go:build unix

package db0103

import (
	"os"
	"path"
	"syscall"
)

func createFileSync(file string) (*os.File, error) {
	fp, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	if err = syncDir(path.Base(file)); err != nil {
		fp.Close()
		return nil, err
	}

	return fp, nil
}

func syncDir(file string) error {
	flags := os.O_RDONLY | syscall.O_DIRECTORY
	dirfd, err := syscall.Open(path.Dir(file), flags, 0o644)
	if err != nil {
		return err
	}

	defer syscall.Close(dirfd)
	return syscall.Fsync(dirfd)
}
