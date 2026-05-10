//go:build !windows

package desync

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/pkg/xattr"
)

func TestLocalFSNoXattrsSkipsExtendedAttributes(t *testing.T) {
	base := testLocalFSWithXattr(t)

	withXattrs := testLocalFSFile(t, NewLocalFS(base, LocalFSOptions{}), "file")
	defer withXattrs.Close()
	if got := withXattrs.Xattrs["user.desync.test"]; got != "present" {
		t.Fatalf("got xattr value %q, expected present", got)
	}

	withoutXattrs := testLocalFSFile(t, NewLocalFS(base, LocalFSOptions{NoXattrs: true}), "file")
	defer withoutXattrs.Close()
	if len(withoutXattrs.Xattrs) != 0 {
		t.Fatalf("got xattrs %v, expected none", withoutXattrs.Xattrs)
	}
}

func TestTarNoXattrsOmitsXattrElements(t *testing.T) {
	base := testLocalFSWithXattr(t)

	var buf bytes.Buffer
	if err := Tar(context.Background(), &buf, NewLocalFS(base, LocalFSOptions{NoXattrs: true})); err != nil {
		t.Fatal(err)
	}

	dec := NewFormatDecoder(&buf)
	for {
		v, err := dec.Next()
		if err != nil {
			t.Fatal(err)
		}
		if v == nil {
			return
		}
		if _, ok := v.(FormatXAttr); ok {
			t.Fatal("archive contains xattr element with NoXattrs enabled")
		}
	}
}

func testLocalFSWithXattr(t *testing.T) string {
	t.Helper()

	base := t.TempDir()
	file := filepath.Join(base, "file")
	if err := os.WriteFile(file, []byte("content"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := xattr.LSet(file, "user.desync.test", []byte("present")); err != nil {
		if errors.Is(err, syscall.ENOTSUP) || errors.Is(err, syscall.EOPNOTSUPP) ||
			errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
			t.Skipf("filesystem does not support user xattrs: %v", err)
		}
		t.Fatal(err)
	}
	return base
}

func testLocalFSFile(t *testing.T, fs *LocalFS, name string) *File {
	t.Helper()

	for {
		f, err := fs.Next()
		if err == io.EOF {
			t.Fatalf("file %q not found", name)
		}
		if err != nil {
			t.Fatal(err)
		}
		if f.Name == name {
			return f
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
