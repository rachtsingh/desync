//go:build !windows
// +build !windows

package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTarCommandNoXattrs(t *testing.T) {
	out := t.TempDir()
	archive := filepath.Join(out, "tree.catar")

	cmd := newTarCommand(context.Background())
	cmd.SetArgs([]string{"--no-xattrs", archive, "testdata/tree"})
	_, err := cmd.ExecuteC()
	require.NoError(t, err)
}
