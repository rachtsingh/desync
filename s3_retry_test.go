package desync

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
)

func TestS3StoreGetChunkRetriesInvalidChunk(t *testing.T) {
	id, validStorage := testS3StorageChunk(t, []byte("valid chunk content"))
	bodies := [][]byte{
		[]byte("not zstd data"),
		validStorage,
	}
	var reads atomic.Int32
	store := testS3StoreWithRetry(2)

	chunk, err := store.getChunkFromS3(id, "test-object", func() ([]byte, error) {
		return bodies[int(reads.Add(1))-1], nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := reads.Load(); got != 2 {
		t.Fatalf("got %d S3 object reads, expected 2", got)
	}
	gotID := chunk.ID()
	if gotID != id {
		t.Fatalf("got chunk with id equal to %s, expected %s", gotID.String(), id.String())
	}
}

func TestS3StoreGetChunkCapsInvalidChunkRetries(t *testing.T) {
	id, validStorage := testS3StorageChunk(t, []byte("valid chunk content"))
	bodies := [][]byte{
		[]byte("not zstd data"),
		[]byte("still not zstd data"),
		[]byte("also not zstd data"),
		validStorage,
	}
	var reads atomic.Int32
	store := testS3StoreWithRetry(3)

	_, err := store.getChunkFromS3(id, "test-object", func() ([]byte, error) {
		return bodies[int(reads.Add(1))-1], nil
	})
	if err == nil {
		t.Fatal("expected invalid chunk error")
	}
	var invalid ChunkInvalid
	if !errors.As(err, &invalid) {
		t.Fatalf("got error %T, expected ChunkInvalid in error chain", err)
	}
	if got := reads.Load(); got != 3 {
		t.Fatalf("got %d S3 object reads, expected 3", got)
	}
	if strings.Contains(err.Error(), fmt.Sprint(id)) {
		t.Fatalf("error should format chunk IDs with ChunkID.String(): %s", err)
	}
	if !strings.Contains(err.Error(), id.String()) {
		t.Fatalf("error %q does not contain chunk id %s", err, id.String())
	}
}

func testS3StoreWithRetry(errorRetry int) S3Store {
	opt := StoreOptions{ErrorRetry: errorRetry}
	return S3Store{S3StoreBase{opt: opt, converters: opt.converters()}}
}

func testS3StorageChunk(t *testing.T, data []byte) (ChunkID, []byte) {
	t.Helper()

	chunk := NewChunk(data)
	opt := StoreOptions{}
	storage, err := chunk.Storage(opt.converters())
	if err != nil {
		t.Fatal(err)
	}
	return chunk.ID(), storage
}
