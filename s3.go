package desync

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	minio "github.com/minio/minio-go/v6"
	"github.com/minio/minio-go/v6/pkg/credentials"
	"github.com/pkg/errors"
)

var _ WriteStore = S3Store{}

const maxS3InvalidChunkRetry = 2

// S3StoreBase is the base object for all chunk and index stores with S3 backing
type S3StoreBase struct {
	Location   string
	client     *minio.Client
	bucket     string
	prefix     string
	opt        StoreOptions
	converters Converters
}

// S3Store is a read-write store with S3 backing
type S3Store struct {
	S3StoreBase
}

// NewS3StoreBase initializes a base object used for chunk or index stores backed by S3.
func NewS3StoreBase(u *url.URL, s3Creds *credentials.Credentials, region string, opt StoreOptions, lookupType minio.BucketLookupType) (S3StoreBase, error) {
	var err error
	s := S3StoreBase{Location: u.String(), opt: opt, converters: opt.converters()}
	if !strings.HasPrefix(u.Scheme, "s3+http") {
		return s, fmt.Errorf("invalid scheme '%s', expected 's3+http' or 's3+https'", u.Scheme)
	}
	var useSSL bool
	if strings.Contains(u.Scheme, "https") {
		useSSL = true
	}

	// Pull the bucket as well as the prefix from a path-style URL
	bPath := strings.Trim(u.Path, "/")
	if bPath == "" {
		return s, fmt.Errorf("expected bucket name in path of '%s'", u.Scheme)
	}
	f := strings.Split(bPath, "/")
	s.bucket = f[0]
	s.prefix = strings.Join(f[1:], "/")

	if s.prefix != "" {
		s.prefix += "/"
	}

	s.client, err = minio.NewWithOptions(u.Host, &minio.Options{
		Creds:        s3Creds,
		Secure:       useSSL,
		Region:       region,
		BucketLookup: lookupType,
	})
	if err != nil {
		return s, errors.Wrap(err, u.String())
	}
	return s, nil
}

func (s S3StoreBase) String() string {
	return s.Location
}

// Close the S3 base store. NOP operation but needed to implement the store interface.
func (s S3StoreBase) Close() error { return nil }

// NewS3Store creates a chunk store with S3 backing. The URL
// should be provided like this: s3+http://host:port/bucket
// Credentials are passed in via the environment variables S3_ACCESS_KEY
// and S3_SECRET_KEY, or via the desync config file.
func NewS3Store(location *url.URL, s3Creds *credentials.Credentials, region string, opt StoreOptions, lookupType minio.BucketLookupType) (s S3Store, e error) {
	b, err := NewS3StoreBase(location, s3Creds, region, opt, lookupType)
	if err != nil {
		return s, err
	}
	return S3Store{b}, nil
}

// GetChunk reads and returns one chunk from the store
func (s S3Store) GetChunk(id ChunkID) (*Chunk, error) {
	name := s.nameFromID(id)
	return s.getChunkFromS3(id, name, func() ([]byte, error) {
		return s.readChunkObject(id, name)
	})
}

func (s S3Store) readChunkObject(id ChunkID, name string) ([]byte, error) {
	var attempt int
retry:
	attempt++
	start := time.Now()
	obj, err := s.client.GetObject(s.bucket, name, minio.GetObjectOptions{})
	if err != nil {
		if retryS3Request(attempt, s.opt.ErrorRetry, s.opt.ErrorRetryBaseInterval) {
			goto retry
		}
		return nil, errors.Wrap(err, s.String())
	}

	b, err := io.ReadAll(obj)
	closeErr := obj.Close()
	if debugStatsActive() {
		globalDebugStats.s3GetCalls.Add(1)
		globalDebugStats.s3GetBytes.Add(int64(len(b)))
		globalDebugStats.s3GetNs.Add(debugStatsSince(start))
	}
	if err != nil {
		var retryable bool
		err, retryable = s.getChunkReadError(id, err)
		if retryable && retryS3Request(attempt, s.opt.ErrorRetry, s.opt.ErrorRetryBaseInterval) {
			goto retry
		}
		return nil, err
	}
	if closeErr != nil {
		if retryS3Request(attempt, s.opt.ErrorRetry, s.opt.ErrorRetryBaseInterval) {
			goto retry
		}
		return nil, errors.Wrapf(closeErr, "closing chunk %s after reading %d bytes from s3 store", id.String(), len(b))
	}

	return b, nil
}

func (s S3Store) getChunkFromS3(id ChunkID, name string, readObject func() ([]byte, error)) (*Chunk, error) {
	var attempt int
retry:
	attempt++
	b, err := readObject()
	if err != nil {
		return nil, err
	}
	chunk, err := NewChunkFromStorage(id, b, s.converters, s.opt.SkipVerify)
	if err != nil {
		if retryS3Request(attempt, s.invalidChunkRetryLimit(), s.opt.ErrorRetryBaseInterval) {
			if debugStatsActive() {
				globalDebugStats.s3InvalidRetries.Add(1)
			}
			goto retry
		}
		return nil, errors.Wrapf(err, "invalid chunk %s from s3 object %s after reading %d bytes", id.String(), name, len(b))
	}
	return chunk, nil
}

func retryS3Request(attempt, retryLimit int, baseInterval time.Duration) bool {
	if attempt > retryLimit {
		return false
	}
	time.Sleep(time.Duration(attempt) * baseInterval)
	return true
}

func (s S3Store) invalidChunkRetryLimit() int {
	if s.opt.ErrorRetry < maxS3InvalidChunkRetry {
		return s.opt.ErrorRetry
	}
	return maxS3InvalidChunkRetry
}

func (s S3Store) getChunkReadError(id ChunkID, err error) (error, bool) {
	var e minio.ErrorResponse
	if errors.As(err, &e) {
		switch e.Code {
		case "NoSuchBucket":
			return fmt.Errorf("bucket '%s' does not exist", s.bucket), false
		case "NoSuchKey":
			return ChunkMissing{ID: id}, false
		}
	}
	// Without ListBucket perms in AWS, a missing chunk can be returned as Permission Denied.
	return errors.Wrapf(err, "chunk %s could not be retrieved from s3 store", id.String()), true
}

// StoreChunk adds a new chunk to the store
func (s S3Store) StoreChunk(chunk *Chunk) error {
	contentType := "application/zstd"
	name := s.nameFromID(chunk.ID())
	b, err := chunk.Storage(s.converters)
	if err != nil {
		return err
	}
	var attempt int
retry:
	attempt++
	start := time.Now()
	_, err = s.client.PutObject(s.bucket, name, bytes.NewReader(b), int64(len(b)), minio.PutObjectOptions{ContentType: contentType})
	if debugStatsActive() {
		globalDebugStats.s3PutCalls.Add(1)
		globalDebugStats.s3PutBytes.Add(int64(len(b)))
		globalDebugStats.s3PutNs.Add(debugStatsSince(start))
	}
	if err != nil {
		if attempt < s.opt.ErrorRetry {
			time.Sleep(time.Duration(attempt) * s.opt.ErrorRetryBaseInterval)
			goto retry
		}
	}
	return errors.Wrap(err, s.String())
}

// HasChunk returns true if the chunk is in the store
func (s S3Store) HasChunk(id ChunkID) (bool, error) {
	name := s.nameFromID(id)
	start := time.Now()
	_, err := s.client.StatObject(s.bucket, name, minio.StatObjectOptions{})
	if debugStatsActive() {
		globalDebugStats.s3StatCalls.Add(1)
		globalDebugStats.s3StatNs.Add(debugStatsSince(start))
		if err == nil {
			globalDebugStats.s3StatHits.Add(1)
		} else {
			globalDebugStats.s3StatMisses.Add(1)
		}
	}
	return err == nil, nil
}

// RemoveChunk deletes a chunk, typically an invalid one, from the filesystem.
// Used when verifying and repairing caches.
func (s S3Store) RemoveChunk(id ChunkID) error {
	name := s.nameFromID(id)
	return s.client.RemoveObject(s.bucket, name)
}

// Prune removes any chunks from the store that are not contained in a list (map)
func (s S3Store) Prune(ctx context.Context, ids map[ChunkID]struct{}) error {
	doneCh := make(chan struct{})
	defer close(doneCh)
	objectCh := s.client.ListObjectsV2(s.bucket, s.prefix, true, doneCh)
	for object := range objectCh {
		if object.Err != nil {
			return object.Err
		}
		// See if we're meant to stop
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}

		id, err := s.idFromName(object.Key)
		if err != nil {
			continue
		}

		// Drop the chunk if it's not on the list
		if _, ok := ids[id]; !ok {
			if err = s.RemoveChunk(id); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s S3Store) nameFromID(id ChunkID) string {
	sID := id.String()
	name := s.prefix + sID[0:4] + "/" + sID
	if s.opt.Uncompressed {
		name += UncompressedChunkExt
	} else {
		name += CompressedChunkExt
	}
	return name
}

func (s S3Store) idFromName(name string) (ChunkID, error) {
	var n string
	if s.opt.Uncompressed {
		if !strings.HasSuffix(name, UncompressedChunkExt) {
			return ChunkID{}, fmt.Errorf("object %s is not a chunk", name)
		}
		n = strings.TrimSuffix(strings.TrimPrefix(name, s.prefix), UncompressedChunkExt)
	} else {
		if !strings.HasSuffix(name, CompressedChunkExt) {
			return ChunkID{}, fmt.Errorf("object %s is not a chunk", name)
		}
		n = strings.TrimSuffix(strings.TrimPrefix(name, s.prefix), CompressedChunkExt)
	}
	fragments := strings.Split(n, "/")
	if len(fragments) != 2 {
		return ChunkID{}, fmt.Errorf("incorrect chunk name for object %s", name)
	}
	idx := fragments[0]
	sid := fragments[1]
	if !strings.HasPrefix(sid, idx) {
		return ChunkID{}, fmt.Errorf("incorrect chunk name for object %s", name)
	}
	return ChunkIDFromString(sid)
}
