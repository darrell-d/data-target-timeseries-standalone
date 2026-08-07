package pennsieve

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testChunks builds n chunks whose keys encode their index, so a
// result's position can be checked against its content.
func testChunks(n int) []ChunkUpload {
	chunks := make([]ChunkUpload, n)
	for i := range chunks {
		chunks[i] = ChunkUpload{
			LocalPath:   fmt.Sprintf("/mnt/efs/channel-%05d_100_200.bin.gz", i),
			RelativeKey: fmt.Sprintf("N:channel:abc-%05d_100_200.bin.gz", i),
		}
	}
	return chunks
}

// TestUploadChunksPreservesOrder is the regression guard for the fan-out:
// uploads complete out of order, but results must stay index-aligned
// with the input because buildRangeChunks pairs them positionally.
func TestUploadChunksPreservesOrder(t *testing.T) {
	chunks := testChunks(200)

	// Finish in a deliberately scrambled order: later chunks return
	// fastest, so an append-based implementation would invert.
	put := func(ctx context.Context, fullKey, localPath string) error {
		i := chunkIndex(fullKey)
		if i < 0 {
			return fmt.Errorf("unparseable test key %q", fullKey)
		}
		time.Sleep(time.Duration(200-i) * 20 * time.Microsecond)
		return nil
	}

	got, err := uploadChunksWith(context.Background(), chunks, "prefix/", 16, put)
	if err != nil {
		t.Fatalf("uploadChunksWith: %v", err)
	}
	if len(got) != len(chunks) {
		t.Fatalf("len(results) = %d, want %d", len(got), len(chunks))
	}
	for i, r := range got {
		if r.RelativeKey != chunks[i].RelativeKey {
			t.Errorf("result[%d].RelativeKey = %q, want %q", i, r.RelativeKey, chunks[i].RelativeKey)
		}
		if r.LocalPath != chunks[i].LocalPath {
			t.Errorf("result[%d].LocalPath = %q, want %q", i, r.LocalPath, chunks[i].LocalPath)
		}
		if want := "prefix/" + chunks[i].RelativeKey; r.FullKey != want {
			t.Errorf("result[%d].FullKey = %q, want %q", i, r.FullKey, want)
		}
	}
}

// TestUploadChunksRespectsConcurrencyLimit guards the semaphore: S3
// allows 3,500 PUT/s per prefix, so an unbounded fan-out over ~210k
// chunks would be a self-inflicted throttle.
func TestUploadChunksRespectsConcurrencyLimit(t *testing.T) {
	const limit = 8

	var inFlight, maxSeen atomic.Int64
	put := func(ctx context.Context, fullKey, localPath string) error {
		n := inFlight.Add(1)
		for {
			old := maxSeen.Load()
			if n <= old || maxSeen.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		inFlight.Add(-1)
		return nil
	}

	if _, err := uploadChunksWith(context.Background(), testChunks(500), "p/", limit, put); err != nil {
		t.Fatalf("uploadChunksWith: %v", err)
	}

	if got := maxSeen.Load(); got > limit {
		t.Errorf("peak concurrency = %d, want <= %d", got, limit)
	}
	if got := maxSeen.Load(); got < 2 {
		t.Errorf("peak concurrency = %d, uploads did not run in parallel", got)
	}
}

// TestUploadChunksCancelsSiblingsOnError covers the failure that took
// down the real run: one PUT fails mid-flight, and the remaining work
// must stop rather than push bytes for another ~190k chunks.
func TestUploadChunksCancelsSiblingsOnError(t *testing.T) {
	wantErr := errors.New("ExpiredToken: the provided token has expired")

	var started atomic.Int64
	put := func(ctx context.Context, fullKey, localPath string) error {
		started.Add(1)
		if chunkIndex(fullKey) == 10 {
			return wantErr
		}
		// Succeed slowly rather than blocking outright: workers must
		// keep retiring so chunk 10 can acquire a slot at all. Once it
		// fails, the cancellation short-circuits everyone still queued.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
			return nil
		}
	}

	_, err := uploadChunksWith(context.Background(), testChunks(5000), "p/", 4, put)
	if err == nil {
		t.Fatal("uploadChunksWith: err = nil, want the underlying failure")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("err = %v, want it to wrap %v", err, wantErr)
	}
	// The dispatch loop should stop well short of the full set rather
	// than queueing every remaining chunk.
	if n := started.Load(); n > 1000 {
		t.Errorf("started %d uploads after failure at chunk 10; dispatch did not stop", n)
	}
}

// TestUploadChunksReportsOriginalError guards against the cancellation
// masking the real cause: siblings fail with context.Canceled, and
// surfacing that instead of ExpiredToken would send a debugger down the
// wrong path entirely.
func TestUploadChunksReportsOriginalError(t *testing.T) {
	wantErr := errors.New("AccessDenied")

	var once sync.Once
	put := func(ctx context.Context, fullKey, localPath string) error {
		var isFirst bool
		once.Do(func() { isFirst = true })
		if isFirst {
			return wantErr
		}
		<-ctx.Done()
		return ctx.Err()
	}

	_, err := uploadChunksWith(context.Background(), testChunks(100), "p/", 8, put)
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want it to wrap %v", err, wantErr)
	}
	if errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, leaked the cancellation instead of the cause", err)
	}
}

// TestUploadChunksSerialFallback covers concurrency <= 0, which must
// degrade to one worker rather than deadlocking on a zero-cap channel.
func TestUploadChunksSerialFallback(t *testing.T) {
	for _, concurrency := range []int{-1, 0, 1} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			var inFlight, maxSeen atomic.Int64
			put := func(ctx context.Context, fullKey, localPath string) error {
				n := inFlight.Add(1)
				if n > maxSeen.Load() {
					maxSeen.Store(n)
				}
				time.Sleep(time.Millisecond)
				inFlight.Add(-1)
				return nil
			}

			got, err := uploadChunksWith(context.Background(), testChunks(20), "p/", concurrency, put)
			if err != nil {
				t.Fatalf("uploadChunksWith: %v", err)
			}
			if len(got) != 20 {
				t.Fatalf("len(results) = %d, want 20", len(got))
			}
			if n := maxSeen.Load(); n != 1 {
				t.Errorf("peak concurrency = %d, want 1", n)
			}
		})
	}
}

// TestUploadChunksEmpty guards the degenerate case; an empty input must
// not hang on the wait group or return a nil-vs-empty surprise.
func TestUploadChunksEmpty(t *testing.T) {
	got, err := uploadChunksWith(context.Background(), nil, "p/", 8, func(context.Context, string, string) error {
		t.Fatal("put called for empty input")
		return nil
	})
	if err != nil {
		t.Fatalf("uploadChunksWith: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("len(results) = %d, want 0", len(got))
	}
}

// chunkIndex pulls the chunk ordinal back out of a key built by
// testChunks, or -1 if it doesn't parse. Deliberately free of
// *testing.T: it runs inside worker goroutines, where t.Fatalf does not
// stop the test and would itself be a bug.
func chunkIndex(fullKey string) int {
	_, after, ok := strings.Cut(fullKey, "abc-")
	if !ok {
		return -1
	}
	digits, _, ok := strings.Cut(after, "_")
	if !ok {
		return -1
	}
	n, err := strconv.Atoi(digits)
	if err != nil {
		return -1
	}
	return n
}
