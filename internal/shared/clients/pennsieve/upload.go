package pennsieve

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const partSize = 64 * 1024 * 1024 // 64 MB per part

// chunkLogInterval controls progress logging during chunk upload. A
// per-chunk line is untenable at this scale: a full run emits ~210k
// chunks, and the workflow finalizer loads every log event of every
// task into memory to archive it. Logging one line per N completions
// keeps that bounded while preserving progress visibility.
const chunkLogInterval = 100

// ChunkUpload pairs a chunk file on disk with the key it should take in
// S3. The two differ: the writer stages chunks under an intra-processor
// channel ordinal ("channel-00007_..."), while the streaming side looks
// them up by channel node id ("N:channel:..._..."). Carrying the target
// key here lets the upload apply that mapping without renaming ~210k
// files on EFS first — each rename is an NFS round-trip, and doing them
// serially cost ~29 minutes of the credential lifetime.
type ChunkUpload struct {
	// LocalPath is the source file on disk, under its staged name.
	LocalPath string
	// RelativeKey is the chunk's key under the asset's prefix — for
	// chunks this is just a basename (no nested dirs). What gets
	// passed to timeseries-service via RangeChunk.S3Key.
	RelativeKey string
}

// ChunkUploadResult is the per-file outcome of an UploadChunks call.
// Used by the time-series flow to register ranges keyed by the chunk's
// relative S3 key.
type ChunkUploadResult struct {
	// LocalPath is the source file on disk.
	LocalPath string
	// RelativeKey is the chunk's path under the asset's prefix — for
	// chunks this is just the basename (no nested dirs). What gets
	// passed to timeseries-service via RangeChunk.S3Key.
	RelativeKey string
	// FullKey is the bucket-relative key (creds.KeyPrefix + RelativeKey).
	FullKey string
}

// UploadChunks uploads time-series chunk files to S3 using STS
// credentials scoped to the asset prefix. Each chunk takes the key
// carried on its ChunkUpload — chunks always live flat under the asset
// prefix per timeseries-service's expectations. Returns one result per
// input chunk, in input order.
//
// Uploads run concurrently, bounded by concurrency (<=0 selects a
// single worker). Each PUT is ~1 MiB and dominated by round-trip
// latency, so the serial version left the connection idle most of the
// time; overlapping them is what keeps a full run inside the lifetime
// of the STS credentials minted at asset creation.
func UploadChunks(ctx context.Context, creds *UploadCredentials, chunks []ChunkUpload, concurrency int) ([]ChunkUploadResult, error) {
	region := creds.Region
	if region == "" {
		region = "us-east-1"
	}
	if concurrency <= 0 {
		concurrency = 1
	}

	expiration, _ := time.Parse(time.RFC3339, creds.Expiration)
	slog.Info("upload credentials loaded", "expiresAt", expiration.Format("15:04:05"))

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			creds.AccessKeyID,
			creds.SecretAccessKey,
			creds.SessionToken,
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("creating S3 config: %w", err)
	}
	s3Client := s3.NewFromConfig(awsCfg)

	slog.Info("uploading chunks", "total", len(chunks),
		"concurrency", concurrency, "bucket", creds.Bucket, "keyPrefix", creds.KeyPrefix)

	return uploadChunksWith(ctx, chunks, creds.KeyPrefix, concurrency,
		func(ctx context.Context, fullKey, localPath string) error {
			return putChunk(ctx, s3Client, creds.Bucket, fullKey, localPath)
		})
}

// putFunc uploads one file to one key. Split out so the fan-out below
// can be exercised without reaching S3.
type putFunc func(ctx context.Context, fullKey, localPath string) error

// uploadChunksWith runs put over chunks with bounded concurrency,
// returning results in input order. The first failure cancels the rest.
func uploadChunksWith(ctx context.Context, chunks []ChunkUpload, keyPrefix string, concurrency int, put putFunc) ([]ChunkUploadResult, error) {
	// Clamp here rather than trusting the caller: a zero would make the
	// semaphore unbuffered and deadlock the dispatch loop.
	if concurrency <= 0 {
		concurrency = 1
	}

	// Results are written by index, never appended: goroutines finish
	// out of order, and buildRangeChunks pairs each result back to its
	// source chunk positionally.
	results := make([]ChunkUploadResult, len(chunks))

	// Cancel in-flight uploads as soon as any one fails, so a failure
	// at chunk 10 doesn't wait on 31 siblings still pushing bytes.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg       sync.WaitGroup
		sem      = make(chan struct{}, concurrency)
		done     atomic.Int64
		errOnce  sync.Once
		firstErr error
	)

	fail := func(err error) {
		errOnce.Do(func() {
			firstErr = err
			cancel()
		})
	}

	for i, chunk := range chunks {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			// Another worker already failed; stop dispatching.
		}
		if ctx.Err() != nil {
			break
		}

		wg.Add(1)
		go func(i int, chunk ChunkUpload) {
			defer wg.Done()
			defer func() { <-sem }()

			fullKey := keyPrefix + chunk.RelativeKey
			if err := put(ctx, fullKey, chunk.LocalPath); err != nil {
				// A cancelled sibling upload is a symptom of the
				// original failure, not a new one worth reporting.
				if ctx.Err() == nil {
					fail(fmt.Errorf("uploading %s: %w", chunk.RelativeKey, err))
				}
				return
			}

			results[i] = ChunkUploadResult{
				LocalPath:   chunk.LocalPath,
				RelativeKey: chunk.RelativeKey,
				FullKey:     fullKey,
			}

			// Count completions rather than loop index: with workers
			// in flight the index is no longer monotonic, so it would
			// make progress logs jump around.
			if n := done.Add(1); n%chunkLogInterval == 0 || int(n) == len(chunks) {
				slog.Info("uploaded chunks", "done", n, "total", len(chunks))
			}
		}(i, chunk)
	}

	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("chunk upload cancelled: %w", err)
	}
	return results, nil
}

// putChunk uploads a single chunk file. Chunks are ~1 MiB, so this is
// always a single-part PUT — the multipart manager would only add
// bookkeeping.
func putChunk(ctx context.Context, client *s3.Client, bucket, key, localPath string) error {
	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("opening %s: %w", localPath, err)
	}
	defer file.Close()

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		Body:              file,
		ChecksumAlgorithm: s3types.ChecksumAlgorithmSha256,
	})
	return err
}

// UploadFiles uploads all files to S3 using temporary credentials scoped
// to the asset prefix. Each file is uploaded with its path relative to
// inputDir appended to creds.KeyPrefix.
func UploadFiles(ctx context.Context, creds *UploadCredentials, files []string, inputDir string) error {
	region := creds.Region
	if region == "" {
		region = "us-east-1"
	}

	expiration, _ := time.Parse(time.RFC3339, creds.Expiration)
	slog.Info("upload credentials loaded", "expiresAt", expiration.Format("15:04:05"))

	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			creds.AccessKeyID,
			creds.SecretAccessKey,
			creds.SessionToken,
		)),
	)
	if err != nil {
		return fmt.Errorf("creating S3 config: %w", err)
	}

	s3Client := s3.NewFromConfig(cfg)
	uploader := manager.NewUploader(s3Client, func(u *manager.Uploader) {
		u.PartSize = partSize
	})

	for i, localPath := range files {
		rel, _ := filepath.Rel(inputDir, localPath)
		s3Key := creds.KeyPrefix + rel
		slog.Info("uploading file", "index", i+1, "total", len(files), "file", rel, "bucket", creds.Bucket, "key", s3Key)

		file, err := os.Open(localPath)
		if err != nil {
			return fmt.Errorf("opening %s: %w", localPath, err)
		}

		_, err = uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket:            aws.String(creds.Bucket),
			Key:               aws.String(s3Key),
			Body:              file,
			ChecksumAlgorithm: s3types.ChecksumAlgorithmSha256,
		})
		file.Close()
		if err != nil {
			return fmt.Errorf("uploading %s: %w", rel, err)
		}
	}

	return nil
}
