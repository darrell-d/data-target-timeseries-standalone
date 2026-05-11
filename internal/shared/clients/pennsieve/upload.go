package pennsieve

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const partSize = 64 * 1024 * 1024 // 64 MB per part

// ChunkUploadResult is the per-file outcome of an UploadChunks call.
// Used by the time-series flow to register ranges keyed by the chunk's
// relative S3 key.
type ChunkUploadResult struct {
	// LocalPath is the source file on disk (post-rename for chunks).
	LocalPath string
	// RelativeKey is the chunk's path under the asset's prefix — for
	// chunks this is just the basename (no nested dirs). What gets
	// passed to timeseries-service via RangeChunk.S3Key.
	RelativeKey string
	// FullKey is the bucket-relative key (creds.KeyPrefix + RelativeKey).
	FullKey string
}

// UploadChunks uploads time-series chunk files to S3 using STS
// credentials scoped to the asset prefix. Unlike UploadFiles, each
// file's relative key is derived from its basename — chunks always
// live flat under the asset prefix per timeseries-service's
// expectations. Returns one result per input file in input order.
//
// Note: inputDir is unused here; kept in the signature for symmetry
// with UploadFiles and forward compatibility with nested-key flows.
func UploadChunks(ctx context.Context, creds *UploadCredentials, files []string, inputDir string) ([]ChunkUploadResult, error) {
	_ = inputDir
	region := creds.Region
	if region == "" {
		region = "us-east-1"
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
	uploader := manager.NewUploader(s3Client, func(u *manager.Uploader) {
		u.PartSize = partSize
	})

	results := make([]ChunkUploadResult, 0, len(files))
	for i, localPath := range files {
		relKey := filepath.Base(localPath)
		fullKey := creds.KeyPrefix + relKey
		slog.Info("uploading chunk", "index", i+1, "total", len(files),
			"file", relKey, "bucket", creds.Bucket, "key", fullKey)

		file, err := os.Open(localPath)
		if err != nil {
			return nil, fmt.Errorf("opening %s: %w", localPath, err)
		}
		_, err = uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket:            aws.String(creds.Bucket),
			Key:               aws.String(fullKey),
			Body:              file,
			ChecksumAlgorithm: s3types.ChecksumAlgorithmSha256,
		})
		file.Close()
		if err != nil {
			return nil, fmt.Errorf("uploading %s: %w", relKey, err)
		}
		results = append(results, ChunkUploadResult{
			LocalPath:   localPath,
			RelativeKey: relKey,
			FullKey:     fullKey,
		})
	}

	return results, nil
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
