package s3

import (
	"context"
	"fmt"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/mitchross/pvc-plumber/internal/backend"
)

type Client struct {
	minioClient *minio.Client
	bucket      string
}

func NewClient(endpoint, bucket, accessKey, secretKey string, secure bool) (*Client, error) {
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: secure,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}

	return &Client{
		minioClient: minioClient,
		bucket:      bucket,
	}, nil
}

func (c *Client) CheckBackupExists(ctx context.Context, namespace, pvc string) backend.CheckResult {
	prefix := fmt.Sprintf("%s/%s/", namespace, pvc)

	opts := minio.ListObjectsOptions{
		Prefix:  prefix,
		MaxKeys: 1,
	}

	objectCh := c.minioClient.ListObjects(ctx, c.bucket, opts)

	// Check if any object exists (MaxKeys=1, so at most one result)
	object, ok := <-objectCh
	if !ok {
		// Channel closed with no objects
		return backend.CheckResult{
			Exists:    false,
			Namespace: namespace,
			Pvc:       pvc,
			Backend:   "s3",
		}
	}

	if object.Err != nil {
		return backend.CheckResult{
			Exists:    false,
			Namespace: namespace,
			Pvc:       pvc,
			Backend:   "s3",
			Error:     fmt.Sprintf("failed to list objects: %v", object.Err),
		}
	}

	return backend.CheckResult{
		Exists:    true,
		Namespace: namespace,
		Pvc:       pvc,
		Backend:   "s3",
	}
}
