package storage

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

type Offloader struct {
	s3Endpoint  string
	s3Bucket    string
	accessToken string
}

func NewOffloader(endpoint, bucket, token string) *Offloader {
	return &Offloader{
		s3Endpoint:  strings.TrimSuffix(endpoint, "/"),
		s3Bucket:    bucket,
		accessToken: token,
	}
}

func (o *Offloader) OffloadSegment(closedPath string) error {
	file, err := os.Open(closedPath)
	if err != nil {
		return fmt.Errorf("offloader: failed to open segment: %w", err)
	}
	defer file.Close()

	// Upload to S3/ServStore: PUT /{bucket}/wal/{segment_name}
	parts := strings.Split(closedPath, "/")
	filename := parts[len(parts)-1]

	url := fmt.Sprintf("%s/%s/wal/%s", o.s3Endpoint, o.s3Bucket, filename)
	
	data, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("PUT", url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if o.accessToken != "" {
		req.Header.Set("Authorization", "Bearer "+o.accessToken)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("offloader: upload failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("offloader: upload returned HTTP status %d", resp.StatusCode)
	}

	// Remove local file once successfully uploaded to cold storage
	_ = os.Remove(closedPath)
	return nil
}
