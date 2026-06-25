package storage

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"servqueue/pkg/otel"
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

func (o *Offloader) OffloadSegment(closedPath string) (err error) {
	// Start trace span
	span := otel.StartSpan("OffloadSegment", "")
	defer func() {
		otel.EndSpan(span, err, map[string]interface{}{
			"segment.path": closedPath,
			"s3.bucket":    o.s3Bucket,
		})
	}()

	file, err := os.Open(closedPath)
	if err != nil {
		return fmt.Errorf("offloader: failed to open segment: %w", err)
	}
	
	data, err := io.ReadAll(file)
	file.Close()
	if err != nil {
		return err
	}

	parts := strings.Split(closedPath, "/")
	filename := parts[len(parts)-1]
	url := fmt.Sprintf("%s/%s/wal/%s", o.s3Endpoint, o.s3Bucket, filename)

	req, err := http.NewRequest("PUT", url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if o.accessToken != "" {
		req.Header.Set("Authorization", "Bearer "+o.accessToken)
	}

	// Inject traceparent if span is active
	if span != nil {
		traceparent := fmt.Sprintf("00-%s-%s-01", span.TraceID, span.SpanID)
		req.Header.Set("traceparent", traceparent)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("offloader: upload failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		err = fmt.Errorf("offloader: upload returned HTTP status %d", resp.StatusCode)
		return err
	}

	// Remove local file once successfully uploaded to cold storage
	_ = os.Remove(closedPath)
	return nil
}
