package ServShared

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// StoreClient handles communication with ServStore (S3-compatible distributed storage).
type StoreClient struct {
	Endpoint  string
	AuthToken string
}

// NewStoreClient returns a new client using environment variables.
func NewStoreClient() *StoreClient {
	endpoint := os.Getenv("SERV_STORE_URL")
	if endpoint == "" {
		endpoint = "http://localhost:8081"
	}
	authToken := os.Getenv("SERV_STORE_TOKEN")
	if authToken == "" {
		authToken = "gateway-secret-token"
	}
	return &StoreClient{
		Endpoint:  strings.TrimSuffix(endpoint, "/"),
		AuthToken: authToken,
	}
}

// IsolateBucket prefixes bucket name with the tenant ID from context.
func (c *StoreClient) IsolateBucket(ctx context.Context, bucket string) string {
	if tid, ok := ctx.Value(TenantContextKey).(string); ok && tid != "" && tid != "default" {
		return tid + "-" + bucket
	}
	return bucket
}

// PutCtx writes object data to a tenant-isolated bucket.
func (c *StoreClient) PutCtx(ctx context.Context, bucket, key string, data []byte) error {
	return c.Put(c.IsolateBucket(ctx, bucket), key, data)
}

// GetCtx reads object data from a tenant-isolated bucket.
func (c *StoreClient) GetCtx(ctx context.Context, bucket, key string) ([]byte, error) {
	return c.Get(c.IsolateBucket(ctx, bucket), key)
}

// EnsureBucket creates a bucket in ServStore if it does not already exist.
func (c *StoreClient) EnsureBucket(bucket string) error {
	url := fmt.Sprintf("%s/%s", c.Endpoint, bucket)
	req, err := http.NewRequest("PUT", url, nil)
	if err != nil {
		return err
	}
	if c.AuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.AuthToken)
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// Put writes object data to a bucket and key in ServStore.
func (c *StoreClient) Put(bucket, key string, data []byte) error {
	_ = c.EnsureBucket(bucket) // Ensure bucket exists first
	url := fmt.Sprintf("%s/%s/%s", c.Endpoint, bucket, key)
	req, err := http.NewRequest("PUT", url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if c.AuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.AuthToken)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to save to ServStore (%d): %s", resp.StatusCode, string(body))
	}
	return nil
}

// Get reads object data from a bucket and key in ServStore.
func (c *StoreClient) Get(bucket, key string) ([]byte, error) {
	url := fmt.Sprintf("%s/%s/%s", c.Endpoint, bucket, key)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	if c.AuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.AuthToken)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, os.ErrNotExist
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to read from ServStore (%d): %s", resp.StatusCode, string(body))
	}
	return io.ReadAll(resp.Body)
}

// SemanticSearch runs a vector similarity/semantic search query on a bucket in ServStore.
func (c *StoreClient) SemanticSearch(bucket, query string, limit int) ([]string, error) {
	searchURL := fmt.Sprintf("%s/api/v1/search?bucket=%s&q=%s&limit=%d", c.Endpoint, bucket, url.QueryEscape(query), limit)
	req, err := http.NewRequest("GET", searchURL, nil)
	if err != nil {
		return nil, err
	}
	if c.AuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.AuthToken)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("search failed (%d): %s", resp.StatusCode, string(body))
	}

	var results []string
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		return nil, err
	}
	return results, nil
}
