package s3

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

type Client struct {
	endpoint   string
	bucket     string
	httpClient *http.Client
}

type ListBucketResult struct {
	XMLName  xml.Name `xml:"ListBucketResult"`
	KeyCount int      `xml:"KeyCount"`
}

type CheckResult struct {
	Exists   bool   `json:"exists"`
	KeyCount int    `json:"keyCount"`
	Error    string `json:"error,omitempty"`
}

func NewClient(endpoint, bucket string, httpClient *http.Client) *Client {
	return &Client{
		endpoint:   endpoint,
		bucket:     bucket,
		httpClient: httpClient,
	}
}

func (c *Client) CheckBackupExists(ctx context.Context, namespace, pvc string) CheckResult {
	prefix := fmt.Sprintf("%s/%s/", namespace, pvc)

	reqURL := fmt.Sprintf("%s/%s?list-type=2&prefix=%s&max-keys=1",
		c.endpoint,
		c.bucket,
		url.QueryEscape(prefix))

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return CheckResult{Exists: false, Error: fmt.Sprintf("failed to create request: %v", err)}
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return CheckResult{Exists: false, Error: fmt.Sprintf("failed to query S3: %v", err)}
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return CheckResult{
			Exists: false,
			Error:  fmt.Sprintf("S3 returned status %d: %s", resp.StatusCode, string(body)),
		}
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return CheckResult{Exists: false, Error: fmt.Sprintf("failed to read response: %v", err)}
	}

	var result ListBucketResult
	if err := xml.Unmarshal(body, &result); err != nil {
		return CheckResult{Exists: false, Error: fmt.Sprintf("failed to parse XML: %v", err)}
	}

	return CheckResult{
		Exists:   result.KeyCount > 0,
		KeyCount: result.KeyCount,
	}
}
