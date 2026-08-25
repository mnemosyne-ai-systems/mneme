package archiver

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// S3Uploader 使用 AWS SigV4 签名把文件 PUT 到 S3 / OSS 兼容存储。
// 保持零额外依赖，兼容 AWS S3、MinIO、阿里云 OSS（S3 兼容模式）等。
type S3Uploader struct {
	cfg    OSSConfig
	client *http.Client
	now    func() time.Time
}

// NewS3Uploader 创建上传器。
func NewS3Uploader(cfg OSSConfig) *S3Uploader {
	return &S3Uploader{
		cfg:    cfg,
		client: &http.Client{Timeout: 10 * time.Minute},
		now:    func() time.Time { return time.Now().UTC() },
	}
}

// Upload 上传本地文件到 remoteKey。
func (u *S3Uploader) Upload(ctx context.Context, localPath, remoteKey string) error {
	data, err := os.ReadFile(localPath)
	if err != nil {
		return fmt.Errorf("uploader: read %s: %w", localPath, err)
	}

	endpoint := strings.TrimSuffix(u.cfg.Endpoint, "/")
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		endpoint = "https://" + endpoint
	}
	key := strings.TrimPrefix(strings.ReplaceAll(remoteKey, "\\", "/"), "/")
	target := fmt.Sprintf("%s/%s/%s", endpoint, u.cfg.Bucket, key)

	parsed, err := url.Parse(target)
	if err != nil {
		return fmt.Errorf("uploader: parse url: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, target, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("uploader: build request: %w", err)
	}
	req.ContentLength = int64(len(data))
	req.Header.Set("Content-Type", "application/octet-stream")

	u.sign(req, parsed, data)

	resp, err := u.client.Do(req)
	if err != nil {
		return fmt.Errorf("uploader: put object: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("uploader: put object returned %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// sign 按 AWS Signature Version 4 为请求添加 Authorization 头。
func (u *S3Uploader) sign(req *http.Request, parsed *url.URL, payload []byte) {
	if u.cfg.AccessKey == "" || u.cfg.SecretKey == "" {
		return
	}
	now := u.now()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")
	region := u.cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	const service = "s3"

	payloadHash := sha256Hex(payload)
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	req.Header.Set("Host", parsed.Host)

	canonicalURI := parsed.EscapedPath()
	if canonicalURI == "" {
		canonicalURI = "/"
	}
	canonicalHeaders := fmt.Sprintf("host:%s\nx-amz-content-sha256:%s\nx-amz-date:%s\n",
		parsed.Host, payloadHash, amzDate)
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"

	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI,
		parsed.RawQuery,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")

	scope := strings.Join([]string{dateStamp, region, service, "aws4_request"}, "/")
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")

	kDate := hmacSHA256([]byte("AWS4"+u.cfg.SecretKey), dateStamp)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	kSigning := hmacSHA256(kService, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(kSigning, stringToSign))

	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		u.cfg.AccessKey, scope, signedHeaders, signature,
	))
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(data))
	return h.Sum(nil)
}

var _ Uploader = (*S3Uploader)(nil)
