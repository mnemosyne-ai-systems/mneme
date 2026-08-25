package models

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// APIResponse 是所有 HTTP 接口的统一响应体。
type APIResponse struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// OK 构造成功响应。
func OK(data interface{}) APIResponse {
	return APIResponse{Code: 0, Message: "ok", Data: data}
}

// Err 构造错误响应。
func Err(code int, message string) APIResponse {
	return APIResponse{Code: code, Message: message}
}

// ValidationError 表示模型校验失败。
type ValidationError struct {
	Field  string `json:"field"`
	Reason string `json:"reason"`
}

// NewValidationError 创建一个校验错误。
func NewValidationError(field, reason string) *ValidationError {
	return &ValidationError{Field: field, Reason: reason}
}

// Error 实现 error 接口。
func (e *ValidationError) Error() string {
	return fmt.Sprintf("invalid field %q: %s", e.Field, e.Reason)
}

// NewID 生成一个 32 位十六进制随机 ID。
func NewID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return ""
	}
	return hex.EncodeToString(b)
}
