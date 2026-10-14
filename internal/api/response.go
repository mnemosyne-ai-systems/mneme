package api

import (
	"encoding/json"
	"net/http"

	"github.com/mnemosyne-labs/mneme/pkg/logger"
	"github.com/mnemosyne-labs/mneme/pkg/models"
)

// 业务错误码。
const (
	CodeOK            = 0
	CodeBadRequest    = 40000
	CodeMissingField  = 40001
	CodeInvalidJSON   = 40002
	CodeMissingTenant = 40003
	CodeUnauthorized  = 40100
	CodeNotFound      = 40400
	CodeInternal      = 50000
	CodeUpstream      = 50200
)

// writeJSON 输出统一格式的 JSON 响应。
func writeJSON(w http.ResponseWriter, status int, body models.APIResponse) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		logger.L().WithError(err).Error("write json response failed")
	}
}

// writeOK 输出 200 成功响应。
func writeOK(w http.ResponseWriter, data interface{}) {
	writeJSON(w, http.StatusOK, models.OK(data))
}

// writeAccepted 输出 202 异步受理响应。
func writeAccepted(w http.ResponseWriter, data interface{}) {
	writeJSON(w, http.StatusAccepted, models.OK(data))
}

// writeError 输出错误响应。
func writeError(w http.ResponseWriter, status, code int, msg string) {
	writeJSON(w, status, models.Err(code, msg))
}

// badRequest 输出 400。
func badRequest(w http.ResponseWriter, code int, msg string) {
	writeError(w, http.StatusBadRequest, code, msg)
}

// internalError 输出 500。
func internalError(w http.ResponseWriter, msg string) {
	writeError(w, http.StatusInternalServerError, CodeInternal, msg)
}
