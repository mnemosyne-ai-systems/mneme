package api

import (
	"bytes"
	"embed"
	"net/http"
)

//go:embed admin_ui.html
var adminUI embed.FS

// AdminUIHandler 提供嵌入式后台管理页面（无需独立前端构建）。
type AdminUIHandler struct {
	title string
}

// NewAdminUIHandler 创建页面 handler；title 为空时使用默认标题。
func NewAdminUIHandler(title string) *AdminUIHandler {
	return &AdminUIHandler{title: title}
}

// ServeHTTP 返回后台管理 HTML 页面。
func (h *AdminUIHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	raw, err := adminUI.ReadFile("admin_ui.html")
	if err != nil {
		http.Error(w, "admin ui not found", http.StatusInternalServerError)
		return
	}
	html := raw
	if h.title != "" {
		html = bytes.ReplaceAll(raw, []byte("Mneme 管理后台"), []byte(h.title))
	}
	_, _ = w.Write(html)
}
