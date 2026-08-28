package api

import (
	"encoding/json"
	"net/http"
)

// openAPISpec 描述 Mneme 对外 HTTP 接口的轻量 OpenAPI 3 文档。
// 仅覆盖核心端点，便于 Swagger UI 渲染"管理界面"。
var openAPISpec = map[string]interface{}{
	"openapi": "3.0.3",
	"info": map[string]interface{}{
		"title":       "Mneme API",
		"version":     "1.0.0",
		"description": "Mneme 长期记忆服务的 HTTP 接口：对话历史接入、记忆事件、用户画像与会话总结查询。",
	},
	"servers": []map[string]interface{}{
		{"url": "http://localhost:8080", "description": "local"},
	},
	"components": map[string]interface{}{
		"securitySchemes": map[string]interface{}{
			"TenantHeader": map[string]interface{}{
				"type": "apiKey",
				"in":   "header",
				"name": "X-Tenant-ID",
			},
		},
		"schemas": map[string]interface{}{
			"HistoryMessage": map[string]interface{}{
				"type": "object",
				"required": []string{"session_id", "role", "content"},
				"properties": map[string]interface{}{
					"session_id": map[string]interface{}{"type": "string", "example": "sess-demo-001"},
					"user_id":    map[string]interface{}{"type": "string", "example": "u1"},
					"role":       map[string]interface{}{"type": "string", "enum": []string{"user", "assistant", "system"}, "example": "user"},
					"content":    map[string]interface{}{"type": "string", "example": "帮我部署一下 mneme 服务"},
					"model_name": map[string]interface{}{"type": "string", "example": "gpt-4o"},
				},
				"example": map[string]interface{}{
					"session_id": "sess-demo-001",
					"user_id":    "u1",
					"role":       "user",
					"content":    "帮我部署一下 mneme 服务",
					"model_name": "gpt-4o",
				},
			},
			"MemoryEvent": map[string]interface{}{
				"type": "object",
				"required": []string{"entity_id", "entity_type", "event_type"},
				"properties": map[string]interface{}{
					"entity_id":   map[string]interface{}{"type": "string"},
					"entity_type": map[string]interface{}{"type": "string"},
					"event_type":  map[string]interface{}{"type": "string"},
					"payload":      map[string]interface{}{"type": "object"},
				},
			},
		},
	},
	"security": []map[string]interface{}{{"TenantHeader": []string{}}},
	"paths": map[string]interface{}{
		"/health": map[string]interface{}{
			"get": map[string]interface{}{
				"summary": "健康检查",
				"responses": map[string]interface{}{
					"200": map[string]interface{}{"description": "ok"},
				},
			},
		},
		"/api/v1/history": map[string]interface{}{
			"post": map[string]interface{}{
				"summary": "写入单条对话历史",
				"requestBody": map[string]interface{}{
					"required": true,
					"content": map[string]interface{}{"application/json": map[string]interface{}{
						"schema":  map[string]interface{}{"$ref": "#/components/schemas/HistoryMessage"},
						"example": map[string]interface{}{"session_id": "sess-demo-001", "user_id": "u1", "role": "user", "content": "帮我部署一下 mneme 服务"},
					}},
				},
				"responses": map[string]interface{}{
					"202": map[string]interface{}{"description": "accepted"},
					"400": map[string]interface{}{"description": "bad request"},
				},
			},
		},
		"/api/v1/history/batch": map[string]interface{}{
			"post": map[string]interface{}{
				"summary": "批量写入对话历史",
				"requestBody": map[string]interface{}{
					"required": true,
					"content": map[string]interface{}{"application/json": map[string]interface{}{
						"schema": map[string]interface{}{
							"type": "object",
							"properties": map[string]interface{}{
								"messages": map[string]interface{}{"type": "array", "items": map[string]interface{}{"$ref": "#/components/schemas/HistoryMessage"}},
							},
						},
						"example": map[string]interface{}{
							"messages": []map[string]interface{}{
								{"session_id": "sess-demo-001", "user_id": "u1", "role": "user", "content": "帮我部署 mneme"},
								{"session_id": "sess-demo-001", "user_id": "u1", "role": "assistant", "content": "已为你生成 docker-compose 配置"},
							},
						},
					}},
				},
				"responses": map[string]interface{}{
					"202": map[string]interface{}{"description": "accepted"},
					"400": map[string]interface{}{"description": "bad request"},
				},
			},
		},
		"/api/v1/memory/events": map[string]interface{}{
			"post": map[string]interface{}{
				"summary": "写入记忆事件",
				"requestBody": map[string]interface{}{
					"required": true,
					"content": map[string]interface{}{"application/json": map[string]interface{}{
						"schema": map[string]interface{}{"$ref": "#/components/schemas/MemoryEvent"},
						"example": map[string]interface{}{
							"entity_id": "u1", "entity_type": "user", "event_type": "preference",
							"payload": map[string]interface{}{"lang": "zh", "timezone": "Asia/Shanghai"},
						},
					}},
				},
				"responses": map[string]interface{}{
					"202": map[string]interface{}{"description": "accepted"},
					"400": map[string]interface{}{"description": "bad request"},
				},
			},
		},
		"/api/v1/memory/{entity_type}/{entity_id}": map[string]interface{}{
			"get": map[string]interface{}{
				"summary": "读取实体画像",
				"parameters": []map[string]interface{}{
					{"name": "entity_type", "in": "path", "required": true, "schema": map[string]interface{}{"type": "string", "example": "user"}},
					{"name": "entity_id", "in": "path", "required": true, "schema": map[string]interface{}{"type": "string", "example": "u1"}},
				},
				"responses": map[string]interface{}{
					"200": map[string]interface{}{"description": "profile"},
					"404": map[string]interface{}{"description": "not found"},
				},
			},
		},
		"/api/v1/sessions/{session_id}": map[string]interface{}{
			"get": map[string]interface{}{
				"summary": "读取会话总结",
				"parameters": []map[string]interface{}{
					{"name": "session_id", "in": "path", "required": true, "schema": map[string]interface{}{"type": "string", "example": "sess-demo-001"}},
				},
				"responses": map[string]interface{}{
					"200": map[string]interface{}{"description": "summary"},
					"404": map[string]interface{}{"description": "not found"},
				},
			},
		},
		"/api/v1/users/{user_id}": map[string]interface{}{
			"get": map[string]interface{}{
				"summary": "聚合读取用户画像 + 其全部会话总结",
				"parameters": []map[string]interface{}{
					{"name": "user_id", "in": "path", "required": true, "schema": map[string]interface{}{"type": "string", "example": "u1"}},
				},
				"responses": map[string]interface{}{
					"200": map[string]interface{}{"description": "user context"},
					"404": map[string]interface{}{"description": "not found"},
				},
			},
		},
		"/admin/api/users": map[string]interface{}{
			"get": map[string]interface{}{
				"summary": "[后台] 列出用户与画像（需 admin.token，若已配置）",
			"parameters": []map[string]interface{}{
				{"name": "limit", "in": "query", "required": false, "schema": map[string]interface{}{"type": "integer"}},
				{"name": "cursor", "in": "query", "required": false, "schema": map[string]interface{}{"type": "string", "description": "分页游标（上一页返回的 next_cursor）"}},
			},
			"responses": map[string]interface{}{"200": map[string]interface{}{"description": "users（含 next_cursor 分页游标）"}},
			},
		},
		"/admin/api/users/{user_id}": map[string]interface{}{
			"get": map[string]interface{}{"summary": "[后台] 查看用户画像 + 会话列表", "parameters": []map[string]interface{}{{"name": "user_id", "in": "path", "required": true, "schema": map[string]interface{}{"type": "string", "example": "u1"}}}, "responses": map[string]interface{}{"200": map[string]interface{}{"description": "user"}, "404": map[string]interface{}{"description": "not found"}}},
			"delete": map[string]interface{}{"summary": "[后台] 删除用户画像及其全部会话", "parameters": []map[string]interface{}{{"name": "user_id", "in": "path", "required": true, "schema": map[string]interface{}{"type": "string", "example": "u1"}}}, "responses": map[string]interface{}{"200": map[string]interface{}{"description": "deleted"}}},
		},
		"/admin/api/users/{user_id}/export": map[string]interface{}{
			"get": map[string]interface{}{"summary": "[后台] 导出用户画像 + 会话（JSON）", "parameters": []map[string]interface{}{{"name": "user_id", "in": "path", "required": true, "schema": map[string]interface{}{"type": "string", "example": "u1"}}}, "responses": map[string]interface{}{"200": map[string]interface{}{"description": "json"}}},
		},
		"/admin/api/sessions": map[string]interface{}{
			"get": map[string]interface{}{
				"summary": "[后台] 检索会话总结（user_id / 关键字过滤）",
				"parameters": []map[string]interface{}{
					{"name": "user_id", "in": "query", "required": false, "schema": map[string]interface{}{"type": "string"}},
					{"name": "q", "in": "query", "required": false, "schema": map[string]interface{}{"type": "string"}},
					{"name": "limit", "in": "query", "required": false, "schema": map[string]interface{}{"type": "integer"}},
					{"name": "cursor", "in": "query", "required": false, "schema": map[string]interface{}{"type": "string", "description": "分页游标（上一页返回的 next_cursor）"}},
				},
				"responses": map[string]interface{}{"200": map[string]interface{}{"description": "sessions（按时间倒序，含 next_cursor 分页游标）"}},
			},
		},
		"/admin/api/sessions/{session_id}": map[string]interface{}{
			"get": map[string]interface{}{"summary": "[后台] 查看单个会话总结", "parameters": []map[string]interface{}{{"name": "session_id", "in": "path", "required": true, "schema": map[string]interface{}{"type": "string", "example": "sess-demo-001"}}}, "responses": map[string]interface{}{"200": map[string]interface{}{"description": "session"}, "404": map[string]interface{}{"description": "not found"}}},
			"delete": map[string]interface{}{"summary": "[后台] 删除单个会话总结", "parameters": []map[string]interface{}{{"name": "session_id", "in": "path", "required": true, "schema": map[string]interface{}{"type": "string", "example": "sess-demo-001"}}}, "responses": map[string]interface{}{"200": map[string]interface{}{"description": "deleted"}}},
		},
		"/admin/api/sessions/{session_id}/export": map[string]interface{}{
			"get": map[string]interface{}{"summary": "[后台] 导出单个会话总结（JSON）", "parameters": []map[string]interface{}{{"name": "session_id", "in": "path", "required": true, "schema": map[string]interface{}{"type": "string", "example": "sess-demo-001"}}}, "responses": map[string]interface{}{"200": map[string]interface{}{"description": "json"}}},
		},
		"/admin/api/audit": map[string]interface{}{
			"get": map[string]interface{}{
				"summary": "[后台] 查看管理操作审计日志（删除/导出等变更）",
				"parameters": []map[string]interface{}{
					{"name": "limit", "in": "query", "required": false, "schema": map[string]interface{}{"type": "integer"}},
				},
				"responses": map[string]interface{}{"200": map[string]interface{}{"description": "audit（最新在前）"}},
			},
		},
	},
}

// OpenAPIHandler 暴露 OpenAPI 文档与 Swagger UI（轻量管理界面）。
type OpenAPIHandler struct{}

// NewOpenAPIHandler 创建 handler。
func NewOpenAPIHandler() *OpenAPIHandler { return &OpenAPIHandler{} }

// Spec 返回 OpenAPI JSON 文档。
func (h *OpenAPIHandler) Spec(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(openAPISpec)
}

// Docs 返回内联的 Swagger UI 页面（从 CDN 加载 swagger-ui）。
func (h *OpenAPIHandler) Docs(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(swaggerUIDoc))
}

const swaggerUIDoc = `<!DOCTYPE html>
<html lang="zh">
<head>
  <meta charset="UTF-8"/>
  <title>Mneme API</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css"/>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    window.onload = function () {
      window.ui = SwaggerUIBundle({
        url: '/openapi.json',
        dom_id: '#swagger-ui',
        presets: [SwaggerUIBundle.presets.apis],
        deepLinking: true
      });
    };
  </script>
</body>
</html>`
