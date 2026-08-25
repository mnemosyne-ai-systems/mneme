package api

import "errors"

// errMultipleJSONObjects 表示请求体包含多个 JSON 文档。
var errMultipleJSONObjects = errors.New("body must contain a single JSON object")
