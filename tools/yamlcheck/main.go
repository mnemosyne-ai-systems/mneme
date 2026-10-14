// Command yamlcheck 校验仓库内的 YAML 文件：语法错误与重复键均视为失败。
// Prometheus / Kubernetes 对重复键会直接报错，因此本检查可提前发现部署事故。
//
// 用法：go run ./tools/yamlcheck
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

var roots = []string{"deploy", "configs", ".github"}

func main() {
	var files []string
	for _, root := range roots {
		_ = filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return nil
			}
			switch strings.ToLower(filepath.Ext(p)) {
			case ".yaml", ".yml":
				files = append(files, p)
			}
			return nil
		})
	}

	bad := 0
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			fmt.Printf("READ-FAIL %s: %v\n", f, err)
			bad++
			continue
		}
		var doc yaml.Node
		if err := yaml.Unmarshal(b, &doc); err != nil {
			fmt.Printf("INVALID  %s: %v\n", f, err)
			bad++
			continue
		}
		if err := walk(&doc, f); err != nil {
			fmt.Printf("INVALID  %v\n", err)
			bad++
			continue
		}
		fmt.Printf("OK       %s\n", f)
	}

	if bad > 0 {
		fmt.Printf("\n%d 个 YAML 文件存在问题\n", bad)
		os.Exit(1)
	}
	fmt.Printf("\n%d 个 YAML 文件全部通过\n", len(files))
}

// walk 递归遍历节点，检测同一映射层级的重复键。
func walk(n *yaml.Node, path string) error {
	switch n.Kind {
	case yaml.DocumentNode, yaml.SequenceNode:
		for _, c := range n.Content {
			if err := walk(c, path); err != nil {
				return err
			}
		}
	case yaml.MappingNode:
		seen := make(map[string]int, len(n.Content)/2)
		for i := 0; i+1 < len(n.Content); i += 2 {
			key := n.Content[i].Value
			if line, dup := seen[key]; dup {
				return fmt.Errorf("%s: 重复键 %q (行 %d 与行 %d)", path, key, n.Content[i].Line, line)
			}
			seen[key] = n.Content[i].Line
			if err := walk(n.Content[i+1], path+"/"+key); err != nil {
				return err
			}
		}
	}
	return nil
}
