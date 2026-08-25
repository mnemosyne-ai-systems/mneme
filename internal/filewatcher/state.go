package filewatcher

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// fileMark 是单个文件的处理水位线。
type fileMark struct {
	Size     int64  `json:"size"`
	ModUnix  int64  `json:"mod_unix"`
	Checksum string `json:"checksum"`
}

// State 记录已处理文件的水位线，防止重复处理。
type State struct {
	mu    sync.Mutex
	path  string
	marks map[string]fileMark
}

// LoadState 从磁盘加载水位线；文件不存在时返回空状态。
func LoadState(path string) (*State, error) {
	s := &State{path: path, marks: map[string]fileMark{}}
	if path == "" {
		return s, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}
		return nil, fmt.Errorf("filewatcher: load state: %w", err)
	}
	if len(raw) == 0 {
		return s, nil
	}
	if err := json.Unmarshal(raw, &s.marks); err != nil {
		return nil, fmt.Errorf("filewatcher: parse state: %w", err)
	}
	return s, nil
}

// Save 持久化水位线。
func (s *State) Save() error {
	if s.path == "" {
		return nil
	}
	s.mu.Lock()
	raw, err := json.MarshalIndent(s.marks, "", "  ")
	s.mu.Unlock()
	if err != nil {
		return fmt.Errorf("filewatcher: marshal state: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("filewatcher: create state dir: %w", err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return fmt.Errorf("filewatcher: write state: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("filewatcher: commit state: %w", err)
	}
	return nil
}

// ShouldProcess 判断文件是否需要处理（内容有变化）。
func (s *State) ShouldProcess(path string) (bool, fileMark, error) {
	info, err := os.Stat(path)
	if err != nil {
		return false, fileMark{}, fmt.Errorf("filewatcher: stat %s: %w", path, err)
	}
	sum, err := checksumFile(path)
	if err != nil {
		return false, fileMark{}, err
	}
	mark := fileMark{Size: info.Size(), ModUnix: info.ModTime().Unix(), Checksum: sum}

	s.mu.Lock()
	prev, ok := s.marks[path]
	s.mu.Unlock()

	if ok && prev.Checksum == mark.Checksum {
		return false, mark, nil
	}
	return true, mark, nil
}

// Commit 记录文件已处理。
func (s *State) Commit(path string, mark fileMark) {
	s.mu.Lock()
	s.marks[path] = mark
	s.mu.Unlock()
}

// Len 返回已记录文件数。
func (s *State) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.marks)
}

// checksumFile 计算文件 SHA-256。
func checksumFile(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("filewatcher: checksum %s: %w", path, err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
