package filewatcher

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/mnemosyne/mneme/internal/service"
	"github.com/mnemosyne/mneme/pkg/logger"
	"github.com/mnemosyne/mneme/pkg/metrics"
)

// Config 文件监听配置。
type Config struct {
	Dirs       []string
	Extensions []string
	Debounce   time.Duration
	StateFile  string
	TenantID   string
	// ScanOnStart 为 true 时启动后先全量扫描一次已有文件。
	ScanOnStart bool
}

func (c Config) withDefaults() Config {
	if len(c.Extensions) == 0 {
		c.Extensions = []string{".md", ".jsonl", ".txt"}
	}
	if c.Debounce <= 0 {
		c.Debounce = 2 * time.Second
	}
	if c.TenantID == "" {
		c.TenantID = "default"
	}
	return c
}

// Watcher 监听目录并把新增/修改的文件解析后投递到 HistorySink。
type Watcher struct {
	cfg      Config
	registry *Registry
	ingest   service.HistorySink
	state    *State

	watcher *fsnotify.Watcher

	mu      sync.Mutex
	pending map[string]*time.Timer

	startedMu sync.Mutex
	started   bool

	stopCh  chan struct{}
	stopped chan struct{}
	once    sync.Once
}

// New 创建文件监听器。sink 决定投递通道：
// 独立采集器通常传 *service.HTTPHistorySink，主服务内嵌模式传 *service.IngestService。
func New(cfg Config, sink service.HistorySink) (*Watcher, error) {
	cfg = cfg.withDefaults()
	state, err := LoadState(cfg.StateFile)
	if err != nil {
		return nil, err
	}
	fw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("filewatcher: create watcher: %w", err)
	}
	return &Watcher{
		cfg:      cfg,
		registry: NewRegistry(),
		ingest:   sink,
		state:    state,
		watcher:  fw,
		pending:  make(map[string]*time.Timer),
		stopCh:   make(chan struct{}),
		stopped:  make(chan struct{}),
	}, nil
}

// Start 添加监听目录并启动事件循环。
func (w *Watcher) Start(ctx context.Context) error {
	for _, dir := range w.cfg.Dirs {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("filewatcher: create dir %s: %w", dir, err)
		}
		if err := w.addRecursive(dir); err != nil {
			return err
		}
	}
	if w.cfg.ScanOnStart {
		if err := w.ScanAll(ctx); err != nil {
			logger.L().WithError(err).Warn("initial scan failed")
		}
	}
	w.startedMu.Lock()
	w.started = true
	w.startedMu.Unlock()

	go w.loop(ctx)
	logger.L().WithField("dirs", w.cfg.Dirs).Info("file watcher started")
	return nil
}

// addRecursive 递归添加子目录。
func (w *Watcher) addRecursive(root string) error {
	return filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if err := w.watcher.Add(path); err != nil {
				return fmt.Errorf("filewatcher: watch %s: %w", path, err)
			}
		}
		return nil
	})
}

// ScanAll 全量扫描监听目录中的已有文件。
func (w *Watcher) ScanAll(ctx context.Context) error {
	var errs []error
	for _, dir := range w.cfg.Dirs {
		err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !w.supported(path) {
				return nil
			}
			if perr := w.ProcessFile(ctx, path); perr != nil {
				errs = append(errs, perr)
			}
			return nil
		})
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (w *Watcher) loop(ctx context.Context) {
	defer close(w.stopped)
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		case event, ok := <-w.watcher.Events:
			if !ok {
				return
			}
			w.handleEvent(ctx, event)
		case err, ok := <-w.watcher.Errors:
			if !ok {
				return
			}
			logger.L().WithError(err).Warn("file watcher error")
		}
	}
}

func (w *Watcher) handleEvent(ctx context.Context, event fsnotify.Event) {
	// 新建目录时自动加入监听
	if event.Has(fsnotify.Create) {
		if info, err := os.Stat(event.Name); err == nil && info.IsDir() {
			if err := w.addRecursive(event.Name); err != nil {
				logger.L().WithError(err).Warn("watch new dir failed")
			}
			return
		}
	}
	if !event.Has(fsnotify.Create) && !event.Has(fsnotify.Write) {
		return
	}
	if !w.supported(event.Name) {
		return
	}
	w.schedule(ctx, event.Name)
}

// schedule 防抖：等待文件写入稳定后再处理。
func (w *Watcher) schedule(ctx context.Context, path string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if timer, ok := w.pending[path]; ok {
		timer.Reset(w.cfg.Debounce)
		return
	}
	w.pending[path] = time.AfterFunc(w.cfg.Debounce, func() {
		w.mu.Lock()
		delete(w.pending, path)
		w.mu.Unlock()

		if err := w.ProcessFile(ctx, path); err != nil {
			logger.L().WithError(err).WithField("file", path).Error("process file failed")
		}
	})
}

// ProcessFile 解析并发送单个文件，带水位线防重。
func (w *Watcher) ProcessFile(ctx context.Context, path string) error {
	ext := strings.ToLower(filepath.Ext(path))

	should, mark, err := w.state.ShouldProcess(path)
	if err != nil {
		metrics.FileWatcherFilesTotal.WithLabelValues(ext, "error").Inc()
		return err
	}
	if !should {
		metrics.FileWatcherFilesTotal.WithLabelValues(ext, "skipped").Inc()
		logger.L().WithField("file", path).Debug("file unchanged, skipped")
		return nil
	}

	msgs, err := w.registry.Parse(path, w.cfg.TenantID)
	if err != nil {
		metrics.FileWatcherFilesTotal.WithLabelValues(ext, "error").Inc()
		return err
	}
	if len(msgs) == 0 {
		w.state.Commit(path, mark)
		metrics.FileWatcherFilesTotal.WithLabelValues(ext, "empty").Inc()
		return w.state.Save()
	}

	if err := w.ingest.PublishHistoryBatch(ctx, msgs); err != nil {
		metrics.FileWatcherFilesTotal.WithLabelValues(ext, "error").Inc()
		return fmt.Errorf("filewatcher: publish %s: %w", path, err)
	}

	w.state.Commit(path, mark)
	metrics.FileWatcherFilesTotal.WithLabelValues(ext, "success").Inc()
	logger.L().WithFields(map[string]interface{}{
		"file":     path,
		"messages": len(msgs),
	}).Info("file processed")
	return w.state.Save()
}

// supported 判断扩展名是否在白名单内，并排除临时文件。
func (w *Watcher) supported(path string) bool {
	base := filepath.Base(path)
	if strings.HasPrefix(base, ".") || strings.HasSuffix(base, "~") || strings.HasSuffix(base, ".tmp") {
		return false
	}
	ext := strings.ToLower(filepath.Ext(path))
	for _, e := range w.cfg.Extensions {
		if strings.EqualFold(e, ext) {
			return true
		}
	}
	return false
}

// Stop 停止监听并保存水位线。
func (w *Watcher) Stop() error {
	var err error
	w.once.Do(func() {
		close(w.stopCh)

		w.startedMu.Lock()
		started := w.started
		w.startedMu.Unlock()
		if started {
			// 事件循环负责关闭 stopped
			<-w.stopped
		} else {
			// 从未调用 Start，loop 不会运行，直接关闭避免死锁
			close(w.stopped)
		}

		w.mu.Lock()
		for _, timer := range w.pending {
			timer.Stop()
		}
		w.pending = map[string]*time.Timer{}
		w.mu.Unlock()

		if e := w.watcher.Close(); e != nil {
			err = e
		}
		if e := w.state.Save(); e != nil && err == nil {
			err = e
		}
		logger.L().Info("file watcher stopped")
	})
	return err
}
