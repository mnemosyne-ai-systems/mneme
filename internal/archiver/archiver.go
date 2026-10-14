// Package archiver 实现 ClickHouse 冷热分离：把过期分区导出为 Parquet，
// 上传到对象存储后从 ClickHouse 删除。
package archiver

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/mnemosyne-labs/mneme/pkg/logger"
	"github.com/mnemosyne-labs/mneme/pkg/metrics"
)

// OSSConfig 对象存储配置。
type OSSConfig struct {
	Endpoint  string
	Region    string
	Bucket    string
	AccessKey string
	SecretKey string
	Prefix    string
}

// Enabled 返回是否配置了对象存储。
func (c OSSConfig) Enabled() bool { return c.Bucket != "" && c.Endpoint != "" }

// Config 归档任务配置。
type Config struct {
	// CronHour 每日执行时刻（0-23）。
	CronHour int
	// RetainMonths 保留最近 N 个月的分区，更早的归档。
	RetainMonths int
	// Tables 需要归档的表（必须按 toYYYYMM 分区）。
	Tables []string
	// ExportDir 本地导出目录。
	ExportDir string
	// HTTPAddr 是 ClickHouse 的 HTTP 接口地址（如 http://clickhouse:8123）。
	// 配置后通过 HTTP 接口在客户端导出 Parquet（容器部署下唯一正确的方式）；
	// 未配置时回退为 INTO OUTFILE，仅当应用与 ClickHouse 共享文件系统时可用。
	HTTPAddr string
	// Username/Password 用于 HTTP 接口认证。
	Username string
	Password string
	OSS      OSSConfig
}

func (c Config) withDefaults() Config {
	if c.RetainMonths <= 0 {
		c.RetainMonths = 3
	}
	if len(c.Tables) == 0 {
		c.Tables = []string{"history_message"}
	}
	if c.ExportDir == "" {
		c.ExportDir = "./data/archive"
	}
	if c.CronHour < 0 || c.CronHour > 23 {
		c.CronHour = 3
	}
	return c
}

// Uploader 把本地文件上传到对象存储。
type Uploader interface {
	Upload(ctx context.Context, localPath, remoteKey string) error
}

// Job 是每日执行的归档任务。
type Job struct {
	conn     driver.Conn
	cfg      Config
	uploader Uploader
	now      func() time.Time

	httpClient *http.Client

	stopCh  chan struct{}
	stopped chan struct{}
	once    sync.Once

	startedMu sync.Mutex
	started   bool
}

// NewJob 创建归档任务；未配置 OSS 时仅导出到本地。
func NewJob(conn driver.Conn, cfg Config) *Job {
	cfg = cfg.withDefaults()
	var up Uploader
	if cfg.OSS.Enabled() {
		up = NewS3Uploader(cfg.OSS)
	}
	return &Job{
		conn:       conn,
		cfg:        cfg,
		uploader:   up,
		now:        time.Now,
		httpClient: &http.Client{Timeout: 30 * time.Minute},
		stopCh:     make(chan struct{}),
		stopped:    make(chan struct{}),
	}
}

// SetUploader 覆盖上传器（测试用）。
func (j *Job) SetUploader(u Uploader) { j.uploader = u }

// Start 启动每日定时调度。
func (j *Job) Start(ctx context.Context) {
	// 必须先置位 started 再启动 loop：若顺序颠倒，Stop 可能看到 started=false
	// 而自行 close(stopped)，随后 loop 的 defer close(stopped) 造成 double close。
	j.startedMu.Lock()
	j.started = true
	j.startedMu.Unlock()
	go j.loop(ctx)
	logger.L().WithField("hour", j.cfg.CronHour).Info("archiver scheduled")
}

func (j *Job) loop(ctx context.Context) {
	defer close(j.stopped)
	for {
		wait := j.durationUntilNextRun()
		timer := time.NewTimer(wait)
		select {
		case <-j.stopCh:
			timer.Stop()
			return
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			if err := j.RunOnce(ctx); err != nil {
				metrics.ArchiveRunsTotal.WithLabelValues("error").Inc()
				logger.L().WithError(err).Error("archive run failed")
				continue
			}
			metrics.ArchiveRunsTotal.WithLabelValues("success").Inc()
		}
	}
}

// durationUntilNextRun 计算距离下一次执行时刻的时长。
func (j *Job) durationUntilNextRun() time.Duration {
	now := j.now()
	next := time.Date(now.Year(), now.Month(), now.Day(), j.cfg.CronHour, 0, 0, 0, now.Location())
	if !next.After(now) {
		next = next.Add(24 * time.Hour)
	}
	return next.Sub(now)
}

// Stop 停止调度。
// 未调用 Start（如 Run 中前置组件启动失败提前返回）时，
// loop 从未运行、stopped 不会被 close，直接等待会造成死锁，故加 started 保护。
func (j *Job) Stop() {
	j.once.Do(func() {
		close(j.stopCh)
		j.startedMu.Lock()
		started := j.started
		j.startedMu.Unlock()
		if started {
			<-j.stopped
		} else {
			close(j.stopped)
		}
		logger.L().Info("archiver stopped")
	})
}

// RunOnce 手动触发一次归档。
func (j *Job) RunOnce(ctx context.Context) error {
	if err := os.MkdirAll(j.cfg.ExportDir, 0o755); err != nil {
		return fmt.Errorf("archiver: create export dir: %w", err)
	}
	cutoff := CutoffPartition(j.now(), j.cfg.RetainMonths)

	for _, table := range j.cfg.Tables {
		partitions, err := j.expiredPartitions(ctx, table, cutoff)
		if err != nil {
			return err
		}
		for _, p := range partitions {
			if err := j.archivePartition(ctx, table, p); err != nil {
				return err
			}
		}
	}
	return nil
}

// CutoffPartition 返回归档分界分区（YYYYMM），小于该值的分区将被归档。
func CutoffPartition(now time.Time, retainMonths int) uint32 {
	t := now.AddDate(0, -retainMonths, 0)
	return uint32(t.Year()*100 + int(t.Month()))
}

// expiredPartitions 查询指定表中早于 cutoff 的分区。
// 注意必须取 partition_id 列：system.parts 的 partition 列对
// toYYYYMM 分区返回 ('202301') 元组形式，无法直接用 ParseUint 解析。
func (j *Job) expiredPartitions(ctx context.Context, table string, cutoff uint32) ([]string, error) {
	const q = `SELECT DISTINCT partition_id FROM system.parts
	           WHERE table = ? AND active = 1 AND database = currentDatabase()`
	rows, err := j.conn.Query(ctx, q, table)
	if err != nil {
		return nil, fmt.Errorf("archiver: query partitions of %s: %w", table, err)
	}
	defer func() { _ = rows.Close() }()

	var result []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, fmt.Errorf("archiver: scan partition: %w", err)
		}
		v, err := strconv.ParseUint(p, 10, 32)
		if err != nil {
			// 非 YYYYMM 分区，跳过
			continue
		}
		if uint32(v) < cutoff {
			result = append(result, p)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("archiver: iterate partitions: %w", err)
	}
	sort.Strings(result)
	return result, nil
}

// archivePartition 导出 → 上传 → 删除。
func (j *Job) archivePartition(ctx context.Context, table, partition string) error {
	filename := fmt.Sprintf("%s_%s.parquet", table, partition)
	localPath := filepath.Join(j.cfg.ExportDir, filename)

	if err := j.exportPartition(ctx, table, partition, localPath); err != nil {
		return err
	}
	if j.uploader != nil {
		// 对象存储 key 使用正斜杠语义，不能用本地路径分隔符
		remoteKey := path.Join(j.cfg.OSS.Prefix, table, filename)
		if err := j.uploader.Upload(ctx, localPath, remoteKey); err != nil {
			return fmt.Errorf("archiver: upload %s: %w", filename, err)
		}
		if err := os.Remove(localPath); err != nil {
			logger.L().WithError(err).Warn("archiver: remove local export failed")
		}
	}
	if err := j.dropPartition(ctx, table, partition); err != nil {
		return err
	}
	logger.L().WithFields(map[string]interface{}{
		"table":     table,
		"partition": partition,
	}).Info("partition archived")
	return nil
}

// exportPartition 导出指定分区为本地 Parquet 文件。
// 优先使用 HTTP 接口在客户端导出；未配置 HTTPAddr 时回退 INTO OUTFILE
// （文件写在 ClickHouse 服务端，仅当与本进程共享文件系统时可用）。
func (j *Job) exportPartition(ctx context.Context, table, partition, localPath string) error {
	if j.cfg.HTTPAddr != "" {
		return j.exportPartitionHTTP(ctx, table, partition, localPath)
	}
	return j.exportPartitionOutfile(ctx, table, partition, localPath)
}

// exportPartitionHTTP 通过 ClickHouse HTTP 接口把分区数据以 Parquet 格式流式写到本地文件。
func (j *Job) exportPartitionHTTP(ctx context.Context, table, partition, localPath string) error {
	query := fmt.Sprintf("SELECT * FROM %s WHERE _partition_id = '%s' FORMAT Parquet",
		quoteIdentifier(table), partition)
	endpoint := strings.TrimSuffix(j.cfg.HTTPAddr, "/") + "/?query=" + url.QueryEscape(query)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return fmt.Errorf("archiver: build export request: %w", err)
	}
	if j.cfg.Username != "" {
		req.SetBasicAuth(j.cfg.Username, j.cfg.Password)
	}

	resp, err := j.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("archiver: export %s partition %s: %w", table, partition, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("archiver: export %s partition %s: http %d: %s",
			table, partition, resp.StatusCode, strings.TrimSpace(string(body)))
	}

	f, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("archiver: create export file: %w", err)
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		_ = f.Close()
		_ = os.Remove(localPath)
		return fmt.Errorf("archiver: write export file: %w", err)
	}
	return f.Close()
}

// exportPartitionOutfile 使用 INTO OUTFILE 在 ClickHouse 服务端导出。
// 警告：文件写在服务端文件系统，仅当应用与 ClickHouse 共享存储（如同机部署）时可用。
func (j *Job) exportPartitionOutfile(ctx context.Context, table, partition, localPath string) error {
	logger.L().WithFields(map[string]interface{}{
		"table": table, "partition": partition,
	}).Warn("archiver: exporting via INTO OUTFILE on server side; configure http_addr for client-side export")
	escapedPath := strings.ReplaceAll(localPath, "'", "\\'")
	query := fmt.Sprintf(
		"SELECT * FROM %s WHERE _partition_id = ? INTO OUTFILE '%s' TRUNCATE FORMAT Parquet",
		quoteIdentifier(table), escapedPath,
	)
	if err := j.conn.Exec(ctx, query, partition); err != nil {
		return fmt.Errorf("archiver: export %s partition %s: %w", table, partition, err)
	}
	return nil
}

// identPattern 校验 ClickHouse 标识符，防止 SQL 注入。
var identPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// quoteIdentifier 校验并返回表名标识符；非法标识符会终止流程（配置错误）。
func quoteIdentifier(table string) string {
	if !identPattern.MatchString(table) {
		// 表名来自静态配置，非法值属于部署错误，直接暴露而非静默拼接。
		panic(fmt.Sprintf("archiver: invalid table identifier %q", table))
	}
	return table
}

// dropPartition 删除已归档分区。
func (j *Job) dropPartition(ctx context.Context, table, partition string) error {
	stmt := fmt.Sprintf("ALTER TABLE %s DROP PARTITION '%s'", table, partition)
	if err := j.conn.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("archiver: drop partition %s of %s: %w", partition, table, err)
	}
	return nil
}
