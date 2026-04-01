package minio

import (
	"context"
	"fmt"
	"io"
	"math"
	"minio-mc-mcp/internal/logger"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"
)

// MigratorConfig 迁移引擎配置
type MigratorConfig struct {
	// 源和目标 S3 客户端
	SourceClient *S3Client
	TargetClient *S3Client

	// 源和目标 bucket/prefix
	SourceBucket string
	SourcePrefix string
	TargetBucket string
	TargetPrefix string

	// 并发 worker 数量（默认 8）
	Workers int
	// 单对象失败重试次数（默认 3）
	MaxRetries int
	// 是否增量模式（跳过目标已存在且大小相同的对象）
	Incremental bool
	// 是否校验 ETag（增量模式下额外比较 ETag）
	CheckETag bool

	// 进度回调（在 worker goroutine 中调用，需线程安全）
	OnProgress func(progress *MigratorProgress)
}

// MigratorProgress 迁移进度
type MigratorProgress struct {
	TotalObjects       int64  `json:"total_objects"`
	TotalBytes         int64  `json:"total_bytes"`
	TransferredObjects int64  `json:"transferred_objects"`
	TransferredBytes   int64  `json:"transferred_bytes"`
	SkippedObjects     int64  `json:"skipped_objects"`
	FailedObjects      int64  `json:"failed_objects"`
	CurrentFile        string `json:"current_file,omitempty"`
	Percent            int    `json:"percent"`
}

// MigratorResult 迁移结果
type MigratorResult struct {
	TotalObjects       int64         `json:"total_objects"`
	TotalBytes         int64         `json:"total_bytes"`
	TransferredObjects int64         `json:"transferred_objects"`
	TransferredBytes   int64         `json:"transferred_bytes"`
	SkippedObjects     int64         `json:"skipped_objects"`
	FailedObjects      int64         `json:"failed_objects"`
	FailedKeys         []string      `json:"failed_keys,omitempty"`
	Duration           time.Duration `json:"duration"`
	Error              string        `json:"error,omitempty"`
}

// objectJob 单个对象的迁移任务
type objectJob struct {
	Key  string
	Size int64
	ETag string
}

// Migrator S3 对象迁移引擎
type Migrator struct {
	cfg MigratorConfig
	log *zap.Logger

	// 原子计数器
	transferredObjects atomic.Int64
	transferredBytes   atomic.Int64
	skippedObjects     atomic.Int64
	failedObjects      atomic.Int64

	// 失败对象列表
	failedKeys []string
	failedMu   sync.Mutex

	// 总量（扫描完成后设置）
	totalObjects int64
	totalBytes   int64

	// 判断是否同实例（可尝试 server-side copy）
	sameInstance bool
}

// NewMigrator 创建迁移引擎
func NewMigrator(cfg MigratorConfig) *Migrator {
	if cfg.Workers <= 0 {
		cfg.Workers = 8
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 3
	}

	m := &Migrator{
		cfg: cfg,
		log: logger.Named("migrator"),
	}

	// 判断是否同实例：源和目标 endpoint 相同
	m.sameInstance = cfg.SourceClient.GetEndpoint() == cfg.TargetClient.GetEndpoint()

	return m
}

// Run 执行迁移，阻塞直到完成或 ctx 取消
func (m *Migrator) Run(ctx context.Context) *MigratorResult {
	startTime := time.Now()

	m.log.Info("开始迁移",
		zap.String("source", fmt.Sprintf("%s/%s", m.cfg.SourceBucket, m.cfg.SourcePrefix)),
		zap.String("target", fmt.Sprintf("%s/%s", m.cfg.TargetBucket, m.cfg.TargetPrefix)),
		zap.Int("workers", m.cfg.Workers),
		zap.Bool("same_instance", m.sameInstance),
		zap.Bool("incremental", m.cfg.Incremental),
	)

	// 确保目标 bucket 存在
	if err := m.ensureTargetBucket(ctx); err != nil {
		return &MigratorResult{
			Error:    fmt.Sprintf("创建目标 bucket 失败: %v", err),
			Duration: time.Since(startTime),
		}
	}

	// 阶段 1：扫描源 bucket，收集所有对象
	jobs, err := m.scanSource(ctx)
	if err != nil {
		return &MigratorResult{
			Error:    fmt.Sprintf("扫描源 bucket 失败: %v", err),
			Duration: time.Since(startTime),
		}
	}

	m.log.Info("扫描完成",
		zap.Int64("total_objects", m.totalObjects),
		zap.Int64("total_bytes", m.totalBytes),
	)

	// 广播初始进度
	m.broadcastProgress("")

	// 阶段 2：worker pool 并发迁移
	jobCh := make(chan objectJob, m.cfg.Workers*2)

	var wg sync.WaitGroup
	for i := 0; i < m.cfg.Workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			m.worker(ctx, workerID, jobCh)
		}(i)
	}

	// 分发任务
	go func() {
		defer close(jobCh)
		for _, job := range jobs {
			select {
			case <-ctx.Done():
				return
			case jobCh <- job:
			}
		}
	}()

	wg.Wait()

	result := &MigratorResult{
		TotalObjects:       m.totalObjects,
		TotalBytes:         m.totalBytes,
		TransferredObjects: m.transferredObjects.Load(),
		TransferredBytes:   m.transferredBytes.Load(),
		SkippedObjects:     m.skippedObjects.Load(),
		FailedObjects:      m.failedObjects.Load(),
		Duration:           time.Since(startTime),
	}

	m.failedMu.Lock()
	result.FailedKeys = m.failedKeys
	m.failedMu.Unlock()

	if ctx.Err() == context.Canceled {
		result.Error = "cancelled"
	} else if result.FailedObjects > 0 {
		result.Error = fmt.Sprintf("%d 个对象迁移失败", result.FailedObjects)
	}

	m.log.Info("迁移完成",
		zap.Int64("transferred", result.TransferredObjects),
		zap.Int64("skipped", result.SkippedObjects),
		zap.Int64("failed", result.FailedObjects),
		zap.Duration("duration", result.Duration),
	)

	return result
}

// ensureTargetBucket 确保目标 bucket 存在
func (m *Migrator) ensureTargetBucket(ctx context.Context) error {
	exists, err := m.cfg.TargetClient.BucketExists(ctx, m.cfg.TargetBucket)
	if err != nil {
		return fmt.Errorf("检查目标 bucket 失败: %w", err)
	}
	if !exists {
		m.log.Info("目标 bucket 不存在，自动创建", zap.String("bucket", m.cfg.TargetBucket))
		if err := m.cfg.TargetClient.MakeBucket(ctx, m.cfg.TargetBucket); err != nil {
			return fmt.Errorf("创建目标 bucket 失败: %w", err)
		}
	}
	return nil
}

// scanSource 扫描源 bucket 收集所有对象
func (m *Migrator) scanSource(ctx context.Context) ([]objectJob, error) {
	opts := minio.ListObjectsOptions{
		Prefix:    m.cfg.SourcePrefix,
		Recursive: true,
	}

	var jobs []objectJob
	var totalBytes int64

	for obj := range m.cfg.SourceClient.GetRawClient().ListObjects(ctx, m.cfg.SourceBucket, opts) {
		if obj.Err != nil {
			return nil, fmt.Errorf("列出对象失败: %w", obj.Err)
		}
		// 跳过目录标记
		if obj.Size == 0 && len(obj.Key) > 0 && obj.Key[len(obj.Key)-1] == '/' {
			continue
		}
		jobs = append(jobs, objectJob{
			Key:  obj.Key,
			Size: obj.Size,
			ETag: obj.ETag,
		})
		totalBytes += obj.Size
	}

	m.totalObjects = int64(len(jobs))
	m.totalBytes = totalBytes
	return jobs, nil
}
// worker 单个迁移 worker
func (m *Migrator) worker(ctx context.Context, id int, jobs <-chan objectJob) {
	for job := range jobs {
		select {
		case <-ctx.Done():
			return
		default:
		}

		m.migrateObject(ctx, job)
	}
}

// migrateObject 迁移单个对象，带重试
func (m *Migrator) migrateObject(ctx context.Context, job objectJob) {
	// 增量模式：检查目标是否已存在且大小一致
	if m.cfg.Incremental {
		if m.shouldSkip(ctx, job) {
			m.skippedObjects.Add(1)
			m.transferredBytes.Add(job.Size)
			m.transferredObjects.Add(1)
			m.broadcastProgress(job.Key)
			return
		}
	}

	// 计算目标 key
	targetKey := m.targetKey(job.Key)

	var lastErr error
	for attempt := 0; attempt < m.cfg.MaxRetries; attempt++ {
		if ctx.Err() != nil {
			return
		}

		if attempt > 0 {
			// 指数退避：1s, 2s, 4s...
			backoff := time.Duration(math.Pow(2, float64(attempt-1))) * time.Second
			m.log.Debug("重试对象迁移",
				zap.String("key", job.Key),
				zap.Int("attempt", attempt+1),
				zap.Duration("backoff", backoff),
			)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}
		}

		// 优先尝试 server-side copy（同实例）
		if m.sameInstance {
			lastErr = m.serverSideCopy(ctx, job.Key, targetKey, job.Size)
			if lastErr == nil {
				m.transferredObjects.Add(1)
				m.transferredBytes.Add(job.Size)
				m.broadcastProgress(job.Key)
				return
			}
			m.log.Debug("server-side copy 失败，回退到 stream copy",
				zap.String("key", job.Key),
				zap.Error(lastErr),
			)
		}

		// stream copy（跨实例或 server-side copy 失败）
		lastErr = m.streamCopy(ctx, job.Key, targetKey, job.Size)
		if lastErr == nil {
			m.transferredObjects.Add(1)
			m.transferredBytes.Add(job.Size)
			m.broadcastProgress(job.Key)
			return
		}

		m.log.Warn("对象迁移失败",
			zap.String("key", job.Key),
			zap.Int("attempt", attempt+1),
			zap.Error(lastErr),
		)
	}

	// 所有重试用尽
	m.failedObjects.Add(1)
	m.failedMu.Lock()
	m.failedKeys = append(m.failedKeys, job.Key)
	m.failedMu.Unlock()

	m.log.Error("对象迁移最终失败",
		zap.String("key", job.Key),
		zap.Int("max_retries", m.cfg.MaxRetries),
		zap.Error(lastErr),
	)

	// 即使失败也推进进度（标记为已处理）
	m.transferredObjects.Add(1)
	m.broadcastProgress(job.Key)
}

// shouldSkip 增量模式下判断是否跳过（目标已存在且大小一致）
func (m *Migrator) shouldSkip(ctx context.Context, job objectJob) bool {
	targetKey := m.targetKey(job.Key)
	info, err := m.cfg.TargetClient.GetRawClient().StatObject(ctx, m.cfg.TargetBucket, targetKey, minio.StatObjectOptions{})
	if err != nil {
		// 对象不存在，需要迁移
		return false
	}

	// 大小不同，需要重新迁移
	if info.Size != job.Size {
		return false
	}

	// 如果启用 ETag 校验，还需比较 ETag
	if m.cfg.CheckETag && job.ETag != "" && info.ETag != job.ETag {
		return false
	}

	return true
}

// targetKey 计算目标对象的 key
// 如果源有 prefix，需要将源 prefix 替换为目标 prefix
func (m *Migrator) targetKey(sourceKey string) string {
	// 去掉源 prefix
	relKey := sourceKey
	if m.cfg.SourcePrefix != "" && len(sourceKey) > len(m.cfg.SourcePrefix) {
		relKey = sourceKey[len(m.cfg.SourcePrefix):]
	}
	// 加上目标 prefix
	return m.cfg.TargetPrefix + relKey
}

// serverSideCopy 服务端复制（同实例，零网络传输）
func (m *Migrator) serverSideCopy(ctx context.Context, sourceKey, targetKey string, size int64) error {
	src := minio.CopySrcOptions{
		Bucket: m.cfg.SourceBucket,
		Object: sourceKey,
	}
	dst := minio.CopyDestOptions{
		Bucket: m.cfg.TargetBucket,
		Object: targetKey,
	}

	_, err := m.cfg.TargetClient.GetRawClient().CopyObject(ctx, dst, src)
	return err
}

// streamCopy 流式复制（跨实例，通过本机中转）
func (m *Migrator) streamCopy(ctx context.Context, sourceKey, targetKey string, size int64) error {
	// 从源读取对象
	obj, err := m.cfg.SourceClient.GetRawClient().GetObject(ctx, m.cfg.SourceBucket, sourceKey, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("获取源对象失败: %w", err)
	}
	defer obj.Close()

	// 获取对象信息以确定 content-type
	info, err := obj.Stat()
	if err != nil {
		return fmt.Errorf("获取源对象信息失败: %w", err)
	}

	// 写入目标
	_, err = m.cfg.TargetClient.GetRawClient().PutObject(ctx, m.cfg.TargetBucket, targetKey, obj, info.Size, minio.PutObjectOptions{
		ContentType: info.ContentType,
		// 大文件自动使用 multipart upload
		PartSize: 64 * 1024 * 1024, // 64MB per part
	})
	if err != nil {
		// 确保读取完毕，避免连接泄漏
		_, _ = io.Copy(io.Discard, obj)
		return fmt.Errorf("写入目标对象失败: %w", err)
	}

	return nil
}

// broadcastProgress 广播当前进度
func (m *Migrator) broadcastProgress(currentFile string) {
	if m.cfg.OnProgress == nil {
		return
	}

	transferred := m.transferredObjects.Load()
	transferredBytes := m.transferredBytes.Load()

	var percent int
	if m.totalBytes > 0 {
		percent = int(float64(transferredBytes) / float64(m.totalBytes) * 100)
	} else if m.totalObjects > 0 {
		percent = int(float64(transferred) / float64(m.totalObjects) * 100)
	}
	if percent > 99 && transferred < m.totalObjects {
		percent = 99
	}

	m.cfg.OnProgress(&MigratorProgress{
		TotalObjects:       m.totalObjects,
		TotalBytes:         m.totalBytes,
		TransferredObjects: transferred,
		TransferredBytes:   transferredBytes,
		SkippedObjects:     m.skippedObjects.Load(),
		FailedObjects:      m.failedObjects.Load(),
		CurrentFile:        currentFile,
		Percent:            percent,
	})
}
