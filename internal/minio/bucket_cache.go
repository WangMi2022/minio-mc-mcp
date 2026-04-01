package minio

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"minio-mc-mcp/internal/logger"
)

// BucketStats 单个 Bucket 的统计信息
type BucketStats struct {
	Name        string `json:"name"`
	CreatedAt   string `json:"created_at"`
	Size        int64  `json:"size"`
	ObjectCount int64  `json:"object_count"`
	ACL         string `json:"acl"`
	UpdatedAt   time.Time
}

// BucketStatsCache Bucket 统计信息缓存
// 后台定期刷新，避免前端逐个请求
type BucketStatsCache struct {
	s3client   *S3Client
	mcExecutor *MCExecutor
	stats      map[string]*BucketStats
	mu         sync.RWMutex
	interval   time.Duration
	stopCh     chan struct{}
	log        *zap.Logger
}

// BucketStatsCacheConfig 缓存配置
type BucketStatsCacheConfig struct {
	S3Client   *S3Client
	MCExecutor *MCExecutor
	Interval   time.Duration // 刷新间隔，默认 5 分钟
}

// NewBucketStatsCache 创建 Bucket 统计缓存
func NewBucketStatsCache(cfg *BucketStatsCacheConfig) *BucketStatsCache {
	interval := cfg.Interval
	if interval <= 0 {
		interval = 5 * time.Minute
	}

	return &BucketStatsCache{
		s3client:   cfg.S3Client,
		mcExecutor: cfg.MCExecutor,
		stats:      make(map[string]*BucketStats),
		interval:   interval,
		stopCh:     make(chan struct{}),
		log:        logger.Named("bucket-cache"),
	}
}

// Start 启动后台刷新
func (c *BucketStatsCache) Start() {
	// 立即执行一次刷新
	go c.refresh()

	// 定期刷新
	go func() {
		ticker := time.NewTicker(c.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				c.refresh()
			case <-c.stopCh:
				return
			}
		}
	}()

	c.log.Info("Bucket 统计缓存已启动", zap.Duration("interval", c.interval))
}

// Stop 停止后台刷新
func (c *BucketStatsCache) Stop() {
	close(c.stopCh)
	c.log.Info("Bucket 统计缓存已停止")
}

// refresh 刷新所有 bucket 的统计信息
func (c *BucketStatsCache) refresh() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	c.log.Debug("开始刷新 Bucket 统计缓存")
	start := time.Now()

	// 获取 bucket 列表
	buckets, err := c.s3client.ListBuckets(ctx)
	if err != nil {
		c.log.Error("刷新缓存失败: 无法获取 Bucket 列表", zap.Error(err))
		return
	}

	newStats := make(map[string]*BucketStats, len(buckets))
	now := time.Now()

	// 并发获取每个 bucket 的统计信息（限制并发数）
	type result struct {
		name  string
		stats *BucketStats
	}

	resultCh := make(chan result, len(buckets))
	sem := make(chan struct{}, 5) // 最多 5 个并发

	var wg sync.WaitGroup
	for _, b := range buckets {
		wg.Add(1)
		go func(bucket string, createdAt time.Time) {
			defer wg.Done()

			sem <- struct{}{}        // 获取信号量
			defer func() { <-sem }() // 释放信号量

			stats := &BucketStats{
				Name:      bucket,
				UpdatedAt: now,
			}

			if !createdAt.IsZero() {
				stats.CreatedAt = createdAt.Format("2006-01-02T15:04:05Z")
			}

			// 获取用量（可能较慢）
			usage, err := c.s3client.GetBucketUsage(ctx, bucket)
			if err == nil && usage != nil {
				stats.Size = usage.Size
				stats.ObjectCount = usage.ObjectCount
			}

			// 获取 ACL
			stats.ACL = c.getBucketACL(ctx, bucket)

			resultCh <- result{name: bucket, stats: stats}
		}(b.Name, b.CreationDate)
	}

	// 等待所有 goroutine 完成
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// 收集结果
	for r := range resultCh {
		newStats[r.name] = r.stats
	}

	// 原子替换缓存
	c.mu.Lock()
	c.stats = newStats
	c.mu.Unlock()

	c.log.Info("Bucket 统计缓存刷新完成",
		zap.Int("buckets", len(newStats)),
		zap.Duration("elapsed", time.Since(start)),
	)
}

// getBucketACL 获取 bucket 的访问级别
func (c *BucketStatsCache) getBucketACL(ctx context.Context, name string) string {
	target := c.mcExecutor.GetAlias() + "/" + name
	result := c.mcExecutor.Execute(ctx, "anonymous", "get", target)
	if !result.Success {
		return "private"
	}

	if m, ok := result.Data.(map[string]any); ok {
		if perm, ok := m["permission"].(string); ok {
			switch perm {
			case "none":
				return "private"
			case "download":
				return "public-read"
			case "upload":
				return "public-write"
			case "public":
				return "public-read-write"
			default:
				return perm
			}
		}
	}
	return "private"
}

// GetAll 获取所有 bucket 的统计信息
func (c *BucketStatsCache) GetAll() []*BucketStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]*BucketStats, 0, len(c.stats))
	for _, s := range c.stats {
		result = append(result, s)
	}
	return result
}

// Get 获取单个 bucket 的统计信息
func (c *BucketStatsCache) Get(name string) *BucketStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stats[name]
}

// RefreshOne 刷新单个 bucket 的统计信息（用于创建/删除后立即更新）
func (c *BucketStatsCache) RefreshOne(ctx context.Context, name string) {
	// 获取 bucket 信息
	buckets, err := c.s3client.ListBuckets(ctx)
	if err != nil {
		return
	}

	var createdAt time.Time
	found := false
	for _, b := range buckets {
		if b.Name == name {
			createdAt = b.CreationDate
			found = true
			break
		}
	}

	if !found {
		// bucket 已删除，从缓存移除
		c.mu.Lock()
		delete(c.stats, name)
		c.mu.Unlock()
		return
	}

	stats := &BucketStats{
		Name:      name,
		UpdatedAt: time.Now(),
	}

	if !createdAt.IsZero() {
		stats.CreatedAt = createdAt.Format("2006-01-02T15:04:05Z")
	}

	usage, err := c.s3client.GetBucketUsage(ctx, name)
	if err == nil && usage != nil {
		stats.Size = usage.Size
		stats.ObjectCount = usage.ObjectCount
	}

	stats.ACL = c.getBucketACL(ctx, name)

	c.mu.Lock()
	c.stats[name] = stats
	c.mu.Unlock()
}

// ForceRefresh 强制立即刷新所有缓存
func (c *BucketStatsCache) ForceRefresh() {
	go c.refresh()
}
