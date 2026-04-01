package tools

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"minio-mc-mcp/internal/logger"
	"minio-mc-mcp/internal/minio"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

// MigrationProgress 迁移进度（对外接口，保持与前端/Python 后端兼容）
type MigrationProgress struct {
	Percent            int    `json:"percent"`
	TransferredObjects int64  `json:"transferred_objects"`
	TotalObjects       int64  `json:"total_objects"`
	TransferredBytes   int64  `json:"transferred_bytes"`
	TotalBytes         int64  `json:"total_bytes"`
	CurrentFile        string `json:"current_file,omitempty"`
	Speed              string `json:"speed,omitempty"`
}

// migrationTask 迁移任务记录
type migrationTask struct {
	ID          string             `json:"id"`
	Source      string             `json:"source"`
	Target      string             `json:"target"`
	Status      string             `json:"status"` // running, completed, failed, cancelled
	StartedAt   time.Time          `json:"started_at"`
	EndedAt     *time.Time         `json:"ended_at,omitempty"`
	Error       string             `json:"error,omitempty"`
	Progress    *MigrationProgress `json:"progress,omitempty"`
	cancel      context.CancelFunc
	subscribers map[chan *MigrationProgress]struct{}
	subMu       sync.RWMutex
}

// migrationStore 内存中的迁移任务存储
var (
	migrationTasks = make(map[string]*migrationTask)
	migrationMu    sync.RWMutex
)

func generateTaskID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("mig-%s", hex.EncodeToString(b))
}

// SubscribeProgress 订阅任务进度
func SubscribeProgress(taskID string) (<-chan *MigrationProgress, func(), error) {
	migrationMu.RLock()
	task, exists := migrationTasks[taskID]
	migrationMu.RUnlock()

	if !exists {
		return nil, nil, fmt.Errorf("任务 %s 不存在", taskID)
	}

	ch := make(chan *MigrationProgress, 10)

	task.subMu.Lock()
	if task.subscribers == nil {
		task.subscribers = make(map[chan *MigrationProgress]struct{})
	}
	task.subscribers[ch] = struct{}{}
	task.subMu.Unlock()

	// 立即发送当前进度
	if task.Progress != nil {
		select {
		case ch <- task.Progress:
		default:
		}
	}

	unsubscribe := func() {
		task.subMu.Lock()
		delete(task.subscribers, ch)
		task.subMu.Unlock()
		close(ch)
	}

	return ch, unsubscribe, nil
}

// GetTaskStatus 获取任务状态（供 SSE handler 使用）
func GetTaskStatus(taskID string) (status string, progress *MigrationProgress, err error) {
	migrationMu.RLock()
	task, exists := migrationTasks[taskID]
	migrationMu.RUnlock()

	if !exists {
		return "", nil, fmt.Errorf("任务 %s 不存在", taskID)
	}

	return task.Status, task.Progress, nil
}

// broadcastProgress 广播进度给所有订阅者
func (t *migrationTask) broadcastProgress(progress *MigrationProgress) {
	t.Progress = progress

	t.subMu.RLock()
	defer t.subMu.RUnlock()

	for ch := range t.subscribers {
		select {
		case ch <- progress:
		default:
			// 通道满了，跳过
		}
	}
}

// RegisterMigrationTools 注册数据迁移工具
// s3client 为本实例的 S3 客户端，用于同实例迁移和获取 bucket 统计
func RegisterMigrationTools(registry *ToolRegistry, executor *minio.MCExecutor, s3client *minio.S3Client) error {
	log := logger.Named("tools.migration")

	// migration.list - 列出迁移任务
	if err := registry.RegisterTool(&Tool{
		Name:        "migration.list",
		Description: "[READ][低风险] 列出所有数据迁移任务",
		Category:    CategoryMigration,
		RiskLevel:   "low",
		InputSchema: &InputSchema{
			Type:       "object",
			Properties: map[string]*ParameterSchema{},
		},
		Handler: func(ctx context.Context, args map[string]any, exec *minio.MCExecutor) (any, string, error) {
			migrationMu.RLock()
			defer migrationMu.RUnlock()

			tasks := make([]map[string]any, 0, len(migrationTasks))
			for _, t := range migrationTasks {
				task := map[string]any{
					"task_id":    t.ID,
					"source":     t.Source,
					"target":     t.Target,
					"status":     t.Status,
					"started_at": t.StartedAt.Format(time.RFC3339),
				}
				if t.EndedAt != nil {
					task["ended_at"] = t.EndedAt.Format(time.RFC3339)
				}
				if t.Error != "" {
					task["error"] = t.Error
				}
				if t.Progress != nil {
					task["progress"] = t.Progress
				}
				tasks = append(tasks, task)
			}

			return map[string]any{
				"tasks": tasks,
				"total": len(tasks),
			}, "", nil
		},
	}); err != nil {
		return err
	}

	// migration.mirror - 启动迁移任务（使用 S3 API）
	if err := registry.RegisterTool(&Tool{
		Name:        "migration.mirror",
		Description: "[CREATE][高风险] 启动 S3 API 数据迁移任务，支持同实例和跨实例迁移",
		Category:    CategoryMigration,
		RiskLevel:   "high",
		InputSchema: &InputSchema{
			Type: "object",
			Properties: map[string]*ParameterSchema{
				"task_id": {
					Type:        "string",
					Description: "任务 ID，由调用方指定以保持一致性，不传则自动生成",
				},
				"source": {
					Type:        "string",
					Description: "源路径，格式: bucket 或 bucket/prefix",
					Required:    true,
				},
				"target": {
					Type:        "string",
					Description: "目标路径，格式: bucket 或 bucket/prefix",
					Required:    true,
				},
				"mode": {
					Type:        "string",
					Description: "迁移模式: overwrite(覆盖), newer(仅新文件)",
					Default:     "overwrite",
				},
				"incremental": {
					Type:        "boolean",
					Description: "是否增量迁移（跳过目标已存在且大小相同的对象）",
					Default:     false,
				},
				"checksum": {
					Type:        "boolean",
					Description: "是否校验 ETag（增量模式下额外比较 ETag）",
					Default:     false,
				},
				"target_endpoint": {
					Type:        "string",
					Description: "跨实例迁移时目标 MinIO 的 endpoint（如 172.30.13.210:9000）",
				},
				"target_access_key": {
					Type:        "string",
					Description: "跨实例迁移时目标 MinIO 的 access key",
				},
				"target_secret_key": {
					Type:        "string",
					Description: "跨实例迁移时目标 MinIO 的 secret key",
				},
				"target_secure": {
					Type:        "boolean",
					Description: "跨实例迁移时目标 MinIO 是否使用 HTTPS",
					Default:     false,
				},
			},
			Required: []string{"source", "target"},
		},
		Handler: func(ctx context.Context, args map[string]any, exec *minio.MCExecutor) (any, string, error) {
			return handleMigrationMirror(ctx, args, exec, s3client, log)
		},
	}); err != nil {
		return err
	}

	// migration.status - 查询迁移任务状态
	if err := registry.RegisterTool(&Tool{
		Name:        "migration.status",
		Description: "[READ][低风险] 查询迁移任务状态",
		Category:    CategoryMigration,
		RiskLevel:   "low",
		InputSchema: &InputSchema{
			Type: "object",
			Properties: map[string]*ParameterSchema{
				"task_id": {
					Type:        "string",
					Description: "迁移任务 ID",
					Required:    true,
				},
			},
			Required: []string{"task_id"},
		},
		Handler: func(ctx context.Context, args map[string]any, exec *minio.MCExecutor) (any, string, error) {
			taskID, _ := args["task_id"].(string)
			if taskID == "" {
				return nil, "", fmt.Errorf("task_id 不能为空")
			}

			migrationMu.RLock()
			task, exists := migrationTasks[taskID]
			migrationMu.RUnlock()

			if !exists {
				return nil, "", fmt.Errorf("迁移任务 '%s' 不存在", taskID)
			}

			result := map[string]any{
				"task_id":    task.ID,
				"source":     task.Source,
				"target":     task.Target,
				"status":     task.Status,
				"started_at": task.StartedAt.Format(time.RFC3339),
			}
			if task.EndedAt != nil {
				result["ended_at"] = task.EndedAt.Format(time.RFC3339)
				result["duration_seconds"] = int64(task.EndedAt.Sub(task.StartedAt).Seconds())
			}
			if task.Error != "" {
				result["error"] = task.Error
			}
			if task.Progress != nil {
				result["progress"] = task.Progress
			}

			return map[string]any{"task": result}, "", nil
		},
	}); err != nil {
		return err
	}

	// migration.cancel - 取消迁移任务
	if err := registry.RegisterTool(&Tool{
		Name:        "migration.cancel",
		Description: "[UPDATE][中风险] 取消正在运行的迁移任务",
		Category:    CategoryMigration,
		RiskLevel:   "medium",
		InputSchema: &InputSchema{
			Type: "object",
			Properties: map[string]*ParameterSchema{
				"task_id": {
					Type:        "string",
					Description: "迁移任务 ID",
					Required:    true,
				},
			},
			Required: []string{"task_id"},
		},
		Handler: func(ctx context.Context, args map[string]any, exec *minio.MCExecutor) (any, string, error) {
			taskID, _ := args["task_id"].(string)
			if taskID == "" {
				return nil, "", fmt.Errorf("task_id 不能为空")
			}

			migrationMu.Lock()
			task, exists := migrationTasks[taskID]
			migrationMu.Unlock()

			if !exists {
				return nil, "", fmt.Errorf("迁移任务 '%s' 不存在", taskID)
			}

			if task.Status != "running" {
				return nil, "", fmt.Errorf("任务 '%s' 当前状态为 %s，无法取消", taskID, task.Status)
			}

			task.cancel()

			return map[string]any{
				"task_id": taskID,
				"message": fmt.Sprintf("迁移任务 %s 已取消", taskID),
			}, "", nil
		},
	}); err != nil {
		return err
	}

	// migration.delete - 删除迁移任务记录
	if err := registry.RegisterTool(&Tool{
		Name:        "migration.delete",
		Description: "[DELETE][低风险] 删除已完成/失败/取消的迁移任务记录",
		Category:    CategoryMigration,
		RiskLevel:   "low",
		InputSchema: &InputSchema{
			Type: "object",
			Properties: map[string]*ParameterSchema{
				"task_id": {
					Type:        "string",
					Description: "迁移任务 ID",
					Required:    true,
				},
			},
			Required: []string{"task_id"},
		},
		Handler: func(ctx context.Context, args map[string]any, exec *minio.MCExecutor) (any, string, error) {
			taskID, _ := args["task_id"].(string)
			if taskID == "" {
				return nil, "", fmt.Errorf("task_id 不能为空")
			}

			migrationMu.Lock()
			defer migrationMu.Unlock()

			task, exists := migrationTasks[taskID]
			if !exists {
				return nil, "", fmt.Errorf("迁移任务 '%s' 不存在", taskID)
			}

			if task.Status == "running" {
				return nil, "", fmt.Errorf("任务 '%s' 正在运行中，请先取消任务", taskID)
			}

			// 关闭所有订阅者
			task.subMu.Lock()
			for ch := range task.subscribers {
				close(ch)
			}
			task.subscribers = nil
			task.subMu.Unlock()

			delete(migrationTasks, taskID)

			return map[string]any{
				"task_id": taskID,
				"message": fmt.Sprintf("迁移任务 %s 已删除", taskID),
			}, "", nil
		},
	}); err != nil {
		return err
	}

	return nil
}

// parseBucketAndPrefix 从路径中解析 bucket 和 prefix
// 格式: "bucket" 或 "bucket/prefix/path"
func parseBucketAndPrefix(path string) (bucket, prefix string) {
	path = strings.TrimPrefix(path, "/")
	if b, p, ok := strings.Cut(path, "/"); ok {
		return b, p
	}
	return path, ""
}

// handleMigrationMirror 处理迁移任务启动
func handleMigrationMirror(
	ctx context.Context,
	args map[string]any,
	exec *minio.MCExecutor,
	s3client *minio.S3Client,
	log *zap.Logger,
) (any, string, error) {
	source, _ := args["source"].(string)
	target, _ := args["target"].(string)
	if source == "" || target == "" {
		return nil, "", fmt.Errorf("source 和 target 不能为空")
	}

	// 解析源和目标的 bucket/prefix
	sourceBucket, sourcePrefix := parseBucketAndPrefix(source)
	targetBucket, targetPrefix := parseBucketAndPrefix(target)

	// 判断是否为跨实例迁移
	targetEndpoint, _ := args["target_endpoint"].(string)
	targetAccessKey, _ := args["target_access_key"].(string)
	targetSecretKey, _ := args["target_secret_key"].(string)
	targetSecure, _ := args["target_secure"].(bool)

	isCrossInstance := targetEndpoint != "" && targetAccessKey != "" && targetSecretKey != ""

	// 确定目标 S3 客户端
	var targetClient *minio.S3Client
	if isCrossInstance {
		var err error
		targetClient, err = minio.NewS3Client(&minio.S3ClientConfig{
			Endpoint:  targetEndpoint,
			AccessKey: targetAccessKey,
			SecretKey: targetSecretKey,
			Secure:    targetSecure,
		})
		if err != nil {
			return nil, "", fmt.Errorf("创建目标 S3 客户端失败: %v", err)
		}
		// 验证目标连接可用
		if !targetClient.CheckConnection(ctx) {
			return nil, "", fmt.Errorf("无法连接到目标 MinIO: %s", targetEndpoint)
		}
	} else {
		// 同实例迁移，源和目标使用同一个客户端
		targetClient = s3client
	}

	// 确定任务 ID
	taskID := generateTaskID()
	if providedTaskID, ok := args["task_id"].(string); ok && providedTaskID != "" {
		taskID = providedTaskID
	}

	// 解析迁移选项
	incremental, _ := args["incremental"].(bool)
	checkETag, _ := args["checksum"].(bool)
	// mode=newer 也视为增量
	if mode, ok := args["mode"].(string); ok && mode == "newer" {
		incremental = true
	}

	taskCtx, cancel := context.WithCancel(context.Background())

	// 构建显示用的源/目标路径
	sourceAlias := exec.GetAlias()
	displaySource := fmt.Sprintf("%s/%s", sourceAlias, source)
	var displayTarget string
	if isCrossInstance {
		displayTarget = fmt.Sprintf("%s/%s", targetEndpoint, target)
	} else {
		displayTarget = fmt.Sprintf("%s/%s", sourceAlias, target)
	}

	task := &migrationTask{
		ID:        taskID,
		Source:    displaySource,
		Target:    displayTarget,
		Status:    "running",
		StartedAt: time.Now(),
		cancel:    cancel,
		Progress:  &MigrationProgress{},
	}

	migrationMu.Lock()
	migrationTasks[taskID] = task
	migrationMu.Unlock()

	// 进度回调节流：最多每 500ms 广播一次
	var lastBroadcast time.Time
	var broadcastMu sync.Mutex

	// 异步执行迁移
	go func() {
		migrator := minio.NewMigrator(minio.MigratorConfig{
			SourceClient: s3client,
			TargetClient: targetClient,
			SourceBucket: sourceBucket,
			SourcePrefix: sourcePrefix,
			TargetBucket: targetBucket,
			TargetPrefix: targetPrefix,
			Workers:      8,
			MaxRetries:   3,
			Incremental:  incremental,
			CheckETag:    checkETag,
			OnProgress: func(p *minio.MigratorProgress) {
				broadcastMu.Lock()
				defer broadcastMu.Unlock()

				now := time.Now()
				// 节流：500ms 内最多广播一次，除非是最后一个对象
				if now.Sub(lastBroadcast) < 500*time.Millisecond && p.TransferredObjects < p.TotalObjects {
					return
				}
				lastBroadcast = now

				progress := &MigrationProgress{
					Percent:            p.Percent,
					TransferredObjects: p.TransferredObjects,
					TotalObjects:       p.TotalObjects,
					TransferredBytes:   p.TransferredBytes,
					TotalBytes:         p.TotalBytes,
					CurrentFile:        p.CurrentFile,
				}

				migrationMu.RLock()
				if t, exists := migrationTasks[taskID]; exists {
					t.broadcastProgress(progress)
				}
				migrationMu.RUnlock()
			},
		})

		result := migrator.Run(taskCtx)

		now := time.Now()
		migrationMu.Lock()
		if t, exists := migrationTasks[taskID]; exists {
			t.EndedAt = &now

			if taskCtx.Err() == context.Canceled {
				t.Status = "cancelled"
				t.broadcastProgress(&MigrationProgress{Percent: 0})
				log.Info("迁移任务已取消", zap.String("task_id", taskID))
			} else if result.FailedObjects > 0 && result.TransferredObjects == result.FailedObjects {
				// 全部失败
				t.Status = "failed"
				t.Error = result.Error
				log.Error("迁移任务失败",
					zap.String("task_id", taskID),
					zap.String("error", result.Error),
				)
			} else {
				// 成功（可能有部分失败，但整体视为完成）
				t.Status = "completed"
				t.broadcastProgress(&MigrationProgress{
					Percent:            100,
					TransferredObjects: result.TotalObjects,
					TotalObjects:       result.TotalObjects,
					TransferredBytes:   result.TotalBytes,
					TotalBytes:         result.TotalBytes,
				})
				if result.FailedObjects > 0 {
					t.Error = fmt.Sprintf("完成，但有 %d 个对象迁移失败", result.FailedObjects)
				}
				log.Info("迁移任务完成",
					zap.String("task_id", taskID),
					zap.Int64("transferred", result.TransferredObjects),
					zap.Int64("skipped", result.SkippedObjects),
					zap.Int64("failed", result.FailedObjects),
					zap.Duration("duration", result.Duration),
				)
			}
		}
		migrationMu.Unlock()
	}()

	log.Info("迁移任务已启动",
		zap.String("task_id", taskID),
		zap.String("source", displaySource),
		zap.String("target", displayTarget),
		zap.Bool("cross_instance", isCrossInstance),
		zap.Bool("incremental", incremental),
	)

	return map[string]any{
		"task": map[string]any{
			"task_id":        taskID,
			"source":         displaySource,
			"target":         displayTarget,
			"status":         "running",
			"started_at":     task.StartedAt.Format(time.RFC3339),
			"cross_instance": isCrossInstance,
		},
		"message": fmt.Sprintf("迁移任务 %s 已启动: %s → %s", taskID, displaySource, displayTarget),
	}, "", nil
}
