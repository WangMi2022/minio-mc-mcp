package tools

import (
	"context"
	"fmt"
	"regexp"

	"go.uber.org/zap"

	"minio-mc-mcp/internal/logger"
	"minio-mc-mcp/internal/minio"
)

var bucketNameRegex = regexp.MustCompile(`^[a-z0-9][a-z0-9\-]{1,61}[a-z0-9]$`)

// RegisterBucketTools 注册 Bucket 管理工具
// s3client 用于 bucket.info 中获取用量统计（替代不可靠的 mc du）
// bucketCache 用于 bucket.list 返回缓存的统计信息
func RegisterBucketTools(registry *ToolRegistry, executor *minio.MCExecutor, s3client *minio.S3Client, bucketCache *minio.BucketStatsCache) error {
	log := logger.Named("tools.bucket")

	// bucket.create - 创建 Bucket（使用 minio-go SDK 检查和创建）
	if err := registry.RegisterTool(&Tool{
		Name:        "bucket.create",
		Description: "[CREATE][低风险] 创建 MinIO Bucket，默认 private 访问级别",
		Category:    CategoryBucket,
		RiskLevel:   "low",
		InputSchema: &InputSchema{
			Type: "object",
			Properties: map[string]*ParameterSchema{
				"name": {
					Type:        "string",
					Description: "Bucket 名称，3-63 字符，仅支持小写字母、数字和连字符",
					Required:    true,
				},
			},
			Required: []string{"name"},
		},
		Handler: func(ctx context.Context, args map[string]any, exec *minio.MCExecutor) (any, string, error) {
			name, _ := args["name"].(string)
			if err := validateBucketName(name); err != nil {
				return nil, "", err
			}

			// 使用 SDK 检查 bucket 是否已存在
			exists, err := s3client.BucketExists(ctx, name)
			if err != nil {
				return nil, "", fmt.Errorf("检查 Bucket 是否存在失败: %w", err)
			}
			if exists {
				return nil, "", fmt.Errorf("Bucket '%s' 已存在", name)
			}

			// 使用 SDK 创建 bucket
			if err := s3client.MakeBucket(ctx, name); err != nil {
				return nil, "", fmt.Errorf("创建 Bucket 失败: %w", err)
			}

			// 强制设置 private 访问级别（mc anonymous set 无 SDK 简单替代）
			aclResult := exec.Execute(ctx, "anonymous", "set", "none", fmt.Sprintf("%s/%s", exec.GetAlias(), name))
			if !aclResult.Success {
				return map[string]any{
					"bucket":  name,
					"acl":     "private",
					"message": fmt.Sprintf("Bucket '%s' 创建成功，但设置 private 访问级别失败", name),
				}, "设置 private 访问级别失败，请手动检查", nil
			}

			// 刷新缓存
			bucketCache.RefreshOne(ctx, name)

			return map[string]any{
				"bucket":  name,
				"acl":     "private",
				"message": fmt.Sprintf("Bucket '%s' 创建成功", name),
			}, "", nil
		},
	}); err != nil {
		return err
	}

	// bucket.delete - 删除 Bucket（仍使用 mc rb，SDK 无 force 删除非空 bucket 的简单接口）
	if err := registry.RegisterTool(&Tool{
		Name:                 "bucket.delete",
		Description:          "[DELETE][高风险] 删除 MinIO Bucket，非空 Bucket 需要 force=true",
		Category:             CategoryBucket,
		RequiresConfirmation: true,
		RiskLevel:            "high",
		InputSchema: &InputSchema{
			Type: "object",
			Properties: map[string]*ParameterSchema{
				"name": {
					Type:        "string",
					Description: "Bucket 名称",
					Required:    true,
				},
				"force": {
					Type:        "boolean",
					Description: "是否强制删除非空 Bucket",
					Default:     false,
				},
			},
			Required: []string{"name"},
		},
		Handler: func(ctx context.Context, args map[string]any, exec *minio.MCExecutor) (any, string, error) {
			name, _ := args["name"].(string)
			if err := validateBucketName(name); err != nil {
				return nil, "", err
			}

			force, _ := args["force"].(bool)

			cmdArgs := []string{"rb"}
			if force {
				cmdArgs = append(cmdArgs, "--force")
			}
			cmdArgs = append(cmdArgs, fmt.Sprintf("%s/%s", exec.GetAlias(), name))

			result := exec.Execute(ctx, cmdArgs...)
			if !result.Success {
				return nil, "", fmt.Errorf("删除 Bucket 失败: %s", result.ErrorMessage)
			}

			log.Info("Bucket 删除成功", zap.String("bucket", name), zap.Bool("force", force))

			// 从缓存移除
			bucketCache.RefreshOne(ctx, name)

			return map[string]any{
				"bucket":  name,
				"message": fmt.Sprintf("Bucket '%s' 删除成功", name),
			}, "", nil
		},
	}); err != nil {
		return err
	}

	// bucket.list - 列出所有 Bucket（从缓存获取，包含 size/object_count/acl）
	if err := registry.RegisterTool(&Tool{
		Name:        "bucket.list",
		Description: "[READ][低风险] 列出所有 MinIO Bucket（含用量、对象数、访问级别）",
		Category:    CategoryBucket,
		RiskLevel:   "low",
		InputSchema: &InputSchema{
			Type:       "object",
			Properties: map[string]*ParameterSchema{},
		},
		Handler: func(ctx context.Context, args map[string]any, exec *minio.MCExecutor) (any, string, error) {
			// 优先从缓存获取
			cachedStats := bucketCache.GetAll()
			if len(cachedStats) > 0 {
				buckets := make([]map[string]any, 0, len(cachedStats))
				for _, s := range cachedStats {
					buckets = append(buckets, map[string]any{
						"name":         s.Name,
						"created_at":   s.CreatedAt,
						"size":         s.Size,
						"object_count": s.ObjectCount,
						"acl":          s.ACL,
					})
				}
				return map[string]any{
					"buckets": buckets,
					"total":   len(buckets),
					"cached":  true,
				}, "", nil
			}

			// 缓存为空时，直接查询（首次启动或缓存未就绪）
			sdkBuckets, err := s3client.ListBuckets(ctx)
			if err != nil {
				return nil, "", fmt.Errorf("列出 Bucket 失败: %w", err)
			}

			buckets := make([]map[string]any, 0, len(sdkBuckets))
			for _, b := range sdkBuckets {
				createdAt := ""
				if !b.CreationDate.IsZero() {
					createdAt = b.CreationDate.Format("2006-01-02T15:04:05Z")
				}
				buckets = append(buckets, map[string]any{
					"name":         b.Name,
					"created_at":   createdAt,
					"size":         int64(0),
					"object_count": int64(0),
					"acl":          "private",
				})
			}

			// 触发后台刷新缓存
			bucketCache.ForceRefresh()

			return map[string]any{
				"buckets": buckets,
				"total":   len(buckets),
				"cached":  false,
			}, "", nil
		},
	}); err != nil {
		return err
	}

	// bucket.info - 获取单个 Bucket 的详细信息（usage、object_count、access）
	if err := registry.RegisterTool(&Tool{
		Name:        "bucket.info",
		Description: "[READ][低风险] 获取单个 Bucket 的用量、对象数和访问级别",
		Category:    CategoryBucket,
		RiskLevel:   "low",
		InputSchema: &InputSchema{
			Type: "object",
			Properties: map[string]*ParameterSchema{
				"name": {
					Type:        "string",
					Description: "Bucket 名称",
					Required:    true,
				},
			},
			Required: []string{"name"},
		},
		Handler: func(ctx context.Context, args map[string]any, exec *minio.MCExecutor) (any, string, error) {
			name, _ := args["name"].(string)
			if err := validateBucketName(name); err != nil {
				return nil, "", err
			}

			info := map[string]any{"name": name}
			enrichBucketInfo(ctx, exec, s3client, name, info)

			return info, "", nil
		},
	}); err != nil {
		return err
	}

	// bucket.acl.set - 设置 Bucket 访问级别
	if err := registry.RegisterTool(&Tool{
		Name:        "bucket.acl.set",
		Description: "[UPDATE][中风险] 设置 Bucket 访问级别。public-write 为高风险操作",
		Category:    CategoryBucket,
		RiskLevel:   "medium",
		InputSchema: &InputSchema{
			Type: "object",
			Properties: map[string]*ParameterSchema{
				"name": {
					Type:        "string",
					Description: "Bucket 名称",
					Required:    true,
				},
				"acl": {
					Type:        "string",
					Description: "访问级别: private, public-read, public, public-write",
					Default:     "private",
					Enum:        []any{"private", "public-read", "public", "public-write"},
				},
			},
			Required: []string{"name"},
		},
		Handler: func(ctx context.Context, args map[string]any, exec *minio.MCExecutor) (any, string, error) {
			name, _ := args["name"].(string)
			if err := validateBucketName(name); err != nil {
				return nil, "", err
			}

			acl, _ := args["acl"].(string)
			if acl == "" {
				acl = "private"
			}

			// mc anonymous set 命令的 ACL 映射
			aclMap := map[string]string{
				"private":      "none",
				"public-read":  "download",
				"public":       "public",
				"public-write": "upload",
			}

			mcACL, ok := aclMap[acl]
			if !ok {
				return nil, "", fmt.Errorf("不支持的 ACL 级别: %s", acl)
			}

			result := exec.Execute(ctx, "anonymous", "set", mcACL, fmt.Sprintf("%s/%s", exec.GetAlias(), name))
			if !result.Success {
				return nil, "", fmt.Errorf("设置 Bucket ACL 失败: %s", result.ErrorMessage)
			}

			log.Info("Bucket ACL 已更新", zap.String("bucket", name), zap.String("acl", acl))

			warning := ""
			if acl == "public-write" {
				warning = "⚠️ 高风险操作：已将 Bucket 设置为 public-write，任何人都可以向该 Bucket 写入数据。请确认这是预期行为。"
			}

			return map[string]any{
				"bucket":  name,
				"acl":     acl,
				"message": fmt.Sprintf("Bucket '%s' 访问级别已设置为 %s", name, acl),
			}, warning, nil
		},
	}); err != nil {
		return err
	}

	return nil
}

// validateBucketName 验证 Bucket 名称
func validateBucketName(name string) error {
	if name == "" {
		return fmt.Errorf("Bucket 名称不能为空")
	}
	if len(name) < 3 || len(name) > 63 {
		return fmt.Errorf("Bucket 名称长度必须在 3-63 字符之间")
	}
	if !bucketNameRegex.MatchString(name) {
		return fmt.Errorf("Bucket 名称格式无效，仅支持小写字母、数字和连字符")
	}
	return nil
}

// enrichBucketInfo 为单个 bucket 补充 usage、object_count、access 信息
// 用量统计使用 minio-go S3 API（替代不可靠的 mc du），ACL 仍使用 mc anonymous get
func enrichBucketInfo(ctx context.Context, exec *minio.MCExecutor, s3client *minio.S3Client, name string, bucket map[string]any) {
	// 使用 minio-go SDK 获取 usage 和 object_count
	usage, err := s3client.GetBucketUsage(ctx, name)
	if err == nil && usage != nil {
		bucket["size"] = usage.Size
		bucket["object_count"] = usage.ObjectCount
	} else {
		bucket["size"] = int64(0)
		bucket["object_count"] = int64(0)
	}

	// mc anonymous get --json <alias>/<bucket> → 获取访问级别
	target := fmt.Sprintf("%s/%s", exec.GetAlias(), name)
	aclResult := exec.Execute(ctx, "anonymous", "get", target)
	acl := "private"
	if aclResult.Success {
		if m, ok := aclResult.Data.(map[string]any); ok {
			if perm, ok := m["permission"].(string); ok {
				acl = mapMCPermissionToACL(perm)
			}
		}
	}
	bucket["acl"] = acl
}

// mapMCPermissionToACL 将 mc anonymous 权限映射为 ACL 标识
func mapMCPermissionToACL(perm string) string {
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
