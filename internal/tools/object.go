package tools

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"minio-mc-mcp/internal/logger"
	"minio-mc-mcp/internal/minio"

	"go.uber.org/zap"
)

// RegisterObjectTools 注册 Object 管理工具
// s3client 用于 object.list 等需要 S3 API 的操作（mc CLI 在某些 MinIO 版本上无法正常列出对象）
func RegisterObjectTools(registry *ToolRegistry, executor *minio.MCExecutor, s3client *minio.S3Client) error {
	log := logger.Named("tools.object")
	// object.upload - 上传文件
	if err := registry.RegisterTool(&Tool{
		Name:        "object.upload",
		Description: "[CREATE][低风险] 上传本地文件到 MinIO Bucket",
		Category:    CategoryObject,
		RiskLevel:   "low",
		InputSchema: &InputSchema{
			Type: "object",
			Properties: map[string]*ParameterSchema{
				"local_path": {
					Type:        "string",
					Description: "本地文件路径",
					Required:    true,
				},
				"bucket": {
					Type:        "string",
					Description: "目标 Bucket 名称",
					Required:    true,
				},
				"object_key": {
					Type:        "string",
					Description: "目标对象路径（Bucket 内的 key）",
					Required:    true,
				},
			},
			Required: []string{"local_path", "bucket", "object_key"},
		},
		Handler: func(ctx context.Context, args map[string]any, exec *minio.MCExecutor) (any, string, error) {
			localPath, _ := args["local_path"].(string)
			bucket, _ := args["bucket"].(string)
			objectKey, _ := args["object_key"].(string)

			if err := validateBucketName(bucket); err != nil {
				return nil, "", err
			}
			if err := validateObjectPath(objectKey); err != nil {
				return nil, "", err
			}
			if err := validateLocalPath(localPath); err != nil {
				return nil, "", err
			}

			remotePath := fmt.Sprintf("%s/%s/%s", exec.GetAlias(), bucket, objectKey)
			result := exec.Execute(ctx, "cp", localPath, remotePath)
			if !result.Success {
				return nil, "", fmt.Errorf("上传文件失败: %s", result.ErrorMessage)
			}

			log.Info("文件上传成功",
				zap.String("bucket", bucket),
				zap.String("object_key", objectKey),
			)

			return map[string]any{
				"bucket":     bucket,
				"object_key": objectKey,
				"local_path": localPath,
				"message":    fmt.Sprintf("文件已上传到 %s/%s", bucket, objectKey),
			}, "", nil
		},
	}); err != nil {
		return err
	}

	// object.download - 下载文件
	if err := registry.RegisterTool(&Tool{
		Name:        "object.download",
		Description: "[READ][低风险] 从 MinIO Bucket 下载对象到本地",
		Category:    CategoryObject,
		RiskLevel:   "low",
		InputSchema: &InputSchema{
			Type: "object",
			Properties: map[string]*ParameterSchema{
				"bucket": {
					Type:        "string",
					Description: "源 Bucket 名称",
					Required:    true,
				},
				"object_key": {
					Type:        "string",
					Description: "源对象路径（Bucket 内的 key）",
					Required:    true,
				},
				"local_path": {
					Type:        "string",
					Description: "本地目标路径（文件或目录）",
					Required:    true,
				},
			},
			Required: []string{"bucket", "object_key", "local_path"},
		},
		Handler: func(ctx context.Context, args map[string]any, exec *minio.MCExecutor) (any, string, error) {
			bucket, _ := args["bucket"].(string)
			objectKey, _ := args["object_key"].(string)
			localPath, _ := args["local_path"].(string)

			if err := validateBucketName(bucket); err != nil {
				return nil, "", err
			}
			if err := validateObjectPath(objectKey); err != nil {
				return nil, "", err
			}

			// 检查目标目录是否存在
			parentDir := filepath.Dir(localPath)
			if parentDir != "." {
				if _, err := os.Stat(parentDir); os.IsNotExist(err) {
					return nil, "", fmt.Errorf("本地目标路径的父目录不存在: %s", parentDir)
				}
			}

			remotePath := fmt.Sprintf("%s/%s/%s", exec.GetAlias(), bucket, objectKey)
			result := exec.Execute(ctx, "cp", remotePath, localPath)
			if !result.Success {
				return nil, "", fmt.Errorf("下载对象失败: %s", result.ErrorMessage)
			}

			log.Info("对象下载成功",
				zap.String("bucket", bucket),
				zap.String("object_key", objectKey),
			)

			return map[string]any{
				"bucket":     bucket,
				"object_key": objectKey,
				"local_path": localPath,
				"message":    fmt.Sprintf("对象 %s/%s 已下载到 %s", bucket, objectKey, localPath),
			}, "", nil
		},
	}); err != nil {
		return err
	}

	// object.delete - 删除对象
	if err := registry.RegisterTool(&Tool{
		Name:                 "object.delete",
		Description:          "[DELETE][中风险] 删除 MinIO Bucket 中的对象",
		Category:             CategoryObject,
		RequiresConfirmation: true,
		RiskLevel:            "medium",
		InputSchema: &InputSchema{
			Type: "object",
			Properties: map[string]*ParameterSchema{
				"bucket": {
					Type:        "string",
					Description: "Bucket 名称",
					Required:    true,
				},
				"object_key": {
					Type:        "string",
					Description: "对象路径（Bucket 内的 key）",
					Required:    true,
				},
			},
			Required: []string{"bucket", "object_key"},
		},
		Handler: func(ctx context.Context, args map[string]any, exec *minio.MCExecutor) (any, string, error) {
			bucket, _ := args["bucket"].(string)
			objectKey, _ := args["object_key"].(string)

			if err := validateBucketName(bucket); err != nil {
				return nil, "", err
			}
			if err := validateObjectPath(objectKey); err != nil {
				return nil, "", err
			}

			remotePath := fmt.Sprintf("%s/%s/%s", exec.GetAlias(), bucket, objectKey)
			result := exec.Execute(ctx, "rm", remotePath)
			if !result.Success {
				return nil, "", fmt.Errorf("删除对象失败: %s", result.ErrorMessage)
			}

			log.Info("对象删除成功",
				zap.String("bucket", bucket),
				zap.String("object_key", objectKey),
			)

			return map[string]any{
				"bucket":     bucket,
				"object_key": objectKey,
				"message":    fmt.Sprintf("对象 %s/%s 已删除", bucket, objectKey),
			}, "", nil
		},
	}); err != nil {
		return err
	}

	// object.deletePrefix - 递归删除目录（删除指定前缀下的所有对象）
	if err := registry.RegisterTool(&Tool{
		Name:                 "object.deletePrefix",
		Description:          "[DELETE][高风险] 递归删除 MinIO Bucket 中指定前缀下的所有对象",
		Category:             CategoryObject,
		RequiresConfirmation: true,
		RiskLevel:            "high",
		InputSchema: &InputSchema{
			Type: "object",
			Properties: map[string]*ParameterSchema{
				"bucket": {
					Type:        "string",
					Description: "Bucket 名称",
					Required:    true,
				},
				"prefix": {
					Type:        "string",
					Description: "要删除的目录前缀（以 / 结尾）",
					Required:    true,
				},
			},
			Required: []string{"bucket", "prefix"},
		},
		Handler: func(ctx context.Context, args map[string]any, exec *minio.MCExecutor) (any, string, error) {
			bucket, _ := args["bucket"].(string)
			prefix, _ := args["prefix"].(string)

			if err := validateBucketName(bucket); err != nil {
				return nil, "", err
			}
			if prefix == "" {
				return nil, "", fmt.Errorf("目录前缀不能为空")
			}

			// 使用 minio-go SDK 递归删除
			deletedCount, err := s3client.DeleteObjectsWithPrefix(ctx, bucket, prefix)
			if err != nil {
				return nil, "", fmt.Errorf("递归删除目录失败: %w", err)
			}

			log.Info("目录递归删除成功",
				zap.String("bucket", bucket),
				zap.String("prefix", prefix),
				zap.Int("deleted_count", deletedCount),
			)

			return map[string]any{
				"bucket":        bucket,
				"prefix":        prefix,
				"deleted_count": deletedCount,
				"message":       fmt.Sprintf("目录 %s 已删除，共删除 %d 个对象", prefix, deletedCount),
			}, "", nil
		},
	}); err != nil {
		return err
	}

	// object.stat - 查询对象元数据
	if err := registry.RegisterTool(&Tool{
		Name:        "object.stat",
		Description: "[READ][低风险] 查询对象元数据（大小、修改时间、Content-Type、ETag）",
		Category:    CategoryObject,
		RiskLevel:   "low",
		InputSchema: &InputSchema{
			Type: "object",
			Properties: map[string]*ParameterSchema{
				"bucket": {
					Type:        "string",
					Description: "Bucket 名称",
					Required:    true,
				},
				"object_key": {
					Type:        "string",
					Description: "对象路径（Bucket 内的 key）",
					Required:    true,
				},
			},
			Required: []string{"bucket", "object_key"},
		},
		Handler: func(ctx context.Context, args map[string]any, exec *minio.MCExecutor) (any, string, error) {
			bucket, _ := args["bucket"].(string)
			objectKey, _ := args["object_key"].(string)

			if err := validateBucketName(bucket); err != nil {
				return nil, "", err
			}
			if err := validateObjectPath(objectKey); err != nil {
				return nil, "", err
			}

			remotePath := fmt.Sprintf("%s/%s/%s", exec.GetAlias(), bucket, objectKey)
			result := exec.Execute(ctx, "stat", remotePath)
			if !result.Success {
				return nil, "", fmt.Errorf("查询对象元数据失败: %s", result.ErrorMessage)
			}

			metadata := parseStatOutput(result.Data)
			return map[string]any{
				"key":           objectKey,
				"size":          metadata["size"],
				"last_modified": metadata["last_modified"],
				"content_type":  metadata["content_type"],
				"etag":          metadata["etag"],
			}, "", nil
		},
	}); err != nil {
		return err
	}

	// object.list - 列出对象（使用 minio-go S3 API，兼容所有 MinIO 版本）
	if err := registry.RegisterTool(&Tool{
		Name:        "object.list",
		Description: "[READ][低风险] 列出 Bucket 中的对象",
		Category:    CategoryObject,
		RiskLevel:   "low",
		InputSchema: &InputSchema{
			Type: "object",
			Properties: map[string]*ParameterSchema{
				"bucket": {
					Type:        "string",
					Description: "Bucket 名称",
					Required:    true,
				},
				"prefix": {
					Type:        "string",
					Description: "对象前缀（可选）",
				},
				"recursive": {
					Type:        "boolean",
					Description: "是否递归列出",
					Default:     false,
				},
			},
			Required: []string{"bucket"},
		},
		Handler: func(ctx context.Context, args map[string]any, exec *minio.MCExecutor) (any, string, error) {
			bucket, _ := args["bucket"].(string)
			prefix, _ := args["prefix"].(string)
			recursive, _ := args["recursive"].(bool)

			if err := validateBucketName(bucket); err != nil {
				return nil, "", err
			}

			s3Objects, err := s3client.ListObjects(ctx, bucket, prefix, recursive)
			if err != nil {
				return nil, "", fmt.Errorf("列出对象失败: %w", err)
			}

			objects := make([]map[string]any, 0, len(s3Objects))
			for _, obj := range s3Objects {
				item := map[string]any{
					"key":           obj.Key,
					"size":          obj.Size,
					"last_modified": obj.LastModified,
				}
				if obj.IsDir {
					item["type"] = "folder"
				} else {
					item["type"] = "file"
				}
				objects = append(objects, item)
			}

			return map[string]any{
				"bucket":  bucket,
				"prefix":  prefix,
				"objects": objects,
				"total":   len(objects),
			}, "", nil
		},
	}); err != nil {
		return err
	}

	return nil
}

// validateObjectPath 验证对象路径
func validateObjectPath(path string) error {
	if path == "" {
		return fmt.Errorf("对象路径不能为空")
	}
	if strings.HasPrefix(path, "/") {
		return fmt.Errorf("对象路径不能以 / 开头")
	}
	if strings.Contains(path, "..") {
		return fmt.Errorf("对象路径不能包含 ..")
	}
	return nil
}

// validateLocalPath 验证本地路径存在性
func validateLocalPath(path string) error {
	if path == "" {
		return fmt.Errorf("本地路径不能为空")
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return fmt.Errorf("本地文件不存在: %s", path)
	}
	return nil
}

// parseStatOutput 解析 mc stat 输出
func parseStatOutput(data any) map[string]any {
	result := map[string]any{
		"size":          int64(0),
		"last_modified": "",
		"content_type":  "",
		"etag":          "",
	}

	if m, ok := data.(map[string]any); ok {
		if v, ok := m["size"].(float64); ok {
			result["size"] = int64(v)
		} else if v, ok := m["Size"].(float64); ok {
			result["size"] = int64(v)
		}

		if v, ok := m["lastModified"].(string); ok {
			result["last_modified"] = v
		} else if v, ok := m["LastModified"].(string); ok {
			result["last_modified"] = v
		}

		if v, ok := m["contentType"].(string); ok {
			result["content_type"] = v
		} else if v, ok := m["Content-Type"].(string); ok {
			result["content_type"] = v
		} else if v, ok := m["type"].(string); ok {
			result["content_type"] = v
		}

		if v, ok := m["etag"].(string); ok {
			result["etag"] = v
		} else if v, ok := m["ETag"].(string); ok {
			result["etag"] = v
		}
	}

	return result
}
