package tools

import (
	"context"
	"fmt"

	"minio-mc-mcp/internal/minio"
)

// RegisterHealthTools 注册 Health 检查工具
func RegisterHealthTools(registry *ToolRegistry, executor *minio.MCExecutor, s3client *minio.S3Client) error {
	// health.check - 健康检查（使用 madmin-go SDK）
	if err := registry.RegisterTool(&Tool{
		Name:        "health.check",
		Description: "[READ][低风险] 查询 MinIO 服务器健康状态（版本、运行时间、节点状态、磁盘状态、存储用量、资源统计）",
		Category:    CategoryHealth,
		RiskLevel:   "low",
		InputSchema: &InputSchema{
			Type:       "object",
			Properties: map[string]*ParameterSchema{},
		},
		Handler: func(ctx context.Context, args map[string]any, exec *minio.MCExecutor) (any, string, error) {
			// 通过 madmin-go SDK 一次调用获取服务器完整状态
			serverStatus, err := s3client.GetServerStatus(ctx)
			if err != nil {
				return nil, "", fmt.Errorf("获取服务器信息失败: %w", err)
			}

			// 通过 SDK 获取用户数量
			users, err := s3client.ListUsers(ctx)
			usersCount := 0
			if err == nil {
				usersCount = len(users)
			}

			// 构建节点列表（含 drives 详情和 network）
			nodes := make([]map[string]any, 0, len(serverStatus.Nodes))
			for _, n := range serverStatus.Nodes {
				// 构建 drives 详情
				drives := make([]map[string]any, 0, len(n.Drives))
				for _, d := range n.Drives {
					drives = append(drives, map[string]any{
						"endpoint":        d.Endpoint,
						"drive_path":      d.DrivePath,
						"state":           d.State,
						"total_space":     d.TotalSpace,
						"used_space":      d.UsedSpace,
						"available_space": d.AvailableSpace,
					})
				}

				// network: 对端 endpoint → 连通状态
				network := make(map[string]any)
				for peer, status := range n.Network {
					network[peer] = status
				}

				nodes = append(nodes, map[string]any{
					"endpoint":       n.Endpoint,
					"state":          n.State,
					"uptime":         n.Uptime,
					"version":        n.Version,
					"drives_online":  n.DrivesOnline,
					"drives_offline": n.DrivesOffline,
					"drives":         drives,
					"network":        network,
				})
			}

			healthData := map[string]any{
				"connected":      serverStatus.Connected,
				"server_version": serverStatus.ServerVersion,
				"uptime":         serverStatus.Uptime,
				"nodes":          nodes,
				"total_nodes":    serverStatus.TotalNodes,
				"online_nodes":   serverStatus.OnlineNodes,
				"offline_nodes":  serverStatus.OfflineNodes,
				"total_drives":   serverStatus.TotalDrives,
				"online_drives":  serverStatus.OnlineDrives,
				"offline_drives": serverStatus.OfflineDrives,
				"storage": map[string]any{
					"total":     int64(serverStatus.Storage.Total),
					"used":      int64(serverStatus.Storage.Used),
					"available": int64(serverStatus.Storage.Available),
				},
				"buckets_count": serverStatus.BucketsCount,
				"objects_count": serverStatus.ObjectsCount,
				"users_count":   usersCount,
			}

			warning := ""
			if serverStatus.OfflineNodes > 0 {
				warning = fmt.Sprintf("⚠️ 检测到 %d 个离线节点，请尽快检查节点状态", serverStatus.OfflineNodes)
			}

			return healthData, warning, nil
		},
	}); err != nil {
		return err
	}

	// health.metrics - 获取 Prometheus 指标（仍使用 mc CLI，SDK 无对应接口）
	if err := registry.RegisterTool(&Tool{
		Name:        "health.metrics",
		Description: "[READ][低风险] 获取 MinIO Prometheus 指标数据（原始文本格式）",
		Category:    CategoryHealth,
		RiskLevel:   "low",
		InputSchema: &InputSchema{
			Type: "object",
			Properties: map[string]*ParameterSchema{
				"metrics_type": {
					Type:        "string",
					Description: "指标类型: cluster, node, bucket, resource",
					Default:     "cluster",
					Enum:        []any{"cluster", "node", "bucket", "resource"},
				},
			},
		},
		Handler: healthMetricsHandler,
	}); err != nil {
		return err
	}

	return nil
}

func healthMetricsHandler(ctx context.Context, args map[string]any, exec *minio.MCExecutor) (any, string, error) {
	metricsType, _ := args["metrics_type"].(string)
	if metricsType == "" {
		metricsType = "cluster"
	}

	cmdArgs := []string{"admin", "prometheus", "metrics", exec.GetAlias(), metricsType}
	result := exec.ExecuteRaw(ctx, cmdArgs...)
	if !result.Success {
		return nil, "", fmt.Errorf("获取 Prometheus 指标失败: %s", result.ErrorMessage)
	}

	return map[string]any{
		"metrics_type": metricsType,
		"raw_metrics":  result.RawOutput,
	}, "", nil
}
