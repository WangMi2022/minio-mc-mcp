package tools

import (
	"context"
	"fmt"
	"regexp"

	"minio-mc-mcp/internal/logger"
	"minio-mc-mcp/internal/minio"

	"go.uber.org/zap"
)

var usernameRegex = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_\-]{2,31}$`)

// RegisterUserTools 注册 User 管理工具
func RegisterUserTools(registry *ToolRegistry, executor *minio.MCExecutor, s3client *minio.S3Client) error {
	log := logger.Named("tools.user")
	// user.list - 列出所有用户（使用 madmin-go SDK）
	if err := registry.RegisterTool(&Tool{
		Name:        "user.list",
		Description: "[READ][低风险] 列出所有 MinIO 用户",
		Category:    CategoryUser,
		RiskLevel:   "low",
		InputSchema: &InputSchema{
			Type:       "object",
			Properties: map[string]*ParameterSchema{},
		},
		Handler: func(ctx context.Context, args map[string]any, exec *minio.MCExecutor) (any, string, error) {
			usersMap, err := s3client.ListUsers(ctx)
			if err != nil {
				return nil, "", fmt.Errorf("列出用户失败: %w", err)
			}

			users := make([]map[string]any, 0, len(usersMap))
			for accessKey, info := range usersMap {
				user := map[string]any{
					"username": accessKey,
					"status":   string(info.Status),
				}
				if len(info.MemberOf) > 0 {
					groups := make([]string, 0, len(info.MemberOf))
					for _, g := range info.MemberOf {
						if g != "" {
							groups = append(groups, g)
						}
					}
					user["member_of"] = groups
				}
				users = append(users, user)
			}

			return map[string]any{
				"users": users,
				"total": len(users),
			}, "", nil
		},
	}); err != nil {
		return err
	}

	// user.info - 查询用户信息
	if err := registry.RegisterTool(&Tool{
		Name:        "user.info",
		Description: "[READ][低风险] 查询 MinIO 用户详细信息",
		Category:    CategoryUser,
		RiskLevel:   "low",
		InputSchema: &InputSchema{
			Type: "object",
			Properties: map[string]*ParameterSchema{
				"username": {
					Type:        "string",
					Description: "用户名",
					Required:    true,
				},
			},
			Required: []string{"username"},
		},
		Handler: func(ctx context.Context, args map[string]any, exec *minio.MCExecutor) (any, string, error) {
			username, _ := args["username"].(string)
			if err := validateUsername(username); err != nil {
				return nil, "", err
			}

			result := exec.Execute(ctx, "admin", "user", "info", exec.GetAlias(), username)
			if !result.Success {
				return nil, "", fmt.Errorf("查询用户信息失败: %s", result.ErrorMessage)
			}

			return map[string]any{
				"username": username,
				"info":     result.Data,
			}, "", nil
		},
	}); err != nil {
		return err
	}

	// user.create - 创建用户
	if err := registry.RegisterTool(&Tool{
		Name:        "user.create",
		Description: "[CREATE][中风险] 创建 MinIO 用户",
		Category:    CategoryUser,
		RiskLevel:   "medium",
		InputSchema: &InputSchema{
			Type: "object",
			Properties: map[string]*ParameterSchema{
				"username": {
					Type:        "string",
					Description: "用户名，3-32 字符，字母数字下划线连字符",
					Required:    true,
				},
				"password": {
					Type:        "string",
					Description: "密码，至少 8 字符",
					Required:    true,
				},
			},
			Required: []string{"username", "password"},
		},
		Handler: func(ctx context.Context, args map[string]any, exec *minio.MCExecutor) (any, string, error) {
			username, _ := args["username"].(string)
			password, _ := args["password"].(string)

			if err := validateUsername(username); err != nil {
				return nil, "", err
			}
			if len(password) < 8 {
				return nil, "", fmt.Errorf("密码长度至少为 8 字符")
			}

			result := exec.Execute(ctx, "admin", "user", "add", exec.GetAlias(), username, password)
			if !result.Success {
				return nil, "", fmt.Errorf("创建用户失败: %s", result.ErrorMessage)
			}

			log.Info("用户创建成功", zap.String("username", username))

			return map[string]any{
				"username": username,
				"message":  fmt.Sprintf("用户 '%s' 创建成功", username),
			}, "", nil
		},
	}); err != nil {
		return err
	}

	// user.delete - 删除用户
	if err := registry.RegisterTool(&Tool{
		Name:                 "user.delete",
		Description:          "[DELETE][高风险] 删除 MinIO 用户",
		Category:             CategoryUser,
		RequiresConfirmation: true,
		RiskLevel:            "high",
		InputSchema: &InputSchema{
			Type: "object",
			Properties: map[string]*ParameterSchema{
				"username": {
					Type:        "string",
					Description: "用户名",
					Required:    true,
				},
			},
			Required: []string{"username"},
		},
		Handler: func(ctx context.Context, args map[string]any, exec *minio.MCExecutor) (any, string, error) {
			username, _ := args["username"].(string)
			if err := validateUsername(username); err != nil {
				return nil, "", err
			}

			result := exec.Execute(ctx, "admin", "user", "remove", exec.GetAlias(), username)
			if !result.Success {
				return nil, "", fmt.Errorf("删除用户失败: %s", result.ErrorMessage)
			}

			log.Info("用户删除成功", zap.String("username", username))

			return map[string]any{
				"username": username,
				"message":  fmt.Sprintf("用户 '%s' 删除成功", username),
			}, "", nil
		},
	}); err != nil {
		return err
	}

	// user.enable - 启用用户
	if err := registry.RegisterTool(&Tool{
		Name:        "user.enable",
		Description: "[UPDATE][低风险] 启用 MinIO 用户",
		Category:    CategoryUser,
		RiskLevel:   "low",
		InputSchema: &InputSchema{
			Type: "object",
			Properties: map[string]*ParameterSchema{
				"username": {
					Type:        "string",
					Description: "用户名",
					Required:    true,
				},
			},
			Required: []string{"username"},
		},
		Handler: func(ctx context.Context, args map[string]any, exec *minio.MCExecutor) (any, string, error) {
			username, _ := args["username"].(string)
			if err := validateUsername(username); err != nil {
				return nil, "", err
			}

			result := exec.Execute(ctx, "admin", "user", "enable", exec.GetAlias(), username)
			if !result.Success {
				return nil, "", fmt.Errorf("启用用户失败: %s", result.ErrorMessage)
			}

			log.Info("用户已启用", zap.String("username", username))

			return map[string]any{
				"username": username,
				"status":   "enabled",
				"message":  fmt.Sprintf("用户 '%s' 已启用", username),
			}, "", nil
		},
	}); err != nil {
		return err
	}

	// user.disable - 禁用用户
	if err := registry.RegisterTool(&Tool{
		Name:        "user.disable",
		Description: "[UPDATE][中风险] 禁用 MinIO 用户",
		Category:    CategoryUser,
		RiskLevel:   "medium",
		InputSchema: &InputSchema{
			Type: "object",
			Properties: map[string]*ParameterSchema{
				"username": {
					Type:        "string",
					Description: "用户名",
					Required:    true,
				},
			},
			Required: []string{"username"},
		},
		Handler: func(ctx context.Context, args map[string]any, exec *minio.MCExecutor) (any, string, error) {
			username, _ := args["username"].(string)
			if err := validateUsername(username); err != nil {
				return nil, "", err
			}

			result := exec.Execute(ctx, "admin", "user", "disable", exec.GetAlias(), username)
			if !result.Success {
				return nil, "", fmt.Errorf("禁用用户失败: %s", result.ErrorMessage)
			}

			log.Info("用户已禁用", zap.String("username", username))

			return map[string]any{
				"username": username,
				"status":   "disabled",
				"message":  fmt.Sprintf("用户 '%s' 已禁用", username),
			}, "", nil
		},
	}); err != nil {
		return err
	}

	// user.attachPolicy - 绑定策略到用户
	if err := registry.RegisterTool(&Tool{
		Name:        "user.attachPolicy",
		Description: "[UPDATE][中风险] 将策略绑定到 MinIO 用户，限定到指定 Bucket",
		Category:    CategoryUser,
		RiskLevel:   "medium",
		InputSchema: &InputSchema{
			Type: "object",
			Properties: map[string]*ParameterSchema{
				"username": {
					Type:        "string",
					Description: "用户名",
					Required:    true,
				},
				"policy": {
					Type:        "string",
					Description: "策略名称（如 readonly、readwrite、consoleAdmin）",
					Required:    true,
				},
				"bucket": {
					Type:        "string",
					Description: "策略范围限定的 Bucket 名称",
					Required:    true,
				},
			},
			Required: []string{"username", "policy", "bucket"},
		},
		Handler: func(ctx context.Context, args map[string]any, exec *minio.MCExecutor) (any, string, error) {
			username, _ := args["username"].(string)
			policy, _ := args["policy"].(string)
			bucket, _ := args["bucket"].(string)

			if err := validateUsername(username); err != nil {
				return nil, "", err
			}
			if policy == "" {
				return nil, "", fmt.Errorf("策略名称不能为空")
			}
			if bucket == "" {
				return nil, "", fmt.Errorf("Bucket 名称不能为空")
			}

			// mc admin policy attach <alias> <policy> --user=<username>
			result := exec.Execute(ctx, "admin", "policy", "attach", exec.GetAlias(), policy, fmt.Sprintf("--user=%s", username))
			if !result.Success {
				return nil, "", fmt.Errorf("绑定策略失败: %s", result.ErrorMessage)
			}

			log.Info("策略绑定成功",
				zap.String("username", username),
				zap.String("policy", policy),
				zap.String("bucket", bucket),
			)

			warning := ""
			if isAdminPolicy(policy) {
				warning = fmt.Sprintf("⚠️ 高风险操作：已将 admin 级别策略 '%s' 分配给用户 '%s'", policy, username)
			}

			return map[string]any{
				"username": username,
				"policy":   policy,
				"bucket":   bucket,
				"message":  fmt.Sprintf("策略 '%s' 已绑定到用户 '%s'，范围: Bucket '%s'", policy, username, bucket),
			}, warning, nil
		},
	}); err != nil {
		return err
	}

	return nil
}

// isAdminPolicy 判断是否为管理员级别策略
func isAdminPolicy(policy string) bool {
	switch policy {
	case "consoleAdmin", "diagnostics", "writeall":
		return true
	}
	return false
}

// RegisterPolicyTools 注册策略管理工具
func RegisterPolicyTools(registry *ToolRegistry, executor *minio.MCExecutor, s3client *minio.S3Client) error {
	// policy.list - 列出所有 IAM 策略（使用 madmin-go SDK）
	if err := registry.RegisterTool(&Tool{
		Name:        "policy.list",
		Description: "[READ][低风险] 列出所有 MinIO IAM 策略",
		Category:    CategoryUser,
		RiskLevel:   "low",
		InputSchema: &InputSchema{
			Type:       "object",
			Properties: map[string]*ParameterSchema{},
		},
		Handler: func(ctx context.Context, args map[string]any, exec *minio.MCExecutor) (any, string, error) {
			policiesMap, err := s3client.ListCannedPolicies(ctx)
			if err != nil {
				return nil, "", fmt.Errorf("列出策略失败: %w", err)
			}

			policies := make([]map[string]any, 0, len(policiesMap))
			for name, rawPolicy := range policiesMap {
				policies = append(policies, map[string]any{
					"name":   name,
					"policy": string(rawPolicy),
				})
			}

			return map[string]any{
				"policies": policies,
				"total":    len(policies),
			}, "", nil
		},
	}); err != nil {
		return err
	}

	// policy.info - 获取单个 IAM 策略详情（使用 madmin-go SDK）
	if err := registry.RegisterTool(&Tool{
		Name:        "policy.info",
		Description: "[READ][低风险] 获取指定 MinIO IAM 策略的详细 JSON 内容",
		Category:    CategoryUser,
		RiskLevel:   "low",
		InputSchema: &InputSchema{
			Type: "object",
			Properties: map[string]*ParameterSchema{
				"name": {
					Type:        "string",
					Description: "策略名称",
					Required:    true,
				},
			},
			Required: []string{"name"},
		},
		Handler: func(ctx context.Context, args map[string]any, exec *minio.MCExecutor) (any, string, error) {
			name, _ := args["name"].(string)
			if name == "" {
				return nil, "", fmt.Errorf("策略名称不能为空")
			}

			rawPolicy, err := s3client.InfoCannedPolicy(ctx, name)
			if err != nil {
				return nil, "", fmt.Errorf("获取策略详情失败: %w", err)
			}

			return map[string]any{
				"name":   name,
				"policy": string(rawPolicy),
			}, "", nil
		},
	}); err != nil {
		return err
	}

	return nil
}

// validateUsername 验证用户名
func validateUsername(username string) error {
	if username == "" {
		return fmt.Errorf("用户名不能为空")
	}
	if len(username) < 3 || len(username) > 32 {
		return fmt.Errorf("用户名长度必须在 3-32 字符之间")
	}
	if !usernameRegex.MatchString(username) {
		return fmt.Errorf("用户名格式无效，仅支持字母、数字、下划线和连字符")
	}
	return nil
}
