package minio

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"minio-mc-mcp/internal/logger"
	"os/exec"
	"strings"
	"time"

	"go.uber.org/zap"
)

// MCExecutorConfig MC 执行器配置
type MCExecutorConfig struct {
	Alias     string
	Endpoint  string
	AccessKey string
	SecretKey string
	Secure    bool
	MCPath    string
	Timeout   time.Duration
}

// MCExecutor MC CLI 执行器
type MCExecutor struct {
	alias     string
	endpoint  string
	accessKey string
	secretKey string
	secure    bool
	mcPath    string
	timeout   time.Duration
	log       *zap.Logger
}

// MCResult mc 命令执行结果
type MCResult struct {
	Success      bool
	Data         any
	ErrorCode    string
	ErrorMessage string
	RawOutput    string
}

// NewMCExecutor 创建新的 MC 执行器
func NewMCExecutor(cfg *MCExecutorConfig) (*MCExecutor, error) {
	if cfg == nil {
		return nil, fmt.Errorf("配置不能为空")
	}

	mcPath := cfg.MCPath
	if mcPath == "" {
		mcPath = "mc"
	}

	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	return &MCExecutor{
		alias:     cfg.Alias,
		endpoint:  cfg.Endpoint,
		accessKey: cfg.AccessKey,
		secretKey: cfg.SecretKey,
		secure:    cfg.Secure,
		mcPath:    mcPath,
		timeout:   timeout,
		log:       logger.Named("mc-executor"),
	}, nil
}

// GetAlias 获取 alias 名称
func (e *MCExecutor) GetAlias() string {
	return e.alias
}

// GetEndpoint 获取 MinIO endpoint
func (e *MCExecutor) GetEndpoint() string {
	return e.endpoint
}

// GetMCPath 获取 mc 可执行文件路径
func (e *MCExecutor) GetMCPath() string {
	return e.mcPath
}

// SetupAlias 配置 mc alias
func (e *MCExecutor) SetupAlias() error {
	scheme := "http"
	if e.secure {
		scheme = "https"
	}
	endpointURL := fmt.Sprintf("%s://%s", scheme, e.endpoint)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, e.mcPath, "alias", "set", e.alias, endpointURL, e.accessKey, e.secretKey)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("配置 mc alias 失败: %s", stderr.String())
	}

	return nil
}

// CheckAvailability 检查 mc CLI 是否可用
func (e *MCExecutor) CheckAvailability() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, e.mcPath, "--version")
	return cmd.Run() == nil
}

// CheckConnection 检查 MinIO 服务器连接状态
func (e *MCExecutor) CheckConnection() bool {
	result := e.Execute(context.Background(), "ls", e.alias+"/")
	return result.Success
}

// Execute 执行 mc 命令
func (e *MCExecutor) Execute(ctx context.Context, args ...string) *MCResult {
	if ctx == nil {
		ctx = context.Background()
	}

	// 添加超时
	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()

	// 构建命令：mc --json [args...]
	cmdArgs := append([]string{"--json"}, args...)
	cmd := exec.CommandContext(ctx, e.mcPath, cmdArgs...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	startTime := time.Now()
	err := cmd.Run()
	duration := time.Since(startTime)
	rawOutput := stdout.String()

	// 构建安全的日志参数（隐藏敏感信息）
	safeArgs := sanitizeArgs(args)

	if ctx.Err() == context.DeadlineExceeded {
		e.log.Warn("命令执行超时",
			zap.Strings("args", safeArgs),
			zap.Duration("timeout", e.timeout),
			zap.Duration("duration", duration),
		)
		return &MCResult{
			Success:      false,
			ErrorCode:    "EXEC_TIMEOUT",
			ErrorMessage: fmt.Sprintf("命令执行超时（%v）", e.timeout),
			RawOutput:    rawOutput,
		}
	}

	if err != nil {
		errOutput := stderr.String()
		if errOutput == "" {
			errOutput = rawOutput
		}
		e.log.Warn("命令执行失败",
			zap.Strings("args", safeArgs),
			zap.Duration("duration", duration),
			zap.String("error", parseErrorMessage(errOutput)),
		)
		return &MCResult{
			Success:      false,
			ErrorCode:    "EXEC_FAILED",
			ErrorMessage: parseErrorMessage(errOutput),
			RawOutput:    errOutput,
		}
	}

	e.log.Debug("命令执行成功",
		zap.Strings("args", safeArgs),
		zap.Duration("duration", duration),
	)

	// 解析输出
	data := parseOutput(rawOutput)
	return &MCResult{
		Success:   true,
		Data:      data,
		RawOutput: rawOutput,
	}
}

// ExecuteRaw 执行 mc 命令（不带 --json 标志）
func (e *MCExecutor) ExecuteRaw(ctx context.Context, args ...string) *MCResult {
	if ctx == nil {
		ctx = context.Background()
	}

	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, e.mcPath, args...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	startTime := time.Now()
	err := cmd.Run()
	duration := time.Since(startTime)
	rawOutput := stdout.String()

	safeArgs := sanitizeArgs(args)

	if ctx.Err() == context.DeadlineExceeded {
		e.log.Warn("Raw 命令执行超时",
			zap.Strings("args", safeArgs),
			zap.Duration("timeout", e.timeout),
			zap.Duration("duration", duration),
		)
		return &MCResult{
			Success:      false,
			ErrorCode:    "EXEC_TIMEOUT",
			ErrorMessage: fmt.Sprintf("命令执行超时（%v）", e.timeout),
			RawOutput:    rawOutput,
		}
	}

	if err != nil {
		errOutput := stderr.String()
		if errOutput == "" {
			errOutput = rawOutput
		}
		e.log.Warn("Raw 命令执行失败",
			zap.Strings("args", safeArgs),
			zap.Duration("duration", duration),
			zap.String("error", parseErrorMessage(errOutput)),
		)
		return &MCResult{
			Success:      false,
			ErrorCode:    "EXEC_FAILED",
			ErrorMessage: parseErrorMessage(errOutput),
			RawOutput:    errOutput,
		}
	}

	e.log.Debug("Raw 命令执行成功",
		zap.Strings("args", safeArgs),
		zap.Duration("duration", duration),
	)

	return &MCResult{
		Success:   true,
		Data:      map[string]any{"raw": rawOutput},
		RawOutput: rawOutput,
	}
}

// parseOutput 解析 mc CLI 的 stdout 输出为结构化数据
func parseOutput(stdout string) any {
	if stdout == "" {
		return nil
	}

	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	var parsedItems []map[string]any

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var parsed map[string]any
		if err := json.Unmarshal([]byte(line), &parsed); err != nil {
			parsedItems = append(parsedItems, map[string]any{"raw": line})
		} else {
			parsedItems = append(parsedItems, parsed)
		}
	}

	if len(parsedItems) == 0 {
		return nil
	}
	if len(parsedItems) == 1 {
		return parsedItems[0]
	}
	return parsedItems
}

// parseErrorMessage 解析错误消息
func parseErrorMessage(output string) string {
	// 尝试解析 JSON 格式的错误
	var errData map[string]any
	if err := json.Unmarshal([]byte(output), &errData); err == nil {
		if msg, ok := errData["error"].(map[string]any); ok {
			if message, ok := msg["message"].(string); ok {
				return message
			}
		}
		if msg, ok := errData["message"].(string); ok {
			return msg
		}
	}

	// 返回原始输出
	output = strings.TrimSpace(output)
	if output == "" {
		return "未知错误"
	}
	return output
}

// sanitizeArgs 清理命令参数中的敏感信息，避免密码等泄露到日志
func sanitizeArgs(args []string) []string {
	safe := make([]string, len(args))
	copy(safe, args)

	// alias set <name> <url> <accessKey> <secretKey> → 隐藏后两个参数
	for i, arg := range safe {
		if arg == "set" && i > 0 && safe[i-1] == "alias" {
			// alias set 后面依次是 name, url, accessKey, secretKey
			if i+4 < len(safe) {
				safe[i+3] = "***"
				safe[i+4] = "***"
			}
			break
		}
	}

	// admin user add <alias> <username> <password> → 隐藏密码
	for i, arg := range safe {
		if arg == "add" && i >= 2 && safe[i-1] == "user" && safe[i-2] == "admin" {
			if i+2 < len(safe) {
				safe[i+2] = "***"
			}
			break
		}
	}

	return safe
}
