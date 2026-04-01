package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"minio-mc-mcp/internal/audit"
	"minio-mc-mcp/internal/logger"
	"minio-mc-mcp/internal/minio"
	"minio-mc-mcp/internal/tools"

	"go.uber.org/zap"
)

// MCPHandler MCP 协议处理器
type MCPHandler struct {
	toolRegistry *tools.ToolRegistry
	mcExecutor   *minio.MCExecutor
	auditLogger  *audit.AuditLogger
	version      string
	startTime    time.Time
	log          *zap.Logger
}

// MCPHandlerConfig MCP 处理器配置
type MCPHandlerConfig struct {
	Version string
}

// NewMCPHandler 创建新的 MCP 协议处理器
func NewMCPHandler(
	toolRegistry *tools.ToolRegistry,
	mcExecutor *minio.MCExecutor,
	auditLogger *audit.AuditLogger,
	config *MCPHandlerConfig,
) (*MCPHandler, error) {
	if toolRegistry == nil {
		return nil, fmt.Errorf("工具注册表不能为空")
	}
	if mcExecutor == nil {
		return nil, fmt.Errorf("MC 执行器不能为空")
	}

	version := "1.0.0"
	if config != nil && config.Version != "" {
		version = config.Version
	}

	return &MCPHandler{
		toolRegistry: toolRegistry,
		mcExecutor:   mcExecutor,
		auditLogger:  auditLogger,
		version:      version,
		startTime:    time.Now(),
		log:          logger.Named("mcp"),
	}, nil
}

// HandleToolCall 处理工具调用请求
func (h *MCPHandler) HandleToolCall(ctx context.Context, req *ToolCallRequest) *ToolCallResponse {
	startTime := time.Now()

	// 验证请求
	if err := h.ValidateRequest(req); err != nil {
		h.log.Warn("工具调用验证失败",
			zap.String("tool", req.GetToolName()),
			zap.Error(err),
		)
		return h.formatErrorResponse(err, req)
	}

	toolName := req.GetToolName()

	h.log.Info("工具调用开始",
		zap.String("tool", toolName),
		zap.String("request_id", req.RequestID),
	)

	// 执行工具
	result, warning, err := h.toolRegistry.ExecuteTool(ctx, toolName, req.Arguments, h.mcExecutor)

	duration := time.Since(startTime)

	// 记录审计日志
	h.logOperation(req, result, err, duration)

	if err != nil {
		h.log.Warn("工具调用失败",
			zap.String("tool", toolName),
			zap.String("request_id", req.RequestID),
			zap.Duration("duration", duration),
			zap.Error(err),
		)
	} else {
		h.log.Info("工具调用成功",
			zap.String("tool", toolName),
			zap.String("request_id", req.RequestID),
			zap.Duration("duration", duration),
		)
	}

	if warning != "" {
		h.log.Warn("工具调用警告",
			zap.String("tool", toolName),
			zap.String("warning", warning),
		)
	}

	// 格式化响应
	response := h.FormatResponse(result, err)
	response.RequestID = req.RequestID
	response.Duration = duration.Milliseconds()
	if warning != "" {
		response.Warning = warning
	}

	return response
}

// ValidateRequest 验证请求参数
func (h *MCPHandler) ValidateRequest(req *ToolCallRequest) error {
	if req == nil {
		return &ValidationError{
			Code:    ErrCodeInvalidRequest,
			Message: "请求不能为空",
		}
	}

	toolName := req.GetToolName()

	if toolName == "" {
		return &ValidationError{
			Code:    ErrCodeInvalidToolName,
			Message: "工具名称不能为空",
		}
	}

	if !isValidToolName(toolName) {
		return &ValidationError{
			Code:    ErrCodeInvalidToolName,
			Message: fmt.Sprintf("工具名称 '%s' 格式无效，只允许字母、数字、下划线、连字符和点号", toolName),
		}
	}

	if !h.toolRegistry.HasTool(toolName) {
		return &ValidationError{
			Code:    ErrCodeToolNotFound,
			Message: fmt.Sprintf("工具 '%s' 不存在", toolName),
		}
	}

	return nil
}

// FormatResponse 格式化响应
func (h *MCPHandler) FormatResponse(result any, err error) *ToolCallResponse {
	if err != nil {
		return h.formatErrorFromError(err)
	}

	var contentText string
	if result != nil {
		jsonBytes, jsonErr := json.Marshal(result)
		if jsonErr == nil {
			contentText = string(jsonBytes)
		}
	}

	response := &ToolCallResponse{
		Success: true,
		Data:    result,
	}

	if contentText != "" {
		response.Content = []ContentItem{
			{Type: "text", Text: contentText},
		}
	}

	return response
}

// GetToolList 获取工具列表
func (h *MCPHandler) GetToolList(category string) *ToolListResponse {
	var toolList []*tools.Tool

	if category != "" {
		toolList = h.toolRegistry.GetToolsByCategory(tools.ToolCategory(category))
	} else {
		toolList = h.toolRegistry.GetAllTools()
	}

	toolInfos := make([]*ToolInfo, 0, len(toolList))
	for _, tool := range toolList {
		toolInfo := &ToolInfo{
			Name:                 tool.Name,
			Description:          tool.Description,
			Category:             string(tool.Category),
			RequiresConfirmation: tool.RequiresConfirmation,
			Example:              tool.Example,
			RiskLevel:            tool.RiskLevel,
		}

		if tool.InputSchema != nil {
			toolInfo.InputSchema = h.convertInputSchema(tool.InputSchema)
		}

		toolInfos = append(toolInfos, toolInfo)
	}

	categories := []string{
		string(tools.CategoryBucket),
		string(tools.CategoryObject),
		string(tools.CategoryUser),
		string(tools.CategoryHealth),
		string(tools.CategoryMigration),
	}

	return &ToolListResponse{
		Tools:      toolInfos,
		TotalCount: len(toolInfos),
		Categories: categories,
	}
}

// GetHealth 获取健康状态
func (h *MCPHandler) GetHealth() *HealthResponse {
	minioStatus := "healthy"
	if !h.mcExecutor.CheckConnection() {
		minioStatus = "unhealthy"
	}

	uptime := int64(time.Since(h.startTime).Seconds())

	status := "healthy"
	if minioStatus == "unhealthy" {
		status = "degraded"
	}

	return &HealthResponse{
		Status:      status,
		Version:     h.version,
		MinioAlias:  h.mcExecutor.GetAlias(),
		MinioStatus: minioStatus,
		Timestamp:   time.Now(),
		Uptime:      uptime,
	}
}

// GetVersion 获取服务器版本
func (h *MCPHandler) GetVersion() string {
	return h.version
}

// GetUptime 获取服务器运行时间（秒）
func (h *MCPHandler) GetUptime() int64 {
	return int64(time.Since(h.startTime).Seconds())
}

// ========== 私有辅助方法 ==========

func (h *MCPHandler) logOperation(req *ToolCallRequest, result any, err error, duration time.Duration) {
	if h.auditLogger == nil {
		return
	}

	opLog := &audit.OperationLog{
		Timestamp: time.Now(),
		Tool:      req.GetToolName(),
		Arguments: req.Arguments,
		Success:   err == nil,
		Duration:  duration,
	}

	if req.User != nil {
		opLog.User = &audit.UserInfo{
			ID:   req.User.ID,
			Name: req.User.Name,
			Role: req.User.Role,
		}
	}

	if err != nil {
		opLog.Error = err.Error()
	}

	_ = h.auditLogger.LogOperation(opLog)
}

func (h *MCPHandler) formatErrorResponse(err error, req *ToolCallRequest) *ToolCallResponse {
	response := h.formatErrorFromError(err)
	if req != nil {
		response.RequestID = req.RequestID
	}
	return response
}

func (h *MCPHandler) formatErrorFromError(err error) *ToolCallResponse {
	if err == nil {
		return &ToolCallResponse{Success: true}
	}

	if validationErr, ok := err.(*ValidationError); ok {
		return &ToolCallResponse{
			Success: false,
			Error: &ErrorInfo{
				Type:       ErrTypeClient,
				Code:       validationErr.Code,
				Message:    validationErr.Message,
				Suggestion: validationErr.Suggestion,
			},
		}
	}

	errInfo := h.classifyError(err)
	return &ToolCallResponse{
		Success: false,
		Error:   errInfo,
	}
}

func (h *MCPHandler) classifyError(err error) *ErrorInfo {
	errMsg := err.Error()

	switch {
	case strings.Contains(errMsg, "not found") || strings.Contains(errMsg, "不存在"):
		return &ErrorInfo{
			Type:       ErrTypeNotFound,
			Code:       ErrCodeBucketNotFound,
			Message:    errMsg,
			Suggestion: "请检查 Bucket 或对象名称是否正确",
		}

	case strings.Contains(errMsg, "already exists") || strings.Contains(errMsg, "已存在"):
		return &ErrorInfo{
			Type:       ErrTypeConflict,
			Code:       ErrCodeBucketExists,
			Message:    errMsg,
			Suggestion: "资源已存在，请使用其他名称或删除后重新创建",
		}

	case strings.Contains(errMsg, "Access Denied") || strings.Contains(errMsg, "权限"):
		return &ErrorInfo{
			Type:       ErrTypeAuth,
			Code:       ErrCodePermissionDenied,
			Message:    errMsg,
			Suggestion: "请检查 MinIO 访问凭证是否有足够的权限",
		}

	case strings.Contains(errMsg, "timeout") || strings.Contains(errMsg, "超时"):
		return &ErrorInfo{
			Type:       ErrTypeTimeout,
			Code:       ErrCodeRequestTimeout,
			Message:    errMsg,
			Suggestion: "请检查网络连接和 MinIO 服务状态",
		}

	default:
		return &ErrorInfo{
			Type:    ErrTypeServer,
			Code:    "INTERNAL_ERROR",
			Message: errMsg,
		}
	}
}

func (h *MCPHandler) convertInputSchema(schema *tools.InputSchema) map[string]any {
	if schema == nil {
		return nil
	}

	result := map[string]any{
		"type": schema.Type,
	}

	if len(schema.Required) > 0 {
		result["required"] = schema.Required
	}

	if len(schema.Properties) > 0 {
		properties := make(map[string]any)
		for name, prop := range schema.Properties {
			properties[name] = h.convertParameterSchema(prop)
		}
		result["properties"] = properties
	}

	return result
}

func (h *MCPHandler) convertParameterSchema(schema *tools.ParameterSchema) map[string]any {
	if schema == nil {
		return nil
	}

	result := map[string]any{
		"type": schema.Type,
	}

	if schema.Description != "" {
		result["description"] = schema.Description
	}

	if schema.Default != nil {
		result["default"] = schema.Default
	}

	if len(schema.Enum) > 0 {
		result["enum"] = schema.Enum
	}

	return result
}

func isValidToolName(name string) bool {
	if name == "" {
		return false
	}

	for _, c := range name {
		if !((c >= 'a' && c <= 'z') ||
			(c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') ||
			c == '_' || c == '-' || c == '.') {
			return false
		}
	}

	return true
}

// ValidationError 验证错误
type ValidationError struct {
	Code       string
	Message    string
	Suggestion string
}

func (e *ValidationError) Error() string {
	return e.Message
}
