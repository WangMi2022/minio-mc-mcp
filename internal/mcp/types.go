package mcp

import "time"

// ========== MCP 请求数据类型 ==========

// UserInfo 用户信息
type UserInfo struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Role string `json:"role,omitempty"`
}

// ToolCallRequest 工具调用请求
type ToolCallRequest struct {
	Tool      string         `json:"tool"`
	Name      string         `json:"name,omitempty"`
	Arguments map[string]any `json:"arguments,omitempty"`
	User      *UserInfo      `json:"user,omitempty"`
	RequestID string         `json:"requestId,omitempty"`
}

// GetToolName 获取工具名称，优先使用 Tool 字段
func (r *ToolCallRequest) GetToolName() string {
	if r.Tool != "" {
		return r.Tool
	}
	return r.Name
}

// ========== MCP 响应数据类型 ==========

// ErrorInfo 错误信息
type ErrorInfo struct {
	Type       string `json:"type"`
	Code       string `json:"code"`
	Message    string `json:"message"`
	Details    string `json:"details,omitempty"`
	Suggestion string `json:"suggestion,omitempty"`
}

// ContentItem MCP 标准内容项
type ContentItem struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

// ToolCallResponse 工具调用响应
type ToolCallResponse struct {
	Success   bool          `json:"success"`
	Data      any           `json:"data,omitempty"`
	Content   []ContentItem `json:"content,omitempty"`
	Error     *ErrorInfo    `json:"error,omitempty"`
	RequestID string        `json:"requestId,omitempty"`
	Duration  int64         `json:"duration,omitempty"`
	Warning   string        `json:"warning,omitempty"`
}

// ========== 工具列表响应 ==========

// ToolInfo 工具信息
type ToolInfo struct {
	Name                 string         `json:"name"`
	Description          string         `json:"description"`
	Category             string         `json:"category"`
	RequiresConfirmation bool           `json:"requiresConfirmation"`
	InputSchema          map[string]any `json:"inputSchema"`
	Example              string         `json:"example,omitempty"`
	RiskLevel            string         `json:"riskLevel,omitempty"`
}

// ToolListResponse 工具列表响应
type ToolListResponse struct {
	Tools      []*ToolInfo `json:"tools"`
	TotalCount int         `json:"totalCount"`
	Categories []string    `json:"categories,omitempty"`
}

// ========== 健康检查响应 ==========

// HealthResponse 健康检查响应
type HealthResponse struct {
	Status       string    `json:"status"`
	Version      string    `json:"version"`
	MinioAlias   string    `json:"minioAlias"`
	MinioStatus  string    `json:"minioStatus"`
	Timestamp    time.Time `json:"timestamp"`
	Uptime       int64     `json:"uptime,omitempty"`
	Message      string    `json:"message,omitempty"`
}

// ========== 错误类型常量 ==========

const (
	ErrTypeClient   = "CLIENT_ERROR"
	ErrTypeServer   = "SERVER_ERROR"
	ErrTypeNetwork  = "NETWORK_ERROR"
	ErrTypeAuth     = "AUTH_ERROR"
	ErrTypeNotFound = "NOT_FOUND"
	ErrTypeConflict = "CONFLICT"
	ErrTypeTimeout  = "TIMEOUT"
)

// 错误码定义
const (
	ErrCodeInvalidRequest  = "INVALID_REQUEST"
	ErrCodeInvalidToolName = "INVALID_TOOL_NAME"
	ErrCodeToolNotFound    = "TOOL_NOT_FOUND"
	ErrCodeInvalidArguments = "INVALID_ARGUMENTS"
	ErrCodeMissingArguments = "MISSING_ARGUMENTS"
	ErrCodeUnauthorized    = "UNAUTHORIZED"
	ErrCodeAuthFailed      = "AUTH_FAILED"
	ErrCodeRequestTimeout  = "REQUEST_TIMEOUT"
	ErrCodeBucketNotFound  = "BUCKET_NOT_FOUND"
	ErrCodeObjectNotFound  = "OBJECT_NOT_FOUND"
	ErrCodeBucketExists    = "BUCKET_EXISTS"
	ErrCodePermissionDenied = "PERMISSION_DENIED"
)

// ========== 辅助函数 ==========

// NewErrorInfo 创建错误信息
func NewErrorInfo(errType, code, message string) *ErrorInfo {
	return &ErrorInfo{
		Type:    errType,
		Code:    code,
		Message: message,
	}
}

// WithDetails 添加详细信息
func (e *ErrorInfo) WithDetails(details string) *ErrorInfo {
	e.Details = details
	return e
}

// WithSuggestion 添加修复建议
func (e *ErrorInfo) WithSuggestion(suggestion string) *ErrorInfo {
	e.Suggestion = suggestion
	return e
}

// NewSuccessResponse 创建成功响应
func NewSuccessResponse(data any) *ToolCallResponse {
	return &ToolCallResponse{
		Success: true,
		Data:    data,
	}
}

// NewErrorResponse 创建错误响应
func NewErrorResponse(errInfo *ErrorInfo) *ToolCallResponse {
	return &ToolCallResponse{
		Success: false,
		Error:   errInfo,
	}
}

// WithRequestID 添加请求 ID
func (r *ToolCallResponse) WithRequestID(requestID string) *ToolCallResponse {
	r.RequestID = requestID
	return r
}

// WithDuration 添加执行耗时
func (r *ToolCallResponse) WithDuration(durationMs int64) *ToolCallResponse {
	r.Duration = durationMs
	return r
}

// WithWarning 添加警告信息
func (r *ToolCallResponse) WithWarning(warning string) *ToolCallResponse {
	r.Warning = warning
	return r
}
