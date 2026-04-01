package tools

import (
	"context"
	"fmt"
	"sync"

	"minio-mc-mcp/internal/minio"
)

// ToolCategory 工具分类
type ToolCategory string

const (
	CategoryBucket    ToolCategory = "bucket"    // Bucket 管理
	CategoryObject    ToolCategory = "object"    // Object 管理
	CategoryUser      ToolCategory = "user"      // User 管理
	CategoryHealth    ToolCategory = "health"    // 健康检查
	CategoryMigration ToolCategory = "migration" // 数据迁移
)

// ToolHandler 工具处理函数类型
// 返回值: result, warning, error
type ToolHandler func(ctx context.Context, args map[string]any, executor *minio.MCExecutor) (any, string, error)

// ParameterSchema 参数 Schema 定义
type ParameterSchema struct {
	Type        string `json:"type"`
	Description string `json:"description,omitempty"`
	Required    bool   `json:"required,omitempty"`
	Default     any    `json:"default,omitempty"`
	Enum        []any  `json:"enum,omitempty"`
}

// InputSchema 工具输入参数 Schema
type InputSchema struct {
	Type       string                      `json:"type"`
	Properties map[string]*ParameterSchema `json:"properties"`
	Required   []string                    `json:"required,omitempty"`
}

// Tool 工具定义
type Tool struct {
	Name                 string       `json:"name"`
	Description          string       `json:"description"`
	Category             ToolCategory `json:"category"`
	RequiresConfirmation bool         `json:"requiresConfirmation"`
	InputSchema          *InputSchema `json:"inputSchema"`
	Handler              ToolHandler  `json:"-"`
	Example              string       `json:"example,omitempty"`
	RiskLevel            string       `json:"riskLevel,omitempty"`
}

// ToolRegistry 工具注册表
type ToolRegistry struct {
	tools map[string]*Tool
	mu    sync.RWMutex
}

// NewToolRegistry 创建新的工具注册表
func NewToolRegistry() *ToolRegistry {
	return &ToolRegistry{
		tools: make(map[string]*Tool),
	}
}

// RegisterTool 注册工具
func (r *ToolRegistry) RegisterTool(tool *Tool) error {
	if tool == nil {
		return fmt.Errorf("工具定义不能为空")
	}

	if tool.Name == "" {
		return fmt.Errorf("工具名称不能为空")
	}

	if tool.Handler == nil {
		return fmt.Errorf("工具 '%s' 的处理函数不能为空", tool.Name)
	}

	if tool.InputSchema == nil {
		return fmt.Errorf("工具 '%s' 的输入 Schema 不能为空", tool.Name)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.tools[tool.Name]; exists {
		return fmt.Errorf("工具 '%s' 已存在", tool.Name)
	}

	r.tools[tool.Name] = tool
	return nil
}

// GetTool 获取指定工具
func (r *ToolRegistry) GetTool(name string) (*Tool, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	tool, exists := r.tools[name]
	return tool, exists
}

// GetAllTools 获取所有工具
func (r *ToolRegistry) GetAllTools() []*Tool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	tools := make([]*Tool, 0, len(r.tools))
	for _, tool := range r.tools {
		tools = append(tools, tool)
	}
	return tools
}

// GetToolsByCategory 按分类获取工具
func (r *ToolRegistry) GetToolsByCategory(category ToolCategory) []*Tool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	tools := make([]*Tool, 0)
	for _, tool := range r.tools {
		if tool.Category == category {
			tools = append(tools, tool)
		}
	}
	return tools
}

// ExecuteTool 执行工具
func (r *ToolRegistry) ExecuteTool(ctx context.Context, toolName string, args map[string]any, executor *minio.MCExecutor) (any, string, error) {
	tool, exists := r.GetTool(toolName)
	if !exists {
		return nil, "", fmt.Errorf("工具 '%s' 不存在", toolName)
	}

	if err := r.validateArgs(tool, args); err != nil {
		return nil, "", fmt.Errorf("参数验证失败: %w", err)
	}

	return tool.Handler(ctx, args, executor)
}

// validateArgs 验证工具参数
func (r *ToolRegistry) validateArgs(tool *Tool, args map[string]any) error {
	if tool.InputSchema == nil {
		return nil
	}

	// 检查必填参数
	for _, required := range tool.InputSchema.Required {
		if _, exists := args[required]; !exists {
			return fmt.Errorf("缺少必填参数: %s", required)
		}
	}

	// 验证参数类型
	for name, value := range args {
		schema, exists := tool.InputSchema.Properties[name]
		if !exists {
			continue
		}

		if err := r.validateParamType(name, value, schema); err != nil {
			return err
		}
	}

	return nil
}

// validateParamType 验证参数类型
func (r *ToolRegistry) validateParamType(name string, value any, schema *ParameterSchema) error {
	if value == nil {
		if schema.Required {
			return fmt.Errorf("参数 '%s' 不能为空", name)
		}
		return nil
	}

	switch schema.Type {
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("参数 '%s' 应为字符串类型", name)
		}
	case "integer":
		switch value.(type) {
		case int, int32, int64, float64:
		default:
			return fmt.Errorf("参数 '%s' 应为整数类型", name)
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("参数 '%s' 应为布尔类型", name)
		}
	}

	// 验证枚举值
	if len(schema.Enum) > 0 {
		found := false
		for _, enumVal := range schema.Enum {
			if value == enumVal {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("参数 '%s' 的值不在允许的枚举范围内", name)
		}
	}

	return nil
}

// ToolCount 获取已注册工具数量
func (r *ToolRegistry) ToolCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.tools)
}

// HasTool 检查工具是否存在
func (r *ToolRegistry) HasTool(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, exists := r.tools[name]
	return exists
}

// GetToolNames 获取所有工具名称
func (r *ToolRegistry) GetToolNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.tools))
	for name := range r.tools {
		names = append(names, name)
	}
	return names
}
