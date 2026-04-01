package server

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"minio-mc-mcp/internal/audit"
	"minio-mc-mcp/internal/config"
	"minio-mc-mcp/internal/mcp"
	"minio-mc-mcp/internal/minio"
	"minio-mc-mcp/internal/tools"
)

// HTTPServer HTTP 服务器结构
type HTTPServer struct {
	router       *gin.Engine
	config       *config.ServerConfig
	mcpHandler   *mcp.MCPHandler
	httpServer   *http.Server
	auditLogger  *audit.AuditLogger
	shutdownOnce sync.Once
	metrics      *audit.MetricsCollector
	log          *zap.Logger
}

// HTTPServerConfig HTTP 服务器配置
type HTTPServerConfig struct {
	Config       *config.ServerConfig
	ToolRegistry *tools.ToolRegistry
	MCExecutor   *minio.MCExecutor
	AuditLogger  *audit.AuditLogger
	Version      string
	Metrics      *audit.MetricsCollector
}

// NewHTTPServer 创建新的 HTTP 服务器
func NewHTTPServer(cfg *HTTPServerConfig) (*HTTPServer, error) {
	if cfg == nil || cfg.Config == nil {
		return nil, fmt.Errorf("服务器配置不能为空")
	}
	if cfg.ToolRegistry == nil {
		return nil, fmt.Errorf("工具注册表不能为空")
	}
	if cfg.MCExecutor == nil {
		return nil, fmt.Errorf("MC 执行器不能为空")
	}

	mcpHandler, err := mcp.NewMCPHandler(
		cfg.ToolRegistry,
		cfg.MCExecutor,
		cfg.AuditLogger,
		&mcp.MCPHandlerConfig{Version: cfg.Version},
	)
	if err != nil {
		return nil, fmt.Errorf("创建 MCP 处理器失败: %w", err)
	}

	if cfg.Config.LogLevel == "debug" {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	router := gin.New()

	metrics := cfg.Metrics
	if metrics == nil {
		metrics = audit.NewMetricsCollector()
	}

	server := &HTTPServer{
		router:      router,
		config:      cfg.Config,
		mcpHandler:  mcpHandler,
		auditLogger: cfg.AuditLogger,
		metrics:     metrics,
		log:         zap.L().Named("http"),
	}

	server.setupMiddleware()
	server.setupRoutes()

	return server, nil
}

// setupMiddleware 配置中间件
func (s *HTTPServer) setupMiddleware() {
	s.router.Use(gin.Recovery())
	s.router.Use(s.loggingMiddleware())
	s.router.Use(s.corsMiddleware())
	s.router.Use(s.methodLimitMiddleware())

	if s.config.APIToken != "" {
		s.router.Use(s.authMiddleware())
	}

	s.router.Use(s.timeoutMiddleware())

	if s.metrics != nil {
		s.router.Use(s.metricsMiddleware())
	}
}

// setupRoutes 配置路由
func (s *HTTPServer) setupRoutes() {
	s.router.POST("/tool", s.handleToolCall)
	s.router.GET("/tools", s.handleListTools)
	s.router.GET("/health", s.handleHealthCheck)
	s.router.GET("/migration/:task_id/progress", s.handleMigrationProgress)

	if s.metrics != nil {
		s.router.GET("/metrics", gin.WrapF(s.metrics.PrometheusHandler()))
	}
}

// handleToolCall 处理工具调用请求
func (s *HTTPServer) handleToolCall(c *gin.Context) {
	var req mcp.ToolCallRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, mcp.NewErrorResponse(
			mcp.NewErrorInfo(mcp.ErrTypeClient, mcp.ErrCodeInvalidRequest, "请求格式无效").
				WithDetails(err.Error()).
				WithSuggestion("请检查请求 JSON 格式是否正确"),
		))
		return
	}

	if req.User == nil {
		userID := c.GetHeader("X-User-ID")
		userName := c.GetHeader("X-User-Name")
		userRole := c.GetHeader("X-User-Role")
		if userID != "" || userName != "" {
			req.User = &mcp.UserInfo{ID: userID, Name: userName, Role: userRole}
		}
	}

	if req.RequestID == "" {
		req.RequestID = c.GetHeader("X-Request-ID")
	}

	ctx := c.Request.Context()
	toolName := req.GetToolName()

	startTime := time.Now()
	response := s.mcpHandler.HandleToolCall(ctx, &req)
	duration := time.Since(startTime)

	if s.metrics != nil {
		s.metrics.RecordToolCall(toolName, response.Success, duration)
	}

	statusCode := http.StatusOK
	if !response.Success && response.Error != nil {
		statusCode = s.getHTTPStatusFromError(response.Error)
	}

	c.JSON(statusCode, response)
}

// handleListTools 处理获取工具列表请求
func (s *HTTPServer) handleListTools(c *gin.Context) {
	category := c.Query("category")
	response := s.mcpHandler.GetToolList(category)
	c.JSON(http.StatusOK, response)
}

// handleHealthCheck 处理健康检查请求
func (s *HTTPServer) handleHealthCheck(c *gin.Context) {
	response := s.mcpHandler.GetHealth()
	c.JSON(http.StatusOK, response)
}

// handleMigrationProgress 处理迁移进度 SSE 请求
func (s *HTTPServer) handleMigrationProgress(c *gin.Context) {
	taskID := c.Param("task_id")
	if taskID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "task_id 不能为空"})
		return
	}

	// 设置 SSE 响应头
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("Access-Control-Allow-Origin", "*")
	c.Header("X-Accel-Buffering", "no") // 禁用 nginx 缓冲

	// 订阅进度更新
	progressCh, unsubscribe, err := tools.SubscribeProgress(taskID)
	if err != nil {
		// 任务不存在，发送错误事件后关闭
		c.SSEvent("error", gin.H{"message": err.Error()})
		c.Writer.Flush()
		return
	}
	defer unsubscribe()

	// 获取初始状态
	status, progress, _ := tools.GetTaskStatus(taskID)

	// 发送初始状态
	c.SSEvent("status", gin.H{
		"task_id":  taskID,
		"status":   status,
		"progress": progress,
	})
	c.Writer.Flush()

	// 如果任务已完成，直接返回
	if status == "completed" || status == "failed" || status == "cancelled" {
		c.SSEvent("done", gin.H{"status": status})
		c.Writer.Flush()
		return
	}

	// 监听进度更新
	clientGone := c.Request.Context().Done()
	ticker := time.NewTicker(30 * time.Second) // 心跳保活
	defer ticker.Stop()

	for {
		select {
		case <-clientGone:
			s.log.Debug("SSE 客户端断开连接", zap.String("task_id", taskID))
			return

		case progress, ok := <-progressCh:
			if !ok {
				// 通道关闭，任务结束
				status, _, _ := tools.GetTaskStatus(taskID)
				c.SSEvent("done", gin.H{"status": status})
				c.Writer.Flush()
				return
			}

			// 发送进度更新
			c.SSEvent("progress", progress)
			c.Writer.Flush()

			// 检查任务是否完成
			status, _, _ := tools.GetTaskStatus(taskID)
			if status == "completed" || status == "failed" || status == "cancelled" {
				c.SSEvent("done", gin.H{"status": status})
				c.Writer.Flush()
				return
			}

		case <-ticker.C:
			// 发送心跳保持连接
			c.SSEvent("heartbeat", gin.H{"time": time.Now().Unix()})
			c.Writer.Flush()
		}
	}
}

// loggingMiddleware 日志中间件
func (s *HTTPServer) loggingMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		startTime := time.Now()
		path := c.Request.URL.Path
		method := c.Request.Method
		clientIP := c.ClientIP()
		requestID := c.GetHeader("X-Request-ID")

		c.Next()

		duration := time.Since(startTime)
		statusCode := c.Writer.Status()

		// 使用 zap 记录 HTTP 请求日志
		fields := []zap.Field{
			zap.String("method", method),
			zap.String("path", path),
			zap.Int("status", statusCode),
			zap.Duration("duration", duration),
			zap.String("client_ip", clientIP),
		}
		if requestID != "" {
			fields = append(fields, zap.String("request_id", requestID))
		}

		if statusCode >= 500 {
			s.log.Error("HTTP 请求", fields...)
		} else if statusCode >= 400 {
			s.log.Warn("HTTP 请求", fields...)
		} else {
			s.log.Info("HTTP 请求", fields...)
		}

		// 审计日志仍然保留，用于操作审计
		if s.auditLogger != nil {
			_ = s.auditLogger.LogOperation(&audit.OperationLog{
				Timestamp: startTime,
				Tool:      fmt.Sprintf("%s %s", method, path),
				Success:   statusCode < 400,
				Duration:  duration,
				Arguments: map[string]any{
					"client_ip":   clientIP,
					"status_code": statusCode,
					"request_id":  requestID,
				},
			})
		}
	}
}

// corsMiddleware CORS 中间件
func (s *HTTPServer) corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		origin := c.Request.Header.Get("Origin")

		allowed := false
		for _, allowedOrigin := range s.config.AllowedOrigins {
			if allowedOrigin == "*" || allowedOrigin == origin {
				allowed = true
				break
			}
		}

		if allowed {
			if origin != "" {
				c.Header("Access-Control-Allow-Origin", origin)
			} else {
				c.Header("Access-Control-Allow-Origin", "*")
			}
		}

		c.Header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-User-ID, X-User-Name, X-User-Role, X-Request-ID")
		c.Header("Access-Control-Max-Age", "86400")
		c.Header("Access-Control-Allow-Credentials", "true")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}

// methodLimitMiddleware HTTP 方法限制中间件
func (s *HTTPServer) methodLimitMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		method := c.Request.Method
		if method != http.MethodGet && method != http.MethodPost && method != http.MethodOptions {
			c.JSON(http.StatusMethodNotAllowed, mcp.NewErrorResponse(
				mcp.NewErrorInfo(mcp.ErrTypeClient, "METHOD_NOT_ALLOWED",
					fmt.Sprintf("不支持的 HTTP 方法: %s", method)).
					WithSuggestion("仅支持 GET 和 POST 方法"),
			))
			c.Abort()
			return
		}
		c.Next()
	}
}

// authMiddleware API Token 认证中间件
func (s *HTTPServer) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 健康检查端点不需要认证
		if c.Request.URL.Path == "/health" {
			c.Next()
			return
		}

		if c.Request.Method == http.MethodOptions {
			c.Next()
			return
		}

		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.JSON(http.StatusUnauthorized, mcp.NewErrorResponse(
				mcp.NewErrorInfo(mcp.ErrTypeAuth, mcp.ErrCodeUnauthorized, "缺少认证信息").
					WithSuggestion("请在 Authorization Header 中提供 Bearer Token"),
			))
			c.Abort()
			return
		}

		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
			c.JSON(http.StatusUnauthorized, mcp.NewErrorResponse(
				mcp.NewErrorInfo(mcp.ErrTypeAuth, mcp.ErrCodeUnauthorized, "认证格式无效").
					WithSuggestion("请使用 Bearer Token 格式: Authorization: Bearer <token>"),
			))
			c.Abort()
			return
		}

		token := parts[1]
		if token != s.config.APIToken {
			c.JSON(http.StatusUnauthorized, mcp.NewErrorResponse(
				mcp.NewErrorInfo(mcp.ErrTypeAuth, mcp.ErrCodeAuthFailed, "认证失败").
					WithSuggestion("请检查 API Token 是否正确"),
			))
			c.Abort()
			return
		}

		c.Next()
	}
}

// timeoutMiddleware 请求超时中间件
func (s *HTTPServer) timeoutMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// WebSocket 端点不设置超时
		if strings.HasPrefix(c.Request.URL.Path, "/migration/") && strings.HasSuffix(c.Request.URL.Path, "/progress") {
			c.Next()
			return
		}

		ctx, cancel := context.WithTimeout(c.Request.Context(), s.config.RequestTimeout)
		defer cancel()

		c.Request = c.Request.WithContext(ctx)

		done := make(chan struct{})
		go func() {
			c.Next()
			close(done)
		}()

		select {
		case <-done:
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				c.JSON(http.StatusGatewayTimeout, mcp.NewErrorResponse(
					mcp.NewErrorInfo(mcp.ErrTypeTimeout, mcp.ErrCodeRequestTimeout, "请求超时").
						WithSuggestion(fmt.Sprintf("请求处理时间超过 %v，请稍后重试", s.config.RequestTimeout)),
				))
				c.Abort()
			}
		}
	}
}

// metricsMiddleware 指标收集中间件
func (s *HTTPServer) metricsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		startTime := time.Now()

		s.metrics.IncrementConcurrent()
		defer s.metrics.DecrementConcurrent()

		c.Next()

		duration := time.Since(startTime)
		success := c.Writer.Status() < 400
		s.metrics.RecordRequest(success, duration)

		if !success {
			statusCode := c.Writer.Status()
			var errType string
			switch {
			case statusCode >= 500:
				errType = "SERVER_ERROR"
			case statusCode == 429:
				errType = "RATE_LIMIT"
			case statusCode == 408 || statusCode == 504:
				errType = "TIMEOUT"
			case statusCode == 401 || statusCode == 403:
				errType = "AUTH_ERROR"
			case statusCode == 404:
				errType = "NOT_FOUND"
			default:
				errType = "CLIENT_ERROR"
			}
			s.metrics.RecordError(errType)
		}
	}
}

// getHTTPStatusFromError 根据错误类型获取 HTTP 状态码
func (s *HTTPServer) getHTTPStatusFromError(errInfo *mcp.ErrorInfo) int {
	if errInfo == nil {
		return http.StatusInternalServerError
	}

	switch errInfo.Type {
	case mcp.ErrTypeClient:
		return http.StatusBadRequest
	case mcp.ErrTypeAuth:
		if errInfo.Code == mcp.ErrCodeUnauthorized {
			return http.StatusUnauthorized
		}
		return http.StatusForbidden
	case mcp.ErrTypeNotFound:
		return http.StatusNotFound
	case mcp.ErrTypeConflict:
		return http.StatusConflict
	case mcp.ErrTypeTimeout:
		return http.StatusGatewayTimeout
	case mcp.ErrTypeNetwork:
		return http.StatusBadGateway
	case mcp.ErrTypeServer:
		return http.StatusInternalServerError
	default:
		return http.StatusInternalServerError
	}
}

// Start 启动 HTTP 服务器
func (s *HTTPServer) Start() error {
	addr := s.config.GetListenAddress()

	s.httpServer = &http.Server{
		Addr:         addr,
		Handler:      s.router,
		ReadTimeout:  s.config.RequestTimeout,
		WriteTimeout: s.config.RequestTimeout + 5*time.Second,
		IdleTimeout:  120 * time.Second,
	}

	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("HTTP 服务器启动失败: %w", err)
	}

	return nil
}

// Shutdown 优雅关闭服务器
func (s *HTTPServer) Shutdown(ctx context.Context) error {
	var shutdownErr error

	s.shutdownOnce.Do(func() {
		if s.httpServer != nil {
			shutdownErr = s.httpServer.Shutdown(ctx)
		}
	})

	return shutdownErr
}

// GetRouter 获取 Gin 路由器（用于测试）
func (s *HTTPServer) GetRouter() *gin.Engine {
	return s.router
}

// GetConfig 获取服务器配置
func (s *HTTPServer) GetConfig() *config.ServerConfig {
	return s.config
}

// GetMCPHandler 获取 MCP 处理器
func (s *HTTPServer) GetMCPHandler() *mcp.MCPHandler {
	return s.mcpHandler
}

// GetMetrics 获取性能指标收集器
func (s *HTTPServer) GetMetrics() *audit.MetricsCollector {
	return s.metrics
}
