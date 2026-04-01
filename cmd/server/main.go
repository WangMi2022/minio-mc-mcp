package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"minio-mc-mcp/internal/audit"
	"minio-mc-mcp/internal/config"
	"minio-mc-mcp/internal/logger"
	"minio-mc-mcp/internal/minio"
	"minio-mc-mcp/internal/server"
	"minio-mc-mcp/internal/tools"
)

const (
	// 服务器版本
	Version = "1.0.7"

	// 构建时间（可通过 -ldflags 注入）
	BuildTime = "2026-02-11"

	// 优雅关闭超时时间
	ShutdownTimeout = 30 * time.Second
)

// Application 应用程序结构，管理所有组件的生命周期
type Application struct {
	config      *config.ServerConfig
	auditLogger *audit.AuditLogger
	mcExecutor  *minio.MCExecutor
	s3Client    *minio.S3Client
	bucketCache *minio.BucketStatsCache
	httpServer  *server.HTTPServer
	metrics     *audit.MetricsCollector
	log         *zap.Logger
}

func main() {
	app := &Application{}

	if err := app.Initialize(); err != nil {
		fmt.Fprintf(os.Stderr, "初始化失败: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	if err := app.Run(); err != nil {
		app.log.Fatal("服务器运行失败", zap.Error(err))
	}
}

// Initialize 初始化应用程序的所有组件
func (app *Application) Initialize() error {
	var err error

	// 1. 加载配置
	app.config, err = config.LoadConfig()
	if err != nil {
		return fmt.Errorf("加载配置失败: %w", err)
	}

	// 2. 初始化 zap logger（必须在其他组件之前）
	if err := logger.Init(&logger.Config{
		Level:      app.config.LogLevel,
		Format:     app.config.LogFormat,
		FilePath:   app.config.LogFile,
		MaxSize:    100,
		MaxBackups: 3,
		MaxAge:     28,
		Compress:   true,
	}); err != nil {
		return fmt.Errorf("初始化日志失败: %w", err)
	}
	app.log = logger.Named("app")
	app.log.Info("配置加载成功", zap.String("listen", app.config.GetListenAddress()))

	// 3. 初始化审计日志
	app.auditLogger, err = app.initAuditLogger()
	if err != nil {
		return fmt.Errorf("初始化审计日志失败: %w", err)
	}
	app.log.Info("审计日志初始化成功")

	// 4. 初始化性能指标收集器
	app.metrics = audit.NewMetricsCollector()
	app.log.Info("性能指标收集器初始化成功")

	// 5. 初始化 MC CLI 执行器
	app.mcExecutor, err = minio.NewMCExecutor(&minio.MCExecutorConfig{
		Alias:     app.config.MinioAlias,
		Endpoint:  app.config.MinioEndpoint,
		AccessKey: app.config.MinioAccessKey,
		SecretKey: app.config.MinioSecretKey,
		Secure:    app.config.MinioSecure,
		MCPath:    app.config.MCPath,
		Timeout:   app.config.CommandTimeout,
	})
	if err != nil {
		return fmt.Errorf("初始化 MC 执行器失败: %w", err)
	}
	app.log.Info("MC 执行器初始化成功", zap.String("alias", app.config.MinioAlias))

	// 6. 初始化 minio-go S3 客户端
	app.s3Client, err = minio.NewS3Client(&minio.S3ClientConfig{
		Endpoint:  app.config.MinioEndpoint,
		AccessKey: app.config.MinioAccessKey,
		SecretKey: app.config.MinioSecretKey,
		Secure:    app.config.MinioSecure,
	})
	if err != nil {
		return fmt.Errorf("初始化 S3 客户端失败: %w", err)
	}
	app.log.Info("S3 客户端初始化成功")

	// 7. 验证 MC CLI 可用性和 MinIO 连接（必须在缓存启动前完成）
	if err := app.mcExecutor.SetupAlias(); err != nil {
		return fmt.Errorf("配置 mc alias 失败: %w", err)
	}
	if !app.mcExecutor.CheckAvailability() {
		return fmt.Errorf("mc CLI 工具不可用")
	}
	if !app.mcExecutor.CheckConnection() {
		return fmt.Errorf("无法连接到 MinIO 服务器")
	}
	app.log.Info("MC CLI 和 MinIO 连接验证通过")

	// 8. 初始化 Bucket 统计缓存（在 alias 配置完成后启动）
	app.bucketCache = minio.NewBucketStatsCache(&minio.BucketStatsCacheConfig{
		S3Client:   app.s3Client,
		MCExecutor: app.mcExecutor,
		Interval:   5 * time.Minute,
	})
	app.bucketCache.Start()
	app.log.Info("Bucket 统计缓存初始化成功")
	// 9. 初始化工具注册表并注册所有工具
	toolRegistry, err := app.initToolRegistry()
	if err != nil {
		return fmt.Errorf("初始化工具注册表失败: %w", err)
	}
	app.log.Info("工具注册成功", zap.Int("count", toolRegistry.ToolCount()))

	// 10. 初始化 HTTP 服务器
	app.httpServer, err = server.NewHTTPServer(&server.HTTPServerConfig{
		Config:       app.config,
		ToolRegistry: toolRegistry,
		MCExecutor:   app.mcExecutor,
		AuditLogger:  app.auditLogger,
		Version:      Version,
		Metrics:      app.metrics,
	})
	if err != nil {
		return fmt.Errorf("初始化 HTTP 服务器失败: %w", err)
	}
	app.log.Info("HTTP 服务器初始化成功")

	return nil
}

// initAuditLogger 初始化审计日志器
func (app *Application) initAuditLogger() (*audit.AuditLogger, error) {
	loggerConfig := &audit.LoggerConfig{
		Level:      app.config.LogLevel,
		Format:     app.config.LogFormat,
		MaxSize:    100,
		MaxBackups: 3,
		MaxAge:     28,
		Compress:   true,
	}

	if app.config.LogFile != "" {
		loggerConfig.Output = "both"
		loggerConfig.FilePath = app.config.LogFile
	} else {
		loggerConfig.Output = "stdout"
	}

	return audit.NewAuditLogger(loggerConfig)
}

// initToolRegistry 初始化工具注册表并注册所有工具
func (app *Application) initToolRegistry() (*tools.ToolRegistry, error) {
	registry := tools.NewToolRegistry()

	if err := tools.RegisterBucketTools(registry, app.mcExecutor, app.s3Client, app.bucketCache); err != nil {
		return nil, fmt.Errorf("注册 Bucket 工具失败: %w", err)
	}
	if err := tools.RegisterObjectTools(registry, app.mcExecutor, app.s3Client); err != nil {
		return nil, fmt.Errorf("注册 Object 工具失败: %w", err)
	}
	if err := tools.RegisterUserTools(registry, app.mcExecutor, app.s3Client); err != nil {
		return nil, fmt.Errorf("注册 User 工具失败: %w", err)
	}
	if err := tools.RegisterHealthTools(registry, app.mcExecutor, app.s3Client); err != nil {
		return nil, fmt.Errorf("注册 Health 工具失败: %w", err)
	}
	if err := tools.RegisterPolicyTools(registry, app.mcExecutor, app.s3Client); err != nil {
		return nil, fmt.Errorf("注册 Policy 工具失败: %w", err)
	}
	if err := tools.RegisterMigrationTools(registry, app.mcExecutor, app.s3Client); err != nil {
		return nil, fmt.Errorf("注册 Migration 工具失败: %w", err)
	}

	return registry, nil
}

// Run 启动服务器并等待关闭信号
func (app *Application) Run() error {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	serverErr := make(chan error, 1)

	go func() {
		app.log.Info("服务器启动",
			zap.String("version", Version),
			zap.String("build", BuildTime),
			zap.String("listen", app.config.GetListenAddress()),
			zap.String("minio_alias", app.config.MinioAlias),
			zap.String("minio_endpoint", app.config.MinioEndpoint),
			zap.String("log_level", app.config.LogLevel),
		)

		if err := app.httpServer.Start(); err != nil {
			serverErr <- err
		}
	}()

	select {
	case sig := <-quit:
		app.log.Info("收到关闭信号", zap.String("signal", sig.String()))
	case err := <-serverErr:
		return fmt.Errorf("服务器异常退出: %w", err)
	}

	return app.Shutdown()
}

// Shutdown 优雅关闭所有组件
func (app *Application) Shutdown() error {
	app.log.Info("正在优雅关闭服务器...")

	ctx, cancel := context.WithTimeout(context.Background(), ShutdownTimeout)
	defer cancel()

	var shutdownErrors []error

	if app.httpServer != nil {
		if err := app.httpServer.Shutdown(ctx); err != nil {
			app.log.Error("关闭 HTTP 服务器失败", zap.Error(err))
			shutdownErrors = append(shutdownErrors, fmt.Errorf("关闭 HTTP 服务器失败: %w", err))
		} else {
			app.log.Info("HTTP 服务器已关闭")
		}
	}

	if app.bucketCache != nil {
		app.bucketCache.Stop()
		app.log.Info("Bucket 统计缓存已关闭")
	}

	if app.auditLogger != nil {
		if err := app.auditLogger.Close(); err != nil {
			app.log.Error("关闭审计日志失败", zap.Error(err))
			shutdownErrors = append(shutdownErrors, fmt.Errorf("关闭审计日志失败: %w", err))
		} else {
			app.log.Info("审计日志已关闭")
		}
	}

	if len(shutdownErrors) > 0 {
		return shutdownErrors[0]
	}

	app.log.Info("服务器已完全关闭")
	return nil
}
