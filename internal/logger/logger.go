package logger

import (
	"fmt"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// Config 日志配置
type Config struct {
	Level      string // debug, info, warn, error
	Format     string // json, text
	FilePath   string // 日志文件路径，为空则仅输出到 stdout
	MaxSize    int    // 单个日志文件最大大小（MB）
	MaxBackups int    // 保留的旧日志文件数量
	MaxAge     int    // 日志文件保留天数
	Compress   bool   // 是否压缩旧日志
}

// 全局 logger 实例
var (
	globalLogger *zap.Logger
	globalSugar  *zap.SugaredLogger
)

// Init 初始化全局 logger
func Init(cfg *Config) error {
	if cfg == nil {
		cfg = &Config{
			Level:  "info",
			Format: "json",
		}
	}

	level, err := parseLevel(cfg.Level)
	if err != nil {
		return err
	}

	encoder := buildEncoder(cfg.Format)
	cores := buildCores(encoder, level, cfg)

	core := zapcore.NewTee(cores...)
	globalLogger = zap.New(core,
		zap.AddCaller(),
		zap.AddCallerSkip(1),
		zap.AddStacktrace(zapcore.ErrorLevel),
	)
	globalSugar = globalLogger.Sugar()

	return nil
}

// L 获取全局 zap.Logger
func L() *zap.Logger {
	if globalLogger == nil {
		// 未初始化时返回 nop logger，避免 nil panic
		return zap.NewNop()
	}
	return globalLogger
}

// S 获取全局 SugaredLogger
func S() *zap.SugaredLogger {
	if globalSugar == nil {
		return zap.NewNop().Sugar()
	}
	return globalSugar
}

// Sync 刷新缓冲区，应在程序退出前调用
func Sync() {
	if globalLogger != nil {
		_ = globalLogger.Sync()
	}
}

// Named 创建带名称前缀的子 logger
func Named(name string) *zap.Logger {
	return L().Named(name)
}

// With 创建带固定字段的子 logger
func With(fields ...zap.Field) *zap.Logger {
	return L().With(fields...)
}

// parseLevel 解析日志级别字符串
func parseLevel(level string) (zapcore.Level, error) {
	switch level {
	case "debug":
		return zapcore.DebugLevel, nil
	case "info":
		return zapcore.InfoLevel, nil
	case "warn":
		return zapcore.WarnLevel, nil
	case "error":
		return zapcore.ErrorLevel, nil
	default:
		return zapcore.InfoLevel, fmt.Errorf("无效的日志级别: %s", level)
	}
}

// buildEncoder 构建日志编码器
func buildEncoder(format string) zapcore.Encoder {
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.MillisDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	if format == "text" {
		encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		return zapcore.NewConsoleEncoder(encoderConfig)
	}
	return zapcore.NewJSONEncoder(encoderConfig)
}

// buildCores 构建日志输出核心
func buildCores(encoder zapcore.Encoder, level zapcore.Level, cfg *Config) []zapcore.Core {
	cores := []zapcore.Core{
		zapcore.NewCore(encoder, zapcore.AddSync(os.Stdout), level),
	}

	if cfg.FilePath != "" {
		fileWriter := &lumberjack.Logger{
			Filename:   cfg.FilePath,
			MaxSize:    cfg.MaxSize,
			MaxBackups: cfg.MaxBackups,
			MaxAge:     cfg.MaxAge,
			Compress:   cfg.Compress,
		}
		// 文件始终使用 JSON 格式，便于日志采集
		fileEncoder := zapcore.NewJSONEncoder(zapcore.EncoderConfig{
			TimeKey:        "time",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "caller",
			MessageKey:     "msg",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.LowercaseLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.MillisDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		})
		cores = append(cores, zapcore.NewCore(fileEncoder, zapcore.AddSync(fileWriter), level))
	}

	return cores
}
