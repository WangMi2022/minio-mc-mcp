package audit

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"
)

// LoggerConfig 日志配置
type LoggerConfig struct {
	Level      string
	Format     string
	Output     string
	FilePath   string
	MaxSize    int
	MaxBackups int
	MaxAge     int
	Compress   bool
}

// UserInfo 用户信息
type UserInfo struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Role string `json:"role,omitempty"`
}

// OperationLog 操作日志
type OperationLog struct {
	Timestamp time.Time      `json:"timestamp"`
	Tool      string         `json:"tool"`
	Arguments map[string]any `json:"arguments,omitempty"`
	User      *UserInfo      `json:"user,omitempty"`
	Success   bool           `json:"success"`
	Duration  time.Duration  `json:"duration"`
	Error     string         `json:"error,omitempty"`
}

// AuditLogger 审计日志器
type AuditLogger struct {
	config *LoggerConfig
	writer io.Writer
	mu     sync.Mutex
	closed bool
}

// NewAuditLogger 创建审计日志器
func NewAuditLogger(config *LoggerConfig) (*AuditLogger, error) {
	if config == nil {
		config = &LoggerConfig{
			Level:  "info",
			Format: "json",
			Output: "stdout",
		}
	}

	var writer io.Writer

	switch config.Output {
	case "stdout":
		writer = os.Stdout
	case "file":
		if config.FilePath == "" {
			return nil, fmt.Errorf("日志文件路径不能为空")
		}
		writer = &lumberjack.Logger{
			Filename:   config.FilePath,
			MaxSize:    config.MaxSize,
			MaxBackups: config.MaxBackups,
			MaxAge:     config.MaxAge,
			Compress:   config.Compress,
		}
	case "both":
		if config.FilePath == "" {
			return nil, fmt.Errorf("日志文件路径不能为空")
		}
		fileWriter := &lumberjack.Logger{
			Filename:   config.FilePath,
			MaxSize:    config.MaxSize,
			MaxBackups: config.MaxBackups,
			MaxAge:     config.MaxAge,
			Compress:   config.Compress,
		}
		writer = io.MultiWriter(os.Stdout, fileWriter)
	default:
		writer = os.Stdout
	}

	return &AuditLogger{
		config: config,
		writer: writer,
	}, nil
}

// LogOperation 记录操作日志
func (l *AuditLogger) LogOperation(log *OperationLog) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return fmt.Errorf("日志器已关闭")
	}

	if l.config.Format == "json" {
		data, err := json.Marshal(log)
		if err != nil {
			return err
		}
		_, err = l.writer.Write(append(data, '\n'))
		return err
	}

	// text 格式
	line := fmt.Sprintf("[%s] tool=%s success=%v duration=%v",
		log.Timestamp.Format(time.RFC3339),
		log.Tool,
		log.Success,
		log.Duration,
	)
	if log.Error != "" {
		line += fmt.Sprintf(" error=%s", log.Error)
	}
	_, err := l.writer.Write([]byte(line + "\n"))
	return err
}

// LogError 记录错误日志
func (l *AuditLogger) LogError(err error, message string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return fmt.Errorf("日志器已关闭")
	}

	log := map[string]any{
		"timestamp": time.Now().Format(time.RFC3339),
		"level":     "error",
		"message":   message,
		"error":     err.Error(),
	}

	if l.config.Format == "json" {
		data, jsonErr := json.Marshal(log)
		if jsonErr != nil {
			return jsonErr
		}
		_, writeErr := l.writer.Write(append(data, '\n'))
		return writeErr
	}

	line := fmt.Sprintf("[%s] ERROR: %s - %v\n", log["timestamp"], message, err)
	_, writeErr := l.writer.Write([]byte(line))
	return writeErr
}

// Close 关闭日志器
func (l *AuditLogger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil
	}

	l.closed = true

	if closer, ok := l.writer.(io.Closer); ok {
		return closer.Close()
	}

	return nil
}
