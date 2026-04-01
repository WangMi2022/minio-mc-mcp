package config

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// ServerConfig 定义服务器的完整配置结构
type ServerConfig struct {
	// HTTP 服务器配置
	Host string `mapstructure:"host"`
	Port int    `mapstructure:"port"`

	// MinIO 连接配置
	MinioEndpoint  string `mapstructure:"minioEndpoint"`
	MinioAccessKey string `mapstructure:"minioAccessKey"`
	MinioSecretKey string `mapstructure:"minioSecretKey"`
	MinioAlias     string `mapstructure:"minioAlias"`
	MinioSecure    bool   `mapstructure:"minioSecure"`

	// MC CLI 配置
	MCPath         string        `mapstructure:"mcPath"`
	CommandTimeout time.Duration `mapstructure:"commandTimeout"`

	// 日志配置
	LogLevel  string `mapstructure:"logLevel"`
	LogFormat string `mapstructure:"logFormat"`
	LogFile   string `mapstructure:"logFile"`

	// 性能配置
	MaxConcurrentRequests int           `mapstructure:"maxConcurrentRequests"`
	RequestTimeout        time.Duration `mapstructure:"requestTimeout"`

	// 安全配置
	APIToken       string   `mapstructure:"apiToken"`
	AllowedOrigins []string `mapstructure:"allowedOrigins"`

	// 缓存配置
	EnableCache bool          `mapstructure:"enableCache"`
	CacheTTL    time.Duration `mapstructure:"cacheTTL"`
}

// LoadConfig 加载配置，优先级：命令行参数 > 环境变量 > 配置文件 > 默认值
func LoadConfig() (*ServerConfig, error) {
	setDefaults()
	bindFlags()

	// 检查是否通过环境变量指定了配置文件
	configFile := os.Getenv("MINIO_MCP_CONFIG")
	if configFile != "" {
		viper.SetConfigFile(configFile)
		if err := viper.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("读取配置文件失败: %w", err)
		}
	} else {
		viper.SetConfigName("config")
		viper.SetConfigType("yaml")
		viper.AddConfigPath(".")
		viper.AddConfigPath("./config")
		viper.AddConfigPath("/etc/minio-mc-mcp")

		if err := viper.ReadInConfig(); err != nil {
			if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
				return nil, fmt.Errorf("读取配置文件失败: %w", err)
			}
		}
	}

	// 绑定环境变量
	viper.SetEnvPrefix("MINIO_MCP")
	viper.AutomaticEnv()

	// 显式绑定环境变量到配置键
	bindEnvVars()

	var config ServerConfig
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("解析配置失败: %w", err)
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("配置验证失败: %w", err)
	}

	return &config, nil
}

// setDefaults 设置默认配置值
func setDefaults() {
	// HTTP 服务器默认值
	viper.SetDefault("host", "0.0.0.0")
	viper.SetDefault("port", 8080)

	// MinIO 默认值
	viper.SetDefault("minioEndpoint", "")
	viper.SetDefault("minioAccessKey", "")
	viper.SetDefault("minioSecretKey", "")
	viper.SetDefault("minioAlias", "myminio")
	viper.SetDefault("minioSecure", true)

	// MC CLI 默认值
	viper.SetDefault("mcPath", "mc")
	viper.SetDefault("commandTimeout", 30*time.Second)

	// 日志默认值
	viper.SetDefault("logLevel", "info")
	viper.SetDefault("logFormat", "json")
	viper.SetDefault("logFile", "")

	// 性能默认值
	viper.SetDefault("maxConcurrentRequests", 100)
	viper.SetDefault("requestTimeout", 30*time.Second)

	// 安全默认值
	viper.SetDefault("apiToken", "")
	viper.SetDefault("allowedOrigins", []string{"*"})

	// 缓存默认值
	viper.SetDefault("enableCache", true)
	viper.SetDefault("cacheTTL", 5*time.Minute)
}

var flagsInitialized bool

// bindFlags 绑定命令行参数
func bindFlags() {
	if flagsInitialized {
		return
	}
	flagsInitialized = true

	pflag.String("host", "", "服务器监听地址")
	pflag.Int("port", 0, "服务器监听端口")
	pflag.String("minio-endpoint", "", "MinIO 服务器地址")
	pflag.String("minio-access-key", "", "MinIO Access Key")
	pflag.String("minio-secret-key", "", "MinIO Secret Key")
	pflag.String("minio-alias", "", "MinIO alias 名称")
	pflag.Bool("minio-secure", true, "是否使用 HTTPS 连接 MinIO")
	pflag.String("mc-path", "", "mc CLI 可执行文件路径")
	pflag.Duration("command-timeout", 0, "命令执行超时时间")
	pflag.String("log-level", "", "日志级别 (debug, info, warn, error)")
	pflag.String("log-format", "", "日志格式 (json, text)")
	pflag.String("log-file", "", "日志文件路径")
	pflag.Int("max-concurrent-requests", 0, "最大并发请求数")
	pflag.Duration("request-timeout", 0, "请求超时时间")
	pflag.String("api-token", "", "API 认证 Token")
	pflag.Bool("enable-cache", true, "是否启用缓存")
	pflag.Duration("cache-ttl", 0, "缓存过期时间")

	pflag.Parse()

	viper.BindPFlag("host", pflag.Lookup("host"))
	viper.BindPFlag("port", pflag.Lookup("port"))
	viper.BindPFlag("minioEndpoint", pflag.Lookup("minio-endpoint"))
	viper.BindPFlag("minioAccessKey", pflag.Lookup("minio-access-key"))
	viper.BindPFlag("minioSecretKey", pflag.Lookup("minio-secret-key"))
	viper.BindPFlag("minioAlias", pflag.Lookup("minio-alias"))
	viper.BindPFlag("minioSecure", pflag.Lookup("minio-secure"))
	viper.BindPFlag("mcPath", pflag.Lookup("mc-path"))
	viper.BindPFlag("commandTimeout", pflag.Lookup("command-timeout"))
	viper.BindPFlag("logLevel", pflag.Lookup("log-level"))
	viper.BindPFlag("logFormat", pflag.Lookup("log-format"))
	viper.BindPFlag("logFile", pflag.Lookup("log-file"))
	viper.BindPFlag("maxConcurrentRequests", pflag.Lookup("max-concurrent-requests"))
	viper.BindPFlag("requestTimeout", pflag.Lookup("request-timeout"))
	viper.BindPFlag("apiToken", pflag.Lookup("api-token"))
	viper.BindPFlag("enableCache", pflag.Lookup("enable-cache"))
	viper.BindPFlag("cacheTTL", pflag.Lookup("cache-ttl"))
}

// bindEnvVars 绑定环境变量
func bindEnvVars() {
	viper.BindEnv("host", "MINIO_MCP_HOST")
	viper.BindEnv("port", "MINIO_MCP_PORT")
	viper.BindEnv("minioEndpoint", "MINIO_MCP_ENDPOINT")
	viper.BindEnv("minioAccessKey", "MINIO_MCP_ACCESS_KEY")
	viper.BindEnv("minioSecretKey", "MINIO_MCP_SECRET_KEY")
	viper.BindEnv("minioAlias", "MINIO_MCP_ALIAS")
	viper.BindEnv("minioSecure", "MINIO_MCP_SECURE")
	viper.BindEnv("mcPath", "MINIO_MCP_MC_PATH")
	viper.BindEnv("commandTimeout", "MINIO_MCP_COMMAND_TIMEOUT")
	viper.BindEnv("logLevel", "MINIO_MCP_LOG_LEVEL")
	viper.BindEnv("logFormat", "MINIO_MCP_LOG_FORMAT")
	viper.BindEnv("logFile", "MINIO_MCP_LOG_FILE")
	viper.BindEnv("maxConcurrentRequests", "MINIO_MCP_MAX_CONCURRENT_REQUESTS")
	viper.BindEnv("requestTimeout", "MINIO_MCP_REQUEST_TIMEOUT")
	viper.BindEnv("apiToken", "MINIO_MCP_API_TOKEN")
	viper.BindEnv("enableCache", "MINIO_MCP_ENABLE_CACHE")
	viper.BindEnv("cacheTTL", "MINIO_MCP_CACHE_TTL")
}

// Validate 验证配置的有效性
func (c *ServerConfig) Validate() error {
	if c.Port < 1 || c.Port > 65535 {
		return fmt.Errorf("端口号必须在 1-65535 范围内，当前值: %d", c.Port)
	}

	if c.MinioEndpoint == "" {
		return fmt.Errorf("MinIO endpoint 不能为空")
	}

	if c.MinioAccessKey == "" {
		return fmt.Errorf("MinIO access key 不能为空")
	}

	if c.MinioSecretKey == "" {
		return fmt.Errorf("MinIO secret key 不能为空")
	}

	validLogLevels := map[string]bool{
		"debug": true, "info": true, "warn": true, "error": true,
	}
	if !validLogLevels[c.LogLevel] {
		return fmt.Errorf("无效的日志级别: %s，有效值: debug, info, warn, error", c.LogLevel)
	}

	validLogFormats := map[string]bool{
		"json": true, "text": true,
	}
	if !validLogFormats[c.LogFormat] {
		return fmt.Errorf("无效的日志格式: %s，有效值: json, text", c.LogFormat)
	}

	if c.MaxConcurrentRequests < 1 {
		return fmt.Errorf("最大并发请求数必须大于 0，当前值: %d", c.MaxConcurrentRequests)
	}

	if c.RequestTimeout < time.Second {
		return fmt.Errorf("请求超时时间必须至少为 1 秒，当前值: %v", c.RequestTimeout)
	}

	if c.EnableCache && c.CacheTTL < time.Second {
		return fmt.Errorf("缓存过期时间必须至少为 1 秒，当前值: %v", c.CacheTTL)
	}

	return nil
}

// GetListenAddress 获取服务器监听地址
func (c *ServerConfig) GetListenAddress() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

// IsDebugMode 判断是否为调试模式
func (c *ServerConfig) IsDebugMode() bool {
	return c.LogLevel == "debug"
}
