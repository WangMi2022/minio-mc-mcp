package minio

import (
	"context"
	"encoding/json"
	"fmt"

	"go.uber.org/zap"

	"minio-mc-mcp/internal/logger"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3Client 基于 minio-go SDK + madmin-go 的 MinIO 客户端
// 用于替代 mc CLI 中不可靠或性能差的操作
type S3Client struct {
	client      *minio.Client
	adminClient *madmin.AdminClient
	endpoint    string
	log         *zap.Logger
}

// S3ClientConfig S3 客户端配置
type S3ClientConfig struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Secure    bool
}

// S3Object 对象信息
type S3Object struct {
	Key          string
	Size         int64
	LastModified string
	IsDir        bool
	ContentType  string
	ETag         string
}

// S3BucketUsage Bucket 用量信息
type S3BucketUsage struct {
	Size        int64
	ObjectCount int64
}

// ServerStatus 服务器状态聚合信息
type ServerStatus struct {
	Connected     bool
	ServerVersion string
	Uptime        string
	Nodes         []NodeInfo
	TotalNodes    int
	OnlineNodes   int
	OfflineNodes  int
	TotalDrives   int
	OnlineDrives  int
	OfflineDrives int
	Storage       StorageInfo
	BucketsCount  int
	ObjectsCount  int64
	UsersCount    int
}

// NodeInfo 节点信息
type NodeInfo struct {
	Endpoint      string
	State         string
	Uptime        string
	Version       string
	DrivesOnline  int
	DrivesOffline int
	Drives        []DriveInfo
	Network       map[string]string
}

// DriveInfo 磁盘信息
type DriveInfo struct {
	Endpoint       string
	DrivePath      string
	State          string
	TotalSpace     uint64
	UsedSpace      uint64
	AvailableSpace uint64
}

// StorageInfo 存储信息
type StorageInfo struct {
	Total     uint64
	Used      uint64
	Available uint64
}

// NewS3Client 创建 S3 客户端（同时初始化 minio-go 和 madmin-go）
func NewS3Client(cfg *S3ClientConfig) (*S3Client, error) {
	if cfg == nil {
		return nil, fmt.Errorf("S3 客户端配置不能为空")
	}

	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.Secure,
	})
	if err != nil {
		return nil, fmt.Errorf("创建 S3 客户端失败: %w", err)
	}

	adminClient, err := madmin.New(cfg.Endpoint, cfg.AccessKey, cfg.SecretKey, cfg.Secure)
	if err != nil {
		return nil, fmt.Errorf("创建 Admin 客户端失败: %w", err)
	}

	return &S3Client{
		client:      client,
		adminClient: adminClient,
		endpoint:    cfg.Endpoint,
		log:         logger.Named("s3client"),
	}, nil
}

// ──────────────────────────────────────────────
// Admin API（madmin-go）
// ──────────────────────────────────────────────

// GetServerStatus 通过 madmin-go ServerInfo() 获取服务器完整状态
// 一次调用即可拿到版本、节点、磁盘、存储空间、bucket/对象统计等信息
func (s *S3Client) GetServerStatus(ctx context.Context) (*ServerStatus, error) {
	s.log.Debug("获取服务器状态")
	info, err := s.adminClient.ServerInfo(ctx)
	if err != nil {
		s.log.Error("获取服务器信息失败", zap.Error(err))
		return nil, fmt.Errorf("获取服务器信息失败: %w", err)
	}

	status := &ServerStatus{
		Connected:    true,
		BucketsCount: int(info.Buckets.Count),
		ObjectsCount: int64(info.Objects.Count),
	}

	if len(info.Servers) > 0 {
		status.ServerVersion = info.Servers[0].Version
	}

	var totalDrives, onlineDrives, offlineDrives int
	var totalSpace, usedSpace uint64
	nodes := make([]NodeInfo, 0, len(info.Servers))

	for _, srv := range info.Servers {
		// madmin-go ServerProperties.State 值为 "online" 或 "offline"
		nodeState := srv.State
		if nodeState == "" {
			nodeState = "offline"
		}

		nodeDrivesOnline := 0
		nodeDrivesOffline := 0
		for _, disk := range srv.Disks {
			if disk.State == "ok" {
				nodeDrivesOnline++
			} else {
				nodeDrivesOffline++
			}
			totalSpace += disk.TotalSpace
			usedSpace += disk.UsedSpace
		}

		totalDrives += len(srv.Disks)
		onlineDrives += nodeDrivesOnline
		offlineDrives += nodeDrivesOffline

		uptime := formatUptimeSeconds(srv.Uptime)

		// 构建 drives 详情
		drives := make([]DriveInfo, 0, len(srv.Disks))
		for _, disk := range srv.Disks {
			drives = append(drives, DriveInfo{
				Endpoint:       disk.Endpoint,
				DrivePath:      disk.DrivePath,
				State:          disk.State,
				TotalSpace:     disk.TotalSpace,
				UsedSpace:      disk.UsedSpace,
				AvailableSpace: disk.AvailableSpace,
			})
		}

		nodes = append(nodes, NodeInfo{
			Endpoint:      srv.Endpoint,
			State:         nodeState,
			Uptime:        uptime,
			Version:       srv.Version,
			DrivesOnline:  nodeDrivesOnline,
			DrivesOffline: nodeDrivesOffline,
			Drives:        drives,
			Network:       srv.Network,
		})
	}

	onlineNodes := 0
	for _, n := range nodes {
		if n.State == "online" {
			onlineNodes++
		}
	}

	// 取第一个在线节点的 uptime 作为全局 uptime
	globalUptime := ""
	for _, n := range nodes {
		if n.State == "online" && n.Uptime != "" {
			globalUptime = n.Uptime
			break
		}
	}

	status.Uptime = globalUptime
	status.Nodes = nodes
	status.TotalNodes = len(nodes)
	status.OnlineNodes = onlineNodes
	status.OfflineNodes = len(nodes) - onlineNodes
	status.TotalDrives = totalDrives
	status.OnlineDrives = onlineDrives
	status.OfflineDrives = offlineDrives
	status.Storage = StorageInfo{
		Total:     totalSpace,
		Used:      usedSpace,
		Available: totalSpace - usedSpace,
	}

	return status, nil
}

// ListUsers 通过 madmin-go 获取用户列表
func (s *S3Client) ListUsers(ctx context.Context) (map[string]madmin.UserInfo, error) {
	return s.adminClient.ListUsers(ctx)
}

// ──────────────────────────────────────────────
// S3 API（minio-go）
// ──────────────────────────────────────────────

// ListBuckets 列出所有 Bucket
func (s *S3Client) ListBuckets(ctx context.Context) ([]minio.BucketInfo, error) {
	return s.client.ListBuckets(ctx)
}

// ListObjects 列出 Bucket 中的对象
// prefix 为空时列出根目录，recursive=false 时只列出当前层级（目录+文件）
func (s *S3Client) ListObjects(ctx context.Context, bucket, prefix string, recursive bool) ([]S3Object, error) {
	s.log.Debug("列出对象",
		zap.String("bucket", bucket),
		zap.String("prefix", prefix),
		zap.Bool("recursive", recursive),
	)

	opts := minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: recursive,
	}

	var objects []S3Object
	for obj := range s.client.ListObjects(ctx, bucket, opts) {
		if obj.Err != nil {
			s.log.Error("列出对象失败",
				zap.String("bucket", bucket),
				zap.Error(obj.Err),
			)
			return nil, fmt.Errorf("列出对象失败: %w", obj.Err)
		}

		isDir := false
		key := obj.Key

		if len(key) > 0 && key[len(key)-1] == '/' && obj.Size == 0 {
			isDir = true
		}

		relKey := key
		if prefix != "" && len(key) > len(prefix) {
			relKey = key[len(prefix):]
		}

		if relKey == "" {
			continue
		}

		lastModified := ""
		if !obj.LastModified.IsZero() {
			lastModified = obj.LastModified.Format("2006-01-02T15:04:05Z")
		}

		objects = append(objects, S3Object{
			Key:          relKey,
			Size:         obj.Size,
			LastModified: lastModified,
			IsDir:        isDir,
			ContentType:  obj.ContentType,
			ETag:         obj.ETag,
		})
	}

	return objects, nil
}

// GetBucketUsage 获取 Bucket 的用量统计（遍历所有对象累加）
func (s *S3Client) GetBucketUsage(ctx context.Context, bucket string) (*S3BucketUsage, error) {
	s.log.Debug("获取 Bucket 用量", zap.String("bucket", bucket))

	opts := minio.ListObjectsOptions{
		Recursive: true,
	}

	var totalSize int64
	var objectCount int64

	for obj := range s.client.ListObjects(ctx, bucket, opts) {
		if obj.Err != nil {
			s.log.Error("统计 Bucket 用量失败",
				zap.String("bucket", bucket),
				zap.Error(obj.Err),
			)
			return nil, fmt.Errorf("统计 Bucket 用量失败: %w", obj.Err)
		}
		totalSize += obj.Size
		objectCount++
	}

	return &S3BucketUsage{
		Size:        totalSize,
		ObjectCount: objectCount,
	}, nil
}

// BucketExists 检查 Bucket 是否存在
func (s *S3Client) BucketExists(ctx context.Context, name string) (bool, error) {
	return s.client.BucketExists(ctx, name)
}

// MakeBucket 创建 Bucket
func (s *S3Client) MakeBucket(ctx context.Context, name string) error {
	return s.client.MakeBucket(ctx, name, minio.MakeBucketOptions{})
}

// DeleteObjectsWithPrefix 递归删除指定前缀下的所有对象
func (s *S3Client) DeleteObjectsWithPrefix(ctx context.Context, bucket, prefix string) (int, error) {
	s.log.Info("递归删除目录",
		zap.String("bucket", bucket),
		zap.String("prefix", prefix),
	)

	// 列出所有匹配前缀的对象
	opts := minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}

	// 收集要删除的对象
	objectsCh := make(chan minio.ObjectInfo)
	go func() {
		defer close(objectsCh)
		for obj := range s.client.ListObjects(ctx, bucket, opts) {
			if obj.Err != nil {
				s.log.Error("列出对象失败", zap.Error(obj.Err))
				continue
			}
			objectsCh <- obj
		}
	}()

	// 批量删除
	deletedCount := 0
	for obj := range objectsCh {
		err := s.client.RemoveObject(ctx, bucket, obj.Key, minio.RemoveObjectOptions{})
		if err != nil {
			s.log.Error("删除对象失败",
				zap.String("key", obj.Key),
				zap.Error(err),
			)
			// 继续删除其他对象，不中断
			continue
		}
		deletedCount++
	}

	s.log.Info("目录删除完成",
		zap.String("bucket", bucket),
		zap.String("prefix", prefix),
		zap.Int("deleted_count", deletedCount),
	)

	return deletedCount, nil
}

// GetRawClient 返回底层 minio-go Client，供需要直接调用 S3 API 的场景使用（如 CopyObject）
func (s *S3Client) GetRawClient() *minio.Client {
	return s.client
}

// GetEndpoint 返回当前连接的 MinIO endpoint
func (s *S3Client) GetEndpoint() string {
	return s.endpoint
}

// CheckConnection 检查 S3 连接是否正常
func (s *S3Client) CheckConnection(ctx context.Context) bool {
	_, err := s.client.ListBuckets(ctx)
	return err == nil
}

// ──────────────────────────────────────────────
// IAM Policy API（madmin-go）
// ──────────────────────────────────────────────

// PolicySummary 策略摘要信息（用于列表展示）
type PolicySummary struct {
	Name   string          `json:"name"`
	Policy json.RawMessage `json:"policy"`
}

// ListCannedPolicies 列出所有 IAM 策略
// 返回策略名 → 策略 JSON 的映射
func (s *S3Client) ListCannedPolicies(ctx context.Context) (map[string]json.RawMessage, error) {
	s.log.Debug("列出所有 IAM 策略")
	return s.adminClient.ListCannedPolicies(ctx)
}

// InfoCannedPolicy 获取单个 IAM 策略的详细 JSON 内容
func (s *S3Client) InfoCannedPolicy(ctx context.Context, policyName string) (json.RawMessage, error) {
	s.log.Debug("获取策略详情", zap.String("policy", policyName))
	policyInfo, err := s.adminClient.InfoCannedPolicyV2(ctx, policyName)
	if err != nil {
		return nil, fmt.Errorf("获取策略 '%s' 详情失败: %w", policyName, err)
	}
	return policyInfo.Policy, nil
}

// formatUptimeSeconds 格式化运行时间（秒）
func formatUptimeSeconds(seconds int64) string {
	if seconds <= 0 {
		return "0s"
	}
	hours := seconds / 3600
	minutes := (seconds % 3600) / 60
	secs := seconds % 60

	result := ""
	if hours > 0 {
		result += fmt.Sprintf("%dh", hours)
	}
	if minutes > 0 {
		result += fmt.Sprintf("%dm", minutes)
	}
	if secs > 0 || result == "" {
		result += fmt.Sprintf("%ds", secs)
	}
	return result
}
