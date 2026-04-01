package audit

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// MetricsCollector 性能指标收集器
type MetricsCollector struct {
	totalRequests   int64
	successRequests int64
	failedRequests  int64
	totalDuration   int64
	concurrent      int64
	cacheHits       int64
	cacheMisses     int64
	toolCalls       sync.Map
	errors          sync.Map
	startTime       time.Time
}

// ToolMetrics 工具调用指标
type ToolMetrics struct {
	Calls         int64
	Successes     int64
	Failures      int64
	TotalDuration int64
}

// NewMetricsCollector 创建指标收集器
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		startTime: time.Now(),
	}
}

// RecordRequest 记录请求
func (m *MetricsCollector) RecordRequest(success bool, duration time.Duration) {
	atomic.AddInt64(&m.totalRequests, 1)
	atomic.AddInt64(&m.totalDuration, int64(duration))

	if success {
		atomic.AddInt64(&m.successRequests, 1)
	} else {
		atomic.AddInt64(&m.failedRequests, 1)
	}
}

// RecordToolCall 记录工具调用
func (m *MetricsCollector) RecordToolCall(toolName string, success bool, duration time.Duration) {
	val, _ := m.toolCalls.LoadOrStore(toolName, &ToolMetrics{})
	metrics := val.(*ToolMetrics)

	atomic.AddInt64(&metrics.Calls, 1)
	atomic.AddInt64(&metrics.TotalDuration, int64(duration))

	if success {
		atomic.AddInt64(&metrics.Successes, 1)
	} else {
		atomic.AddInt64(&metrics.Failures, 1)
	}
}

// RecordError 记录错误
func (m *MetricsCollector) RecordError(errType string) {
	val, _ := m.errors.LoadOrStore(errType, new(int64))
	atomic.AddInt64(val.(*int64), 1)
}

// RecordCacheHit 记录缓存命中
func (m *MetricsCollector) RecordCacheHit() {
	atomic.AddInt64(&m.cacheHits, 1)
}

// RecordCacheMiss 记录缓存未命中
func (m *MetricsCollector) RecordCacheMiss() {
	atomic.AddInt64(&m.cacheMisses, 1)
}

// IncrementConcurrent 增加并发计数
func (m *MetricsCollector) IncrementConcurrent() {
	atomic.AddInt64(&m.concurrent, 1)
}

// DecrementConcurrent 减少并发计数
func (m *MetricsCollector) DecrementConcurrent() {
	atomic.AddInt64(&m.concurrent, -1)
}

// GetStats 获取统计信息
func (m *MetricsCollector) GetStats() map[string]any {
	uptime := time.Since(m.startTime)
	total := atomic.LoadInt64(&m.totalRequests)
	success := atomic.LoadInt64(&m.successRequests)
	failed := atomic.LoadInt64(&m.failedRequests)
	totalDuration := atomic.LoadInt64(&m.totalDuration)

	avgDuration := int64(0)
	if total > 0 {
		avgDuration = totalDuration / total
	}

	successRate := float64(0)
	if total > 0 {
		successRate = float64(success) / float64(total) * 100
	}

	return map[string]any{
		"uptime_seconds":   int64(uptime.Seconds()),
		"total_requests":   total,
		"success_requests": success,
		"failed_requests":  failed,
		"success_rate":     successRate,
		"avg_duration_ms":  avgDuration / int64(time.Millisecond),
		"concurrent":       atomic.LoadInt64(&m.concurrent),
		"cache_hits":       atomic.LoadInt64(&m.cacheHits),
		"cache_misses":     atomic.LoadInt64(&m.cacheMisses),
	}
}

// PrometheusHandler 返回 Prometheus 格式的指标处理器
func (m *MetricsCollector) PrometheusHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")

		uptime := time.Since(m.startTime).Seconds()
		total := atomic.LoadInt64(&m.totalRequests)
		success := atomic.LoadInt64(&m.successRequests)
		failed := atomic.LoadInt64(&m.failedRequests)
		concurrent := atomic.LoadInt64(&m.concurrent)
		cacheHits := atomic.LoadInt64(&m.cacheHits)
		cacheMisses := atomic.LoadInt64(&m.cacheMisses)

		fmt.Fprintf(w, "# HELP minio_mcp_uptime_seconds Server uptime in seconds\n")
		fmt.Fprintf(w, "# TYPE minio_mcp_uptime_seconds gauge\n")
		fmt.Fprintf(w, "minio_mcp_uptime_seconds %.2f\n\n", uptime)

		fmt.Fprintf(w, "# HELP minio_mcp_requests_total Total number of requests\n")
		fmt.Fprintf(w, "# TYPE minio_mcp_requests_total counter\n")
		fmt.Fprintf(w, "minio_mcp_requests_total %d\n\n", total)

		fmt.Fprintf(w, "# HELP minio_mcp_requests_success_total Total number of successful requests\n")
		fmt.Fprintf(w, "# TYPE minio_mcp_requests_success_total counter\n")
		fmt.Fprintf(w, "minio_mcp_requests_success_total %d\n\n", success)

		fmt.Fprintf(w, "# HELP minio_mcp_requests_failed_total Total number of failed requests\n")
		fmt.Fprintf(w, "# TYPE minio_mcp_requests_failed_total counter\n")
		fmt.Fprintf(w, "minio_mcp_requests_failed_total %d\n\n", failed)

		fmt.Fprintf(w, "# HELP minio_mcp_concurrent_requests Current concurrent requests\n")
		fmt.Fprintf(w, "# TYPE minio_mcp_concurrent_requests gauge\n")
		fmt.Fprintf(w, "minio_mcp_concurrent_requests %d\n\n", concurrent)

		fmt.Fprintf(w, "# HELP minio_mcp_cache_hits_total Total cache hits\n")
		fmt.Fprintf(w, "# TYPE minio_mcp_cache_hits_total counter\n")
		fmt.Fprintf(w, "minio_mcp_cache_hits_total %d\n\n", cacheHits)

		fmt.Fprintf(w, "# HELP minio_mcp_cache_misses_total Total cache misses\n")
		fmt.Fprintf(w, "# TYPE minio_mcp_cache_misses_total counter\n")
		fmt.Fprintf(w, "minio_mcp_cache_misses_total %d\n\n", cacheMisses)

		// 工具调用指标
		fmt.Fprintf(w, "# HELP minio_mcp_tool_calls_total Total tool calls by tool name\n")
		fmt.Fprintf(w, "# TYPE minio_mcp_tool_calls_total counter\n")
		m.toolCalls.Range(func(key, value any) bool {
			toolName := key.(string)
			metrics := value.(*ToolMetrics)
			fmt.Fprintf(w, "minio_mcp_tool_calls_total{tool=\"%s\"} %d\n", toolName, atomic.LoadInt64(&metrics.Calls))
			return true
		})
	}
}
