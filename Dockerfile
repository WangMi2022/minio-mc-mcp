# MinIO MC MCP Server Dockerfile - Go 版本多阶段构建
# 支持多架构: docker buildx build --platform linux/amd64,linux/arm64 .
ARG TARGETARCH=amd64

# Stage 1: 构建阶段
FROM hub.1panel.dev/library/golang:1.24-alpine AS builder

ARG TARGETARCH

WORKDIR /app

# 配置 Go 代理（国内镜像源）
ENV GOPROXY=https://goproxy.cn,direct

# 配置 Alpine 国内镜像源
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.tuna.tsinghua.edu.cn/g' /etc/apk/repositories

# 安装构建依赖
RUN apk add --no-cache git

# 复制依赖文件，优化缓存层
COPY go.mod go.sum ./
RUN go mod download

# 复制源代码
COPY cmd/ ./cmd/
COPY internal/ ./internal/

# 构建静态链接二进制文件，根据目标架构动态编译
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} \
    go build -ldflags="-s -w -X main.Version=1.0.0 -X main.BuildTime=$(date +%Y-%m-%d)" \
    -o minio-mc-mcp ./cmd/server

# Stage 2: 运行阶段
FROM hub.1panel.dev/library/alpine:3.19

ARG TARGETARCH

# 配置 Alpine 国内镜像源并安装运行时依赖（移除 wget，mc 使用本地预编译二进制）
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.tuna.tsinghua.edu.cn/g' /etc/apk/repositories \
    && apk --no-cache add ca-certificates tzdata \
    && cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
    && echo "Asia/Shanghai" > /etc/timezone \
    && apk del tzdata

# 安装 curl 用于健康检查
RUN apk --no-cache add curl

# 复制本地预编译的 MinIO Client (mc)，根据目标架构选择对应二进制
COPY mc-${TARGETARCH} /usr/local/bin/mc
RUN chmod +x /usr/local/bin/mc

# 创建非 root 用户
RUN addgroup -g 1000 mcpminio \
    && adduser -D -u 1000 -G mcpminio mcpminio

WORKDIR /app

# 从构建阶段复制二进制文件
COPY --from=builder /app/minio-mc-mcp /app/minio-mc-mcp

# 创建必要目录并设置权限
RUN mkdir -p /app/logs /home/mcpminio/.mc \
    && chown -R mcpminio:mcpminio /app /home/mcpminio/.mc

# 切换到非 root 用户
USER mcpminio

# 暴露端口
EXPOSE 8080

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -sf http://localhost:8080/health || exit 1

# 运行时环境变量（仅保留必须的默认值，其余由 docker-compose 管理）
ENV MINIO_MCP_HOST=0.0.0.0 \
    MINIO_MCP_PORT=8080 \
    MINIO_MCP_MC_PATH=/usr/local/bin/mc \
    TZ=Asia/Shanghai

# 启动服务器
ENTRYPOINT ["/app/minio-mc-mcp"]
