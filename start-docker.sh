#!/bin/bash

# ============================================
# 股票分析系统 Docker 一键启动脚本
# 双击此脚本或拖拽到终端即可启动
# ============================================

echo "🚀 启动股票分析系统 Docker 服务..."
echo "=================================="

# 进入项目目录
cd "$(dirname "$0")" || exit 1

# 检查 Docker 是否运行
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker 未运行！请先启动 Docker Desktop"
    echo "   1. 打开 Docker Desktop"
    echo "   2. 等待 Docker 启动完成"
    echo "   3. 再次运行此脚本"
    exit 1
fi

# 检查 docker-compose 文件
if [ ! -f "./docker/docker-compose.yml" ]; then
    echo "❌ 找不到 docker-compose.yml 文件"
    exit 1
fi

echo "📁 项目目录: $(pwd)"
echo "🐳 Docker 版本: $(docker --version)"
echo "📦 服务配置: analyzer + server"

# 停止已存在的服务（如果存在）
echo "🛑 停止现有服务..."
docker-compose -f ./docker/docker-compose.yml down 2>/dev/null

# 构建镜像（如果需要）
echo "🔨 构建 Docker 镜像..."
docker-compose -f ./docker/docker-compose.yml build --pull

# 启动服务
echo "🚀 启动服务..."
docker-compose -f ./docker/docker-compose.yml up -d

# 显示服务状态
echo ""
echo "✅ 服务启动完成！"
echo "=================================="
echo ""
echo "📊 服务状态:"
docker-compose -f ./docker/docker-compose.yml ps
echo ""
echo "🌐 访问地址:"
echo "   • Web 界面: http://localhost:8000"
echo "   • API 文档: http://localhost:8000/docs"
echo "   • 健康检查: http://localhost:8000/api/health"
echo ""
echo "📝 查看日志:"
echo "   分析器日志: docker logs -f stock-analyzer"
echo "   服务器日志: docker logs -f stock-server"
echo ""
echo "🛑 停止服务:"
echo "   双击 stop-docker.sh 或运行:"
echo "   docker-compose -f ./docker/docker-compose.yml down"
echo ""
echo "⏳ 正在启动，请稍等 30-60 秒..."
echo "   首次启动可能需要下载基础镜像，请耐心等待"

# 等待服务启动
sleep 5
echo ""
echo "🔍 检查服务状态..."
docker-compose -f ./docker/docker-compose.yml logs --tail=10