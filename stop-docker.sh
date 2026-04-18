#!/bin/bash

# ============================================
# 股票分析系统 Docker 一键停止脚本
# 双击此脚本或拖拽到终端即可停止服务
# ============================================

echo "🛑 停止股票分析系统 Docker 服务..."
echo "=================================="

# 进入项目目录
cd "$(dirname "$0")" || exit 1

# 检查 docker-compose 文件
if [ ! -f "./docker/docker-compose.yml" ]; then
    echo "❌ 找不到 docker-compose.yml 文件"
    exit 1
fi

echo "📁 项目目录: $(pwd)"

# 显示当前运行的服务
echo "📊 当前运行的服务:"
docker-compose -f ./docker/docker-compose.yml ps

# 停止服务
echo ""
echo "⏳ 正在停止服务..."
docker-compose -f ./docker/docker-compose.yml down

echo ""
echo "✅ 服务已停止！"
echo ""
echo "📝 清理选项:"
echo "   1. 删除数据卷（清除所有数据）:"
echo "      docker-compose -f ./docker/docker-compose.yml down -v"
echo "   2. 删除镜像:"
echo "      docker rmi daily-stock-analysis-analyzer daily-stock-analysis-server"
echo ""
echo "🚀 重新启动:"
echo "   双击 start-docker.sh"