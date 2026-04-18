#!/bin/bash

# ============================================
# 股票分析系统 Docker 状态检查脚本
# ============================================

echo "📊 股票分析系统 Docker 服务状态"
echo "=================================="

# 进入项目目录
cd "$(dirname "$0")" || exit 1

# 检查 Docker 是否运行
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker 未运行"
    echo "   请先启动 Docker Desktop"
    exit 1
fi

# 检查 docker-compose 文件
if [ ! -f "./docker/docker-compose.yml" ]; then
    echo "❌ 找不到 docker-compose.yml 文件"
    exit 1
fi

echo "📁 项目目录: $(pwd)"
echo "🐳 Docker 版本: $(docker --version)"
echo ""

# 显示服务状态
echo "🔧 服务状态:"
docker-compose -f ./docker/docker-compose.yml ps
echo ""

# 显示容器资源使用
echo "💾 资源使用:"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.BlockIO}}" 2>/dev/null || echo "   无法获取资源使用信息"
echo ""

# 显示最近日志
echo "📝 最近日志 (最后10行):"
docker-compose -f ./docker/docker-compose.yml logs --tail=10
echo ""

# 显示访问信息
echo "🌐 访问信息:"
echo "   • Web 界面: http://localhost:8000"
echo "   • API 文档: http://localhost:8000/docs"
echo "   • 健康检查: http://localhost:8000/api/health"
echo ""

# 显示磁盘使用
echo "💿 数据目录大小:"
du -sh data/ logs/ reports/ 2>/dev/null | while read -r size path; do
    echo "   • $path: $size"
done
echo ""

echo "🛠️  管理命令:"
echo "   启动服务: ./start-docker.sh"
echo "   停止服务: ./stop-docker.sh"
echo "   查看完整日志: docker-compose -f ./docker/docker-compose.yml logs -f"
echo "   重启服务: docker-compose -f ./docker/docker-compose.yml restart"