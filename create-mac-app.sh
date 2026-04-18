#!/bin/bash

# ============================================
# 创建 macOS 应用程序快捷方式
# ============================================

APP_NAME="股票分析系统"
APP_DIR="$HOME/Applications/$APP_NAME.app"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "📱 创建 macOS 应用程序: $APP_NAME"
echo "=================================="

# 创建应用程序目录结构
mkdir -p "$APP_DIR/Contents/MacOS"
mkdir -p "$APP_DIR/Contents/Resources"

# 创建 Info.plist
cat > "$APP_DIR/Contents/Info.plist" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleExecutable</key>
    <string>launcher</string>
    <key>CFBundleIdentifier</key>
    <string>com.stock.analysis</string>
    <key>CFBundleName</key>
    <string>$APP_NAME</string>
    <key>CFBundleVersion</key>
    <string>1.0</string>
    <key>CFBundleShortVersionString</key>
    <string>1.0</string>
    <key>CFBundleIconFile</key>
    <string>icon.icns</string>
    <key>NSHighResolutionCapable</key>
    <true/>
</dict>
</plist>
EOF

# 创建启动脚本
cat > "$APP_DIR/Contents/MacOS/launcher" << 'EOF'
#!/bin/bash

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "$0")/../../../.." && pwd)/Documents/daily_stock_analysis"

# 切换到项目目录
cd "$SCRIPT_DIR" || {
    osascript -e 'display dialog "无法找到股票分析系统目录！" buttons {"OK"} default button 1 with icon stop'
    exit 1
}

# 检查 Docker
if ! docker info > /dev/null 2>&1; then
    osascript << 'APPLESCRIPT'
display dialog "Docker 未运行！" & return & return & \
"请先启动 Docker Desktop，然后重试。" & return & return & \
"点击 OK 打开 Docker Desktop。" \
buttons {"取消", "打开 Docker"} default button 2 with icon caution
if button returned of result is "打开 Docker" then
    tell application "Docker" to activate
end if
APPLESCRIPT
    exit 1
fi

# 显示选项菜单
CHOICE=$(osascript << 'APPLESCRIPT'
set options to {"启动服务", "停止服务", "查看状态", "打开 Web 界面", "退出"}
choose from list options with title "股票分析系统" with prompt "请选择操作:" default items {"启动服务"}
if result is false then
    return "退出"
else
    return item 1 of result
end if
APPLESCRIPT
)

case "$CHOICE" in
    "启动服务")
        osascript -e 'display notification "正在启动股票分析系统..." with title "股票分析系统"'
        ./start-docker.sh
        ;;
    "停止服务")
        osascript -e 'display notification "正在停止服务..." with title "股票分析系统"'
        ./stop-docker.sh
        ;;
    "查看状态")
        osascript -e 'display notification "正在检查状态..." with title "股票分析系统"'
        ./status-docker.sh
        ;;
    "打开 Web 界面")
        open "http://localhost:8000"
        osascript -e 'display notification "已打开浏览器" with title "股票分析系统"'
        ;;
    "退出")
        exit 0
        ;;
esac

# 保持窗口打开
if [ "$CHOICE" != "退出" ]; then
    echo ""
    echo "按 Enter 键继续..."
    read -r
fi
EOF

chmod +x "$APP_DIR/Contents/MacOS/launcher"

# 创建默认图标（如果没有自定义图标）
if [ ! -f "$SCRIPT_DIR/icon.icns" ]; then
    echo "📷 创建默认图标..."
    # 这里可以添加创建图标的代码，或者使用系统默认图标
    echo "   使用系统默认图标"
fi

echo ""
echo "✅ 应用程序创建完成！"
echo "📁 位置: $APP_DIR"
echo ""
echo "🎯 使用方法:"
echo "   1. 将 '$APP_NAME.app' 拖到 Dock 中"
echo "   2. 点击图标即可管理服务"
echo ""
echo "🚀 或者直接使用脚本:"
echo "   • 启动: 双击 start-docker.sh"
echo "   • 停止: 双击 stop-docker.sh"
echo "   • 状态: 双击 status-docker.sh"