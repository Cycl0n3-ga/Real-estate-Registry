#!/bin/bash

# 獲取腳本所在目錄的絕對路徑
PROJECT_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$PROJECT_ROOT"

echo "==== 正在重啟 良富居地產 API 伺服器 ===="

# 1. 尋找並清理佔用 5001 埠號的進程
OLD_PID=$(lsof -t -i:5001)
if [ -n "$OLD_PID" ]; then
    echo "清理舊進程 (PID: $OLD_PID)..."
    kill -9 $OLD_PID
    sleep 1
fi

# 2. 啟動伺服器
echo "啟動 server.py..."
nohup python3 web/server.py > web/server.log 2>&1 &

# 3. 獲取新 PID 並顯示
NEW_PID=$!
echo "伺服器已在背景啟動。"
echo "PID: $NEW_PID"
echo "日誌文件: web/server.log"
echo "======================================"

# 4. 簡單檢查是否成功
sleep 2
if ps -p $NEW_PID > /dev/null; then
    echo "✅ 伺服器運行中。"
else
    echo "❌ 伺ice 可能啟動失敗，請檢查 web/server.log"
fi
