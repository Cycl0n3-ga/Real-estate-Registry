# 推送到 GitHub 的步驟

## 1. 在 GitHub 上創建倉庫

1. 前往 https://github.com/new
2. 倉庫名稱：`real-estate-map` 或 `房地產地圖系統`
3. **不要** 初始化任何文件（README、.gitignore、license 等）
4. 點擊 "Create repository"

## 2. 添加遠端倉庫並推送

執行以下命令（用你的 GitHub 用戶名替換 `YOUR_USERNAME`）：

```bash
cd /home/cyclone/land

# 添加遠端倉庫
git remote add origin https://github.com/YOUR_USERNAME/real-estate-map.git

# 設定 main 分支並推送
git branch -M main
git push -u origin main
```

如果使用 SSH（推薦）：

```bash
# 添加遠端倉庫（SSH）
git remote add origin git@github.com:YOUR_USERNAME/real-estate-map.git

# 推送
git branch -M main
git push -u origin main
```

## 3. 確保 git lfs 文件正確推送

檢查遠端倉庫中的 CSV 文件是否通過 git lfs 追蹤：

```bash
# 本地檢查
git lfs ls-files

# 推送 LFS 文件（如果之前沒推送）
git lfs push --all origin main
```

## 常見問題

### Q: 推送失敗，提示 "fatal: 'origin' does not appear to be a 'git' repository"

A: 檢查遠端 URL 是否正確設定：
```bash
git remote -v
```

如果沒有 origin，執行上面的 `git remote add origin` 命令。

### Q: 收到 "Permission denied" 錯誤

A: 如果使用 HTTPS，你需要提供 GitHub 個人訪問令牌（Personal Access Token）：

1. 前往 https://github.com/settings/tokens
2. 創建新 token（勾選 `repo` 和 `write:packages`）
3. 複製 token
4. 推送時，用戶名是 `git`，密碼是 token

如果使用 SSH，確保已設定 SSH 密鑰：
```bash
ssh -T git@github.com
```

### Q: git lfs 文件未正確推送

A: 確保已安裝並初始化 git lfs：
```bash
git lfs install
git lfs push --all origin main
```

## 快速檢查清單

- [ ] 已在 GitHub 上創建倉庫
- [ ] 已執行 `git remote add origin ...`
- [ ] 已執行 `git push -u origin main`
- [ ] 倉庫中顯示所有文件（.env 除外）
- [ ] CSV 文件顯示為 LFS 指針（如果超過 100MB）

## 後續維護

推送後，如果有新的更改：

```bash
# 添加更改
git add -A

# 提交
git commit -m "描述你的更改"

# 推送
git push origin main
```
