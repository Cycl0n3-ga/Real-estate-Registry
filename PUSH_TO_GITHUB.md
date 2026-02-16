# 📤 推送到 GitHub 的步驟

## 1. 在 GitHub 上創建倉庫

1. 前往 https://github.com/new
2. 倉庫名稱：`real-estate-map` 或你喜歡的名稱
3. 描述：`專業房地產地圖系統 - 建案查詢、價格分析、銷控面板`
4. **不要** 初始化任何文件（README、.gitignore、license 等）
5. 點擊 "Create repository"

## 2. 添加遠端倉庫並推送

執行以下命令（用你的 GitHub 用戶名替換 `YOUR_USERNAME`）：

```bash
cd /home/cyclone/land

# 添加遠端倉庫（HTTPS）
git remote add origin https://github.com/YOUR_USERNAME/real-estate-map.git

# 設定 main 分支並推送
git branch -M main
git push -u origin main
```

**如果使用 SSH**（推薦）：

```bash
# 添加遠端倉庫（SSH）
git remote add origin git@github.com:YOUR_USERNAME/real-estate-map.git

# 推送
git branch -M main
git push -u origin main
```

## 3. 推送 Git LFS 文件

確保 CSV 大文件正確推送：

```bash
# 推送 LFS 文件
git lfs push --all origin main
```

## 4. 驗證推送

檢查 GitHub 倉庫：
- 所有文件都已上傳
- `.env` 文件沒有被上傳（應該被 .gitignore 排除）
- CSV 文件顯示為 LFS 指針

## 🎉 完成！

你的專案現在已經在 GitHub 上了！

### 後續更新

當有新的更改時：

```bash
# 添加更改
git add -A

# 提交
git commit -m "描述你的更改"

# 推送
git push origin main
```

## 📝 注意事項

1. **永遠不要提交 `.env` 文件**（包含 API Key）
2. 確保 CSV 文件使用 Git LFS 追蹤
3. 定期推送更改以備份你的工作

## 🔑 GitHub Personal Access Token

如果使用 HTTPS 推送，需要個人訪問令牌：

1. 前往 https://github.com/settings/tokens
2. 創建新 token（勾選 `repo`）
3. 複製 token
4. 推送時使用 token 作為密碼

## 🚀 GitHub Actions（進階）

可以設定 GitHub Actions 自動測試和部署：

在倉庫中創建 `.github/workflows/test.yml`
