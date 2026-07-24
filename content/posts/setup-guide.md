---
title: "用 Hugo + GitHub Pages 搭建博客"
date: 2026-07-24
tags: ["Hugo", "GitHub Pages", "教程"]
author: "Woody"
---

## 从零搭建技术博客

本文记录完整的搭建过程。

### 步骤

1. **创建 Hugo 站点**
   ```bash
   hugo new site tech-blog
   cd tech-blog
   ```

2. **选择主题**
   ```bash
   git submodule add https://github.com/adityatelange/hugo-PaperMod.git themes/PaperMod
   ```

3. **配置 GitHub Pages**
   - 在仓库 Settings > Pages 中设置
   - 选择 GitHub Actions 部署

4. **写文章** — 放在 `content/posts/` 下，使用 Markdown + frontmatter

5. **提交 PR → 合并 → 自动部署**

搞定！🎉
