# 🚀 加密库优化 - 升级方案

## 问题诊断 ✅

### 好消息！
1. ✅ **你的代码没有直接使用老旧加密库** (tweetnacl, jscrypto, jsbn)
2. ✅ **libp2p v3 已经移除了对老旧库的依赖**
3. ✅ **可以通过升级解决问题**

### 当前状态
- libp2p: `2.10.0` → 最新: `3.1.3` 🎯
- @libp2p/interface: `2.11.0` → 最新: `3.1.0` 🎯
- @multiformats/multiaddr: `12.5.1` → 最新: `13.0.1` 🎯

## 📋 执行方案

### 方案 1: 渐进式升级（推荐）⭐

分步升级，避免 breaking changes：

```bash
# 1. 备份当前状态
git add .
git commit -m "chore: backup before libp2p upgrade"

# 2. 升级 patch 版本（安全）
npm update

# 3. 查看变化
npm run build
ls -lh dist/grpc.min.js

# 4. 测试功能
npm test  # 如果有测试的话

# 5. 如果一切正常，升级到 v3（Breaking Change）
npm install libp2p@^3.0.0 @libp2p/interface@^3.0.0 @multiformats/multiaddr@^13.0.0

# 6. 检查代码兼容性
npm run build
```

### 方案 2: 直接升级到 v3（激进）⚡

```bash
# 一次性升级到最新
npm install libp2p@latest @libp2p/interface@latest @multiformats/multiaddr@latest

# 检查 breaking changes
npm run build

# 如果报错，查看 libp2p v3 迁移指南
```

## ⚠️ 可能的 Breaking Changes

libp2p v2 → v3 的主要变化：

### 1. API 变化
```typescript
// v2 (旧)
import { createLibp2p } from 'libp2p'

// v3 (新) - 可能需要调整配置
import { createLibp2p } from 'libp2p'
```

### 2. Interface 变化
```typescript
// 检查这些类型定义
import type { Connection, Stream } from "@libp2p/interface"
```

### 3. 配置选项变化
检查你的 README.md 中的示例是否需要更新。

## 🔍 验证步骤

### 1. 构建验证
```bash
npm run build
```

### 2. 体积对比
```bash
# 升级前
ls -lh dist/grpc.min.js
# 应该是 67K

# 升级后
npm run build
ls -lh dist/grpc.min.js
# 期望: 40-50K (减少 20-30%)
```

### 3. 依赖检查
```bash
# 检查是否还有老旧库
npm ls tweetnacl jscrypto jsbn
# 应该显示 (empty)

# 查看新的加密库
npm ls @libp2p/crypto @noble/curves @noble/hashes
```

### 4. 功能测试
```bash
# 如果有示例代码
cd examples
node dynamic-batching.ts
```

## 📊 预期效果

### 打包体积
- **当前**: 67K (grpc.min.js)
- **预期**: 40-50K (减少 20-30%) ✨
- **最佳**: 35-40K (如果配合其他优化)

### 依赖清理
- ❌ 移除: tweetnacl, jscrypto, jsbn
- ✅ 使用: @libp2p/crypto (基于 @noble 系列)
- ✅ 现代化: 纯 JS, Tree-shakable

### node_modules 体积
- **当前**: 80M
- **预期**: 60-70M (减少 10-20M)

## 🛠️ 代码适配检查清单

升级后需要检查的文件：

- [ ] [src/index.ts](src/index.ts) - 主入口，检查 libp2p 导入
- [ ] [README.md](README.md) - 更新示例代码
- [ ] [examples/dynamic-batching.ts](examples/dynamic-batching.ts) - 测试示例
- [ ] [examples/optimized-streaming.ts](examples/optimized-streaming.ts) - 测试示例

### 快速检查命令
```bash
# 搜索可能受影响的代码
grep -r "from 'libp2p'" src/
grep -r "from '@libp2p/interface'" src/
grep -r "from '@multiformats/multiaddr'" src/
```

## 🎯 推荐执行顺序

### Step 1: 准备工作
```bash
# 确保工作区干净
git status

# 创建升级分支
git checkout -b upgrade/libp2p-v3

# 安装分析工具（已完成）
# npm install --save-dev rollup-plugin-visualizer
```

### Step 2: 升级依赖
```bash
# 选择方案 1 或方案 2
npm install libp2p@^3.0.0 @libp2p/interface@^3.0.0 @multiformats/multiaddr@^13.0.0
```

### Step 3: 验证构建
```bash
npm run build

# 查看分析报告
open dist/stats.html
```

### Step 4: 对比体积
```bash
# 查看文件大小
ls -lh dist/*.js

# 对比压缩后体积
du -sh dist/grpc.min.js
```

### Step 5: 测试功能
```bash
# 如果有测试
npm test

# 手动测试示例
# node examples/xxx.ts
```

### Step 6: 提交变更
```bash
git add package.json package-lock.json
git commit -m "chore: upgrade to libp2p v3 to reduce bundle size"

# 对比前后差异
git diff HEAD~1 package.json
```

## 📚 参考资源

- [libp2p v3 发布说明](https://github.com/libp2p/js-libp2p/releases)
- [迁移指南](https://github.com/libp2p/js-libp2p/blob/main/doc/migrations/v2-v3.md)
- [@libp2p/crypto](https://github.com/libp2p/js-libp2p-crypto)
- [@noble/curves](https://github.com/paulmillr/noble-curves)

## 💡 额外优化建议

升级完成后，可以进一步优化：

### 1. 优化 Rollup 配置
```javascript
// rollup.config.js
resolve({
  browser: true,
  preferBuiltins: false,
  modulesOnly: true, // 只处理 ES 模块，提升 tree-shaking
})
```

### 2. 使用现代目标
```javascript
typescript({
  target: 'es2020', // 而不是 es5
  module: 'esnext'
})
```

### 3. 启用 Brotli 压缩
```javascript
terser({
  compress: {
    ecma: 2020,
    module: true,
  }
})
```

## ❓ 遇到问题？

### 如果构建失败
1. 查看错误信息
2. 检查 TypeScript 类型定义
3. 参考 libp2p v3 迁移指南

### 如果体积没有减少
1. 查看 `dist/stats.html` 分析报告
2. 检查 `package-lock.json` 是否有重复依赖
3. 运行 `npm dedupe`

### 如果功能异常
1. 检查 API 是否有变化
2. 查看 libp2p v3 changelog
3. 回退到 v2: `git checkout package.json package-lock.json && npm install`

---

**现在就开始升级吧！** 🚀

```bash
npm install libp2p@^3.0.0 @libp2p/interface@^3.0.0 @multiformats/multiaddr@^13.0.0
npm run build
```
