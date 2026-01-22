# 打包体积优化分析报告

## 📊 当前状况

### 打包体积
- **UMD 未压缩**: 229K (grpc.js)
- **UMD 压缩**: 67K (grpc.min.js)
- **ESM**: 依赖外部化，体积较小

### 依赖分析
```bash
node_modules: 80M
dist: 2.2M
```

## 🔍 加密库使用情况

### ✅ 保留的库（现代、高效）
- **@noble/curves** - 现代椭圆曲线加密
- **@noble/hashes** - 现代哈希算法
- **jose** - JWT/JWE (如果使用)

### ⚠️ 需要检查的库
- **tweetnacl** - 未在源码中直接使用 ✓
- **jscrypto** - 未在源码中直接使用 ✓
- **jsbn** - 未在源码中直接使用 ✓

**结论**: 这些老旧库是 `libp2p` 的间接依赖，不是你的代码引入的。

## 🎯 优化建议

### 1. 查看打包分析报告
```bash
# 打开 dist/stats.html 查看详细的依赖关系图
open dist/stats.html
```

### 2. 检查 libp2p 版本
当前使用: `libp2p@^2.8.1`

最新版本的 libp2p 可能已经移除了对老旧加密库的依赖：
```bash
npm outdated libp2p
npm update libp2p
```

### 3. 使用 npm 8.3+ 的 overrides 功能

如果 libp2p 的间接依赖仍使用老旧库，可以在 `package.json` 中强制覆盖：

```json
{
  "overrides": {
    "tweetnacl": "npm:@noble/curves@latest",
    "jscrypto": "npm:@noble/hashes@latest"
  }
}
```

⚠️ 注意：这可能导致兼容性问题，需要充分测试。

### 4. 优化 Rollup 配置

#### 当前配置分析
- ✅ 已配置 `external` 排除依赖
- ✅ 已开启 `browser: true`
- ✅ 已使用 terser 压缩
- ✅ 已添加 visualizer 分析

#### 进一步优化选项

**选项 A: 完全内联依赖（适合纯前端使用）**
```javascript
// rollup.config.js
const external = []; // 清空外部依赖，全部打包

// 添加更激进的 tree-shaking
plugins: [
  resolve({
    browser: true,
    preferBuiltins: false,
    modulesOnly: true, // 只处理 ES 模块
  }),
  // ...
]
```

**选项 B: 使用 modern 构建目标**
```javascript
// 针对现代浏览器，减少 polyfill
typescript({
  target: 'es2020', // 而不是 es5
  lib: ['es2020', 'dom']
})
```

### 5. 分析具体占用

运行构建后，查看 `dist/stats.html`，重点关注：
- 📦 哪些依赖占用最大
- 🔄 是否有重复打包的模块
- 🌲 Tree-shaking 是否生效

## 📋 执行步骤

1. ✅ **安装分析工具** (已完成)
   ```bash
   npm install --save-dev rollup-plugin-visualizer
   ```

2. ✅ **添加分析配置** (已完成)
   
3. ✅ **构建并生成报告** (已完成)
   ```bash
   npm run build
   ```

4. **查看分析报告**
   ```bash
   open dist/stats.html
   ```

5. **根据报告决定优化策略**
   - 如果老旧库确实占用大量空间 → 考虑升级 libp2p 或使用 overrides
   - 如果主要是 libp2p 本身 → 考虑按需导入
   - 如果是重复依赖 → 检查 package-lock.json 并执行 `npm dedupe`

## 🚀 快速测试

### 方案 1: 升级 libp2p (推荐)
```bash
npm update libp2p @libp2p/interface @multiformats/multiaddr
npm run build
# 对比打包体积
```

### 方案 2: 依赖去重
```bash
npm dedupe
npm run build
```

### 方案 3: 使用 pnpm (更激进的去重)
```bash
npm install -g pnpm
pnpm import  # 从 package-lock.json 导入
pnpm install
pnpm run build
```

## 📈 预期效果

- **最佳情况**: 体积减少 30-50% (如果成功移除老旧库)
- **中等情况**: 体积减少 10-20% (通过去重和优化)
- **最差情况**: 体积不变 (老旧库被 libp2p 强依赖)

## 🔗 相关资源

- [rollup-plugin-visualizer](https://github.com/btd/rollup-plugin-visualizer)
- [@noble/curves](https://github.com/paulmillr/noble-curves)
- [npm overrides](https://docs.npmjs.com/cli/v8/configuring-npm/package-json#overrides)
- [libp2p releases](https://github.com/libp2p/js-libp2p/releases)

## 📝 下一步

1. 打开 `dist/stats.html` 查看详细分析
2. 截图或记录最大的依赖包
3. 决定采用哪种优化方案
4. 执行优化并对比前后体积
