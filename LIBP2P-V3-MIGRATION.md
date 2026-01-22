# libp2p v3 升级成功 ✅

## 升级摘要

已成功将项目升级到 libp2p v3，并移除了所有老旧加密库依赖。

### 版本变更
- **libp2p**: 2.10.0 → 3.1.3
- **@libp2p/interface**: 2.11.0 → 3.1.0  
- **@multiformats/multiaddr**: 12.5.1 → 13.0.1

### 加密库优化
- ❌ **已移除**: tweetnacl, jscrypto, jsbn (老旧库)
- ✅ **现在使用**: @libp2p/crypto@5.1.13 + @noble/curves + @noble/hashes (现代、Tree-shakable)

---

## 主要代码变更

### 1. Stream API 重大变化

libp2p v3 完全改变了 Stream 的 API 设计：

**v2 (旧 API):**
```typescript
// 使用 sink/source 模式
stream.sink   // 写入函数
stream.source // 读取迭代器

// 使用 it-pipe
import { pipe } from 'it-pipe'
pipe(source, transform, stream.sink)
```

**v3 (新 API):**
```typescript
// 使用 MessageStream 接口
stream.send(data)       // 发送数据，返回 boolean
stream.close()          // 关闭 stream
stream.onDrain()        // 等待 drain 事件
for await (data of stream) // stream 本身是 AsyncIterable

// 不再需要 it-pipe
```

### 2. StreamWriter 类适配

**文件**: [src/dc-http2/stream.ts](src/dc-http2/stream.ts)

#### 变更点：

1. **移除 it-pipe 依赖**
```typescript
// 删除
- import { pipe } from 'it-pipe'

// 添加
+ import type { Stream } from '@libp2p/interface'
```

2. **构造函数参数改变**
```typescript
constructor(
-  private sink: any,
+  private stream: Stream,
   private options: StreamWriterOptions = {}
)
```

3. **数据发送方式重写**
```typescript
// 旧方式：使用 pipe
- private startPipeline() {
-   pipe(this.p, this.createTransform(), this.sink)
-     .catch((err) => this.handleError(err))
- }

// 新方式：直接迭代并调用 send()
+ private startPipeline() {
+   this.pipeToStream().catch((err) => this.handleError(err))
+ }
+
+ private async pipeToStream() {
+   for await (const chunk of this.createTransform()) {
+     const canContinue = this.stream.send(chunk)
+     if (!canContinue) {
+       await this.stream.onDrain()  // 背压处理
+     }
+   }
+ }
```

4. **返回类型明确**
```typescript
- private createTransform() {
+ private createTransform(): AsyncIterable<Uint8Array> {
```

5. **end() 方法更新**
```typescript
async end(): Promise<void> {
  this.p.end()
-  await this.sink.return?.()
+  await this.stream.close()  // 使用新 API
  this.cleanup()
}
```

### 3. 调用方更新

**文件**: [src/index.ts](src/index.ts)

所有创建 StreamWriter 的地方：
```typescript
- const writer = new StreamWriter(stream.sink, {
+ const writer = new StreamWriter(stream, {
    bufferSize: 16 * 1024 * 1024,
  })
```

### 4. Parser 类适配

**文件**: [src/dc-http2/parser.ts](src/dc-http2/parser.ts)

Stream 读取数据的方式改变：
```typescript
async processStream(stream: Stream) {
  try {
    // v2: 使用 stream.source
-   for await (const chunk of stream.source) {

    // v3: stream 本身就是 AsyncIterable
+   for await (const chunk of stream) {
      const newData = chunk.subarray()
      // ...
    }
  }
}
```

---

## 迁移关键点

### API 核心变化

1. **Stream 不再有 sink/source**
   - 使用 `stream.send(data)` 发送
   - stream 本身实现 AsyncIterable 接口

2. **背压处理机制**
   - `send()` 返回 `false` → 缓冲区满
   - 调用 `stream.onDrain()` 等待可写

3. **关闭流的方式**
   - 使用 `stream.close()` 
   - 不再使用 `sink.return()`

### 兼容性检查

- [x] StreamWriter 适配新 API
- [x] Parser 读取流数据适配
- [x] 移除 it-pipe 依赖
- [x] 更新类型定义
- [x] 编译构建成功
- [x] 修复所有 stream.source/stream.sink 引用
- [ ] **需要测试**: 运行时功能验证
  - [ ] Unary 调用
  - [ ] Server Streaming  
  - [ ] Client Streaming
  - [ ] Bidirectional Streaming

---

## 测试建议

运行现有的示例代码验证功能：

```bash
# 测试示例
node examples/dynamic-batching.ts
node examples/optimized-streaming.ts

# 或运行你的测试套件
npm test
```

---

## 参考文档

- [libp2p v3 迁移指南](https://github.com/libp2p/js-libp2p/blob/main/doc/migrations/v2-v3.md)
- [@libp2p/interface MessageStream](https://github.com/libp2p/js-libp2p/tree/main/packages/interface/src/message-stream.ts)
- [Stream API 文档](https://github.com/libp2p/js-libp2p/tree/main/packages/interface/src/stream.ts)

---

**迁移完成日期**: 2026-01-22  
**构建状态**: ✅ 成功  
**下一步**: 运行时测试验证
