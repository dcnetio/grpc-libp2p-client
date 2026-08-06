import { pushable, Pushable } from 'it-pushable'  

/**
 * The client can be consumed from an application that owns a different
 * @libp2p/interface installation. Keep this boundary structural so libp2p's
 * private stream fields do not make otherwise compatible streams unassignable.
 */
export interface WritableMessageStream {
  send(data: Uint8Array): boolean
  onDrain(options?: { signal?: AbortSignal }): Promise<void>
  close(options?: { signal?: AbortSignal }): Promise<void>
  abort(error?: Error): void
}

interface StreamWriterOptions {  
  /** 分块大小（默认1MB） */  
  chunkSize?: number  
  /** 背压缓冲区最大值（默认5MB） */  
  bufferSize?: number  
  /** 失败重试次数（默认3次） */  
  retries?: number  
}  

interface BackpressureEventDetail {  
  currentSize: number  
  averageSize: number  
  threshold: number  
  waitingTime: number  
}  

interface EnhancedPushable<T> extends Pushable<T> {  
    // 使用类型合并增加自定义方法  
    _originalPush: Pushable<T>['push']  
    _originalNext: Pushable<T>['next']
    _queueSize: number  
  }  
  

const MaxChunkSize = 4*1024 -5
export class StreamWriter {  
  private p: EnhancedPushable<Uint8Array>  
  //private p = pushable({ objectMode: false }) 
  
  private bytesWritten = 0  
  private abortController = new AbortController()  
  
  // 背压控制相关属性  
  private backpressureHistory: number[] = []  
  private isBackpressure = false  
  private writeQueue: (() => Promise<void>)[] = []  
  private isProcessingQueue = false  
  private lastBackpressureCheck = 0  // 添加时间戳缓存  
  private bytesDrained = 0 // 统计下游实际消化的字节数
  private lastDrainEventAt = 0
  private watchdogTimer: ReturnType<typeof setTimeout> | undefined
  private stallStartAt = 0
  private lastBytesDrainedSeen = 0
  private lastBpWarnAt = 0
  private isHandlingError = false // 防止重复错误处理
  /** drain 事件驱动等待者，替代 flush() 中的 setInterval 轮询 */
  private drainWaiters: Array<() => void> = []

  private log?: { trace?: (...args: unknown[]) => void }

  constructor(  
    private stream: WritableMessageStream,
    private options: StreamWriterOptions = {}  
  ) {  
    // 验证 stream 参数
    if (!stream) {
      throw new Error('StreamWriter requires a valid stream object')
    }
    
    this.log = { trace: (...args: unknown[]) => console.debug('[StreamWriter]', ...args) }
    
    if (options){
        this.options = {  
            chunkSize: options.chunkSize ?? MaxChunkSize,  
            bufferSize: options.bufferSize ?? 5 * 1024 * 1024,  
            retries: options.retries ?? 3  
          }  
    }else{
        this.options = {  
            chunkSize: MaxChunkSize,  
            bufferSize: 5 * 1024 * 1024,  
            retries: 3
        }
    }
   
    if (this.options.chunkSize && this.options.chunkSize > MaxChunkSize) {  
      this.options.chunkSize = MaxChunkSize 
    }
    const basePushable = pushable<Uint8Array>({ objectMode: false }) as EnhancedPushable<Uint8Array>  
    
     // 保留原始方法引用  
     basePushable._originalPush = basePushable.push.bind(basePushable) 
        basePushable._originalNext = basePushable.next.bind(basePushable) 
   basePushable._queueSize = 0  
  // 不重写 next，避免影响迭代器语义；在 transform 中于“下游消费后”扣减 _queueSize
     // 安全重写 push 方法  
     Object.defineProperty(basePushable, 'push', {  
       value: (chunk: Uint8Array) => {  
         basePushable._queueSize += chunk.byteLength  
         return basePushable._originalPush(chunk)  
       },  
       writable: false,  
       configurable: false  
     })  
     this.p = basePushable  
    this.startPipeline()  
    this.startWatchdog()
  }  

   
  get queueSize() {  
    return this.p._queueSize  
  }  

  get aborted(): boolean {
    return this.abortController.signal.aborted
  }

  // 智能获取平均队列大小
  private getAverageQueueSize(): number {
    const now = Date.now()
    const currentSize = this.queueSize
    
    // 每100ms才更新一次历史记录，避免频繁计算
    if (now - this.lastBackpressureCheck > 100) {
      if (currentSize > 0) {
        this.backpressureHistory.push(currentSize)
        if (this.backpressureHistory.length > 5) { // 减少历史记录大小
          this.backpressureHistory.shift()
        }
      }
      this.lastBackpressureCheck = now
    }
    
    if (this.backpressureHistory.length === 0) return currentSize
    return this.backpressureHistory.reduce((a, b) => a + b, 0) / this.backpressureHistory.length
  }  

  

  private handleError(err: Error) {  
    // 避免重复触发错误处理
    if (this.abortController.signal.aborted || this.isHandlingError) {
      return
    }
    
    this.isHandlingError = true
    
    // 先 abort（清理资源），再触发事件，避免事件处理器中再次操作已关闭的流
    this.abort(err.message)
    
    // 在 abort 之后触发错误事件，此时流已经安全关闭
    try {
      this.dispatchEvent(new CustomEvent('error', { detail: err }))
    } catch (eventErr) {
      // 忽略事件处理器中的错误，避免循环
      this.log?.trace?.('Error in error event handler:', eventErr)
    }
  }  


  private startPipeline() {  
    // libp2p v3: 使用 MessageStream API
    this.pipeToStream().catch((err: Error) => this.handleError(err))
  }

  private async pipeToStream() {
    // 检查 stream 是否有效
    if (!this.stream) {
      this.log?.trace?.('Stream is null/undefined, cannot start pipeline')
      return
    }
    
    // createTransform 返回一个转换函数，需要传入源数据
    for await (const chunk of this.createTransform()(this.p)) {
      // Check if stream is aborted before sending
      if (this.abortController.signal.aborted) {
        break
      }
      
      try {
        // 使用 stream.send() 发送数据，返回 false 表示需要等待 drain
        const canContinue = this.stream.send(chunk)
        if (!canContinue) {
          // 传入 abort signal，当流被 abort 时 onDrain() 会立即 reject，
          // 避免在 abort 路径下永久挂住
          await this.stream.onDrain({ signal: this.abortController.signal })
        }
      } catch (err: unknown) {
        // Gracefully handle stream closing errors - 不要传递到 handleError
        const errMsg = (err instanceof Error ? err.message : '').toLowerCase()
        if ((err instanceof Error && err.name === 'StreamStateError') || 
            errMsg.includes('closing') || 
            errMsg.includes('closed') ||
            errMsg.includes('write to a stream that is closed')) {
          this.log?.trace?.('Stream is closing/closed, stopping pipeline')
          break
        }
        throw err
      }
    }
    // pipeline 正常结束（stream 关闭或 pushable 耗尽）—— 确保资源清理
    // 若已通过 abort() 触发则 cleanup() 内部幂等处理
    this.cleanup()
  }

  private createTransform() {  
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this;  
    return async function* (source: AsyncIterable<Uint8Array>) {  
      for await (const chunk of source) {  
        // 将数据交给下游
        yield chunk  
        // 注意：在 async generator 中，yield 返回后到下一次循环之间，表示下游已经“取走并处理了这个 chunk”，
        // 因此这里统计的 bytesDrained 更接近实际被 sink 消费的字节数
        try {
          // 在下游消费后再扣减待消费队列，避免误判“next 没取”
          if (self.p._queueSize != null) {
            self.p._queueSize = Math.max(0, self.p._queueSize - chunk.byteLength)
          }
          self.bytesDrained += chunk.byteLength
          const now = Date.now()
          if (now - self.lastDrainEventAt > 250) { // 每 ~250ms 通知一次，避免频繁
            self.lastDrainEventAt = now
            self.dispatchEvent(new CustomEvent('drain', { detail: { drained: self.bytesDrained, queueSize: self.queueSize } }))
          }
          // 唤醒所有在等 flush() 或背压解除 的 drainWaiters（队列降低时就可唤醒）
          if (self.drainWaiters.length > 0) {
            const ws = self.drainWaiters.splice(0);
            for (const fn of ws) { try { fn(); } catch { /* ignore */ } }
          }
          // 记录本次已消耗字节，用于看门狗判断是否前进
          self.lastBytesDrainedSeen = self.bytesDrained
        } catch (err) {
          //打印错误
          console.error('Error updating bytesDrained in StreamWriter transform', err)

        }
      }  
    }  
  }  

  // 简单的卡顿看门狗：当队列长期高位且 bytesDrained 无进展时发出 stalled 事件
  // 使用递归 setTimeout 而非 setInterval，避免标签页后台恢复时回调堆积导致 Violation
  private startWatchdog(intervalMs: number = 500, stallMs: number = 1500) {
    if (this.watchdogTimer) return
    const tick = () => {
      if (this.abortController.signal.aborted) return
      const baseThreshold = this.options.bufferSize! * 0.7
      const q = this.queueSize
      const now = Date.now()
      if (q >= baseThreshold) {
        if (this.lastBytesDrainedSeen === this.bytesDrained) {
          if (!this.stallStartAt) this.stallStartAt = now
          if (now - this.stallStartAt >= stallMs) {
            // 异步触发事件，让当前 tick 立即返回，避免同步事件处理器阻塞主线程
            const detail = { queueSize: q, drained: this.bytesDrained, sinceMs: now - this.stallStartAt }
            setTimeout(() => {
              if (!this.abortController.signal.aborted) {
                this.dispatchEvent(new CustomEvent('stalled', { detail }))
              }
            }, 0)
            // 避免持续触发，推进起点
            this.stallStartAt = now
          }
        } else {
          // 有进展，重置
          this.stallStartAt = 0
          this.lastBytesDrainedSeen = this.bytesDrained
        }
      } else {
        // 队列回落，重置
        this.stallStartAt = 0
      }
      // 本次 tick 完成后再安排下一次，不会因主线程繁忙而堆积
      this.watchdogTimer = setTimeout(tick, intervalMs)
    }
    this.watchdogTimer = setTimeout(tick, intervalMs)
  }

  async write(data: ArrayBuffer | Uint8Array | Blob | string): Promise<void> {  
    // 静默处理 aborted 状态，避免在正常的流关闭场景下抛出错误
    if (this.abortController.signal.aborted) {
      return Promise.resolve()
    }

    return new Promise((resolve, reject) => {  
      const task = async () => {  
        try {  
          // 任务执行时再次检查状态，静默跳过
          if (this.abortController.signal.aborted) {
            resolve()
            return
          }
          const buffer = await this.convertToBuffer(data)  
          await this.writeChunks(buffer)  
          resolve()  
        } catch (err) {  
          reject(err)  
        }  
      }  

      this.writeQueue.push(task)  
      this.processQueue()  
    })  
  }  

  private async convertToBuffer(data: ArrayBuffer | Uint8Array | Blob | string): Promise<ArrayBuffer> {  
    if (data instanceof Blob) return data.arrayBuffer()  
    if (typeof data === 'string') return new TextEncoder().encode(data).buffer as ArrayBuffer
    if (data instanceof Uint8Array) return data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength) as ArrayBuffer
    return data  
  }  

  private async writeChunks(buffer: ArrayBuffer) {  
    const src = new Uint8Array(buffer);
    for (let offset = 0; offset < src.byteLength; offset += this.options.chunkSize!) {  
      const end = Math.min(offset + this.options.chunkSize!, src.byteLength)
      // subarray 创建视图，不拷贝内存。pushable.push 不修改内容，安全。
      const chunk = src.subarray(offset, end)

      await this.retryableWrite(chunk)  
      this.updateProgress(chunk.byteLength)  
    }  
  }  

  private async retryableWrite(chunk: Uint8Array, attempt = 0): Promise<void> {  
    // 在尝试写入前立即检查流状态，避免向已关闭的流写入
    if (this.abortController.signal.aborted) {
      throw new Error('Stream is aborted, cannot write')
    }
    
    try {  
      // 只在队列大小超过阈值时才检查背压
      const currentSize = this.queueSize
      const threshold = this.options.bufferSize! * 0.7
      
      if (currentSize > threshold) {
        await this.monitorBackpressure()  
      }
      
      // 再次检查，因为 monitorBackpressure 是异步的
      if (this.abortController.signal.aborted) {
        throw new Error('Stream aborted during backpressure monitoring')
      }
      
      // push 是同步操作，直接调用即可
      this.p.push(chunk)
    } catch (err) {
      // aborted 时不重试，立即抛出
      if (!this.abortController.signal.aborted && attempt < this.options.retries!) {  
        const delay = this.calculateRetryDelay(attempt)  
        await new Promise(r => setTimeout(r, delay))  
        return this.retryableWrite(chunk, attempt + 1)  
      }  
      throw err  
    }  
  }  

  private async monitorBackpressure(): Promise<void> {
    const baseThreshold = this.options.bufferSize! * 0.7
    const criticalThreshold = this.options.bufferSize! * 0.9

    // 快速路径
    if (this.queueSize < baseThreshold) {
      if (this.isBackpressure) {
        this.isBackpressure = false
        this.dispatchBackpressureEvent({ currentSize: this.queueSize, averageSize: this.getAverageQueueSize(), threshold: baseThreshold, waitingTime: 0 })
      }
      return
    }

    if (!this.isBackpressure) {
      this.isBackpressure = true
      this.dispatchBackpressureEvent({ currentSize: this.queueSize, averageSize: this.getAverageQueueSize(), threshold: baseThreshold, waitingTime: 0 })
    }

    // 事件驱动等待：每轮等到 drain 触发或超时，最多 3 轮
    const maxRounds = 3
    for (let i = 0; i < maxRounds; i++) {
      if (this.abortController.signal.aborted) break
      if (this.queueSize < baseThreshold) break

      const isCritical = this.queueSize >= criticalThreshold
      const waitMs = isCritical ? 100 : 30

      await new Promise<void>(resolve => {
        let done = false
        const timer = setTimeout(() => { if (!done) { done = true; resolve() } }, waitMs)
        this.drainWaiters.push(() => { if (!done) { done = true; clearTimeout(timer); resolve() } })
      })
    }

    if (this.queueSize >= baseThreshold) {
      const now = Date.now()
      if (now - this.lastBpWarnAt > 1000) {
        this.lastBpWarnAt = now
        console.warn(`Stream writer: High backpressure detected (${this.queueSize} bytes), continuing anyway`)
      }
    }
  }


  private calculateRetryDelay(attempt: number): number {  
    const baseDelay = 10  
    const maxDelay = 100  
    return Math.min(  
      baseDelay * Math.pow(2, attempt) + Math.random() * 100,  
      maxDelay  
    )  
  }  

  private async processQueue() {  
    if (this.isProcessingQueue || this.abortController.signal.aborted) return  
    this.isProcessingQueue = true  

    while (this.writeQueue.length > 0) {  
      if (this.abortController.signal.aborted) break  
      
      // 只在队列积压时才检查背压，避免每个任务都检查
      const currentSize = this.queueSize
      const threshold = this.options.bufferSize! * 0.5  // 更宽松的阈值
      
      if (currentSize > threshold) {
        await this.monitorBackpressure()  
      }
      
      const task = this.writeQueue.shift()!  
      await task()  
    }  

    this.isProcessingQueue = false  
  }  

  private updateProgress(bytes: number) {  
    this.bytesWritten += bytes  
    this.dispatchEvent(new CustomEvent('progress', {  
      detail: { loaded: this.bytesWritten }  
    }))  
  }  

  async end(): Promise<void> {  
    this.p.end()  
    // libp2p v3: 关闭 stream
    if (this.stream && typeof this.stream.close === 'function') {
      try {
        await this.stream.close()
      } catch (err: unknown) {
        // 忽略关闭已关闭流的错误
        const errMsg = (err instanceof Error ? err.message : '').toLowerCase()
        if (!errMsg.includes('closed') && !errMsg.includes('closing')) {
          this.log?.trace?.('Stream close error:', err)
        }
      }
    }
    this.cleanup()  
  }  

  abort(reason = 'User aborted') {  
    if (this.abortController.signal.aborted) {
      return // Already aborted, prevent multiple abort calls
    }
    
    this.abortController.abort(reason)
    
    try {
      // libp2p v3: 调用 stream.abort() 通知底层 stream
      // 先检查流状态，避免在已关闭的流上调用 abort
      if (this.stream && typeof this.stream.abort === 'function') {
        // 检查流的状态，避免操作已关闭的流
        const streamState = (this.stream as { status?: string; state?: string }).status ||
                             (this.stream as { status?: string; state?: string }).state
        if (streamState !== 'closed' && streamState !== 'closing') {
          this.stream.abort(new Error(reason))
        }
      }
    } catch (err: unknown) {
      // Stream may already be closed, ignore all stream-related errors
      // 完全忽略流操作错误，避免在错误处理中再次抛出错误
      const errMsg = (err instanceof Error ? err.message : '').toLowerCase()
      if (!errMsg.includes('closed') && !errMsg.includes('closing') && !errMsg.includes('write')) {
        this.log?.trace?.('Stream abort error:', err)
      }
    }
    
    this.cleanup()  
    
    // 安全地触发 abort 事件
    try {
      this.dispatchEvent(new CustomEvent('abort', { detail: reason }))
    } catch (eventErr) {
      // 忽略事件处理器错误
      this.log?.trace?.('Error in abort event handler:', eventErr)
    }
  }  

  private cleanup() {  
    // 先设置 abort 标志，阻止新的写入
    if (!this.abortController.signal.aborted) {
      this.abortController.abort()
    }
    
    // 执行所有待处理的写入任务：它们会检查 signal.aborted 并立即 resolve，
    // 不执行的话调用方的 Promise 会永远挂住
    const pendingTasks = this.writeQueue.splice(0)
    for (const task of pendingTasks) {
      task().catch(() => { /* already aborted, ignore */ })
    }

    // 唤醒所有 drainWaiters（flush / monitorBackpressure 中的等待者），
    // 让它们检查 signal.aborted 并立即 resolve，不必等到各自的超时
    const ws = this.drainWaiters.splice(0)
    for (const fn of ws) { try { fn() } catch { /* ignore */ } }
    
    try {
      this.p.end()  
    } catch {
      // Ignore errors when ending pushable
    }
    
    if (this.watchdogTimer) { clearTimeout(this.watchdogTimer); this.watchdogTimer = undefined }
  }  

  // 等待内部队列被下游完全消费（用于在结束前确保尽量发送完数据）
  // 默认超时 10s，避免无限等待
  async flush(timeoutMs: number = 10000): Promise<void> {
    // 快速路径
    if (this.queueSize <= 0 && !this.isProcessingQueue && this.writeQueue.length === 0) return
    if (this.abortController.signal.aborted) return

    await new Promise<void>((resolve) => {
      // 已经清空
      if (this.queueSize <= 0 && !this.isProcessingQueue && this.writeQueue.length === 0) {
        resolve(); return
      }
      let done = false
      const timer = setTimeout(() => {
        if (!done) {
          done = true
          console.warn(`Stream writer: flush timeout with ${this.queueSize} bytes still queued`)
          resolve()
        }
      }, timeoutMs)
      // 由 createTransform 在每个 chunk 被下游消耗后唤醒
      const check = () => {
        if (this.abortController.signal.aborted || (this.queueSize <= 0 && !this.isProcessingQueue && this.writeQueue.length === 0)) {
          if (!done) { done = true; clearTimeout(timer); resolve() }
        } else {
          // 下次 drain 时再检查
          this.drainWaiters.push(check)
        }
      }
      this.drainWaiters.push(check)
    })
  }

  // 事件系统  
  private listeners = new Map<string, ((event: CustomEvent) => void)[]>()  
  
  addEventListener(type: string, callback: (event: CustomEvent) => void) {  
    const handlers = this.listeners.get(type) || []  
    handlers.push(callback)  
    this.listeners.set(type, handlers)  
  }  

// 修复后的代码片段  
private dispatchEvent(event: CustomEvent) {  
    const handlers = this.listeners.get(event.type) || []  
    handlers.forEach(handler => handler(event))  
  }  
  
  // 明确指定事件类型  
  private dispatchBackpressureEvent(detail: BackpressureEventDetail) {  
    this.dispatchEvent(new CustomEvent<BackpressureEventDetail>('backpressure', {   
      detail   
    }))  
  }  
   
}
