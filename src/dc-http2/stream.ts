import { pipe } from 'it-pipe'  
import { pushable, Pushable } from 'it-pushable'  

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


  constructor(  
    private sink: any,  
    private options: StreamWriterOptions = {}  
  ) {  
    
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
    // 重写 next 方法  
    Object.defineProperty(basePushable, 'next', {  
        value: async () => {  
        const result = await basePushable._originalNext();  
        if (!result.done && result.value) {  
            basePushable._queueSize -= result.value.byteLength;  
        }  
        return result;  
        },  
        writable: false,  
        configurable: false  
    });  
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
  }  

   
  get queueSize() {  
    return this.p._queueSize  
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

  // 在 next() 操作时更新队列大小  
  private async safeNext() {  
    const result = await this.p.next()  
    if (!result.done) {  
      this.p._queueSize -= result.value?result.value.byteLength : 0
    }  
    return result  
  }  

  private handleError(err: Error) {  
    this.dispatchEvent(new CustomEvent('error', { detail: err }))  
    this.abort(err.message)  
  }  


  private startPipeline() {  
    // 在 sink 之前加入一个轻量 tap，用于统计“已被下游实际消费”的字节数
    pipe(  
      this.p,  
      this.createTransform(),  
      this.sink  
    ).catch((err: Error) => this.handleError(err)) // 正确绑定this  
  }  

  private createTransform() {  
    const self = this;  
    return async function* (source: AsyncIterable<Uint8Array>) {  
      for await (const chunk of source) {  
        // 将数据交给下游
        yield chunk  
        // 注意：在 async generator 中，yield 返回后到下一次循环之间，表示下游已经“取走并处理了这个 chunk”，
        // 因此这里统计的 bytesDrained 更接近实际被 sink 消费的字节数
        try {
          self.bytesDrained += chunk.byteLength
          const now = Date.now()
          if (now - self.lastDrainEventAt > 250) { // 每 ~250ms 通知一次，避免频繁
            self.lastDrainEventAt = now
            self.dispatchEvent(new CustomEvent('drain', { detail: { drained: self.bytesDrained, queueSize: self.queueSize } }))
          }
        } catch {}
      }  
    }  
  }  

  async write(data: ArrayBuffer | Blob | string): Promise<void> {  
    if (this.abortController.signal.aborted) return  

    return new Promise((resolve, reject) => {  
      const task = async () => {  
        try {  
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

  private async convertToBuffer(data: ArrayBuffer | Blob | string): Promise<ArrayBuffer> {  
    if (data instanceof Blob) return data.arrayBuffer()  
    if (typeof data === 'string') return new TextEncoder().encode(data).buffer  
    return data  
  }  

  private async writeChunks(buffer: ArrayBuffer) {  
    for (let offset = 0; offset < buffer.byteLength; offset += this.options.chunkSize!) {  
      const end = Math.min(offset + this.options.chunkSize!, buffer.byteLength)  
      const chunk = new Uint8Array( end - offset)  
      chunk.set(new Uint8Array(buffer.slice(offset, end)))

      await this.retryableWrite(chunk)  
      this.updateProgress(chunk.byteLength)  
    }  
  }  

  private async retryableWrite(chunk: Uint8Array, attempt = 0): Promise<void> {  
    try {  
      // 只在队列大小超过阈值时才检查背压
      const currentSize = this.queueSize
      const threshold = this.options.bufferSize! * 0.7
      
      if (currentSize > threshold) {
        await this.monitorBackpressure()  
      }
      
      await new Promise<void>((resolve, reject) => {  
        try {
            this.p.push(chunk)
        }catch(err){
            reject(err)
        }
        resolve()
      })  
    } catch (err) {  
      if (attempt < this.options.retries!) {  
        const delay = this.calculateRetryDelay(attempt)  
        await new Promise(r => setTimeout(r, delay))  
        return this.retryableWrite(chunk, attempt + 1)  
      }  
      throw err  
    }  
  }  

  private async monitorBackpressure(): Promise<void> {  
    const currentSize = this.queueSize
    const baseThreshold = this.options.bufferSize! * 0.7  // 降低基础阈值，更早检测
    const criticalThreshold = this.options.bufferSize! * 0.9  // 临界阈值
    
    // 快速路径：无背压时直接返回
    if (currentSize < baseThreshold) {
      if (this.isBackpressure) {
        this.isBackpressure = false
        this.dispatchBackpressureEvent({
          currentSize,
          averageSize: this.getAverageQueueSize(),
          threshold: baseThreshold,
          waitingTime: 0
        })
      }
      return
    }
    
    // 进入背压状态
    if (!this.isBackpressure) {
      this.isBackpressure = true
      this.dispatchBackpressureEvent({
        currentSize,
        averageSize: this.getAverageQueueSize(),
        threshold: baseThreshold,
        waitingTime: 0
      })
    }
    
    // 智能等待策略
    const pressure = currentSize / this.options.bufferSize!
    let waitTime: number
    
    if (currentSize >= criticalThreshold) {
      // 临界状态：长时间等待
      waitTime = 50 + Math.min(200, pressure * 100)
    } else {
      // 轻度背压：短时间等待
      waitTime = Math.min(20, pressure * 30)
    }
    
    // 使用指数退避，但最多等待3次
    let retryCount = 0
    const maxRetries = 3
    
    while (this.queueSize >= baseThreshold && retryCount < maxRetries) {
      if (this.abortController.signal.aborted) break
      
      await new Promise(r => setTimeout(r, waitTime))
      retryCount++
      
      // 动态调整等待时间
      waitTime = Math.min(waitTime * 1.5, 100)
    }
    
    // 如果仍然背压但达到最大重试次数，记录警告但继续执行
    if (this.queueSize >= baseThreshold) {
      console.warn(`Stream writer: High backpressure detected (${this.queueSize} bytes), continuing anyway`)
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
    await this.sink.return?.()  
    this.cleanup()  
  }  

  abort(reason = 'User aborted') {  
    this.abortController.abort(reason)  
    this.cleanup()  
    this.dispatchEvent(new CustomEvent('abort', { detail: reason }))  
  }  

  private cleanup() {  
    this.p.end()  
    this.abortController.abort()  
    this.writeQueue = []  
  }  

  // 等待内部队列被下游完全消费（用于在结束前确保尽量发送完数据）
  // 默认超时 10s，避免无限等待
  async flush(timeoutMs: number = 10000): Promise<void> {
    const start = Date.now()
    // 快速路径
    if (this.queueSize <= 0 && !this.isProcessingQueue && this.writeQueue.length === 0) return
    // 轮询等待队列清空
    while (true) {
      if (this.abortController.signal.aborted) return
      if (this.queueSize <= 0 && !this.isProcessingQueue && this.writeQueue.length === 0) return
      if (Date.now() - start > timeoutMs) {
        console.warn(`Stream writer: flush timeout with ${this.queueSize} bytes still queued`)
        return
      }
      await new Promise(r => setTimeout(r, 10))
    }
  }

  // 事件系统  
  private listeners = new Map<string, Function[]>()  
  
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