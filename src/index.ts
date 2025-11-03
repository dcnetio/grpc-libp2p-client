
import type { Libp2p } from 'libp2p'
import { HTTP2Parser } from './dc-http2/parser';
import {StreamWriter} from './dc-http2/stream'
import { Http2Frame } from './dc-http2/frame';
import type { Connection, Stream } from '@libp2p/interface';
import { HPACK} from './dc-http2/hpack'

import type { Multiaddr } from '@multiformats/multiaddr';

const dialTimeout = 20000 // 20秒
const DEFAULT_SEND_WINDOW_TIMEOUT = 15000

type CallMode = 'unary' | 'server-streaming' | 'client-streaming' | 'bidirectional'
type TransportProfile = 'flow-control' | 'compatibility'

interface CallOptions {
  batchSize?: number
  maxBatchWaitMs?: number
  freshConnection?: boolean
  transportProfile?: TransportProfile
}

interface ConnectionState {
  activeStreams: number
  maxConcurrentStreams: number
  waiters: Array<{ resolve: () => void; reject: (error: Error) => void }>
}


class StreamManager {  
    currentStreamId: number;
    private streamIdLock: Promise<void> = Promise.resolve();

    constructor() {  
      this.currentStreamId = 1; // 从 1 开始，以模拟奇数 ID  
    }  

    async getNextAppLevelStreamId(): Promise<number> {
      // 使用 Promise 链来确保原子性操作
      return new Promise<number>((resolve) => {
        this.streamIdLock = this.streamIdLock.then(() => {
          const id = this.currentStreamId;  
          this.currentStreamId += 2; // 确保奇数步进  
          resolve(id);
        });
      });
    }
  } 


export class Libp2pGrpcClient {
    node: Libp2p;
    protocol: string;
    peerAddr: Multiaddr;
    token: string;
  private connectionStreamManagers: WeakMap<object, StreamManager>;
  private connectionStates: WeakMap<object, ConnectionState>;
    private connectionPool: Map<string, { promise: Promise<Connection>, connection?: Connection }>;


    constructor(node:Libp2p,peerAddr:Multiaddr,token:string,protocol?:string) {
        this.node = node
        this.peerAddr = peerAddr
        if (protocol) {
            this.protocol = protocol
        }else{
             this.protocol = '/dc/thread/0.0.1'
        }
    this.token = token
  this.connectionStreamManagers = new WeakMap()
  this.connectionStates = new WeakMap()
  this.connectionPool = new Map()
    }  

  private getStreamManagerFor(connection: object): StreamManager {
    let manager = this.connectionStreamManagers.get(connection)
    if (!manager) {
      manager = new StreamManager()
      this.connectionStreamManagers.set(connection, manager)
    }
    return manager
  }

  private getConnectionState(connection: object): ConnectionState {
    let state = this.connectionStates.get(connection)
    if (!state) {
      state = {
        activeStreams: 0,
        maxConcurrentStreams: Number.POSITIVE_INFINITY,
        waiters: []
      }
      this.connectionStates.set(connection, state)
    }
    return state
  }

  private notifyStreamSlotAvailable(state: ConnectionState) {
    if (state.waiters.length === 0) {
      return
    }
    while (state.waiters.length > 0 && state.activeStreams < state.maxConcurrentStreams) {
      const waiter = state.waiters.shift()
      if (!waiter) break
      try {
        waiter.resolve()
      } catch (err) {
        console.error('Error resolving stream waiter:', err)
      }
    }
  }

  private rejectStreamWaiters(state: ConnectionState, error: Error) {
    if (state.waiters.length === 0) {
      return
    }
    const waiters = state.waiters.splice(0)
    for (const waiter of waiters) {
      try {
        waiter.reject(error)
      } catch (err) {
        console.error('Error rejecting stream waiter:', err)
      }
    }
  }

  private async waitForStreamSlot(state: ConnectionState, signal?: AbortSignal, timeoutMs: number = dialTimeout): Promise<void> {
    if (state.maxConcurrentStreams <= 0) {
      throw new Error('No available HTTP/2 streams: server advertised zero concurrent streams')
    }
    if (!Number.isFinite(state.maxConcurrentStreams) || state.activeStreams < state.maxConcurrentStreams) {
      return
    }

    await new Promise<void>((resolve, reject) => {
      let settled = false
      let timeoutId: ReturnType<typeof setTimeout> | undefined
      const cleanup = () => {
        const idx = state.waiters.indexOf(waiter)
        if (idx >= 0) {
          state.waiters.splice(idx, 1)
        }
        if (timeoutId) {
          clearTimeout(timeoutId)
        }
        if (signal) {
          signal.removeEventListener('abort', onAbort)
        }
      }
      const waiter = {
        resolve: () => {
          if (settled) return
          settled = true
          cleanup()
          resolve()
        },
        reject: (err: Error) => {
          if (settled) return
          settled = true
          cleanup()
          reject(err)
        }
      }
      const onAbort = () => {
        waiter.reject(new Error('Aborted while waiting for available HTTP/2 stream slot'))
      }
      if (signal) {
        if (signal.aborted) {
          onAbort()
          return
        }
        signal.addEventListener('abort', onAbort, { once: true })
      }
      timeoutId = timeoutMs > 0 ? setTimeout(() => {
        waiter.reject(new Error('Timed out waiting for available HTTP/2 stream slot'))
      }, timeoutMs) : undefined
      state.waiters.push(waiter)
    })
  }

  private async sendFrameWithFlowControl(
    parser: HTTP2Parser,
    streamId: number,
    frame: Uint8Array,
    writer: StreamWriter,
    signal?: AbortSignal,
    timeoutMs: number = DEFAULT_SEND_WINDOW_TIMEOUT
  ): Promise<void> {
    if (signal?.aborted) {
      throw new Error('Operation aborted')
    }
    const payloadLength = Math.max(0, frame.length - 9)
    if (payloadLength > 0) {
      const { conn, stream } = parser.getSendWindows(streamId)
      if (conn < payloadLength || stream < payloadLength) {
        console.debug(`[stream ${streamId}] waiting for send window: need=${payloadLength} conn=${conn} stream=${stream}`)
      }
      try {
        await parser.waitForSendWindow(streamId, payloadLength, timeoutMs)
      } catch (err) {
        console.warn(`[stream ${streamId}] send window wait failed (${(err as Error)?.message ?? err}); continuing best-effort`)
        const forcedCredit = Math.max(payloadLength, 256 << 10)
        parser.unsafeForceExtendSendWindow(streamId, forcedCredit)
      }
    }
    if (signal?.aborted) {
      throw new Error('Operation aborted')
    }
    await writer.write(frame as any)
    if (payloadLength > 0) {
      parser.consumeSendWindow(streamId, payloadLength)
    }
  }

  private async acquireConnection(forceNew: boolean): Promise<Connection> {
  const key = this.peerAddr.toString()
    if (!forceNew) {
      const pooled = this.connectionPool.get(key)
      if (pooled) {
        const conn = pooled.connection
        if (conn) {
          const status = (conn as any)?.stat?.status?.toLowerCase?.()
          if (!status || status === 'open') {
            return conn
          }
          this.connectionPool.delete(key)
        } else {
          return pooled.promise
        }
      }
    }

    const dialPromise = this.node.dial(this.peerAddr, {
      signal: AbortSignal.timeout(dialTimeout)
    })
      .then(async (conn) => {
        if (!forceNew) {
          const poolEntry = this.connectionPool.get(key)
          if (poolEntry && poolEntry.promise === dialPromise) {
            poolEntry.connection = conn
            const removeFromPool = () => {
              const current = this.connectionPool.get(key)
              if (current && current.promise === dialPromise) {
                this.connectionPool.delete(key)
              }
            }
            try {
              const anyConn = conn as any
              if (typeof anyConn.addEventListener === 'function') {
                anyConn.addEventListener('close', removeFromPool, { once: true })
              } else if (typeof anyConn.once === 'function') {
                anyConn.once('close', removeFromPool)
              }
            } catch {}
          }
        }
        return conn
      })
      .catch((err) => {
        if (!forceNew) {
          const pooled = this.connectionPool.get(key)
          if (pooled && pooled.promise === dialPromise) {
            this.connectionPool.delete(key)
          }
        }
        throw err
      })

    if (!forceNew) {
      this.connectionPool.set(key, { promise: dialPromise })
    }

    return dialPromise
  }

  private getDefaultTransportProfile(mode: CallMode): TransportProfile {
    switch (mode) {
      case 'server-streaming':
        return 'compatibility'
      case 'client-streaming':
      case 'bidirectional':
        return 'flow-control'
      default:
        return 'flow-control'
    }
  }

    setToken(token:string) {
        this.token = token
    }
    

    async unaryCall(method:string, requestData:Uint8Array,timeout:number = 30000): Promise<Uint8Array> {  
    let stream:Stream|null = null
    let responseData: Uint8Array | null = null
    let responseBuffer: Uint8Array[] = [] // 添加缓冲区来累积数据
    let responseDataExpectedLength = -1 // 当前响应的期望长度
    const hpack = new HPACK()
    let exitFlag = false
    let errMsg = ''
    let isResponseComplete = false // 添加标志来标识响应是否完成
    let connection: Connection | null = null
    let state: ConnectionState | null = null
    let streamSlotAcquired = false
    try {
       
       // const stream = await this.node.dialProtocol(this.peerAddr, this.protocol)
        connection = await this.acquireConnection(false)
        const connectionKey = this.peerAddr.toString()
        state = this.getConnectionState(connection as object)
        try {
          await this.waitForStreamSlot(state, undefined, timeout)
          state.activeStreams += 1
          streamSlotAcquired = true
        } catch (err) {
          console.warn('[unaryCall] waiting for stream slot failed:', err)
          throw err
        }
        stream = await connection.newStream(this.protocol, {
          maxOutboundStreams: 10,
          negotiateFully: false
        })
        const streamManager = this.getStreamManagerFor(connection as object)
        const streamId = await streamManager.getNextAppLevelStreamId()
        const writer = new StreamWriter(stream.sink, { bufferSize: 16 * 1024 * 1024 })  
        try {
          writer.addEventListener('backpressure', (e: any) => {
            const d = e?.detail || {};
            console.warn(`[unary stream ${streamId}] backpressure current=${d.currentSize} avg=${d.averageSize} threshold=${d.threshold}`)
          })
          writer.addEventListener('drain', (e: any) => {
            const d = e?.detail || {};
            console.debug(`[unary stream ${streamId}] drain drained=${d.drained} queue=${d.queueSize}`)
          })
          writer.addEventListener('stalled', (e: any) => {
            const d = e?.detail || {};
            console.warn(`[unary stream ${streamId}] stalled queue=${d.queueSize} drained=${d.drained} since=${d.sinceMs}ms — sending PING`)
            try {
              const payload = new Uint8Array(8);
              crypto.getRandomValues?.(payload);
              const ping = Http2Frame.createFrame(0x6, 0x0, 0, payload);
              writer.write(ping as any);
            } catch {}
          })
        } catch {}
  const parser = new HTTP2Parser(writer);  
        parser.onGoaway = (info) => {
          console.warn('[unaryCall] GOAWAY received from server', info);
          this.connectionPool.delete(connectionKey);
          if (state) {
            this.rejectStreamWaiters(state, new Error('Connection received GOAWAY'))
          }
          try {
            (connection as any)?.close?.();
          } catch (err) {
            console.warn('Error closing connection after GOAWAY:', err);
          }
        };
        parser.onSettingsParsed = (settings) => {
          if (state && settings.maxConcurrentStreams !== undefined && settings.maxConcurrentStreams > 0) {
            state.maxConcurrentStreams = settings.maxConcurrentStreams
            this.notifyStreamSlotAvailable(state)
          }
        }
  parser.registerOutboundStream(streamId)
        responseDataExpectedLength  = -1 // 重置期望长度
        responseBuffer = [] // 重置缓冲区
        parser.onData = (payload,frameHeader) => {//接收数据
            if (responseDataExpectedLength === -1) {//grpc消息头部未读取
              //提取gRPC消息头部
              if (payload.length < 5) {
                return
              }
              const compressionFlag = payload[0] // 压缩标志
              const lengthBytes = payload.slice(1, 5) // 消息长度的4字节
              responseDataExpectedLength = new DataView(lengthBytes.buffer, lengthBytes.byteOffset).getUint32(0, false) // big-endian
              if (responseDataExpectedLength < 0) {
                throw new Error('Invalid gRPC message length')
              }
              if (responseDataExpectedLength + 5 >  payload.length) {
                // 如果当前 payload 不足以包含完整的 gRPC 消息，缓存数据
                const grpcData = payload.subarray(5) 
                responseBuffer.push(grpcData)
                responseDataExpectedLength -= grpcData.length // 更新期望长度
                return
              }else {
                // 如果当前 payload 足以包含完整的 gRPC 消息，重置缓冲区
                const grpcData = payload.subarray(5)  // 提取完整的 gRPC 消息
                responseBuffer.push(grpcData)
                responseData = grpcData
                isResponseComplete = true
                responseDataExpectedLength = -1 // 重置期望长度
              }
            }else if (responseDataExpectedLength > 0) {//grpc消息头部已读取
              responseBuffer.push(payload) // 将数据添加到缓冲区
              responseDataExpectedLength -= payload.length // 更新期望长度
              if (responseDataExpectedLength <= 0) {
                // 如果缓冲区中的数据已经完全处理，重置缓冲区
                responseData = new Uint8Array(responseBuffer.reduce((sum, chunk) => sum + chunk.length, 0))
                let offset = 0
                for (const chunk of responseBuffer) {
                    responseData.set(chunk, offset)
                    offset += chunk.length

                }
                responseDataExpectedLength = -1
                isResponseComplete = true // 设置响应完成标志
              }
            }
            // 检查是否是流的最后一个帧（END_STREAM 标志）
            if (frameHeader && (frameHeader.flags & 0x1) && !isResponseComplete) { // END_STREAM flag
                // 合并所有缓冲的数据
                const totalLength = responseBuffer.reduce((sum, chunk) => sum + chunk.length, 0)
                responseData = new Uint8Array(totalLength)
                let offset = 0
                for (const chunk of responseBuffer) {
                    responseData.set(chunk, offset)
                    offset += chunk.length
                }
                isResponseComplete = true
            }
        }
        parser.onEnd = () => {//接收结束
          if (!isResponseComplete) {
            isResponseComplete = true // 设置响应完成标志
            if (responseBuffer.length === 0) {
                responseData = new Uint8Array() // 如果没有数据，返回空数组
            }else{
                // 合并所有缓冲的数据
                const totalLength = responseBuffer.reduce((sum, chunk) => sum + chunk.length, 0)
                responseData = new Uint8Array(totalLength)
                let offset = 0
                for (const chunk of responseBuffer) {
                    responseData.set(chunk, offset)
                    offset += chunk.length
                }
                isResponseComplete = true
            }
          }
        }
        parser.onSettings = () => {//接收settings,反馈ack
            const ackSettingFrame = Http2Frame.createSettingsAckFrame()
            writer.write(ackSettingFrame as any);
        }
        parser.onHeaders = (headers,header) => {
            const plainHeaders = hpack.decodeHeaderFields(headers)
            if (plainHeaders.get('grpc-status') === '0') {
            } else if (plainHeaders.get('grpc-status') !== undefined) {
                exitFlag = true
                errMsg = plainHeaders.get('grpc-message') || 'gRPC call failed'
            }
        }
        parser.processStream(stream);
        // 握手
        const preface = Http2Frame.createPreface();  
        await writer.write(preface as any);
        // 发送Settings请求
        const settingFrme = Http2Frame.createSettingsFrame()
        await writer.write(settingFrme as any);
        // 等待对端 SETTINGS 或 ACK，择一即可，避免偶发握手竞态
        {
          let progressed = false
          await Promise.race([
            (async()=>{ await parser.waitForPeerSettings(1000); progressed = true })(),
            (async()=>{ await parser.waitForSettingsAck(); progressed = true })(),
            new Promise<void>(res=>setTimeout(res, 300))
          ])
          // 即使未等到，也继续；多数实现会随后发送
        }
        // 创建头部帧
        const headerFrame = Http2Frame.createHeadersFrame( streamId,method,true,this.token)
        await writer.write(headerFrame as any);
        // 直接按帧大小分片发送（保持与之前一致的稳定路径）
        const dataFrames = Http2Frame.createDataFrames(streamId, requestData, true)
        const frameSendTimeout = timeout > 0 ? timeout : DEFAULT_SEND_WINDOW_TIMEOUT
        for (const df of dataFrames) {
          await this.sendFrameWithFlowControl(parser, streamId, df, writer, undefined, frameSendTimeout)
        }
        // 等待responseData 不为空,或超时
        await new Promise((resolve, reject) => {
            const t = setTimeout(() => {
                reject(new Error('gRPC response timeout'))
            }, timeout)
            const checkResponse = () => {
                if (isResponseComplete || exitFlag) { // 使用新的完成标志
                    clearTimeout(t)
                    resolve(responseData)
                } else {
                    setTimeout(checkResponse, 50)
                }
            }
            checkResponse()
        })
  try { await (writer as any).flush?.(timeout); } catch {}
  await writer.end()
    } catch (err) {
        console.error('unaryCall error:', err)
        throw err
    }finally{
        if (stream) {
            await stream.close()
        }
        if (streamSlotAcquired && state) {
          state.activeStreams = Math.max(0, state.activeStreams - 1)
          this.notifyStreamSlotAvailable(state)
        }
    }
    if (exitFlag) {
        throw new Error(errMsg)
    }
    if (!responseData) {
       responseData = new Uint8Array()
    }
    return responseData
}
    
  /**
 * 执行GRPC调用，支持通过context和返回的取消函数控制终止
 * @param method GRPC方法名
 * @param requestData 请求数据
 * @param timeout 超时时间(毫秒)
 * @param mode 调用模式: 'unary'|'server-streaming'|'client-streaming'|'bidirectional'
 * @param onDataCallback 数据回调函数
 * @param dataSourceCallback 客户端流数据源回调，支持单个chunk或批量chunks（使用frames策略优化）
 * @param onEndCallback 结束回调函数
 * @param onErrorCallback 错误回调函数
 * @param context 操作上下文，包含AbortSignal用于取消操作
 * @param options 调用选项（可配置批处理、连接行为以及传输策略）
 * @param options.transportProfile 传输策略: 'flow-control'（默认，适合上传/双向流）或 'compatibility'（兼容旧逻辑，适合高并发 server-streaming）
 * @returns 取消函数，可随时调用终止操作
 */
async Call(   
  method: string,  
  requestData: Uint8Array,  
  timeout: number = 30000,  
  mode: CallMode, 
  onDataCallback: (payload: Uint8Array) => void,  
  dataSourceCallback?: () => AsyncIterable<Uint8Array | Uint8Array[]>,
  onEndCallback?: () => void,  
  onErrorCallback?: (error: unknown) => void,
  context?: { signal?: AbortSignal },
  options?: CallOptions
) {  
  // 创建内部AbortController用于控制操作
  const internalController = new AbortController();
  let timeoutHandle: any;
  let stream: Stream | null = null;

  const profile: TransportProfile = options?.transportProfile ?? this.getDefaultTransportProfile(mode);
  const useFlowControl = profile === 'flow-control';
  
  // 取消函数 - 将在最后返回给调用者
  const cancelOperation = () => {
    internalController.abort();
    if (timeoutHandle) {
      clearTimeout(timeoutHandle);
    }
    if (stream) {
      try {
        stream.close();
      } catch (err) {
        console.error('Error closing stream on cancel:', err);
      }
    }
  };
  
  // 如果提供了外部信号，监听它
  if (context?.signal) {
    // 如果外部信号已经触发中止，立即返回
    if (context.signal.aborted) {
      if (onErrorCallback) {
        onErrorCallback(new Error('Operation aborted by context'));
      }
      cancelOperation();
    }
    
    // 监听外部的abort事件
    context.signal.addEventListener('abort', () => {
      cancelOperation();
    });
  }
  
  // 超时Promise
  const timeoutPromise = new Promise<never>((_, reject) => {
    timeoutHandle = setTimeout(() => {
      reject(new Error('Operation timed out'));
      cancelOperation();
    }, timeout);
  });
  
  // 主操作Promise
  const operationPromise = (async () => {  
    let messageBuffer = new Uint8Array(0); // 用于累积跨帧的消息数据
    let expectedMessageLength = -1; // 当前消息的期望长度
    const hpack = new HPACK();
    let connection: Connection | null = null;
    let connectionKey: string | null = null;
    let state: ConnectionState | null = null;
    let streamSlotAcquired = false;
    
    try {
      // 检查是否已经中止
      if (internalController.signal.aborted) {
        throw new Error('Operation aborted');
      }
      
      // 如开启 freshConnection，则在拨号前尝试断开现有连接，确保本次使用全新连接（注意：会影响与该节点的其他并发）
      if (options?.freshConnection) {
        try {
          this.connectionPool.delete(this.peerAddr.toString())
          await (this as any).node?.hangUp?.(this.peerAddr as any);
          console.warn('[Call] hangUp existing connection before dialing due to freshConnection=true');
        } catch (err) {
          console.warn('[Call] hangUp failed or not supported, proceeding to dial', err);
        }
      }

      connection = await this.acquireConnection(Boolean(options?.freshConnection));
      connectionKey = this.peerAddr.toString();
      state = this.getConnectionState(connection as object);
      if (state) {
        try {
          await this.waitForStreamSlot(state, internalController.signal, timeout);
          state.activeStreams += 1;
          streamSlotAcquired = true;
        } catch (err) {
          console.warn('[Call] waiting for stream slot failed:', err);
          throw err;
        }
      }
        stream = await connection.newStream(this.protocol, {
          maxOutboundStreams: 50,
          signal: AbortSignal.timeout(10000),
          negotiateFully: false
        })
        const streamManager = this.getStreamManagerFor(connection as object);
      const streamId = await streamManager.getNextAppLevelStreamId();  
  const writer = new StreamWriter(stream.sink, { bufferSize: 16 * 1024 * 1024 });  
      try {
        writer.addEventListener('backpressure', (e: any) => {
          const d = e?.detail || {};
          console.warn(`[stream ${streamId}] backpressure current=${d.currentSize} avg=${d.averageSize} threshold=${d.threshold}`)
        })
        writer.addEventListener('drain', (e: any) => {
          const d = e?.detail || {};
        })
        writer.addEventListener('stalled', (e: any) => {
          const d = e?.detail || {};
          console.warn(`[stream ${streamId}] stalled queue=${d.queueSize} drained=${d.drained} since=${d.sinceMs}ms — sending PING`)
          try {
            const payload = new Uint8Array(8);
            crypto.getRandomValues?.(payload);
            const ping = Http2Frame.createFrame(0x6, 0x0, 0, payload);
            writer.write(ping as any);
          } catch {}
        })
      } catch {}
  const parser = new HTTP2Parser(writer, { compatibilityMode: !useFlowControl });
      parser.onGoaway = (info) => {
        console.warn('[Call] GOAWAY received from server', info);
        if (connectionKey) {
          this.connectionPool.delete(connectionKey);
        }
        if (state) {
          this.rejectStreamWaiters(state, new Error('Connection received GOAWAY'));
        }
        try {
          (connection as any)?.close?.();
        } catch (err) {
          console.warn('Error closing connection after GOAWAY:', err);
        }
      };
      parser.onSettingsParsed = (settings) => {
        if (state && settings.maxConcurrentStreams !== undefined && settings.maxConcurrentStreams > 0) {
          state.maxConcurrentStreams = settings.maxConcurrentStreams;
          this.notifyStreamSlotAvailable(state);
        }
      };
      if (useFlowControl) {
        parser.registerOutboundStream(streamId);
      }
      const sendWindowTimeout = timeout > 0 ? timeout : DEFAULT_SEND_WINDOW_TIMEOUT;
      const writeFrame = async (frame: Uint8Array) => {
        if (useFlowControl) {
          await this.sendFrameWithFlowControl(parser, streamId, frame, writer, internalController.signal, sendWindowTimeout);
        } else {
          if (internalController.signal.aborted) {
            throw new Error('Operation aborted');
          }
          await writer.write(frame as any);
        }
      };
      const writeDataFrames = async (frames: Uint8Array[]) => {
        for (const frame of frames) {
          await writeFrame(frame);
        }
      };
      
      // 在各个回调中检查是否已中止
      parser.onData = async (payload, frameHeader): Promise<void> => {
        // 检查是否已中止
        if (internalController.signal.aborted) {
          return;
        }
        
        try {  
          // 将新数据添加到消息缓冲区
          const newBuffer = new Uint8Array(messageBuffer.length + payload.length);
          newBuffer.set(messageBuffer);
          newBuffer.set(payload, messageBuffer.length);
          messageBuffer = newBuffer;
          
          // 处理缓冲区中的完整消息
          while (messageBuffer.length > 0) {
            // 如果已经中止，停止处理
            if (internalController.signal.aborted) {
              return;
            }
            
            // 如果还没有读取消息长度，且缓冲区有足够数据
            if (expectedMessageLength === -1 && messageBuffer.length >= 5) {
              // 读取 gRPC 消息头：1字节压缩标志 + 4字节长度
              const compressionFlag = messageBuffer[0];
              const lengthBytes = messageBuffer.slice(1, 5);
              expectedMessageLength = new DataView(lengthBytes.buffer, lengthBytes.byteOffset).getUint32(0, false); // big-endian
            }
            
            // 如果知道期望长度且有足够数据
            if (expectedMessageLength !== -1 && messageBuffer.length >= expectedMessageLength + 5) {
              // 提取完整消息（跳过5字节头部）
              const completeMessage = messageBuffer.slice(5, expectedMessageLength + 5);
              
              // 调用回调处理这个完整消息
              onDataCallback(completeMessage);
              
              // 移除已处理的消息，保留剩余数据
              messageBuffer = messageBuffer.slice(expectedMessageLength + 5);
              expectedMessageLength = -1;
            } else {
              // 没有足够数据构成完整消息，等待更多数据
              break;
            }
          }
        } catch (error: unknown) {  
          if (onErrorCallback) {  
            onErrorCallback(error);  
          } else {  
            throw error;  
          }  
        }  
      };
      
      parser.onSettings = () => {
        // 检查是否已中止
        if (internalController.signal.aborted) return;
        
        const ackSettingFrame = Http2Frame.createSettingsAckFrame();
        writer.write(ackSettingFrame as any);
      }
      
      parser.onHeaders = (headers, header) => {
        // 检查是否已中止
        if (internalController.signal.aborted) return;
        
        const plainHeaders = hpack.decodeHeaderFields(headers);
        if (plainHeaders.get('grpc-status') === '0') {
          // 成功状态
        } else if (plainHeaders.get('grpc-status') !== undefined) {
          const errMsg = plainHeaders.get('grpc-message') || 'gRPC call failed';
          const err = new Error(errMsg);
          if (onErrorCallback) {  
            onErrorCallback(err);  
          } else {  
            throw err;  
          }  
        }
      }
      
      parser.processStream(stream);
      
      // 检查是否已中止
      if (internalController.signal.aborted) {
        throw new Error('Operation aborted');
      }
      
  // Handshake - send HTTP/2 preface  
      const preface = Http2Frame.createPreface();  
      await writer.write(preface as any);
      
      // 检查是否已中止
      if (internalController.signal.aborted) {
        throw new Error('Operation aborted');
      }

  // Send Settings request  
      const settingFrame = Http2Frame.createSettingsFrame();  
      await writer.write(settingFrame as any);
      
      // 检查是否已中止
      if (internalController.signal.aborted) {
        throw new Error('Operation aborted');
      }
      
      // 等待对端 SETTINGS 或 ACK，择一即可，避免偶发握手竞态
      {
        let progressed = false;
        await Promise.race([
          (async()=>{ await parser.waitForPeerSettings(1000); progressed = true })(),
          (async()=>{ await parser.waitForSettingsAck(); progressed = true })(),
          new Promise<void>(res=>setTimeout(res, 300))
        ]);
        // 即使未等到，也继续；多数实现会随后发送
      }

      // 检查是否已中止
      if (internalController.signal.aborted) {
        throw new Error('Operation aborted');
      }
      
      // 检查是否已中止
      if (internalController.signal.aborted) {
        throw new Error('Operation aborted');
      }

      // Create header frame  
      const headerFrame = Http2Frame.createHeadersFrame(streamId, method, true, this.token);  
      if (mode === 'unary' || mode === 'server-streaming') {  
        await writer.write(new Uint8Array([...headerFrame]) as any);  
  const dfs = Http2Frame.createDataFrames(streamId, requestData, true)
  await writeDataFrames(dfs)
        
        // 检查是否已中止
        if (internalController.signal.aborted) {
          throw new Error('Operation aborted');
        }
      } else if ((mode === 'client-streaming' || mode === 'bidirectional') && dataSourceCallback) {  
        await writer.write(headerFrame as any);
        
        // 检查是否已中止
        if (internalController.signal.aborted) {
          throw new Error('Operation aborted');
        }
        
        if (requestData.length > 0) {
          const dfs0 = Http2Frame.createDataFrames(streamId, requestData, false)
          await writeDataFrames(dfs0)
        }
        
        // 动态批量处理逻辑 - 在处理过程中动态补充新数据
        const batchSize = options?.batchSize || 10;
        const maxBatchWaitMs = options?.maxBatchWaitMs || 50;
        
        // 动态批处理器
        const processingQueue: {
          chunk: Uint8Array;
          resolve: (value: void | PromiseLike<void>) => void;
          reject: (reason?: any) => void;
        }[] = [];
        
        let isProcessing = false;
        
        const processNextBatch = async () => {
          if (isProcessing || processingQueue.length === 0) return;
          isProcessing = true;
          
          let currentBatch: typeof processingQueue = [];
          
          try {
            // 收集当前批次的数据
            currentBatch = processingQueue.splice(0, Math.min(batchSize, processingQueue.length));
            
            if (currentBatch.length > 1) {
              // 批量处理：为每个chunk创建HTTP/2帧
              // 顺序按窗口发送每个 chunk（避免跨流窗口互相干扰）
              for (const item of currentBatch) {
                if (internalController.signal.aborted) throw new Error('Operation aborted');
                const frames = Http2Frame.createDataFrames(streamId, item.chunk, false)
                await writeDataFrames(frames)
              }
              
              // 通知所有chunk处理完成
              currentBatch.forEach(item => item.resolve());
            } else if (currentBatch.length === 1) {
              // 单个chunk处理
              const item = currentBatch[0];
              const frames1 = Http2Frame.createDataFrames(streamId, item.chunk, false)
              await writeDataFrames(frames1)
              item.resolve();
            }
          } catch (error) {
            // 处理错误，通知当前批次的所有chunk处理失败
            currentBatch.forEach(item => {
              try {
                item.reject(error);
              } catch (err) {
                // 忽略 reject 可能的错误（Promise 已经被处理）
                console.warn('Error rejecting promise:', err);
              }
            });
          } finally {
            isProcessing = false;
            
            // 如果队列中还有数据，继续处理
            if (processingQueue.length > 0 && !internalController.signal.aborted) {
              // 使用 setTimeout 避免阻塞，让新数据有机会加入队列
              setTimeout(() => processNextBatch(), 0);
            }
          }
        };
        
        const addToQueue = (chunk: Uint8Array): Promise<void> => {
          return new Promise((resolve, reject) => {
            // 检查是否已经取消
            if (internalController.signal.aborted) {
              reject(new Error('Operation aborted'));
              return;
            }
            
            processingQueue.push({ chunk, resolve, reject });
            
            // 如果队列达到批量大小或没有在处理，立即开始处理
            if (processingQueue.length >= batchSize || !isProcessing) {
              processNextBatch().catch(err => {
                console.error('Error in processNextBatch:', err);
              });
            }
          });
        };
        
        // 处理数据源
        try {
          for await (const chunkOrChunks of dataSourceCallback()) {
            // 检查是否已中止
            if (internalController.signal.aborted) {
              throw new Error('Operation aborted');
            }
            
            // 处理单个chunk或批量chunks
            const chunksToProcess: Uint8Array[] = Array.isArray(chunkOrChunks) 
              ? chunkOrChunks 
              : [chunkOrChunks];
            
            // 将所有chunks添加到动态处理队列
            const addPromises = chunksToProcess.map(chunk => addToQueue(chunk));
            
            // 等待当前批次的chunks被添加到队列（不等待处理完成）
            await Promise.all(addPromises);
          }
        } catch (error) {
          // 取消所有待处理的Promise
          const remainingQueue = processingQueue.splice(0);
          remainingQueue.forEach(item => {
            try {
              item.reject(error);
            } catch (err) {
              console.warn('Error rejecting remaining promise:', err);
            }
          });
          throw error;
        }
        
        // 等待所有剩余的数据处理完成，添加超时保护
        const queueWaitStart = Date.now();
        const maxQueueWaitMs = timeout; // 使用主超时时间
        
        while (processingQueue.length > 0 || isProcessing) {
          if (internalController.signal.aborted) {
            throw new Error('Operation aborted');
          }
          
          // 防止无限等待
          if (Date.now() - queueWaitStart > maxQueueWaitMs) {
            // 清理剩余队列
            const remainingQueue = processingQueue.splice(0);
            remainingQueue.forEach(item => {
              try {
                item.reject(new Error('Queue wait timeout'));
              } catch (err) {
                console.warn('Error rejecting timeout promise:', err);
              }
            });
            throw new Error('Queue processing timeout');
          }
          
          await new Promise(resolve => setTimeout(resolve, 10));
        }  
        
        // 检查是否已中止
        if (internalController.signal.aborted) {
          throw new Error('Operation aborted');
        }
        
  const finalFrame = Http2Frame.createDataFrame(streamId, new Uint8Array(), true)
  await writeFrame(finalFrame)
  // 在结束前尽量冲刷内部队列，避免服务器看到部分数据 + context canceled
  try { await (writer as any).flush?.(timeout); } catch {}
  await writer.end();
      }
      
      // 检查是否已中止
      if (internalController.signal.aborted) {
        throw new Error('Operation aborted');
      }
      
      await parser.waitForEndOfStream(0);
      
      if (onEndCallback) {  
        onEndCallback();  
      }  
    } catch (err: unknown) {
      // 如果是由于取消导致的错误，使用特定的错误消息
      if (internalController.signal.aborted && err instanceof Error && err.message === 'Operation aborted') {
        if (onErrorCallback) {
          onErrorCallback(new Error('Operation cancelled by user'));
        }
      } else if (onErrorCallback) {  
        onErrorCallback(err);  
      } else {  
        if (err instanceof Error) {  
          console.error('asyncCall error:', err.message);  
        } else {  
          console.error('asyncCall error:', err);  
        }  
      }  
    } finally {
      clearTimeout(timeoutHandle);
      if (stream) {
        try {
          await stream.close();
        } catch (err) {
          console.error('Error closing stream:', err);
        }
      }
      // 如果本次强制使用了新连接，结束时尽量关闭它，避免连接泄漏
      if (options?.freshConnection) {
        try {
          // 通过 libp2p 连接管理器关闭到该 peer 的连接
          const conns = (this as any).node?.getConnections?.(this.peerAddr as any) || [];
          for (const c of conns) {
            try { await c.close?.(); } catch {}
          }
        } catch {}
      }
      if (streamSlotAcquired && state) {
        state.activeStreams = Math.max(0, state.activeStreams - 1);
        this.notifyStreamSlotAvailable(state);
      }
    } 
  })();
  
  try {
    // 执行操作并返回取消函数
    await Promise.race([operationPromise, timeoutPromise]);
    return cancelOperation;
  } catch (error) {
    // 确保在出错时也进行清理
    cancelOperation();
    throw error;
  }
}


   
}