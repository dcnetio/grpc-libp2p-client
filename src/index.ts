import type { Libp2p } from "libp2p";
import { HTTP2Parser } from "./dc-http2/parser.js";
import { StreamWriter } from "./dc-http2/stream.js";
import { Http2Frame } from "./dc-http2/frame.js";
import type { Connection, Stream } from "@libp2p/interface";
import { HPACK } from "./dc-http2/hpack.js";

import type { Multiaddr } from "@multiformats/multiaddr";

const dialTimeout = 5000; // 5秒
const DEFAULT_SEND_WINDOW_TIMEOUT = 15000;

type CallMode =
  | "unary"
  | "server-streaming"
  | "client-streaming"
  | "bidirectional";
type TransportProfile = "flow-control" | "compatibility";

interface CallOptions {
  batchSize?: number;
  maxBatchWaitMs?: number;
  freshConnection?: boolean;
  transportProfile?: TransportProfile;
}

interface ConnectionState {
  activeStreams: number;
  maxConcurrentStreams: number;
  waiters: Array<{ resolve: () => void; reject: (error: Error) => void }>;
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
  private connectionPool: Map<
    string,
    { promise: Promise<Connection>; connection?: Connection }
  >;

  constructor(
    node: Libp2p,
    peerAddr: Multiaddr,
    token: string,
    protocol?: string
  ) {
    this.node = node;
    this.peerAddr = peerAddr;
    if (protocol) {
      this.protocol = protocol;
    } else {
      this.protocol = "/dc/thread/0.0.1";
    }
    this.token = token;
    this.connectionStreamManagers = new WeakMap();
    this.connectionStates = new WeakMap();
    this.connectionPool = new Map();
  }

  private getStreamManagerFor(connection: object): StreamManager {
    let manager = this.connectionStreamManagers.get(connection);
    if (!manager) {
      manager = new StreamManager();
      this.connectionStreamManagers.set(connection, manager);
    }
    return manager;
  }

  private getConnectionState(connection: object): ConnectionState {
    let state = this.connectionStates.get(connection);
    if (!state) {
      state = {
        activeStreams: 0,
        maxConcurrentStreams: Number.POSITIVE_INFINITY,
        waiters: [],
      };
      this.connectionStates.set(connection, state);
    }
    return state;
  }

  private notifyStreamSlotAvailable(state: ConnectionState) {
    if (state.waiters.length === 0) {
      return;
    }
    while (
      state.waiters.length > 0 &&
      state.activeStreams < state.maxConcurrentStreams
    ) {
      const waiter = state.waiters.shift();
      if (!waiter) break;
      try {
        waiter.resolve();
      } catch (err) {
        console.error("Error resolving stream waiter:", err);
      }
    }
  }

  private rejectStreamWaiters(state: ConnectionState, error: Error) {
    if (state.waiters.length === 0) {
      return;
    }
    const waiters = state.waiters.splice(0);
    for (const waiter of waiters) {
      try {
        waiter.reject(error);
      } catch (err) {
        console.error("Error rejecting stream waiter:", err);
      }
    }
  }

  private async waitForStreamSlot(
    state: ConnectionState,
    signal?: AbortSignal,
    timeoutMs: number = dialTimeout
  ): Promise<void> {
    if (state.maxConcurrentStreams <= 0) {
      throw new Error(
        "No available HTTP/2 streams: server advertised zero concurrent streams"
      );
    }
    if (
      !Number.isFinite(state.maxConcurrentStreams) ||
      state.activeStreams < state.maxConcurrentStreams
    ) {
      return;
    }

    await new Promise<void>((resolve, reject) => {
      let settled = false;
      // eslint-disable-next-line prefer-const
      let timeoutId: ReturnType<typeof setTimeout> | undefined;
      const cleanup = () => {
        const idx = state.waiters.indexOf(waiter);
        if (idx >= 0) {
          state.waiters.splice(idx, 1);
        }
        if (timeoutId) {
          clearTimeout(timeoutId);
        }
        if (signal) {
          signal.removeEventListener("abort", onAbort);
        }
      };
      const waiter = {
        resolve: () => {
          if (settled) return;
          settled = true;
          cleanup();
          resolve();
        },
        reject: (err: Error) => {
          if (settled) return;
          settled = true;
          cleanup();
          reject(err);
        },
      };
      const onAbort = () => {
        waiter.reject(
          new Error("Aborted while waiting for available HTTP/2 stream slot")
        );
      };
      if (signal) {
        if (signal.aborted) {
          onAbort();
          return;
        }
        signal.addEventListener("abort", onAbort, { once: true });
      }
      timeoutId =
        timeoutMs > 0
          ? setTimeout(() => {
              waiter.reject(
                new Error("Timed out waiting for available HTTP/2 stream slot")
              );
            }, timeoutMs)
          : undefined;
      state.waiters.push(waiter);
    });
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
      throw new Error("Operation aborted");
    }
    const payloadLength = Math.max(0, frame.length - 9);
    if (payloadLength > 0) {
      const { conn, stream } = parser.getSendWindows(streamId);
      if (conn < payloadLength || stream < payloadLength) {
        console.debug(
          `[stream ${streamId}] waiting for send window: need=${payloadLength} conn=${conn} stream=${stream}`
        );
      }
      try {
        await parser.waitForSendWindow(streamId, payloadLength, timeoutMs);
      } catch (err) {
        console.warn(
          `[stream ${streamId}] send window wait failed (${
            (err as Error)?.message ?? err
          }); continuing best-effort`
        );
        const forcedCredit = Math.max(payloadLength, 256 << 10);
        parser.unsafeForceExtendSendWindow(streamId, forcedCredit);
      }
    }
    if (signal?.aborted) {
      throw new Error("Operation aborted");
    }
    await writer.write(frame);
    if (payloadLength > 0) {
      parser.consumeSendWindow(streamId, payloadLength);
    }
  }

  private async acquireConnection(forceNew: boolean): Promise<Connection> {
    const key = this.peerAddr.toString();
    if (!forceNew) {
      const pooled = this.connectionPool.get(key);
      if (pooled) {
        const conn = pooled.connection;
        if (conn) {
          const status = conn.status;
          if (!status || status === "open") {
            return conn;
          }
          this.connectionPool.delete(key);
        } else {
          return pooled.promise;
        }
      }
    }

    const dialPromise = this.node
      .dial(this.peerAddr, {
        signal: AbortSignal.timeout(dialTimeout),
      })
      .then(async (conn) => {
        if (!forceNew) {
          const poolEntry = this.connectionPool.get(key);
          if (poolEntry && poolEntry.promise === dialPromise) {
            poolEntry.connection = conn;
            const removeFromPool = () => {
              const current = this.connectionPool.get(key);
              if (current && current.promise === dialPromise) {
                this.connectionPool.delete(key);
              }
            };
            try {
              conn.addEventListener("close", removeFromPool, { once: true });
            } catch { /* ignore event listener registration errors */ }
          }
        }
        return conn;
      })
      .catch((err) => {
        if (!forceNew) {
          const pooled = this.connectionPool.get(key);
          if (pooled && pooled.promise === dialPromise) {
            this.connectionPool.delete(key);
          }
        }
        throw err;
      });

    if (!forceNew) {
      this.connectionPool.set(key, { promise: dialPromise });
    }

    return dialPromise;
  }

  private getDefaultTransportProfile(mode: CallMode): TransportProfile {
    switch (mode) {
      case "server-streaming":
        return "compatibility";
      case "client-streaming":
      case "bidirectional":
        return "flow-control";
      default:
        return "flow-control";
    }
  }

  setToken(token: string) {
    this.token = token;
  }

  /** 从 peerAddr 提取 HTTP/2 :authority 字段（host:port 格式） */
  private getAuthority(): string {
    try {
      const addr = this.peerAddr.toString();
      const ip4 = addr.match(/\/ip4\/(\d[\d.]+)\/tcp\/(\d+)/);
      if (ip4) return `${ip4[1]}:${ip4[2]}`;
      const ip6 = addr.match(/\/ip6\/([^/]+)\/tcp\/(\d+)/);
      if (ip6) return `[${ip6[1]}]:${ip6[2]}`;
      const dns = addr.match(/\/dns(?:4|6)?\/([.\w-]+)\/tcp\/(\d+)/);
      if (dns) return `${dns[1]}:${dns[2]}`;
    } catch { /* ignore */ }
    return 'localhost';
  }

  async unaryCall(
    method: string,
    requestData: Uint8Array,
    timeout: number = 30000
  ): Promise<Uint8Array> {
    let stream: Stream | null = null;
    let responseData: Uint8Array | null = null;
    let responseBuffer: Uint8Array[] = []; // 添加缓冲区来累积数据
    let responseDataExpectedLength = -1; // 当前响应的期望长度
    /** 跨 DATA 帧的部分 gRPC 消息头缓冲（当一帧的 payload < 5 字节时积累） */
    let headerPartialBuffer: Uint8Array[] = [];
    const hpack = new HPACK();
    let exitFlag = false;
    let errMsg = "";
    let isResponseComplete = false; // 添加标志来标识响应是否完成
    /** 事件驱动：响应完成时的唤醒函数 */
    let notifyResponseComplete: (() => void) | null = null;
    let connection: Connection | null = null;
    let state: ConnectionState | null = null;
    let streamSlotAcquired = false;
    // 提升 writer 作用域到 finally 可访问，确保错误路径下也能调用 abort() 清理资源
    let writerRef: StreamWriter | null = null;
    try {
      // const stream = await this.node.dialProtocol(this.peerAddr, this.protocol)
      connection = await this.acquireConnection(false);
      const connectionKey = this.peerAddr.toString();
      state = this.getConnectionState(connection as object);
      try {
        await this.waitForStreamSlot(state, undefined, timeout);
        state.activeStreams += 1;
        streamSlotAcquired = true;
      } catch (err) {
        console.warn("[unaryCall] waiting for stream slot failed:", err);
        throw err;
      }
      stream = await connection.newStream(this.protocol, {
        maxOutboundStreams: 10,
        negotiateFully: false,
      });
      const streamManager = this.getStreamManagerFor(connection as object);
      const streamId = await streamManager.getNextAppLevelStreamId();
      const writer = new StreamWriter(stream, {
        bufferSize: 16 * 1024 * 1024,
      });
      writerRef = writer;
      try {
        writer.addEventListener("backpressure", (e: CustomEvent) => {
          const d = e.detail || {};
          console.warn(
            `[unary stream ${streamId}] backpressure current=${d.currentSize} avg=${d.averageSize} threshold=${d.threshold}`
          );
        });
        writer.addEventListener("drain", () => {
          // drain event - no action needed
        });
        writer.addEventListener("stalled", (e: CustomEvent) => {
          const d = e.detail || {};
          console.warn(
            `[unary stream ${streamId}] stalled queue=${d.queueSize} drained=${d.drained} since=${d.sinceMs}ms — sending PING`
          );
          try {
            const payload = new Uint8Array(8);
            crypto.getRandomValues?.(payload);
            const ping = Http2Frame.createFrame(0x6, 0x0, 0, payload);
            writer.write(ping);
          } catch { /* ignore ping write errors */ }
        });
      } catch { /* ignore addEventListener errors */ }
      const parser = new HTTP2Parser(writer);
      parser.onGoaway = (info) => {
        console.warn("[unaryCall] GOAWAY received from server", info);
        this.connectionPool.delete(connectionKey);
        if (state) {
          this.rejectStreamWaiters(
            state,
            new Error("Connection received GOAWAY")
          );
        }
        exitFlag = true;
        errMsg = `GOAWAY received: code=${info.errorCode}`;
        notifyResponseComplete?.(); // 唤醒等待中的 Promise
        try {
          connection?.close();
        } catch (err) {
          console.warn("Error closing connection after GOAWAY:", err);
        }
      };
      parser.onSettingsParsed = (settings) => {
        if (
          state &&
          settings.maxConcurrentStreams !== undefined &&
          settings.maxConcurrentStreams > 0
        ) {
          state.maxConcurrentStreams = settings.maxConcurrentStreams;
          this.notifyStreamSlotAvailable(state);
        }
      };
      parser.registerOutboundStream(streamId);
      responseDataExpectedLength = -1; // 重置期望长度
      responseBuffer = []; // 重置缓冲区
      headerPartialBuffer = []; // 重置跨帧头部缓冲
      parser.onData = (payload, frameHeader)  => {
        //接收数据
        if (responseDataExpectedLength === -1) {
          //grpc消息头部未读取
          // 如果有跨帧积累的部分头字节，先与本帧 payload 合并
          let effectivePayload = payload;
          if (headerPartialBuffer.length > 0) {
            headerPartialBuffer.push(payload);
            const totalLen = headerPartialBuffer.reduce((s, c) => s + c.length, 0);
            effectivePayload = new Uint8Array(totalLen);
            let off = 0;
            for (const c of headerPartialBuffer) { effectivePayload.set(c, off); off += c.length; }
            headerPartialBuffer = [];
          }
          //提取gRPC消息头部
          if (effectivePayload.length < 5) {
            // 头部字节不足 5，先缓存，等待后续帧补全
            headerPartialBuffer.push(effectivePayload);
            return;
          }
          const lengthBytes = effectivePayload.slice(1, 5); // 消息长度的4字节
          responseDataExpectedLength = new DataView(
            lengthBytes.buffer,
            lengthBytes.byteOffset
          ).getUint32(0, false); // big-endian（getUint32 返回无符号整数，结果不会为负）
          if (responseDataExpectedLength + 5 > effectivePayload.length) {
            // 如果当前 payload 不足以包含完整的 gRPC 消息，缓存数据
            const grpcData = effectivePayload.subarray(5);
            responseBuffer.push(grpcData);
            responseDataExpectedLength -= grpcData.length; // 更新期望长度
            return;
          } else {
            // payload 已包含完整的 gRPC 消息体，精确截取（避免尾部多余字节污染）
            const msgLen = responseDataExpectedLength;
            const grpcData = effectivePayload.slice(5, 5 + msgLen);
            responseBuffer.push(grpcData);
            responseData = grpcData;
            isResponseComplete = true;
            responseDataExpectedLength = -1;
            notifyResponseComplete?.();
          }
        } else if (responseDataExpectedLength > 0) {
          //grpc消息头部已读取
          responseDataExpectedLength -= payload.length;
          if (responseDataExpectedLength <= 0) {
            // 超收时截掉多余字节
            const exactPayload = responseDataExpectedLength < 0
              ? payload.slice(0, payload.length + responseDataExpectedLength)
              : payload;
            responseBuffer.push(exactPayload);
            responseData = new Uint8Array(
              responseBuffer.reduce((sum, chunk) => sum + chunk.length, 0)
            );
            let offset = 0;
            for (const chunk of responseBuffer) {
              responseData.set(chunk, offset);
              offset += chunk.length;
            }
            responseDataExpectedLength = -1;
            isResponseComplete = true;
            notifyResponseComplete?.();
          } else {
            responseBuffer.push(payload); // 还不完整，继续累积
          }
        }
        // END_STREAM 兜底：数据路径已处理大多数情况；此分支仅在边缘情况下触发
        if (frameHeader && frameHeader.flags & 0x1 && !isResponseComplete) {
          if (responseBuffer.length > 0) {
            const totalLength = responseBuffer.reduce((sum, c) => sum + c.length, 0);
            responseData = new Uint8Array(totalLength);
            let offset = 0;
            for (const chunk of responseBuffer) { responseData.set(chunk, offset); offset += chunk.length; }
          } else {
            responseData = new Uint8Array(0);
          }
          isResponseComplete = true;
          notifyResponseComplete?.();
        }
      };
      parser.onEnd = () => {
        // 流结束时若响应未标记完成（空响应 / 纯 trailers），强制标记并唤醒等待者
        if (!isResponseComplete) {
          isResponseComplete = true;
          notifyResponseComplete?.();
        }
      };
      parser.onSettings = () => {
        //接收settings,反馈ack
        const ackSettingFrame = Http2Frame.createSettingsAckFrame();
        writer.write(ackSettingFrame);
      };
      parser.onHeaders = (headers) => {
        const plainHeaders = hpack.decodeHeaderFields(headers);
        if (plainHeaders.get("grpc-status") === "0") {
          // 成功状态
        } else if (plainHeaders.get("grpc-status") !== undefined) {
          exitFlag = true;
          errMsg = plainHeaders.get("grpc-message") || "gRPC call failed";
          notifyResponseComplete?.(); // 唤醒等待中的 Promise
        }
      };
      // 启动后台流处理，捕获任何异步错误
      parser.processStream(stream).catch((error: unknown) => {
        // 若响应已完整收到（isResponseComplete=true），后置的网络层错误属于正常的
        // 连接拆除过程（如服务端 RST、连接关闭），不影响已成功的调用结果，静默忽略。
        // 若响应尚未完成，才记录错误并唤醒等待者，触发超时/错误路径。
        if (!isResponseComplete) {
          console.error('Error in processStream:', error);
          exitFlag = true;
          if (!errMsg) {
            errMsg = error instanceof Error ? error.message : 'Stream processing failed';
          }
          notifyResponseComplete?.(); // 流处理异常也需唤醒等待者
        }
      });
      
      // 握手
      const preface = Http2Frame.createPreface();
      await writer.write(preface);
      // 发送Settings请求
      const settingFrme = Http2Frame.createSettingsFrame();
      await writer.write(settingFrme);
      // 等待对端 SETTINGS 或 ACK，择一即可，避免偶发握手竞态
      // 注意：未胜出的 promise 内部有超时定时器，它们最终会 reject。
      // 必须绑定 .catch(…) 消除错误，否则在 Node.js 新版本中会导致 UnhandledPromiseRejection 崩溃。
      await Promise.race([
        parser.waitForPeerSettings(1000).catch(() => {}),
        parser.waitForSettingsAck().catch(() => {}),
        new Promise<void>((res) => setTimeout(res, 300)),
      ]);
      // 即使未等到，也继续；多数实现会随后发送
      // 创建头部帧
      const headerFrame = Http2Frame.createHeadersFrame(
        streamId,
        method,
        true,
        this.token,
        this.getAuthority()
      );
      await writer.write(headerFrame);
      // 直接按帧大小分片发送（保持与之前一致的稳定路径）
      const dataFrames = Http2Frame.createDataFrames(
        streamId,
        requestData,
        true
      );
      const frameSendTimeout =
        timeout > 0 ? timeout : DEFAULT_SEND_WINDOW_TIMEOUT;
      for (const df of dataFrames) {
        await this.sendFrameWithFlowControl(
          parser,
          streamId,
          df,
          writer,
          undefined,
          frameSendTimeout
        );
      }
      // 等待 responseData 不为空，或超时（事件驱动，不轮询）
      await new Promise<void>((resolve, reject) => {
        if (isResponseComplete || exitFlag) { resolve(); return; }
        const t = setTimeout(() => {
          notifyResponseComplete = null;
          reject(new Error("gRPC response timeout"));
        }, timeout);
        notifyResponseComplete = () => {
          clearTimeout(t);
          notifyResponseComplete = null;
          resolve();
        };
      });
      try {
        await writer.flush(timeout);
      } catch { /* ignore flush errors */ }
      await writer.end();
    } catch (err) {
      console.error("unaryCall error:", err);
      throw err;
    } finally {
      // 必须先 abort writer（立即强制停止 pushable + stream），再 close stream。
      // 若顺序颠倒：stream.close() 会等待服务端半关闭确认，网络异常时永久挂住，
      // 导致 writer.abort() 永远不执行 → watchdog 定时器 / pushable 泄漏。
      // writer.abort() 内部幂等，成功路径下 writer.end() 已调用 cleanup()，安全。
      writerRef?.abort('unaryCall cleanup');
      if (stream) {
        try {
          await stream.close();
        } catch {
          // 流已被 abort，close() 会立即抛出，忽略即可。
        }
      }
      if (streamSlotAcquired && state) {
        state.activeStreams = Math.max(0, state.activeStreams - 1);
        this.notifyStreamSlotAvailable(state);
      }
    }
    if (exitFlag) {
      throw new Error(errMsg);
    }
    if (!responseData) {
      responseData = new Uint8Array();
    }
    return responseData;
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
    let timeoutHandle: ReturnType<typeof setTimeout> | undefined;
    let stream: Stream | null = null;
    // 保存外部 abort 监听器引用，以便操作结束后移除，防止内存泄漏
    let contextAbortHandler: (() => void) | undefined;

    const profile: TransportProfile =
      options?.transportProfile ?? this.getDefaultTransportProfile(mode);
    const useFlowControl = profile === "flow-control";

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
          console.error("Error closing stream on cancel:", err);
        }
      }
    };

    // 如果提供了外部信号，监听它
    if (context?.signal) {
      // 如果外部信号已经触发中止，立即返回——避免启动 IIFE 后在 catch 中再次调用 onErrorCallback
      if (context.signal.aborted) {
        if (onErrorCallback) {
          onErrorCallback(new Error("Operation aborted by context"));
        }
        return cancelOperation;
      }

      // 监听外部的abort事件（保存引用以便后续移除，防止内存泄漏）
      contextAbortHandler = () => { cancelOperation(); };
      context.signal.addEventListener("abort", contextAbortHandler);
    }

    // 超时Promise
    const timeoutPromise = new Promise<never>((_, reject) => {
      timeoutHandle = setTimeout(() => {
        reject(new Error("Operation timed out"));
        cancelOperation();
      }, timeout);
    });

    // 主操作Promise
    const operationPromise = (async () => {
      /**
       * 统一错误报告：确保 onErrorCallback 只被调用一次，
       * 并同时中止操作，防止后续再触发 onEndCallback。
       * 适用于 onGoaway / onHeaders / processStream.catch / onData 等各个错误路径。
       */
      let errorCallbackFired = false;
      const reportError = (err: unknown) => {
        if (errorCallbackFired) return;
        errorCallbackFired = true;
        internalController.abort();
        if (onErrorCallback) onErrorCallback(err);
      };

      /** 分段列表缓冲，避免每次 payload 到达时 O(n) 全量拷贝 */
      let msgChunks: Uint8Array[] = [];
      let msgTotalLen = 0;
      let expectedMessageLength = -1; // 当前消息的期望长度
      /** 将分段列表合并为单一 Uint8Array（仅在需要时调用） */
      const flattenMsgBuffer = (): Uint8Array => {
        if (msgChunks.length === 0) return new Uint8Array(0);
        if (msgChunks.length === 1) return msgChunks[0];
        const out = new Uint8Array(msgTotalLen);
        let off = 0;
        for (const c of msgChunks) { out.set(c, off); off += c.length; }
        return out;
      };
      const hpack = new HPACK();
      let connection: Connection | null = null;
      let connectionKey: string | null = null;
      let state: ConnectionState | null = null;
      let streamSlotAcquired = false;
      // 提升 writer 作用域到 finally 可访问，确保 unary/server-streaming 模式下也能清理资源
      let writer: StreamWriter | null = null;

      try {
        // 检查是否已经中止
        if (internalController.signal.aborted) {
          throw new Error("Operation aborted");
        }

        // 如开启 freshConnection，则在拨号前尝试断开现有连接，确保本次使用全新连接（注意：会影响与该节点的其他并发）
        if (options?.freshConnection) {
          try {
            this.connectionPool.delete(this.peerAddr.toString());
            await this.node.hangUp(this.peerAddr);
            console.warn(
              "[Call] hangUp existing connection before dialing due to freshConnection=true"
            );
          } catch (err) {
            console.warn(
              "[Call] hangUp failed or not supported, proceeding to dial",
              err
            );
          }
        }

        connection = await this.acquireConnection(
          Boolean(options?.freshConnection)
        );
        connectionKey = this.peerAddr.toString();
        state = this.getConnectionState(connection as object);
        if (state) {
          try {
            await this.waitForStreamSlot(
              state,
              internalController.signal,
              timeout
            );
            state.activeStreams += 1;
            streamSlotAcquired = true;
          } catch (err) {
            console.warn("[Call] waiting for stream slot failed:", err);
            throw err;
          }
        }
        stream = await connection.newStream(this.protocol, {
          maxOutboundStreams: 50,
          signal: AbortSignal.timeout(10000),
          negotiateFully: false,
        });
        const streamManager = this.getStreamManagerFor(connection as object);
        const streamId = await streamManager.getNextAppLevelStreamId();
        writer = new StreamWriter(stream, {
          bufferSize: 16 * 1024 * 1024,
        });
        try {
          writer.addEventListener("backpressure", (e: CustomEvent) => {
            const d = e.detail || {};
            console.warn(
              `[stream ${streamId}] backpressure current=${d.currentSize} avg=${d.averageSize} threshold=${d.threshold}`
            );
          });
          writer.addEventListener("drain", () => {
            // drain event - no action needed
          });
          writer.addEventListener("stalled", (e: CustomEvent) => {
            const d = e.detail || {};
            console.warn(
              `[stream ${streamId}] stalled queue=${d.queueSize} drained=${d.drained} since=${d.sinceMs}ms — sending PING`
            );
            try {
              const payload = new Uint8Array(8);
              crypto.getRandomValues?.(payload);
              const ping = Http2Frame.createFrame(0x6, 0x0, 0, payload);
              writer!.write(ping);
            } catch { /* ignore ping write errors */ }
          });
        } catch { /* ignore addEventListener errors */ }
        const parser = new HTTP2Parser(writer!, {
          compatibilityMode: !useFlowControl,
        });
        parser.onGoaway = (info) => {
          console.warn("[Call] GOAWAY received from server", info);
          if (connectionKey) {
            this.connectionPool.delete(connectionKey);
          }
          if (state) {
            this.rejectStreamWaiters(
              state,
              new Error("Connection received GOAWAY")
            );
          }
          // reportError 统一完成：标记已报错 + abort + 触发回调（幂等，不会重复触发）
          reportError(new Error(`GOAWAY received: code=${info.errorCode}`));
          try {
            connection?.close();
          } catch (err) {
            console.warn("Error closing connection after GOAWAY:", err);
          }
        };
        parser.onSettingsParsed = (settings) => {
          if (
            state &&
            settings.maxConcurrentStreams !== undefined &&
            settings.maxConcurrentStreams > 0
          ) {
            state.maxConcurrentStreams = settings.maxConcurrentStreams;
            this.notifyStreamSlotAvailable(state);
          }
        };
        if (useFlowControl) {
          parser.registerOutboundStream(streamId);
        }
        const sendWindowTimeout =
          timeout > 0 ? timeout : DEFAULT_SEND_WINDOW_TIMEOUT;
        const writeFrame = async (frame: Uint8Array) => {
          if (useFlowControl) {
            await this.sendFrameWithFlowControl(
              parser,
              streamId,
              frame,
              writer!,
              internalController.signal,
              sendWindowTimeout
            );
          } else {
            if (internalController.signal.aborted) {
              throw new Error("Operation aborted");
            }
            await writer!.write(frame);
          }
        };
        const writeDataFrames = async (frames: Uint8Array[]) => {
          for (const frame of frames) {
            await writeFrame(frame);
          }
        };

        // 在各个回调中检查是否已中止
        parser.onData = async (payload): Promise<void> => {
          if (internalController.signal.aborted) return;

          try {
            // 追加到分段列表，O(1)，不拷贝历史数据
            msgChunks.push(payload);
            msgTotalLen += payload.length;

            // 处理缓冲区中的完整消息
            while (msgTotalLen > 0) {
              if (internalController.signal.aborted) return;

              // 读取 gRPC 消息头（5字节）
              if (expectedMessageLength === -1 && msgTotalLen >= 5) {
                const flat = flattenMsgBuffer();
                msgChunks = [flat];
                const lengthBytes = flat.slice(1, 5);
                expectedMessageLength = new DataView(
                  lengthBytes.buffer,
                  lengthBytes.byteOffset
                ).getUint32(0, false);
              }

              // 有完整消息
              if (expectedMessageLength !== -1 && msgTotalLen >= expectedMessageLength + 5) {
                const flat = flattenMsgBuffer();
                msgChunks = [flat];
                const completeMessage = flat.slice(5, expectedMessageLength + 5);
                onDataCallback(completeMessage);
                // 移除已处理消息，保留剩余
                const remaining = flat.slice(expectedMessageLength + 5);
                msgChunks = remaining.length > 0 ? [remaining] : [];
                msgTotalLen = remaining.length;
                expectedMessageLength = -1;
              } else {
                break;
              }
            }
          } catch (error: unknown) {
            // reportError 统一报错并中止，防止 onEndCallback 在数据处理异常后仍被调用
            reportError(error);
          }
        };

        parser.onSettings = () => {
          // 检查是否已中止
          if (internalController.signal.aborted) return;

          const ackSettingFrame = Http2Frame.createSettingsAckFrame();
          writer!.write(ackSettingFrame);
        };

        parser.onHeaders = (headers) => {
          // 检查是否已中止
          if (internalController.signal.aborted) return;

          const plainHeaders = hpack.decodeHeaderFields(headers);
          if (plainHeaders.get("grpc-status") === "0") {
            // 成功状态
          } else if (plainHeaders.get("grpc-status") !== undefined) {
            const errMsg =
              plainHeaders.get("grpc-message") || "gRPC call failed";
            // reportError 统一完成：标记已报错 + abort + 触发回调（幂等，不会重复触发）
            reportError(new Error(errMsg));
          }
        };
      // 启动后台流处理
      parser.processStream(stream).catch((error: unknown) => {
        // abort() 触发的清理错误属于预期行为，不打印错误日志，不重复触发回调
        if (!internalController.signal.aborted) {
          console.error('Error in processStream:', error);
          reportError(error);
        }
      });

        // 检查是否已中止
        if (internalController.signal.aborted) {
          throw new Error("Operation aborted");
        }

        // Handshake - send HTTP/2 preface
        const preface = Http2Frame.createPreface();
        await writer.write(preface);

        // 检查是否已中止
        if (internalController.signal.aborted) {
          throw new Error("Operation aborted");
        }

        // Send Settings request
        const settingFrame = Http2Frame.createSettingsFrame();
        await writer.write(settingFrame);

        // 检查是否已中止
        if (internalController.signal.aborted) {
          throw new Error("Operation aborted");
        }

        // 等待对端 SETTINGS 或 ACK，择一即可，避免偶发握手竞态
        // 注意：未胜出的 promise 内部有超时定时器，它们最终会 reject。
        // 必须绑定 .catch(…) 消除错误，否则在 Node.js 新版本中会导致 UnhandledPromiseRejection 崩溃。
        {
          await Promise.race([
            parser.waitForPeerSettings(1000).catch(() => {}),
            parser.waitForSettingsAck().catch(() => {}),
            new Promise<void>((res) => setTimeout(res, 300)),
          ]);
          // 即使未等到，也继续；多数实现会随后发送
        }

        // 检查是否已中止
        if (internalController.signal.aborted) {
          throw new Error("Operation aborted");
        }

        // Create header frame
        const headerFrame = Http2Frame.createHeadersFrame(
          streamId,
          method,
          true,
          this.token,
          this.getAuthority()
        );
        if (mode === "unary" || mode === "server-streaming") {
          await writer.write(headerFrame);
          const dfs = Http2Frame.createDataFrames(streamId, requestData, true);
          await writeDataFrames(dfs);

          // 检查是否已中止
          if (internalController.signal.aborted) {
            throw new Error("Operation aborted");
          }
        } else if (
          (mode === "client-streaming" || mode === "bidirectional") &&
          dataSourceCallback
        ) {
          await writer.write(headerFrame);

          // 检查是否已中止
          if (internalController.signal.aborted) {
            throw new Error("Operation aborted");
          }

          if (requestData.length > 0) {
            const dfs0 = Http2Frame.createDataFrames(
              streamId,
              requestData,
              false
            );
            await writeDataFrames(dfs0);
          }

          // 动态批量处理逻辑 - 在处理过程中动态补充新数据
          const batchSize = options?.batchSize || 10;

          // 动态批处理器
          const processingQueue: {
            chunk: Uint8Array;
            resolve: (value: void | PromiseLike<void>) => void;
            reject: (reason?: unknown) => void;
          }[] = [];

          /** 事件驱动：批处理完成后唤醒 waitForQueue 等待者 */
          const batchDoneWaiters: Array<() => void> = [];

          let isProcessing = false;

          const _notifyBatchDone = () => {
            const ws = batchDoneWaiters.splice(0);
            for (const fn of ws) { try { fn(); } catch { /* ignore */ } }
          };

          const processNextBatch = async () => {
            if (isProcessing || processingQueue.length === 0) return;
            isProcessing = true;

            let currentBatch: typeof processingQueue = [];

            try {
              // 收集当前批次的数据
              currentBatch = processingQueue.splice(
                0,
                Math.min(batchSize, processingQueue.length)
              );

              if (currentBatch.length > 1) {
                // 批量处理：为每个chunk创建HTTP/2帧
                // 顺序按窗口发送每个 chunk（避免跨流窗口互相干扰）
                for (const item of currentBatch) {
                  if (internalController.signal.aborted)
                    throw new Error("Operation aborted");
                  const frames = Http2Frame.createDataFrames(
                    streamId,
                    item.chunk,
                    false
                  );
                  await writeDataFrames(frames);
                }

                // 通知所有chunk处理完成
                currentBatch.forEach((item) => item.resolve());
              } else if (currentBatch.length === 1) {
                // 单个chunk处理
                const item = currentBatch[0];
                const frames1 = Http2Frame.createDataFrames(
                  streamId,
                  item.chunk,
                  false
                );
                await writeDataFrames(frames1);
                item.resolve();
              }
            } catch (error) {
              // 处理错误，通知当前批次的所有chunk处理失败
              currentBatch.forEach((item) => {
                try {
                  item.reject(error);
                } catch (err) {
                  // 忽略 reject 可能的错误（Promise 已经被处理）
                  console.warn("Error rejecting promise:", err);
                }
              });
            } finally {
              isProcessing = false;

              // 如果队列中还有数据，继续处理
              if (processingQueue.length > 0 && !internalController.signal.aborted) {
                // 直接递归调用（已是 async，自动让出事件循环）
                processNextBatch().catch((err) => { console.error("Error in processNextBatch:", err); });
              } else {
                // 队列清空，唤醒等待者
                _notifyBatchDone();
              }
            }
          };

          const addToQueue = (chunk: Uint8Array): Promise<void> => {
            return new Promise((resolve, reject) => {
              // 检查是否已经取消
              if (internalController.signal.aborted) {
                reject(new Error("Operation aborted"));
                return;
              }

              processingQueue.push({ chunk, resolve, reject });

              // 如果队列达到批量大小或没有在处理，立即开始处理
              if (processingQueue.length >= batchSize || !isProcessing) {
                processNextBatch().catch((err) => {
                  console.error("Error in processNextBatch:", err);
                });
              }
            });
          };

          // 处理数据源
          try {
            for await (const chunkOrChunks of dataSourceCallback()) {
              // 检查是否已中止
              if (internalController.signal.aborted) {
                throw new Error("Operation aborted");
              }

              // 处理单个chunk或批量chunks
              const chunksToProcess: Uint8Array[] = Array.isArray(chunkOrChunks)
                ? chunkOrChunks
                : [chunkOrChunks];

              // 将所有chunks添加到动态处理队列
              const addPromises = chunksToProcess.map((chunk) =>
                addToQueue(chunk)
              );

              // 等待当前批次的chunks被添加到队列（不等待处理完成）
              await Promise.all(addPromises);
            }
          } catch (error) {
            // 取消所有待处理的Promise
            const remainingQueue = processingQueue.splice(0);
            remainingQueue.forEach((item) => {
              try {
                item.reject(error);
              } catch (err) {
                console.warn("Error rejecting remaining promise:", err);
              }
            });
            throw error;
          }

          // 等待所有剩余的数据处理完成（事件驱动，无 10ms 轮询）
          await new Promise<void>((resolve, reject) => {
            const check = () => {
              if (internalController.signal.aborted) {
                reject(new Error("Operation aborted"));
                return;
              }
              if (processingQueue.length === 0 && !isProcessing) {
                resolve();
                return;
              }
              // processNextBatch 结束时会通知这里
              batchDoneWaiters.push(check);
            };
            check();
          });

          // 检查是否已中止
          if (internalController.signal.aborted) {
            throw new Error("Operation aborted");
          }

          // 发送纯 HTTP/2 END_STREAM 信号帧（0 字节 payload），而非带 gRPC 消息头的空消息。
          // createDataFrame 会额外附加 5 字节 gRPC 消息头 [0,0,0,0,0]，服务端会将其解析
          // 为一个长度=0 的额外 gRPC 消息，而不仅仅是流结束信号，可能导致协议混淆。
          const finalFrame = Http2Frame.createFrame(0x0, 0x01, streamId, new Uint8Array(0));
          await writeFrame(finalFrame);
          // 在结束前尽量冲刷内部队列，避免服务器看到部分数据 + context canceled
          try {
            await writer.flush(timeout);
          } catch { /* ignore flush errors */ }
          await writer.end();
        }

        // 检查是否已中止
        if (internalController.signal.aborted) {
          throw new Error("Operation aborted");
        }

        // 仅在未中止时等待并回调：
        // 1. 若已中止（如 onHeaders gRPC 错误），跳过 waitForEndOfStream(0) 避免永久阻塞
        //    （waitForEndOfStream(0) 无超时，需等到 processStream 自然结束，
        //     而 processStream 结束依赖 stream.close()，但 stream.close() 在 finally 中——形成死锁）
        // 2. 避免在 onErrorCallback 之后再调用 onEndCallback
        if (!internalController.signal.aborted) {
          await parser.waitForEndOfStream(0);
          // Yield one microtask tick so that processStream.catch (which calls
          // reportError + internalController.abort()) has a chance to run before
          // we check abort status. Without this yield, if the stream died
          // unexpectedly (network error), onEndCallback and onErrorCallback
          // could both fire because _notifyEndOfStream() is called in
          // processStream's catch block before the re-throw schedules the
          // .catch handler as a microtask.
          await Promise.resolve();
          if (!internalController.signal.aborted && onEndCallback) {
            onEndCallback();
          }
        }
      } catch (err: unknown) {
        // 如果是由于取消导致的错误，使用特定的错误消息
        if (
          internalController.signal.aborted &&
          err instanceof Error &&
          err.message === "Operation aborted"
        ) {
          // onHeaders / onGoaway / processStream 错误已通过 reportError 处理，
          // 此处仅在回调尚未触发时才报告（外部取消/超时场景）
          if (!errorCallbackFired && onErrorCallback) {
            onErrorCallback(new Error("Operation cancelled by user"));
          }
        } else if (!errorCallbackFired && onErrorCallback) {
          onErrorCallback(err);
        } else if (!errorCallbackFired) {
          if (err instanceof Error) {
            console.error("asyncCall error:", err.message);
          } else {
            console.error("asyncCall error:", err);
          }
        }
      } finally {
        clearTimeout(timeoutHandle);
        // 移除外部 abort 监听器，防止 AbortController 复用时触发迟到的 cancelOperation()
        if (contextAbortHandler && context?.signal) {
          context.signal.removeEventListener("abort", contextAbortHandler);
        }
        // 首先标记操作已结束（正常或异常），确保 processStream.catch 不会把
        // writer.abort() 产生的 'Call cleanup' 错误误判为真实错误并触发 onErrorCallback。
        // internalController.abort() 是幂等的，重复调用安全。
        internalController.abort();
        // 必须先 abort writer（立即强制停止 pushable + stream），再 close stream。
        // 若顺序颠倒：stream.close() 等待服务端半关闭确认，网络异常时永久挂住，
        // writer.abort() 永远不执行 → watchdog / pushable 泄漏。
        // abort() 内部幂等，重复调用安全。
        writer?.abort('Call cleanup');
        if (stream) {
          try {
            await stream.close();
          } catch {
            // 流已被 abort，close() 会立即抛出，忽略即可。
          }
        }
        // 如果本次强制使用了新连接，结束时尽量关闭它，避免连接泄漏
        if (options?.freshConnection) {
          try {
            // 通过 libp2p 连接管理器关闭到该 peer 的连接
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            const conns = (this.node as any).getConnections?.(this.peerAddr as any) || [];
            for (const c of conns) {
              try {
                await c.close?.();
              } catch { /* ignore close errors */ }
            }
          } catch { /* ignore connection cleanup errors */ }
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
