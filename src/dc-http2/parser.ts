import type { Frame } from "./types.js";
import { FRAME_TYPES, FRAME_FLAGS } from "./types.js";
import { Http2Frame } from "./frame.js";
import { StreamWriter } from "./stream.js";

type ParserOptions = {
  compatibilityMode?: boolean
}

const HTTP2_PREFACE = new TextEncoder().encode("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");

export class HTTP2Parser {
  /** 分段缓冲：避免每次 chunk 到达时 O(n) 全量拷贝 */
  private bufferChunks: Uint8Array[] = [];
  private bufferTotalLength = 0;
  /** 兼容旧代码读取 buffer —— 仅在必须全量访问时调用 _flattenBuffer() */
  get buffer(): Uint8Array { return this._flattenBuffer(); }
  set buffer(v: Uint8Array) { this.bufferChunks = v.length ? [v] : []; this.bufferTotalLength = v.length; }
  settingsAckReceived: boolean;
  peerSettingsReceived: boolean;
  connectionWindowSize: number;
  streams: Map<number, unknown>;
  defaultStreamWindowSize: number;
  // 发送方向（对端的接收窗口）跟踪
  sendConnWindow: number;
  sendStreamWindows: Map<number, number>;
  peerInitialStreamWindow: number;
  private sendWindowWaiters: Array<{ resolve: () => void; reject: (e: Error) => void; cleanup?: () => void }>;
  // 事件驱动等待器
  private settingsAckWaiters: Array<{ resolve: () => void; reject: (e: Error) => void }>;
  private peerSettingsWaiters: Array<{ resolve: () => void; reject: (e: Error) => void }>;
  private endOfStreamWaiters: Array<{ resolve: () => void; reject: (e: Error) => void }>;
  onSettings?: (frameHeader: Frame) => void;
  onData?: (payload: Uint8Array, frameHeader: Frame) => void;
  onEnd?: () => void;
  onHeaders?: (headers: Uint8Array, frameHeader: Frame) => void;
  onGoaway?: (info: { lastStreamId?: number; errorCode?: number }) => void;
  onSettingsParsed?: (settings: { maxConcurrentStreams?: number; initialWindowSize?: number }) => void;
  endFlag: boolean;
  writer: StreamWriter;
  private readonly compatibilityMode: boolean;

  constructor(writer: StreamWriter, options?: ParserOptions) {
    this.bufferChunks = [];
    this.bufferTotalLength = 0;
    this.settingsAckReceived = false;
    this.peerSettingsReceived = false;
    // 初始化连接级别的流控制窗口大小（默认值：65,535）
    this.connectionWindowSize = 4 << 20;
    // 存储流的Map
    this.streams = new Map();
    // 默认的流级别初始窗口大小
    this.defaultStreamWindowSize = 4 << 20;
    // 发送方向窗口（对端接收窗口）默认均为 65535
    this.sendConnWindow = 65535;
    this.sendStreamWindows = new Map();
    this.peerInitialStreamWindow = 65535;
    this.sendWindowWaiters = [];
    this.settingsAckWaiters = [];
    this.peerSettingsWaiters = [];
    this.endOfStreamWaiters = [];
    // 结束标志
    this.endFlag = false;

    this.writer = writer;
    this.compatibilityMode = options?.compatibilityMode ?? false;
  }

  /** 将所有分段合并为一个连续 Uint8Array（仅在必要时调用）*/
  private _flattenBuffer(): Uint8Array {
    if (this.bufferChunks.length === 0) return new Uint8Array(0);
    if (this.bufferChunks.length === 1) return this.bufferChunks[0];
    const out = new Uint8Array(this.bufferTotalLength);
    let off = 0;
    for (const c of this.bufferChunks) { out.set(c, off); off += c.length; }
    return out;
  }

  /** 唤醒所有发送窗口等待者 */
  private _wakeWindowWaiters() {
    const ws = this.sendWindowWaiters.splice(0);
    for (const w of ws) { try { w.resolve(); } catch { /* ignore */ } }
  }

  // 持续处理流数据
  async processStream(stream: AsyncIterable<unknown>) {
    try {
      // libp2p v3: Stream 实现了 AsyncIterable
      for await (const chunk of stream) {
        this._processChunk(chunk);
      }

      // Stream 结束后的清理工作
      if (!this.compatibilityMode && !this.endFlag) {
        try { this.onEnd?.(); } catch (err) { console.error("Error during onEnd callback:", err); }
      }
      // 无论何种模式，stream 结束时都通知 waitForEndOfStream 等待者，
      // 防止 compatibilityMode=true（server-streaming）时 waitForEndOfStream(0) 永久挂死
      if (!this.endFlag) {
        this._notifyEndOfStream();
      }
    } catch (error) {
      // 确保 waitForEndOfStream 等待者得到通知，防止 operationPromise 后台挂死
      if (!this.endFlag) {
        this._notifyEndOfStream();
      }
      // 仅过滤我们自己主动触发的清理错误（reason 固定为 'Call cleanup' / 'unaryCall cleanup'）。
      // 不使用 /aborted/i，因为 libp2p / 浏览器网络层可能抛出含 "aborted" 字样的真实错误
      // （如 "AbortError: The operation was aborted", "Connection aborted by peer"），
      // 若误匹配会导致 reportError 不被调用，onErrorCallback 丢失，调用方误认为成功。
      const errMsg = error instanceof Error ? error.message : String(error);
      if (/cleanup/i.test(errMsg)) {
        // 预期的主动清理，无需 re-throw，.catch in index.ts 不需要处理它
        return;
      }
      // The caller owns error reporting. Logging here and then rethrowing
      // produces duplicate console errors, including expected local aborts.
      throw error;
    }
  }

  // 处理单个数据块 — 分段列表追加，避免每次 O(n) 全量拷贝
  private _processChunk(chunk: unknown): void {
    // Uint8Array and Uint8ArrayList both expose subarray(). Validate the
    // transport value at runtime instead of coupling to one libp2p type copy.
    const chunkWithSubarray = chunk as {
      subarray?: (begin?: number, end?: number) => Uint8Array
    } | null;
    if (typeof chunkWithSubarray?.subarray !== "function") {
      throw new TypeError("Received an invalid libp2p stream chunk");
    }
    const newData = chunkWithSubarray.subarray();

    // 追加到分段列表，O(1)，不拷贝历史数据
    if (newData.length > 0) {
      this.bufferChunks.push(newData);
      this.bufferTotalLength += newData.length;
    }

    // 将所有分段合并为一块后处理帧（只合并一次，后续 slice 替换）
    // 仅在确实有完整帧时才触发合并，碎片仅 push 不合并
    if (this.bufferTotalLength < 9) return;

    // 合并一次
    const flat = this._flattenBuffer();
    this.bufferChunks = [flat];
    // bufferTotalLength 保持不变

    // 持续处理所有完整的帧
    let readOffset = 0;
    while (flat.length - readOffset >= 9) {
      // 判断是否有HTTP/2前导
      if (flat.length - readOffset >= 24 && this.isHttp2Preface(flat.subarray(readOffset))) {
        readOffset += 24;
        // 发送SETTINGS帧
        const settingFrame = Http2Frame.createSettingsFrame();
        this.writer.write(settingFrame);
        continue;
      }

      const frameHeader = this._parseFrameHeader(flat.subarray(readOffset));
      const totalFrameLength = 9 + frameHeader.length;

      // 检查是否有完整的帧
      if (flat.length - readOffset < totalFrameLength) {
        break;
      }
      // 获取完整帧数据（subarray 视图，零拷贝）
      const frameData = flat.subarray(readOffset, readOffset + totalFrameLength);

      // 处理不同类型的帧
      this._handleFrame(frameHeader, frameData).catch((err) => {
        console.error("Error handling frame:", err);
      });

      readOffset += totalFrameLength;
    }

    // 保留未消费的尾部字节（slice 一次，后续仍分段追加）
    if (readOffset > 0) {
      if (readOffset >= flat.length) {
        this.bufferChunks = [];
        this.bufferTotalLength = 0;
      } else {
        const remaining = flat.slice(readOffset);
        this.bufferChunks = [remaining];
        this.bufferTotalLength = remaining.length;
      }
    }
  }

  private isHttp2Preface(buffer: Uint8Array): boolean {
    if (buffer.length < HTTP2_PREFACE.length) return false;
    for (let i = 0; i < HTTP2_PREFACE.length; i++) {
      if (buffer[i] !== HTTP2_PREFACE[i]) return false;
    }
    return true;
  }

  // 等待SETTINGS ACK — 事件驱动，无轮询
  waitForSettingsAck(): Promise<void> {
    return new Promise((resolve, reject) => {
      if (this.settingsAckReceived) { resolve(); return; }
      const waiter = { resolve, reject };
      this.settingsAckWaiters.push(waiter);
      const timeout = setTimeout(() => {
        const idx = this.settingsAckWaiters.indexOf(waiter);
        if (idx >= 0) this.settingsAckWaiters.splice(idx, 1);
        reject(new Error("Settings ACK timeout"));
      }, 30000);
      // 覆盖 resolve 以便超时前自动清理定时器
      waiter.resolve = () => { clearTimeout(timeout); resolve(); };
      waiter.reject  = (e: Error) => { clearTimeout(timeout); reject(e); };
    });
  }

  /** 内部调用：SETTINGS ACK 收到时唤醒所有等待者 */
  private _notifySettingsAck() {
    this.settingsAckReceived = true;
    const ws = this.settingsAckWaiters.splice(0);
    for (const w of ws) { try { w.resolve(); } catch { /* ignore */ } }
  }

  // 等待接收来自对端的 SETTINGS（非 ACK）— 事件驱动，无轮询
  waitForPeerSettings(timeoutMs: number = 30000): Promise<void> {
    return new Promise((resolve, reject) => {
      if (this.peerSettingsReceived) { resolve(); return; }
      const waiter = { resolve, reject };
      this.peerSettingsWaiters.push(waiter);
      const timeout = setTimeout(() => {
        const idx = this.peerSettingsWaiters.indexOf(waiter);
        if (idx >= 0) this.peerSettingsWaiters.splice(idx, 1);
        reject(new Error("Peer SETTINGS timeout"));
      }, timeoutMs);
      waiter.resolve = () => { clearTimeout(timeout); resolve(); };
      waiter.reject  = (e: Error) => { clearTimeout(timeout); reject(e); };
    });
  }

  /** 内部调用：收到对端 SETTINGS（非 ACK）时唤醒等待者 */
  private _notifyPeerSettings() {
    this.peerSettingsReceived = true;
    const ws = this.peerSettingsWaiters.splice(0);
    for (const w of ws) { try { w.resolve(); } catch { /* ignore */ } }
  }

  // 注册我们要发送数据的出站流（用于初始化该流的对端窗口）
  registerOutboundStream(streamId: number) {
    if (!this.sendStreamWindows.has(streamId)) {
      this.sendStreamWindows.set(streamId, this.peerInitialStreamWindow);
    }
  }

  // 获取发送窗口
  getSendWindows(streamId: number) {
    const s = this.sendStreamWindows.get(streamId) ?? 0;
    return { conn: this.sendConnWindow, stream: s };
  }

  // 消耗发送窗口（成功写入 DATA 之后调用）
  consumeSendWindow(streamId: number, bytes: number) {
    this.sendConnWindow = Math.max(0, this.sendConnWindow - bytes);
    const cur = this.sendStreamWindows.get(streamId) ?? 0;
    this.sendStreamWindows.set(streamId, Math.max(0, cur - bytes));
  }

  // 非标准兜底：在对端未及时发送 WINDOW_UPDATE 时，手动回填窗口额度以避免阻塞
  unsafeForceExtendSendWindow(streamId: number, bytes: number) {
    if (this.compatibilityMode) return;
    if (bytes <= 0) return;
    this.sendConnWindow = Math.min(0x7fffffff, this.sendConnWindow + bytes);
    const cur = this.sendStreamWindows.get(streamId) ?? 0;
    this.sendStreamWindows.set(streamId, Math.min(0x7fffffff, cur + bytes));
    // 窗口增大，唤醒等待者
    this._wakeWindowWaiters();
  }

  // 等待可用发送窗口 — 事件驱动，WINDOW_UPDATE/SETTINGS 收到时直接唤醒
  waitForSendWindow(streamId: number, minBytes: number = 1, timeoutMs: number = 30000): Promise<void> {
    const { conn, stream } = this.getSendWindows(streamId);
    if (conn >= minBytes && stream >= minBytes) return Promise.resolve();

    return new Promise((resolve, reject) => {
      let settled = false;
      const timeout = timeoutMs > 0
        ? setTimeout(() => {
            if (settled) return;
            settled = true;
            const idx = this.sendWindowWaiters.findIndex(w => w.resolve === resolveWrap);
            if (idx >= 0) this.sendWindowWaiters.splice(idx, 1);
            reject(new Error('Send window wait timeout'));
          }, timeoutMs)
        : undefined;

      const resolveWrap = () => {
        if (settled) return;
        const { conn: c2, stream: s2 } = this.getSendWindows(streamId);
        if (c2 >= minBytes && s2 >= minBytes) {
          settled = true;
          if (timeout) clearTimeout(timeout);
          resolve();
        } else {
          // 窗口仍不够，重新入队等待下一次更新
          this.sendWindowWaiters.push({ resolve: resolveWrap, reject: rejectWrap });
        }
      };
      const rejectWrap = (e: Error) => {
        if (settled) return;
        settled = true;
        if (timeout) clearTimeout(timeout);
        reject(e);
      };
      this.sendWindowWaiters.push({ resolve: resolveWrap, reject: rejectWrap });
    });
  }

  // 处理单个帧
  async _handleFrame(frameHeader: Frame, frameData: Uint8Array) {
    switch (frameHeader.type) {
      case FRAME_TYPES.SETTINGS:
        if ((frameHeader.flags & FRAME_FLAGS.ACK) === FRAME_FLAGS.ACK) {
          this._notifySettingsAck();
        } else {
          //接收到Setting请求,进行解析
          const settingsPayload = frameData.slice(9);
          const settings: Record<number, number> = {};
          let initialWindowDelta = 0;
          let maxConcurrentStreams: number | undefined;
          for (let i = 0; i < settingsPayload.length; i += 6) {
            // 正确解析：2字节ID + 4字节值
            const id = (settingsPayload[i] << 8) | settingsPayload[i + 1];
            // >>> 0 将结果转为无符号 32 位整数，防止高位为 1 时（如 0xffffffff）
            // 被 JS 按有符号解读为负数，导致 maxConcurrentStreams 等字段为负值
            const value = (
              (settingsPayload[i + 2] << 24) |
              (settingsPayload[i + 3] << 16) |
              (settingsPayload[i + 4] << 8) |
              settingsPayload[i + 5]
            ) >>> 0;

            settings[id] = value;
            if (id === 4) {
              // SETTINGS_INITIAL_WINDOW_SIZE
              this.defaultStreamWindowSize = value; // 我方接收窗口（入站）
              initialWindowDelta = value - this.peerInitialStreamWindow;
              this.peerInitialStreamWindow = value; // 对端接收窗口（我方发送）
            } else if (id === 3) {
              // SETTINGS_MAX_CONCURRENT_STREAMS
              maxConcurrentStreams = value;
            }
          }

          if (!this.compatibilityMode && initialWindowDelta !== 0) {
            Array.from(this.sendStreamWindows.entries()).forEach(([sid, current]) => {
              const updated = Math.max(0, current + initialWindowDelta);
              this.sendStreamWindows.set(sid, updated);
            });
          }

          try {
            if (this.onSettingsParsed && (maxConcurrentStreams !== undefined || initialWindowDelta !== 0)) {
              const payload: { maxConcurrentStreams?: number; initialWindowSize?: number } = {};
              if (maxConcurrentStreams !== undefined) {
                payload.maxConcurrentStreams = maxConcurrentStreams;
              }
              if (initialWindowDelta !== 0) {
                payload.initialWindowSize = this.peerInitialStreamWindow;
              }
              this.onSettingsParsed(payload);
            }
          } catch (err) {
            console.error('Error handling parsed SETTINGS callback:', err);
          }

          //发送ACK
          if (this.onSettings) {
            this.onSettings(frameHeader);
          }
          // 标记已收到对端 SETTINGS 并唤醒等待者
          this._notifyPeerSettings();
          // 唤醒发送窗口等待者（以防部分实现通过 SETTINGS 改变有效窗口）
          this._wakeWindowWaiters();
        }
        break;

      case FRAME_TYPES.DATA: {
        // 处理数据帧
        if (this.onData) {
          this.onData(frameData.slice(9), frameHeader); // 跳过帧头
        }
        // 更新流窗口和连接窗口
        // 仅在帧有实际数据时才发送 WINDOW_UPDATE：
        // RFC 7540 §6.9.1 明确禁止 increment=0 的 WINDOW_UPDATE，
        // 服务端必须以 PROTOCOL_ERROR 响应，会导致连接被强制关闭。
        // 空 DATA 帧（如纯 END_STREAM 帧）length=0，不需要归还窗口。
        const dataLength = frameHeader.length ?? 0;
        if (dataLength > 0) {
          try {
            // 更新流级别的窗口
            if (frameHeader.streamId !== 0) {
              const streamWindowUpdate = Http2Frame.createWindowUpdateFrame(
                frameHeader.streamId,
                dataLength
              );
              this.writer.write(streamWindowUpdate);
            }

            // 更新连接级别的窗口
            const connWindowUpdate = Http2Frame.createWindowUpdateFrame(
              0,
              dataLength
            );
            this.writer.write(connWindowUpdate);
          } catch (err) {
            console.error("[HTTP2] Error sending window update:", err);
          }
        }
        //判断是否是最后一个帧
        if (
          (frameHeader.flags & FRAME_FLAGS.END_STREAM) ===
          FRAME_FLAGS.END_STREAM
        ) {
          this.onEnd?.();
          this._notifyEndOfStream();
          return;
        }
        break;
      }
      case FRAME_TYPES.HEADERS:
        // 处理头部帧
        if (this.onHeaders) {
          this.onHeaders(frameData.slice(9), frameHeader);
        }
        //判断是否是最后一个帧
        if (
          (frameHeader.flags & FRAME_FLAGS.END_STREAM) ===
          FRAME_FLAGS.END_STREAM
        ) {
          this.onEnd?.();
          this._notifyEndOfStream();
          return;
        }
        break;
      case FRAME_TYPES.WINDOW_UPDATE:
        // 处理窗口更新帧（同时更新接收侧诊断计数器和发送侧流控窗口，只解析一次）
        try {
          const result = this.handleWindowUpdateFrame(frameHeader, frameData);
          // 更新发送方向窗口（对端的接收窗口）
          if (frameHeader.streamId === 0) {
            this.sendConnWindow += result.windowSizeIncrement;
          } else {
            const cur = this.sendStreamWindows.get(frameHeader.streamId) ?? this.peerInitialStreamWindow;
            this.sendStreamWindows.set(frameHeader.streamId, cur + result.windowSizeIncrement);
          }
          this._wakeWindowWaiters();
        } catch { /* ignore WINDOW_UPDATE parse errors (e.g. increment=0 is RFC PROTOCOL_ERROR) */ }
        break;
      case FRAME_TYPES.PING:
        // 处理PING帧
        this._handlePingFrame(frameHeader, frameData);
        break;
      case FRAME_TYPES.GOAWAY: {
        let info: { lastStreamId?: number; errorCode?: number } | undefined;
        try {
          const body = frameData.subarray(9);
          if (body.length >= 8) {
            const view = new DataView(body.buffer, body.byteOffset, body.byteLength);
            const lastStreamId = view.getUint32(0, false) & 0x7fffffff;
            const errorCode = view.getUint32(4, false);
            info = { lastStreamId, errorCode };
            console.warn('[HTTP2] GOAWAY received', info);
          } else {
            console.warn('[HTTP2] GOAWAY received');
            info = {};
          }
        } catch { /* ignore GOAWAY parse errors */ }
        try {
          this.onGoaway?.(info ?? {});
        } catch (err) {
          console.error('Error during GOAWAY callback:', err);
        }
        try {
          this.onEnd?.();
        } catch (err) {
          console.error('Error during GOAWAY onEnd callback:', err);
        }
        this._notifyEndOfStream();
        break;
      }

      // case FRAME_TYPES.PUSH_PROMISE:
      //     // 处理服务器推送承诺帧
      //     this.handlePushPromiseFrame(frameHeader, frameData);
      //     break;
      case FRAME_TYPES.RST_STREAM:
        this.onEnd?.();
        this._notifyEndOfStream();
        break;
      default:
        console.debug("Unknown frame type:", frameHeader.type);
    }
  }

  _parseFrameHeader(buffer: Uint8Array) {
    const length = (buffer[0] << 16) | (buffer[1] << 8) | buffer[2];
    const type = buffer[3];
    const flags = buffer[4];
    // RFC 7540 §4.1: most significant bit is reserved and MUST be ignored on receipt
    const streamId =
      ((buffer[5] << 24) | (buffer[6] << 16) | (buffer[7] << 8) | buffer[8]) & 0x7fffffff;

    return {
      length,
      type,
      flags,
      streamId,
      payload: buffer.slice(0, 9),
    };
  }
  // 解析PING帧
  _handlePingFrame(frameHeader: Frame, frameData: Uint8Array) {
    // PING帧的payload固定为8字节
    if (frameHeader.length !== 8) {
      throw new Error("PING frame must have a length of 8 bytes");
    }
    if (frameHeader.flags & FRAME_FLAGS.ACK) {
      // 是ACK，不需要回应
      return;
    }
    // 反馈PONG帧
    const pongFrame = Http2Frame.createPongFrame(frameData.slice(9));
    try {
      this.writer.write(pongFrame);
    } catch (error) {
      console.error("Error sending PONG frame:", error);
      throw error;
    }
  }

  // 等待流结束 — 事件驱动，onEnd 触发时直接唤醒，无 setInterval 轮询
  waitForEndOfStream(waitTime: number): Promise<void> {
    return new Promise((resolve, reject) => {
      if (this.endFlag) { resolve(); return; }

      const waiter = { resolve, reject };
      this.endOfStreamWaiters.push(waiter);

      const timeout = waitTime > 0
        ? setTimeout(() => {
            const idx = this.endOfStreamWaiters.indexOf(waiter);
            if (idx >= 0) this.endOfStreamWaiters.splice(idx, 1);
            reject(new Error("End of stream timeout"));
          }, waitTime)
        : null;

      waiter.resolve = () => { if (timeout) clearTimeout(timeout); resolve(); };
      waiter.reject  = (e: Error) => { if (timeout) clearTimeout(timeout); reject(e); };
    });
  }

  /** 内部调用：流结束时唤醒所有 waitForEndOfStream 等待者 */
  private _notifyEndOfStream() {
    this.endFlag = true;
    const ws = this.endOfStreamWaiters.splice(0);
    for (const w of ws) { try { w.resolve(); } catch { /* ignore */ } }
  }

  // 解析 WINDOW_UPDATE 帧
  parseWindowUpdateFrame(frameBuffer: Uint8Array, frameHeader: Frame) {
    // WINDOW_UPDATE帧的payload固定为4字节
    if (frameHeader.length !== 4) {
      throw new Error("WINDOW_UPDATE frame must have a length of 4 bytes");
    }

    // 确保frameBuffer是Uint8Array类型
    //  const buffer = new Uint8Array(frameBuffer);
    const buffer = new Uint8Array(frameBuffer.slice(9));

    // 读取window size increment (4字节，大端序)
    // 手动计算32位无符号整数，确保最高位为0
    const windowSizeIncrement =
      ((buffer[0] & 0x7f) << 24) |
      (buffer[1] << 16) |
      (buffer[2] << 8) |
      buffer[3];

    // 验证window size increment
    if (windowSizeIncrement === 0) {
      throw new Error("WINDOW_UPDATE increment must not be zero");
    }

    return {
      windowSizeIncrement: windowSizeIncrement,
    };
  }

  // 处理 WINDOW_UPDATE 帧
  handleWindowUpdateFrame(
    frameHeader: Frame,
    payload: Uint8Array
  ) {
    try {
      const windowUpdate = this.parseWindowUpdateFrame(payload, frameHeader);

      this.connectionWindowSize += windowUpdate.windowSizeIncrement;

      return windowUpdate;
    } catch (error) {
      // 处理错误情况
      console.error("Error handling WINDOW_UPDATE frame:", error);
      throw error;
    }
  }
}
