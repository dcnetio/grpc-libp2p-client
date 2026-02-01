import type { Frame } from "./types.js";
import type { Stream } from "@libp2p/interface";
import { FRAME_TYPES, FRAME_FLAGS } from "./types.js";
import { Http2Frame } from "./frame.js";
import { StreamWriter } from "./stream.js";

type ParserOptions = {
  compatibilityMode?: boolean
}

export class HTTP2Parser {
  buffer: Uint8Array;
  settingsAckReceived: boolean;
  peerSettingsReceived: boolean;
  connectionWindowSize: number;
  streams: Map<number, any>;
  defaultStreamWindowSize: number;
  // 发送方向（对端的接收窗口）跟踪
  sendConnWindow: number;
  sendStreamWindows: Map<number, number>;
  peerInitialStreamWindow: number;
  private sendWindowWaiters: Array<() => void>;
  onSettings?: (frameHeader: any) => void;
  onData?: (payload: Uint8Array, frameHeader: any) => void;
  onEnd?: () => void;
  onHeaders?: (headers: Uint8Array, frameHeader: any) => void;
  onGoaway?: (info: { lastStreamId?: number; errorCode?: number }) => void;
  onSettingsParsed?: (settings: { maxConcurrentStreams?: number; initialWindowSize?: number }) => void;
  endFlag: boolean;
  writer: StreamWriter;
  private readonly compatibilityMode: boolean;

  constructor(writer: StreamWriter, options?: ParserOptions) {
    this.buffer = new Uint8Array(0);
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
    // 结束标志
    this.endFlag = false;

    this.writer = writer;
    this.compatibilityMode = options?.compatibilityMode ?? false;
  }

  // 持续处理流数据
  async processStream(stream: Stream) {
    try {
      // libp2p v3: Stream 实现了 AsyncIterable
      for await (const chunk of stream) {
        this._processChunk(chunk);
      }
      
      // Stream 结束后的清理工作
      if (!this.compatibilityMode && !this.endFlag) {
        this.endFlag = true;
        try {
          this.onEnd?.();
        } catch (err) {
          console.error("Error during onEnd callback:", err);
        }
      }
    } catch (error) {
      console.error("Error processing stream:", error);
      throw error;
    }
  }

  // 处理单个数据块
  private _processChunk(chunk: any): void {
    // chunk 是 Uint8ArrayList 或 Uint8Array
    const newData = chunk.subarray ? chunk.subarray() : new Uint8Array(chunk);

    // 累积数据到buffer
    const newBuffer = new Uint8Array(this.buffer.length + newData.length);
    newBuffer.set(this.buffer);
    newBuffer.set(newData, this.buffer.length);
    this.buffer = newBuffer;
    
    // 持续处理所有完整的帧
    while (this.buffer.length >= 9) {
      // 判断是否有HTTP/2前导
      if (this.buffer.length >= 24 && this.isHttp2Preface(this.buffer)) {
        this.buffer = this.buffer.slice(24);
        // 发送SETTINGS帧
        const settingFrame = Http2Frame.createSettingsFrame();
        this.writer.write(settingFrame as any);
        break;
      }
      const frameHeader = this._parseFrameHeader(this.buffer);
      const totalFrameLength = 9 + frameHeader.length;

      // 检查是否有完整的帧
      if (this.buffer.length < totalFrameLength) {
        break;
      }
      // 获取完整帧数据
      const frameData = this.buffer.slice(0, totalFrameLength);

      // 处理不同类型的帧
      this._handleFrame(frameHeader, frameData).catch((err) => {
        console.error("Error handling frame:", err);
      });

      // 移除已处理的帧
      this.buffer = this.buffer.slice(totalFrameLength);
    }
  }

  private isHttp2Preface(buffer: Uint8Array): boolean {
    const PREFACE = new TextEncoder().encode(
      "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
    );
    if (buffer.length < PREFACE.length) return false;
    for (let i = 0; i < PREFACE.length; i++) {
      if (buffer[i] !== PREFACE[i]) return false;
    }
    return true;
  }

  // 移除之前的 for await 循环代码
  private _oldProcessStream_removed() {
    // 这个方法已被上面的事件驱动实现替代
  }

  // 等待SETTINGS ACK
  waitForSettingsAck(): Promise<void> {
    return new Promise((resolve, reject) => {
      if (this.settingsAckReceived) {
        resolve();
        return;
      }
      const interval = setInterval(() => {
        if (this.settingsAckReceived) {
          clearInterval(interval);
          clearTimeout(timeout);
          resolve();
        }
      }, 100);

      const timeout = setTimeout(() => {
        clearInterval(interval);
        reject(new Error("Settings ACK timeout"));
      }, 30000);
    });
  }

  // 等待接收来自对端的 SETTINGS（非 ACK）
  waitForPeerSettings(timeoutMs: number = 30000): Promise<void> {
    return new Promise((resolve, reject) => {
      if (this.peerSettingsReceived) {
        resolve();
        return;
      }
      const interval = setInterval(() => {
        if (this.peerSettingsReceived) {
          clearInterval(interval);
          clearTimeout(timeout);
          resolve();
        }
      }, 100);

      const timeout = setTimeout(() => {
        clearInterval(interval);
        reject(new Error("Peer SETTINGS timeout"));
      }, timeoutMs);
    });
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
  }

  // 等待可用发送窗口（两个窗口都需要 >0）
  async waitForSendWindow(streamId: number, minBytes: number = 1, timeoutMs: number = 30000): Promise<void> {
    const start = Date.now();
    return new Promise((resolve, reject) => {
      let interval: NodeJS.Timeout | null = null;
      let settled = false;
      const check = () => {
        const { conn, stream } = this.getSendWindows(streamId);
        if (conn >= minBytes && stream >= minBytes) {
          if (!settled) {
            settled = true;
            if (interval) {
              clearInterval(interval);
              interval = null;
            }
            resolve();
          }
          return true;
        }
        if (Date.now() - start > timeoutMs) {
          if (!settled) {
            settled = true;
            if (interval) {
              clearInterval(interval);
              interval = null;
            }
            reject(new Error('Send window wait timeout'));
          }
          return true;
        }
        return false;
      };
      if (check()) return;
      const tick = () => {
        if (!check()) {
          // 继续等待
        }
      };
      const wake = () => { tick(); };
      // 简单的等待模型：依赖 WINDOW_UPDATE 到达时调用 wake
      this.sendWindowWaiters.push(wake);
      // 同时做一个轻微的轮询，防止错过唤醒
      interval = setInterval(() => {
        if (check() && interval) {
          clearInterval(interval);
          interval = null;
        }
      }, 50);
    });
  }

  // 处理单个帧
  async _handleFrame(frameHeader: Frame, frameData: Uint8Array) {
    switch (frameHeader.type) {
      case FRAME_TYPES.SETTINGS:
        if ((frameHeader.flags & FRAME_FLAGS.ACK) === FRAME_FLAGS.ACK) {
          this.settingsAckReceived = true;
        } else {
          //接收到Setting请求,进行解析
          const settingsPayload = frameData.slice(9);
          const settings: Record<number, number> = {};
          let initialWindowDelta = 0;
          let maxConcurrentStreams: number | undefined;
          for (let i = 0; i < settingsPayload.length; i += 6) {
            // 正确解析：2字节ID + 4字节值
            const id = (settingsPayload[i] << 8) | settingsPayload[i + 1];
            const value =
              (settingsPayload[i + 2] << 24) |
              (settingsPayload[i + 3] << 16) |
              (settingsPayload[i + 4] << 8) |
              settingsPayload[i + 5];

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
          // 标记已收到对端 SETTINGS
          this.peerSettingsReceived = true;
          // 唤醒等待窗口（以防部分实现通过 SETTINGS 改变有效窗口）
          const waiters = this.sendWindowWaiters.splice(0);
          waiters.forEach(fn => { try { fn(); } catch {} });
        }
        break;

      case FRAME_TYPES.DATA:
        // 处理数据帧
        if (this.onData) {
          this.onData(frameData.slice(9), frameHeader); // 跳过帧头
        }
        // 更新流窗口和连接窗口
        try {
          // 更新流级别的窗口
          if (frameHeader.streamId !== 0) {
            const streamWindowUpdate = Http2Frame.createWindowUpdateFrame(
              frameHeader.streamId,
              frameHeader.length
            );
            this.writer.write(streamWindowUpdate as any);
          }

          // 更新连接级别的窗口
          const connWindowUpdate = Http2Frame.createWindowUpdateFrame(
            0,
            frameHeader.length
          );
          this.writer.write(connWindowUpdate as any);
        } catch (err) {
          console.error("[HTTP2] Error sending window update:", err);
        }
        //判断是否是最后一个帧
        if (
          (frameHeader.flags & FRAME_FLAGS.END_STREAM) ===
          FRAME_FLAGS.END_STREAM
        ) {
          this.endFlag = true;
          if (this.onEnd) {
            this.onEnd();
          }
          return;
        }
        break;
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
          this.endFlag = true;
          if (this.onEnd) {
            this.onEnd();
          }
          return;
        }
        break;
      case FRAME_TYPES.WINDOW_UPDATE:
        // 处理窗口更新帧
        this.handleWindowUpdateFrame(
          frameHeader,
          frameData,
          frameHeader.streamId
        );
        // 更新发送窗口（对端接收窗口）
        try {
          const inc = this.parseWindowUpdateFrame(frameData, frameHeader).windowSizeIncrement;
          if (frameHeader.streamId === 0) {
            this.sendConnWindow += inc;
          } else {
            const cur = this.sendStreamWindows.get(frameHeader.streamId) ?? this.peerInitialStreamWindow;
            this.sendStreamWindows.set(frameHeader.streamId, cur + inc);
          }
          const waiters = this.sendWindowWaiters.splice(0);
          waiters.forEach(fn => { try { fn(); } catch {} });
        } catch (e) {}
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
        } catch {}
        try {
          this.onGoaway?.(info ?? {});
        } catch (err) {
          console.error('Error during GOAWAY callback:', err);
        }
        this.endFlag = true;
        try {
          this.onEnd?.();
        } catch (err) {
          console.error('Error during GOAWAY onEnd callback:', err);
        }
        break;
      }

      // case FRAME_TYPES.PUSH_PROMISE:
      //     // 处理服务器推送承诺帧
      //     this.handlePushPromiseFrame(frameHeader, frameData);
      //     break;
      case FRAME_TYPES.RST_STREAM:
        this.endFlag = true;
        if (this.onEnd) {
          this.onEnd();
        }
        break;
      default:
        console.debug("Unknown frame type:", frameHeader.type);
    }
  }

  _parseFrameHeader(buffer: Uint8Array) {
    const length = (buffer[0] << 16) | (buffer[1] << 8) | buffer[2];
    const type = buffer[3];
    const flags = buffer[4];
    const streamId =
      (buffer[5] << 24) | (buffer[6] << 16) | (buffer[7] << 8) | buffer[8];

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
      this.writer.write(pongFrame as any);
    } catch (error) {
      console.error("Error sending PONG frame:", error);
      throw error;
    }
  }

  //等待流结束
  waitForEndOfStream(waitTime: number): Promise<void> {
    return new Promise((resolve, reject) => {
      // If the stream has already ended, resolve immediately
      if (this.endFlag) {
        resolve();
        return;
      }
      // 如果是0 ,则不设置超时
      let timeout: NodeJS.Timeout | null = null;
      if (waitTime > 0) {
        timeout = setTimeout(() => {
          clearInterval(interval);
          reject(new Error("End of stream timeout"));
        }, waitTime);
      }

      // Check interval for real-time endFlag monitoring
      const checkInterval = 100; // Check every 100 milliseconds
      // Set an interval to check the endFlag regularly
      const interval = setInterval(() => {
        if (this.endFlag) {
          if (timeout !== null) {
            clearTimeout(timeout);
          }
          clearInterval(interval);
          resolve();
        }
      }, checkInterval);

      // If the onEnd is triggered externally, it should now be marked manually
      const originalOnEnd = this.onEnd;
      this.onEnd = () => {
        if (!this.endFlag) {
          // The external trigger may set endFlag; if not, handle here
          this.endFlag = true;
        }
        if (timeout !== null) {
          clearTimeout(timeout);
        }
        clearInterval(interval);
        resolve();
        if (originalOnEnd) {
          originalOnEnd(); // Call the original onEnd function if set
        }
      };
    });
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
    payload: Uint8Array,
    streamId: number
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
