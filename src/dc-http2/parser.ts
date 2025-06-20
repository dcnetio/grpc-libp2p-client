import type { Frame } from "./types";
import type { Stream } from "@libp2p/interface";
import { FRAME_TYPES, FRAME_FLAGS } from "./types";
import { Http2Frame } from "./frame";
import { StreamWriter } from "./stream";

export class HTTP2Parser {
  buffer: Uint8Array;
  settingsAckReceived: boolean;
  connectionWindowSize: number;
  streams: Map<number, any>;
  defaultStreamWindowSize: number;
  onSettings?: (frameHeader: any) => void;
  onData?: (payload: Uint8Array, frameHeader: any) => void;
  onEnd?: () => void;
  onHeaders?: (headers: Uint8Array, frameHeader: any) => void;
  endFlag: boolean;
  writer: StreamWriter;

  constructor(writer: StreamWriter) {
    this.buffer = new Uint8Array(0);
    this.settingsAckReceived = false;
    // 初始化连接级别的流控制窗口大小（默认值：65,535）
    this.connectionWindowSize = 4 << 20;
    // 存储流的Map
    this.streams = new Map();
    // 默认的流级别初始窗口大小
    this.defaultStreamWindowSize = 4 << 20;
    // 结束标志
    this.endFlag = false;

    this.writer = writer;
  }

  // 持续处理流数据
  async processStream(stream: Stream) {
    try {
      for await (const chunk of stream.source) {
        const newData = chunk.subarray();

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
            const settingFrme = Http2Frame.createSettingsFrame();
            this.writer.write(settingFrme);
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
          await this._handleFrame(frameHeader, frameData);

          // 移除已处理的帧
          this.buffer = this.buffer.slice(totalFrameLength);
        }
      }
    } catch (error) {
      console.error("Error processing stream:", error);
      throw error;
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

  // 处理单个帧
  async _handleFrame(frameHeader: Frame, frameData: Uint8Array) {
    switch (frameHeader.type) {
      case FRAME_TYPES.SETTINGS:
        if ((frameHeader.flags & FRAME_FLAGS.ACK) === FRAME_FLAGS.ACK) {
          this.settingsAckReceived = true;
        } else {
          //接收到Setting请求,进行解析
          const settingsPayload = frameData.slice(9);
          const settings = {};
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
              this.defaultStreamWindowSize = value;
            }
          }

          //发送ACK
          if (this.onSettings) {
            this.onSettings(frameHeader);
          }
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
            this.writer.write(streamWindowUpdate);
          }

          // 更新连接级别的窗口
          const connWindowUpdate = Http2Frame.createWindowUpdateFrame(
            0,
            frameHeader.length
          );
          this.writer.write(connWindowUpdate);
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
        break;
      case FRAME_TYPES.PING:
        // 处理PING帧
        this._handlePingFrame(frameHeader, frameData);
        break;

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
      this.writer.write(pongFrame);
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
