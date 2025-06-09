
import type { Libp2p } from 'libp2p'
import { HTTP2Parser } from './dc-http2/parser';
import {StreamWriter} from './dc-http2/stream'
import { Http2Frame } from './dc-http2/frame';
import  type { Stream } from '@libp2p/interface';
import { HPACK} from './dc-http2/hpack'

import type { Multiaddr } from '@multiformats/multiaddr';




class StreamManager {  
    currentStreamId: number;

    constructor() {  
      this.currentStreamId = 1; // 从 1 开始，以模拟奇数 ID  
    }  

    getNextAppLevelStreamId() {  
      const id = this.currentStreamId;  
      this.currentStreamId += 2; // 确保奇数步进  
      return id;  
    }  
  } 


export class Libp2pGrpcClient {
    node: Libp2p;
    protocol: string;
    steamManager: StreamManager;
    peerAddr: Multiaddr;
    token: string;


    constructor(node:Libp2p,peerAddr:Multiaddr,token:string,protocol?:string) {
        this.node = node
        this.peerAddr = peerAddr
        if (protocol) {
            this.protocol = protocol
        }else{
             this.protocol = '/dc/thread/0.0.1'
        }
        this.steamManager = new StreamManager()
        this.token = token
    }  

    setToken(token:string) {
        this.token = token
    }
    

    async unaryCall(method:string, requestData:Uint8Array,timeout:number): Promise<Uint8Array> {  
    let stream:Stream|null = null
    let responseData: Uint8Array | null = null
    let responseBuffer: Uint8Array[] = [] // 添加缓冲区来累积数据
    let responseDataExpectedLength = -1 // 当前响应的期望长度
    const hpack = new HPACK()
    let exitFlag = false
    let errMsg = ''
    let isResponseComplete = false // 添加标志来标识响应是否完成
    try {
       
       // const stream = await this.node.dialProtocol(this.peerAddr, this.protocol)
        const connection = await this.node.dial(this.peerAddr)
        stream = await connection.newStream(this.protocol, {
          maxOutboundStreams: 5
        })
        const streamId = this.steamManager.getNextAppLevelStreamId()
        const writer = new StreamWriter(stream.sink)  
        const parser = new HTTP2Parser(writer);  
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
            writer.write(ackSettingFrame)
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
        await writer.write(preface)
        // 发送Settings请求
        const settingFrme = Http2Frame.createSettingsFrame()
        await writer.write(settingFrme)
        await parser.waitForSettingsAck()
        // 创建头部帧
        const headerFrame = Http2Frame.createHeadersFrame( streamId,method,true,this.token)
        await writer.write(headerFrame)
        // 创建数据帧
        const dataFrames = Http2Frame.createDataFrames( streamId,requestData, true)
        // 发送请求
        for (const dataFrame of dataFrames) {
            await writer.write(dataFrame)
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
        await writer.end()
    } catch (err) {
        console.error('unaryCall error:', err)
        throw err
    }finally{
        if (stream) {
            await stream.close()
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
    
    async  Call(   
    method: string,  
    requestData: Uint8Array,  
    timeout: number,  
    mode: 'unary' | 'server-streaming' | 'client-streaming' | 'bidirectional', 
    onDataCallback: (payload: Uint8Array) => void,  
    dataSourceCallback?: () => AsyncIterable<Uint8Array>,
    onEndCallback?: () => void,  
    onErrorCallback?: (error: unknown) => void,    
  ): Promise<void> {  
     let timeoutHandle ;
    const timeoutPromise = new Promise<never>((_, reject) =>  
     timeoutHandle = setTimeout(() => reject(new Error('Operation timed out')), timeout)  
    );  
    const hpack = new HPACK()
    const operationPromise = (async () => {  
      let stream: Stream | null = null;
      let messageBuffer = new Uint8Array(0) // 用于累积跨帧的消息数据
      let expectedMessageLength = -1 // 当前消息的期望长度
      
      try {  
        const connection = await this.node.dial(this.peerAddr)
        stream = await connection.newStream(this.protocol)
        const streamId = this.steamManager.getNextAppLevelStreamId();  
        const writer = new StreamWriter(stream.sink);  
        const parser = new HTTP2Parser(writer);  
        clearTimeout(timeoutHandle)
        
        // Define the onData method to utilize the provided callback  
        parser.onData = async (payload, frameHeader): Promise<void> => {  
          try {  
            // 将新数据添加到消息缓冲区
            const newBuffer = new Uint8Array(messageBuffer.length + payload.length)
            newBuffer.set(messageBuffer)
            newBuffer.set(payload, messageBuffer.length)
            messageBuffer = newBuffer
            
            // 处理缓冲区中的完整消息
            while (messageBuffer.length > 0) {
              // 如果还没有读取消息长度，且缓冲区有足够数据
              if (expectedMessageLength === -1 && messageBuffer.length >= 5) {
                // 读取 gRPC 消息头：1字节压缩标志 + 4字节长度
                const compressionFlag = messageBuffer[0]
                const lengthBytes = messageBuffer.slice(1, 5)
                expectedMessageLength = new DataView(lengthBytes.buffer, lengthBytes.byteOffset).getUint32(0, false) // big-endian
              }
              
              // 如果知道期望长度且有足够数据
              if (expectedMessageLength !== -1 && messageBuffer.length >= expectedMessageLength + 5) {
                // 提取完整消息（跳过5字节头部）
                const completeMessage = messageBuffer.slice(5, expectedMessageLength + 5)
                
                // 调用回调处理这个完整消息
                onDataCallback(completeMessage)
                
                // 移除已处理的消息，保留剩余数据
                messageBuffer = messageBuffer.slice(expectedMessageLength + 5)
                expectedMessageLength = -1
              } else {
                // 没有足够数据构成完整消息，等待更多数据
                break
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
        
        parser.onSettings = () => {//接收settings,反馈ack
            const ackSettingFrame = Http2Frame.createSettingsAckFrame()
            writer.write(ackSettingFrame)
        }
        parser.onHeaders = (headers,header) => {
            const plainHeaders = hpack.decodeHeaderFields(headers)
            if (plainHeaders.get('grpc-status') === '0') {
            } else if (plainHeaders.get('grpc-status') !== undefined) {
                const errMsg = plainHeaders.get('grpc-message') || 'gRPC call failed'
                const err  = new Error(errMsg)
                 if (onErrorCallback) {  
                  onErrorCallback(err);  
                } else {  
                  throw err;  
                }  
            }
        }
        parser.processStream(stream);  
        // Handshake - send HTTP/2 preface  
        const preface = Http2Frame.createPreface();  
        await writer.write(preface);  

        // Send Settings request  
        const settingFrame = Http2Frame.createSettingsFrame();  
        await writer.write(settingFrame);  
        // Wait for the acknowledgement of SETTINGS  
        await parser.waitForSettingsAck();  
        // Send Settings ACK  
        const ackSettingFrame = Http2Frame.createSettingsAckFrame();  
        await writer.write(ackSettingFrame);  
       

        // Create header frame  
        const headerFrame = Http2Frame.createHeadersFrame(streamId, method, true,this.token);  
        if (mode === 'unary' || mode === 'server-streaming') {  
          const dataFrames = Http2Frame.createDataFrames (streamId,requestData,  true);  
          await writer.write(new Uint8Array([...headerFrame]));  
          for (const dataFrame of dataFrames) {  
            await writer.write(dataFrame);  
          }
        } else if ((mode === 'client-streaming' || mode === 'bidirectional') && dataSourceCallback) {  
          await writer.write(headerFrame);  
          if (requestData.length > 0) {
            const dataFrames = Http2Frame.createDataFrames(streamId,requestData,  false); 
            for (const dataFrame of dataFrames) {  
              await writer.write(dataFrame);  
            }
        }
          for await (const chunk of dataSourceCallback()) {  
            const dataFrames = Http2Frame.createDataFrames( streamId,chunk, false);  
            for (const dataFrame of dataFrames) {
              await writer.write(dataFrame);
            } 
          }  
          const finalFrame = Http2Frame.createDataFrame(streamId,new Uint8Array(),  true);  
          await writer.write(finalFrame);  
          await writer.end()
        }  
        await parser.waitForEndOfStream(0);  
        if (onEndCallback) {  
          onEndCallback();  
        }  
      } catch (err: unknown) {  
        if (onErrorCallback) {  
          onErrorCallback(err);  
        } else {  
          if (err instanceof Error) {  
            console.error('asyncCall error:', err.message);  
          } else {  
            console.error('asyncCall error:', err);  
          }  
        }  
      } finally {
        if (stream) {
            await stream.close()
        }
      } 
    })();  
    return Promise.race([operationPromise, timeoutPromise]);  
  }  
}


   
