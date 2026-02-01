import { HPACK } from './hpack.js'
import type {Byte,Frame,FrameSettingPayload} from './types.js'
import {FrameEncoder} from './encoder.js'

const  SETTINGS_PARAMETERS = {  
    HEADER_TABLE_SIZE: 0x1,  
    ENABLE_PUSH: 0x2,  
    MAX_CONCURRENT_STREAMS: 0x3,  
    INITIAL_WINDOW_SIZE: 0x4,  
    MAX_FRAME_SIZE: 0x5,  
    MAX_HEADER_LIST_SIZE: 0x6  
}; 

const defaultSettings = {
    [SETTINGS_PARAMETERS.HEADER_TABLE_SIZE]: 4096,  
    [SETTINGS_PARAMETERS.ENABLE_PUSH]: 1,  
    [SETTINGS_PARAMETERS.MAX_CONCURRENT_STREAMS]: 100,  
    [SETTINGS_PARAMETERS.INITIAL_WINDOW_SIZE]: 16<<10, // 16k
    [SETTINGS_PARAMETERS.MAX_FRAME_SIZE]:16<<10 , // 16k
    [SETTINGS_PARAMETERS.MAX_HEADER_LIST_SIZE]: 8192  
}; 

const HTTP2_PREFACE = 'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n'

export class Http2Frame {

     

    // 创建并编码PREFACE帧
    static createPreface():Uint8Array {  
        return new TextEncoder().encode(HTTP2_PREFACE);  
    }
    static createPongFrame(payload:Uint8Array):Uint8Array {
        return Http2Frame.createFrame(0x6, 0x1, 0, payload) 
    }
    // 创建并编码SETTINGS帧  
    static createSettingsFrame(settings = {}) {  
        // 先创建帧对象  
        const frame = Http2Frame.createOriginSettingsFrame(settings);
        // 然后编码  
        return FrameEncoder.encodeSettingsFrame(frame);  
    } 
     // 创建并编码SETTINGS ACK帧  
    static createSettingsAckFrame() {  
        return FrameEncoder.encodeSettingsAckFrame();  
    }  

    /**
 * 创建 WINDOW_UPDATE 帧
 * @param streamId 流ID，0表示连接级别的窗口更新
 * @param increment 窗口大小增量（必须为正数）
 * @returns 编码好的 WINDOW_UPDATE 帧
 */
static createWindowUpdateFrame(streamId: number, increment: number): Uint8Array {
    // 验证窗口增量
    if (increment <= 0 || increment > 2147483647) { // 2^31 - 1
        throw new Error('Window size increment must be between 1 and 2^31-1');
    }
    
    // 创建4字节的payload
    const payload = new Uint8Array(4);
    
    // 写入窗口大小增量 (31位无符号整数，最高位保留为0)
    payload[0] = (increment >> 24) & 0x7F; // 只用低7位，确保最高位为0
    payload[1] = (increment >> 16) & 0xFF;
    payload[2] = (increment >> 8) & 0xFF;
    payload[3] = increment & 0xFF;
    
    // 创建帧，类型0x8表示WINDOW_UPDATE
    return Http2Frame.createFrame(0x8, 0x0, streamId, payload);
}


    static createDataFrames(streamId: number, data: Uint8Array, shouldEnd: boolean = false, maxFrameSize: number = 16384): Uint8Array[] {
    const frames: Uint8Array[] = [];
    
    // 首先构建完整的 gRPC 消息
    // 格式: Compressed-Flag(1) + Message-Length(4) + Message-Data
    const messageLength = data.length;
    const grpcMessage = new Uint8Array(5 + messageLength);
    
    // Compressed-Flag (0 = 不压缩)
    grpcMessage[0] = 0;
    
    // Message-Length (4 bytes) - 整个消息的长度
    grpcMessage[1] = (messageLength >> 24) & 0xFF;
    grpcMessage[2] = (messageLength >> 16) & 0xFF;
    grpcMessage[3] = (messageLength >> 8) & 0xFF;
    grpcMessage[4] = messageLength & 0xFF;
    
    // Message-Data
    grpcMessage.set(data, 5);
    
    // 然后将完整的 gRPC 消息分割成多个 HTTP/2 DATA 帧
    // HTTP/2 帧头为 9 字节
    const maxDataPerFrame = maxFrameSize - 9;
    
    for (let offset = 0; offset < grpcMessage.length; offset += maxDataPerFrame) {
        const remaining = grpcMessage.length - offset;
        const chunkSize = Math.min(maxDataPerFrame, remaining);
        const chunk = grpcMessage.slice(offset, offset + chunkSize);
        
        // 判断是否是最后一个数据块
        const isLastChunk = (offset + chunkSize) >= grpcMessage.length;
        const endStream = isLastChunk && shouldEnd;
        
        // 创建 HTTP/2 DATA 帧
        const frame = Http2Frame.createFrame(
            0x0, // DATA 帧类型
            endStream ? 0x01 : 0x0, // 只在最后一块且需要结束流时设置 END_STREAM
            streamId,
            chunk
        );
        
        frames.push(frame);
    }
    
    return frames;
}

   static createDataFrame( streamId:number,data:Uint8Array,endStream:boolean = true):Uint8Array {  
        // gRPC 消息格式: 压缩标志(1字节) + 消息长度(4字节) + 消息内容  
        const messageLen = data.length  
        const framedData = new Uint8Array(5+messageLen)  
        
        // Compression flag (0 = 不压缩)  
        framedData[0] = 0  
        
        // Message length (4 bytes)  
        framedData[1] = (messageLen >> 24) & 0xFF  
        framedData[2] = (messageLen >> 16) & 0xFF  
        framedData[3] = (messageLen >> 8) & 0xFF  
        framedData[4] = messageLen & 0xFF  
        
        // Message content  
        framedData.set(data, 5)  

        const flags = endStream ? 0x01 : 0x0 // END_STREAM flag  
        return Http2Frame.createFrame(0x0, flags, streamId, framedData)  
    }  

    static createHeadersFrame(streamId:number,path:string, endHeaders:boolean = true,token?:string):Uint8Array {  
        // gRPC-Web 需要的标准 headers  
        const headersList: { [key: string]: string } = {  
            ':path': path,  
            ':method': 'POST',  
            ':scheme': 'http',  
            ':authority': 'localhost',  
            'content-type': 'application/grpc+proto',  
            'user-agent': 'grpc-web-client/0.1',  
            'accept': 'application/grpc+proto',  
            'grpc-timeout': '3600S'  
        };  
        if (token) {  
            headersList['authorization'] = `Bearer ${token}`  
        }
        // 将 headers 编码为 HPACK 格式  
        const hpack = new HPACK();
        const encodedHeaders = hpack.encode(headersList);
        // HEADERS frame flags: END_HEADERS | END_STREAM  
        const flags = endHeaders ? 0x04 : 0x00  
        return Http2Frame.createFrame(0x01, flags, streamId, encodedHeaders)  
    }  

    static createResponseHeadersFrame(streamId:number,headersList:{ [key: string]: string }, endHeaders:boolean = true):Uint8Array {  
        // 将 headers 编码为 HPACK 格式  
        const hpack = new HPACK();
        const encodedHeaders = hpack.encode(headersList);
        // HEADERS frame flags: END_HEADERS | END_STREAM  
        const flags = endHeaders ? 0x04 : 0x00  
        return Http2Frame.createFrame(0x01, flags, streamId, encodedHeaders)  
    }  


    static createTrailersFrame(streamId: number, trailers: { [key: string]: string }): Uint8Array {
        // 将 trailers 编码为 HPACK 格式
        const hpack = new HPACK();
        const encodedTrailers = hpack.encode(trailers);
    
        // HEADERS frame flags: END_HEADERS | END_STREAM
        const flags = 0x05; // 0x04 (END_HEADERS) | 0x01 (END_STREAM)
        return Http2Frame.createFrame(0x01, flags, streamId, encodedTrailers);
    }
  

    // 创建 SETTINGS 帧  
    static createOriginSettingsFrame(settings = {}):Frame {  
    
       
        // 合并默认值和用户提供的设置  
        const finalSettings = { ...defaultSettings, ...settings }; 
         // 验证设置值  
        _validateSettings(finalSettings);   
        // 创建帧  
        const frame = {  
            type: 0x4, // SETTINGS frame type  
            flags: 0x0, // 无标志  
            streamId: 0, // SETTINGS 总是在 stream 0 上发送  
            payload: _createPayload(finalSettings)  
        };  
        return frame;  
    }  

    // 创建确认帧（SETTINGS ACK）  
    static createOriginSettingsAckFrame() {  
        return {  
            type: 0x4, // SETTINGS frame type  
            flags: 0x1, // ACK flag  
            streamId: 0,  
            payload: [] // ACK 帧没有payload  
        };  
    }  

    static createFrame(type:Byte, flags:Byte, streamId:number, payload:Uint8Array):Uint8Array {  
        const length = payload ? payload.length : 0  
        const frame = new Uint8Array(9 + length) // 9 bytes for header + payload  

        // Length (24 bits)  
        frame[0] = (length >> 16) & 0xFF  
        frame[1] = (length >> 8) & 0xFF  
        frame[2] = length & 0xFF  
        
        // Type (8 bits)  
        frame[3] = type  
        
        // Flags (8 bits)  
        frame[4] = flags  
        
        // Stream Identifier (32 bits)  
        frame[5] = (streamId >> 24) & 0xFF  
        frame[6] = (streamId >> 16) & 0xFF  
        frame[7] = (streamId >> 8) & 0xFF  
        frame[8] = streamId & 0xFF  

        if (payload && length > 0) {  
            frame.set(payload, 9)  
        }  
        return frame  
    }  
}

 // 验证设置值  
 function _validateSettings(settings: { [key: Byte]: number }) {  
    for (const [id, value] of Object.entries(settings)) {  
        switch (Number(id)) {  
            case SETTINGS_PARAMETERS.HEADER_TABLE_SIZE:  
                if (value < 0) {  
                    throw new Error('HEADER_TABLE_SIZE must be non-negative');  
                }  
                break;  

            case SETTINGS_PARAMETERS.ENABLE_PUSH:  
                if (value !== 0 && value !== 1) {  
                    throw new Error('ENABLE_PUSH must be 0 or 1');  
                }  
                break;  

            case SETTINGS_PARAMETERS.INITIAL_WINDOW_SIZE:  
                if (value < 0 || value > 2147483647) { // 2^31 - 1  
                    throw new Error('INITIAL_WINDOW_SIZE must be between 0 and 2^31-1');  
                }  
                break;  

            case SETTINGS_PARAMETERS.MAX_FRAME_SIZE:  
                if (value < 16384 || value > 16777215) { // 2^14 to 2^24-1  
                    throw new Error('MAX_FRAME_SIZE must be between 16,384 and 16,777,215');  
                }  
                break;  

            case SETTINGS_PARAMETERS.MAX_CONCURRENT_STREAMS:  
            case SETTINGS_PARAMETERS.MAX_HEADER_LIST_SIZE:  
                if (value < 0) {  
                    throw new Error(`Parameter ${id} must be non-negative`);  
                }  
                break;  

            default:  
                console.warn(`Unknown settings parameter: ${id}`);  
        }  
    }  
}  

// 创建payload  
 function _createPayload(settings: { [key: Byte]: number }) {  
    const payload: FrameSettingPayload = [];  
    for (const [id, value] of Object.entries(settings)) {  
        payload.push({  
            identifier: Number(id),  
            value: value  
        });  
    }  
    return payload;  
}  