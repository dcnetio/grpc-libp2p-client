
# grpc-libp2p-client

`grpc-libp2p-client` 是一个在浏览器环境中运行的 gRPC 客户端库，它构建于 [libp2p](https://libp2p.io/) 之上。它使得浏览器可以直接与基于 libp2p 的 gRPC 服务端进行安全、高效的点对点通信。

本库完整支持四种 gRPC通信模式：
*   **Unary (一元调用)**
*   **Server Streaming (服务端流)**
*   **Client Streaming (客户端流)**
*   **Bidirectional Streaming (双向流)**

---

## 目录

- [安装](#安装)
- [快速上手](#快速上手)
- [用法示例](#用法示例)
  - [1. Unary (请求-响应)](#1-unary-请求-响应)
  - [2. Server Streaming (服务端流)](#2-server-streaming-服务端流)
  - [3. Client Streaming (客户端流)](#3-client-streaming-客户端流)
  - [4. Bidirectional (双向流)](#4-bidirectional-双向流)
- [API 参考](#api-参考)

---

## 安装

```bash
npm install grpc-libp2p-client
# 或者
yarn add grpc-libp2p-client
```

## 快速上手

下面是一个快速设置并初始化客户端的示例。

```javascript
import { createLibp2p } from 'libp2p';
import { webRTC } from '@libp2p/webrtc';
import { noise } from '@chainsafe/libp2p-noise';
import { yamux } from '@chainsafe/libp2p-yamux';
import { Libp2pGrpcClient } from 'grpc-libp2p-client';

// 1. 创建并启动一个 libp2p 节点
// 这是客户端运行的基础，负责网络连接和数据传输
const node = await createLibp2p({
  transports: [
    webRTC()
  ],
  connectionEncryption: [
    noise()
  ],
  streamMuxers: [
    yamux()
  ]
});
await node.start();
console.log('Libp2p node started with Peer ID:', node.peerId.toString());


// 2. 定义服务端信息
const serverPeerAddr = '/ip4/x.x.x.x/tcp/4006/p2p/12D3KooW...'; // 替换为你的 gRPC 服务端的 PeerAddr
const serverProtocol = '/my-grpc-service/1.0.0'; // 替换为服务端使用的协议ID

// 3. 实例化 gRPC 客户端
// 将 libp2p 节点实例和服务端信息传入
const grpcClient = new Libp2pGrpcClient(node, serverPeerAddr, "",serverProtocol);

console.log('gRPC client is ready to make calls.');
```

## 用法示例

在初始化客户端后，你可以根据需要选择不同的模式与服务端通信。

**注意**: 所有模式中，请求体(`messageBytes`)都需要预先使用 `protobuf` 进行编码。

### 1. Unary (请求-响应)

这是最基础的模式，客户端发送一个请求，服务端返回一个响应。

```javascript
// 假设 messageBytes 是已经编码好的请求数据
// const messageBytes = MyRequest.encode({ field: 'value' }).finish();

try {
  const responseData = await grpcClient.unaryCall(
    "/myservice.Service/MyMethod", // gRPC 方法路径
    messageBytes,                  // Protobuf 编码后的请求字节流
    3000                           // 调用超时时间 (ms)
  );
  
  // 解码响应数据
  // const response = MyResponse.decode(responseData);
  console.log('Unary call successful, response:', responseData);

} catch (error) {
  console.error('Unary call failed:', error);
}
```

### 2. Server Streaming (服务端流)

客户端发送一个请求，服务端以流的形式持续返回多个响应。

```javascript
// 定义一个回调函数来处理服务端发来的每一条数据
const onDataCallback = (payload: Uint8Array) => {
  // const message = MyStreamResponse.decode(payload);
  console.log('Received stream data:', payload);
};

await grpcClient.Call(
  "/myservice.Service/MyServerStreamMethod",
  messageBytes,
  30000,
  "server-streaming",
  onDataCallback,
);
```

### 3. Client Streaming (客户端流)

客户端以流的形式发送多个请求，服务端在接收完所有请求后返回一个响应。

```javascript
// 创建一个异步迭代器，用于持续提供要发送的数据
const dataSourceCallback = async function* (): AsyncIterable<Uint8Array> {
  for (let i = 0; i < 5; i++) {
    // const requestPart = MyStreamRequest.encode({ part: i }).finish();
    const requestPart = new Uint8Array([i]); // 示例数据
    yield requestPart;
    await new Promise(resolve => setTimeout(resolve, 500)); // 模拟异步发送
  }
};

// 定义数据回调，只会在服务端最终响应时触发一次
const onDataCallback = (payload: Uint8Array) => {
  // const finalResponse = MyFinalResponse.decode(payload);
  console.log('Client stream finished, final response:', payload);
};

await grpcClient.Call(
  "/myservice.Service/MyClientStreamMethod",
  messageBytes, // 初始消息体
  30000,
  "client-streaming",
  onDataCallback,
  dataSourceCallback,
);
```

### 4. Bidirectional (双向流)

客户端和服务端可以同时向对方发送数据流，实现全双工通信。

```javascript
// 定义数据回调，处理服务端发来的每一条数据
const onDataCallback = (payload: Uint8Array) => {
  console.log('Bidi: Received data from server:', payload);
};

// 定义数据源，持续向服务端发送数据
const dataSourceCallback = async function* (): AsyncIterable<Uint8Array> {
  for (let i = 0; i < 5; i++) {
    const requestPart = new Uint8Array([i]);
    console.log(`Bidi: Sending data to server: ${i}`);
    yield requestPart;
    await new Promise(resolve => setTimeout(resolve, 500));
  }
};

// 定义结束和错误处理回调
const onEndCallback = () => console.log('Bidi stream ended.');
const onErrorCallback = (err: unknown) => console.error('Bidi stream error:', err);

await grpcClient.Call(
  "/myservice.Service/MyBidiMethod",
  messageBytes, // 可以发送一个初始消息
  30000,
  "bidirectional",
  onDataCallback,
  dataSourceCallback,
  onEndCallback,
  onErrorCallback
);
```

---

## API 参考

### `new Libp2pGrpcClient(node, serverPeerAddr,token, protocol)`

-   `node` (`Libp2p`): 一个已创建并启动的 libp2p 节点实例。
-   `serverPeerAddr` (`string`): 目标 gRPC 服务端的p2p地址字符串。
-   `token`   (`string`): 目标 gRPC 服务端要求的认证信息,在http2头的authorization字段中提交,可以为空。
-   `protocol` (`string`): 双方协定好的、用于 gRPC 通信的 libp2p 协议 ID。

### `grpcClient.unaryCall(path, payload, timeout)`

用于发起一次性的请求-响应调用。

-   返回: `Promise<Uint8Array>` - 服务端返回的 Protobuf 编码后的字节流。

### `grpcClient.Call(path, payload, timeout, mode, onDataCallback,dataSourceCallback?,,onEndCallback?,onErrorCallback?,context?)`

用于发起所有类型的流式调用。

-   `path` (`string`): gRPC 方法的完整路径。
-   `payload` (`Uint8Array` | `null`): 初始请求的字节流。
-   `timeout` (`number`): 调用超时时间 (ms)。
-   `mode` (`string`): 调用模式。
    -   `"server-streaming"`
    -   `"client-streaming"`
    -   `"bidirectional"`
-   `onDataCallback` (`(data: Uint8Array) => void`): 接收数据的回调。
-   `dataSourceCallback?` (`() => AsyncIterable<Uint8Array>`): 提供发送数据的异步迭代器 (用于客户端流和双向流)。
-   `onEndCallback?` (`() => void`): 流正常结束时的回调。
-   `onErrorCallback?` (`(err: unknown) => void`): 发生错误时的回调。
-   `context?` (` { signal?: AbortSignal }`): 中间取消。


