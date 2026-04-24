import { Libp2pGrpcClient } from '../src/index.js';

/**
 * 示例：使用优化后的批量数据源回调
 * 这个示例展示了如何通过批量返回chunks来提高性能
 */

// 示例1：传统方式 - 逐个返回chunks（较慢）
async function* traditionalDataSource(): AsyncIterable<Uint8Array> {
    for (let i = 0; i < 100; i++) {
        const data = new TextEncoder().encode(`chunk-${i}`);
        yield new Uint8Array(data);
        
        // 模拟异步操作延迟
        await new Promise(resolve => setTimeout(resolve, 10));
    }
}

// 示例2：优化方式 - 批量返回chunks（更快）
async function* optimizedDataSource(): AsyncIterable<Uint8Array[]> {
    const batchSize = 10;
    
    for (let batch = 0; batch < 10; batch++) {
        const chunks: Uint8Array[] = [];
        
        // 批量准备多个chunks
        for (let i = 0; i < batchSize; i++) {
            const chunkIndex = batch * batchSize + i;
            const data = new TextEncoder().encode(`batch-chunk-${chunkIndex}`);
            chunks.push(new Uint8Array(data));
        }
        
        // 一次性返回整个批次
        yield chunks;
        
        // 批次间的延迟比单个chunk的延迟更长，但总体更少
        await new Promise(resolve => setTimeout(resolve, 50));
    }
}

// 示例3：智能批量 - 根据数据大小动态批量
async function* smartBatchDataSource(): AsyncIterable<Uint8Array | Uint8Array[]> {
    const targetBatchSize = 64 * 1024; // 目标批量大小: 64KB
    let currentBatch: Uint8Array[] = [];
    let currentBatchSize = 0;
    
    for (let i = 0; i < 1000; i++) {
        const data = new TextEncoder().encode(`smart-chunk-${i}-${'x'.repeat(Math.random() * 1000)}`);
        const chunk = new Uint8Array(data);
        
        currentBatch.push(chunk);
        currentBatchSize += chunk.length;
        
        // 当批量大小达到目标时，返回批量
        if (currentBatchSize >= targetBatchSize) {
            yield currentBatch;
            currentBatch = [];
            currentBatchSize = 0;
        }
        
        await new Promise(resolve => setTimeout(resolve, 1));
    }
    
    // 返回剩余的chunks
    if (currentBatch.length > 0) {
        yield currentBatch;
    }
}

// 使用示例 - 展示优化的批量处理
async function demonstrateOptimization(client: Libp2pGrpcClient) {
    console.log('开始批量优化对比测试...');
    
    // 传统方式：逐个chunk发送
    console.time('传统单chunk方式');
    
    await client.Call(
        '/example.Service/TraditionalStream',
        new Uint8Array(),
        30000,
        'client-streaming',
        (data) => {
            console.log('传统方式收到响应:', data.length, '字节');
        },
        traditionalDataSource,
        () => {
            console.timeEnd('传统单chunk方式');
        },
        (error) => {
            console.error('传统方式错误:', error);
        }
    );
    
    // 优化方式：批量发送chunks（frames策略）
    console.time('优化批量方式');
    
    await client.Call(
        '/example.Service/OptimizedStream',
        new Uint8Array(),
        30000,
        'client-streaming',
        (data) => {
            console.log('优化方式收到响应:', data.length, '字节');
        },
        optimizedDataSource,
        () => {
            console.timeEnd('优化批量方式');
        },
        (error) => {
            console.error('优化方式错误:', error);
        },
        undefined,
        {
            batchSize: 10,        // 批量大小
            maxBatchWaitMs: 50    // 最大等待时间
        }
    );
    
    // 智能批量方式：动态调整批量大小
    console.time('智能批量方式');
    
   await client.Call(
        '/example.Service/SmartStream',
        new Uint8Array(),
        30000,
        'client-streaming',
        (data) => {
            console.log('智能方式收到响应:', data.length, '字节');
        },
        smartBatchDataSource,
        () => {
            console.timeEnd('智能批量方式');
        },
        (error) => {
            console.error('智能方式错误:', error);
        },
        undefined,
        {
            batchSize: 20,        // 更大的批量大小
            maxBatchWaitMs: 100   // 更长的等待时间
        }
    );
}

export {
    traditionalDataSource,
    optimizedDataSource,
    smartBatchDataSource,
    demonstrateOptimization
};