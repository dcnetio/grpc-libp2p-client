import { Libp2pGrpcClient } from '../src/index.js';

/**
 * 动态批处理示例
 * 展示如何在处理过程中动态补充新数据，提高并发利用率
 */

// 模拟不同速度的数据源
async function* variableSpeedDataSource(): AsyncIterable<Uint8Array> {
    for (let i = 0; i < 50; i++) {
        const data = new TextEncoder().encode(`chunk-${i}`);
        yield new Uint8Array(data);
        
        // 模拟不同的生产速度
        const delay = Math.random() * 100; // 0-100ms随机延迟
        await new Promise(resolve => setTimeout(resolve, delay));
    }
}

// 模拟批量数据源（某些时候会批量产生数据）
async function* burstyDataSource(): AsyncIterable<Uint8Array[]> {
    for (let batch = 0; batch < 10; batch++) {
        // 模拟突发性数据产生
        if (Math.random() > 0.5) {
            // 50%概率产生批量数据
            const chunks: Uint8Array[] = [];
            const batchSize = Math.floor(Math.random() * 8) + 2; // 2-10个chunks
            
            for (let i = 0; i < batchSize; i++) {
                const data = new TextEncoder().encode(`burst-batch-${batch}-chunk-${i}`);
                chunks.push(new Uint8Array(data));
            }
            
            console.log(`产生批量数据: ${batchSize} chunks`);
            yield chunks;
        } else {
            // 50%概率产生单个数据
            const data = new TextEncoder().encode(`single-batch-${batch}`);
            yield [new Uint8Array(data)];
        }
        
        // 批次间的延迟
        await new Promise(resolve => setTimeout(resolve, 200));
    }
}

// 模拟混合速度数据源
async function* mixedSpeedDataSource(): AsyncIterable<Uint8Array | Uint8Array[]> {
    let totalChunks = 0;
    
    for (let i = 0; i < 20; i++) {
        if (i % 3 === 0) {
            // 每3个循环产生一批数据
            const chunks: Uint8Array[] = [];
            const count = Math.floor(Math.random() * 5) + 3; // 3-7个chunks
            
            for (let j = 0; j < count; j++) {
                const data = new TextEncoder().encode(`mixed-batch-${i}-${j}`);
                chunks.push(new Uint8Array(data));
                totalChunks++;
            }
            
            console.log(`🚀 批量产生 ${count} chunks (总计: ${totalChunks})`);
            yield chunks;
            
            // 批量数据后短暂停顿
            await new Promise(resolve => setTimeout(resolve, 50));
        } else {
            // 产生单个数据
            const data = new TextEncoder().encode(`mixed-single-${i}`);
            console.log(`📦 单个chunk ${i} (总计: ${++totalChunks})`);
            yield new Uint8Array(data);
            
            // 单个数据间较长延迟
            await new Promise(resolve => setTimeout(resolve, 150));
        }
    }
}

// 性能监控数据源
async function* monitoredDataSource(): AsyncIterable<Uint8Array> {
    let startTime = Date.now();
    let chunkCount = 0;
    
    for (let i = 0; i < 100; i++) {
        const data = new TextEncoder().encode(`monitored-chunk-${i}-${Date.now()}`);
        chunkCount++;
        
        if (chunkCount % 10 === 0) {
            const elapsed = Date.now() - startTime;
            const rate = chunkCount / (elapsed / 1000);
            console.log(`📊 已产生 ${chunkCount} chunks, 速率: ${rate.toFixed(2)} chunks/秒`);
        }
        
        yield new Uint8Array(data);
        
        // 变化的延迟模式
        let delay;
        if (i < 20) {
            delay = 10; // 快速开始
        } else if (i < 60) {
            delay = Math.random() * 50; // 中间变化
        } else {
            delay = 5; // 最后加速
        }
        
        await new Promise(resolve => setTimeout(resolve, delay));
    }
}

// 动态批处理性能测试
async function demonstrateDynamicBatching(client: Libp2pGrpcClient) {
    console.log('🎯 开始动态批处理测试...\n');
    
    const results: Array<{name: string, duration: number, throughput: number}> = [];
    
    // 测试1: 变速数据源
    console.log('📈 测试1: 变速数据源');
    console.time('变速数据源处理');
    
    let receivedCount1 = 0;
    const cancel1 = await client.Call(
        '/example.Service/VariableSpeed',
        new Uint8Array(),
        60000,
        'client-streaming',
        (data) => {
            receivedCount1++;
            if (receivedCount1 % 10 === 0) {
                console.log(`  ✅ 已接收 ${receivedCount1} 响应`);
            }
        },
        variableSpeedDataSource,
        () => {
            console.timeEnd('变速数据源处理');
            console.log(`  📊 总计处理: ${receivedCount1} 消息\n`);
        },
        (error) => {
            console.error('变速数据源错误:', error);
        },
        undefined,
        {
            batchSize: 8,           // 较小批次，快速响应
            maxBatchWaitMs: 30      // 短等待时间
        }
    );
    
    // 测试2: 突发数据源
    console.log('💥 测试2: 突发数据源');
    console.time('突发数据源处理');
    
    let receivedCount2 = 0;
    const cancel2 = await client.Call(
        '/example.Service/BurstyData',
        new Uint8Array(),
        60000,
        'client-streaming',
        (data) => {
            receivedCount2++;
        },
        burstyDataSource,
        () => {
            console.timeEnd('突发数据源处理');
            console.log(`  📊 总计处理: ${receivedCount2} 消息\n`);
        },
        (error) => {
            console.error('突发数据源错误:', error);
        },
        undefined,
        {
            batchSize: 15,          // 较大批次，处理突发
            maxBatchWaitMs: 100     // 较长等待，收集更多数据
        }
    );
    
    // 测试3: 混合速度数据源
    console.log('🔄 测试3: 混合速度数据源');
    console.time('混合数据源处理');
    
    let receivedCount3 = 0;
    const cancel3 = await client.Call(
        '/example.Service/MixedSpeed',
        new Uint8Array(),
        60000,
        'client-streaming',
        (data) => {
            receivedCount3++;
        },
        mixedSpeedDataSource,
        () => {
            console.timeEnd('混合数据源处理');
            console.log(`  📊 总计处理: ${receivedCount3} 消息\n`);
        },
        (error) => {
            console.error('混合数据源错误:', error);
        },
        undefined,
        {
            batchSize: 12,          // 平衡的批次大小
            maxBatchWaitMs: 60      // 平衡的等待时间
        }
    );
    
    // 测试4: 性能监控数据源
    console.log('📊 测试4: 性能监控');
    console.time('性能监控处理');
    
    let receivedCount4 = 0;
    const startTime = Date.now();
    
    const cancel4 = await client.Call(
        '/example.Service/MonitoredData',
        new Uint8Array(),
        60000,
        'client-streaming',
        (data) => {
            receivedCount4++;
        },
        monitoredDataSource,
        () => {
            const duration = Date.now() - startTime;
            const throughput = receivedCount4 / (duration / 1000);
            
            console.timeEnd('性能监控处理');
            console.log(`  📊 总计处理: ${receivedCount4} 消息`);
            console.log(`  ⏱️  总耗时: ${duration}ms`);
            console.log(`  🚀 平均吞吐量: ${throughput.toFixed(2)} 消息/秒\n`);
            
            results.push({
                name: '性能监控',
                duration,
                throughput
            });
        },
        (error) => {
            console.error('性能监控错误:', error);
        },
        undefined,
        {
            batchSize: 20,          // 大批次，最大化吞吐量
            maxBatchWaitMs: 25      // 短等待，保持响应性
        }
    );
    
    // 输出性能总结
    setTimeout(() => {
        console.log('🎯 动态批处理测试总结:');
        console.log('动态批处理的优势:');
        console.log('  ✅ 自适应批次大小');
        console.log('  ✅ 处理过程中可补充新数据');
        console.log('  ✅ 更好的并发利用率');
        console.log('  ✅ 降低平均延迟');
        console.log('  ✅ 提高整体吞吐量');
    }, 1000);
}

// 批处理配置建议
const BatchingRecommendations = {
    // 低延迟场景
    lowLatency: {
        batchSize: 5,
        maxBatchWaitMs: 10,
        description: '优先响应速度，适合实时交互'
    },
    
    // 平衡场景
    balanced: {
        batchSize: 10,
        maxBatchWaitMs: 50,
        description: '平衡延迟和吞吐量，通用推荐'
    },
    
    // 高吞吐场景
    highThroughput: {
        batchSize: 25,
        maxBatchWaitMs: 100,
        description: '优先吞吐量，适合批量数据处理'
    },
    
    // 自适应场景
    adaptive: {
        batchSize: 15,
        maxBatchWaitMs: 30,
        description: '动态调整，适合变化的数据模式'
    }
};

export {
    variableSpeedDataSource,
    burstyDataSource,
    mixedSpeedDataSource,
    monitoredDataSource,
    demonstrateDynamicBatching,
    BatchingRecommendations
};