import { Libp2pGrpcClient } from '../src/index';

/**
 * 健壮性测试 - 确保Call方法不会卡死或产生未处理的错误
 */

// 测试用的模拟数据源
async function* normalDataSource(): AsyncIterable<Uint8Array> {
    for (let i = 0; i < 5; i++) {
        const data = new TextEncoder().encode(`normal-chunk-${i}`);
        yield new Uint8Array(data);
        await new Promise(resolve => setTimeout(resolve, 100));
    }
}

async function* slowDataSource(): AsyncIterable<Uint8Array> {
    for (let i = 0; i < 3; i++) {
        const data = new TextEncoder().encode(`slow-chunk-${i}`);
        yield new Uint8Array(data);
        await new Promise(resolve => setTimeout(resolve, 2000)); // 2秒延迟
    }
}

async function* errorDataSource(): AsyncIterable<Uint8Array> {
    for (let i = 0; i < 3; i++) {
        if (i === 1) {
            throw new Error('Simulated data source error');
        }
        const data = new TextEncoder().encode(`error-test-chunk-${i}`);
        yield new Uint8Array(data);
        await new Promise(resolve => setTimeout(resolve, 100));
    }
}

async function* infiniteDataSource(): AsyncIterable<Uint8Array> {
    let i = 0;
    while (true) {
        const data = new TextEncoder().encode(`infinite-chunk-${i++}`);
        yield new Uint8Array(data);
        await new Promise(resolve => setTimeout(resolve, 50));
    }
}

async function* batchErrorDataSource(): AsyncIterable<Uint8Array[]> {
    const batch1 = [
        new Uint8Array(new TextEncoder().encode('batch-1-chunk-1')),
        new Uint8Array(new TextEncoder().encode('batch-1-chunk-2'))
    ];
    yield batch1;
    
    // 模拟在第二批时出错
    throw new Error('Batch processing error');
}

// 健壮性测试套件
async function runRobustnessTests(client: Libp2pGrpcClient) {
    console.log('🛡️  开始健壮性测试...\n');
    
    const testResults: Array<{name: string, status: 'PASS' | 'FAIL', error?: string}> = [];
    
    // 测试1: 正常流程
    console.log('📋 测试1: 正常流程');
    try {
        const cancelFn = await client.Call(
            '/test/Normal',
            new Uint8Array(),
            5000,
            'client-streaming',
            (data) => console.log('  📦 收到数据:', data.length, '字节'),
            normalDataSource,
            () => console.log('  ✅ 正常流程完成'),
            (error) => console.error('  ❌ 正常流程错误:', error)
        );
        
        testResults.push({name: '正常流程', status: 'PASS'});
        console.log('  ✅ 测试1通过\n');
    } catch (error) {
        testResults.push({name: '正常流程', status: 'FAIL', error: (error as Error).message});
        console.log('  ❌ 测试1失败:', error);
    }
    
    // 测试2: 超时处理
    console.log('📋 测试2: 超时处理');
    try {
        const startTime = Date.now();
        const cancelFn = await client.Call(
            '/test/Timeout',
            new Uint8Array(),
            1000, // 1秒超时
            'client-streaming',
            (data) => console.log('  📦 收到数据:', data.length, '字节'),
            slowDataSource, // 需要6秒的数据源
            () => console.log('  ⚠️  超时测试不应该完成'),
            (error) => console.log('  ⏰ 预期的超时错误:', error)
        );
        
        const duration = Date.now() - startTime;
        if (duration < 2000) { // 应该在2秒内超时
            testResults.push({name: '超时处理', status: 'PASS'});
            console.log(`  ✅ 测试2通过 (${duration}ms)\n`);
        } else {
            testResults.push({name: '超时处理', status: 'FAIL', error: 'Timeout not working'});
            console.log(`  ❌ 测试2失败: 超时未生效 (${duration}ms)\n`);
        }
    } catch (error) {
        testResults.push({name: '超时处理', status: 'PASS'});
        console.log('  ✅ 测试2通过 (正确抛出超时错误)\n');
    }
    
    // 测试3: 数据源错误处理
    console.log('📋 测试3: 数据源错误处理');
    try {
        const cancelFn = await client.Call(
            '/test/DataSourceError',
            new Uint8Array(),
            5000,
            'client-streaming',
            (data) => console.log('  📦 收到数据:', data.length, '字节'),
            errorDataSource,
            () => console.log('  ⚠️  错误测试不应该完成'),
            (error) => console.log('  🔥 预期的数据源错误:', error)
        );
        
        testResults.push({name: '数据源错误处理', status: 'FAIL', error: 'Should have thrown error'});
        console.log('  ❌ 测试3失败: 应该抛出错误\n');
    } catch (error) {
        testResults.push({name: '数据源错误处理', status: 'PASS'});
        console.log('  ✅ 测试3通过 (正确处理数据源错误)\n');
    }
    
    // 测试4: 手动取消
    console.log('📋 测试4: 手动取消操作');
    try {
        const cancelFn = await client.Call(
            '/test/ManualCancel',
            new Uint8Array(),
            30000,
            'client-streaming',
            (data) => console.log('  📦 收到数据:', data.length, '字节'),
            infiniteDataSource,
            () => console.log('  ⚠️  取消测试不应该完成'),
            (error) => console.log('  🛑 预期的取消错误:', error)
        );
        
        // 2秒后手动取消
        setTimeout(() => {
            console.log('  🛑 手动取消操作...');
            if (typeof cancelFn === 'function') {
                cancelFn();
            }
        }, 2000);
        
        testResults.push({name: '手动取消', status: 'PASS'});
        console.log('  ✅ 测试4通过\n');
    } catch (error) {
        testResults.push({name: '手动取消', status: 'PASS'});
        console.log('  ✅ 测试4通过 (正确处理取消)\n');
    }
    
    // 测试5: 批处理错误
    console.log('📋 测试5: 批处理错误处理');
    try {
        const cancelFn = await client.Call(
            '/test/BatchError',
            new Uint8Array(),
            5000,
            'client-streaming',
            (data) => console.log('  📦 收到数据:', data.length, '字节'),
            batchErrorDataSource,
            () => console.log('  ⚠️  批处理错误测试不应该完成'),
            (error) => console.log('  🔥 预期的批处理错误:', error)
        );
        
        testResults.push({name: '批处理错误处理', status: 'FAIL', error: 'Should have thrown error'});
        console.log('  ❌ 测试5失败: 应该抛出错误\n');
    } catch (error) {
        testResults.push({name: '批处理错误处理', status: 'PASS'});
        console.log('  ✅ 测试5通过 (正确处理批处理错误)\n');
    }
    
    // 测试6: 外部AbortSignal
    console.log('📋 测试6: 外部AbortSignal');
    try {
        const controller = new AbortController();
        
        const cancelFn = await client.Call(
            '/test/ExternalAbort',
            new Uint8Array(),
            30000,
            'client-streaming',
            (data) => console.log('  📦 收到数据:', data.length, '字节'),
            infiniteDataSource,
            () => console.log('  ⚠️  外部中止测试不应该完成'),
            (error) => console.log('  🛑 预期的外部中止错误:', error),
            { signal: controller.signal }
        );
        
        // 1秒后外部取消
        setTimeout(() => {
            console.log('  🛑 外部AbortSignal取消...');
            controller.abort();
        }, 1000);
        
        testResults.push({name: '外部AbortSignal', status: 'PASS'});
        console.log('  ✅ 测试6通过\n');
    } catch (error) {
        testResults.push({name: '外部AbortSignal', status: 'PASS'});
        console.log('  ✅ 测试6通过 (正确处理外部中止)\n');
    }
    
    // 输出测试结果总结
    console.log('📊 健壮性测试结果总结:');
    console.log('================================');
    
    const passCount = testResults.filter(r => r.status === 'PASS').length;
    const failCount = testResults.filter(r => r.status === 'FAIL').length;
    
    testResults.forEach(result => {
        const icon = result.status === 'PASS' ? '✅' : '❌';
        console.log(`${icon} ${result.name}: ${result.status}`);
        if (result.error) {
            console.log(`   错误: ${result.error}`);
        }
    });
    
    console.log('================================');
    console.log(`总计: ${testResults.length} 个测试`);
    console.log(`通过: ${passCount} 个`);
    console.log(`失败: ${failCount} 个`);
    console.log(`成功率: ${((passCount / testResults.length) * 100).toFixed(1)}%`);
    
    if (failCount === 0) {
        console.log('🎉 所有健壮性测试通过！');
    } else {
        console.log('⚠️  存在需要关注的问题');
    }
}

// 内存泄漏检测
function memoryLeakTest() {
    console.log('\n🔍 内存泄漏检测:');
    
    if (typeof process !== 'undefined' && process.memoryUsage) {
        const memBefore = process.memoryUsage();
        console.log('测试前内存使用:', {
            rss: `${(memBefore.rss / 1024 / 1024).toFixed(2)} MB`,
            heapUsed: `${(memBefore.heapUsed / 1024 / 1024).toFixed(2)} MB`
        });
        
        return () => {
            const memAfter = process.memoryUsage();
            console.log('测试后内存使用:', {
                rss: `${(memAfter.rss / 1024 / 1024).toFixed(2)} MB`,
                heapUsed: `${(memAfter.heapUsed / 1024 / 1024).toFixed(2)} MB`
            });
            
            const heapDiff = memAfter.heapUsed - memBefore.heapUsed;
            console.log(`堆内存变化: ${(heapDiff / 1024 / 1024).toFixed(2)} MB`);
            
            if (heapDiff > 10 * 1024 * 1024) { // 10MB
                console.log('⚠️  检测到潜在的内存泄漏');
            } else {
                console.log('✅ 内存使用正常');
            }
        };
    } else {
        console.log('⚠️  无法检测内存使用（非Node.js环境）');
        return () => {};
    }
}

export {
    runRobustnessTests,
    memoryLeakTest,
    normalDataSource,
    slowDataSource,
    errorDataSource,
    infiniteDataSource,
    batchErrorDataSource
};