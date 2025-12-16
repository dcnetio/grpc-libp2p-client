import { runRobustnessTests, memoryLeakTest } from './tests/robustness-test';

/**
 * 运行健壮性测试的主程序
 */

// 模拟Libp2pGrpcClient实例
const mockClient = {
    async Call(
        path: string,
        requestData: Uint8Array,
        timeout: number,
        type: string,
        onData: (data: Uint8Array) => void,
        dataSourceCallback: any,
        onEnd: () => void,
        onError: (error: Error) => void,
        options?: { signal?: AbortSignal }
    ) {
        console.log(`🔧 模拟调用: ${path}`);
        
        // 模拟一些场景的行为
        if (path.includes('Timeout')) {
            return new Promise((resolve, reject) => {
                setTimeout(() => {
                    reject(new Error('Request timeout'));
                }, timeout);
            });
        }
        
        if (path.includes('Error') || path.includes('BatchError')) {
            throw new Error('Simulated call error');
        }
        
        // 模拟正常的取消函数返回
        return () => {
            console.log(`🛑 取消操作: ${path}`);
            onError(new Error('Operation cancelled'));
        };
    }
};

async function main() {
    console.log('🚀 启动健壮性测试程序\n');
    
    const checkMemory = memoryLeakTest();
    
    try {
        await runRobustnessTests(mockClient as any);
    } catch (error) {
        console.error('❌ 测试程序执行错误:', error);
    } finally {
        checkMemory();
    }
    
    console.log('\n✅ 健壮性测试程序完成');
}

// 处理未捕获的错误
process.on('unhandledRejection', (reason, promise) => {
    console.error('🔥 未处理的Promise拒绝:', reason);
});

process.on('uncaughtException', (error) => {
    console.error('🔥 未捕获的异常:', error);
    process.exit(1);
});

// 运行测试
main().catch(error => {
    console.error('❌ 主程序错误:', error);
    process.exit(1);
});