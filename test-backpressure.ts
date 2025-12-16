// 背压优化测试
import { StreamWriter } from './src/dc-http2/stream'

// 模拟 sink
const mockSink = {
  async *[Symbol.asyncIterator]() {
    // 模拟慢速消费
    yield new Uint8Array(0)
  },
  return: async () => {}
}

async function testBackpressureOptimization() {
  console.log('开始背压优化测试...')
  
  const writer = new StreamWriter(mockSink, {
    chunkSize: 1024,
    bufferSize: 5 * 1024, // 5KB 缓冲区用于测试
    retries: 3
  })

  // 监听背压事件
  writer.addEventListener('backpressure', (event) => {
    console.log('背压事件:', event.detail)
  })

  writer.addEventListener('progress', (event) => {
    console.log('进度:', event.detail)
  })

  const startTime = Date.now()
  const testData = new Uint8Array(20 * 1024) // 20KB 数据
  testData.fill(65) // 填充 'A'

  try {
    // 快速连续写入多个块来测试背压
    const promises = []
    for (let i = 0; i < 10; i++) {
      const chunk = testData.slice(i * 2048, (i + 1) * 2048)
      promises.push(writer.write(chunk.buffer))
    }
    
    await Promise.all(promises)
    
    const endTime = Date.now()
    console.log(`写入完成，耗时: ${endTime - startTime}ms`)
    console.log(`当前队列大小: ${writer.queueSize} bytes`)
    
    await writer.end()
    
  } catch (error) {
    console.error('测试失败:', error)
  }
}

// 运行测试
if (require.main === module) {
  testBackpressureOptimization()
    .then(() => console.log('测试完成'))
    .catch(console.error)
}

export { testBackpressureOptimization }