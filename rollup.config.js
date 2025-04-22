import resolve from '@rollup/plugin-node-resolve';  
import commonjs from '@rollup/plugin-commonjs';  
import typescript from '@rollup/plugin-typescript';  
import json from '@rollup/plugin-json';  
// import { terser } from 'rollup-plugin-terser';  
import dts from 'rollup-plugin-dts';  
import pkg from './package.json' assert { type: 'json' }; // 使用 .mjs 时  

// 获取依赖列表作为外部模块  
const external = [  
  ...Object.keys(pkg.dependencies || {}),  
  ...Object.keys(pkg.peerDependencies || {})  
];  

export default [  
  // 主要的 ESM 和 CJS 构建  
  {  
    input: 'src/index.ts', // 入口文件路径  
    output: [  
      {  
        file: pkg.module, // 例如 'dist/index.esm.js'  
        format: 'esm',  
        sourcemap: true  
      },  
      {  
        file: pkg.main, // 例如 'dist/index.cjs.js'  
        format: 'cjs',  
        sourcemap: true  
      }  
    ],  
    external, // 外部依赖，不会被打包进最终文件  
    plugins: [  
      resolve({  
        preferBuiltins: true, // 优先使用 Node.js 内置模块  
        // 为难以解析的 Node.js 模块提供替代实现  
        browser: true,  
        mainFields: ['browser', 'module', 'main']  
      }),  
      commonjs({ 
        // 有些包可能需要特殊处理  
        transformMixedEsModules: true,  
        // 针对特定包的额外配置  
        namedExports: {  
          // 'it-pushable': ['pushable'],  
          // 添加其他需要特殊处理的包  
        }  
      }), // 转换 CommonJS 模块  
      json(), // 支持导入 JSON 文件  
      typescript({  
        tsconfig: './tsconfig.json',  
        declaration: false // 类型声明将在单独的构建中生成  
      }),  
      // 可选: 压缩代码 (生产版本)  
      // terser()  
    ]  
  },  
  // 类型定义文件构建  
  {  
    input: 'src/index.ts',  
    output: {  
      file: pkg.types, // 例如 'dist/index.d.ts'  
      format: 'es'  
    },  
    plugins: [dts()],  
    external  
  },
  // dc-http2/stream
  {  
    input: 'src/dc-http2/stream.ts', // 入口文件路径  
    output: [  
      {  
        file: 'dist/dc-http2/stream.esm.js',
        format: 'esm',  
        sourcemap: true  
      },  
      {  
        file: 'dist/dc-http2/stream.cjs.js',
        format: 'cjs',  
        sourcemap: true  
      }  
    ],  
    external, // 外部依赖，不会被打包进最终文件  
    plugins: [  
      resolve({  
        preferBuiltins: true, // 优先使用 Node.js 内置模块  
        // 为难以解析的 Node.js 模块提供替代实现  
        browser: true,  
        mainFields: ['browser', 'module', 'main']  
      }),  
      commonjs({ 
        // 有些包可能需要特殊处理  
        transformMixedEsModules: true,  
        // 针对特定包的额外配置  
        namedExports: {  
          // 'it-pushable': ['pushable'],  
          // 添加其他需要特殊处理的包  
        }  
      }), // 转换 CommonJS 模块  
      json(), // 支持导入 JSON 文件  
      typescript({  
        tsconfig: './tsconfig.json',  
        declaration: false // 类型声明将在单独的构建中生成  
      }),  
      // 可选: 压缩代码 (生产版本)  
      // terser()  
    ]  
  },  
  // 类型定义文件构建  
  {  
    input: 'src/dc-http2/stream.ts', // 入口文件路径  
    output: {  
      file: 'dist/dc-http2/stream.d.ts',
      format: 'es'  
    },  
    plugins: [dts()],  
    external  
  },
  // dc-http2/frame
  {  
    input: 'src/dc-http2/frame.ts', // 入口文件路径  
    output: [  
      {  
        file: 'dist/dc-http2/frame.esm.js',
        format: 'esm',  
        sourcemap: true  
      },  
      {  
        file: 'dist/dc-http2/frame.cjs.js',
        format: 'cjs',  
        sourcemap: true  
      }  
    ],  
    external, // 外部依赖，不会被打包进最终文件  
    plugins: [  
      resolve({  
        preferBuiltins: true, // 优先使用 Node.js 内置模块  
        // 为难以解析的 Node.js 模块提供替代实现  
        browser: true,  
        mainFields: ['browser', 'module', 'main']  
      }),  
      commonjs({ 
        // 有些包可能需要特殊处理  
        transformMixedEsModules: true,  
        // 针对特定包的额外配置  
        namedExports: {  
          // 'it-pushable': ['pushable'],  
          // 添加其他需要特殊处理的包  
        }  
      }), // 转换 CommonJS 模块  
      json(), // 支持导入 JSON 文件  
      typescript({  
        tsconfig: './tsconfig.json',  
        declaration: false // 类型声明将在单独的构建中生成  
      }),  
      // 可选: 压缩代码 (生产版本)  
      // terser()  
    ]  
  },  
  // 类型定义文件构建  
  {  
    input: 'src/dc-http2/frame.ts', // 入口文件路径  
    output: {  
      file: 'dist/dc-http2/frame.d.ts',
      format: 'es'  
    },  
    plugins: [dts()],  
    external  
  },
  // dc-http2/parser
  {  
    input: 'src/dc-http2/parser.ts', // 入口文件路径  
    output: [  
      {  
        file: 'dist/dc-http2/parser.esm.js',
        format: 'esm',  
        sourcemap: true  
      },  
      {  
        file: 'dist/dc-http2/parser.cjs.js',
        format: 'cjs',  
        sourcemap: true  
      }  
    ],  
    external, // 外部依赖，不会被打包进最终文件  
    plugins: [  
      resolve({  
        preferBuiltins: true, // 优先使用 Node.js 内置模块  
        // 为难以解析的 Node.js 模块提供替代实现  
        browser: true,  
        mainFields: ['browser', 'module', 'main']  
      }),  
      commonjs({ 
        // 有些包可能需要特殊处理  
        transformMixedEsModules: true,  
        // 针对特定包的额外配置  
        namedExports: {  
          // 'it-pushable': ['pushable'],  
          // 添加其他需要特殊处理的包  
        }  
      }), // 转换 CommonJS 模块  
      json(), // 支持导入 JSON 文件  
      typescript({  
        tsconfig: './tsconfig.json',  
        declaration: false // 类型声明将在单独的构建中生成  
      }),  
      // 可选: 压缩代码 (生产版本)  
      // terser()  
    ]  
  },  
  // 类型定义文件构建  
  {  
    input: 'src/dc-http2/parser.ts', // 入口文件路径  
    output: {  
      file: 'dist/dc-http2/parser.d.ts',
      format: 'es'  
    },  
    plugins: [dts()],  
    external  
  },
  // dc-http2/hpack
  {  
    input: 'src/dc-http2/hpack.ts', // 入口文件路径  
    output: [  
      {  
        file: 'dist/dc-http2/hpack.esm.js',
        format: 'esm',  
        sourcemap: true  
      },  
      {  
        file: 'dist/dc-http2/hpack.cjs.js',
        format: 'cjs',  
        sourcemap: true  
      }  
    ],  
    external, // 外部依赖，不会被打包进最终文件  
    plugins: [  
      resolve({  
        preferBuiltins: true, // 优先使用 Node.js 内置模块  
        // 为难以解析的 Node.js 模块提供替代实现  
        browser: true,  
        mainFields: ['browser', 'module', 'main']  
      }),  
      commonjs({ 
        // 有些包可能需要特殊处理  
        transformMixedEsModules: true,  
        // 针对特定包的额外配置  
        namedExports: {  
          // 'it-pushable': ['pushable'],  
          // 添加其他需要特殊处理的包  
        }  
      }), // 转换 CommonJS 模块  
      json(), // 支持导入 JSON 文件  
      typescript({  
        tsconfig: './tsconfig.json',  
        declaration: false // 类型声明将在单独的构建中生成  
      }),  
      // 可选: 压缩代码 (生产版本)  
      // terser()  
    ]  
  },  
  // 类型定义文件构建  
  {  
    input: 'src/dc-http2/hpack.ts', // 入口文件路径  
    output: {  
      file: 'dist/dc-http2/hpack.d.ts',
      format: 'es'  
    },  
    plugins: [dts()],  
    external  
  },
];  