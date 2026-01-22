
import resolve from '@rollup/plugin-node-resolve';
import commonjs from '@rollup/plugin-commonjs';
import typescript from '@rollup/plugin-typescript';
import json from '@rollup/plugin-json';
import terser from '@rollup/plugin-terser';
import { visualizer } from 'rollup-plugin-visualizer';

import dts from 'rollup-plugin-dts';
import fs from 'node:fs';

// 兼容较旧 Node 版本，避免 JSON import assertions 导致的语法错误
const pkg = JSON.parse(fs.readFileSync(new URL('./package.json', import.meta.url), 'utf8'));

const tsconfig = {
  tsconfig: './tsconfig.json',
  declaration: false
};

// 外部依赖（这些将不会被打包进最终文件）
const external = [
  ...Object.keys(pkg.dependencies || {}),
  ...Object.keys(pkg.peerDependencies || {})
];

// 全局变量名
const GLOBAL_NAME = 'GrpcLibp2pClient';

// 定义所有需要构建的模块
const modules = [
  {
    input: 'src/index.ts',
    name: 'index',
    outputs: {
      esm: './dist/index.esm.js',
      cjs: './dist/index.cjs.js',
      types: './dist/index.d.ts'
    }
  },
  {
    input: 'src/dc-http2/stream.ts',
    name: 'dc-http2/stream',
    outputs: {
      esm: './dist/dc-http2/stream.esm.js',
      cjs: './dist/dc-http2/stream.cjs.js',
      types: './dist/dc-http2/stream.d.ts'
    }
  },
  {
    input: 'src/dc-http2/frame.ts',
    name: 'dc-http2/frame',
    outputs: {
      esm: './dist/dc-http2/frame.esm.js',
      cjs: './dist/dc-http2/frame.cjs.js',
      types: './dist/dc-http2/frame.d.ts'
    }
  },
  {
    input: 'src/dc-http2/parser.ts',
    name: 'dc-http2/parser',
    outputs: {
      esm: './dist/dc-http2/parser.esm.js',
      cjs: './dist/dc-http2/parser.cjs.js',
      types: './dist/dc-http2/parser.d.ts'
    }
  },
  {
    input: 'src/dc-http2/hpack.ts',
    name: 'dc-http2/hpack',
    outputs: {
      esm: './dist/dc-http2/hpack.esm.js',
      cjs: './dist/dc-http2/hpack.cjs.js',
      types: './dist/dc-http2/hpack.d.ts'
    }
  }
];

// 生成构建配置的函数
function createBuildConfig(module) {
  return {
    input: module.input,
    output: [
      {
        file: module.outputs.esm,
        format: 'esm',
        sourcemap: true
      },
      {
        file: module.outputs.cjs,
        format: 'cjs',
        sourcemap: true
      }
    ],
    external,
    plugins: [
      resolve({
        preferBuiltins: false,
        browser: true
      }),
      commonjs({
        transformMixedEsModules: true
      }),
      json(),
      typescript(tsconfig)
    ]
  };
}

// 生成类型定义配置的函数
function createTypesConfig(module) {
  return {
    input: module.input,
    output: {
      file: module.outputs.types,
      format: 'es'
    },
    plugins: [dts()],
    external
  };
}

// 生成所有模块的构建配置
const buildConfigs = modules.map(createBuildConfig);
const typesConfigs = modules.map(createTypesConfig);

// 主入口的 UMD 构建（通常只有主入口需要 UMD）
const umdConfigs = [
  // 未压缩版本
  {
    input: 'src/index.ts',
    output: {
      file: 'dist/grpc.js',
      format: 'umd',
      name: GLOBAL_NAME,
      sourcemap: true,
      exports: 'named',
      intro: `var global = typeof window !== 'undefined' ? window : this;`
    },
    plugins: [
      resolve({
        browser: true,
        preferBuiltins: false
      }),
      commonjs({
        transformMixedEsModules: true
      }),
      json(),
      typescript({
        ...tsconfig,
        target: 'es5'
      })
    ]
  },
  
  // 压缩版本
  {
    input: 'src/index.ts',
    output: {
      file: 'dist/grpc.min.js',
      format: 'umd',
      name: GLOBAL_NAME,
      sourcemap: true,
      exports: 'named',
      intro: `var global = typeof window !== 'undefined' ? window : this;`,
      compact: true,
      minifyInternalExports: true
    },
    plugins: [
      resolve({
        browser: true,
        preferBuiltins: false
      }),
      commonjs({
        transformMixedEsModules: true
      }),
      json(),
      typescript({
        ...tsconfig,
        target: 'es5'
      }),
      terser(),
      visualizer({
        filename: 'dist/stats.html',
        gzipSize: true,
        brotliSize: true
      })
    ]
  }
];

// 导出所有配置
export default [
  ...buildConfigs,
  ...typesConfigs,
  ...umdConfigs
];
