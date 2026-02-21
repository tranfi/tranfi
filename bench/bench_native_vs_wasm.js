/**
 * bench_native_vs_wasm.js — Compare native N-API vs WASM backend.
 *
 * Usage: node bench/bench_native_vs_wasm.js [rows]
 */

import { createRequire } from 'module'

const ROWS = parseInt(process.argv[2] || '100000', 10)
const RUNS = 3

// --- Generate test data ---
function generateCSV(n) {
  const lines = ['name,age,score,city']
  const names = ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 'Hank']
  const cities = ['NY', 'LA', 'Chicago', 'Houston', 'Phoenix']
  for (let i = 0; i < n; i++) {
    lines.push(`${names[i % names.length]},${20 + (i % 60)},${(i * 7 + 13) % 100},${cities[i % cities.length]}`)
  }
  return lines.join('\n') + '\n'
}

// --- Load native backend ---
let nativeBackend = null
try {
  const require = createRequire(import.meta.url)
  nativeBackend = require('../js/build/Release/tranfi_napi.node')
} catch {
  console.log('Native N-API not available (skipping)\n')
}

// --- Load WASM backend ---
let wasmBackend = null
try {
  const { default: createTranfi } = await import('../js/wasm/tranfi_core.js')
  const wasm = await createTranfi()
  wasmBackend = {
    compileDsl(dsl) {
      const len = wasm.lengthBytesUTF8(dsl)
      const ptr = wasm._malloc(len + 1)
      wasm.stringToUTF8(dsl, ptr, len + 1)
      const resultPtr = wasm.ccall('wasm_compile_dsl', 'number', ['number', 'number'], [ptr, len])
      wasm._free(ptr)
      if (!resultPtr) throw new Error('DSL parse failed')
      const json = wasm.UTF8ToString(resultPtr)
      wasm._free(resultPtr)
      return json
    },
    createPipeline(planJson) {
      const len = wasm.lengthBytesUTF8(planJson)
      const ptr = wasm._malloc(len + 1)
      wasm.stringToUTF8(planJson, ptr, len + 1)
      const handle = wasm.ccall('wasm_pipeline_create', 'number', ['number', 'number'], [ptr, len])
      wasm._free(ptr)
      if (handle < 0) throw new Error('Failed to create pipeline')
      return handle
    },
    push(handle, buffer) {
      const len = buffer.length
      const ptr = wasm._malloc(len)
      wasm.HEAPU8.set(buffer, ptr)
      wasm.ccall('wasm_pipeline_push', 'number', ['number', 'number', 'number'], [handle, ptr, len])
      wasm._free(ptr)
    },
    finish(handle) {
      wasm.ccall('wasm_pipeline_finish', 'number', ['number'], [handle])
    },
    pull(handle, channel) {
      const bufSize = 65536
      const ptr = wasm._malloc(bufSize)
      const chunks = []
      for (;;) {
        const n = wasm.ccall('wasm_pipeline_pull', 'number',
          ['number', 'number', 'number', 'number'], [handle, channel, ptr, bufSize])
        if (n === 0) break
        chunks.push(new Uint8Array(wasm.HEAPU8.buffer, ptr, n).slice())
      }
      wasm._free(ptr)
      const total = chunks.reduce((s, c) => s + c.length, 0)
      const result = Buffer.alloc(total)
      let offset = 0
      for (const c of chunks) { result.set(c, offset); offset += c.length }
      return result
    },
    free(handle) {
      wasm.ccall('wasm_pipeline_free', null, ['number'], [handle])
    }
  }
} catch (e) {
  console.log(`WASM not available: ${e.message}\n`)
}

// --- Run a pipeline ---
function runPipeline(backend, planJson, inputBuf) {
  const handle = backend.createPipeline(planJson)
  const chunkSize = 64 * 1024
  for (let i = 0; i < inputBuf.length; i += chunkSize) {
    backend.push(handle, inputBuf.subarray(i, Math.min(i + chunkSize, inputBuf.length)))
  }
  backend.finish(handle)
  const output = backend.pull(handle, 0)
  backend.free(handle)
  return output
}

// --- Benchmark ---
function bench(backend, planJson, inputBuf) {
  // Warmup
  runPipeline(backend, planJson, inputBuf)

  const times = []
  for (let i = 0; i < RUNS; i++) {
    const start = performance.now()
    runPipeline(backend, planJson, inputBuf)
    times.push(performance.now() - start)
  }
  return Math.min(...times)
}

// --- Main ---
const backends = []
if (nativeBackend) backends.push(['Native', nativeBackend])
if (wasmBackend) backends.push(['WASM', wasmBackend])

if (backends.length === 0) {
  console.log('No backends available!')
  process.exit(1)
}

console.log(`Native vs WASM: ${ROWS.toLocaleString()} rows, best of ${RUNS}\n`)

const csv = generateCSV(ROWS)
const csvBuf = Buffer.from(csv, 'utf-8')
console.log(`Input: ${(csvBuf.length / 1024 / 1024).toFixed(1)} MB\n`)

const tasks = [
  ['passthrough',   'csv | csv'],
  ['filter 50%',    'csv | filter "col(age) > 50" | csv'],
  ['select 2 cols', 'csv | select name,age | csv'],
  ['head 1000',     'csv | head 1000 | csv'],
  ['derive',        'csv | derive bonus=col(score)*2 | csv'],
  ['sort',          'csv | sort -score | csv'],
  ['unique',        'csv | unique name,city | csv'],
  ['stats',         'csv | stats | csv'],
  ['group-agg',     'csv | group-agg city sum:score:total | csv'],
  ['frequency',     'csv | frequency city | csv'],
]

// Compile DSLs using first available backend
const compiler = backends[0][1]
const plans = {}
for (const [label, dsl] of tasks) {
  plans[label] = compiler.compileDsl(dsl)
}

// Header
const nameW = 16, colW = 12
let header = 'Task'.padEnd(nameW)
for (const [name] of backends) header += name.padStart(colW)
if (backends.length === 2) header += 'Ratio'.padStart(colW)
console.log(header)
console.log('-'.repeat(header.length))

// Run benchmarks
for (const [label] of tasks) {
  let line = label.padEnd(nameW)
  const times = []
  for (const [, backend] of backends) {
    const ms = bench(backend, plans[label], csvBuf)
    times.push(ms)
    line += `${ms.toFixed(0)} ms`.padStart(colW)
  }
  if (times.length === 2) {
    const ratio = times[1] / times[0]
    line += `${ratio.toFixed(1)}x`.padStart(colW)
  }
  console.log(line)
}
