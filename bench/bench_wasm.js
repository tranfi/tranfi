/**
 * bench_wasm.js — 3-way benchmark: native C (N-API) vs WASM vs online-stats.
 *
 * Usage: node bench/bench_wasm.js [rows]
 */

import { createRequire } from 'module'
import Stats from 'online-stats'

const require = createRequire(import.meta.url)
const native = require('../packages/js/build/Release/tranfi_napi.node')
const createTranfi = require('../packages/js/wasm/tranfi_core.js')

const N = parseInt(process.argv[2] || '1000000', 10)

// ---- Helpers ----

function fmt(ms) { return ms.toFixed(1).padStart(8) + ' ms' }
function fmtRate(rows, ms) { return (rows / ms).toFixed(0).padStart(7) + ' Krows/s' }

function genValues(n) {
  const values = new Float64Array(n)
  for (let i = 0; i < n; i++) {
    values[i] = Math.sin(i * 0.001) * 100 + 50 + (i % 37) * 0.7
  }
  return values
}

function genCSV(values) {
  let csv = 'val\n'
  for (let i = 0; i < values.length; i++) {
    csv += values[i].toFixed(6) + '\n'
  }
  return csv
}

// ---- Benchmark runners ----

function benchNative(csv, n, statsList, label) {
  const plan = JSON.stringify({
    steps: [
      { op: 'codec.csv.decode', args: {} },
      { op: 'stats', args: { stats: statsList } },
      { op: 'codec.csv.encode', args: {} }
    ]
  })

  const t0 = performance.now()
  const handle = native.createPipeline(plan)
  const buf = Buffer.from(csv, 'utf-8')
  const chunkSize = 64 * 1024
  for (let i = 0; i < buf.length; i += chunkSize) {
    native.push(handle, buf.subarray(i, Math.min(i + chunkSize, buf.length)))
  }
  native.finish(handle)
  const output = native.pull(handle, 0)
  native.free(handle)

  const elapsed = performance.now() - t0
  const line = output.toString('utf-8').trim().split('\n')[1] || ''
  console.log(`  ${label.padEnd(44)} ${fmt(elapsed)}  ${fmtRate(n, elapsed)}`)
  return { elapsed, result: line }
}

function benchNativePassthrough(csv, n) {
  const plan = JSON.stringify({
    steps: [
      { op: 'codec.csv.decode', args: {} },
      { op: 'codec.csv.encode', args: {} }
    ]
  })
  const t0 = performance.now()
  const handle = native.createPipeline(plan)
  const buf = Buffer.from(csv, 'utf-8')
  const chunkSize = 64 * 1024
  for (let i = 0; i < buf.length; i += chunkSize) {
    native.push(handle, buf.subarray(i, Math.min(i + chunkSize, buf.length)))
  }
  native.finish(handle)
  native.pull(handle, 0)
  native.free(handle)
  return performance.now() - t0
}

async function benchWasm(wasm, csv, n, statsList, label) {
  const plan = JSON.stringify({
    steps: [
      { op: 'codec.csv.decode', args: {} },
      { op: 'stats', args: { stats: statsList } },
      { op: 'codec.csv.encode', args: {} }
    ]
  })

  const t0 = performance.now()

  // Create pipeline
  const planLen = wasm.lengthBytesUTF8(plan)
  const planPtr = wasm._malloc(planLen + 1)
  wasm.stringToUTF8(plan, planPtr, planLen + 1)
  const handle = wasm.ccall('wasm_pipeline_create', 'number', ['number', 'number'], [planPtr, planLen])
  wasm._free(planPtr)

  if (handle < 0) {
    const err = wasm.ccall('wasm_pipeline_error', 'string', ['number'], [handle])
    throw new Error(`WASM create failed: ${err}`)
  }

  // Push CSV in chunks
  const buf = Buffer.from(csv, 'utf-8')
  const chunkSize = 64 * 1024
  for (let i = 0; i < buf.length; i += chunkSize) {
    const chunk = buf.subarray(i, Math.min(i + chunkSize, buf.length))
    const ptr = wasm._malloc(chunk.length)
    wasm.HEAPU8.set(chunk, ptr)
    wasm.ccall('wasm_pipeline_push', 'number', ['number', 'number', 'number'], [handle, ptr, chunk.length])
    wasm._free(ptr)
  }

  wasm.ccall('wasm_pipeline_finish', 'number', ['number'], [handle])

  // Pull output
  const outBufSize = 65536
  const outPtr = wasm._malloc(outBufSize)
  const chunks = []
  for (;;) {
    const n = wasm.ccall('wasm_pipeline_pull', 'number',
      ['number', 'number', 'number', 'number'], [handle, 0, outPtr, outBufSize])
    if (n === 0) break
    chunks.push(new Uint8Array(wasm.HEAPU8.buffer, outPtr, n).slice())
  }
  wasm._free(outPtr)
  wasm.ccall('wasm_pipeline_free', null, ['number'], [handle])

  const elapsed = performance.now() - t0
  const total = chunks.reduce((s, c) => s + c.length, 0)
  const result = Buffer.alloc(total)
  let offset = 0
  for (const c of chunks) { result.set(c, offset); offset += c.length }
  const line = result.toString('utf-8').trim().split('\n')[1] || ''
  console.log(`  ${label.padEnd(44)} ${fmt(elapsed)}  ${fmtRate(n, elapsed)}`)
  return { elapsed, result: line }
}

async function benchWasmPassthrough(wasm, csv, n) {
  const plan = JSON.stringify({
    steps: [
      { op: 'codec.csv.decode', args: {} },
      { op: 'codec.csv.encode', args: {} }
    ]
  })
  const t0 = performance.now()
  const planLen = wasm.lengthBytesUTF8(plan)
  const planPtr = wasm._malloc(planLen + 1)
  wasm.stringToUTF8(plan, planPtr, planLen + 1)
  const handle = wasm.ccall('wasm_pipeline_create', 'number', ['number', 'number'], [planPtr, planLen])
  wasm._free(planPtr)
  const buf = Buffer.from(csv, 'utf-8')
  const chunkSize = 64 * 1024
  for (let i = 0; i < buf.length; i += chunkSize) {
    const chunk = buf.subarray(i, Math.min(i + chunkSize, buf.length))
    const ptr = wasm._malloc(chunk.length)
    wasm.HEAPU8.set(chunk, ptr)
    wasm.ccall('wasm_pipeline_push', 'number', ['number', 'number', 'number'], [handle, ptr, chunk.length])
    wasm._free(ptr)
  }
  wasm.ccall('wasm_pipeline_finish', 'number', ['number'], [handle])
  const outPtr = wasm._malloc(65536)
  for (;;) {
    const n = wasm.ccall('wasm_pipeline_pull', 'number',
      ['number', 'number', 'number', 'number'], [handle, 0, outPtr, 65536])
    if (n === 0) break
  }
  wasm._free(outPtr)
  wasm.ccall('wasm_pipeline_free', null, ['number'], [handle])
  return performance.now() - t0
}

function benchOnlineStatsAll(values) {
  const t0 = performance.now()
  const mean = Stats.Mean()
  const variance = Stats.Variance({ ddof: 1 })
  const std = Stats.Std({ ddof: 1 })
  const min = Stats.Min()
  const max = Stats.Max()
  const median = Stats.Median()

  for (let i = 0; i < values.length; i++) {
    const v = values[i]
    mean(v); variance(v); std(v); min(v); max(v); median(v)
  }

  const results = { mean: mean(), variance: variance(), std: std(), min: min(), max: max(), median: median() }
  const elapsed = performance.now() - t0
  console.log(`  ${'online-stats (6 stats, pre-parsed)'.padEnd(44)} ${fmt(elapsed)}  ${fmtRate(values.length, elapsed)}`)
  return { elapsed, results }
}

// ---- Main ----

async function main() {
  console.log('Initializing WASM...')
  const wasm = await createTranfi()
  const wasmVer = wasm.ccall('wasm_version', 'string', [], [])
  console.log(`WASM version: ${wasmVer}`)
  console.log(`Native version: ${native.version()}`)
  console.log()

  console.log(`Benchmark: Native C (N-API) vs WASM vs online-stats`)
  console.log(`Rows: ${(N / 1000).toFixed(0)}K\n`)

  const values = genValues(N)
  console.log('Generating CSV...')
  const csv = genCSV(values)
  console.log(`CSV size: ${(csv.length / 1024 / 1024).toFixed(1)} MB\n`)

  const defaultStats = ['count', 'sum', 'avg', 'min', 'max', 'var', 'stddev', 'median']
  const allStats = ['count', 'sum', 'avg', 'min', 'max', 'var', 'stddev', 'median',
    'p25', 'p75', 'skewness', 'kurtosis', 'distinct', 'hist', 'sample']

  // Warmup
  console.log('Warmup...')
  benchNative(csv, N, ['count'], 'native warmup')
  await benchWasm(wasm, csv, N, ['count'], 'wasm warmup')
  benchOnlineStatsAll(values)
  console.log()

  // CSV passthrough baseline
  console.log('=== CSV passthrough (decode + encode, no stats) ===')
  const nativeCSVOverhead = benchNativePassthrough(csv, N)
  console.log(`  ${'native'.padEnd(44)} ${fmt(nativeCSVOverhead)}`)
  const wasmCSVOverhead = await benchWasmPassthrough(wasm, csv, N)
  console.log(`  ${'wasm'.padEnd(44)} ${fmt(wasmCSVOverhead)}`)
  console.log(`  WASM / native CSV overhead: ${(wasmCSVOverhead / nativeCSVOverhead).toFixed(2)}x`)
  console.log()

  // Default 8 stats
  console.log('=== Default 8 stats (count,sum,avg,min,max,var,stddev,median) ===')
  const nDef = benchNative(csv, N, defaultStats, 'native (N-API)')
  const wDef = await benchWasm(wasm, csv, N, defaultStats, 'wasm')
  const jsDef = benchOnlineStatsAll(values)
  console.log()

  // All 15 stats
  console.log('=== All 15 stats ===')
  const nAll = benchNative(csv, N, allStats, 'native (N-API)')
  const wAll = await benchWasm(wasm, csv, N, allStats, 'wasm')
  console.log()

  // Individual stats breakdown (native vs wasm)
  console.log('=== Individual stats: native vs wasm ===')
  const individuals = [
    [['count', 'sum', 'avg', 'min', 'max'], 'basic (count,sum,avg,min,max)'],
    [['var', 'stddev'], 'Welford (var,stddev)'],
    [['median'], 'P2 quantile (median)'],
    [['median', 'p25', 'p75'], 'quantiles (p25,median,p75)'],
    [['skewness', 'kurtosis'], 'moments (skewness,kurtosis)'],
    [['distinct'], 'HyperLogLog (distinct)'],
    [['hist'], 'histogram (32 bins)'],
    [['sample'], 'reservoir sample (k=10)']
  ]
  for (const [stats, label] of individuals) {
    const nt = benchNative(csv, N, stats, `native: ${label}`)
    const wt = await benchWasm(wasm, csv, N, stats, `wasm:   ${label}`)
    console.log(`  ${''.padEnd(44)} wasm/native: ${(wt.elapsed / nt.elapsed).toFixed(2)}x`)
    console.log()
  }

  // Summary
  console.log('=== Summary ===')
  console.log()
  const nDefStats = Math.max(0, nDef.elapsed - nativeCSVOverhead)
  const wDefStats = Math.max(0, wDef.elapsed - wasmCSVOverhead)
  const nAllStats = Math.max(0, nAll.elapsed - nativeCSVOverhead)
  const wAllStats = Math.max(0, wAll.elapsed - wasmCSVOverhead)

  console.log(`  ${''.padEnd(40)}  End-to-end   CSV parse    Stats only`)
  console.log(`  ${'online-stats (6 stats, pre-parsed)'.padEnd(40)} ${fmt(jsDef.elapsed)}                  ${fmt(jsDef.elapsed)}`)
  console.log(`  ${'native N-API (8 stats, from CSV)'.padEnd(40)} ${fmt(nDef.elapsed)}   ~${fmt(nativeCSVOverhead)}   ~${fmt(nDefStats)}`)
  console.log(`  ${'WASM (8 stats, from CSV)'.padEnd(40)} ${fmt(wDef.elapsed)}   ~${fmt(wasmCSVOverhead)}   ~${fmt(wDefStats)}`)
  console.log(`  ${'native N-API (15 stats, from CSV)'.padEnd(40)} ${fmt(nAll.elapsed)}   ~${fmt(nativeCSVOverhead)}   ~${fmt(nAllStats)}`)
  console.log(`  ${'WASM (15 stats, from CSV)'.padEnd(40)} ${fmt(wAll.elapsed)}   ~${fmt(wasmCSVOverhead)}   ~${fmt(wAllStats)}`)
  console.log()

  console.log('  Ratios (stats compute only, CSV overhead removed):')
  console.log(`    WASM vs native (8 stats):   ${(wDefStats / nDefStats).toFixed(2)}x`)
  console.log(`    WASM vs native (15 stats):  ${(wAllStats / nAllStats).toFixed(2)}x`)
  console.log(`    native vs online-stats:     ${(jsDef.elapsed / nDefStats).toFixed(1)}x faster`)
  console.log(`    WASM vs online-stats:       ${(jsDef.elapsed / wDefStats).toFixed(1)}x faster`)
  console.log()

  console.log('  End-to-end (including CSV parse):')
  console.log(`    WASM / native (8 stats):    ${(wDef.elapsed / nDef.elapsed).toFixed(2)}x`)
  console.log(`    WASM / native (15 stats):   ${(wAll.elapsed / nAll.elapsed).toFixed(2)}x`)
  console.log(`    native vs online-stats:     ${(jsDef.elapsed / nDef.elapsed).toFixed(1)}x`)
  console.log(`    WASM vs online-stats:       ${(jsDef.elapsed / wDef.elapsed).toFixed(1)}x`)
}

main().catch(err => { console.error(err); process.exit(1) })
