/**
 * bench_memory.js — Memory consumption benchmark.
 *
 * Tests whether memory stays constant (O(1)) as data scales
 * for streaming stats algorithms.
 *
 * Usage: node bench/bench_memory.js
 */

import { createRequire } from 'module'
import Stats from 'online-stats'

const require = createRequire(import.meta.url)
const native = require('../packages/js/build/Release/tranfi_napi.node')

function fmt(bytes) {
  if (bytes < 1024) return bytes + ' B'
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB'
  return (bytes / 1024 / 1024).toFixed(1) + ' MB'
}

function getMemMB() {
  global.gc && global.gc()
  return process.memoryUsage().rss / 1024 / 1024
}

function genCSVChunk(startRow, nRows) {
  let csv = ''
  if (startRow === 0) csv = 'val\n'
  for (let i = startRow; i < startRow + nRows; i++) {
    csv += (Math.sin(i * 0.001) * 100 + 50 + (i % 37) * 0.7).toFixed(6) + '\n'
  }
  return csv
}

// ---- Test 1: online-stats memory scaling ----
function benchOnlineStatsMemory(sizes) {
  console.log('=== online-stats memory scaling ===')
  console.log(`  ${'Rows'.padStart(12)}  ${'RSS'.padStart(10)}  ${'Delta'.padStart(10)}  ${'Time'.padStart(10)}`)

  for (const n of sizes) {
    global.gc && global.gc()
    const memBefore = process.memoryUsage().rss

    const mean = Stats.Mean()
    const variance = Stats.Variance({ ddof: 1 })
    const std = Stats.Std({ ddof: 1 })
    const min = Stats.Min()
    const max = Stats.Max()
    const median = Stats.Median()

    const t0 = performance.now()
    for (let i = 0; i < n; i++) {
      const v = Math.sin(i * 0.001) * 100 + 50 + (i % 37) * 0.7
      mean(v); variance(v); std(v); min(v); max(v); median(v)
    }
    const elapsed = performance.now() - t0

    // Force results to be computed
    mean(); variance(); std(); min(); max(); median()

    global.gc && global.gc()
    const memAfter = process.memoryUsage().rss
    const delta = memAfter - memBefore

    console.log(`  ${n.toLocaleString().padStart(12)}  ${fmt(memAfter).padStart(10)}  ${(delta > 0 ? '+' : '') + fmt(delta).padStart(9)}  ${elapsed.toFixed(0).padStart(7)} ms`)
  }
}

// ---- Test 2: tranfi stats memory scaling ----
function benchTranfiMemory(sizes, statsList, label) {
  console.log(`\n=== tranfi memory scaling (${label}) ===`)
  console.log(`  ${'Rows'.padStart(12)}  ${'RSS'.padStart(10)}  ${'Delta'.padStart(10)}  ${'Time'.padStart(10)}`)

  for (const n of sizes) {
    global.gc && global.gc()
    const memBefore = process.memoryUsage().rss

    const plan = JSON.stringify({
      steps: [
        { op: 'codec.csv.decode', args: {} },
        { op: 'stats', args: { stats: statsList } },
        { op: 'codec.csv.encode', args: {} }
      ]
    })

    const t0 = performance.now()
    const handle = native.createPipeline(plan)

    // Push data in chunks to avoid building entire CSV string in memory
    const chunkRows = 50000
    for (let start = 0; start < n; start += chunkRows) {
      const rows = Math.min(chunkRows, n - start)
      const chunk = genCSVChunk(start, rows)
      native.push(handle, Buffer.from(chunk, 'utf-8'))
    }

    native.finish(handle)
    const output = native.pull(handle, 0)
    native.free(handle)
    const elapsed = performance.now() - t0

    global.gc && global.gc()
    const memAfter = process.memoryUsage().rss
    const delta = memAfter - memBefore

    console.log(`  ${n.toLocaleString().padStart(12)}  ${fmt(memAfter).padStart(10)}  ${(delta > 0 ? '+' : '') + fmt(delta).padStart(9)}  ${elapsed.toFixed(0).padStart(7)} ms`)
  }
}

// ---- Test 3: pure JS Welford memory scaling ----
function benchPureJSMemory(sizes) {
  console.log('\n=== pure JS (Welford) memory scaling ===')
  console.log(`  ${'Rows'.padStart(12)}  ${'RSS'.padStart(10)}  ${'Delta'.padStart(10)}  ${'Time'.padStart(10)}`)

  for (const n of sizes) {
    global.gc && global.gc()
    const memBefore = process.memoryUsage().rss

    let count = 0, sum = 0, min = Infinity, max = -Infinity
    let wfMean = 0, wfM2 = 0

    const t0 = performance.now()
    for (let i = 0; i < n; i++) {
      const v = Math.sin(i * 0.001) * 100 + 50 + (i % 37) * 0.7
      count++; sum += v
      if (v < min) min = v; if (v > max) max = v
      const oldMean = wfMean
      wfMean += (v - wfMean) / count
      wfM2 += (v - wfMean) * (v - oldMean)
    }
    const elapsed = performance.now() - t0

    global.gc && global.gc()
    const memAfter = process.memoryUsage().rss
    const delta = memAfter - memBefore

    console.log(`  ${n.toLocaleString().padStart(12)}  ${fmt(memAfter).padStart(10)}  ${(delta > 0 ? '+' : '') + fmt(delta).padStart(9)}  ${elapsed.toFixed(0).padStart(7)} ms`)
  }
}

// ---- Main ----

const sizes = [100_000, 500_000, 1_000_000, 5_000_000, 10_000_000]

console.log('Memory scaling benchmark')
console.log(`Sizes: ${sizes.map(n => (n/1000000).toFixed(1) + 'M').join(', ')}`)
console.log(`GC exposed: ${typeof global.gc === 'function' ? 'yes' : 'no (run with --expose-gc for accurate results)'}`)
console.log()

benchPureJSMemory(sizes)
benchOnlineStatsMemory(sizes)
benchTranfiMemory(sizes, ['count', 'sum', 'avg', 'min', 'max', 'var', 'stddev', 'median'], 'default 8 stats')
benchTranfiMemory(sizes, ['count', 'sum', 'avg', 'min', 'max', 'var', 'stddev', 'median', 'p25', 'p75', 'skewness', 'kurtosis', 'distinct', 'hist', 'sample'], 'all 15 stats')

console.log('\n=== What to look for ===')
console.log('  O(1) memory: RSS delta stays constant as rows increase (streaming)')
console.log('  O(n) memory: RSS delta grows linearly with rows (buffering)')
