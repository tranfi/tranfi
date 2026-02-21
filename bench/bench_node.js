/**
 * bench_node.js — Benchmark tranfi-core (native C via N-API) vs online-stats (pure JS).
 *
 * Compares: mean, variance, stddev, min, max, median, histogram
 *
 * Usage: node bench/bench_node.js [rows]
 */

import { createRequire } from 'module'
import Stats from 'online-stats'

const require = createRequire(import.meta.url)
const native = require('../packages/js/build/Release/tranfi_napi.node')

const N = parseInt(process.argv[2] || '1000000', 10)

// ---- Generate data ----

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

function fmt(ms) { return ms.toFixed(1).padStart(8) + ' ms' }
function fmtRate(rows, ms) {
  return (rows / ms).toFixed(0).padStart(7) + ' Krows/s'
}

// ---- Benchmarks ----

function benchOnlineStats(values, label, factory, feedFn) {
  const t0 = performance.now()
  const stat = factory()
  for (let i = 0; i < values.length; i++) {
    feedFn ? feedFn(stat, values[i]) : stat(values[i])
  }
  const result = stat()
  const elapsed = performance.now() - t0
  console.log(`  ${label.padEnd(36)} ${fmt(elapsed)}  ${fmtRate(values.length, elapsed)}  result=${typeof result === 'number' ? result.toFixed(4) : result}`)
  return elapsed
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
    mean(v)
    variance(v)
    std(v)
    min(v)
    max(v)
    median(v)
  }

  const results = {
    mean: mean(), variance: variance(), std: std(),
    min: min(), max: max(), median: median()
  }
  const elapsed = performance.now() - t0
  console.log(`  ${'online-stats (all 6 combined)'.padEnd(36)} ${fmt(elapsed)}  ${fmtRate(values.length, elapsed)}`)
  return elapsed
}

function benchTranfiRaw(csv, n, steps, label) {
  const plan = JSON.stringify({ steps })

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
  console.log(`  ${label.padEnd(36)} ${fmt(elapsed)}  ${fmtRate(n, elapsed)}`)
  return elapsed
}

function benchTranfi(csv, n, statsList, label) {
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

  // Push in 64KB chunks
  const chunkSize = 64 * 1024
  for (let i = 0; i < buf.length; i += chunkSize) {
    native.push(handle, buf.subarray(i, Math.min(i + chunkSize, buf.length)))
  }
  native.finish(handle)

  const output = native.pull(handle, 0) // CHAN_MAIN
  native.free(handle)

  const elapsed = performance.now() - t0
  const outputStr = output.toString('utf-8').trim()
  const lines = outputStr.split('\n')
  const resultLine = lines.length > 1 ? lines[1] : ''
  console.log(`  ${label.padEnd(36)} ${fmt(elapsed)}  ${fmtRate(n, elapsed)}  ${resultLine.substring(0, 80)}`)
  return elapsed
}

function benchPureJSStats(values) {
  // Manual pure JS implementation of the same stats tranfi computes
  const t0 = performance.now()
  let count = 0, sum = 0, min = Infinity, max = -Infinity
  let wfMean = 0, wfM2 = 0 // Welford

  for (let i = 0; i < values.length; i++) {
    const v = values[i]
    count++
    sum += v
    if (v < min) min = v
    if (v > max) max = v
    const oldMean = wfMean
    wfMean += (v - wfMean) / count
    wfM2 += (v - wfMean) * (v - oldMean)
  }

  const avg = sum / count
  const variance = wfM2 / (count - 1)
  const stddev = Math.sqrt(variance)

  const elapsed = performance.now() - t0
  console.log(`  ${'pure JS (manual Welford)'.padEnd(36)} ${fmt(elapsed)}  ${fmtRate(values.length, elapsed)}  mean=${avg.toFixed(4)} var=${variance.toFixed(4)}`)
  return elapsed
}

// ---- Main ----

console.log(`Benchmark: tranfi-core (native C) vs online-stats (pure JS)`)
console.log(`Rows: ${(N / 1000).toFixed(0)}K\n`)

const values = genValues(N)
console.log('Generating CSV...')
const csv = genCSV(values)
console.log(`CSV size: ${(csv.length / 1024 / 1024).toFixed(1)} MB\n`)

// -- Warmup --
console.log('Warmup...')
benchTranfi(csv, N, ['count'], 'warmup')
benchOnlineStats(values, 'warmup', () => Stats.Mean())
console.log()

// -- CSV overhead baseline --
console.log('=== CSV overhead (tranfi passthrough) ===')
const csvOverhead = benchTranfiRaw(csv, N,
  [{ op: 'codec.csv.decode', args: {} }, { op: 'codec.csv.encode', args: {} }],
  'tranfi csv passthrough')

console.log()
console.log('=== Individual stats (online-stats, pre-parsed values) ===')
benchOnlineStats(values, 'online-stats Mean', () => Stats.Mean())
benchOnlineStats(values, 'online-stats Variance', () => Stats.Variance({ ddof: 1 }))
benchOnlineStats(values, 'online-stats Std', () => Stats.Std({ ddof: 1 }))
benchOnlineStats(values, 'online-stats Min', () => Stats.Min())
benchOnlineStats(values, 'online-stats Max', () => Stats.Max())
benchOnlineStats(values, 'online-stats Median', () => Stats.Median())

console.log()
console.log('=== Combined (online-stats, all 6) ===')
const jsAllTime = benchOnlineStatsAll(values)

console.log()
console.log('=== Pure JS baseline (manual Welford, pre-parsed) ===')
const pureJSTime = benchPureJSStats(values)

console.log()
console.log('=== tranfi-core (native C, includes CSV parse) ===')
benchTranfi(csv, N, ['count', 'sum', 'avg', 'min', 'max'], 'tranfi basic (count,sum,avg,min,max)')
benchTranfi(csv, N, ['var', 'stddev'], 'tranfi var,stddev')
benchTranfi(csv, N, ['median'], 'tranfi median (P2)')
benchTranfi(csv, N, ['median', 'p25', 'p75'], 'tranfi quantiles (p25,median,p75)')
benchTranfi(csv, N, ['skewness', 'kurtosis'], 'tranfi moments (skewness,kurtosis)')
benchTranfi(csv, N, ['distinct'], 'tranfi HyperLogLog (distinct)')
benchTranfi(csv, N, ['hist'], 'tranfi histogram (32 bins)')
benchTranfi(csv, N, ['sample'], 'tranfi reservoir sample (k=10)')

console.log()
console.log('=== Combined: all stats ===')
const tranfiDefaultTime = benchTranfi(csv, N,
  ['count', 'sum', 'avg', 'min', 'max', 'var', 'stddev', 'median'],
  'tranfi (default 8 stats)')
const tranfiAllTime = benchTranfi(csv, N,
  ['count', 'sum', 'avg', 'min', 'max', 'var', 'stddev', 'median', 'p25', 'p75',
   'skewness', 'kurtosis', 'distinct', 'hist', 'sample'],
  'tranfi (all 15 stats)')

console.log()
console.log('=== Summary ===')
const statsOnlyDefault = Math.max(0, tranfiDefaultTime - csvOverhead)
const statsOnlyAll = Math.max(0, tranfiAllTime - csvOverhead)
console.log(`  ${''.padEnd(40)}  Total     CSV parse   Stats only`)
console.log(`  ${'online-stats (6 stats, pre-parsed)'.padEnd(40)} ${fmt(jsAllTime)}                ${fmt(jsAllTime)}`)
console.log(`  ${'pure JS Welford (7 stats, pre-parsed)'.padEnd(40)} ${fmt(pureJSTime)}                ${fmt(pureJSTime)}`)
console.log(`  ${'tranfi (8 default stats, from CSV)'.padEnd(40)} ${fmt(tranfiDefaultTime)}  ~${fmt(csvOverhead)}   ~${fmt(statsOnlyDefault)}`)
console.log(`  ${'tranfi (all 15 stats, from CSV)'.padEnd(40)} ${fmt(tranfiAllTime)}  ~${fmt(csvOverhead)}   ~${fmt(statsOnlyAll)}`)
console.log()
console.log(`  Apples-to-apples (stats compute only):`)
console.log(`    tranfi 8 stats:  ~${fmt(statsOnlyDefault)} vs online-stats 6 stats: ${fmt(jsAllTime)} → tranfi is ${(jsAllTime / statsOnlyDefault).toFixed(1)}x faster`)
console.log(`    tranfi 15 stats: ~${fmt(statsOnlyAll)} vs online-stats 6 stats: ${fmt(jsAllTime)} → tranfi is ${(jsAllTime / statsOnlyAll).toFixed(1)}x faster`)
console.log()
console.log(`  End-to-end (CSV → stats → CSV output):`)
console.log(`    tranfi 8 stats:  ${fmt(tranfiDefaultTime)} vs online-stats: ${fmt(jsAllTime)} → ${(jsAllTime / tranfiDefaultTime).toFixed(1)}x`)
console.log(`    tranfi 15 stats: ${fmt(tranfiAllTime)} vs online-stats: ${fmt(jsAllTime)} → ${(jsAllTime / tranfiAllTime).toFixed(1)}x`)
