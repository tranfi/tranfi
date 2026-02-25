/**
 * tranfi-viz.js — Render step for Tranfi visualizations.
 *
 * Standalone vanilla JS (no ES modules). JSEE loads this via URL,
 * evals it, and extracts the `tranfiViz` function by name.
 *
 * Runs on main thread (render step, not worker).
 * Receives worker output, returns additional function-type outputs
 * for JSEE to render into DOM containers.
 */

function tranfiViz (result) {
  if (!result) return result

  // --- Detect stats table and parse structured data ---
  var profileData = null
  if (result.stats && result.stats.columns && result.stats.columns.length > 1) {
    profileData = parseStatsOutput(result.stats)
  }

  // --- Detect frequency data (2-col output, second is numeric) ---
  var chartData = null
  if (result.output && result.output.columns && result.output.columns.length === 2) {
    var allNum = true
    for (var i = 0; i < result.output.rows.length; i++) {
      if (isNaN(Number(result.output.rows[i][1]))) { allNum = false; break }
    }
    if (allNum && result.output.rows.length > 0) chartData = result.output
  }

  // --- Create render functions for JSEE function-type outputs ---
  if (profileData) {
    var pd = profileData
    result.profile = function (container) { renderProfile(container, pd) }
  }
  if (chartData) {
    var cd = chartData
    result.chart = function (container) { renderBarChart(container, cd) }
  }

  return result
}

// ============================================================
// Parsing
// ============================================================

function parseHist (str) {
  if (!str || typeof str !== 'string') return null
  var parts = str.split(':')
  if (parts.length < 3) return null
  var lo = Number(parts[0])
  var hi = Number(parts[1])
  var rest = parts.slice(2).join(':')
  var counts = rest.split(',').map(Number)
  if (counts.length < 2) return null
  return { lo: lo, hi: hi, counts: counts }
}

function parseSample (str) {
  if (!str || typeof str !== 'string') return null
  var vals = str.split(',').map(Number).filter(function (n) { return !isNaN(n) })
  return vals.length > 0 ? vals : null
}

function parseStatsOutput (table) {
  var cols = table.columns
  var rows = table.rows

  // Find special column indices
  var histIdx = cols.indexOf('hist')
  var sampleIdx = cols.indexOf('sample')

  var result = []
  for (var r = 0; r < rows.length; r++) {
    var row = rows[r]
    var entry = {
      name: row[0] || '',
      stats: {},
      hist: null,
      samples: null
    }

    for (var c = 1; c < cols.length; c++) {
      var colName = cols[c]
      if (c === histIdx) {
        entry.hist = parseHist(row[c])
      } else if (c === sampleIdx) {
        entry.samples = parseSample(row[c])
      } else {
        var val = row[c]
        if (val !== '' && val !== null && val !== undefined) {
          var num = Number(val)
          entry.stats[colName] = isNaN(num) ? val : num
        }
      }
    }
    result.push(entry)
  }
  return result
}

// ============================================================
// SVG helpers
// ============================================================

var SVG_NS = 'http://www.w3.org/2000/svg'

function svgEl (tag, attrs) {
  var el = document.createElementNS(SVG_NS, tag)
  if (attrs) {
    for (var k in attrs) {
      if (attrs.hasOwnProperty(k)) el.setAttribute(k, attrs[k])
    }
  }
  return el
}

function fmtNum (v) {
  if (v === null || v === undefined || v === '') return '-'
  var n = Number(v)
  if (isNaN(n)) return String(v)
  var a = Math.abs(n)
  if (a === 0) return '0'
  if (a >= 1e6) return n.toExponential(2)
  if (a >= 100) return n.toFixed(1)
  if (a >= 1) return n.toFixed(2)
  return n.toFixed(4)
}

function fmtInt (v) {
  var n = Math.round(Number(v))
  if (isNaN(n)) return '-'
  return n.toLocaleString()
}

// ============================================================
// Profile dashboard
// ============================================================

function renderProfile (container, columns) {
  container.style.cssText = 'display:flex;flex-wrap:wrap;gap:16px;padding:8px 0;'

  for (var i = 0; i < columns.length; i++) {
    var col = columns[i]
    var card = document.createElement('div')
    card.style.cssText = 'flex:1 1 280px;max-width:400px;border:1px solid #e0e0e0;border-radius:8px;padding:16px;background:#fff;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;font-size:13px;'

    // Column name
    var nameEl = document.createElement('div')
    nameEl.style.cssText = 'font-weight:600;font-size:15px;margin-bottom:8px;color:#333;'
    nameEl.textContent = col.name
    card.appendChild(nameEl)

    // Stats table
    var isNumeric = col.stats.min !== undefined || col.stats.avg !== undefined
    var statsEl = document.createElement('div')
    statsEl.style.cssText = 'display:grid;grid-template-columns:auto 1fr;gap:2px 12px;margin-bottom:8px;'

    var statKeys = Object.keys(col.stats)
    for (var s = 0; s < statKeys.length; s++) {
      var key = statKeys[s]
      var val = col.stats[key]

      var labelEl = document.createElement('span')
      labelEl.style.cssText = 'color:#888;font-size:11px;text-transform:uppercase;'
      labelEl.textContent = key

      var valEl = document.createElement('span')
      valEl.style.cssText = 'color:#333;font-variant-numeric:tabular-nums;'
      if (key === 'count' || key === 'distinct') {
        valEl.textContent = fmtInt(val)
      } else if (typeof val === 'number') {
        valEl.textContent = fmtNum(val)
      } else {
        valEl.textContent = String(val)
      }

      statsEl.appendChild(labelEl)
      statsEl.appendChild(valEl)
    }
    card.appendChild(statsEl)

    // Histogram
    if (col.hist) {
      var histEl = document.createElement('div')
      histEl.style.cssText = 'margin-top:4px;'
      renderHistogram(histEl, col.hist)
      card.appendChild(histEl)
    }

    // Sample dot strip
    if (col.samples && isNumeric) {
      var dotEl = document.createElement('div')
      dotEl.style.cssText = 'margin-top:4px;'
      var mn = col.stats.min !== undefined ? col.stats.min : Math.min.apply(null, col.samples)
      var mx = col.stats.max !== undefined ? col.stats.max : Math.max.apply(null, col.samples)
      renderDotStrip(dotEl, col.samples, mn, mx)
      card.appendChild(dotEl)
    }

    container.appendChild(card)
  }
}

// ============================================================
// Histogram SVG
// ============================================================

function renderHistogram (container, hist) {
  var W = 280, H = 80
  var padL = 0, padR = 0, padT = 4, padB = 16
  var chartW = W - padL - padR
  var chartH = H - padT - padB
  var nBins = hist.counts.length

  var maxCount = 0
  for (var i = 0; i < nBins; i++) {
    if (hist.counts[i] > maxCount) maxCount = hist.counts[i]
  }
  if (maxCount === 0) return

  var svg = svgEl('svg', { width: W, height: H, viewBox: '0 0 ' + W + ' ' + H })
  svg.style.display = 'block'

  var barW = chartW / nBins
  var binStep = (hist.hi - hist.lo) / nBins

  for (var i = 0; i < nBins; i++) {
    if (hist.counts[i] === 0) continue
    var barH = (hist.counts[i] / maxCount) * chartH
    var x = padL + i * barW
    var y = padT + chartH - barH

    var rect = svgEl('rect', {
      x: x + 0.5, y: y, width: Math.max(barW - 1, 1), height: barH,
      fill: '#4fc3f7', rx: 1
    })

    // Tooltip
    var binLo = hist.lo + i * binStep
    var binHi = binLo + binStep
    var title = svgEl('title')
    title.textContent = fmtNum(binLo) + ' – ' + fmtNum(binHi) + ': ' + hist.counts[i]
    rect.appendChild(title)

    svg.appendChild(rect)
  }

  // X-axis labels
  var loLabel = svgEl('text', { x: padL, y: H - 2, fill: '#999', 'font-size': 10, 'font-family': 'sans-serif' })
  loLabel.textContent = fmtNum(hist.lo)
  svg.appendChild(loLabel)

  var hiLabel = svgEl('text', { x: W - padR, y: H - 2, fill: '#999', 'font-size': 10, 'font-family': 'sans-serif', 'text-anchor': 'end' })
  hiLabel.textContent = fmtNum(hist.hi)
  svg.appendChild(hiLabel)

  container.appendChild(svg)
}

// ============================================================
// Dot strip (reservoir sample)
// ============================================================

function renderDotStrip (container, samples, min, max) {
  var W = 280, H = 20
  var range = max - min
  if (range <= 0) return

  var svg = svgEl('svg', { width: W, height: H, viewBox: '0 0 ' + W + ' ' + H })
  svg.style.display = 'block'

  // Axis line
  svg.appendChild(svgEl('line', { x1: 0, y1: H / 2, x2: W, y2: H / 2, stroke: '#e0e0e0', 'stroke-width': 1 }))

  // Dots
  for (var i = 0; i < samples.length; i++) {
    var x = ((samples[i] - min) / range) * (W - 8) + 4
    svg.appendChild(svgEl('circle', {
      cx: x, cy: H / 2, r: 3,
      fill: '#66bb6a', opacity: 0.7
    }))
  }

  container.appendChild(svg)
}

// ============================================================
// Bar chart (frequency data)
// ============================================================

function renderBarChart (container, tableData) {
  container.style.cssText = 'padding:8px 0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;font-size:13px;'

  // Sort by value descending
  var rows = tableData.rows.slice().sort(function (a, b) {
    return Number(b[1]) - Number(a[1])
  })

  // Limit to 20 bars
  var maxBars = 20
  var otherCount = 0
  if (rows.length > maxBars) {
    for (var i = maxBars; i < rows.length; i++) {
      otherCount += Number(rows[i][1])
    }
    rows = rows.slice(0, maxBars)
  }

  var maxVal = 0
  for (var i = 0; i < rows.length; i++) {
    var v = Number(rows[i][1])
    if (v > maxVal) maxVal = v
  }
  if (maxVal === 0) return

  var maxBarWidth = 240

  for (var i = 0; i < rows.length; i++) {
    var val = Number(rows[i][1])
    var row = document.createElement('div')
    row.style.cssText = 'display:flex;align-items:center;gap:8px;margin-bottom:3px;'

    var label = document.createElement('span')
    label.style.cssText = 'width:120px;text-align:right;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#555;flex-shrink:0;'
    label.textContent = rows[i][0]
    label.title = rows[i][0]

    var barWrap = document.createElement('div')
    barWrap.style.cssText = 'flex:1;display:flex;align-items:center;gap:6px;'

    var bar = document.createElement('div')
    var w = Math.max((val / maxVal) * maxBarWidth, 2)
    bar.style.cssText = 'height:16px;border-radius:2px;background:#4fc3f7;width:' + w + 'px;'

    var count = document.createElement('span')
    count.style.cssText = 'color:#888;font-size:12px;font-variant-numeric:tabular-nums;'
    count.textContent = fmtInt(val)

    barWrap.appendChild(bar)
    barWrap.appendChild(count)
    row.appendChild(label)
    row.appendChild(barWrap)
    container.appendChild(row)
  }

  // "Other" row
  if (otherCount > 0) {
    var row = document.createElement('div')
    row.style.cssText = 'display:flex;align-items:center;gap:8px;margin-top:4px;'

    var label = document.createElement('span')
    label.style.cssText = 'width:120px;text-align:right;color:#999;font-style:italic;flex-shrink:0;'
    label.textContent = 'other'

    var count = document.createElement('span')
    count.style.cssText = 'color:#999;font-size:12px;'
    count.textContent = fmtInt(otherCount)

    row.appendChild(label)
    row.appendChild(count)
    container.appendChild(row)
  }
}

// ============================================================
// Box plot (when quantile stats are present)
// ============================================================

function renderBoxPlots (container, columns) {
  container.style.cssText = 'padding:8px 0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;font-size:13px;'

  // Filter to columns with five-number summary
  var eligible = []
  for (var i = 0; i < columns.length; i++) {
    var s = columns[i].stats
    if (s.min !== undefined && s.p25 !== undefined && s.median !== undefined && s.p75 !== undefined && s.max !== undefined) {
      eligible.push(columns[i])
    }
  }
  if (eligible.length === 0) return

  // Global range for consistent scaling
  var globalMin = Infinity, globalMax = -Infinity
  for (var i = 0; i < eligible.length; i++) {
    if (eligible[i].stats.min < globalMin) globalMin = eligible[i].stats.min
    if (eligible[i].stats.max > globalMax) globalMax = eligible[i].stats.max
  }
  var range = globalMax - globalMin
  if (range <= 0) return

  var W = 280, rowH = 28, padL = 80

  for (var i = 0; i < eligible.length; i++) {
    var col = eligible[i]
    var s = col.stats
    var rowEl = document.createElement('div')
    rowEl.style.cssText = 'display:flex;align-items:center;gap:8px;margin-bottom:4px;'

    var label = document.createElement('span')
    label.style.cssText = 'width:' + padL + 'px;text-align:right;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#555;flex-shrink:0;'
    label.textContent = col.name

    var svg = svgEl('svg', { width: W, height: rowH, viewBox: '0 0 ' + W + ' ' + rowH })
    svg.style.display = 'block'

    var scale = function (v) { return ((v - globalMin) / range) * (W - 8) + 4 }
    var cy = rowH / 2

    // Whisker line (min to max)
    svg.appendChild(svgEl('line', { x1: scale(s.min), y1: cy, x2: scale(s.max), y2: cy, stroke: '#999', 'stroke-width': 1 }))

    // Box (p25 to p75)
    var bx = scale(s.p25)
    var bw = scale(s.p75) - bx
    svg.appendChild(svgEl('rect', { x: bx, y: cy - 8, width: Math.max(bw, 1), height: 16, fill: '#4fc3f7', rx: 2, opacity: 0.6 }))

    // Median line
    svg.appendChild(svgEl('line', { x1: scale(s.median), y1: cy - 8, x2: scale(s.median), y2: cy + 8, stroke: '#1565c0', 'stroke-width': 2 }))

    // Min/max caps
    svg.appendChild(svgEl('line', { x1: scale(s.min), y1: cy - 4, x2: scale(s.min), y2: cy + 4, stroke: '#999', 'stroke-width': 1 }))
    svg.appendChild(svgEl('line', { x1: scale(s.max), y1: cy - 4, x2: scale(s.max), y2: cy + 4, stroke: '#999', 'stroke-width': 1 }))

    rowEl.appendChild(label)
    rowEl.appendChild(svg)
    container.appendChild(rowEl)
  }
}
