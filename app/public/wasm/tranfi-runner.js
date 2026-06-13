/**
 * tranfi-runner.js — JSEE-compatible model function for tranfi WASM pipeline.
 *
 * Loaded in JSEE's web worker via importScripts.
 * Expects createTranfi global from imports (tranfi_core.js).
 *
 * Receives: { file: ChunkedReader|Uint8Array, dsl: string, preview_rows?: number, collect_output?: boolean }
 * Returns:  bounded output preview table plus stats side-channel table
 */

let wasm = null

async function initWasm () {
  if (wasm) return
  wasm = await createTranfi({
    locateFile: (path) => `/wasm/${path}`
  })
}

function compileDsl (dsl) {
  const len = wasm.lengthBytesUTF8(dsl)
  const ptr = wasm._malloc(len + 1)
  wasm.stringToUTF8(dsl, ptr, len + 1)
  const resultPtr = wasm.ccall('wasm_compile_dsl', 'number', ['number', 'number'], [ptr, len])
  wasm._free(ptr)
  if (!resultPtr) {
    const err = wasm.ccall('wasm_pipeline_error', 'string', ['number'], [-1])
    throw new Error(err || 'DSL parse failed')
  }
  const json = wasm.UTF8ToString(resultPtr)
  wasm._free(resultPtr)
  return json
}

function pipelineCreate (planJson) {
  const len = wasm.lengthBytesUTF8(planJson)
  const ptr = wasm._malloc(len + 1)
  wasm.stringToUTF8(planJson, ptr, len + 1)
  const handle = wasm.ccall('wasm_pipeline_create', 'number', ['number', 'number'], [ptr, len])
  wasm._free(ptr)
  if (handle < 0) {
    const err = wasm.ccall('wasm_pipeline_error', 'string', ['number'], [handle])
    throw new Error(err || 'Pipeline creation failed')
  }
  return handle
}

function pipelinePush (handle, data) {
  const ptr = wasm._malloc(data.length)
  wasm.HEAPU8.set(data, ptr)
  const rc = wasm.ccall('wasm_pipeline_push', 'number', ['number', 'number', 'number'], [handle, ptr, data.length])
  wasm._free(ptr)
  if (rc !== 0) {
    const err = wasm.ccall('wasm_pipeline_error', 'string', ['number'], [handle])
    throw new Error(err || 'Push failed')
  }
}

function pipelineFinishStep (handle) {
  const rc = wasm.ccall('wasm_pipeline_finish_step', 'number', ['number'], [handle])
  if (rc < 0) {
    const err = wasm.ccall('wasm_pipeline_error', 'string', ['number'], [handle])
    throw new Error(err || 'Finish failed')
  }
  return rc === 1
}

function pipelinePullChunk (handle, channel, bufSize) {
  bufSize = bufSize || 65536
  const ptr = wasm._malloc(bufSize)
  const n = wasm.ccall('wasm_pipeline_pull', 'number',
    ['number', 'number', 'number', 'number'], [handle, channel, ptr, bufSize])
  const result = new Uint8Array(wasm.HEAPU8.buffer, ptr, n).slice()
  wasm._free(ptr)
  return result
}

function pipelinePullAll (handle, channel) {
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
  if (chunks.length === 0) return ''
  const total = chunks.reduce((s, c) => s + c.length, 0)
  const result = new Uint8Array(total)
  let offset = 0
  for (const c of chunks) { result.set(c, offset); offset += c.length }
  return new TextDecoder().decode(result)
}

function pipelineFree (handle) {
  wasm.ccall('wasm_pipeline_free', null, ['number'], [handle])
}

function csvParseLine (line) {
  var fields = []
  var i = 0, len = line.length
  while (i < len) {
    if (line[i] === '"') {
      i++ // skip opening quote
      var start = i
      while (i < len) {
        if (line[i] === '"') {
          if (i + 1 < len && line[i + 1] === '"') { i += 2; continue }
          break
        }
        i++
      }
      fields.push(line.slice(start, i).replace(/""/g, '"'))
      if (i < len) i++ // skip closing quote
      if (i < len && line[i] === ',') i++ // skip comma
    } else {
      var start = i
      while (i < len && line[i] !== ',') i++
      fields.push(line.slice(start, i).trim())
      if (i < len) i++ // skip comma
    }
  }
  return fields
}

function csvToTable (csv, label) {
  if (!csv || !csv.trim()) return null
  var lines = csv.trim().split('\n')
  var columns = csvParseLine(lines[0])
  var rows = []
  for (var r = 1; r < lines.length; r++) {
    if (lines[r].trim()) rows.push(csvParseLine(lines[r]))
  }
  return { columns: columns, rows: rows, label: label }
}

function createCsvPreview (maxRows) {
  maxRows = Number(maxRows)
  if (!isFinite(maxRows) || maxRows < 0) maxRows = 200
  maxRows = Math.floor(maxRows)
  var decoder = new TextDecoder()
  var pending = ''
  var columns = null
  var rows = []
  var rowsSeen = 0
  var bytesSeen = 0
  var maxPendingChars = 1024 * 1024
  var truncatedRecord = false

  function consumeLine (line) {
    if (line.endsWith('\r')) line = line.slice(0, -1)
    if (!columns) {
      if (!line.trim()) return
      columns = csvParseLine(line)
      return
    }
    if (!line.trim()) return
    rowsSeen++
    if (rows.length < maxRows) rows.push(csvParseLine(line))
  }

  function feed (chunk) {
    if (!chunk || chunk.length === 0) return
    bytesSeen += chunk.length
    pending += decoder.decode(chunk, { stream: true })
    if (pending.length > maxPendingChars) {
      pending = pending.slice(0, maxPendingChars)
      truncatedRecord = true
    }
    var start = 0
    for (;;) {
      var idx = pending.indexOf('\n', start)
      if (idx < 0) break
      consumeLine(pending.slice(start, idx))
      start = idx + 1
    }
    pending = pending.slice(start)
  }

  function finish () {
    var tail = decoder.decode()
    if (tail) pending += tail
    if (pending) consumeLine(pending)
    if (!columns) return null
    var label = 'Output preview'
    if (rowsSeen > rows.length) label += ' (first ' + rows.length + ' of ' + rowsSeen + ' rows)'
    return {
      columns: columns,
      rows: rows,
      label: label,
      preview: true,
      rows_seen: rowsSeen,
      rows_shown: rows.length,
      truncated: rowsSeen > rows.length || truncatedRecord,
      bytes_seen: bytesSeen
    }
  }

  return { feed: feed, finish: finish }
}


// JSEE model function — set on worker global
self.tranfiRunner = async function tranfiRunner (inputs, ctx) {
  await initWasm()

  const dsl = inputs.dsl
  const file = inputs.file
  const previewRows = inputs.preview_rows == null ? 200 : inputs.preview_rows
  const collectOutput = inputs.collect_output === true || inputs.collect_output === 'true'

  if (!dsl) throw new Error('No DSL pipeline provided')
  if (!file) throw new Error('No file provided')

  ctx.log('Compiling DSL:', dsl)
  const planJson = compileDsl(dsl)
  const handle = pipelineCreate(planJson)
  const preview = createCsvPreview(previewRows)
  const outputChunks = collectOutput ? [] : null
  const CHUNK = 64 * 1024

  function drainMain () {
    for (;;) {
      const chunk = pipelinePullChunk(handle, 0, CHUNK)
      if (chunk.length === 0) break
      preview.feed(chunk)
      if (outputChunks) outputChunks.push(chunk)
    }
  }

  try {
    let bytesProcessed = 0
    const totalBytes = file.size || 0

    if (file[Symbol.asyncIterator]) {
      // JSEE ChunkedReader — async iterable of Uint8Array chunks.
      for await (const chunk of file) {
        pipelinePush(handle, chunk)
        drainMain()
        bytesProcessed += chunk.length
        if (totalBytes > 0) {
          ctx.progress(Math.round(bytesProcessed / totalBytes * 100))
        }
      }
    } else if (file instanceof Uint8Array || file instanceof ArrayBuffer) {
      // Raw bytes fallback.
      const data = file instanceof Uint8Array ? file : new Uint8Array(file)
      for (let i = 0; i < data.length; i += CHUNK) {
        pipelinePush(handle, data.subarray(i, Math.min(i + CHUNK, data.length)))
        drainMain()
        ctx.progress(Math.round(Math.min(i + CHUNK, data.length) / data.length * 100))
      }
    } else if (file.stream && typeof file.stream === 'function') {
      const reader = file.stream().getReader()
      try {
        for (;;) {
          const next = await reader.read()
          if (next.done) break
          const chunk = next.value instanceof Uint8Array ? next.value : new Uint8Array(next.value)
          pipelinePush(handle, chunk)
          drainMain()
          bytesProcessed += chunk.length
          if (totalBytes > 0) ctx.progress(Math.round(bytesProcessed / totalBytes * 100))
        }
      } finally {
        reader.releaseLock()
      }
    } else {
      throw new Error('Unsupported file input type')
    }

    for (;;) {
      const done = pipelineFinishStep(handle)
      drainMain()
      if (done) break
    }

    const errors = pipelinePullAll(handle, 1)
    const stats = pipelinePullAll(handle, 2)

    if (errors && errors.trim()) {
      throw new Error(errors.trim())
    }

    const result = {}
    const previewTable = preview.finish()
    if (previewTable) result.output = previewTable
    if (collectOutput && outputChunks.length > 0) {
      const total = outputChunks.reduce((sum, c) => sum + c.length, 0)
      const all = new Uint8Array(total)
      let offset = 0
      for (const c of outputChunks) { all.set(c, offset); offset += c.length }
      result.output_full = new TextDecoder().decode(all)
    }
    if (stats) result.stats = csvToTable(stats, 'Stats')

    ctx.log('Pipeline complete')
    return result
  } finally {
    pipelineFree(handle)
    ctx.progress(0)
  }
}
