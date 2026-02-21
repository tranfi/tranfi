/**
 * tranfi-runner.js — JSEE-compatible model function for tranfi WASM pipeline.
 *
 * Loaded in JSEE's web worker via importScripts.
 * Expects createTranfi global from imports (tranfi_core.js).
 *
 * Receives: { file: ChunkedReader|Uint8Array, dsl: string }
 * Returns:  { output: htmlString, stats: htmlString }
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

function pipelineFinish (handle) {
  const rc = wasm.ccall('wasm_pipeline_finish', 'number', ['number'], [handle])
  if (rc !== 0) {
    const err = wasm.ccall('wasm_pipeline_error', 'string', ['number'], [handle])
    throw new Error(err || 'Finish failed')
  }
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

function esc (s) {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;')
}

function csvToHtml (csv, label) {
  if (!csv || !csv.trim()) return ''
  const lines = csv.trim().split('\n')
  const header = lines[0].split(',')
  const body = lines.slice(1)
  const maxRows = 500
  const truncated = body.length > maxRows
  const displayBody = truncated ? body.slice(0, maxRows) : body

  let h = `<div style="font-size:12px;color:#888;margin-bottom:6px">`
  h += `${esc(label)} (${body.length} rows${truncated ? ', showing first ' + maxRows : ''})</div>`
  h += '<div style="overflow-x:auto;max-height:500px;overflow-y:auto">'
  h += '<table style="width:100%;border-collapse:collapse;font-size:13px;font-family:monospace">'
  h += '<thead><tr>'
  for (const col of header) {
    h += '<th style="padding:6px 12px;text-align:left;border-bottom:2px solid #ddd;'
    h += 'background:#f8f8f8;font-weight:600;position:sticky;top:0">'
    h += esc(col.trim()) + '</th>'
  }
  h += '</tr></thead><tbody>'
  for (const row of displayBody) {
    h += '<tr>'
    for (const cell of row.split(',')) {
      h += '<td style="padding:4px 12px;text-align:left;border-bottom:1px solid #eee">'
      h += esc(cell.trim()) + '</td>'
    }
    h += '</tr>'
  }
  h += '</tbody></table></div>'
  return h
}

// JSEE model function — set on worker global
self.tranfiRunner = async function tranfiRunner (inputs, ctx) {
  await initWasm()

  const dsl = inputs.dsl
  const file = inputs.file

  if (!dsl) throw new Error('No DSL pipeline provided')
  if (!file) throw new Error('No file provided')

  ctx.log('Compiling DSL:', dsl)
  const planJson = compileDsl(dsl)
  const handle = pipelineCreate(planJson)

  try {
    let bytesProcessed = 0
    const totalBytes = file.size || 0

    if (file[Symbol.asyncIterator]) {
      // JSEE ChunkedReader — async iterable of Uint8Array chunks
      for await (const chunk of file) {
        pipelinePush(handle, chunk)
        bytesProcessed += chunk.length
        if (totalBytes > 0) {
          ctx.progress(Math.round(bytesProcessed / totalBytes * 100))
        }
      }
    } else if (file instanceof Uint8Array || file instanceof ArrayBuffer) {
      // Raw bytes fallback
      const data = file instanceof Uint8Array ? file : new Uint8Array(file)
      const CHUNK = 64 * 1024
      for (let i = 0; i < data.length; i += CHUNK) {
        pipelinePush(handle, data.subarray(i, Math.min(i + CHUNK, data.length)))
        ctx.progress(Math.round(Math.min(i + CHUNK, data.length) / data.length * 100))
      }
    } else {
      throw new Error('Unsupported file input type')
    }

    pipelineFinish(handle)

    const output = pipelinePullAll(handle, 0)
    const errors = pipelinePullAll(handle, 1)
    const stats = pipelinePullAll(handle, 2)

    if (errors && errors.trim()) {
      throw new Error(errors.trim())
    }

    const result = {}
    if (output) result.output = csvToHtml(output, 'Output')
    if (stats) result.stats = csvToHtml(stats, 'Stats')

    ctx.log('Pipeline complete')
    return result
  } finally {
    pipelineFree(handle)
    ctx.progress(0)
  }
}
