/**
 * tranfi-worker.js — Web Worker that runs WASM pipelines.
 *
 * Messages:
 *   { type: 'init' }                      → load WASM module
 *   { type: 'compile', dsl }              → compile DSL to plan JSON
 *   { type: 'run', dsl, chunks }          → create pipeline, push data, return output
 *   { type: 'run-stream', dsl, id }       → create pipeline, wait for push-chunk messages
 *   { type: 'push-chunk', id, chunk }     → push a chunk to running pipeline
 *   { type: 'finish', id }                → finish and pull results
 *
 * Responses:
 *   { type: 'ready' }
 *   { type: 'compiled', json }
 *   { type: 'output', output, stats, errors }
 *   { type: 'progress', bytesProcessed }
 *   { type: 'error', message }
 */

let wasm = null

async function initWasm() {
  if (wasm) return
  // Load emscripten CJS/UMD glue — sets global createTranfi
  importScripts('/wasm/tranfi_core.js')
  wasm = await createTranfi({
    locateFile: (path) => `/wasm/${path}`
  })
}

function compileDsl(dsl) {
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

function createPipeline(planJson) {
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

function pushData(handle, data) {
  const ptr = wasm._malloc(data.length)
  wasm.HEAPU8.set(data, ptr)
  const rc = wasm.ccall('wasm_pipeline_push', 'number', ['number', 'number', 'number'], [handle, ptr, data.length])
  wasm._free(ptr)
  if (rc !== 0) {
    const err = wasm.ccall('wasm_pipeline_error', 'string', ['number'], [handle])
    throw new Error(err || 'Push failed')
  }
}

function finishPipeline(handle) {
  const rc = wasm.ccall('wasm_pipeline_finish', 'number', ['number'], [handle])
  if (rc !== 0) {
    const err = wasm.ccall('wasm_pipeline_error', 'string', ['number'], [handle])
    throw new Error(err || 'Finish failed')
  }
}

function pullAll(handle, channel) {
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
  if (chunks.length === 0) return new Uint8Array(0)
  const total = chunks.reduce((s, c) => s + c.length, 0)
  const result = new Uint8Array(total)
  let offset = 0
  for (const c of chunks) { result.set(c, offset); offset += c.length }
  return result
}

function freePipeline(handle) {
  wasm.ccall('wasm_pipeline_free', null, ['number'], [handle])
}

// Streaming pipelines keyed by id
const activePipelines = new Map()

self.onmessage = async ({ data: msg }) => {
  try {
    switch (msg.type) {
      case 'init': {
        await initWasm()
        const ver = wasm.ccall('wasm_version', 'string', [], [])
        self.postMessage({ type: 'ready', version: ver })
        break
      }

      case 'compile': {
        await initWasm()
        const json = compileDsl(msg.dsl)
        self.postMessage({ type: 'compiled', json })
        break
      }

      case 'run': {
        await initWasm()
        const planJson = compileDsl(msg.dsl)
        const handle = createPipeline(planJson)
        try {
          // Push all chunks
          const CHUNK_SIZE = 64 * 1024
          const data = msg.data instanceof Uint8Array ? msg.data : new Uint8Array(msg.data)
          for (let i = 0; i < data.length; i += CHUNK_SIZE) {
            pushData(handle, data.subarray(i, Math.min(i + CHUNK_SIZE, data.length)))
            self.postMessage({ type: 'progress', bytesProcessed: Math.min(i + CHUNK_SIZE, data.length) })
          }
          finishPipeline(handle)
          const output = pullAll(handle, 0)
          const errors = pullAll(handle, 1)
          const stats = pullAll(handle, 2)
          self.postMessage(
            { type: 'output', output, stats, errors },
            [output.buffer, stats.buffer, errors.buffer]
          )
        } finally {
          freePipeline(handle)
        }
        break
      }

      case 'run-stream': {
        await initWasm()
        const planJson = compileDsl(msg.dsl)
        const handle = createPipeline(planJson)
        activePipelines.set(msg.id, handle)
        self.postMessage({ type: 'stream-ready', id: msg.id })
        break
      }

      case 'push-chunk': {
        const handle = activePipelines.get(msg.id)
        if (handle == null) throw new Error('No active pipeline for id: ' + msg.id)
        const chunk = msg.chunk instanceof Uint8Array ? msg.chunk : new Uint8Array(msg.chunk)
        pushData(handle, chunk)
        self.postMessage({ type: 'progress', id: msg.id, bytesProcessed: msg.totalBytes || 0 })
        break
      }

      case 'finish': {
        const handle = activePipelines.get(msg.id)
        if (handle == null) throw new Error('No active pipeline for id: ' + msg.id)
        finishPipeline(handle)
        const output = pullAll(handle, 0)
        const errors = pullAll(handle, 1)
        const stats = pullAll(handle, 2)
        freePipeline(handle)
        activePipelines.delete(msg.id)
        self.postMessage(
          { type: 'output', id: msg.id, output, stats, errors },
          [output.buffer, stats.buffer, errors.buffer]
        )
        break
      }

      default:
        self.postMessage({ type: 'error', message: `Unknown message type: ${msg.type}` })
    }
  } catch (err) {
    self.postMessage({ type: 'error', message: err.message, id: msg.id })
  }
}
