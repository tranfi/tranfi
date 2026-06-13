/**
 * pipeline.js — Pipeline builder and runner.
 *
 * Tries native N-API addon first, falls back to WASM.
 */

const { readFile, writeFile } = require('fs/promises')
const { createReadStream } = require('fs')
const { Readable } = require('stream')
const { createGunzip } = require('zlib')
const { finished } = require('stream/promises')
const nativeBinding = require('./native.js')
const { prepareNativePlan, validateNativeMemoryPolicy } = require('./memory_policy.js')

// Channel IDs (match tranfi.h)
const CHAN_MAIN = 0
const CHAN_ERRORS = 1
const CHAN_STATS = 2
const CHAN_SAMPLES = 3

const CHUNK_SIZE = 64 * 1024

/**
 * Get the backend (native or WASM).
 * Returns an object with: createPipeline, push, finish, pull, free, version
 */
let _backend = null

async function getBackend() {
  if (_backend) return _backend

  // Try native first
  if (nativeBinding) {
    _backend = nativeBinding
    return _backend
  }

  // Fall back to WASM
  const { loadWasm } = require('./wasm.js')
  const wasm = await loadWasm()

  _backend = {
    compileDsl(dsl) {
      const len = wasm.lengthBytesUTF8(dsl)
      const ptr = wasm._malloc(len + 1)
      wasm.stringToUTF8(dsl, ptr, len + 1)
      const resultPtr = wasm.ccall('wasm_compile_dsl', 'number', ['number', 'number'], [ptr, len])
      wasm._free(ptr)
      if (!resultPtr) {
        const err = wasm.ccall('wasm_pipeline_error', 'string', ['number'], [-1])
        throw new Error(`DSL parse failed: ${err || 'unknown error'}`)
      }
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
      if (handle < 0) {
        const err = wasm.ccall('wasm_pipeline_error', 'string', ['number'], [handle])
        throw new Error(`Failed to create pipeline: ${err || 'unknown error'}`)
      }
      return handle
    },
    push(handle, buffer) {
      const len = buffer.length
      const ptr = wasm._malloc(len)
      wasm.HEAPU8.set(buffer, ptr)
      const rc = wasm.ccall('wasm_pipeline_push', 'number', ['number', 'number', 'number'], [handle, ptr, len])
      wasm._free(ptr)
      if (rc !== 0) {
        const err = wasm.ccall('wasm_pipeline_error', 'string', ['number'], [handle])
        throw new Error(`Push failed: ${err || 'unknown error'}`)
      }
    },
    flushInput(handle) {
      const rc = wasm.ccall('wasm_pipeline_flush_input', 'number', ['number'], [handle])
      if (rc !== 0) {
        const err = wasm.ccall('wasm_pipeline_error', 'string', ['number'], [handle])
        throw new Error(`Input flush failed: ${err || 'unknown error'}`)
      }
    },
    setSourceName(handle, name) {
      const source = name == null ? '' : String(name)
      const len = wasm.lengthBytesUTF8(source)
      const ptr = wasm._malloc(len + 1)
      wasm.stringToUTF8(source, ptr, len + 1)
      const rc = wasm.ccall('wasm_pipeline_set_source_name', 'number', ['number', 'number'], [handle, ptr])
      wasm._free(ptr)
      if (rc !== 0) {
        const err = wasm.ccall('wasm_pipeline_error', 'string', ['number'], [handle])
        throw new Error(`Set source name failed: ${err || 'unknown error'}`)
      }
    },
    finish(handle) {
      const rc = wasm.ccall('wasm_pipeline_finish', 'number', ['number'], [handle])
      if (rc !== 0) {
        const err = wasm.ccall('wasm_pipeline_error', 'string', ['number'], [handle])
        throw new Error(`Finish failed: ${err || 'unknown error'}`)
      }
    },
    finishStep(handle) {
      const rc = wasm.ccall('wasm_pipeline_finish_step', 'number', ['number'], [handle])
      if (rc < 0) {
        const err = wasm.ccall('wasm_pipeline_error', 'string', ['number'], [handle])
        throw new Error(`Finish failed: ${err || 'unknown error'}`)
      }
      return rc === 1
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
      // Concatenate
      const total = chunks.reduce((s, c) => s + c.length, 0)
      const result = Buffer.alloc(total)
      let offset = 0
      for (const c of chunks) {
        result.set(c, offset)
        offset += c.length
      }
      return result
    },
    pullChunk(handle, channel, chunkSize = 65536) {
      if (chunkSize <= 0) throw new Error('chunkSize must be positive')
      const ptr = wasm._malloc(chunkSize)
      const n = wasm.ccall('wasm_pipeline_pull', 'number',
        ['number', 'number', 'number', 'number'], [handle, channel, ptr, chunkSize])
      const result = Buffer.from(new Uint8Array(wasm.HEAPU8.buffer, ptr, n))
      wasm._free(ptr)
      return result
    },
    free(handle) {
      wasm.ccall('wasm_pipeline_free', null, ['number'], [handle])
    },
    version() {
      return wasm.ccall('wasm_version', 'string', [], [])
    },
    recipeCount() {
      return wasm.ccall('wasm_recipe_count', 'number', [], [])
    },
    recipeName(index) {
      return wasm.ccall('wasm_recipe_name', 'string', ['number'], [index])
    },
    recipeDsl(index) {
      return wasm.ccall('wasm_recipe_dsl', 'string', ['number'], [index])
    },
    recipeDescription(index) {
      return wasm.ccall('wasm_recipe_description', 'string', ['number'], [index])
    },
    recipeFindDsl(name) {
      const len = wasm.lengthBytesUTF8(name)
      const ptr = wasm._malloc(len + 1)
      wasm.stringToUTF8(name, ptr, len + 1)
      const result = wasm.ccall('wasm_recipe_find_dsl', 'string', ['number'], [ptr])
      wasm._free(ptr)
      return result || null
    },
    compileToSql(dsl) {
      const len = wasm.lengthBytesUTF8(dsl)
      const ptr = wasm._malloc(len + 1)
      wasm.stringToUTF8(dsl, ptr, len + 1)
      const resultPtr = wasm.ccall('wasm_compile_to_sql', 'number', ['number', 'number'], [ptr, len])
      wasm._free(ptr)
      if (!resultPtr) {
        const err = wasm.ccall('wasm_pipeline_error', 'string', ['number'], [-1])
        throw new Error(`SQL compile failed: ${err || 'unknown error'}`)
      }
      const sql = wasm.UTF8ToString(resultPtr)
      wasm._free(resultPtr)
      return sql
    }
  }

  return _backend
}


function asInputBuffer(input) {
  if (Buffer.isBuffer(input)) return input
  if (typeof input === 'string') return Buffer.from(input, 'utf-8')
  if (input instanceof ArrayBuffer) return Buffer.from(new Uint8Array(input))
  return Buffer.from(input)
}

function hasInput(value) {
  return value !== undefined && value !== null
}

function assertSingleInputSource({ input, inputFile, inputFiles, inputStream }) {
  const count = (hasInput(input) ? 1 : 0) + (hasInput(inputFile) ? 1 : 0) + (hasInput(inputFiles) ? 1 : 0) + (hasInput(inputStream) ? 1 : 0)
  if (count > 1) throw new Error('pass only one of input, inputFile, inputFiles, or inputStream')
}

function normalizeInputFiles(inputFiles) {
  if (!hasInput(inputFiles)) return null
  if (typeof inputFiles === 'string' || Buffer.isBuffer(inputFiles)) {
    throw new TypeError('inputFiles must be an iterable of paths, not a single path')
  }
  if (typeof inputFiles[Symbol.iterator] !== 'function') {
    throw new TypeError('inputFiles must be an iterable of paths')
  }
  const files = Array.from(inputFiles)
  if (files.length === 0) throw new Error('inputFiles must not be empty')
  return files
}

function injectSourceNameStep(planJson, sourceColumn) {
  if (sourceColumn === undefined || sourceColumn === null) return planJson
  const result = String(sourceColumn)
  if (!result) throw new Error('sourceColumn must be a non-empty column name')
  const plan = JSON.parse(planJson)
  if (!plan || !Array.isArray(plan.steps)) throw new Error('plan must contain a steps array')
  let insertAt = 0
  if (plan.steps.length > 0) {
    const op = plan.steps[0] && plan.steps[0].op
    if (typeof op === 'string' && op.startsWith('codec.') && op.endsWith('.decode')) insertAt = 1
  }
  plan.steps.splice(insertAt, 0, { op: 'source-name', args: { result } })
  return JSON.stringify(plan)
}

function normalizeCompression(compression = 'auto') {
  const mode = compression == null ? 'auto' : String(compression).toLowerCase()
  if (!['auto', 'none', 'gzip'].includes(mode)) {
    throw new Error("compression must be 'auto', 'none', or 'gzip'")
  }
  return mode
}

function resolveFileCompression(inputFile, compression) {
  const mode = normalizeCompression(compression)
  if (mode === 'auto') {
    return String(inputFile).toLowerCase().endsWith('.gz') ? 'gzip' : 'none'
  }
  return mode
}

async function *yieldChunkItems(chunk, chunkSize, sourceName) {
  const buf = asInputBuffer(chunk)
  for (let i = 0; i < buf.length; i += chunkSize) {
    yield { chunk: buf.subarray(i, Math.min(i + chunkSize, buf.length)), sourceName }
  }
}

async function *yieldFileItems(inputFile, chunkSize, requestedCompression) {
  const sourceName = String(inputFile)
  const fileCompression = resolveFileCompression(inputFile, requestedCompression)
  let stream = createReadStream(inputFile, { highWaterMark: chunkSize })
  if (fileCompression === 'gzip') stream = stream.pipe(createGunzip())
  for await (const chunk of stream) {
    yield * yieldChunkItems(chunk, chunkSize, sourceName)
  }
  yield { flushInput: true, sourceName }
}

async function *iterInputChunks({ input, inputFile, inputFiles, inputStream, chunkSize, compression = 'auto' }) {
  const files = normalizeInputFiles(inputFiles)
  assertSingleInputSource({ input, inputFile, inputFiles: files, inputStream })
  const requestedCompression = normalizeCompression(compression)

  if (files) {
    for (const file of files) yield * yieldFileItems(file, chunkSize, requestedCompression)
    return
  }

  if (hasInput(inputFile)) {
    yield * yieldFileItems(inputFile, chunkSize, requestedCompression)
    return
  }

  if (hasInput(inputStream)) {
    const isIterable = typeof inputStream[Symbol.asyncIterator] === 'function' || typeof inputStream[Symbol.iterator] === 'function'
    if (!isIterable || typeof inputStream.pipe !== 'function' && requestedCompression === 'gzip') {
      throw new TypeError('inputStream must be a Node.js stream or an iterable of chunks')
    }
    let stream = inputStream
    if (requestedCompression === 'gzip') stream = inputStream.pipe(createGunzip())
    for await (const chunk of stream) {
      yield * yieldChunkItems(chunk, chunkSize)
    }
    return
  }

  if (requestedCompression === 'gzip') throw new Error('compression=gzip requires inputFile, inputFiles, or inputStream')

  if (hasInput(input)) {
    const buf = asInputBuffer(input)
    for (let i = 0; i < buf.length; i += chunkSize) {
      yield { chunk: buf.subarray(i, Math.min(i + chunkSize, buf.length)) }
    }
  }
}

function concatBuffers(chunks) {
  if (chunks.length === 0) return Buffer.alloc(0)
  return Buffer.concat(chunks)
}

function waitForWritableDrain(writable) {
  return new Promise((resolve, reject) => {
    function cleanup() {
      writable.off('drain', onDrain)
      writable.off('error', onError)
    }
    function onDrain() {
      cleanup()
      resolve()
    }
    function onError(err) {
      cleanup()
      reject(err)
    }
    writable.once('drain', onDrain)
    writable.once('error', onError)
  })
}

async function writeWritableChunk(writable, chunk) {
  if (chunk.length === 0) return
  if (!writable.write(chunk)) await waitForWritableDrain(writable)
}

function pullOneChunk(backend, handle, channel, chunkSize) {
  if (typeof backend.pullChunk === 'function') {
    return Buffer.from(backend.pullChunk(handle, channel, chunkSize))
  }
  return Buffer.from(backend.pull(handle, channel))
}

async function drainMain(backend, handle, chunkSize, outputChunks, onOutput, collectOutput) {
  for (;;) {
    const chunk = pullOneChunk(backend, handle, CHAN_MAIN, chunkSize)
    if (chunk.length === 0) break
    if (onOutput) await onOutput(chunk)
    if (collectOutput) outputChunks.push(chunk)
  }
}

async function finishAndDrainMain(backend, handle, chunkSize, outputChunks, onOutput, collectOutput) {
  if (typeof backend.finishStep === 'function') {
    for (;;) {
      const done = backend.finishStep(handle)
      await drainMain(backend, handle, chunkSize, outputChunks, onOutput, collectOutput)
      if (done) break
    }
  } else {
    backend.finish(handle)
    await drainMain(backend, handle, chunkSize, outputChunks, onOutput, collectOutput)
  }
}

class PipelineResult {
  constructor(output, errors, stats, samples) {
    this.output = output
    this.errors = errors
    this.stats = stats
    this.samples = samples
  }

  get outputText() {
    return this.output.toString('utf-8')
  }

  get statsText() {
    return this.stats.toString('utf-8')
  }

  toString() {
    return `PipelineResult(output=${this.output.length} bytes, errors=${this.errors.length} bytes, stats=${this.stats.length} bytes)`
  }
}


class Pipeline {
  constructor(steps, { planJson, engine } = {}) {
    this._engine = engine || null
    if (planJson) {
      this._planJson = planJson
      this._dsl = null
      this._steps = null
    } else if (typeof steps === 'string') {
      this._dsl = steps
      this._steps = null
      this._planJson = null
    } else {
      this._dsl = null
      this._steps = steps
      this._planJson = null
    }
  }

  async _toPlanJson(backend) {
    if (this._planJson) return this._planJson
    if (this._dsl) {
      // Check if it's a built-in recipe name (no pipes, no braces)
      const s = this._dsl.trim()
      if (!s.includes('|') && !s.startsWith('{')) {
        const recipeDsl = backend.recipeFindDsl(s)
        if (recipeDsl) return backend.compileDsl(recipeDsl)
      }
      return backend.compileDsl(this._dsl)
    }
    return JSON.stringify({ steps: this._steps })
  }

  async run({ input, inputFile, inputFiles, inputStream, sourceColumn, onOutput, collectOutput = true, chunkSize = CHUNK_SIZE, allowBlocking = false, memory, spillDir, compression = 'auto' } = {}) {
    if (chunkSize <= 0) throw new Error('chunkSize must be positive')
    if (sourceColumn !== undefined && sourceColumn !== null && !hasInput(inputFile) && !hasInput(inputFiles)) {
      throw new Error('sourceColumn requires inputFile or inputFiles')
    }
    if (onOutput !== undefined && typeof onOutput !== 'function') {
      throw new TypeError('onOutput must be a function')
    }
    if (this._engine && this._engine !== 'native') {
      if (onOutput || collectOutput === false || hasInput(inputStream) || hasInput(inputFiles) || sourceColumn !== undefined && sourceColumn !== null) {
        throw new Error('streaming input/output, inputFiles, and sourceColumn require the native or WASM backend')
      }
      return this._runEngine({ input, inputFile, memory, spillDir })
    }

    const backend = await getBackend()
    if (spillDir && backend.isWasm) {
      throw new Error('spillDir is not supported by WASM native execution because browser/WASM spill storage would occupy WASM memory; use the Node native addon, CLI/direct C, or DuckDB')
    }
    let planJson = await this._toPlanJson(backend)
    planJson = injectSourceNameStep(planJson, sourceColumn)
    planJson = prepareNativePlan(planJson, { allowBlocking, memory, spillDir })
    const handle = backend.createPipeline(planJson)
    const outputChunks = []

    try {
      for await (const item of iterInputChunks({ input, inputFile, inputFiles, inputStream, chunkSize, compression })) {
        if (item.sourceName !== undefined && typeof backend.setSourceName === 'function') backend.setSourceName(handle, item.sourceName)
        if (item.flushInput) {
          if (typeof backend.flushInput !== 'function') throw new Error('inputFiles require backend.flushInput')
          backend.flushInput(handle)
          await drainMain(backend, handle, chunkSize, outputChunks, onOutput, collectOutput)
          continue
        }
        backend.push(handle, item.chunk)
        await drainMain(backend, handle, chunkSize, outputChunks, onOutput, collectOutput)
      }

      await finishAndDrainMain(backend, handle, chunkSize, outputChunks, onOutput, collectOutput)

      const output = collectOutput ? concatBuffers(outputChunks) : Buffer.alloc(0)
      const errors = backend.pull(handle, CHAN_ERRORS)
      const stats = backend.pull(handle, CHAN_STATS)
      const samples = backend.pull(handle, CHAN_SAMPLES)

      return new PipelineResult(output, errors, stats, samples)
    } finally {
      backend.free(handle)
    }
  }

  async *iterChunks({ input, inputFile, inputFiles, inputStream, sourceColumn, chunkSize = CHUNK_SIZE, allowBlocking = false, memory, spillDir, compression = 'auto' } = {}) {
    if (chunkSize <= 0) throw new Error('chunkSize must be positive')
    if (sourceColumn !== undefined && sourceColumn !== null && !hasInput(inputFile) && !hasInput(inputFiles)) {
      throw new Error('sourceColumn requires inputFile or inputFiles')
    }
    if (this._engine && this._engine !== 'native') {
      throw new Error('streaming chunk iteration requires the native or WASM backend')
    }

    const backend = await getBackend()
    if (spillDir && backend.isWasm) {
      throw new Error('spillDir is not supported by WASM native execution because browser/WASM spill storage would occupy WASM memory; use the Node native addon, CLI/direct C, or DuckDB')
    }
    let planJson = await this._toPlanJson(backend)
    planJson = injectSourceNameStep(planJson, sourceColumn)
    planJson = prepareNativePlan(planJson, { allowBlocking, memory, spillDir })
    const handle = backend.createPipeline(planJson)

    try {
      for await (const item of iterInputChunks({ input, inputFile, inputFiles, inputStream, chunkSize, compression })) {
        if (item.sourceName !== undefined && typeof backend.setSourceName === 'function') backend.setSourceName(handle, item.sourceName)
        if (item.flushInput) {
          if (typeof backend.flushInput !== 'function') throw new Error('inputFiles require backend.flushInput')
          backend.flushInput(handle)
        } else {
          backend.push(handle, item.chunk)
        }
        for (;;) {
          const out = pullOneChunk(backend, handle, CHAN_MAIN, chunkSize)
          if (out.length === 0) break
          yield out
        }
      }

      if (typeof backend.finishStep === 'function') {
        for (;;) {
          const done = backend.finishStep(handle)
          for (;;) {
            const out = pullOneChunk(backend, handle, CHAN_MAIN, chunkSize)
            if (out.length === 0) break
            yield out
          }
          if (done) break
        }
      } else {
        backend.finish(handle)
        for (;;) {
          const out = pullOneChunk(backend, handle, CHAN_MAIN, chunkSize)
          if (out.length === 0) break
          yield out
        }
      }
    } finally {
      backend.free(handle)
    }
  }

  toReadable({ highWaterMark, ...options } = {}) {
    const streamOptions = { objectMode: false }
    if (highWaterMark !== undefined) streamOptions.highWaterMark = highWaterMark
    return Readable.from(this.iterChunks(options), streamOptions)
  }

  async writeTo(writable, { end = true, ...options } = {}) {
    if (!writable || typeof writable.write !== 'function') {
      throw new TypeError('writable must be a Node.js Writable stream')
    }
    const result = await this.run({
      ...options,
      collectOutput: false,
      onOutput: async chunk => {
        await writeWritableChunk(writable, chunk)
      }
    })
    if (end && typeof writable.end === 'function') {
      writable.end()
      await finished(writable)
    }
    return result
  }

  async _runEngine({ input, inputFile, inputFiles, sourceColumn, memory, spillDir } = {}) {
    if (hasInput(inputFiles) || sourceColumn !== undefined && sourceColumn !== null) {
      throw new Error('inputFiles and sourceColumn require the native or WASM backend')
    }
    const { getEngine } = require('./engines/duckdb.js')
    const engine = getEngine(this._engine)
    const dsl = this._dsl
    if (!dsl) throw new Error('DuckDB engine requires a DSL string pipeline')
    return engine.run(dsl, { input, inputFile, memory, spillDir })
  }
}

async function compileDsl(dsl) {
  const backend = await getBackend()
  return backend.compileDsl(dsl)
}

async function compileToSql(dsl) {
  const backend = await getBackend()
  return backend.compileToSql(dsl)
}

async function loadRecipe(source) {
  let planJson
  if (typeof source === 'object' && source.steps) {
    planJson = JSON.stringify(source)
  } else if (typeof source === 'string' && source.trimStart().startsWith('{')) {
    planJson = source
  } else {
    planJson = await readFile(source, 'utf-8')
  }
  return new Pipeline(null, { planJson })
}

async function saveRecipe(steps, path) {
  await writeFile(path, JSON.stringify({ steps }))
}

async function recipes() {
  const backend = await getBackend()
  const n = backend.recipeCount()
  const result = []
  for (let i = 0; i < n; i++) {
    result.push({
      name: backend.recipeName(i),
      dsl: backend.recipeDsl(i),
      description: backend.recipeDescription(i)
    })
  }
  return result
}

module.exports = {
  Pipeline,
  PipelineResult,
  compileDsl,
  compileToSql,
  loadRecipe,
  saveRecipe,
  recipes
}
