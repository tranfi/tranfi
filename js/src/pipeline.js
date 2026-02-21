/**
 * pipeline.js — Pipeline builder and runner.
 *
 * Tries native N-API addon first, falls back to WASM.
 */

import { readFile, writeFile } from 'fs/promises'
import { existsSync } from 'fs'
import nativeBinding from './native.js'

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
  const { loadWasm } = await import('./wasm.js')
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
    finish(handle) {
      const rc = wasm.ccall('wasm_pipeline_finish', 'number', ['number'], [handle])
      if (rc !== 0) {
        const err = wasm.ccall('wasm_pipeline_error', 'string', ['number'], [handle])
        throw new Error(`Finish failed: ${err || 'unknown error'}`)
      }
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


export class PipelineResult {
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


export class Pipeline {
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

  async run({ input, inputFile } = {}) {
    if (this._engine && this._engine !== 'native') {
      return this._runEngine({ input, inputFile })
    }

    const backend = await getBackend()
    const planJson = await this._toPlanJson(backend)
    const handle = backend.createPipeline(planJson)

    try {
      if (inputFile) {
        const data = await readFile(inputFile)
        // Push in chunks
        for (let i = 0; i < data.length; i += CHUNK_SIZE) {
          const chunk = data.subarray(i, Math.min(i + CHUNK_SIZE, data.length))
          backend.push(handle, chunk)
        }
      } else if (input) {
        const buf = typeof input === 'string' ? Buffer.from(input, 'utf-8') : input
        backend.push(handle, buf)
      }

      backend.finish(handle)

      const output = backend.pull(handle, CHAN_MAIN)
      const errors = backend.pull(handle, CHAN_ERRORS)
      const stats = backend.pull(handle, CHAN_STATS)
      const samples = backend.pull(handle, CHAN_SAMPLES)

      return new PipelineResult(output, errors, stats, samples)
    } finally {
      backend.free(handle)
    }
  }

  async _runEngine({ input, inputFile } = {}) {
    const { getEngine } = await import('./engines/duckdb.js')
    const engine = getEngine(this._engine)
    const dsl = this._dsl
    if (!dsl) throw new Error('DuckDB engine requires a DSL string pipeline')
    return engine.run(dsl, { input, inputFile })
  }
}

export async function compileDsl(dsl) {
  const backend = await getBackend()
  return backend.compileDsl(dsl)
}

export async function compileToSql(dsl) {
  const backend = await getBackend()
  return backend.compileToSql(dsl)
}

export async function loadRecipe(source) {
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

export async function saveRecipe(steps, path) {
  await writeFile(path, JSON.stringify({ steps }))
}

export async function recipes() {
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
