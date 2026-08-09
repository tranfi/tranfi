
var createTransformAPI = require('./transform.js')

var BLOCKING_OPS = {
  'sort': true,
  'pivot': true,
  'interpolate': true,
  'normalize': true,
  'acf': true,
  'codec.table.encode': true
}

var KEY_STATE_OPS = {
  'rowid': true,
  'unique': true,
  'dedup': true,
  'group-agg': true,
  'frequency': true,
  'join': true,
  'semi-join': true,
  'anti-join': true,
  'onehot': true,
  'label-encode': true,
  'intersect': true,
  'setdiff': true,
  'union': true
}

var SIZE_UNITS = {
  '': 1,
  b: 1,
  k: 1024,
  kb: 1024,
  kib: 1024,
  m: 1024 * 1024,
  mb: 1024 * 1024,
  mib: 1024 * 1024,
  g: 1024 * 1024 * 1024,
  gb: 1024 * 1024 * 1024,
  gib: 1024 * 1024 * 1024
}

var UNIQUE_DEFAULT_BLOOM_BYTES = 1024 * 1024

function normalizeSqlDialect(options) {
  var dialect = 'duckdb'
  if (typeof options === 'string') {
    dialect = options
  } else if (options && typeof options === 'object') {
    dialect = options.dialect || options.sqlDialect || dialect
  } else if (options !== undefined && options !== null) {
    throw new TypeError('compileToSql options must be an object or dialect string')
  }
  if (typeof dialect !== 'string') throw new TypeError('SQL dialect must be a string')
  var name = dialect.toLowerCase()
  if (name === 'duckdb') return name
  if (name === 'sqlite' || name === 'postgres') {
    throw new Error("SQL dialect '" + name + "' is recognized but not implemented yet; only duckdb lowering is available")
  }
  throw new Error("unknown SQL dialect '" + dialect + "' (expected duckdb, sqlite, or postgres)")
}

function parseMemorySize(value) {
  if (value === undefined || value === null) return null
  if (typeof value === 'number') {
    if (!isFinite(value) || value <= 0 || Math.floor(value) !== value) throw new Error('memory must be a positive integer byte count')
    return value
  }
  if (typeof value !== 'string') throw new TypeError('memory must be a positive byte count or size string')
  var text = value.trim()
  var lower = text.toLowerCase()
  if (lower.indexOf('max:') === 0 || lower.indexOf('max=') === 0) text = text.slice(4).trim()
  var match = /^(\d+)\s*([a-zA-Z]*)$/.exec(text)
  if (!match) throw new Error('memory must look like 64MB or max:64MB')
  var amount = Number(match[1])
  var suffix = match[2].toLowerCase()
  if (!SIZE_UNITS.hasOwnProperty(suffix)) throw new Error("unsupported memory size suffix '" + suffix + "'")
  return amount * SIZE_UNITS[suffix]
}

function formatBytes(n) {
  if (n < 1024) return String(n) + 'B'
  if (n < 1024 * 1024) return (n / 1024).toFixed(1) + 'KB'
  if (n < 1024 * 1024 * 1024) return (n / (1024 * 1024)).toFixed(1) + 'MB'
  return (n / (1024 * 1024 * 1024)).toFixed(1) + 'GB'
}

function memoryClass(step) {
  if (typeof step.memory_class === 'string') return step.memory_class
  if (BLOCKING_OPS[step.op]) return 'blocking'
  if (KEY_STATE_OPS[step.op]) return 'key_state'
  return null
}

function argsOf(step) {
  return step && step.args && typeof step.args === 'object' ? step.args : {}
}

function positiveInt(value) {
  return typeof value === 'number' && isFinite(value) && value > 0 && Math.floor(value) === value ? value : null
}

function arrayLen(value) {
  return Array.isArray(value) ? value.length : 0
}

function stringArrayBytes(value) {
  if (!Array.isArray(value)) return 0
  var enc = new TextEncoder()
  var total = 0
  for (var i = 0; i < value.length; i++) {
    if (typeof value[i] === 'string') total += enc.encode(value[i]).length + 1
  }
  return total
}

function categoryCap(step) {
  var args = argsOf(step)
  var maxCategories = positiveInt(args.max_categories)
  var nCategories = arrayLen(args.categories)
  var literalBytes = stringArrayBytes(args.categories)
  if (args.unknown === 'other') {
    nCategories += 1
    literalBytes += 'other'.length + 1
  }
  if (maxCategories !== null) return [maxCategories, literalBytes]
  if (nCategories > 0) return [nCategories, literalBytes]
  return [null, literalBytes]
}

function estimateKeyStateStepBytes(step) {
  var args = argsOf(step)
  var op = step.op
  if (op === 'rowid') {
    var nCols = arrayLen(args.columns)
    if (nCols === 0 || args.sorted === true) return 2048 + nCols * 128
    var maxStateBytes = positiveInt(args.max_state_bytes)
    if (maxStateBytes !== null) return maxStateBytes
    var maxKeys = positiveInt(args.max_keys)
    if (maxKeys === null) throw new Error("step 'rowid' needs max_keys, max_state_bytes, or sorted=true")
    return 1024 + maxKeys * 224 + nCols * 128
  }
  if (op === 'unique' || op === 'dedup') {
    var ukNCols = arrayLen(args.columns)
    if (args.sorted === true) return 2048 + ukNCols * 128
    if (args.approx === true || args.mode === 'approx') {
      var ukBloomBytes = positiveInt(args.bloom_bytes)
      return ukBloomBytes !== null ? ukBloomBytes : UNIQUE_DEFAULT_BLOOM_BYTES
    }
    var ukMaxStateBytes = positiveInt(args.max_state_bytes)
    if (ukMaxStateBytes !== null) return ukMaxStateBytes
    var ukMaxKeys = positiveInt(args.max_keys)
    if (ukMaxKeys === null) throw new Error("step '" + op + "' needs max_keys, max_state_bytes, or sorted=true for byte-bounded native execution")
    return 1024 + ukMaxKeys * (256 + ukNCols * 32)
  }
  if (op === 'group-agg') {
    var nGroup = arrayLen(args.group_by)
    var nAggs = arrayLen(args.aggs)
    var maxStateBytes = positiveInt(args.max_state_bytes)
    if (maxStateBytes !== null) return maxStateBytes
    if (args.sorted === true) return 4096 + nGroup * 128 + nAggs * 128
    var maxGroups = positiveInt(args.max_groups)
    if (maxGroups === null) throw new Error("step 'group-agg' needs max_groups, max_state_bytes, or sorted=true for byte-bounded native execution")
    return 2048 + maxGroups * (384 + nGroup * 64 + nAggs * 96)
  }
  if (op === 'frequency') {
    var maxStateBytes = positiveInt(args.max_state_bytes)
    if (maxStateBytes !== null) return maxStateBytes
    var maxValues = positiveInt(args.max_values)
    if (maxValues === null) throw new Error("step 'frequency' needs max_values or max_state_bytes for byte-bounded native execution")
    var cappedValues = args.overflow === 'other' ? maxValues + 1 : maxValues
    return 1024 + cappedValues * (224 + arrayLen(args.columns) * 32)
  }
  if (op === 'onehot' || op === 'label-encode') {
    var catMaxStateBytes = positiveInt(args.max_state_bytes)
    if (catMaxStateBytes !== null) return catMaxStateBytes
    var cap = categoryCap(step)
    if (cap[0] === null) throw new Error("step '" + op + "' needs max_categories, max_state_bytes, or declared categories")
    return 1024 + cap[1] + cap[0] * (op === 'onehot' ? 320 : 224)
  }
  if (op === 'intersect' || op === 'setdiff') {
    var setNCols = arrayLen(args.columns)
    if (args.sorted === true) return 4096 + setNCols * 256
    var setMaxLookupBytes = positiveInt(args.max_lookup_bytes)
    if (setMaxLookupBytes === null) throw new Error("step '" + op + "' needs max_lookup_bytes or sorted=true for byte-bounded native execution")
    var setMaxStateBytes = positiveInt(args.max_state_bytes)
    if (setMaxStateBytes !== null) return 4096 + setMaxLookupBytes * 3 + setMaxStateBytes
    var setMaxOutputKeys = positiveInt(args.max_output_keys)
    if (setMaxOutputKeys === null) throw new Error("step '" + op + "' needs max_output_keys or max_state_bytes for duplicate-eliminating set semantics")
    var setEst = 4096 + setMaxLookupBytes * 3
    var setMaxLookupKeys = positiveInt(args.max_lookup_keys)
    if (setMaxLookupKeys !== null) setEst += setMaxLookupKeys * (192 + setNCols * 32)
    setEst += setMaxOutputKeys * (192 + setNCols * 32)
    return setEst
  }
  if (op === 'union') {
    var unionMaxStateBytes = positiveInt(args.max_state_bytes)
    if (unionMaxStateBytes !== null) return unionMaxStateBytes
    var unionNCols = arrayLen(args.columns)
    var unionMaxOutputKeys = positiveInt(args.max_output_keys)
    if (unionMaxOutputKeys === null) throw new Error("step 'union' needs max_output_keys or max_state_bytes for duplicate-eliminating set semantics")
    return 4096 + unionMaxOutputKeys * (192 + unionNCols * 32)
  }
  if (op === 'join' || op === 'semi-join' || op === 'anti-join') {
    if (args.sorted === true) {
      var filtering = op === 'semi-join' || op === 'anti-join' || args.how === 'semi' || args.how === 'anti'
      if (filtering) return 4096
      var maxMatches = positiveInt(args.max_matches_per_row)
      if (maxMatches === null) throw new Error("step '" + op + "' sorted=true needs max_matches_per_row for byte-bounded mutating join")
      return 4096 + maxMatches * 512
    }
    var maxLookupBytes = positiveInt(args.max_lookup_bytes)
    if (maxLookupBytes === null) throw new Error("step '" + op + "' needs max_lookup_bytes or sorted=true for byte-bounded native execution")
    var est = 4096 + maxLookupBytes * 4
    var maxStateBytes = positiveInt(args.max_state_bytes)
    if (maxStateBytes !== null) return est + maxStateBytes
    var maxLookupRows = positiveInt(args.max_lookup_rows)
    var maxLookupKeys = positiveInt(args.max_lookup_keys)
    if (maxLookupRows !== null) est += maxLookupRows * 96
    if (maxLookupKeys !== null) est += maxLookupKeys * 192
    return est
  }
  throw new Error("step '" + op + "' has no native byte estimator")
}

function loadPlan(planJson) {
  var plan = typeof planJson === 'string' ? JSON.parse(planJson) : planJson
  if (!plan || !Array.isArray(plan.steps)) throw new Error('plan must contain a steps array')
  return plan
}

function blockingSteps(steps) {
  var out = []
  for (var i = 0; i < steps.length; i++) if (memoryClass(steps[i]) === 'blocking') out.push(steps[i])
  return out
}

function keyStateSteps(steps) {
  var out = []
  for (var i = 0; i < steps.length; i++) if (memoryClass(steps[i]) === 'key_state') out.push(steps[i])
  return out
}

function blockingPlanCanUseNativeSpill(steps) {
  if (steps.length === 0) return false
  for (var i = 0; i < steps.length; i++) if (steps[i].op !== 'sort') return false
  return true
}

function injectSortSpill(plan, spillDir, memoryBytes) {
  for (var i = 0; i < plan.steps.length; i++) {
    var step = plan.steps[i]
    if (step.op !== 'sort') continue
    if (!step.args || typeof step.args !== 'object') step.args = {}
    step.args.spill_dir = spillDir
    if (memoryBytes !== null) step.args.spill_memory_bytes = memoryBytes
  }
  return JSON.stringify(plan)
}

function prepareNativePlan(planJson, options) {
  options = options || {}
  var plan = loadPlan(planJson)
  var blocking = blockingSteps(plan.steps)
  var firstBlocking = blocking.length ? blocking[0] : null
  var keySteps = keyStateSteps(plan.steps)
  var memoryBytes = parseMemorySize(options.memory)
  var canSpillBlocking = false

  if (options.spillDir) {
    throw new Error('spillDir is not supported by standalone WASM native execution because browser/WASM spill storage would occupy WASM memory; use the Node native addon, CLI/direct C, or DuckDB')
  }

  if (options.spillDir) {
    if (keySteps.length > 0) {
      throw new Error("spillDir was requested, but native spill is not implemented yet for key-state step '" + keySteps[0].op + "'")
    }
    for (var i = 0; i < blocking.length; i++) {
      if (blocking[i].op !== 'sort') {
        throw new Error("spillDir was requested, but native spill is not implemented yet for blocking step '" + blocking[i].op + "'")
      }
    }
  }

  if (memoryBytes !== null && firstBlocking && !canSpillBlocking) {
    throw new Error("memory " + formatBytes(memoryBytes) + " was requested, but native byte caps are not implemented for blocking step '" + firstBlocking.op + "'")
  }
  if (memoryBytes !== null && keySteps.length > 0) {
    var total = 0
    for (var j = 0; j < keySteps.length; j++) total += estimateKeyStateStepBytes(keySteps[j])
    if (total > memoryBytes) throw new Error('estimated native key-state memory ' + formatBytes(total) + ' exceeds memory ' + formatBytes(memoryBytes))
  }
  if (firstBlocking && !options.allowBlocking && !canSpillBlocking) {
    throw new Error("blocking step '" + firstBlocking.op + "' requires full input in native mode; pass allowBlocking: true for known-small data or use an external engine")
  }
  if (canSpillBlocking) return injectSortSpill(plan, options.spillDir, memoryBytes)
  return typeof planJson === 'string' ? planJson : JSON.stringify(plan)
}

function validateNativeMemoryPolicy(planJson, options) {
  options = options || {}
  var plan = loadPlan(planJson)
  var keySteps = keyStateSteps(plan.steps)
  var memoryBytes = parseMemorySize(options.memory)
  prepareNativePlan(plan, options)
  if (memoryBytes === null || keySteps.length === 0) return null
  var total = 0
  for (var i = 0; i < keySteps.length; i++) total += estimateKeyStateStepBytes(keySteps[i])
  return total
}

/**
 * tranfi/wasm — Browser-friendly WASM wrapper.
 *
 * Usage:
 *   import createTranfi from 'tranfi/wasm'
 *   const tf = await createTranfi()
 *
 *   // Compile DSL to SQL
 *   const sql = tf.compileToSql('csv | filter "age > 25" | csv', { dialect: 'duckdb' })
 *
 *   // Run with DuckDB-WASM
 *   import * as duckdb from '@duckdb/duckdb-wasm'
 *   const db = new duckdb.AsyncDuckDB(...)
 *   const result = await tf.runDuckDB(db, 'csv | filter "age > 25" | csv', csvBlob)
 */

async function createTranfi() {
  // Dynamic import to handle CJS/ESM interop
  var mod = await import('./tranfi_core.js')
  var createModule = mod.default || mod
  var wasm = await createModule()
  var transformAPI = createTransformAPI(wasm)

  function allocString(str) {
    var len = wasm.lengthBytesUTF8(str)
    var ptr = wasm._malloc(len + 1)
    wasm.stringToUTF8(str, ptr, len + 1)
    return { ptr, len }
  }

  function toBytes(data) {
    return data instanceof Uint8Array ? data : new TextEncoder().encode(data)
  }

  function concatUint8(chunks) {
    var total = chunks.reduce(function(s, c) { return s + c.length }, 0)
    var result = new Uint8Array(total)
    var offset = 0
    for (var i = 0; i < chunks.length; i++) {
      result.set(chunks[i], offset)
      offset += chunks[i].length
    }
    return result
  }

  function hostPolicyOptions(options) {
    options = options || {}
    return {
      allowFs: options.allowFs === undefined ? Boolean(options.spillDir) : Boolean(options.allowFs),
      allowSpill: Boolean(options.spillDir),
      allowRulesFile: Boolean(options.allowRulesFile),
      workspaceRoot: options.workspaceRoot
    }
  }

  function planUsesHostPaths(planJson) {
    try {
      var plan = JSON.parse(planJson)
      var steps = Array.isArray(plan.steps) ? plan.steps : []
      return steps.some(function(step) {
        var args = step && step.args
        if (!args || typeof args !== 'object') return false
        return ['file', 'rules_file', 'rulesFile', 'spill_dir', 'spillDir'].some(function(key) {
          return typeof args[key] === 'string' && args[key].length > 0
        })
      })
    } catch (_) {
      return false
    }
  }

  return {
    /** Raw WASM module (for advanced use) */
    _wasm: wasm,

    TranfiTransformError: transformAPI.TranfiTransformError,
    TransformRecipe: transformAPI.TransformRecipe,
    TransformAnalyzer: transformAPI.TransformAnalyzer,
    TransformPlan: transformAPI.TransformPlan,
    TransformApply: transformAPI.TransformApply,
    TransformCancelToken: transformAPI.TransformCancelToken,
    createTransformCancelToken: transformAPI.createTransformCancelToken,
    safeTransformLimits: transformAPI.safeTransformLimits,

    /** Library version */
    version() {
      return wasm.ccall('wasm_version', 'string', [], [])
    },

    /** Compile DSL string to plan JSON */
    compileDsl(dsl) {
      var s = allocString(dsl)
      var resultPtr = wasm.ccall('wasm_compile_dsl', 'number', ['number', 'number'], [s.ptr, s.len])
      wasm._free(s.ptr)
      if (!resultPtr) {
        var err = wasm.ccall('wasm_pipeline_error', 'string', ['number'], [-1])
        throw new Error('DSL parse failed: ' + (err || 'unknown error'))
      }
      var json = wasm.UTF8ToString(resultPtr)
      wasm._free(resultPtr)
      return json
    },

    /** Apply standalone WASM native memory policy to a compiled plan */
    prepareNativePlan(planJson, options) {
      return prepareNativePlan(planJson, options || {})
    },

    /** Compile DSL string to SQL query */
    compileToSql(dsl, options) {
      normalizeSqlDialect(options)
      var s = allocString(dsl)
      var resultPtr = wasm.ccall('wasm_compile_to_sql', 'number', ['number', 'number'], [s.ptr, s.len])
      wasm._free(s.ptr)
      if (!resultPtr) {
        var err = wasm.ccall('wasm_pipeline_error', 'string', ['number'], [-1])
        throw new Error('SQL compile failed: ' + (err || 'unknown error'))
      }
      var sql = wasm.UTF8ToString(resultPtr)
      wasm._free(resultPtr)
      return sql
    },

    /** Create a native pipeline (push/pull streaming) */
    createPipeline(planJson, options) {
      options = options || {}
      var s = allocString(planJson)
      var root = options.workspaceRoot ? allocString(String(options.workspaceRoot)) : { ptr: 0 }
      var hasPolicyCreate = typeof wasm._wasm_pipeline_create_with_policy === 'function'
      var needsPolicyCreate = options.allowFs || options.allowSpill || options.allowRulesFile || root.ptr || planUsesHostPaths(planJson)
      var handle = -1
      if (hasPolicyCreate) {
        handle = wasm.ccall('wasm_pipeline_create_with_policy', 'number',
          ['number', 'number', 'number', 'number', 'number', 'number'],
          [s.ptr, s.len, options.allowFs ? 1 : 0, options.allowSpill ? 1 : 0,
            options.allowRulesFile ? 1 : 0, root.ptr])
      } else if (!needsPolicyCreate) {
        handle = wasm.ccall('wasm_pipeline_create', 'number', ['number', 'number'], [s.ptr, s.len])
      }
      wasm._free(s.ptr)
      if (root.ptr) wasm._free(root.ptr)
      if (!hasPolicyCreate && needsPolicyCreate) {
        throw new Error('Failed to create pipeline: WASM host policy export unavailable for this plan; rebuild tranfi_core.js')
      }
      if (handle < 0) {
        var err = wasm.ccall('wasm_pipeline_error', 'string', ['number'], [handle])
        throw new Error('Failed to create pipeline: ' + (err || 'unknown error'))
      }
      return handle
    },

    /** Push data into pipeline */
    push(handle, data) {
      var buf = data instanceof Uint8Array ? data : new TextEncoder().encode(data)
      var ptr = wasm._malloc(buf.length)
      wasm.HEAPU8.set(buf, ptr)
      var rc = wasm.ccall('wasm_pipeline_push', 'number', ['number', 'number', 'number'], [handle, ptr, buf.length])
      wasm._free(ptr)
      if (rc !== 0) {
        var err = wasm.ccall('wasm_pipeline_error', 'string', ['number'], [handle])
        throw new Error('Push failed: ' + (err || 'unknown error'))
      }
    },

    /** Flush the decoder input boundary without finishing transforms */
    flushInput(handle) {
      var rc = wasm.ccall('wasm_pipeline_flush_input', 'number', ['number'], [handle])
      if (rc !== 0) {
        var err = wasm.ccall('wasm_pipeline_error', 'string', ['number'], [handle])
        throw new Error('Input flush failed: ' + (err || 'unknown error'))
      }
    },

    /** Set or clear the current host source name */
    setSourceName(handle, name) {
      var source = name == null ? '' : String(name)
      var s = allocString(source)
      var rc = wasm.ccall('wasm_pipeline_set_source_name', 'number', ['number', 'number'], [handle, s.ptr])
      wasm._free(s.ptr)
      if (rc !== 0) {
        var err = wasm.ccall('wasm_pipeline_error', 'string', ['number'], [handle])
        throw new Error('Set source name failed: ' + (err || 'unknown error'))
      }
    },

    /** Signal end of input */
    finish(handle) {
      var rc = wasm.ccall('wasm_pipeline_finish', 'number', ['number'], [handle])
      if (rc !== 0) {
        var err = wasm.ccall('wasm_pipeline_error', 'string', ['number'], [handle])
        throw new Error('Finish failed: ' + (err || 'unknown error'))
      }
    },

    /** Advance finish by one flush boundary. Returns true when complete. */
    finishStep(handle) {
      var rc = wasm.ccall('wasm_pipeline_finish_step', 'number', ['number'], [handle])
      if (rc < 0) {
        var err = wasm.ccall('wasm_pipeline_error', 'string', ['number'], [handle])
        throw new Error('Finish failed: ' + (err || 'unknown error'))
      }
      return rc === 1
    },

    /** Pull output from pipeline */
    pull(handle, channel) {
      var bufSize = 65536
      var ptr = wasm._malloc(bufSize)
      var chunks = []
      for (;;) {
        var n = wasm.ccall('wasm_pipeline_pull', 'number',
          ['number', 'number', 'number', 'number'], [handle, channel, ptr, bufSize])
        if (n === 0) break
        chunks.push(new Uint8Array(wasm.HEAPU8.buffer, ptr, n).slice())
      }
      wasm._free(ptr)
      var total = chunks.reduce(function(s, c) { return s + c.length }, 0)
      var result = new Uint8Array(total)
      var offset = 0
      for (var i = 0; i < chunks.length; i++) {
        result.set(chunks[i], offset)
        offset += chunks[i].length
      }
      return result
    },

    /** Pull at most bufSize bytes from pipeline */
    pullChunk(handle, channel, bufSize) {
      bufSize = bufSize || 65536
      if (bufSize <= 0) throw new Error('bufSize must be positive')
      var ptr = wasm._malloc(bufSize)
      var n = wasm.ccall('wasm_pipeline_pull', 'number',
        ['number', 'number', 'number', 'number'], [handle, channel, ptr, bufSize])
      var result = new Uint8Array(wasm.HEAPU8.buffer, ptr, n).slice()
      wasm._free(ptr)
      return result
    },

    /** Free pipeline */
    free(handle) {
      wasm.ccall('wasm_pipeline_free', null, ['number'], [handle])
    },

    /** Run pipeline with native streaming engine */
    run(dsl, data, options) {
      options = options || {}
      var chunkSize = options.chunkSize || 65536
      if (chunkSize <= 0) throw new Error('chunkSize must be positive')
      var onOutput = options.onOutput
      if (onOutput && typeof onOutput !== 'function') throw new TypeError('onOutput must be a function')
      var collectOutput = options.collectOutput !== false
      var allowBlocking = options.allowBlocking === true

      var planJson = this.compileDsl(dsl)
      planJson = prepareNativePlan(planJson, { allowBlocking: allowBlocking, memory: options.memory, spillDir: options.spillDir })
      var handle = this.createPipeline(planJson, hostPolicyOptions(options))
      var outputChunks = []
      var self = this
      function drainMain() {
        for (;;) {
          var chunk = self.pullChunk(handle, 0, chunkSize)
          if (chunk.length === 0) break
          if (onOutput) onOutput(chunk)
          if (collectOutput) outputChunks.push(chunk)
        }
      }

      try {
        var buf = toBytes(data || '')
        for (var i = 0; i < buf.length; i += chunkSize) {
          this.push(handle, buf.subarray(i, Math.min(i + chunkSize, buf.length)))
          drainMain()
        }
        for (;;) {
          var done = this.finishStep(handle)
          drainMain()
          if (done) break
        }
        var output = collectOutput ? concatUint8(outputChunks) : new Uint8Array(0)
        return {
          output: output,
          errors: this.pull(handle, 1),
          stats: this.pull(handle, 2),
          samples: this.pull(handle, 3),
          get outputText() { return new TextDecoder().decode(this.output) },
          get statsText() { return new TextDecoder().decode(this.stats) }
        }
      } finally {
        this.free(handle)
      }
    },

    /** Yield native main-output chunks while a WASM pipeline runs */
    *iterChunks(dsl, data, options) {
      options = options || {}
      var chunkSize = options.chunkSize || 65536
      if (chunkSize <= 0) throw new Error('chunkSize must be positive')
      var allowBlocking = options.allowBlocking === true
      var planJson = this.compileDsl(dsl)
      planJson = prepareNativePlan(planJson, { allowBlocking: allowBlocking, memory: options.memory, spillDir: options.spillDir })
      var handle = this.createPipeline(planJson, hostPolicyOptions(options))
      try {
        var buf = toBytes(data || '')
        for (var i = 0; i < buf.length; i += chunkSize) {
          this.push(handle, buf.subarray(i, Math.min(i + chunkSize, buf.length)))
          for (;;) {
            var chunk = this.pullChunk(handle, 0, chunkSize)
            if (chunk.length === 0) break
            yield chunk
          }
        }
        for (;;) {
          var done = this.finishStep(handle)
          for (;;) {
            var out = this.pullChunk(handle, 0, chunkSize)
            if (out.length === 0) break
            yield out
          }
          if (done) break
        }
      } finally {
        this.free(handle)
      }
    },

    /**
     * Run pipeline via DuckDB-WASM.
     *
     * @param {object} db — An initialized @duckdb/duckdb-wasm AsyncDuckDB or DuckDB instance
     * @param {string} dsl — Tranfi DSL string
     * @param {string|Uint8Array|File} data — CSV input data
     * @returns {Promise<{output: Uint8Array, outputText: string, rows: Array}>}
     */
    async runDuckDB(db, dsl, data) {
      var sql = this.compileToSql(dsl)
      var conn = await db.connect()

      try {
        // Register input data as a table
        if (typeof data === 'string' || data instanceof Uint8Array) {
          var buf = data instanceof Uint8Array ? data : new TextEncoder().encode(data)
          await db.registerFileBuffer('input.csv', buf)
          sql = sql.replaceAll('input_data', "read_csv('input.csv')")
        } else if (data && data.name) {
          // File object
          await db.registerFileHandle(data.name, data)
          sql = sql.replaceAll('input_data', "read_csv('" + data.name + "')")
        } else {
          throw new Error('data must be a string, Uint8Array, or File')
        }

        var result = await conn.query(sql)
        var rows = result.toArray().map(function(row) { return row.toJSON() })

        // Convert to CSV
        var output = ''
        if (rows.length > 0) {
          var cols = Object.keys(rows[0])
          output = cols.join(',') + '\n'
          for (var i = 0; i < rows.length; i++) {
            var vals = cols.map(function(c) {
              var v = rows[i][c]
              if (v === null || v === undefined) return ''
              var s = String(v)
              if (s.indexOf(',') >= 0 || s.indexOf('"') >= 0 || s.indexOf('\n') >= 0) {
                return '"' + s.replace(/"/g, '""') + '"'
              }
              return s
            })
            output += vals.join(',') + '\n'
          }
        }

        var outputBytes = new TextEncoder().encode(output)
        return {
          output: outputBytes,
          rows: rows,
          get outputText() { return output }
        }
      } finally {
        await conn.close()
      }
    },

    /** Recipe helpers */
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
      var s = allocString(name)
      var result = wasm.ccall('wasm_recipe_find_dsl', 'string', ['number'], [s.ptr])
      wasm._free(s.ptr)
      return result || null
    },
    recipes() {
      var n = this.recipeCount()
      var result = []
      for (var i = 0; i < n; i++) {
        result.push({
          name: this.recipeName(i),
          dsl: this.recipeDsl(i),
          description: this.recipeDescription(i)
        })
      }
      return result
    }
  }
}

// Support both CJS and ESM
if (typeof module !== 'undefined') module.exports = createTranfi
if (typeof exports !== 'undefined') exports.default = createTranfi
