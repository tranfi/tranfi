/**
 * Prepared, reusable typed transforms backed by Tranfi's C engine.
 *
 * These objects are separate from the byte-stream Pipeline API. Recipes analyze
 * one or more typed batches, finalize an immutable plan, and apply that plan to
 * a second pass over the reference data or to later compatible batches.
 */

const nativeBinding = require('./native.js')
const { TranfiTransformError } = require('./transform_error.js')

const SCHEMA_INPUT = 1
const SCHEMA_OUTPUT = 2

const LIMIT_FIELDS = new Set([
  'maxRecipeBytes',
  'maxPlanBytes',
  'maxJsonDepth',
  'maxObjectKeys',
  'maxSteps',
  'maxInputColumns',
  'maxOutputColumns',
  'maxCategoriesPerColumn',
  'maxTotalCategories',
  'maxStringBytes',
  'maxDecodedStringBytes',
  'maxAnalyzerRows',
  'maxAnalyzerInputBytes',
  'maxResidentStateBytes',
  'maxSpillBytes',
  'maxApplyRows',
  'maxApplyInputBytes',
  'maxOutputElementsPerCall',
  'maxAllocationBytes',
  'maxAllocationsPerSession',
  'maxLiveHandles',
  'maxRetiredHandleSlots'
])

function requireNative() {
  if (!nativeBinding || typeof nativeBinding.transformRecipeFromJson !== 'function') {
    throw new Error('prepared transforms require the Tranfi native addon; WASM support is not available in this build')
  }
  return nativeBinding
}

function callNative(name, ...args) {
  const binding = requireNative()
  try {
    return binding[name](...args)
  } catch (error) {
    if (error && error.name === 'TranfiTransformError' && Number.isInteger(error.code)) {
      throw new TranfiTransformError(error.code, error.message, error)
    }
    throw error
  }
}

function assertPlainObject(value, label) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new TypeError(`${label} must be an object`)
  }
}

function assertWellFormedUnicode(value, label, runtime, rejectNul = false) {
  for (let index = 0; index < value.length; index++) {
    if (index % 4096 === 0) throwIfCancelled(runtime)
    const unit = value.charCodeAt(index)
    if (rejectNul && unit === 0) {
      throw new TypeError(`${label} must not contain NUL`)
    }
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const next = index + 1 < value.length ? value.charCodeAt(index + 1) : 0
      if (next < 0xdc00 || next > 0xdfff) {
        throw new TypeError(`${label} must not contain an unpaired UTF-16 surrogate`)
      }
      index++
    } else if (unit >= 0xdc00 && unit <= 0xdfff) {
      throw new TypeError(`${label} must not contain an unpaired UTF-16 surrogate`)
    }
  }
  throwIfCancelled(runtime)
  return value
}

function normalizeLimits(limits) {
  if (limits === undefined || limits === null) return undefined
  assertPlainObject(limits, 'limits')
  const normalized = {}
  for (const [name, value] of Object.entries(limits)) {
    if (!LIMIT_FIELDS.has(name)) throw new TypeError(`unknown transform limit field: ${name}`)
    if (!Number.isSafeInteger(value) || value <= 0) {
      throw new TranfiTransformError(100, `${name} must be a positive safe integer`)
    }
    normalized[name] = value
  }
  return normalized
}

function safeTransformLimits(overrides = {}) {
  const normalized = normalizeLimits(overrides) || {}
  return { ...callNative('transformSafeLimits'), ...normalized }
}

function normalizeCancelFlag(cancelFlag) {
  if (cancelFlag === undefined || cancelFlag === null) return undefined
  const shared = typeof SharedArrayBuffer !== 'undefined'
    && cancelFlag instanceof Int32Array
    && Object.prototype.toString.call(cancelFlag.buffer) === '[object SharedArrayBuffer]'
  if (!shared || cancelFlag.length < 1) {
    throw new TypeError('cancelFlag must be a nonempty SharedArrayBuffer-backed Int32Array')
  }
  return cancelFlag
}

function normalizeRuntimeOptions(options = {}) {
  if (!options || typeof options !== 'object' || Array.isArray(options)) {
    throw new TypeError('transform runtime options must be an object')
  }
  const limits = normalizeLimits(options.limits)
  const cancelFlag = normalizeCancelFlag(options.cancelFlag)
  if (limits === undefined && cancelFlag === undefined) return undefined
  return { limits, cancelFlag }
}

function throwIfCancelled(runtime) {
  if (runtime && runtime.cancelFlag
      && Atomics.load(runtime.cancelFlag, 0) !== 0) {
    throw new TranfiTransformError(109, 'prepared transform cancelled')
  }
}

function encodeUtf8(value, label, runtime) {
  assertWellFormedUnicode(value, label, runtime, true)
  const chunks = []
  let total = 0
  for (let offset = 0; offset < value.length;) {
    throwIfCancelled(runtime)
    let end = Math.min(offset + 4096, value.length)
    if (end < value.length) {
      const last = value.charCodeAt(end - 1)
      if (last >= 0xd800 && last <= 0xdbff) end--
    }
    const chunk = Buffer.from(value.slice(offset, end), 'utf8')
    chunks.push(chunk)
    total += chunk.byteLength
    offset = end
  }
  throwIfCancelled(runtime)
  const encoded = Buffer.allocUnsafe(total)
  let destination = 0
  for (const chunk of chunks) {
    throwIfCancelled(runtime)
    chunk.copy(encoded, destination)
    destination += chunk.byteLength
  }
  throwIfCancelled(runtime)
  return encoded
}

function normalizeSchema(schema, runtime, limits) {
  const fields = Array.isArray(schema)
    ? schema
    : schema && typeof schema === 'object' && Array.isArray(schema.fields)
      ? schema.fields
      : null
  if (!fields || fields.length === 0) {
    throw new TypeError('schema must be a nonempty array or { fields } object')
  }
  if (fields.length > limits.maxInputColumns) {
    throw new TranfiTransformError(104, 'prepared-transform input column limit exceeded')
  }
  const normalized = []
  let decodedBytes = 0
  for (let index = 0; index < fields.length; index++) {
    if (index % 4096 === 0) throwIfCancelled(runtime)
    const field = fields[index]
    assertPlainObject(field, `schema field ${index}`)
    const id = field.id
    const name = field.name === undefined ? id : field.name
    const dtype = field.dtype
    if (typeof id !== 'string' || id.length === 0) {
      throw new TypeError(`schema field ${index} id must be a nonempty string`)
    }
    if (typeof name !== 'string' || name.length === 0) {
      throw new TypeError(`schema field ${index} name must be a nonempty string`)
    }
    if (id.length > limits.maxStringBytes || name.length > limits.maxStringBytes) {
      throw new TranfiTransformError(104, 'prepared-transform schema string limit exceeded')
    }
    const idBytes = encodeUtf8(id, `schema field ${index} id`, runtime)
    const nameBytes = encodeUtf8(name, `schema field ${index} name`, runtime)
    if (idBytes.byteLength > limits.maxStringBytes
        || nameBytes.byteLength > limits.maxStringBytes
        || idBytes.byteLength >= limits.maxAllocationBytes
        || nameBytes.byteLength >= limits.maxAllocationBytes) {
      throw new TranfiTransformError(104, 'prepared-transform schema string limit exceeded')
    }
    decodedBytes += idBytes.byteLength + nameBytes.byteLength
    if (decodedBytes > limits.maxDecodedStringBytes) {
      throw new TranfiTransformError(104, 'prepared-transform decoded string limit exceeded')
    }
    if (dtype !== 'float32' && dtype !== 'float64') {
      throw new TypeError(`schema field ${index} dtype must be float32 or float64`)
    }
    normalized.push({ id: idBytes, name: nameBytes, dtype })
  }
  throwIfCancelled(runtime)
  return {
    fields: normalized
  }
}

function normalizeRecipeJSON(recipe) {
  if (typeof recipe === 'string') {
    return assertWellFormedUnicode(recipe, 'recipe')
  }
  if (Buffer.isBuffer(recipe) || recipe instanceof Uint8Array) {
    return recipe
  }
  assertPlainObject(recipe, 'recipe')
  return JSON.stringify(recipe, (key, value) => {
    assertWellFormedUnicode(key, 'recipe key')
    if (typeof value === 'string') assertWellFormedUnicode(value, 'recipe string')
    return value
  })
}

class OwnedTransform {
  constructor(handle) {
    this._handle = handle
  }

  _requireOpen() {
    if (this._handle === null) {
      throw new TranfiTransformError(112, 'prepared transform object is closed')
    }
    return this._handle
  }

  close() {
    if (this._handle !== null) {
      callNative('transformDispose', this._handle)
      this._handle = null
    }
  }

  dispose() {
    this.close()
  }
}

class TransformRecipe extends OwnedTransform {
  static fromJSON(recipe, { limits } = {}) {
    return new TransformRecipe(callNative(
      'transformRecipeFromJson', normalizeRecipeJSON(recipe), normalizeLimits(limits)
    ))
  }

  analyzer(schema, options = {}) {
    const recipeHandle = this._requireOpen()
    const runtime = normalizeRuntimeOptions(options)
    throwIfCancelled(runtime)
    const effectiveLimits = safeTransformLimits(runtime && runtime.limits)
    throwIfCancelled(runtime)
    const normalizedSchema = normalizeSchema(schema, runtime, effectiveLimits)
    throwIfCancelled(runtime)
    return new TransformAnalyzer(callNative(
      'transformAnalyzerCreate', recipeHandle, normalizedSchema, runtime
    ))
  }
}

class TransformAnalyzer extends OwnedTransform {
  push(table) {
    callNative('transformAnalyzerPush', this._requireOpen(), table)
    return this
  }

  finalize() {
    return new TransformPlan(callNative('transformAnalyzerFinalize', this._requireOpen()))
  }
}

class TransformPlan extends OwnedTransform {
  static fromBytes(bytes, options = {}) {
    return new TransformPlan(callNative(
      'transformPlanImport', bytes, normalizeRuntimeOptions(options)
    ))
  }

  toBytes({ limits } = {}) {
    return callNative('transformPlanExport', this._requireOpen(), normalizeLimits(limits))
  }

  schemaJSON(which = 'output', { limits } = {}) {
    const selector = which === 'input'
      ? SCHEMA_INPUT
      : which === 'output'
        ? SCHEMA_OUTPUT
        : null
    if (selector === null) throw new TypeError("which must be 'input' or 'output'")
    return callNative(
      'transformPlanSchemaJson', this._requireOpen(), selector, normalizeLimits(limits)
    )
  }

  recipeSha256({ limits } = {}) {
    return callNative(
      'transformPlanRecipeSha256', this._requireOpen(), normalizeLimits(limits)
    )
  }

  apply(schema, options = {}) {
    const planHandle = this._requireOpen()
    const runtime = normalizeRuntimeOptions(options)
    throwIfCancelled(runtime)
    const effectiveLimits = safeTransformLimits(runtime && runtime.limits)
    throwIfCancelled(runtime)
    const normalizedSchema = normalizeSchema(schema, runtime, effectiveLimits)
    throwIfCancelled(runtime)
    return new TransformApply(callNative(
      'transformApplyCreate', planHandle, normalizedSchema, runtime
    ))
  }
}

class TransformApply extends OwnedTransform {
  run(table) {
    return callNative('transformApplyRun', this._requireOpen(), table)
  }
}

module.exports = {
  TranfiTransformError,
  TransformRecipe,
  TransformAnalyzer,
  TransformPlan,
  TransformApply,
  safeTransformLimits
}
