/**
 * Prepared, reusable typed transforms backed by Tranfi's C engine.
 *
 * These objects are separate from the byte-stream Pipeline API. Recipes analyze
 * one or more typed batches, finalize an immutable plan, and apply that plan to
 * a second pass over the reference data or to later compatible batches.
 */

const { stringifyRecipe } = require('./recipe_json.js')
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
const RUNTIME_OPTION_FIELDS = new Set([
  'limits', 'cancelFlag', 'hostPolicy', 'spillDir'
])

// Check availability without allocating handles or swallowing runtime errors.
const NATIVE_PREPARED_METHODS = [
  'transformSafeLimits', 'transformDispose', 'transformRecipeFromJson',
  'transformAnalyzerCreate', 'transformAnalyzerPush', 'transformAnalyzerFinalize',
  'transformPlanImport', 'transformPlanExport', 'transformPlanSchemaJson',
  'transformPlanRecipeSha256', 'transformApplyCreate', 'transformApplyRun'
]

function hasNativePreparedTransforms() {
  return Boolean(nativeBinding) && NATIVE_PREPARED_METHODS.every(
    name => typeof nativeBinding[name] === 'function'
  )
}

function requireNative() {
  if (!hasNativePreparedTransforms()) {
    throw new Error("prepared transforms on require('tranfi') need a compatible native addon; use await require('tranfi/wasm')()")
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

function utf8ByteLength(value, label, runtime, rejectNul = false, maxBytes = null) {
  let bytes = 0
  for (let index = 0; index < value.length; index++) {
    if (index % 4096 === 0) throwIfCancelled(runtime)
    const unit = value.charCodeAt(index)
    if (rejectNul && unit === 0) {
      throw new TypeError(`${label} must not contain NUL`)
    }
    if (unit <= 0x7f) {
      bytes += 1
    } else if (unit <= 0x7ff) {
      bytes += 2
    } else if (unit >= 0xd800 && unit <= 0xdbff) {
      const next = index + 1 < value.length ? value.charCodeAt(index + 1) : 0
      if (next < 0xdc00 || next > 0xdfff) {
        throw new TypeError(`${label} must not contain an unpaired UTF-16 surrogate`)
      }
      bytes += 4
      index++
    } else if (unit >= 0xdc00 && unit <= 0xdfff) {
      throw new TypeError(`${label} must not contain an unpaired UTF-16 surrogate`)
    } else {
      bytes += 3
    }
    if (maxBytes !== null && bytes > maxBytes) {
      throw new TranfiTransformError(104, `${label} exceeds its configured byte limit`)
    }
  }
  throwIfCancelled(runtime)
  return bytes
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
  for (const name of Object.keys(options)) {
    if (!RUNTIME_OPTION_FIELDS.has(name)) {
      throw new TypeError(`unknown transform runtime option: ${name}`)
    }
  }
  if (options.hostPolicy !== undefined && options.hostPolicy !== null) {
    throw new TranfiTransformError(
      113, 'prepared-transform hostPolicy is reserved but not implemented'
    )
  }
  if (options.spillDir !== undefined && options.spillDir !== null &&
      options.spillDir !== '') {
    throw new TranfiTransformError(
      113, 'prepared-transform spillDir is reserved but not implemented'
    )
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

function encodeUtf8(value, label, runtime, maxBytes, knownLength = null) {
  const total = knownLength === null
    ? utf8ByteLength(value, label, runtime, true, maxBytes)
    : knownLength
  const encoded = Buffer.allocUnsafe(total)
  let destination = 0
  for (let offset = 0; offset < value.length;) {
    throwIfCancelled(runtime)
    let end = Math.min(offset + 4096, value.length)
    if (end < value.length) {
      const last = value.charCodeAt(end - 1)
      if (last >= 0xd800 && last <= 0xdbff) end--
    }
    destination += encoded.write(
      value.slice(offset, end), destination, total - destination, 'utf8'
    )
    offset = end
  }
  throwIfCancelled(runtime)
  if (destination !== total) {
    throw new Error(`internal UTF-8 length mismatch for ${label}`)
  }
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
  const prepared = []
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
    if (dtype !== 'float32' && dtype !== 'float64') {
      throw new TypeError(`schema field ${index} dtype must be float32 or float64`)
    }
    const maxEncodedBytes = Math.min(
      limits.maxStringBytes, limits.maxAllocationBytes - 1
    )
    const idLength = utf8ByteLength(
      id, `schema field ${index} id`, runtime, true, maxEncodedBytes
    )
    const nameLength = utf8ByteLength(
      name, `schema field ${index} name`, runtime, true, maxEncodedBytes
    )
    if (decodedBytes + idLength + nameLength > limits.maxDecodedStringBytes) {
      throw new TranfiTransformError(104, 'prepared-transform decoded string limit exceeded')
    }
    decodedBytes += idLength + nameLength
    prepared.push({ id, name, dtype, idLength, nameLength })
  }
  throwIfCancelled(runtime)

  const normalized = []
  for (let index = 0; index < prepared.length; index++) {
    if (index % 4096 === 0) throwIfCancelled(runtime)
    const { id, name, dtype, idLength, nameLength } = prepared[index]
    const maxEncodedBytes = Math.min(
      limits.maxStringBytes, limits.maxAllocationBytes - 1
    )
    const idBytes = encodeUtf8(
      id, `schema field ${index} id`, runtime, maxEncodedBytes, idLength
    )
    const nameBytes = encodeUtf8(
      name, `schema field ${index} name`, runtime, maxEncodedBytes, nameLength
    )
    normalized.push({ id: idBytes, name: nameBytes, dtype })
  }
  throwIfCancelled(runtime)
  return {
    fields: normalized
  }
}

function normalizeRecipeJSON(recipe, limits) {
  if (typeof recipe === 'string') {
    return assertWellFormedUnicode(recipe, 'recipe')
  }
  if (Buffer.isBuffer(recipe) || recipe instanceof Uint8Array) {
    return recipe
  }
  assertPlainObject(recipe, 'recipe')
  return stringifyRecipe(recipe, limits)
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
    const normalized = normalizeLimits(limits)
    const effective = safeTransformLimits(normalized)
    return new TransformRecipe(callNative(
      'transformRecipeFromJson', normalizeRecipeJSON(recipe, effective), normalized
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
  safeTransformLimits,
  hasNativePreparedTransforms
}
