'use strict'

const { TranfiTransformError } = require('../src/transform_error.js')

const VIEW_FLOAT32 = 0x0101
const VIEW_FLOAT64 = 0x0102
const SCHEMA_INPUT = 1
const SCHEMA_OUTPUT = 2
const NO_HOST_CANCEL = 0xffffffff

const LIMIT_FIELDS = [
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
]
const LIMIT_FIELD_SET = new Set(LIMIT_FIELDS)

const ERROR_NAMES = {
  100: 'invalid prepared-transform argument',
  101: 'invalid prepared-transform recipe',
  102: 'prepared-transform schema mismatch',
  103: 'insufficient data to finalize prepared transform',
  104: 'prepared-transform resource limit exceeded',
  105: 'unsupported prepared-transform version',
  106: 'corrupt prepared-transform plan',
  107: 'prepared-transform numeric domain error',
  108: 'unknown prepared-transform category',
  109: 'prepared transform cancelled',
  110: 'prepared-transform allocation failed',
  111: 'prepared-transform I/O failed',
  112: 'prepared-transform object is closed or terminal',
  113: 'unsupported prepared-transform runtime',
  199: 'internal prepared-transform error'
}

function createTransformAPI(wasm) {
  var encoder = new TextEncoder()
  var decoder = new TextDecoder()
  var cancelPolls = new Map()
  var nextCancelSlot = 1
  var safeLimitProfile = null

  wasm.tfTransformCancelPoll = function(slot) {
    var poll = cancelPolls.get(slot >>> 0)
    if (!poll) return false
    if (poll.observer) {
      var observer = poll.observer
      poll.observer = null
      observer()
    }
    return Atomics.load(poll.flag, 0) !== 0
  }

  function dataView() {
    return new DataView(wasm.HEAPU8.buffer)
  }

  function readU32(offset) {
    return dataView().getUint32(offset, true)
  }

  function writeU32(offset, value) {
    dataView().setUint32(offset, value >>> 0, true)
  }

  function readU64(offset) {
    var value = dataView().getBigUint64(offset, true)
    if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
      throw new RangeError('transform limit exceeds JavaScript safe integer range')
    }
    return Number(value)
  }

  function writeU64(offset, value) {
    dataView().setBigUint64(offset, BigInt(value), true)
  }

  function alloc(size, alignment) {
    alignment = alignment || 1
    if (!Number.isSafeInteger(size) || size < 0
        || !Number.isSafeInteger(alignment) || alignment <= 0
        || size > 0xffffffff - alignment + 1) {
      throw resourceLimit('WASM transport allocation exceeds wasm32')
    }
    var actualSize = Math.max(1, size + alignment - 1)
    var base = wasm._malloc(actualSize) >>> 0
    if (!base) throw new TranfiTransformError(110, 'WASM allocation failed')
    var pointer = Math.ceil(base / alignment) * alignment
    return { base: base >>> 0, pointer: pointer >>> 0, size: size >>> 0 }
  }

  function free(owner) {
    if (owner && owner.base) wasm._free(owner.base)
  }

  function allocBytes(bytes, alignment, cancelToken) {
    throwIfCancelled(cancelToken)
    var owner = alloc(bytes.byteLength, alignment || 1)
    try {
      for (var offset = 0; offset < bytes.byteLength; offset += 65536) {
        throwIfCancelled(cancelToken)
        var end = Math.min(offset + 65536, bytes.byteLength)
        wasm.HEAPU8.set(bytes.subarray(offset, end), owner.pointer + offset)
      }
      throwIfCancelled(cancelToken)
      return owner
    } catch (error) {
      free(owner)
      throw error
    }
  }

  function allocWord() {
    var owner = alloc(4, 4)
    writeU32(owner.pointer, 0)
    return owner
  }

  function raw(name, args) {
    var types = new Array(args.length).fill('number')
    return wasm.ccall(name, 'number', types, args) >>> 0
  }

  function destroyHandle(handle) {
    return raw('tf_wasm_transform_handle_destroy', [handle >>> 0])
  }

  function errorFromHandle(code, handle) {
    var message = ERROR_NAMES[code] || ('prepared transform failed with code ' + code)
    handle = handle >>> 0
    if (!handle) return new TranfiTransformError(code, message)
    var sizeOwner = null
    var messageOwner = null
    try {
      sizeOwner = allocWord()
      if (raw('tf_wasm_transform_error_message_size', [handle, sizeOwner.pointer]) === 0) {
        var length = readU32(sizeOwner.pointer)
        messageOwner = alloc(length, 1)
        if (raw('tf_wasm_transform_error_message_copy', [
          handle, messageOwner.pointer, length
        ]) === 0) {
          message = decoder.decode(wasm.HEAPU8.slice(
            messageOwner.pointer, messageOwner.pointer + length
          ))
        }
      }
    } finally {
      free(messageOwner)
      free(sizeOwner)
      destroyHandle(handle)
    }
    return new TranfiTransformError(code, message)
  }

  function callWithError(name, args) {
    var errorOwner = allocWord()
    try {
      var code = raw(name, args.concat([errorOwner.pointer]))
      if (code !== 0) throw errorFromHandle(code, readU32(errorOwner.pointer))
    } finally {
      free(errorOwner)
    }
  }

  function callForHandle(name, args) {
    var outputOwner = allocWord()
    var errorOwner = null
    try {
      errorOwner = allocWord()
      var code = raw(name, args.concat([outputOwner.pointer, errorOwner.pointer]))
      if (code !== 0) throw errorFromHandle(code, readU32(errorOwner.pointer))
      return readU32(outputOwner.pointer) >>> 0
    } finally {
      free(errorOwner)
      free(outputOwner)
    }
  }

  function normalizeLimits(limits) {
    if (limits === undefined || limits === null) return null
    if (!limits || typeof limits !== 'object' || Array.isArray(limits)) {
      throw new TypeError('limits must be an object')
    }
    var normalized = {}
    Object.keys(limits).forEach(function(name) {
      if (!LIMIT_FIELD_SET.has(name)) {
        throw new TypeError('unknown transform limit field: ' + name)
      }
      var value = limits[name]
      if (!Number.isSafeInteger(value) || value <= 0) {
        throw new TranfiTransformError(
          100, name + ' must be a positive safe integer'
        )
      }
      normalized[name] = value
    })
    return normalized
  }

  function safeTransformLimits(overrides) {
    var normalized = normalizeLimits(overrides || {}) || {}
    if (safeLimitProfile === null) {
      var owner = alloc(184, 8)
      try {
        var code = raw('tf_wasm_transform_limits_init_safe_v1', [owner.pointer, 184])
        if (code !== 0) throw new TranfiTransformError(code, ERROR_NAMES[code])
        safeLimitProfile = {}
        for (var i = 0; i < LIMIT_FIELDS.length; i++) {
          safeLimitProfile[LIMIT_FIELDS[i]] = readU64(owner.pointer + 8 + i * 8)
        }
      } finally {
        free(owner)
      }
    }
    return Object.assign({}, safeLimitProfile, normalized)
  }

  function limitsOwner(limits, resolved) {
    var normalized = normalizeLimits(limits)
    if (normalized === null) return null
    var values = resolved || safeTransformLimits(normalized)
    var owner = alloc(184, 8)
    try {
      var code = raw('tf_wasm_transform_limits_init_safe_v1', [owner.pointer, 184])
      if (code !== 0) throw new TranfiTransformError(code, ERROR_NAMES[code])
      for (var i = 0; i < LIMIT_FIELDS.length; i++) {
        writeU64(owner.pointer + 8 + i * 8, values[LIMIT_FIELDS[i]])
      }
      return owner
    } catch (error) {
      free(owner)
      throw error
    }
  }

  function resourceLimit(message) {
    return new TranfiTransformError(104, message)
  }

  function resolvedLimits(limits) {
    return safeTransformLimits(limits || {})
  }

  function assertWellFormedUnicode(value, label, cancelToken, rejectNul) {
    for (var index = 0; index < value.length; index++) {
      if (index % 4096 === 0) throwIfCancelled(cancelToken)
      var unit = value.charCodeAt(index)
      if (rejectNul && unit === 0) {
        throw new TypeError(label + ' must not contain NUL')
      }
      if (unit >= 0xd800 && unit <= 0xdbff) {
        var next = index + 1 < value.length ? value.charCodeAt(index + 1) : 0
        if (next < 0xdc00 || next > 0xdfff) {
          throw new TypeError(label + ' must not contain an unpaired UTF-16 surrogate')
        }
        index++
      } else if (unit >= 0xdc00 && unit <= 0xdfff) {
        throw new TypeError(label + ' must not contain an unpaired UTF-16 surrogate')
      }
    }
    throwIfCancelled(cancelToken)
    return value
  }

  function utf8ByteLength(value, label, cancelToken, rejectNul, maxBytes) {
    var bytes = 0
    for (var index = 0; index < value.length; index++) {
      if (index % 4096 === 0) throwIfCancelled(cancelToken)
      var unit = value.charCodeAt(index)
      if (rejectNul && unit === 0) {
        throw new TypeError(label + ' must not contain NUL')
      }
      if (unit <= 0x7f) {
        bytes += 1
      } else if (unit <= 0x7ff) {
        bytes += 2
      } else if (unit >= 0xd800 && unit <= 0xdbff) {
        var next = index + 1 < value.length ? value.charCodeAt(index + 1) : 0
        if (next < 0xdc00 || next > 0xdfff) {
          throw new TypeError(label + ' must not contain an unpaired UTF-16 surrogate')
        }
        bytes += 4
        index++
      } else if (unit >= 0xdc00 && unit <= 0xdfff) {
        throw new TypeError(label + ' must not contain an unpaired UTF-16 surrogate')
      } else {
        bytes += 3
      }
      if (maxBytes !== undefined && bytes > maxBytes) {
        throw resourceLimit(label + ' exceeds its configured byte limit')
      }
    }
    throwIfCancelled(cancelToken)
    return bytes
  }

  function encodeUtf8(value, label, cancelToken, maxBytes, knownLength) {
    var total = knownLength === undefined
      ? utf8ByteLength(value, label, cancelToken, true, maxBytes)
      : knownLength
    var encoded = new Uint8Array(total)
    var destination = 0
    for (var offset = 0; offset < value.length;) {
      throwIfCancelled(cancelToken)
      var end = Math.min(offset + 4096, value.length)
      if (end < value.length) {
        var last = value.charCodeAt(end - 1)
        if (last >= 0xd800 && last <= 0xdbff) end--
      }
      var chunk = value.slice(offset, end)
      var result = encoder.encodeInto(chunk, encoded.subarray(destination))
      if (result.read !== chunk.length) {
        throw new Error('internal UTF-8 destination mismatch for ' + label)
      }
      destination += result.written
      offset = end
    }
    throwIfCancelled(cancelToken)
    if (destination !== total) {
      throw new Error('internal UTF-8 length mismatch for ' + label)
    }
    return encoded
  }

  function normalizeSchema(schema, limits, cancelToken) {
    var fields = Array.isArray(schema)
      ? schema
      : schema && typeof schema === 'object' && Array.isArray(schema.fields)
        ? schema.fields
        : null
    if (!fields || fields.length === 0) {
      throw new TypeError('schema must be a nonempty array or { fields } object')
    }
    if (fields.length > limits.maxInputColumns) {
      throw resourceLimit('prepared-transform input column limit exceeded')
    }
    var normalized = []
    fields.forEach(function(field, index) {
      if (index % 4096 === 0) throwIfCancelled(cancelToken)
      if (!field || typeof field !== 'object' || Array.isArray(field)) {
        throw new TypeError('schema field ' + index + ' must be an object')
      }
      var id = field.id
      var name = field.name === undefined ? id : field.name
      if (typeof id !== 'string' || !id) {
        throw new TypeError('schema field ' + index + ' id must be a nonempty string')
      }
      if (typeof name !== 'string' || !name) {
        throw new TypeError('schema field ' + index + ' name must be a nonempty string')
      }
      assertWellFormedUnicode(
        id, 'schema field ' + index + ' id', cancelToken, true
      )
      assertWellFormedUnicode(
        name, 'schema field ' + index + ' name', cancelToken, true
      )
      if (field.dtype !== 'float32' && field.dtype !== 'float64') {
        throw new TypeError('schema field ' + index + ' dtype must be float32 or float64')
      }
      normalized.push({ id: id, name: name, dtype: field.dtype })
    })
    throwIfCancelled(cancelToken)
    return normalized
  }

  function schemaOwner(schema, limits, cancelToken) {
    var fields = normalizeSchema(schema, limits, cancelToken)
    var descriptorBytes = fields.length * 32
    if (descriptorBytes > limits.maxAllocationBytes) {
      throw resourceLimit('prepared-transform schema allocation limit exceeded')
    }
    var decodedBytes = 0
    var preparedFields = fields.map(function(field, index) {
      if (index % 4096 === 0) throwIfCancelled(cancelToken)
      var maxEncodedBytes = Math.min(
        limits.maxStringBytes, limits.maxAllocationBytes - 1
      )
      var idLength = utf8ByteLength(
        field.id, 'schema field ' + index + ' id', cancelToken, true,
        maxEncodedBytes
      )
      var nameLength = utf8ByteLength(
        field.name, 'schema field ' + index + ' name', cancelToken, true,
        maxEncodedBytes
      )
      if (decodedBytes + idLength + nameLength > limits.maxDecodedStringBytes) {
        throw resourceLimit('prepared-transform decoded string limit exceeded')
      }
      decodedBytes += idLength + nameLength
      return { field: field, idLength: idLength, nameLength: nameLength }
    })
    throwIfCancelled(cancelToken)
    var encodedFields = preparedFields.map(function(prepared, index) {
      if (index % 4096 === 0) throwIfCancelled(cancelToken)
      var field = prepared.field
      var maxEncodedBytes = Math.min(
        limits.maxStringBytes, limits.maxAllocationBytes - 1
      )
      return {
        field: field,
        id: encodeUtf8(
          field.id, 'schema field ' + index + ' id', cancelToken,
          maxEncodedBytes, prepared.idLength
        ),
        name: encodeUtf8(
          field.name, 'schema field ' + index + ' name', cancelToken,
          maxEncodedBytes, prepared.nameLength
        )
      }
    })
    var allocations = []
    var dtypes = []
    var fieldOwner = null
    var schemaAllocation = null
    try {
      fieldOwner = alloc(descriptorBytes, 4)
      allocations.push(fieldOwner)
      schemaAllocation = alloc(20, 4)
      allocations.push(schemaAllocation)
      encodedFields.forEach(function(encoded, index) {
        if (index % 4096 === 0) throwIfCancelled(cancelToken)
        var field = encoded.field
        var idOwner = allocBytes(encoded.id, 1, cancelToken)
        allocations.push(idOwner)
        var nameOwner = allocBytes(encoded.name, 1, cancelToken)
        allocations.push(nameOwner)
        var offset = fieldOwner.pointer + index * 32
        writeU32(offset, 1)
        writeU32(offset + 4, 32)
        writeU32(offset + 8, field.dtype === 'float32' ? VIEW_FLOAT32 : VIEW_FLOAT64)
        writeU32(offset + 12, 0)
        writeU32(offset + 16, idOwner.pointer)
        writeU32(offset + 20, idOwner.size)
        writeU32(offset + 24, nameOwner.pointer)
        writeU32(offset + 28, nameOwner.size)
        dtypes.push(field.dtype)
      })
      writeU32(schemaAllocation.pointer, 1)
      writeU32(schemaAllocation.pointer + 4, 20)
      writeU32(schemaAllocation.pointer + 8, fields.length)
      writeU32(schemaAllocation.pointer + 12, fieldOwner.pointer)
      writeU32(schemaAllocation.pointer + 16, descriptorBytes)
      return {
        pointer: schemaAllocation.pointer,
        dtypes: dtypes,
        free: function() { allocations.reverse().forEach(free) }
      }
    } catch (error) {
      allocations.reverse().forEach(free)
      throw error
    }
  }

  function tableOwner(table, dtypes, limits, phase, cancelToken) {
    throwIfCancelled(cancelToken)
    if (!table || typeof table !== 'object' || Array.isArray(table)) {
      throw new TypeError('table must be an object with rows and columns')
    }
    var rows = table.rows
    var columns = table.columns
    if (!Number.isInteger(rows) || rows < 0 || rows > 0xffffffff) {
      throw new TypeError('table.rows must be a nonnegative uint32 integer')
    }
    if (!Array.isArray(columns) || columns.length !== dtypes.length) {
      throw new TypeError('table column count does not match the prepared schema')
    }
    var maxRows = phase === 'analyze'
      ? limits.maxAnalyzerRows
      : limits.maxApplyRows
    var maxInputBytes = phase === 'analyze'
      ? limits.maxAnalyzerInputBytes
      : limits.maxApplyInputBytes
    if (rows > maxRows) {
      throw resourceLimit('prepared-transform row limit exceeded')
    }
    var descriptorBytes = columns.length * 36
    if (descriptorBytes > limits.maxAllocationBytes) {
      throw resourceLimit('prepared-transform table allocation limit exceeded')
    }
    var totalInputBytes = 0n
    var prepared = columns.map(function(value, index) {
      if (index % 4096 === 0) throwIfCancelled(cancelToken)
      var config = ArrayBuffer.isView(value) ? { data: value } : value
      if (!config || typeof config !== 'object' || !ArrayBuffer.isView(config.data)) {
        throw new TypeError('table column ' + index + ' data must be a typed array')
      }
      var expected = dtypes[index]
      var ArrayType = expected === 'float32' ? Float32Array : Float64Array
      if (!(config.data instanceof ArrayType)) {
        throw new TypeError('table column ' + index + ' dtype mismatches schema')
      }
      if (rows === 0 && (config.data.byteLength !== 0
          || config.validity !== undefined && config.validity !== null
          || config.validityBitOffset !== undefined
          || config.validityBitStride !== undefined)) {
        throw new TranfiTransformError(
          100, 'zero-row column must carry empty data and no validity span'
        )
      }
      var itemSize = expected === 'float32' ? 4 : 8
      var stride = config.strideBytes === undefined ? itemSize : config.strideBytes
      if (!Number.isInteger(stride) || stride <= 0 || stride > 0xffffffff) {
        throw new TypeError('table column ' + index + ' strideBytes must be positive')
      }
      var requiredData = rows === 0
        ? 0n
        : BigInt(rows - 1) * BigInt(stride) + BigInt(itemSize)
      if (requiredData > BigInt(config.data.byteLength)) {
        throw new TypeError('table column ' + index + ' data buffer is too small')
      }
      if (requiredData > 0xffffffffn
          || requiredData > BigInt(limits.maxAllocationBytes)) {
        throw resourceLimit('prepared-transform input allocation limit exceeded')
      }

      var bitOffset = 0
      var bitStride = 0
      var requiredValidity = 0n
      if (config.validity !== undefined && config.validity !== null) {
        if (!(config.validity instanceof Uint8Array)) {
          throw new TypeError('table column ' + index + ' validity must be Uint8Array')
        }
        bitOffset = config.validityBitOffset === undefined ? 0 : config.validityBitOffset
        bitStride = config.validityBitStride === undefined ? 1 : config.validityBitStride
        if (!Number.isInteger(bitOffset) || bitOffset < 0 || bitOffset > 0xffffffff
            || !Number.isInteger(bitStride) || bitStride <= 0 || bitStride > 0xffffffff) {
          throw new TypeError('table column ' + index + ' validity bit metadata is invalid')
        }
        var requiredBits = rows === 0
          ? 0n
          : BigInt(bitOffset) + BigInt(rows - 1) * BigInt(bitStride) + 1n
        requiredValidity = (requiredBits + 7n) / 8n
        if (requiredValidity > BigInt(config.validity.byteLength)) {
          throw new TypeError('table column ' + index + ' validity buffer is too small')
        }
        if (requiredValidity > 0xffffffffn
            || requiredValidity > BigInt(limits.maxAllocationBytes)) {
          throw resourceLimit('prepared-transform validity allocation limit exceeded')
        }
      } else if (config.validityBitOffset !== undefined
                 || config.validityBitStride !== undefined) {
        throw new TypeError('validity bit metadata requires a validity bitmap')
      }
      totalInputBytes += requiredData + requiredValidity
      if (totalInputBytes > BigInt(maxInputBytes)) {
        throw resourceLimit('prepared-transform input byte limit exceeded')
      }
      return {
        config: config,
        itemSize: itemSize,
        stride: stride,
        dataBytes: Number(requiredData),
        validityBytes: Number(requiredValidity),
        bitOffset: bitOffset,
        bitStride: bitStride
      }
    })
    var allocations = []
    var columnsOwner = null
    var tableAllocation = null
    try {
      columnsOwner = alloc(descriptorBytes, 4)
      allocations.push(columnsOwner)
      tableAllocation = alloc(24, 4)
      allocations.push(tableAllocation)
      prepared.forEach(function(input, index) {
        if (index % 4096 === 0) throwIfCancelled(cancelToken)
        var config = input.config
        var bytes = new Uint8Array(
          config.data.buffer, config.data.byteOffset, input.dataBytes
        )
        var dataOwner = bytes.byteLength
          ? allocBytes(bytes, input.itemSize, cancelToken) : null
        if (dataOwner) allocations.push(dataOwner)
        var validityOwner = null
        if (input.validityBytes) {
          validityOwner = input.validityBytes
            ? allocBytes(new Uint8Array(
              config.validity.buffer,
              config.validity.byteOffset,
              input.validityBytes
            ), 1, cancelToken)
            : null
          if (validityOwner) allocations.push(validityOwner)
        }
        var offset = columnsOwner.pointer + index * 36
        writeU32(offset, 1)
        writeU32(offset + 4, 36)
        writeU32(offset + 8, dataOwner ? dataOwner.pointer : 0)
        writeU32(offset + 12, input.dataBytes)
        writeU32(offset + 16, input.stride)
        writeU32(offset + 20, validityOwner ? validityOwner.pointer : 0)
        writeU32(offset + 24, input.validityBytes)
        writeU32(offset + 28, input.bitOffset)
        writeU32(offset + 32, input.bitStride)
      })
      writeU32(tableAllocation.pointer, 1)
      writeU32(tableAllocation.pointer + 4, 24)
      writeU32(tableAllocation.pointer + 8, rows)
      writeU32(tableAllocation.pointer + 12, columns.length)
      writeU32(tableAllocation.pointer + 16, columnsOwner.pointer)
      writeU32(tableAllocation.pointer + 20, descriptorBytes)
      return {
        pointer: tableAllocation.pointer,
        free: function() { allocations.reverse().forEach(free) }
      }
    } catch (error) {
      allocations.reverse().forEach(free)
      throw error
    }
  }

  function bytesFromHandle(handle) {
    var sizeOwner = null
    var dataOwner = null
    try {
      sizeOwner = allocWord()
      var code = raw('tf_wasm_transform_owned_bytes_size', [handle, sizeOwner.pointer])
      if (code !== 0) throw new TranfiTransformError(code, ERROR_NAMES[code])
      var length = readU32(sizeOwner.pointer)
      dataOwner = alloc(length, 1)
      code = raw('tf_wasm_transform_owned_bytes_copy', [
        handle, dataOwner.pointer, length
      ])
      if (code !== 0) throw new TranfiTransformError(code, ERROR_NAMES[code])
      return wasm.HEAPU8.slice(dataOwner.pointer, dataOwner.pointer + length)
    } finally {
      free(dataOwner)
      free(sizeOwner)
      destroyHandle(handle)
    }
  }

  function denseFromHandle(handle, cancelToken) {
    var infoOwner = null
    var dataOwner = null
    try {
      infoOwner = alloc(28, 4)
      var code = raw('tf_wasm_transform_owned_dense_info', [handle, infoOwner.pointer])
      if (code !== 0) throw new TranfiTransformError(code, ERROR_NAMES[code])
      var dtype = readU32(infoOwner.pointer + 8)
      var rows = readU32(infoOwner.pointer + 16)
      var columns = readU32(infoOwner.pointer + 20)
      var byteLength = readU32(infoOwner.pointer + 24)
      if (dtype !== VIEW_FLOAT64 || byteLength % 8 !== 0
          || BigInt(rows) * BigInt(columns) * 8n !== BigInt(byteLength)) {
        throw new TranfiTransformError(199, 'invalid dense output from WASM')
      }
      throwIfCancelled(cancelToken)
      dataOwner = alloc(byteLength, 8)
      code = raw('tf_wasm_transform_owned_dense_copy', [
        handle, dataOwner.pointer, byteLength
      ])
      if (code !== 0) throw new TranfiTransformError(code, ERROR_NAMES[code])
      throwIfCancelled(cancelToken)
      var result = new Float64Array(byteLength / 8)
      var view = dataView()
      for (var i = 0; i < result.length; i++) {
        if (i % 8192 === 0) throwIfCancelled(cancelToken)
        result[i] = view.getFloat64(dataOwner.pointer + i * 8, true)
      }
      throwIfCancelled(cancelToken)
      return { rows: rows, columns: columns, data: result }
    } finally {
      free(dataOwner)
      free(infoOwner)
      destroyHandle(handle)
    }
  }

  function normalizeRecipeJSON(recipe, limits) {
    if (typeof recipe === 'string') {
      var stringLimit = Math.min(
        limits.maxRecipeBytes, limits.maxAllocationBytes - 1
      )
      var stringLength = utf8ByteLength(
        recipe, 'recipe', null, false, stringLimit
      )
      return encodeUtf8(recipe, 'recipe', null, stringLimit, stringLength)
    }
    if (recipe instanceof Uint8Array) {
      if (recipe.byteLength > limits.maxRecipeBytes
          || recipe.byteLength > limits.maxAllocationBytes) {
        throw resourceLimit('prepared-transform recipe byte limit exceeded')
      }
      return new Uint8Array(recipe.buffer, recipe.byteOffset, recipe.byteLength)
    }
    if (!recipe || typeof recipe !== 'object' || Array.isArray(recipe)) {
      throw new TypeError('recipe must be an object, string, or Uint8Array')
    }
    var text = JSON.stringify(recipe, function(key, value) {
      assertWellFormedUnicode(key, 'recipe key')
      if (typeof value === 'string') {
        assertWellFormedUnicode(value, 'recipe string')
      }
      return value
    })
    var textLimit = Math.min(
      limits.maxRecipeBytes, limits.maxAllocationBytes - 1
    )
    var textLength = utf8ByteLength(
      text, 'recipe', null, false, textLimit
    )
    return encodeUtf8(text, 'recipe', null, textLimit, textLength)
  }

  function runtimeOptions(options) {
    if (options === undefined) options = {}
    if (typeof options !== 'object' || Array.isArray(options)) {
      throw new TypeError('transform runtime options must be an object')
    }
    Object.keys(options).forEach(function(name) {
      if (['limits', 'cancelToken', 'hostPolicy', 'spillDir'].indexOf(name) < 0) {
        throw new TypeError('unknown transform runtime option: ' + name)
      }
    })
    if (options.hostPolicy !== undefined && options.hostPolicy !== null) {
      throw new TranfiTransformError(
        113, 'prepared-transform hostPolicy is reserved but not implemented'
      )
    }
    if (options.spillDir !== undefined && options.spillDir !== null
        && options.spillDir !== '') {
      throw new TranfiTransformError(
        113, 'prepared-transform spillDir is reserved but not implemented'
      )
    }
    return {
      limits: options.limits,
      cancelToken: options.cancelToken || null
    }
  }

  function throwIfCancelled(token) {
    if (token && token._isRequested()) {
      throw new TranfiTransformError(109, ERROR_NAMES[109])
    }
  }

  function tokenHandle(token) {
    if (token === null) return 0
    if (!(token instanceof TransformCancelToken)) {
      throw new TypeError('cancelToken must be a TransformCancelToken')
    }
    return token._requireOpen()
  }

  class OwnedTransform {
    constructor(handle, token) {
      this._handle = handle >>> 0
      this._token = token || null
      if (this._token) this._token._retain()
    }

    _requireOpen() {
      if (!this._handle) {
        throw new TranfiTransformError(112, 'prepared transform object is closed')
      }
      return this._handle
    }

    close() {
      if (this._handle) {
        var code = destroyHandle(this._handle)
        this._handle = 0
        if (this._token) {
          this._token._release()
          this._token = null
        }
        if (code !== 0) throw new TranfiTransformError(code, ERROR_NAMES[code])
      }
    }

    dispose() {
      this.close()
    }
  }

  class TransformCancelToken extends OwnedTransform {
    constructor(handle, sharedFlag, hostSlot) {
      super(handle, null)
      this._sharedFlag = sharedFlag || null
      this._hostSlot = hostSlot
      this._references = 1
      this._requested = false
    }

    _retain() {
      this._requireOpen()
      ++this._references
    }

    _release() {
      --this._references
      if (this._references === 0 && this._hostSlot !== NO_HOST_CANCEL) {
        cancelPolls.delete(this._hostSlot)
      }
    }

    request() {
      var handle = this._requireOpen()
      this._requested = true
      if (this._sharedFlag) Atomics.store(this._sharedFlag, 0, 1)
      var code = raw('tf_wasm_transform_cancel_request', [handle])
      if (code !== 0) throw new TranfiTransformError(code, ERROR_NAMES[code])
    }

    _observeNextPoll(observer) {
      this._requireOpen()
      if (typeof observer !== 'function') return
      var poll = cancelPolls.get(this._hostSlot)
      if (poll) poll.observer = observer
    }

    _isRequested() {
      return this._requested || Boolean(
        this._sharedFlag && Atomics.load(this._sharedFlag, 0) !== 0
      )
    }

    close() {
      if (this._handle) {
        var code = destroyHandle(this._handle)
        this._handle = 0
        this._release()
        if (code !== 0) throw new TranfiTransformError(code, ERROR_NAMES[code])
      }
    }
  }

  function createTransformCancelToken(options) {
    options = options || {}
    var flag = options.sharedFlag || null
    var slot = NO_HOST_CANCEL
    if (flag !== null) {
      var shared = typeof SharedArrayBuffer !== 'undefined'
        && flag instanceof Int32Array
        && Object.prototype.toString.call(flag.buffer) === '[object SharedArrayBuffer]'
      if (!shared || flag.length < 1 || typeof Atomics === 'undefined') {
        throw new TypeError('sharedFlag must be a nonempty SharedArrayBuffer-backed Int32Array')
      }
      do {
        slot = nextCancelSlot++ >>> 0
        if (nextCancelSlot === NO_HOST_CANCEL) nextCancelSlot = 1
      } while (cancelPolls.has(slot))
      cancelPolls.set(slot, { flag: flag, observer: null })
    }
    var limits = null
    try {
      var effectiveLimits = resolvedLimits(options.limits)
      limits = limitsOwner(options.limits, effectiveLimits)
      var handle = callForHandle('tf_wasm_transform_cancel_create', [
        slot, limits ? limits.pointer : 0
      ])
      return new TransformCancelToken(handle, flag, slot)
    } catch (error) {
      if (slot !== NO_HOST_CANCEL) cancelPolls.delete(slot)
      throw error
    } finally {
      free(limits)
    }
  }

  class TransformRecipe extends OwnedTransform {
    static fromJSON(recipe, options) {
      options = options || {}
      var bytes = null
      var limits = null
      try {
        var effectiveLimits = resolvedLimits(options.limits)
        var normalized = normalizeRecipeJSON(recipe, effectiveLimits)
        limits = limitsOwner(options.limits, effectiveLimits)
        bytes = allocBytes(normalized, 1)
        var handle = callForHandle('tf_wasm_transform_recipe_from_json', [
          bytes.pointer, bytes.size, limits ? limits.pointer : 0
        ])
        return new TransformRecipe(handle)
      } finally {
        free(limits)
        free(bytes)
      }
    }

    analyzer(schema, options) {
      var runtime = runtimeOptions(options)
      var schemaAllocation = null
      var limits = null
      try {
        var effectiveLimits = resolvedLimits(runtime.limits)
        var cancelHandle = tokenHandle(runtime.cancelToken)
        throwIfCancelled(runtime.cancelToken)
        schemaAllocation = schemaOwner(
          schema, effectiveLimits, runtime.cancelToken
        )
        throwIfCancelled(runtime.cancelToken)
        limits = limitsOwner(runtime.limits, effectiveLimits)
        var handle = callForHandle('tf_wasm_transform_analyzer_create', [
          this._requireOpen(), schemaAllocation.pointer,
          limits ? limits.pointer : 0, cancelHandle
        ])
        return new TransformAnalyzer(
          handle, schemaAllocation.dtypes, runtime.cancelToken, effectiveLimits
        )
      } finally {
        free(limits)
        if (schemaAllocation) schemaAllocation.free()
      }
    }
  }

  class TransformAnalyzer extends OwnedTransform {
    constructor(handle, dtypes, token, limits) {
      super(handle, token)
      this._dtypes = dtypes.slice()
      this._limits = limits
    }

    push(table) {
      try {
        throwIfCancelled(this._token)
      } catch (error) {
        this.close()
        throw error
      }
      var tableAllocation
      try {
        tableAllocation = tableOwner(
          table, this._dtypes, this._limits, 'analyze', this._token
        )
      } catch (error) {
        if (error && error.code === 109) this.close()
        throw error
      }
      try {
        try {
          throwIfCancelled(this._token)
        } catch (error) {
          this.close()
          throw error
        }
        try {
          callWithError('tf_wasm_transform_analyzer_push', [
            this._requireOpen(), tableAllocation.pointer
          ])
        } catch (error) {
          if (error && error.code === 109) this.close()
          throw error
        }
        return this
      } finally {
        tableAllocation.free()
      }
    }

    finalize() {
      try {
        return new TransformPlan(callForHandle(
          'tf_wasm_transform_analyzer_finalize', [this._requireOpen()]
        ))
      } catch (error) {
        if (error && error.code === 109) this.close()
        throw error
      }
    }
  }

  class TransformPlan extends OwnedTransform {
    static fromBytes(bytes, options) {
      options = options || {}
      if (!(bytes instanceof Uint8Array)) {
        throw new TypeError('plan bytes must be Uint8Array')
      }
      var input = null
      var runtime = runtimeOptions(options)
      var limits = null
      try {
        var effectiveLimits = resolvedLimits(runtime.limits)
        if (bytes.byteLength > effectiveLimits.maxPlanBytes
            || bytes.byteLength > effectiveLimits.maxAllocationBytes) {
          throw resourceLimit('prepared-transform plan byte limit exceeded')
        }
        throwIfCancelled(runtime.cancelToken)
        limits = limitsOwner(runtime.limits, effectiveLimits)
        input = allocBytes(new Uint8Array(
          bytes.buffer, bytes.byteOffset, bytes.byteLength
        ), 1, runtime.cancelToken)
        throwIfCancelled(runtime.cancelToken)
        var handle = callForHandle('tf_wasm_transform_plan_import', [
          input.pointer, input.size, limits ? limits.pointer : 0,
          tokenHandle(runtime.cancelToken)
        ])
        return new TransformPlan(handle)
      } finally {
        free(limits)
        free(input)
      }
    }

    toBytes(options) {
      options = options || {}
      var limits = limitsOwner(options.limits)
      try {
        return bytesFromHandle(callForHandle('tf_wasm_transform_plan_export', [
          this._requireOpen(), limits ? limits.pointer : 0
        ]))
      } finally {
        free(limits)
      }
    }

    schemaJSON(which, options) {
      which = which === undefined ? 'output' : which
      options = options || {}
      var selector = which === 'input'
        ? SCHEMA_INPUT
        : which === 'output'
          ? SCHEMA_OUTPUT
          : 0
      if (!selector) throw new TypeError("which must be 'input' or 'output'")
      var limits = limitsOwner(options.limits)
      try {
        return bytesFromHandle(callForHandle('tf_wasm_transform_plan_schema_json', [
          this._requireOpen(), selector, limits ? limits.pointer : 0
        ]))
      } finally {
        free(limits)
      }
    }

    recipeSha256(options) {
      options = options || {}
      var limits = limitsOwner(options.limits)
      try {
        return decoder.decode(bytesFromHandle(callForHandle(
          'tf_wasm_transform_plan_recipe_sha256', [
            this._requireOpen(), limits ? limits.pointer : 0
          ]
        )))
      } finally {
        free(limits)
      }
    }

    apply(schema, options) {
      var runtime = runtimeOptions(options)
      var schemaAllocation = null
      var limits = null
      try {
        var effectiveLimits = resolvedLimits(runtime.limits)
        var cancelHandle = tokenHandle(runtime.cancelToken)
        throwIfCancelled(runtime.cancelToken)
        schemaAllocation = schemaOwner(
          schema, effectiveLimits, runtime.cancelToken
        )
        throwIfCancelled(runtime.cancelToken)
        limits = limitsOwner(runtime.limits, effectiveLimits)
        var handle = callForHandle('tf_wasm_transform_apply_create', [
          this._requireOpen(), schemaAllocation.pointer,
          limits ? limits.pointer : 0, cancelHandle
        ])
        return new TransformApply(
          handle, schemaAllocation.dtypes, runtime.cancelToken, effectiveLimits
        )
      } finally {
        free(limits)
        if (schemaAllocation) schemaAllocation.free()
      }
    }
  }

  class TransformApply extends OwnedTransform {
    constructor(handle, dtypes, token, limits) {
      super(handle, token)
      this._dtypes = dtypes.slice()
      this._limits = limits
    }

    run(table) {
      try {
        throwIfCancelled(this._token)
      } catch (error) {
        this.close()
        throw error
      }
      var tableAllocation
      try {
        tableAllocation = tableOwner(
          table, this._dtypes, this._limits, 'apply', this._token
        )
      } catch (error) {
        if (error && error.code === 109) this.close()
        throw error
      }
      try {
        try {
          throwIfCancelled(this._token)
        } catch (error) {
          this.close()
          throw error
        }
        var denseHandle
        try {
          denseHandle = callForHandle('tf_wasm_transform_apply_run', [
            this._requireOpen(), tableAllocation.pointer
          ])
        } catch (error) {
          if (error && error.code === 109) this.close()
          throw error
        }
        try {
          return denseFromHandle(denseHandle, this._token)
        } catch (error) {
          try {
            this.close()
          } catch (_) {}
          throw error
        }
      } finally {
        tableAllocation.free()
      }
    }
  }

  return {
    TranfiTransformError: TranfiTransformError,
    TransformRecipe: TransformRecipe,
    TransformAnalyzer: TransformAnalyzer,
    TransformPlan: TransformPlan,
    TransformApply: TransformApply,
    TransformCancelToken: TransformCancelToken,
    createTransformCancelToken: createTransformCancelToken,
    safeTransformLimits: safeTransformLimits
  }
}

module.exports = createTransformAPI
