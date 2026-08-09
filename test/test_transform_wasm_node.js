'use strict'

const assert = require('assert/strict')
const { once } = require('events')
const path = require('path')
const { Worker } = require('worker_threads')

const native = require(path.join(__dirname, '../js/src/index.js'))
const createTranfi = require(path.join(__dirname, '../js/wasm/index.js'))
const vectors = require('./vectors/prepared_transform_v1.json')

const schema64 = [{ id: 'x0', dtype: 'float64' }]
const supportedRecipes = new Set([
  'categorical_mode_label_other',
  'categorical_mode_label_sentinel',
  'categorical_mode_onehot_all_zero',
  'categorical_mode_onehot_other',
  'categorical_mode_zero_onehot_all_zero',
  'categorical_mode_zero_label_other',
  'categorical_mode_none',
  'categorical_mode_zero_none',
  'numeric_mean_standard',
  'numeric_median_none',
  'numeric_median_zero_none',
  'numeric_none_none',
  'numeric_zero_minmax'
])

function doubleFromBits(bits) {
  return bits === null ? NaN : Buffer.from(bits, 'hex').readDoubleBE(0)
}

function doubleBits(value) {
  const bytes = Buffer.alloc(8)
  bytes.writeDoubleBE(value, 0)
  return bytes.toString('hex')
}

function float32FromBits(bits) {
  const buffer = new ArrayBuffer(bits.length * 4)
  const view = new DataView(buffer)
  for (let i = 0; i < bits.length; ++i) view.setUint32(i * 4, bits[i], true)
  return new Float32Array(buffer)
}

function table(values, ArrayType = Float64Array, validity) {
  const column = validity === undefined
    ? new ArrayType(values)
    : { data: new ArrayType(values), validity: Uint8Array.from(validity) }
  return { rows: values.length, columns: [column] }
}

function closeAll(...values) {
  for (const value of values.reverse()) if (value) value.close()
}

function largeCancellationFixture() {
  const id = `x${'a'.repeat(4 * 1024 * 1024)}`
  const recipe = structuredClone(vectors.recipes.numeric_none_none)
  recipe.columns[0].sourceId = id
  return {
    recipe,
    schema: [{ id, name: 'large feature', dtype: 'float64' }],
    limits: {
      maxRecipeBytes: 8 * 1024 * 1024,
      maxStringBytes: 8 * 1024 * 1024
    }
  }
}

async function cancelOnNativePoll(token, flag) {
  const start = new Int32Array(new SharedArrayBuffer(4))
  const worker = new Worker(`
    const { parentPort, workerData } = require('worker_threads')
    const start = new Int32Array(workerData.start)
    const flag = new Int32Array(workerData.flag)
    parentPort.postMessage('ready')
    Atomics.wait(start, 0, 0)
    Atomics.store(flag, 0, 1)
    Atomics.notify(flag, 0)
  `, {
    eval: true,
    workerData: { start: start.buffer, flag: flag.buffer }
  })
  const exit = once(worker, 'exit')
  await once(worker, 'message')
  let observed = false
  token._observeNextPoll(() => {
    observed = true
    Atomics.store(start, 0, 1)
    Atomics.notify(start, 0)
  })
  return async function finish() {
    await exit
    assert.equal(observed, true)
    assert.equal(Atomics.load(flag, 0), 1)
  }
}

async function cancelAfterSignal(flag) {
  const signal = new Int32Array(new SharedArrayBuffer(4))
  const worker = new Worker(`
    const { parentPort, workerData } = require('worker_threads')
    const signal = new Int32Array(workerData.signal)
    const flag = new Int32Array(workerData.flag)
    parentPort.postMessage('ready')
    Atomics.wait(signal, 0, 0)
    Atomics.store(flag, 0, 1)
    Atomics.notify(flag, 0)
  `, {
    eval: true,
    workerData: { signal: signal.buffer, flag: flag.buffer }
  })
  const exit = once(worker, 'exit')
  await once(worker, 'message')
  return {
    signal() {
      Atomics.store(signal, 0, 1)
      Atomics.notify(signal, 0)
    },
    async finish() {
      await exit
      assert.equal(Atomics.load(flag, 0), 1)
    }
  }
}

async function main() {
  const wasm = await createTranfi()
  function assertAllocationFailureClean(failAt, action) {
    const raw = wasm._wasm
    const originalMalloc = raw._malloc
    const originalFree = raw._free
    const live = new Set()
    let allocation = 0
    raw._malloc = function(size) {
      allocation++
      if (allocation === failAt) return 0
      const pointer = originalMalloc(size) >>> 0
      if (pointer) live.add(pointer)
      return pointer
    }
    raw._free = function(pointer) {
      live.delete(pointer >>> 0)
      return originalFree(pointer)
    }
    try {
      assert.throws(
        action,
        (error) => error instanceof native.TranfiTransformError
          && error.code === 110
      )
    } finally {
      raw._malloc = originalMalloc
      raw._free = originalFree
    }
    assert.equal(live.size, 0, `wrapper allocations leaked at failure ${failAt}`)
  }
  function assertResourceBeforeMalloc(action) {
    const raw = wasm._wasm
    const originalMalloc = raw._malloc
    let allocations = 0
    raw._malloc = function(size) {
      allocations++
      return originalMalloc(size)
    }
    try {
      assert.throws(
        action,
        (error) => error instanceof native.TranfiTransformError
          && error.code === 104
      )
    } finally {
      raw._malloc = originalMalloc
    }
    assert.equal(allocations, 0, 'over-limit transport allocated WASM memory')
  }
  function assertCancelledBeforeMalloc(action) {
    const raw = wasm._wasm
    const originalMalloc = raw._malloc
    let allocations = 0
    raw._malloc = function(size) {
      allocations++
      return originalMalloc(size)
    }
    try {
      assert.throws(
        action,
        (error) => error instanceof native.TranfiTransformError
          && error.code === 109
      )
    } finally {
      raw._malloc = originalMalloc
    }
    assert.equal(allocations, 0, 'cancelled transport allocated WASM memory')
  }
  assert.equal(wasm.TranfiTransformError, native.TranfiTransformError)
  for (const name of [
    '_tf_wasm_transform_recipe_from_json',
    '_tf_wasm_transform_analyzer_create',
    '_tf_wasm_transform_analyzer_push',
    '_tf_wasm_transform_analyzer_finalize',
    '_tf_wasm_transform_plan_export',
    '_tf_wasm_transform_plan_import',
    '_tf_wasm_transform_apply_create',
    '_tf_wasm_transform_apply_run',
    '_tf_wasm_transform_handle_destroy'
  ]) assert.equal(typeof wasm._wasm[name], 'function', `${name} export`)

  const limits = wasm.safeTransformLimits({ maxApplyRows: 7 })
  assert.equal(limits.maxRecipeBytes, 1048576)
  assert.equal(limits.maxApplyRows, 7)
  assert.equal(limits.maxLiveHandles, 65535)
  assert.equal(Object.keys(limits).length, 22)
  for (const name of Object.keys(limits)) {
    assert.throws(
      () => wasm.safeTransformLimits({ [name]: 0 }),
      (error) => error instanceof native.TranfiTransformError
        && error.code === 100
    )
  }

  for (const invalid of ['\ud800', '\udc00']) {
    assert.throws(
      () => wasm.TransformRecipe.fromJSON(invalid),
      /unpaired UTF-16 surrogate/
    )
    const invalidRecipe = structuredClone(vectors.recipes.numeric_none_none)
    invalidRecipe.columns[0].sourceId = invalid
    assert.throws(
      () => wasm.TransformRecipe.fromJSON(invalidRecipe),
      /unpaired UTF-16 surrogate/
    )
    const schemaRecipe = wasm.TransformRecipe.fromJSON(
      vectors.recipes.numeric_none_none
    )
    assert.throws(
      () => schemaRecipe.analyzer([
        { id: 'x0', name: invalid, dtype: 'float64' }
      ]),
      /unpaired UTF-16 surrogate/
    )
    assert.throws(
      () => schemaRecipe.analyzer([
        { id: invalid, name: 'x0', dtype: 'float64' }
      ]),
      /unpaired UTF-16 surrogate/
    )
    schemaRecipe.close()
  }
  {
    const astralId = 'x\ud83d\ude00'
    const astralName = 'feature\ud83d\ude80'
    const config = structuredClone(vectors.recipes.numeric_none_none)
    config.columns[0].sourceId = astralId
    const schema = [{ id: astralId, name: astralName, dtype: 'float64' }]
    const wasmRecipe = wasm.TransformRecipe.fromJSON(config)
    const wasmAnalyzer = wasmRecipe.analyzer(schema)
    wasmAnalyzer.push(table([1]))
    const wasmPlan = wasmAnalyzer.finalize()
    const nativeRecipe = native.TransformRecipe.fromJSON(config)
    const nativeAnalyzer = nativeRecipe.analyzer(schema)
    nativeAnalyzer.push(table([1]))
    const nativePlan = nativeAnalyzer.finalize()
    assert.deepEqual(
      Buffer.from(wasmPlan.schemaJSON('input')),
      nativePlan.schemaJSON('input')
    )
    closeAll(
      wasmRecipe, wasmAnalyzer, wasmPlan,
      nativeRecipe, nativeAnalyzer, nativePlan
    )
  }

  for (const item of vectors.semanticCases.filter((entry) =>
    supportedRecipes.has(entry.recipe) && !entry.expectedError
  )) {
    const inputDtype = item.inputDtype ?? 'float64'
    const inputSchema = inputDtype === 'float32'
      ? [{ id: 'x0', dtype: 'float32' }]
      : schema64
    const InputArray = inputDtype === 'float32' ? Float32Array : Float64Array
    const analyzeValues = item.analyze.rows.map((row) => doubleFromBits(row[0]))
    const applyValues = item.apply.rows.map((row) => doubleFromBits(row[0]))
    const expected = item.apply.expectedRows.flat()
    const expectedColumns = item.apply.expectedColumns ?? 1
    let reference = null
    for (const split of item.analyze.chunkSplits) {
      const recipe = wasm.TransformRecipe.fromJSON(vectors.recipes[item.recipe])
      const analyzer = recipe.analyzer(inputSchema)
      let offset = 0
      for (const count of split) {
        analyzer.push(table(
          analyzeValues.slice(offset, offset + count), InputArray
        ))
        offset += count
      }
      const plan = analyzer.finalize()
      const planBytes = plan.toBytes()
      if (reference === null) reference = planBytes
      else assert.deepEqual(planBytes, reference, `${item.id}: chunk plan bytes`)
      const loadedNative = native.TransformPlan.fromBytes(planBytes)
      assert.deepEqual(loadedNative.toBytes(), Buffer.from(planBytes))
      loadedNative.close()
      assert.equal(
        new TextDecoder().decode(plan.schemaJSON('input')),
        `[{"dtype":"${inputDtype}","id":"x0","name":"x0"}]`
      )
      if (item.expectedPlan.outputIds) {
        const outputSchema = JSON.parse(
          new TextDecoder().decode(plan.schemaJSON('output'))
        )
        assert.deepEqual(
          outputSchema.map((field) => field.id),
          item.expectedPlan.outputIds
        )
        if (vectors.recipes[item.recipe].columns[0].categorical.encode.op === 'onehot') {
          assert.deepEqual(
            outputSchema.map((field) => field.role),
            item.expectedPlan.outputIds.map(() => 'onehot')
          )
          assert.deepEqual(
            outputSchema.map((field) => field.category),
            item.expectedPlan.categories.map((bits) => ({
              t: inputDtype === 'float32' ? 'f32' : 'f64', v: bits
            })).concat(
              item.expectedPlan.otherOrdinal === null ? [] : [{ t: 'other' }]
            )
          )
        } else {
          assert.deepEqual(outputSchema.map((field) => field.category), [null])
          assert.deepEqual(outputSchema.map((field) => field.role), ['label'])
        }
      }
      const apply = plan.apply(inputSchema)
      const result = apply.run(table(applyValues, InputArray))
      assert.equal(result.rows, applyValues.length)
      assert.equal(result.columns, expectedColumns)
      assert.deepEqual(Array.from(result.data, doubleBits), expected, `${item.id}: output`)
      closeAll(recipe, analyzer, plan, apply)
    }
    const loaded = wasm.TransformPlan.fromBytes(reference)
    assert.deepEqual(loaded.toBytes(), reference)
    loaded.close()
    if (inputDtype === 'float32' &&
        vectors.recipes[item.recipe].columns[0].categorical?.encode.op === 'onehot') {
      const nativeRecipe = native.TransformRecipe.fromJSON(
        vectors.recipes[item.recipe]
      )
      const nativeAnalyzer = nativeRecipe.analyzer(inputSchema)
      nativeAnalyzer.push(table(analyzeValues, InputArray))
      const nativePlan = nativeAnalyzer.finalize()
      assert.deepEqual(
        nativePlan.toBytes(),
        Buffer.from(reference),
        `${item.id}: independently fitted native/WASM TFTR bytes`
      )
      closeAll(nativeRecipe, nativeAnalyzer, nativePlan)
    }
  }

  {
    const values = Array.from({ length: 11 }, (_, index) => index)
    const sourceId = 'é:🔥'
    const config = structuredClone(
      vectors.recipes.categorical_mode_onehot_all_zero
    )
    config.columns[0].sourceId = sourceId
    const schema = [{ id: sourceId, name: sourceId, dtype: 'float64' }]
    const wasmRecipe = wasm.TransformRecipe.fromJSON(config)
    const wasmAnalyzer = wasmRecipe.analyzer(schema)
    wasmAnalyzer.push(table(values))
    const wasmPlan = wasmAnalyzer.finalize()
    const nativeRecipe = native.TransformRecipe.fromJSON(config)
    const nativeAnalyzer = nativeRecipe.analyzer(schema)
    nativeAnalyzer.push(table(values))
    const nativePlan = nativeAnalyzer.finalize()
    assert.deepEqual(
      Buffer.from(wasmPlan.toBytes()),
      nativePlan.toBytes(),
      'native/WASM Unicode multi-digit one-hot TFTR bytes'
    )
    const output = JSON.parse(new TextDecoder().decode(
      wasmPlan.schemaJSON('output')
    ))
    assert.equal(output.length, 11)
    assert.equal(
      output[10].id,
      '%C3%A9%3A%F0%9F%94%A5%3Aonehot%3A10'
    )
    assert.equal(output[10].name, output[10].id)
    const apply = wasmPlan.apply(schema)
    assert.deepEqual(Array.from(apply.run(table([10])).data), [
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1
    ])
    closeAll(
      wasmRecipe, wasmAnalyzer, wasmPlan, apply,
      nativeRecipe, nativeAnalyzer, nativePlan
    )
  }

  {
    const analyzeValues = [1, 2]
    const applyValues = [1, 3]
    const recipe = wasm.TransformRecipe.fromJSON(
      vectors.recipes.categorical_mode_onehot_other
    )
    const analyzer = recipe.analyzer(schema64)
    analyzer.push(table(analyzeValues))
    const plan = analyzer.finalize()
    const bytes = plan.toBytes()
    assert.throws(
      () => wasm.TransformPlan.fromBytes(bytes, {
        limits: { maxOutputElementsPerCall: 134217727 }
      }),
      (error) => error instanceof native.TranfiTransformError
        && error.code === 104
    )
    wasm.TransformPlan.fromBytes(bytes, {
      limits: { maxOutputElementsPerCall: 134217728 }
    }).close()
    const hostUnder = plan.apply(schema64, {
      limits: { maxOutputElementsPerCall: 5 }
    })
    assert.throws(
      () => hostUnder.run(table(applyValues)),
      (error) => error instanceof native.TranfiTransformError
        && error.code === 104
    )
    const hostExact = plan.apply(schema64, {
      limits: { maxOutputElementsPerCall: 6 }
    })
    assert.equal(hostExact.run(table(applyValues)).data.length, 6)
    closeAll(recipe, analyzer, plan, hostUnder, hostExact)

    for (const semanticLimit of [5, 6]) {
      const config = structuredClone(
        vectors.recipes.categorical_mode_onehot_other
      )
      config.semanticLimits.maxOutputElementsPerApply = semanticLimit
      const semanticRecipe = wasm.TransformRecipe.fromJSON(config)
      const semanticAnalyzer = semanticRecipe.analyzer(schema64)
      semanticAnalyzer.push(table(analyzeValues))
      const semanticPlan = semanticAnalyzer.finalize()
      const semanticApply = semanticPlan.apply(schema64)
      if (semanticLimit === 5) {
        assert.throws(
          () => semanticApply.run(table(applyValues)),
          (error) => error instanceof native.TranfiTransformError
            && error.code === 104
        )
      } else {
        assert.equal(semanticApply.run(table(applyValues)).data.length, 6)
      }
      closeAll(
        semanticRecipe, semanticAnalyzer, semanticPlan, semanticApply
      )
    }
  }

  for (const item of vectors.semanticCases.filter((entry) =>
    entry.recipe === 'numeric_median_none' && entry.expectedError
  )) {
    const recipe = wasm.TransformRecipe.fromJSON(vectors.recipes[item.recipe])
    const analyzer = recipe.analyzer(schema64)
    analyzer.push(table(item.analyze.rows.map((row) => doubleFromBits(row[0]))))
    assert.throws(
      () => analyzer.finalize(),
      (error) => error instanceof native.TranfiTransformError
        && error.code === item.expectedError.code
    )
    closeAll(recipe, analyzer)
  }

  for (const item of vectors.semanticCases.filter((entry) =>
    entry.recipe.startsWith('categorical_mode_') && entry.expectedError &&
      entry.expectedError.phase === 'finalize'
  )) {
    const recipe = wasm.TransformRecipe.fromJSON(vectors.recipes[item.recipe])
    const analyzer = recipe.analyzer(schema64)
    analyzer.push(table(item.analyze.rows.map((row) => doubleFromBits(row[0]))))
    assert.throws(
      () => analyzer.finalize(),
      (error) => error instanceof native.TranfiTransformError &&
        error.code === item.expectedError.code
    )
    assert.throws(
      () => analyzer.finalize(),
      (error) => error instanceof native.TranfiTransformError && error.code === 112
    )
    closeAll(recipe, analyzer)
  }

  for (const caseId of [
    'categorical-mode-label-unknown-error',
    'categorical-mode-onehot-unknown-error'
  ]) {
    const item = vectors.semanticCases.find(
      (entry) => entry.id === caseId
    )
    const recipe = wasm.TransformRecipe.fromJSON(vectors.recipes[item.recipe])
    const analyzer = recipe.analyzer(schema64)
    analyzer.push(table(item.analyze.rows.map((row) => doubleFromBits(row[0]))))
    const plan = analyzer.finalize()
    const apply = plan.apply(schema64)
    assert.throws(
      () => apply.run(table(item.apply.rows.map((row) => doubleFromBits(row[0])))),
      (error) => error instanceof native.TranfiTransformError && error.code === 108
    )
    assert.throws(
      () => apply.run(table([1])),
      (error) => error instanceof native.TranfiTransformError && error.code === 112
    )
    closeAll(recipe, analyzer, plan, apply)
  }

  for (const sentinel of [Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER]) {
    const config = structuredClone(vectors.recipes.categorical_mode_label_sentinel)
    config.columns[0].categorical.encode.sentinelLabel = sentinel
    wasm.TransformRecipe.fromJSON(config).close()
  }
  for (const sentinel of [0.5, Number.MAX_SAFE_INTEGER + 1]) {
    const config = structuredClone(vectors.recipes.categorical_mode_label_sentinel)
    config.columns[0].categorical.encode.sentinelLabel = sentinel
    assert.throws(
      () => wasm.TransformRecipe.fromJSON(config),
      (error) => error instanceof native.TranfiTransformError && error.code === 101
    )
  }

  {
    const config = structuredClone(vectors.recipes.categorical_mode_label_error)
    config.columns[0].sourceId = 'a'
    const second = structuredClone(config.columns[0])
    second.sourceId = 'a%3Alabel'
    config.columns.push(second)
    const collisionSchema = [
      { id: 'a', name: 'first', dtype: 'float64' },
      { id: 'a%3Alabel', name: 'second', dtype: 'float64' }
    ]
    const recipe = wasm.TransformRecipe.fromJSON(config)
    const analyzer = recipe.analyzer(collisionSchema)
    analyzer.push({
      rows: 1,
      columns: [new Float64Array([0]), new Float64Array([0])]
    })
    assert.throws(
      () => analyzer.finalize(),
      (error) => error instanceof native.TranfiTransformError && error.code === 102
    )
    assert.throws(
      () => analyzer.finalize(),
      (error) => error instanceof native.TranfiTransformError && error.code === 112
    )
    closeAll(recipe, analyzer)
  }

  {
    const recipe = wasm.TransformRecipe.fromJSON(
      vectors.recipes.categorical_mode_none
    )
    const analyzer = recipe.analyzer(schema64)
    analyzer.push(table([1, 2]))
    const plan = analyzer.finalize()
    const apply = plan.apply(schema64)
    assert.throws(
      () => apply.run(table([3])),
      (error) => error instanceof native.TranfiTransformError && error.code === 108
    )
    assert.throws(
      () => apply.run(table([1])),
      (error) => error instanceof native.TranfiTransformError && error.code === 112
    )
    const limited = recipe.analyzer(schema64, {
      limits: { maxCategoriesPerColumn: 2 }
    })
    assert.throws(
      () => limited.push(table([1, 2, 3])),
      (error) => error instanceof native.TranfiTransformError && error.code === 104
    )
    assert.throws(
      () => limited.push(table([1])),
      (error) => error instanceof native.TranfiTransformError && error.code === 112
    )
    closeAll(recipe, analyzer, plan, apply, limited)
  }

  {
    const schema32 = [{ id: 'x0', name: 'x0', dtype: 'float32' }]
    const config = vectors.recipes.categorical_mode_label_other
    const wasmRecipe = wasm.TransformRecipe.fromJSON(config)
    const wasmAnalyzer = wasmRecipe.analyzer(schema32)
    wasmAnalyzer.push(table([2, 1, 2], Float32Array))
    const wasmPlan = wasmAnalyzer.finalize()
    const nativeRecipe = native.TransformRecipe.fromJSON(config)
    const nativeAnalyzer = nativeRecipe.analyzer(schema32)
    nativeAnalyzer.push(table([2, 1, 2], Float32Array))
    const nativePlan = nativeAnalyzer.finalize()
    assert.deepEqual(
      Buffer.from(wasmPlan.toBytes()),
      nativePlan.toBytes(),
      'native/WASM float32 categorical-label TFTR bytes'
    )
    const apply = wasmPlan.apply(schema32)
    assert.deepEqual(
      Array.from(
        apply.run(table([1, 3, NaN], Float32Array)).data,
        doubleBits
      ),
      ['0000000000000000', '4000000000000000', '3ff0000000000000']
    )
    closeAll(
      wasmRecipe, wasmAnalyzer, wasmPlan, apply,
      nativeRecipe, nativeAnalyzer, nativePlan
    )
  }

  {
    const schema32 = [{ id: 'x0', name: 'x0', dtype: 'float32' }]
    const config = vectors.recipes.categorical_mode_none
    const wasmRecipe = wasm.TransformRecipe.fromJSON(config)
    const wasmAnalyzer = wasmRecipe.analyzer(schema32)
    wasmAnalyzer.push(table([-0, 0, 1, 1], Float32Array))
    const wasmPlan = wasmAnalyzer.finalize()
    const nativeRecipe = native.TransformRecipe.fromJSON(config)
    const nativeAnalyzer = nativeRecipe.analyzer(schema32)
    nativeAnalyzer.push(table([-0, 0, 1, 1], Float32Array))
    const nativePlan = nativeAnalyzer.finalize()
    assert.deepEqual(
      Buffer.from(wasmPlan.toBytes()),
      nativePlan.toBytes(),
      'native/WASM float32 categorical-mode TFTR bytes'
    )
    const payload = JSON.parse(Buffer.from(wasmPlan.toBytes()).subarray(52))
    assert.deepEqual(payload.steps[0].categorical.categories, [
      { t: 'f32', v: '00000000' },
      { t: 'f32', v: '3f800000' }
    ])
    const apply = wasmPlan.apply(schema32)
    assert.deepEqual(
      Array.from(apply.run(table([-0, NaN], Float32Array)).data, doubleBits),
      ['8000000000000000', '0000000000000000']
    )
    closeAll(
      wasmRecipe, wasmAnalyzer, wasmPlan, apply,
      nativeRecipe, nativeAnalyzer, nativePlan
    )
  }

  {
    const schema32 = [{ id: 'x0', name: 'x0', dtype: 'float32' }]
    const config = vectors.recipes.categorical_mode_none
    const analyzeValues = float32FromBits([
      0x00000001, 0x80000001, 0x00000000
    ])
    const applyValues = float32FromBits([
      0x7fc00000, 0x80000001, 0x00000000, 0x00000001
    ])
    const wasmRecipe = wasm.TransformRecipe.fromJSON(config)
    const wasmAnalyzer = wasmRecipe.analyzer(schema32)
    wasmAnalyzer.push({ rows: 3, columns: [analyzeValues] })
    const wasmPlan = wasmAnalyzer.finalize()
    const nativeRecipe = native.TransformRecipe.fromJSON(config)
    const nativeAnalyzer = nativeRecipe.analyzer(schema32)
    nativeAnalyzer.push({ rows: 3, columns: [analyzeValues] })
    const nativePlan = nativeAnalyzer.finalize()
    assert.deepEqual(
      Buffer.from(wasmPlan.toBytes()),
      nativePlan.toBytes(),
      'native/WASM float32 subnormal categorical-mode TFTR bytes'
    )
    const payload = JSON.parse(Buffer.from(wasmPlan.toBytes()).subarray(52))
    assert.deepEqual(payload.steps[0].categorical.categories, [
      { t: 'f32', v: '80000001' },
      { t: 'f32', v: '00000000' },
      { t: 'f32', v: '00000001' }
    ])
    const wasmApply = wasmPlan.apply(schema32)
    assert.deepEqual(
      Array.from(wasmApply.run({ rows: 4, columns: [applyValues] }).data,
        doubleBits),
      [
        'b6a0000000000000', 'b6a0000000000000',
        '0000000000000000', '36a0000000000000'
      ]
    )
    closeAll(
      wasmRecipe, wasmAnalyzer, wasmPlan, wasmApply,
      nativeRecipe, nativeAnalyzer, nativePlan
    )
  }

  {
    const recipe = wasm.TransformRecipe.fromJSON(
      vectors.recipes.numeric_median_none
    )
    const analyzer = recipe.analyzer(schema64, {
      limits: { maxAllocationsPerSession: 9 }
    })
    assert.throws(
      () => analyzer.push(table([1])),
      (error) => error instanceof native.TranfiTransformError && error.code === 104
    )
    assert.throws(
      () => analyzer.push(table([1])),
      (error) => error instanceof native.TranfiTransformError && error.code === 112
    )
    closeAll(recipe, analyzer)
  }

  {
    const schema = [{ id: 'x0', name: 'feature', dtype: 'float32' }]
    const config = structuredClone(vectors.recipes.numeric_median_none)
    const source = new Float32Array([1, 99, 2, 99, 3])
    const recipe = wasm.TransformRecipe.fromJSON(config)
    const analyzer = recipe.analyzer(schema)
    recipe.close()
    analyzer.push({
      rows: 3,
      columns: [{ data: source, strideBytes: 8, validity: Uint8Array.of(0x05) }]
    })
    const plan = analyzer.finalize()
    analyzer.close()
    const apply = plan.apply(schema)
    plan.close()
    const result = apply.run({
      rows: 3,
      columns: [{ data: source, strideBytes: 8, validity: Uint8Array.of(0x05) }]
    })
    assert.deepEqual(Array.from(result.data, doubleBits), [
      '3ff0000000000000', '4000000000000000', '4008000000000000'
    ])
    assert.deepEqual(Array.from(source), [1, 99, 2, 99, 3])
    apply.close()
    apply.close()
  }

  {
    assert.throws(
      () => wasm.TransformRecipe.fromJSON({}),
      (error) => error instanceof native.TranfiTransformError && error.code === 101
    )
    assert.throws(
      () => wasm.TransformPlan.fromBytes(Uint8Array.from([1, 2, 3])),
      (error) => error instanceof native.TranfiTransformError && error.code === 106
    )
    const recipe = wasm.TransformRecipe.fromJSON(vectors.recipes.numeric_none_none)
    const analyzer = recipe.analyzer(schema64)
    assert.throws(
      () => analyzer.push(table([Infinity])),
      (error) => error instanceof native.TranfiTransformError && error.code === 107
    )
    assert.throws(
      () => analyzer.push(table([1])),
      (error) => error instanceof native.TranfiTransformError && error.code === 112
    )
    closeAll(recipe, analyzer)

    const preFlag = new Int32Array(new SharedArrayBuffer(4))
    const preToken = wasm.createTransformCancelToken({ sharedFlag: preFlag })
    Atomics.store(preFlag, 0, 1)
    let schemaReads = 0
    const unreadSchema = new Proxy({}, {
      get() {
        schemaReads++
        throw new Error('cancelled schema must not be read')
      }
    })
    const checkedRecipe = wasm.TransformRecipe.fromJSON(
      vectors.recipes.numeric_none_none
    )
    assertCancelledBeforeMalloc(
      () => checkedRecipe.analyzer(unreadSchema, { cancelToken: preToken })
    )
    assert.equal(schemaReads, 0)
    closeAll(checkedRecipe, preToken)
  }

  if (typeof SharedArrayBuffer !== 'undefined') {
    const flag = new Int32Array(new SharedArrayBuffer(4))
    const token = wasm.createTransformCancelToken({ sharedFlag: flag })
    const recipe = wasm.TransformRecipe.fromJSON(vectors.recipes.numeric_none_none)
    const analyzer = recipe.analyzer(schema64, { cancelToken: token })
    token.close()
    Atomics.store(flag, 0, 1)
    assert.throws(
      () => analyzer.push(table([1, 2, 3])),
      (error) => error instanceof native.TranfiTransformError && error.code === 109
    )
    closeAll(recipe, analyzer)

    const large = largeCancellationFixture()
    const largeRecipe = wasm.TransformRecipe.fromJSON(large.recipe, {
      limits: large.limits
    })
    const schemaFlag = new Int32Array(new SharedArrayBuffer(4))
    const schemaToken = wasm.createTransformCancelToken({
      sharedFlag: schemaFlag,
      limits: large.limits
    })
    const schemaCancel = await cancelAfterSignal(schemaFlag)
    const signalledField = new Proxy(large.schema[0], {
      get(target, property, receiver) {
        if (property === 'id') schemaCancel.signal()
        return Reflect.get(target, property, receiver)
      }
    })
    assert.throws(
      () => largeRecipe.analyzer([signalledField], {
        limits: large.limits,
        cancelToken: schemaToken
      }),
      (error) => error instanceof native.TranfiTransformError
        && error.code === 109
    )
    await schemaCancel.finish()
    schemaToken.close()
    const largeAnalyzer = largeRecipe.analyzer(large.schema, {
      limits: large.limits
    })
    largeAnalyzer.push({ rows: 1, columns: [new Float64Array([1])] })
    const largePlan = largeAnalyzer.finalize()
    const largePlanBytes = largePlan.toBytes({ limits: large.limits })
    const exactStringBytes = new TextEncoder().encode(large.schema[0].id).length
    assert.throws(
      () => largePlan.toBytes({
        limits: { ...large.limits, maxStringBytes: exactStringBytes - 1 }
      }),
      (error) => error instanceof native.TranfiTransformError
        && error.code === 104
    )
    assert.throws(
      () => largePlan.schemaJSON('input', {
        limits: { ...large.limits, maxStringBytes: exactStringBytes - 1 }
      }),
      (error) => error instanceof native.TranfiTransformError
        && error.code === 104
    )
    assert.deepEqual(
      largePlan.toBytes({
        limits: { ...large.limits, maxStringBytes: exactStringBytes }
      }),
      largePlanBytes
    )
    closeAll(largeRecipe, largeAnalyzer, largePlan)

    const finalizeFlag = new Int32Array(new SharedArrayBuffer(4))
    const finalizeToken = wasm.createTransformCancelToken({
      sharedFlag: finalizeFlag,
      limits: large.limits
    })
    const finalizeRecipe = wasm.TransformRecipe.fromJSON(large.recipe, {
      limits: large.limits
    })
    const finalizeAnalyzer = finalizeRecipe.analyzer(large.schema, {
      limits: large.limits,
      cancelToken: finalizeToken
    })
    finalizeAnalyzer.push({ rows: 1, columns: [new Float64Array([1])] })
    const finishFinalizeCancel = await cancelOnNativePoll(
      finalizeToken, finalizeFlag
    )
    assert.throws(
      () => finalizeAnalyzer.finalize(),
      (error) => error instanceof native.TranfiTransformError
        && error.code === 109
    )
    await finishFinalizeCancel()
    assert.throws(
      () => finalizeAnalyzer.finalize(),
      (error) => error instanceof native.TranfiTransformError
        && error.code === 112
    )
    closeAll(finalizeRecipe, finalizeAnalyzer, finalizeToken)

    const importFlag = new Int32Array(new SharedArrayBuffer(4))
    const importToken = wasm.createTransformCancelToken({
      sharedFlag: importFlag,
      limits: large.limits
    })
    const finishImportCancel = await cancelOnNativePoll(importToken, importFlag)
    assert.throws(
      () => wasm.TransformPlan.fromBytes(largePlanBytes, {
        limits: large.limits,
        cancelToken: importToken
      }),
      (error) => error instanceof native.TranfiTransformError
        && error.code === 109
    )
    await finishImportCancel()
    importToken.close()

    const imported = wasm.TransformPlan.fromBytes(largePlanBytes, {
      limits: large.limits
    })
    const applyFlag = new Int32Array(new SharedArrayBuffer(4))
    const applyToken = wasm.createTransformCancelToken({
      sharedFlag: applyFlag,
      limits: large.limits
    })
    const apply = imported.apply(large.schema, {
      limits: large.limits,
      cancelToken: applyToken
    })
    const finishApplyCancel = await cancelOnNativePoll(applyToken, applyFlag)
    const applyRows = 4 * 1024 * 1024
    assert.throws(
      () => apply.run({
        rows: applyRows,
        columns: [new Float64Array(applyRows)]
      }),
      (error) => error instanceof native.TranfiTransformError
        && error.code === 109
    )
    await finishApplyCancel()
    assert.throws(
      () => apply.run(table([1])),
      (error) => error instanceof native.TranfiTransformError
        && error.code === 112
    )
    closeAll(imported, apply, applyToken)

    const copyFlag = new Int32Array(new SharedArrayBuffer(4))
    const copyToken = wasm.createTransformCancelToken({ sharedFlag: copyFlag })
    const copyRecipe = wasm.TransformRecipe.fromJSON(
      vectors.recipes.numeric_none_none
    )
    const copyAnalyzer = copyRecipe.analyzer(schema64, {
      cancelToken: copyToken
    })
    const copyCancel = await cancelAfterSignal(copyFlag)
    const copyRows = 4 * 1024 * 1024
    const copyData = new Float64Array(copyRows)
    const signalledColumn = new Proxy({ data: copyData }, {
      get(target, property, receiver) {
        if (property === 'data') copyCancel.signal()
        return Reflect.get(target, property, receiver)
      }
    })
    assert.throws(
      () => copyAnalyzer.push({ rows: copyRows, columns: [signalledColumn] }),
      (error) => error instanceof native.TranfiTransformError
        && error.code === 109
    )
    await copyCancel.finish()
    assert.throws(
      () => copyAnalyzer.push(table([1])),
      (error) => error instanceof native.TranfiTransformError
        && error.code === 112
    )
    closeAll(copyRecipe, copyAnalyzer, copyToken)
  }

  {
    assertAllocationFailureClean(4, () => {
      wasm.TransformRecipe.fromJSON({})
    })
    assert.throws(
      () => wasm.TransformRecipe.fromJSON({}),
      (error) => error instanceof native.TranfiTransformError && error.code === 101
    )
    assertAllocationFailureClean(3, () => {
      wasm.TransformRecipe.fromJSON(vectors.recipes.numeric_none_none)
    })
    assertAllocationFailureClean(3, () => {
      wasm.TransformRecipe.fromJSON(vectors.recipes.numeric_none_none, {
        limits: { maxRecipeBytes: 1048576 }
      })
    })

    const recipe = wasm.TransformRecipe.fromJSON(vectors.recipes.numeric_none_none)
    assertAllocationFailureClean(2, () => recipe.analyzer(schema64))
    assertAllocationFailureClean(4, () => recipe.analyzer(schema64))
    const analyzer = recipe.analyzer(schema64)
    assertAllocationFailureClean(2, () => analyzer.push(table([1])))
    assertAllocationFailureClean(4, () => analyzer.push({
      rows: 1,
      columns: [{ data: new Float64Array([1]), validity: Uint8Array.of(1) }]
    }))
    analyzer.push(table([1, 2]))
    const plan = analyzer.finalize()
    assertAllocationFailureClean(3, () => plan.toBytes())
    assertAllocationFailureClean(4, () => plan.toBytes())
    const planBytes = plan.toBytes()
    assert(planBytes.length > 52)
    for (const failAt of [6, 7]) {
      const failedApply = plan.apply(schema64, { limits: { maxApplyRows: 1 } })
      assertAllocationFailureClean(failAt, () => failedApply.run(table([3])))
      assert.throws(
        () => failedApply.run(table([3])),
        (error) => error instanceof native.TranfiTransformError
          && error.code === 112
      )
      failedApply.close()
    }
    const apply = plan.apply(schema64)
    assert.deepEqual(Array.from(apply.run(table([3])).data), [3])
    closeAll(recipe, analyzer, plan, apply)
  }

  {
    assertResourceBeforeMalloc(() => wasm.TransformRecipe.fromJSON(
      new Uint8Array([1, 2]), { limits: { maxRecipeBytes: 1 } }
    ))

    const recipe = wasm.TransformRecipe.fromJSON(vectors.recipes.numeric_none_none)
    let overLimitReads = 0
    const overLimitField = new Proxy({}, {
      get() {
        overLimitReads++
        throw new Error('over-limit schema field must not be read')
      }
    })
    assertResourceBeforeMalloc(() => recipe.analyzer(
      [...schema64, overLimitField],
      { limits: { maxInputColumns: 1 } }
    ))
    assert.equal(overLimitReads, 0)
    const analyzer = recipe.analyzer(schema64, {
      limits: { maxAnalyzerRows: 1, maxAnalyzerInputBytes: 8 }
    })
    assertResourceBeforeMalloc(() => analyzer.push(table([1, 2])))
    analyzer.push(table([1]))
    const plan = analyzer.finalize()
    const planBytes = plan.toBytes()
    assertResourceBeforeMalloc(() => wasm.TransformPlan.fromBytes(
      planBytes, { limits: { maxPlanBytes: planBytes.length - 1 } }
    ))
    const apply = plan.apply(schema64, {
      limits: { maxApplyRows: 1, maxApplyInputBytes: 8 }
    })
    const emptyBacking = new ArrayBuffer(16)
    const emptyApply = plan.apply(schema64)
    for (const view of [
      new Float64Array(emptyBacking, 0, 0),
      new Float64Array(emptyBacking, 8, 0)
    ]) {
      const result = emptyApply.run({ rows: 0, columns: [view] })
      assert.equal(result.rows, 0)
      assert.equal(result.columns, 1)
      assert.equal(result.data.length, 0)
    }
    assert.deepEqual(Array.from(emptyApply.run(table([2])).data), [2])
    const zeroApply = plan.apply(schema64)
    assert.throws(
      () => zeroApply.run({ rows: 0, columns: [new Float64Array([1])] }),
      (error) => error instanceof native.TranfiTransformError && error.code === 100
    )
    assert.deepEqual(Array.from(zeroApply.run(table([3])).data), [3])
    zeroApply.close()
    assertResourceBeforeMalloc(() => apply.run(table([1, 2])))
    assert.deepEqual(Array.from(apply.run(table([2])).data), [2])
    closeAll(recipe, analyzer, plan, apply, emptyApply)
  }

  console.log('prepared-transform WASM Node tests passed')
}

main().catch((error) => {
  console.error(error)
  process.exitCode = 1
})
