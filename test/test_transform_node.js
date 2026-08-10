'use strict'

const assert = require('assert/strict')
const { createHash } = require('crypto')
const { once } = require('events')
const path = require('path')
const { Worker } = require('worker_threads')

const tf = require(path.join(__dirname, '../js/src/index.js'))
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
  'categorical_none_label_error',
  'categorical_none_label_other',
  'categorical_none_label_sentinel',
  'categorical_none_none',
  'categorical_none_onehot_all_zero',
  'categorical_none_onehot_error',
  'categorical_none_onehot_other',
  'infer_two_categories',
  'infer_two_categories_none',
  'infer_two_categories_median',
  'infer_two_categories_onehot_all_zero',
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

function medianNegativeZeroPlan(planBytes) {
  const original = Buffer.from(planBytes)
  const decoded = JSON.parse(original.subarray(52).toString('utf8'))
  assert.deepEqual(
    Buffer.from(JSON.stringify(decoded)),
    original.subarray(52),
    'exported TFTR payload must already be canonical JSON'
  )
  decoded.steps[0].numeric.impute.value.v = '8000000000000000'
  const payload = Buffer.from(JSON.stringify(decoded))
  const mutated = Buffer.alloc(52 + payload.length)
  original.copy(mutated, 0, 0, 20)
  mutated.writeBigUInt64LE(BigInt(payload.length), 12)
  createHash('sha256').update(payload).digest().copy(mutated, 20)
  payload.copy(mutated, 52)
  return mutated
}

function categoricalNegativeZeroPlan(planBytes) {
  const original = Buffer.from(planBytes)
  const decoded = JSON.parse(original.subarray(52).toString('utf8'))
  decoded.steps[0].categorical.categories[0].v = '8000000000000000'
  const payload = Buffer.from(JSON.stringify(decoded))
  const mutated = Buffer.alloc(52 + payload.length)
  original.copy(mutated, 0, 0, 20)
  mutated.writeBigUInt64LE(BigInt(payload.length), 12)
  createHash('sha256').update(payload).digest().copy(mutated, 20)
  payload.copy(mutated, 52)
  return mutated
}

function inferredSingleCategoryPlan(planBytes) {
  const original = Buffer.from(planBytes)
  const decoded = JSON.parse(original.subarray(52).toString('utf8'))
  assert.deepEqual(decoded.recipe, vectors.recipes.infer_two_categories)
  assert.equal(decoded.recipe.columns[0].kind.op, 'infer')
  assert.equal(decoded.steps[0].kind, 'categorical')
  assert.equal(decoded.steps[0].categorical.categories.length, 2)
  decoded.steps[0].categorical.categories.pop()
  const payload = Buffer.from(JSON.stringify(decoded))
  const mutated = Buffer.alloc(52 + payload.length)
  original.copy(mutated, 0, 0, 20)
  mutated.writeBigUInt64LE(BigInt(payload.length), 12)
  createHash('sha256').update(payload).digest().copy(mutated, 20)
  payload.copy(mutated, 52)
  return mutated
}

function table(values, ArrayType = Float64Array, validity) {
  const column = validity === undefined
    ? new ArrayType(values)
    : { data: new ArrayType(values), validity: Uint8Array.from(validity) }
  return { rows: values.length, columns: [column] }
}

function closeAll(...values) {
  for (const value of values.reverse()) {
    if (value) value.close()
  }
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

function largeFinalizeCancellationFixture() {
  const recipe = structuredClone(vectors.recipes.numeric_none_none)
  recipe.columns = []
  const schema = []
  for (let index = 0; index < 4; index++) {
    const id = `x${index}${'a'.repeat(2 * 1024 * 1024)}`
    const column = structuredClone(vectors.recipes.numeric_none_none.columns[0])
    column.sourceId = id
    recipe.columns.push(column)
    schema.push({
      id,
      name: `n${index}${'b'.repeat(2 * 1024 * 1024)}`,
      dtype: 'float64'
    })
  }
  return {
    recipe,
    schema,
    limits: {
      maxRecipeBytes: 16 * 1024 * 1024,
      maxStringBytes: 4 * 1024 * 1024
    }
  }
}

async function delayedCancel(delayMs = 2) {
  const control = new Int32Array(new SharedArrayBuffer(8))
  const worker = new Worker(`
    const { parentPort, workerData } = require('worker_threads')
    const control = new Int32Array(workerData)
    parentPort.postMessage('ready')
    Atomics.wait(control, 0, 0)
    const deadline = performance.now() + ${delayMs}
    while (performance.now() < deadline) {}
    Atomics.store(control, 1, 1)
    Atomics.notify(control, 1)
  `, { eval: true, workerData: control.buffer })
  const exit = once(worker, 'exit')
  await once(worker, 'message')
  return {
    flag: new Int32Array(control.buffer, 4, 1),
    start() {
      Atomics.store(control, 0, 1)
      Atomics.notify(control, 0)
    },
    async finish() {
      await exit
      assert.equal(Atomics.load(control, 1), 1)
    }
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

async function cancelAfterNativePoll(state, baseline) {
  const worker = new Worker(`
    const { parentPort, workerData } = require('worker_threads')
    const state = new Int32Array(workerData.buffer)
    parentPort.postMessage('ready')
    while (Atomics.load(state, 1) === workerData.baseline) {}
    Atomics.store(state, 0, 1)
    Atomics.notify(state, 0)
  `, {
    eval: true,
    workerData: { buffer: state.buffer, baseline }
  })
  const exit = once(worker, 'exit')
  await once(worker, 'message')
  return {
    async finish() {
      await exit
      assert.equal(Atomics.load(state, 0), 1)
      assert.notEqual(Atomics.load(state, 1), baseline)
    }
  }
}

function testSafeLimits() {
  const limits = tf.safeTransformLimits({ maxApplyRows: 7 })
  assert.equal(limits.maxRecipeBytes, 1048576)
  assert.equal(limits.maxOutputElementsPerCall, 134217728)
  assert.equal(limits.maxLiveHandles, 65535)
  assert.equal(limits.maxApplyRows, 7)
  assert.throws(
    () => tf.safeTransformLimits({ unknownLimit: 1 }),
    /unknown transform limit field/
  )
  assert.equal(Object.keys(limits).length, 22)
  for (const name of Object.keys(limits)) {
    assert.throws(
      () => tf.safeTransformLimits({ [name]: 0 }),
      (error) => error instanceof tf.TranfiTransformError && error.code === 100
    )
  }
}

function testSharedSemanticVectors() {
  const cases = vectors.semanticCases.filter((item) =>
    supportedRecipes.has(item.recipe) && !item.expectedError
  )
  for (const item of cases) {
    const inputDtype = item.inputDtype ?? 'float64'
    const inputSchema = inputDtype === 'float32'
      ? [{ id: 'x0', dtype: 'float32' }]
      : schema64
    const InputArray = inputDtype === 'float32' ? Float32Array : Float64Array
    const analyzeValues = item.analyze.rows.map((row) => doubleFromBits(row[0]))
    const applyValues = item.apply.rows.map((row) => doubleFromBits(row[0]))
    const expected = item.apply.expectedRows.flat()
    const expectedColumns = item.apply.expectedColumns ?? 1
    let referenceBytes = null

    for (const split of item.analyze.chunkSplits) {
      const recipe = tf.TransformRecipe.fromJSON(vectors.recipes[item.recipe])
      const analyzer = recipe.analyzer(inputSchema)
      let offset = 0
      for (const count of split) {
        analyzer.push(table(
          analyzeValues.slice(offset, offset + count), InputArray
        ))
        offset += count
      }
      assert.equal(offset, analyzeValues.length, `${item.id}: consumed rows`)
      const plan = analyzer.finalize()
      const bytes = plan.toBytes()
      if (referenceBytes === null) referenceBytes = bytes
      else assert.deepEqual(bytes, referenceBytes, `${item.id}: chunk-invariant TFTR`)
      assert.equal(
        plan.schemaJSON('input').toString(),
        `[{"dtype":"${inputDtype}","id":"x0","name":"x0"}]`
      )
      assert.match(plan.recipeSha256(), /^[0-9a-f]{64}$/)
      if (item.recipe === 'numeric_none_none' && inputDtype === 'float64') {
        assert.equal(
          plan.recipeSha256(),
          '12d0bfe06b5ceb8cdd63e857eff568b2ed0fbde4f1153490656b21558c621fda'
        )
      }
      const imported = tf.TransformPlan.fromBytes(bytes)
      assert.equal(imported.recipeSha256(), plan.recipeSha256())
      imported.close()
      if (item.expectedPlan.outputIds) {
        const outputSchema = JSON.parse(plan.schemaJSON('output').toString())
        assert.deepEqual(
          outputSchema.map((field) => field.id),
          item.expectedPlan.outputIds
        )
        const encodeOp = vectors.recipes[item.recipe].columns[0].categorical.encode.op
        if (encodeOp === 'onehot') {
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
        } else if (encodeOp === 'label') {
          assert.deepEqual(outputSchema.map((field) => field.category), [null])
          assert.deepEqual(outputSchema.map((field) => field.role), ['label'])
        } else {
          assert.deepEqual(outputSchema.map((field) => field.category), [null])
          assert.deepEqual(outputSchema.map((field) => field.role), ['value'])
        }
      }
      const apply = plan.apply(inputSchema)
      const result = apply.run(table(applyValues, InputArray))
      assert.equal(result.rows, applyValues.length)
      assert.equal(result.columns, expectedColumns)
      assert(result.data instanceof Float64Array)
      assert.deepEqual(Array.from(result.data, doubleBits), expected, `${item.id}: result bits`)
      closeAll(recipe, analyzer, plan, apply)
    }

    const loaded = tf.TransformPlan.fromBytes(referenceBytes)
    assert.deepEqual(loaded.toBytes(), referenceBytes, `${item.id}: imported TFTR`)
    loaded.close()
    if (item.id === 'infer-two-distinct-becomes-categorical') {
      assert.throws(
        () => tf.TransformPlan.fromBytes(referenceBytes, {
          limits: { maxCategoriesPerColumn: 1, maxTotalCategories: 1 }
        }),
        (error) => error instanceof tf.TranfiTransformError && error.code === 104
      )
      assert.throws(
        () => tf.TransformPlan.fromBytes(inferredSingleCategoryPlan(referenceBytes), {
          limits: { maxCategoriesPerColumn: 1, maxTotalCategories: 1 }
        }),
        (error) => error instanceof tf.TranfiTransformError && error.code === 106
      )
    }
    if (item.id === 'numeric-signed-zero-median') {
      assert.throws(
        () => tf.TransformPlan.fromBytes(medianNegativeZeroPlan(referenceBytes)),
        (error) => error instanceof tf.TranfiTransformError && error.code === 106
      )
    }
    if (item.id === 'categorical-mode-tie-smallest-signed-zero') {
      assert.throws(
        () => tf.TransformPlan.fromBytes(categoricalNegativeZeroPlan(referenceBytes)),
        (error) => error instanceof tf.TranfiTransformError && error.code === 106
      )
    }
  }
}

function testInferenceDeferredNumericDomain() {
  const item = vectors.semanticCases.find((entry) =>
    entry.id === 'infer-late-numeric-surfaces-deferred-overflow'
  )
  const values = item.analyze.rows.map((row) => doubleFromBits(row[0]))
  for (const split of item.analyze.chunkSplits) {
    const recipe = tf.TransformRecipe.fromJSON(vectors.recipes[item.recipe])
    const analyzer = recipe.analyzer(schema64)
    let offset = 0
    for (let index = 0; index < split.length; ++index) {
      const count = split[index]
      const input = table(values.slice(offset, offset + count))
      offset += count
      if (index + 1 === split.length) {
        assert.throws(
          () => analyzer.push(input),
          (error) => error instanceof tf.TranfiTransformError
            && error.code === item.expectedError.code
        )
        assert.throws(
          () => analyzer.push(input),
          (error) => error instanceof tf.TranfiTransformError && error.code === 112
        )
      } else {
        analyzer.push(input)
      }
    }
    assert.equal(offset, values.length)
    closeAll(recipe, analyzer)
  }
}

function testCategoricalModeErrorsLimitsAndFloat32() {
  {
    const item = vectors.semanticCases.find(
      (entry) => entry.id === 'infer-zero-rows-insufficient'
    )
    const recipe = tf.TransformRecipe.fromJSON(vectors.recipes[item.recipe])
    const analyzer = recipe.analyzer(schema64)
    analyzer.push(table([]))
    assert.throws(
      () => analyzer.finalize(),
      (error) => error instanceof tf.TranfiTransformError && error.code === 103
    )
    assert.throws(
      () => analyzer.finalize(),
      (error) => error instanceof tf.TranfiTransformError && error.code === 112
    )
    closeAll(recipe, analyzer)
  }
  for (const limits of [
    { maxCategoriesPerColumn: 1 },
    { maxTotalCategories: 1 }
  ]) {
    assert.throws(
      () => tf.TransformRecipe.fromJSON(vectors.recipes.infer_two_categories, {
        limits
      }),
      (error) => error instanceof tf.TranfiTransformError && error.code === 104
    )
  }
  {
    const recipe = tf.TransformRecipe.fromJSON(
      vectors.recipes.infer_two_categories,
      { limits: { maxCategoriesPerColumn: 2, maxTotalCategories: 2 } }
    )
    assert.throws(
      () => recipe.analyzer(schema64, {
        limits: { maxCategoriesPerColumn: 1 }
      }),
      (error) => error instanceof tf.TranfiTransformError && error.code === 104
    )
    const analyzer = recipe.analyzer(schema64, {
      limits: { maxCategoriesPerColumn: 2, maxTotalCategories: 2 }
    })
    analyzer.push(table([0, 1, 2]))
    const plan = analyzer.finalize()
    const apply = plan.apply(schema64)
    assert.deepEqual(Array.from(apply.run(table([NaN])).data), [1])
    closeAll(recipe, analyzer, plan, apply)
  }

  for (const item of vectors.semanticCases.filter((entry) =>
    entry.recipe.startsWith('categorical_') && entry.expectedError &&
      entry.expectedError.phase === 'finalize'
  )) {
    const recipe = tf.TransformRecipe.fromJSON(vectors.recipes[item.recipe])
    const analyzer = recipe.analyzer(schema64)
    analyzer.push(table(item.analyze.rows.map((row) => doubleFromBits(row[0]))))
    assert.throws(
      () => analyzer.finalize(),
      (error) => error instanceof tf.TranfiTransformError &&
        error.code === item.expectedError.code
    )
    assert.throws(
      () => analyzer.finalize(),
      (error) => error instanceof tf.TranfiTransformError && error.code === 112
    )
    closeAll(recipe, analyzer)
  }

  for (const caseId of [
    'categorical-mode-label-unknown-error',
    'categorical-mode-onehot-unknown-error',
    'categorical-none-label-error-missing',
    'categorical-none-onehot-error-missing'
  ]) {
    const item = vectors.semanticCases.find(
      (entry) => entry.id === caseId
    )
    const recipe = tf.TransformRecipe.fromJSON(vectors.recipes[item.recipe])
    const analyzer = recipe.analyzer(schema64)
    analyzer.push(table(item.analyze.rows.map((row) => doubleFromBits(row[0]))))
    const plan = analyzer.finalize()
    const apply = plan.apply(schema64)
    assert.throws(
      () => apply.run(table(item.apply.rows.map((row) => doubleFromBits(row[0])))),
      (error) => error instanceof tf.TranfiTransformError && error.code === 108
    )
    assert.throws(
      () => apply.run(table([1])),
      (error) => error instanceof tf.TranfiTransformError && error.code === 112
    )
    closeAll(recipe, analyzer, plan, apply)
  }

  for (const sentinel of [Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER]) {
    const config = structuredClone(vectors.recipes.categorical_mode_label_sentinel)
    config.columns[0].categorical.encode.sentinelLabel = sentinel
    tf.TransformRecipe.fromJSON(config).close()
  }
  for (const sentinel of [0.5, Number.MAX_SAFE_INTEGER + 1]) {
    const config = structuredClone(vectors.recipes.categorical_mode_label_sentinel)
    config.columns[0].categorical.encode.sentinelLabel = sentinel
    assert.throws(
      () => tf.TransformRecipe.fromJSON(config),
      (error) => error instanceof tf.TranfiTransformError && error.code === 101
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
    const recipe = tf.TransformRecipe.fromJSON(config)
    const analyzer = recipe.analyzer(collisionSchema)
    analyzer.push({
      rows: 1,
      columns: [new Float64Array([0]), new Float64Array([0])]
    })
    assert.throws(
      () => analyzer.finalize(),
      (error) => error instanceof tf.TranfiTransformError && error.code === 102
    )
    assert.throws(
      () => analyzer.finalize(),
      (error) => error instanceof tf.TranfiTransformError && error.code === 112
    )
    closeAll(recipe, analyzer)
  }

  const recipe = tf.TransformRecipe.fromJSON(vectors.recipes.categorical_mode_none)
  const analyzer = recipe.analyzer(schema64)
  analyzer.push(table([1, 2]))
  const plan = analyzer.finalize()
  const apply = plan.apply(schema64)
  assert.throws(
    () => apply.run(table([3])),
    (error) => error instanceof tf.TranfiTransformError && error.code === 108
  )
  assert.throws(
    () => apply.run(table([1])),
    (error) => error instanceof tf.TranfiTransformError && error.code === 112
  )
  const limited = recipe.analyzer(schema64, {
    limits: { maxCategoriesPerColumn: 2 }
  })
  assert.throws(
    () => limited.push(table([1, 2, 3])),
    (error) => error instanceof tf.TranfiTransformError && error.code === 104
  )
  assert.throws(
    () => limited.push(table([1])),
    (error) => error instanceof tf.TranfiTransformError && error.code === 112
  )
  closeAll(recipe, analyzer, plan, apply, limited)

  const schema32 = [{ id: 'x0', name: 'x0', dtype: 'float32' }]
  const recipe32 = tf.TransformRecipe.fromJSON(vectors.recipes.categorical_mode_none)
  const analyzer32 = recipe32.analyzer(schema32)
  analyzer32.push(table([-0, 0, 1, 1], Float32Array))
  const plan32 = analyzer32.finalize()
  const payload = JSON.parse(Buffer.from(plan32.toBytes()).subarray(52))
  assert.deepEqual(payload.steps[0].categorical.categories, [
    { t: 'f32', v: '00000000' },
    { t: 'f32', v: '3f800000' }
  ])
  const apply32 = plan32.apply(schema32)
  assert.deepEqual(
    Array.from(apply32.run(table([-0, NaN], Float32Array)).data, doubleBits),
    ['8000000000000000', '0000000000000000']
  )
  closeAll(recipe32, analyzer32, plan32, apply32)

  const labelRecipe32 = tf.TransformRecipe.fromJSON(
    vectors.recipes.categorical_mode_label_other
  )
  const labelAnalyzer32 = labelRecipe32.analyzer(schema32)
  labelAnalyzer32.push(table([2, 1, 2], Float32Array))
  const labelPlan32 = labelAnalyzer32.finalize()
  const labelApply32 = labelPlan32.apply(schema32)
  assert.deepEqual(
    Array.from(
      labelApply32.run(table([1, 3, NaN], Float32Array)).data,
      doubleBits
    ),
    ['0000000000000000', '4000000000000000', '3ff0000000000000']
  )
  closeAll(labelRecipe32, labelAnalyzer32, labelPlan32, labelApply32)

  const subnormalAnalyze = float32FromBits([0x00000001, 0x80000001, 0x00000000])
  const subnormalApply = float32FromBits([
    0x7fc00000, 0x80000001, 0x00000000, 0x00000001
  ])
  const subnormalRecipe = tf.TransformRecipe.fromJSON(
    vectors.recipes.categorical_mode_none
  )
  const subnormalAnalyzer = subnormalRecipe.analyzer(schema32)
  subnormalAnalyzer.push({ rows: 3, columns: [subnormalAnalyze] })
  const subnormalPlan = subnormalAnalyzer.finalize()
  const subnormalPayload = JSON.parse(
    Buffer.from(subnormalPlan.toBytes()).subarray(52)
  )
  assert.deepEqual(subnormalPayload.steps[0].categorical.categories, [
    { t: 'f32', v: '80000001' },
    { t: 'f32', v: '00000000' },
    { t: 'f32', v: '00000001' }
  ])
  const subnormalSession = subnormalPlan.apply(schema32)
  assert.deepEqual(
    Array.from(subnormalSession.run({ rows: 4, columns: [subnormalApply] }).data,
      doubleBits),
    [
      'b6a0000000000000', 'b6a0000000000000',
      '0000000000000000', '36a0000000000000'
    ]
  )
  closeAll(
    subnormalRecipe, subnormalAnalyzer, subnormalPlan, subnormalSession
  )
}

function testOnehotExactLimitsAndUnicode() {
  const values = Array.from({ length: 11 }, (_, index) => index)
  const sourceId = 'é:🔥'
  const unicodeConfig = structuredClone(
    vectors.recipes.categorical_mode_onehot_all_zero
  )
  unicodeConfig.columns[0].sourceId = sourceId
  const unicodeSchema = [{ id: sourceId, name: sourceId, dtype: 'float64' }]
  const unicodeRecipe = tf.TransformRecipe.fromJSON(unicodeConfig)
  const unicodeAnalyzer = unicodeRecipe.analyzer(unicodeSchema)
  unicodeAnalyzer.push(table(values))
  const unicodePlan = unicodeAnalyzer.finalize()
  const unicodeOutput = JSON.parse(unicodePlan.schemaJSON('output'))
  assert.equal(unicodeOutput.length, 11)
  assert.equal(
    unicodeOutput[10].id,
    '%C3%A9%3A%F0%9F%94%A5%3Aonehot%3A10'
  )
  assert.equal(unicodeOutput[10].name, unicodeOutput[10].id)
  const unicodeApply = unicodePlan.apply(unicodeSchema)
  const unicodeResult = unicodeApply.run(table([10]))
  assert.equal(unicodeResult.columns, 11)
  assert.deepEqual(Array.from(unicodeResult.data), [
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1
  ])
  closeAll(unicodeRecipe, unicodeAnalyzer, unicodePlan, unicodeApply)

  const analyze = [1, 2]
  const applyValues = [1, 3]
  const recipe = tf.TransformRecipe.fromJSON(
    vectors.recipes.categorical_mode_onehot_other
  )
  const analyzer = recipe.analyzer(schema64)
  analyzer.push(table(analyze))
  const plan = analyzer.finalize()
  const bytes = plan.toBytes()
  assert.throws(
    () => tf.TransformPlan.fromBytes(bytes, {
      limits: { maxOutputElementsPerCall: 134217727 }
    }),
    (error) => error instanceof tf.TranfiTransformError && error.code === 104
  )
  tf.TransformPlan.fromBytes(bytes, {
    limits: { maxOutputElementsPerCall: 134217728 }
  }).close()
  const hostUnder = plan.apply(schema64, {
    limits: { maxOutputElementsPerCall: 5 }
  })
  assert.throws(
    () => hostUnder.run(table(applyValues)),
    (error) => error instanceof tf.TranfiTransformError && error.code === 104
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
    const semanticRecipe = tf.TransformRecipe.fromJSON(config)
    const semanticAnalyzer = semanticRecipe.analyzer(schema64)
    semanticAnalyzer.push(table(analyze))
    const semanticPlan = semanticAnalyzer.finalize()
    const semanticApply = semanticPlan.apply(schema64)
    if (semanticLimit === 5) {
      assert.throws(
        () => semanticApply.run(table(applyValues)),
        (error) => error instanceof tf.TranfiTransformError && error.code === 104
      )
    } else {
      assert.equal(semanticApply.run(table(applyValues)).data.length, 6)
    }
    closeAll(semanticRecipe, semanticAnalyzer, semanticPlan, semanticApply)
  }
}

function testMedianErrorsAndLimits() {
  for (const item of vectors.semanticCases.filter((entry) =>
    entry.recipe === 'numeric_median_none' && entry.expectedError
  )) {
    const recipe = tf.TransformRecipe.fromJSON(vectors.recipes[item.recipe])
    const analyzer = recipe.analyzer(schema64)
    const values = item.analyze.rows.map((row) => doubleFromBits(row[0]))
    analyzer.push(table(values))
    assert.throws(
      () => analyzer.finalize(),
      (error) => error instanceof tf.TranfiTransformError
        && error.code === item.expectedError.code
    )
    closeAll(recipe, analyzer)
  }

  const recipe = tf.TransformRecipe.fromJSON(vectors.recipes.numeric_median_none)
  const analyzer = recipe.analyzer(schema64, {
    limits: { maxAllocationsPerSession: 9 }
  })
  assert.throws(
    () => analyzer.push(table([1])),
    (error) => error instanceof tf.TranfiTransformError && error.code === 104
  )
  assert.throws(
    () => analyzer.push(table([1])),
    (error) => error instanceof tf.TranfiTransformError && error.code === 112
  )
  closeAll(recipe, analyzer)
}

function testF32ValidityAndParentLifetime() {
  const schema = [{ id: 'x0', name: 'feature', dtype: 'float32' }]
  const config = structuredClone(vectors.recipes.numeric_median_none)
  const recipe = tf.TransformRecipe.fromJSON(config)
  const analyzer = recipe.analyzer(schema)
  recipe.close()
  analyzer.push(table([1, 99, 3], Float32Array, [0x05]))
  const plan = analyzer.finalize()
  analyzer.close()
  const apply = plan.apply(schema)
  plan.close()
  const result = apply.run(table([1, NaN, 3], Float32Array))
  assert.deepEqual(
    Array.from(result.data, doubleBits),
    ['3ff0000000000000', '4000000000000000', '4008000000000000']
  )
  apply.close()
  apply.close()

  const strideRecipe = tf.TransformRecipe.fromJSON(vectors.recipes.numeric_none_none)
  const strideAnalyzer = strideRecipe.analyzer(schema)
  const stridedSource = new Float32Array([1, 99, 2, 99, 3])
  strideAnalyzer.push({
    rows: 3,
    columns: [{ data: stridedSource.subarray(0), strideBytes: 8 }]
  })
  const stridePlan = strideAnalyzer.finalize()
  const strideApply = stridePlan.apply(schema)
  const strideResult = strideApply.run({
    rows: 3,
    columns: [{ data: stridedSource.subarray(0), strideBytes: 8 }]
  })
  assert.deepEqual(Array.from(strideResult.data), [1, 2, 3])
  assert.deepEqual(Array.from(stridedSource), [1, 99, 2, 99, 3])
  closeAll(strideRecipe, strideAnalyzer, stridePlan, strideApply)
}

function testErrorsLimitsAndClosedState() {
  for (const invalid of ['\ud800', '\udc00']) {
    assert.throws(
      () => tf.TransformRecipe.fromJSON(invalid),
      /unpaired UTF-16 surrogate/
    )
    const invalidRecipe = structuredClone(vectors.recipes.numeric_none_none)
    invalidRecipe.columns[0].sourceId = invalid
    assert.throws(
      () => tf.TransformRecipe.fromJSON(invalidRecipe),
      /unpaired UTF-16 surrogate/
    )
    const schemaRecipe = tf.TransformRecipe.fromJSON(
      vectors.recipes.numeric_none_none
    )
    assert.throws(
      () => schemaRecipe.analyzer([{ id: 'x0', name: invalid, dtype: 'float64' }]),
      /unpaired UTF-16 surrogate/
    )
    assert.throws(
      () => schemaRecipe.analyzer([{ id: invalid, name: 'x0', dtype: 'float64' }]),
      /unpaired UTF-16 surrogate/
    )
    schemaRecipe.close()
  }
  const astralId = 'x\ud83d\ude00'
  const astralName = 'feature\ud83d\ude80'
  const astralConfig = structuredClone(vectors.recipes.numeric_none_none)
  astralConfig.columns[0].sourceId = astralId
  const astralRecipe = tf.TransformRecipe.fromJSON(astralConfig)
  const astralAnalyzer = astralRecipe.analyzer([
    { id: astralId, name: astralName, dtype: 'float64' }
  ])
  astralAnalyzer.push(table([1]))
  const astralPlan = astralAnalyzer.finalize()
  assert.equal(
    astralPlan.schemaJSON('input').toString(),
    `[{"dtype":"float64","id":"${astralId}","name":"${astralName}"}]`
  )
  closeAll(astralRecipe, astralAnalyzer, astralPlan)

  assert.throws(
    () => tf.TransformRecipe.fromJSON({}),
    (error) => error instanceof tf.TranfiTransformError && error.code === 101
  )
  assert.throws(
    () => tf.TransformPlan.fromBytes(Buffer.from('not-a-plan')),
    (error) => error instanceof tf.TranfiTransformError && error.code === 106
  )

  const recipe = tf.TransformRecipe.fromJSON(vectors.recipes.numeric_none_none)
  assert.throws(
    () => recipe.analyzer([{ id: 'wrong', dtype: 'float64' }]),
    (error) => error instanceof tf.TranfiTransformError && error.code === 102
  )
  const analyzer = recipe.analyzer(schema64)
  assert.throws(
    () => analyzer.push(table([Infinity])),
    (error) => error instanceof tf.TranfiTransformError && error.code === 107
  )
  analyzer.close()
  recipe.close()
  assert.throws(
    () => recipe.analyzer(schema64),
    (error) => error instanceof tf.TranfiTransformError && error.code === 112
  )

  const limitedRecipe = tf.TransformRecipe.fromJSON(vectors.recipes.numeric_none_none)
  const limitedAnalyzer = limitedRecipe.analyzer(schema64)
  limitedAnalyzer.push(table([1]))
  const plan = limitedAnalyzer.finalize()
  const planBytes = plan.toBytes()
  const emptyBacking = new ArrayBuffer(16)
  const offsetEmptyApply = plan.apply(schema64)
  for (const view of [
    new Float64Array(emptyBacking, 0, 0),
    new Float64Array(emptyBacking, 8, 0)
  ]) {
    const result = offsetEmptyApply.run({ rows: 0, columns: [view] })
    assert.equal(result.rows, 0)
    assert.equal(result.columns, 1)
    assert.equal(result.data.length, 0)
  }
  assert.deepEqual(Array.from(offsetEmptyApply.run(table([2])).data), [2])
  const emptyApply = plan.apply(schema64)
  const empty = emptyApply.run(table([]))
  assert.equal(empty.rows, 0)
  assert.equal(empty.columns, 1)
  assert.equal(empty.data.length, 0)
  assert.throws(
    () => emptyApply.run({ rows: 0, columns: [new Float64Array([1])] }),
    (error) => error instanceof tf.TranfiTransformError && error.code === 100
  )
  assert.throws(
    () => emptyApply.run(table([1])),
    (error) => error instanceof tf.TranfiTransformError && error.code === 112
  )
  const limitedApply = plan.apply(schema64, { limits: { maxApplyRows: 1 } })
  assert.throws(
    () => limitedApply.run(table([1, 2])),
    (error) => error instanceof tf.TranfiTransformError && error.code === 104
  )
  assert.throws(
    () => limitedApply.run(table([1])),
    (error) => error instanceof tf.TranfiTransformError && error.code === 112
  )
  const outputLimitedApply = plan.apply(schema64, {
    limits: { maxOutputElementsPerCall: 1 }
  })
  assert.throws(
    () => outputLimitedApply.run(table([1, 2])),
    (error) => error instanceof tf.TranfiTransformError && error.code === 104
  )
  assert.deepEqual(Array.from(outputLimitedApply.run(table([1])).data), [1])
  closeAll(
    limitedRecipe, limitedAnalyzer, plan,
    offsetEmptyApply, emptyApply, limitedApply, outputLimitedApply
  )

  if (typeof SharedArrayBuffer !== 'undefined') {
    const importFlag = new Int32Array(new SharedArrayBuffer(4))
    Atomics.store(importFlag, 0, 1)
    assert.throws(
      () => tf.TransformPlan.fromBytes(planBytes, { cancelFlag: importFlag }),
      (error) => error instanceof tf.TranfiTransformError && error.code === 109
    )

    Atomics.store(importFlag, 0, 0)
    const imported = tf.TransformPlan.fromBytes(planBytes)
    const applyFlag = new Int32Array(new SharedArrayBuffer(4))
    const cancelledApply = imported.apply(schema64, { cancelFlag: applyFlag })
    Atomics.store(applyFlag, 0, 1)
    assert.throws(
      () => cancelledApply.run(table([1])),
      (error) => error instanceof tf.TranfiTransformError && error.code === 109
    )
    assert.throws(
      () => cancelledApply.run(table([1])),
      (error) => error instanceof tf.TranfiTransformError && error.code === 112
    )
    closeAll(imported, cancelledApply)

    const finalizeFlag = new Int32Array(new SharedArrayBuffer(4))
    const finalizeRecipe = tf.TransformRecipe.fromJSON(
      vectors.recipes.numeric_none_none
    )
    const finalizeAnalyzer = finalizeRecipe.analyzer(schema64, {
      cancelFlag: finalizeFlag
    })
    finalizeAnalyzer.push(table([1]))
    Atomics.store(finalizeFlag, 0, 1)
    assert.throws(
      () => finalizeAnalyzer.finalize(),
      (error) => error instanceof tf.TranfiTransformError && error.code === 109
    )
    assert.throws(
      () => finalizeAnalyzer.finalize(),
      (error) => error instanceof tf.TranfiTransformError && error.code === 112
    )
    closeAll(finalizeRecipe, finalizeAnalyzer)
  }

  const schemaLimitedRecipe = tf.TransformRecipe.fromJSON(vectors.recipes.numeric_none_none)
  let overLimitReads = 0
  const overLimitField = new Proxy({}, {
    get() {
      overLimitReads++
      throw new Error('over-limit schema field must not be read')
    }
  })
  assert.throws(
    () => schemaLimitedRecipe.analyzer([
      ...schema64,
      overLimitField
    ], { limits: { maxInputColumns: 1 } }),
    (error) => error instanceof tf.TranfiTransformError && error.code === 104
  )
  assert.equal(overLimitReads, 0)
  assert.throws(
    () => schemaLimitedRecipe.analyzer(schema64, { limits: { maxStringBytes: 1 } }),
    (error) => error instanceof tf.TranfiTransformError && error.code === 104
  )
  schemaLimitedRecipe.close()

  const native = require('../js/build/Release/tranfi_napi.node')
  const recipeJSON = JSON.stringify(vectors.recipes.numeric_none_none)
  const recipeBytes = Buffer.from(recipeJSON)
  for (const input of [
    recipeJSON,
    recipeBytes,
    new Uint8Array(recipeBytes.buffer, recipeBytes.byteOffset, recipeBytes.byteLength)
  ]) {
    assert.throws(
      () => native.transformRecipeFromJson(input, {
        maxRecipeBytes: recipeBytes.length - 1
      }),
      (error) => error.name === 'TranfiTransformError' && error.code === 104
    )
  }
  for (const name of Object.keys(native.transformSafeLimits())) {
    assert.throws(
      () => native.transformRecipeFromJson(recipeJSON, { [name]: 0 }),
      (error) => error.name === 'TranfiTransformError' && error.code === 100
    )
  }
  const rawRecipe = native.transformRecipeFromJson(recipeJSON)
  assert.throws(
    () => native.transformAnalyzerCreate(rawRecipe, {
      fields: [{ id: 'x0', name: 'x0', dtype: 'float64' }]
    }, { limits: { maxAllocationBytes: 1 } }),
    (error) => error.name === 'TranfiTransformError' && error.code === 104
  )
  native.transformDispose(rawRecipe)
  if (typeof SharedArrayBuffer !== 'undefined') {
    const requested = new Int32Array(new SharedArrayBuffer(4))
    Atomics.store(requested, 0, 1)
    const checkedRecipe = tf.TransformRecipe.fromJSON(
      vectors.recipes.numeric_none_none
    )
    let schemaReads = 0
    const unreadSchema = new Proxy({}, {
      get() {
        schemaReads++
        throw new Error('cancelled schema must not be read')
      }
    })
    assert.throws(
      () => checkedRecipe.analyzer(unreadSchema, { cancelFlag: requested }),
      (error) => error instanceof tf.TranfiTransformError && error.code === 109
    )
    assert.equal(schemaReads, 0)
    checkedRecipe.close()
  }
  assert.throws(
    () => native.transformAnalyzerCreate({}, { fields: [] }),
    /prepared-transform handle/
  )
  assert.throws(() => native.transformDispose({}), /prepared-transform handle/)
}

async function testInCallCancellation() {
  if (typeof SharedArrayBuffer === 'undefined') return
  const control = new Int32Array(new SharedArrayBuffer(8))
  const cancelFlag = new Int32Array(control.buffer, 4, 1)
  const worker = new Worker(`
    const { parentPort, workerData } = require('worker_threads')
    const control = new Int32Array(workerData)
    parentPort.postMessage('ready')
    Atomics.wait(control, 0, 0)
    const deadline = performance.now() + 10
    while (performance.now() < deadline) {}
    Atomics.store(control, 1, 1)
    Atomics.notify(control, 1)
  `, { eval: true, workerData: control.buffer })
  const exitPromise = once(worker, 'exit')
  await once(worker, 'message')

  const recipe = tf.TransformRecipe.fromJSON(vectors.recipes.numeric_none_none)
  const analyzer = recipe.analyzer(schema64, { cancelFlag })
  const values = new Float64Array(16 * 1024 * 1024)
  Atomics.store(control, 0, 1)
  Atomics.notify(control, 0)
  assert.throws(
    () => analyzer.push({ rows: values.length, columns: [values] }),
    (error) => error instanceof tf.TranfiTransformError && error.code === 109
  )
  assert.equal(Atomics.load(cancelFlag, 0), 1)
  assert.throws(
    () => analyzer.push(table([1])),
    (error) => error instanceof tf.TranfiTransformError && error.code === 112
  )
  closeAll(recipe, analyzer)
  await exitPromise

  const large = largeCancellationFixture()
  const largeRecipe = tf.TransformRecipe.fromJSON(large.recipe, {
    limits: large.limits
  })
  const schemaCancelFlag = new Int32Array(new SharedArrayBuffer(4))
  const schemaCancel = await cancelAfterSignal(schemaCancelFlag)
  const signalledField = new Proxy(large.schema[0], {
    get(target, property, receiver) {
      if (property === 'id') schemaCancel.signal()
      return Reflect.get(target, property, receiver)
    }
  })
  assert.throws(
    () => largeRecipe.analyzer([signalledField], {
      limits: large.limits,
      cancelFlag: schemaCancelFlag
    }),
    (error) => error instanceof tf.TranfiTransformError && error.code === 109
  )
  await schemaCancel.finish()
  const largeAnalyzer = largeRecipe.analyzer(large.schema, {
    limits: large.limits
  })
  largeAnalyzer.push({ rows: 1, columns: [new Float64Array([1])] })
  const largePlan = largeAnalyzer.finalize()
  const largePlanBytes = largePlan.toBytes({ limits: large.limits })
  const exactStringBytes = Buffer.byteLength(large.schema[0].id)
  assert.throws(
    () => largePlan.toBytes({
      limits: { ...large.limits, maxStringBytes: exactStringBytes - 1 }
    }),
    (error) => error instanceof tf.TranfiTransformError && error.code === 104
  )
  assert.throws(
    () => largePlan.schemaJSON('input', {
      limits: { ...large.limits, maxStringBytes: exactStringBytes - 1 }
    }),
    (error) => error instanceof tf.TranfiTransformError && error.code === 104
  )
  assert.deepEqual(
    largePlan.toBytes({
      limits: { ...large.limits, maxStringBytes: exactStringBytes }
    }),
    largePlanBytes
  )
  assert(largePlan.schemaJSON('input', {
    limits: { ...large.limits, maxStringBytes: exactStringBytes }
  }).length > exactStringBytes)
  closeAll(largeRecipe, largeAnalyzer, largePlan)

  const finalizeLarge = largeFinalizeCancellationFixture()
  const finalizeState = new Int32Array(new SharedArrayBuffer(8))
  const finalizeRecipe = tf.TransformRecipe.fromJSON(finalizeLarge.recipe, {
    limits: finalizeLarge.limits
  })
  const finalizeAnalyzer = finalizeRecipe.analyzer(finalizeLarge.schema, {
    limits: finalizeLarge.limits,
    cancelFlag: finalizeState
  })
  finalizeAnalyzer.push({
    rows: 1,
    columns: finalizeLarge.schema.map(() => new Float64Array([1]))
  })
  const finalizeBaseline = Atomics.load(finalizeState, 1)
  const finalizeCancel = await cancelAfterNativePoll(
    finalizeState, finalizeBaseline
  )
  const finalizeStarted = performance.now()
  assert.throws(
    () => finalizeAnalyzer.finalize(),
    (error) => error instanceof tf.TranfiTransformError && error.code === 109
  )
  assert(performance.now() - finalizeStarted < 5000)
  await finalizeCancel.finish()
  assert.notEqual(Atomics.load(finalizeState, 1), finalizeBaseline)
  assert.throws(
    () => finalizeAnalyzer.finalize(),
    (error) => error instanceof tf.TranfiTransformError && error.code === 112
  )
  closeAll(finalizeRecipe, finalizeAnalyzer)

  const importCancel = await delayedCancel()
  importCancel.start()
  const importStarted = performance.now()
  assert.throws(
    () => tf.TransformPlan.fromBytes(largePlanBytes, {
      limits: large.limits,
      cancelFlag: importCancel.flag
    }),
    (error) => error instanceof tf.TranfiTransformError && error.code === 109
  )
  assert(performance.now() - importStarted < 5000)
  await importCancel.finish()
  const imported = tf.TransformPlan.fromBytes(largePlanBytes, {
    limits: large.limits
  })

  const applyCancel = await delayedCancel()
  const apply = imported.apply(large.schema, {
    limits: large.limits,
    cancelFlag: applyCancel.flag
  })
  const applyRows = 4 * 1024 * 1024
  const applyValues = new Float64Array(applyRows)
  applyCancel.start()
  const applyStarted = performance.now()
  assert.throws(
    () => apply.run({ rows: applyRows, columns: [applyValues] }),
    (error) => error instanceof tf.TranfiTransformError && error.code === 109
  )
  assert(performance.now() - applyStarted < 5000)
  await applyCancel.finish()
  assert.throws(
    () => apply.run(table([1])),
    (error) => error instanceof tf.TranfiTransformError && error.code === 112
  )
  closeAll(imported, apply)
}

async function main() {
  testSafeLimits()
  testSharedSemanticVectors()
  testInferenceDeferredNumericDomain()
  testMedianErrorsAndLimits()
  testCategoricalModeErrorsLimitsAndFloat32()
  testOnehotExactLimitsAndUnicode()
  testF32ValidityAndParentLifetime()
  testErrorsLimitsAndClosedState()
  await testInCallCancellation()
  console.log('prepared-transform Node tests passed')
}

main().catch((error) => {
  console.error(error)
  process.exitCode = 1
})
