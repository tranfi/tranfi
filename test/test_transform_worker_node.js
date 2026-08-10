'use strict'

const assert = require('assert/strict')
const path = require('path')
const { Worker } = require('worker_threads')

const native = require(path.join(__dirname, '../js/src/index.js'))
const { createWorkerClient } = require(path.join(__dirname, '../js/wasm/worker.js'))
const vectors = require('./vectors/prepared_transform_v1.json')

const schema = [{ id: 'x0', dtype: 'float64' }]

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

function buildNativePlan(fixture) {
  const recipe = native.TransformRecipe.fromJSON(fixture.recipe, {
    limits: fixture.limits
  })
  const analyzer = recipe.analyzer(fixture.schema, {
    limits: fixture.limits
  })
  analyzer.push({ rows: 1, columns: [new Float64Array([1])] })
  const plan = analyzer.finalize()
  const bytes = plan.toBytes({ limits: fixture.limits })
  plan.close()
  analyzer.close()
  recipe.close()
  return bytes
}

function doubleBits(value) {
  const buffer = new ArrayBuffer(8)
  const view = new DataView(buffer)
  view.setFloat64(0, value, false)
  return view.getBigUint64(0, false).toString(16).padStart(16, '0')
}

function float32FromBits(bits) {
  const buffer = new ArrayBuffer(bits.length * 4)
  const view = new DataView(buffer)
  for (let i = 0; i < bits.length; ++i) view.setUint32(i * 4, bits[i], true)
  return new Float32Array(buffer)
}

async function main() {
  const workerPath = path.join(__dirname, '../js/wasm/worker.js')
  const worker = new Worker(workerPath)
  const client = createWorkerClient(worker, { terminateOnDispose: true })
  try {
    const analyzeTable = {
      rows: 4,
      columns: [new Float64Array([1, NaN, 3, NaN])]
    }
    const planBytes = await client.analyzeTransform(
      vectors.recipes.numeric_mean_standard,
      schema,
      [
        { rows: 1, columns: [analyzeTable.columns[0].slice(0, 1)] },
        { rows: 3, columns: [analyzeTable.columns[0].slice(1)] }
      ]
    )
    const nativePlan = native.TransformPlan.fromBytes(planBytes)
    assert.deepEqual(nativePlan.toBytes(), Buffer.from(planBytes))
    nativePlan.close()

    const result = await client.applyTransform(
      planBytes,
      schema,
      { rows: 3, columns: [new Float64Array([1, NaN, 3])] }
    )
    assert.equal(result.rows, 3)
    assert.equal(result.columns, 1)
    assert.deepEqual(Array.from(result.data, doubleBits), [
      'bff6a09e667f3bcc',
      '0000000000000000',
      '3ff6a09e667f3bcc'
    ])

    const medianChunks = [
      { rows: 2, columns: [new Float64Array([9, 1])] },
      { rows: 2, columns: [new Float64Array([5, NaN])] }
    ]
    const medianPlanBytes = await client.analyzeTransform(
      vectors.recipes.numeric_median_none,
      schema,
      medianChunks
    )
    const medianRecipe = native.TransformRecipe.fromJSON(
      vectors.recipes.numeric_median_none
    )
    const medianAnalyzer = medianRecipe.analyzer(schema)
    medianAnalyzer.push({ rows: 2, columns: [new Float64Array([9, 1])] })
    medianAnalyzer.push({ rows: 2, columns: [new Float64Array([5, NaN])] })
    const medianNativePlan = medianAnalyzer.finalize()
    assert.deepEqual(
      Buffer.from(medianPlanBytes),
      medianNativePlan.toBytes(),
      'worker and native exact-median TFTR bytes'
    )
    const medianResult = await client.applyTransform(
      medianPlanBytes,
      schema,
      { rows: 1, columns: [new Float64Array([NaN])] }
    )
    assert.deepEqual(Array.from(medianResult.data, doubleBits), [
      '4014000000000000'
    ])
    medianNativePlan.close()
    medianAnalyzer.close()
    medianRecipe.close()

    const categoricalChunks = [
      { rows: 3, columns: [new Float64Array([-0, 0, 1])] },
      { rows: 4, columns: [new Float64Array([3, 1, 3, NaN])] }
    ]
    const categoricalPlanBytes = await client.analyzeTransform(
      vectors.recipes.categorical_mode_none,
      schema,
      categoricalChunks
    )
    const categoricalRecipe = native.TransformRecipe.fromJSON(
      vectors.recipes.categorical_mode_none
    )
    const categoricalAnalyzer = categoricalRecipe.analyzer(schema)
    for (const chunk of categoricalChunks) categoricalAnalyzer.push(chunk)
    const categoricalNativePlan = categoricalAnalyzer.finalize()
    assert.deepEqual(
      Buffer.from(categoricalPlanBytes),
      categoricalNativePlan.toBytes(),
      'worker and native categorical-mode TFTR bytes'
    )
    const categoricalResult = await client.applyTransform(
      categoricalPlanBytes,
      schema,
      { rows: 2, columns: [new Float64Array([-0, NaN])] }
    )
    assert.deepEqual(Array.from(categoricalResult.data, doubleBits), [
      '8000000000000000',
      '0000000000000000'
    ])
    await assert.rejects(
      client.applyTransform(
        categoricalPlanBytes,
        schema,
        { rows: 1, columns: [new Float64Array([2])] }
      ),
      (error) => error instanceof native.TranfiTransformError && error.code === 108
    )
    categoricalNativePlan.close()
    categoricalAnalyzer.close()
    categoricalRecipe.close()

    const noneOnehotChunks = [
      { rows: 1, columns: [new Float64Array([2])] },
      { rows: 2, columns: [new Float64Array([1, NaN])] }
    ]
    const noneOnehotPlanBytes = await client.analyzeTransform(
      vectors.recipes.categorical_none_onehot_all_zero,
      schema,
      noneOnehotChunks
    )
    const noneOnehotRecipe = native.TransformRecipe.fromJSON(
      vectors.recipes.categorical_none_onehot_all_zero
    )
    const noneOnehotAnalyzer = noneOnehotRecipe.analyzer(schema)
    for (const chunk of noneOnehotChunks) noneOnehotAnalyzer.push(chunk)
    const noneOnehotNativePlan = noneOnehotAnalyzer.finalize()
    assert.deepEqual(
      Buffer.from(noneOnehotPlanBytes),
      noneOnehotNativePlan.toBytes(),
      'worker and native categorical-impute-none TFTR bytes'
    )
    const noneOnehotResult = await client.applyTransform(
      noneOnehotPlanBytes,
      schema,
      { rows: 3, columns: [new Float64Array([1, 3, NaN])] }
    )
    assert.deepEqual(Array.from(noneOnehotResult.data, doubleBits), [
      '3ff0000000000000', '0000000000000000',
      '0000000000000000', '0000000000000000',
      '0000000000000000', '0000000000000000'
    ])
    noneOnehotNativePlan.close()
    noneOnehotAnalyzer.close()
    noneOnehotRecipe.close()

    const inferenceChunks = [
      { rows: 2, columns: [new Float64Array([0, 1])] },
      { rows: 1, columns: [new Float64Array([0])] }
    ]
    const inferencePlanBytes = await client.analyzeTransform(
      vectors.recipes.infer_two_categories_onehot_all_zero,
      schema,
      inferenceChunks,
      { limits: { maxCategoriesPerColumn: 2, maxTotalCategories: 2 } }
    )
    const inferenceRecipe = native.TransformRecipe.fromJSON(
      vectors.recipes.infer_two_categories_onehot_all_zero,
      { limits: { maxCategoriesPerColumn: 2, maxTotalCategories: 2 } }
    )
    const inferenceAnalyzer = inferenceRecipe.analyzer(schema, {
      limits: { maxCategoriesPerColumn: 2, maxTotalCategories: 2 }
    })
    for (const chunk of inferenceChunks) inferenceAnalyzer.push(chunk)
    const inferenceNativePlan = inferenceAnalyzer.finalize()
    assert.deepEqual(
      Buffer.from(inferencePlanBytes),
      inferenceNativePlan.toBytes(),
      'worker and native inferred-one-hot TFTR bytes'
    )
    const inferenceResult = await client.applyTransform(
      inferencePlanBytes,
      schema,
      { rows: 2, columns: [new Float64Array([NaN, 3])] }
    )
    assert.deepEqual(Array.from(inferenceResult.data, doubleBits), [
      '3ff0000000000000',
      '0000000000000000',
      '0000000000000000',
      '0000000000000000'
    ])
    inferenceNativePlan.close()
    inferenceAnalyzer.close()
    inferenceRecipe.close()

    const labelChunks = [
      { rows: 2, columns: [new Float64Array([1, 2])] },
      { rows: 2, columns: [new Float64Array([2, NaN])] }
    ]
    const labelPlanBytes = await client.analyzeTransform(
      vectors.recipes.categorical_mode_label_other,
      schema,
      labelChunks
    )
    const labelRecipe = native.TransformRecipe.fromJSON(
      vectors.recipes.categorical_mode_label_other
    )
    const labelAnalyzer = labelRecipe.analyzer(schema)
    for (const chunk of labelChunks) labelAnalyzer.push(chunk)
    const labelNativePlan = labelAnalyzer.finalize()
    assert.deepEqual(
      Buffer.from(labelPlanBytes),
      labelNativePlan.toBytes(),
      'worker and native categorical-label TFTR bytes'
    )
    assert.equal(
      labelNativePlan.schemaJSON('output').toString(),
      '[{"category":null,"dtype":"float64","id":"x0%3Alabel",' +
        '"name":"x0%3Alabel","role":"label","sourceId":"x0"}]'
    )
    const labelResult = await client.applyTransform(
      labelPlanBytes,
      schema,
      { rows: 3, columns: [new Float64Array([1, 3, NaN])] }
    )
    assert.deepEqual(Array.from(labelResult.data, doubleBits), [
      '0000000000000000',
      '4000000000000000',
      '3ff0000000000000'
    ])
    labelNativePlan.close()
    labelAnalyzer.close()
    labelRecipe.close()

    const onehotSchema = [{ id: 'x0', dtype: 'float32' }]
    const onehotAnalyze = float32FromBits([
      0x80000001, 0x80000000, 0x00000000, 0x00000001, 0x00000001
    ])
    const onehotChunks = [
      { rows: 2, columns: [onehotAnalyze.slice(0, 2)] },
      { rows: 3, columns: [onehotAnalyze.slice(2)] }
    ]
    const onehotPlanBytes = await client.analyzeTransform(
      vectors.recipes.categorical_mode_onehot_other,
      onehotSchema,
      onehotChunks
    )
    const onehotRecipe = native.TransformRecipe.fromJSON(
      vectors.recipes.categorical_mode_onehot_other
    )
    const onehotAnalyzer = onehotRecipe.analyzer(onehotSchema)
    for (const chunk of onehotChunks) onehotAnalyzer.push(chunk)
    const onehotNativePlan = onehotAnalyzer.finalize()
    assert.deepEqual(
      Buffer.from(onehotPlanBytes),
      onehotNativePlan.toBytes(),
      'worker and native categorical-onehot TFTR bytes'
    )
    assert.deepEqual(
      JSON.parse(onehotNativePlan.schemaJSON('output').toString()),
      [
        {
          category: { t: 'f32', v: '80000001' },
          dtype: 'float64',
          id: 'x0%3Aonehot%3A0',
          name: 'x0%3Aonehot%3A0',
          role: 'onehot',
          sourceId: 'x0'
        },
        {
          category: { t: 'f32', v: '00000000' },
          dtype: 'float64',
          id: 'x0%3Aonehot%3A1',
          name: 'x0%3Aonehot%3A1',
          role: 'onehot',
          sourceId: 'x0'
        },
        {
          category: { t: 'f32', v: '00000001' },
          dtype: 'float64',
          id: 'x0%3Aonehot%3A2',
          name: 'x0%3Aonehot%3A2',
          role: 'onehot',
          sourceId: 'x0'
        },
        {
          category: { t: 'other' },
          dtype: 'float64',
          id: 'x0%3Aonehot%3A3',
          name: 'x0%3Aonehot%3A3',
          role: 'onehot',
          sourceId: 'x0'
        }
      ]
    )
    const onehotApply = float32FromBits([
      0x80000001, 0x80000000, 0x00000001, 0x3f800000, 0x7fc00000
    ])
    const onehotResult = await client.applyTransform(
      onehotPlanBytes,
      onehotSchema,
      { rows: 5, columns: [onehotApply] }
    )
    assert.equal(onehotResult.rows, 5)
    assert.equal(onehotResult.columns, 4)
    assert.deepEqual(Array.from(onehotResult.data, doubleBits), [
      '3ff0000000000000', '0000000000000000', '0000000000000000', '0000000000000000',
      '0000000000000000', '3ff0000000000000', '0000000000000000', '0000000000000000',
      '0000000000000000', '0000000000000000', '3ff0000000000000', '0000000000000000',
      '0000000000000000', '0000000000000000', '0000000000000000', '3ff0000000000000',
      '0000000000000000', '3ff0000000000000', '0000000000000000', '0000000000000000'
    ])
    onehotNativePlan.close()
    onehotAnalyzer.close()
    onehotRecipe.close()

    if (typeof SharedArrayBuffer !== 'undefined') {
      const controller = new AbortController()
      const rows = 8 * 1024 * 1024
      const values = new Float64Array(rows)
      values.fill(1)
      let ready = false
      await assert.rejects(
        client.analyzeTransform(
          vectors.recipes.numeric_none_none,
          schema,
          { rows, columns: [values] },
          {
            signal: controller.signal,
            onReady(phase) {
              if (phase === 'analyze' && !ready) {
                ready = true
                setTimeout(() => controller.abort(), 25)
              }
            }
          }
        ),
        (error) => error instanceof native.TranfiTransformError
          && error.code === 109
      )
      assert.equal(ready, true)

      const large = largeCancellationFixture()
      const largePlanBytes = buildNativePlan(large)

      const finalizeController = new AbortController()
      let finalizeReady = false
      await assert.rejects(
        client.analyzeTransform(
          large.recipe,
          large.schema,
          { rows: 1, columns: [new Float64Array([1])] },
          {
            limits: large.limits,
            signal: finalizeController.signal,
            onReady(phase) {
              if (phase === 'finalize' && !finalizeReady) {
                finalizeReady = true
                finalizeController.abort()
              }
            }
          }
        ),
        (error) => error instanceof native.TranfiTransformError
          && error.code === 109
      )
      assert.equal(finalizeReady, true)

      const importController = new AbortController()
      let importReady = false
      await assert.rejects(
        client.applyTransform(
          largePlanBytes,
          large.schema,
          { rows: 1, columns: [new Float64Array([1])] },
          {
            limits: large.limits,
            signal: importController.signal,
            onReady(phase) {
              if (phase === 'import' && !importReady) {
                importReady = true
                importController.abort()
              }
            }
          }
        ),
        (error) => error instanceof native.TranfiTransformError
          && error.code === 109
      )
      assert.equal(importReady, true)

      const applyController = new AbortController()
      const applyRows = 4 * 1024 * 1024
      let applyReady = false
      await assert.rejects(
        client.applyTransform(
          largePlanBytes,
          large.schema,
          { rows: applyRows, columns: [new Float64Array(applyRows)] },
          {
            limits: large.limits,
            signal: applyController.signal,
            onReady(phase) {
              if (phase === 'apply' && !applyReady) {
                applyReady = true
                applyController.abort()
              }
            }
          }
        ),
        (error) => error instanceof native.TranfiTransformError
          && error.code === 109
      )
      assert.equal(applyReady, true)
    }
  } finally {
    await client.dispose()
  }

  const fallbackFactory = () => new Worker(workerPath)
  const fallbackClient = createWorkerClient(fallbackFactory(), {
    workerFactory: fallbackFactory,
    sharedCancellation: false,
    terminateOnDispose: true
  })
  try {
    const controller = new AbortController()
    const rows = 2 * 1024 * 1024
    let ready = false
    await assert.rejects(
      fallbackClient.analyzeTransform(
        vectors.recipes.numeric_none_none,
        schema,
        { rows, columns: [new Float64Array(rows)] },
        {
          signal: controller.signal,
          onReady(phase) {
            if (phase === 'analyze' && !ready) {
              ready = true
              controller.abort()
            }
          }
        }
      ),
      (error) => error instanceof native.TranfiTransformError
        && error.code === 109
    )
    assert.equal(ready, true)

    const racedController = new AbortController()
    const racedRequest = fallbackClient.analyzeTransform(
      vectors.recipes.numeric_none_none,
      schema,
      { rows: 1, columns: [new Float64Array([1])] },
      { signal: racedController.signal }
    )
    racedController.abort()
    await assert.rejects(
      racedRequest,
      (error) => error instanceof native.TranfiTransformError
        && error.code === 109
    )

    const replacementPlan = await fallbackClient.analyzeTransform(
      vectors.recipes.numeric_none_none,
      schema,
      { rows: 1, columns: [new Float64Array([2])] }
    )
    const nativeReplacementPlan = native.TransformPlan.fromBytes(replacementPlan)
    nativeReplacementPlan.close()
  } finally {
    await fallbackClient.dispose()
  }

  const noFactoryWorker = new Worker(workerPath)
  const noFactoryClient = createWorkerClient(noFactoryWorker, {
    sharedCancellation: false,
    terminateOnDispose: true
  })
  try {
    await assert.rejects(
      noFactoryClient.analyzeTransform(
        vectors.recipes.numeric_none_none,
        schema,
        { rows: 1, columns: [new Float64Array([1])] },
        { signal: new AbortController().signal }
      ),
      /worker URL or workerFactory/
    )
  } finally {
    await noFactoryClient.dispose()
  }
  console.log('prepared-transform worker tests passed')
}

main().catch((error) => {
  console.error(error)
  process.exitCode = 1
})
