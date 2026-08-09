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
