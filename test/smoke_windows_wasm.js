'use strict'

const assert = require('node:assert/strict')
const { createRequire } = require('node:module')
const { resolve } = require('node:path')

const consumerRequire = createRequire(resolve(process.cwd(), 'package.json'))

async function main() {
  const createTranfi = consumerRequire('tranfi/wasm')
  const tf = await createTranfi()
  assert.equal(tf.version(), '0.2.1')

  const streamed = tf.run(
    'csv | filter "col(age) > 25" | csv',
    'name,age\nA,20\nB,30\n'
  )
  assert.match(streamed.outputText, /B,30/)
  assert.doesNotMatch(streamed.outputText, /A,20/)

  const recipeSpec = {
    format: 'tranfi.transform-recipe',
    version: 1,
    policyVersion: 1,
    outputDtype: 'float64',
    semanticLimits: {
      maxOutputColumns: 65536,
      maxOutputElementsPerApply: 134217728
    },
    columns: [{
      sourceId: 'x0',
      kind: {
        op: 'declared', value: 'numeric', rule: null, maxCategories: null
      },
      numeric: {
        impute: { op: 'mean', constant: null, allMissing: 'zero' },
        normalize: { op: 'standard', ddof: 0 }
      },
      categorical: null
    }]
  }
  const schema = [{ id: 'x0', dtype: 'float64' }]
  const recipe = tf.TransformRecipe.fromJSON(recipeSpec)
  const analyzer = recipe.analyzer(schema)
  let plan
  let apply
  try {
    analyzer.push({ rows: 3, columns: [new Float64Array([1, NaN, 3])] })
    plan = analyzer.finalize()
    apply = plan.apply(schema)
    const transformed = apply.run({
      rows: 3,
      columns: [new Float64Array([1, NaN, 3])]
    })
    assert.equal(transformed.rows, 3)
    assert.equal(transformed.columns, 1)
    assert.ok(transformed.data.every(Number.isFinite))
    assert.ok(Math.abs(transformed.data[1]) < 1e-12)
  } finally {
    if (apply) apply.close()
    if (plan) plan.close()
    analyzer.close()
    recipe.close()
  }

  console.log('Packed WASM smoke passed')
}

main().catch(error => {
  console.error(error)
  process.exitCode = 1
})
