'use strict'

const assert = require('node:assert/strict')
const vectors = require('./vectors/prepared_transform_v1.json')

function recipe() {
  const value = structuredClone(vectors.recipes.categorical_mode_label_sentinel)
  value.columns[0].categorical.encode.categories = [0, 2, 5].map(number => {
    const bytes = Buffer.alloc(8)
    bytes.writeDoubleBE(number)
    return { t: 'f64', v: bytes.toString('hex') }
  })
  return value
}

const schema = [{ id: 'x0', dtype: 'float64' }]
const table = values => ({ rows: values.length, columns: [Float64Array.from(values)] })

function check(backend) {
  const spec = recipe()
  const parsed = backend.TransformRecipe.fromJSON(spec)
  const analyzer = parsed.analyzer(schema)
  let plan, restored, apply
  try {
    analyzer.push(table([2, 2, 99, 5]))
    plan = analyzer.finalize()
    const bytes = Buffer.from(plan.toBytes())
    restored = backend.TransformPlan.fromBytes(bytes)
    assert.deepEqual(Buffer.from(restored.toBytes()), bytes)
    apply = restored.apply(schema)
    assert.deepEqual(Array.from(apply.run(table([0, 2, 5, 99, NaN])).data), [0, 1, 2, -1, 1])
    assert.throws(() => backend.TransformRecipe.fromJSON(spec, {
      limits: { maxCategoriesPerColumn: 2 }
    }), error => error.code === 104)
    const invalid = recipe()
    invalid.columns[0].categorical.encode.categories.reverse()
    assert.throws(() => backend.TransformRecipe.fromJSON(invalid), error => error.code === 101)
    return bytes
  } finally {
    for (const handle of [apply, restored, plan, analyzer, parsed]) if (handle) handle.close()
  }
}

module.exports = { check, recipe, schema, table }
