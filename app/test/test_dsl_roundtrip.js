/**
 * Tests for DSL ↔ blocks bidirectional sync.
 * Run: node app/test/test_dsl_roundtrip.js
 */
import { parseDsl } from '../src/lib/parse-dsl.js'
import { compileToDsl } from '../src/lib/compile-dsl.js'
import { createBlock } from '../src/lib/block-factory.js'

let passed = 0
let failed = 0

function assert(cond, msg) {
  if (!cond) {
    console.error(`  FAIL: ${msg}`)
    failed++
  } else {
    passed++
  }
}

function assertEq(actual, expected, msg) {
  if (actual !== expected) {
    console.error(`  FAIL: ${msg}\n    expected: ${JSON.stringify(expected)}\n    actual:   ${JSON.stringify(actual)}`)
    failed++
  } else {
    passed++
  }
}

// --- parseDsl produces correct block types ---

function test_parse_basic_pipeline() {
  console.log('test_parse_basic_pipeline')
  const blocks = parseDsl('csv | filter "age > 30" | csv')
  assert(blocks !== null, 'should parse')
  assertEq(blocks.length, 3, '3 blocks')
  assertEq(blocks[0].typeCode, 'csv-decode', 'first is csv-decode')
  assertEq(blocks[1].typeCode, 'filter', 'middle is filter')
  assertEq(blocks[1].args.expr, 'age > 30', 'filter expr')
  assertEq(blocks[2].typeCode, 'csv-encode', 'last is csv-encode')
}

function test_parse_head_skip() {
  console.log('test_parse_head_skip')
  const blocks = parseDsl('csv | head 5 | skip 2 | csv')
  assertEq(blocks[1].typeCode, 'head', 'head')
  assertEq(blocks[1].args.n, 5, 'head n=5')
  assertEq(blocks[2].typeCode, 'skip', 'skip')
  assertEq(blocks[2].args.n, 2, 'skip n=2')
}

function test_parse_sort_select() {
  console.log('test_parse_sort_select')
  const blocks = parseDsl('csv | sort age | select name,age | csv')
  assertEq(blocks[1].typeCode, 'sort', 'sort')
  assertEq(blocks[1].args.columns, 'age', 'sort columns')
  assertEq(blocks[2].typeCode, 'select', 'select')
  assertEq(blocks[2].args.columns, 'name,age', 'select columns')
}

function test_parse_ewma() {
  console.log('test_parse_ewma')
  const blocks = parseDsl('csv | ewma price 0.3 | csv')
  assertEq(blocks[1].typeCode, 'ewma', 'ewma')
  assertEq(blocks[1].args.column, 'price', 'column')
  assertEq(blocks[1].args.alpha, 0.3, 'alpha')
}

function test_parse_diff_order2() {
  console.log('test_parse_diff_order2')
  const blocks = parseDsl('csv | diff price 2 | csv')
  assertEq(blocks[1].typeCode, 'diff', 'diff')
  assertEq(blocks[1].args.column, 'price', 'column')
  assertEq(blocks[1].args.order, 2, 'order=2')
}

function test_parse_anomaly() {
  console.log('test_parse_anomaly')
  const blocks = parseDsl('csv | anomaly price 2.5 | csv')
  assertEq(blocks[1].typeCode, 'anomaly', 'anomaly')
  assertEq(blocks[1].args.column, 'price', 'column')
  assertEq(blocks[1].args.threshold, 2.5, 'threshold')
}

function test_parse_onehot_drop() {
  console.log('test_parse_onehot_drop')
  const blocks = parseDsl('csv | onehot city --drop | csv')
  assertEq(blocks[1].typeCode, 'onehot', 'onehot')
  assertEq(blocks[1].args.column, 'city', 'column')
  assertEq(blocks[1].args.drop, true, 'drop=true')
}

function test_parse_split_data() {
  console.log('test_parse_split_data')
  const blocks = parseDsl('csv | split-data 0.7 --seed 123 | csv')
  assertEq(blocks[1].typeCode, 'split-data', 'split-data')
  assertEq(blocks[1].args.ratio, 0.7, 'ratio')
  assertEq(blocks[1].args.seed, 123, 'seed')
}

function test_parse_normalize() {
  console.log('test_parse_normalize')
  const blocks = parseDsl('csv | normalize price,score zscore | csv')
  assertEq(blocks[1].typeCode, 'normalize', 'normalize')
  assertEq(blocks[1].args.columns, 'price,score', 'columns')
  assertEq(blocks[1].args.method, 'zscore', 'method')
}

function test_parse_acf() {
  console.log('test_parse_acf')
  const blocks = parseDsl('csv | acf price 30 | csv')
  assertEq(blocks[1].typeCode, 'acf', 'acf')
  assertEq(blocks[1].args.column, 'price', 'column')
  assertEq(blocks[1].args.lags, 30, 'lags')
}

function test_parse_interpolate() {
  console.log('test_parse_interpolate')
  const blocks = parseDsl('csv | interpolate price forward | csv')
  assertEq(blocks[1].typeCode, 'interpolate', 'interpolate')
  assertEq(blocks[1].args.column, 'price', 'column')
  assertEq(blocks[1].args.method, 'forward', 'method')
}

function test_parse_label_encode() {
  console.log('test_parse_label_encode')
  const blocks = parseDsl('csv | label-encode city city_id | csv')
  assertEq(blocks[1].typeCode, 'label-encode', 'label-encode')
  assertEq(blocks[1].args.column, 'city', 'column')
  assertEq(blocks[1].args.result, 'city_id', 'result')
}

// --- Roundtrip: compile → parse → compile produces same DSL ---

function test_roundtrip(dsl, label) {
  console.log(`test_roundtrip: ${label}`)
  const blocks = parseDsl(dsl)
  assert(blocks !== null, `${label}: should parse`)
  const compiled = compileToDsl(blocks)
  assertEq(compiled, dsl, `${label}: roundtrip`)
}

function test_roundtrips() {
  test_roundtrip('csv | head 5 | csv', 'head')
  test_roundtrip('csv | skip 20 | csv', 'skip')
  test_roundtrip('csv | tail 10 | csv', 'tail')
  test_roundtrip('csv | sample 100 | csv', 'sample')
  test_roundtrip('csv | filter "age > 30" | csv', 'filter')
  test_roundtrip('csv | sort name | csv', 'sort')
  test_roundtrip('csv | select name,age | csv', 'select')
  test_roundtrip('csv | ewma price 0.3 | csv', 'ewma')
  test_roundtrip('csv | diff price | csv', 'diff')
  test_roundtrip('csv | diff price 2 | csv', 'diff order 2')
  test_roundtrip('csv | anomaly price | csv', 'anomaly default')
  test_roundtrip('csv | anomaly price 2.5 | csv', 'anomaly threshold')
  test_roundtrip('csv | onehot city | csv', 'onehot')
  test_roundtrip('csv | onehot city --drop | csv', 'onehot drop')
  test_roundtrip('csv | label-encode city | csv', 'label-encode')
  test_roundtrip('csv | label-encode city city_id | csv', 'label-encode result')
  test_roundtrip('csv | split-data 0.8 | csv', 'split-data')
  test_roundtrip('csv | split-data 0.7 --seed 123 | csv', 'split-data seed')
  test_roundtrip('csv | interpolate price | csv', 'interpolate default')
  test_roundtrip('csv | interpolate price forward | csv', 'interpolate forward')
  test_roundtrip('csv | normalize price,score | csv', 'normalize default')
  test_roundtrip('csv | normalize price,score zscore | csv', 'normalize zscore')
  test_roundtrip('csv | acf price | csv', 'acf default')
  test_roundtrip('csv | acf price 30 | csv', 'acf lags')
  test_roundtrip('csv | dedup name | csv', 'dedup')
  test_roundtrip('csv | trim name | csv', 'trim')
  test_roundtrip('csv | clip score 0 100 | csv', 'clip')
  test_roundtrip('csv | step price running-sum | csv', 'step')
}

// --- Compile from blocks (factory defaults with args set) ---

function test_compile_from_blocks() {
  console.log('test_compile_from_blocks')
  const b1 = createBlock('csv-decode')
  const b2 = createBlock('ewma')
  b2.args.column = 'price'
  b2.args.alpha = 0.5
  const b3 = createBlock('csv-encode')
  const dsl = compileToDsl([b1, b2, b3])
  assertEq(dsl, 'csv | ewma price 0.5 | csv', 'compile from blocks')
}

function test_complex_pipeline_roundtrip() {
  console.log('test_complex_pipeline_roundtrip')
  const dsl = 'csv | filter "status == active" | ewma score 0.2 | diff price 2 | anomaly price 2.5 | head 100 | csv'
  const blocks = parseDsl(dsl)
  assert(blocks !== null, 'parse complex')
  assertEq(blocks.length, 7, '7 blocks')
  const compiled = compileToDsl(blocks)
  assertEq(compiled, dsl, 'complex roundtrip')
}

function test_empty_and_null() {
  console.log('test_empty_and_null')
  assert(parseDsl('') === null, 'empty string')
  assert(parseDsl('   ') === null, 'whitespace')
  assert(parseDsl(null) === null, 'null')
}

// --- Run all ---

test_parse_basic_pipeline()
test_parse_head_skip()
test_parse_sort_select()
test_parse_ewma()
test_parse_diff_order2()
test_parse_anomaly()
test_parse_onehot_drop()
test_parse_split_data()
test_parse_normalize()
test_parse_acf()
test_parse_interpolate()
test_parse_label_encode()
test_roundtrips()
test_compile_from_blocks()
test_complex_pipeline_roundtrip()
test_empty_and_null()

console.log(`\n==================`)
console.log(`${passed}/${passed + failed} tests passed`)
if (failed > 0) process.exit(1)
