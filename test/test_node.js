/**
 * test_node.js — Integration tests for the Node.js tranfi package.
 *
 * Run: node test/test_node.js
 */

const { pipeline, codec, ops, expr, compileDsl, compileToSql, loadRecipe, saveRecipe, recipes } = require('../js/src/index.js')
const { join } = require('path')
const { tmpdir } = require('os')
const { writeFile, unlink, mkdtemp, readdir, rm } = require('fs/promises')
const { readFileSync } = require('fs')
const vm = require('vm')
const { Readable, Writable } = require('stream')
const { gzipSync } = require('zlib')

const fixturesDir = join(__dirname, 'fixtures')

let tests = 0
let passed = 0

async function test(name, fn) {
  tests++
  process.stdout.write(`  ${name.padEnd(50)}`)
  try {
    await fn()
    passed++
    console.log('PASS')
  } catch (e) {
    console.log('FAIL')
    console.error(`    ${e.message}`)
  }
}

function assert(cond, msg) {
  if (!cond) throw new Error(msg || 'assertion failed')
}

async function assertRejects(fn, pattern) {
  try {
    await fn()
  } catch (e) {
    if (pattern && !pattern.test(String(e.message || e))) {
      throw new Error(`expected error matching ${pattern}, got: ${e.message || e}`)
    }
    return
  }
  throw new Error('expected promise to reject')
}

function assertThrows(fn, pattern) {
  try {
    fn()
  } catch (e) {
    if (pattern && !pattern.test(String(e.message || e))) {
      throw new Error(`expected error matching ${pattern}, got: ${e.message || e}`)
    }
    return
  }
  throw new Error('expected function to throw')
}

// ---- Tests ----

async function main() {

console.log('Tranfi Node.js Tests')
console.log('====================\n')

console.log('CSV:')

await test('csv passthrough', async () => {
  const p = pipeline([
    codec.csv(),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'name,age\nAlice,30\nBob,25\n' })
  const text = result.outputText
  assert(text.includes('name,age'), 'should have header')
  assert(text.includes('Alice'), 'should have Alice')
  assert(text.includes('Bob'), 'should have Bob')
})

await test('csv filter', async () => {
  const p = pipeline([
    codec.csv(),
    ops.filter(expr("col('age') > 27")),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'name,age,score\nAlice,30,85\nBob,25,92\nCharlie,35,78\n' })
  const text = result.outputText
  assert(text.includes('Alice'), 'Alice age 30 > 27')
  assert(text.includes('Charlie'), 'Charlie age 35 > 27')
  assert(!text.includes('Bob'), 'Bob age 25 not > 27')
})

await test('csv null literals', async () => {
  const p = pipeline([
    codec.csv({ batchSize: 2, nulls: ['NA', 'NULL'] }),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'name,score,note\nA,10,ok\nB,NA,bad\nC,NULL,NULL\nD,5,NA\n' })
  const text = result.outputText
  assert(text.includes('A,10,ok'), 'should keep ordinary values')
  assert(text.includes('B,,bad'), 'NA score should become null')
  assert(text.includes('C,,'), 'NULL score and note should become null')
  assert(text.includes('D,5,'), 'NA note should become null')
  assert(!text.includes('NA'), 'null literal should not survive output')
  assert(!text.includes('NULL'), 'null literal should not survive output')
})

await test('csv quoted null literals disabled', async () => {
  const p = pipeline([
    codec.csv({ batchSize: 2, nulls: ['NA'], quotedNulls: false }),
    ops.fillNull({ note: 'MISSING' }),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'name,note\nA,"NA"\nB,NA\nC,""\nD,\n' })
  const text = result.outputText
  assert(text.includes('A,NA'), 'quoted NA should stay a string')
  assert(text.includes('B,MISSING'), 'unquoted NA should become null')
  assert(text.includes('C,'), 'quoted empty string should stay non-null')
  assert(!text.includes('C,MISSING'), 'quoted empty string should not be filled')
  assert(text.includes('D,MISSING'), 'unquoted empty field should remain null')
})

await test('csv comments empty rows and trim control', async () => {
  const step = codec.csv({ comment: '#', skipEmptyRows: true, trimWs: false, skip: 2, nMax: 3 })
  assert(step.args.comment === '#', 'helper should expose comment')
  assert(step.args.skip_empty_rows === true, 'helper should expose skip_empty_rows')
  assert(step.args.trim_ws === false, 'helper should expose trim_ws=false')
  assert(step.args.skip === 2, 'helper should expose skip')
  assert(step.args.n_max === 3, 'helper should expose n_max')

  const p = pipeline([
    codec.csv({ batchSize: 1, comment: '#', skipEmptyRows: true }),
    codec.csvEncode(),
  ])
  const input = '# generated\nname,score,note\n Alice , 10 , ok # trailing\n\n"Bob # literal",20," keep # inside "\n'
  const result = await p.run({ input, chunkSize: 7 })
  const text = result.outputText
  assert(text.includes('Alice,10,ok'), 'should trim unquoted fields and remove inline comment')
  assert(text.includes('Bob # literal,20, keep # inside '), 'should preserve comment marker inside quoted fields')
  assert(!text.includes('generated'), 'should skip whole-line comment before header')
  assert(!text.includes('trailing'), 'should remove inline comment')

  const noTrim = await pipeline([codec.csv({ trimWs: false }), codec.csvEncode()])
    .run({ input: 'name,score\n Alice , 10 \n' })
  assert(noTrim.outputText.includes(' Alice , 10 '), 'trimWs=false should preserve spaces')

  const skipped = await pipeline([codec.csv({ skip: 2, comment: '#' }), codec.csvEncode()])
    .run({ input: 'generated by system\nexported today\n# ignored after skip\nname,score\nAlice,10\nBob,20\n', chunkSize: 11 })
  assert(skipped.outputText.includes('name,score'), 'skip should leave real header')
  assert(skipped.outputText.includes('Alice,10'), 'skip should keep first data row')
  assert(skipped.outputText.includes('Bob,20'), 'skip should keep second data row')
  assert(!skipped.outputText.includes('generated by system'), 'skip should remove first preamble row')
  assert(!skipped.outputText.includes('exported today'), 'skip should remove second preamble row')
  assert(!skipped.outputText.includes('ignored after skip'), 'comment should apply after skip')

  const limited = await pipeline([codec.csv({ nMax: 2, batchSize: 1 }), codec.csvEncode()])
    .run({ input: 'name,score\nAlice,10\nBob,20\nCara,30\n', chunkSize: 9 })
  assert(limited.outputText.includes('Alice,10'), 'nMax should keep first row')
  assert(limited.outputText.includes('Bob,20'), 'nMax should keep second row')
  assert(!limited.outputText.includes('Cara,30'), 'nMax should stop after requested rows')

  const headerOnly = await pipeline([codec.csv({ maxRows: 0 }), codec.csvEncode()])
    .run({ input: 'name,score\nAlice,10\n' })
  assert(headerOnly.outputText === 'name,score\n', 'maxRows=0 should preserve header schema')
})


await test('csv header false', async () => {
  const result = await pipeline([
    codec.csv({ header: false, batchSize: 1 }),
    codec.csvEncode(),
  ]).run({ input: 'Alice,30\nBob,25\n', chunkSize: 2 })
  assert(result.outputText === 'col1,col2\nAlice,30\nBob,25\n', 'header=false should synthesize column names and keep first row')

  const selected = await pipeline([
    codec.csv({ header: false, batchSize: 1 }),
    ops.select(['col2']),
    codec.csvEncode(),
  ]).run({ input: 'Alice,30\nBob,25\n', chunkSize: 3 })
  assert(selected.outputText === 'col2\n30\n25\n', 'header=false synthetic columns should be addressable')

  const headerOnly = await pipeline([
    codec.csv({ header: false, maxRows: 0 }),
    codec.csvEncode(),
  ]).run({ input: 'Alice,30\nBob,25\n', chunkSize: 4 })
  assert(headerOnly.outputText === 'col1,col2\n', 'header=false maxRows=0 should emit synthetic schema only')
})


await test('fillNull audit side channel', async () => {
  const p = pipeline([
    codec.csv({ batchSize: 2, nulls: ['NA'] }),
    ops.fillNull({ note: 'MISSING' }, { audit: true, auditLimit: 1 }),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'name,note\nA,ok\nB,NA\nC,\n' })
  assert(result.outputText.includes('B,MISSING'), 'first null should be filled')
  assert(result.outputText.includes('C,MISSING'), 'second null should be filled')
  assert(result.statsText.includes('"op":"fill-null"'), 'fill-null audit should identify op')
  assert(result.statsText.includes('"event":"null_filled"'), 'fill-null audit should identify event')
  assert(result.statsText.includes('"row":2'), 'fill-null audit should include first changed row')
  assert(result.statsText.includes('"before":null'), 'fill-null audit should include null before value')
  assert(result.statsText.includes('"after":"MISSING"'), 'fill-null audit should include filled value')
  assert(!result.statsText.includes('"row":3'), 'fill-null audit should enforce auditLimit')
})


await test('cast audit side channel', async () => {
  const p = pipeline([
    codec.csv({ batchSize: 2 }),
    ops.cast({ age: 'int' }, { audit: true, auditLimit: 3 }),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'age\n10\nbad\n12x\n' })
  assert(result.outputText.includes('10'), 'cast should keep valid numeric value')
  assert(result.outputText.includes('0'), 'cast should preserve legacy invalid integer coercion')
  assert(result.outputText.includes('12'), 'cast should preserve legacy partial integer coercion')
  assert(result.statsText.includes('"op":"cast"'), 'cast audit should identify op')
  assert(result.statsText.includes('"event":"value_changed"'), 'cast audit should record successful changes')
  assert(result.statsText.includes('"event":"coercion_failed"'), 'cast audit should record failed coercions')
  assert(result.statsText.includes('"reason":"invalid_integer"'), 'cast audit should record invalid integer')
  assert(result.statsText.includes('"reason":"trailing_characters"'), 'cast audit should record trailing characters')
  assert(result.statsText.includes('"expected":"int"'), 'cast audit should include expected type')
  assert(result.statsText.includes('"on_error":"coerce"'), 'cast audit should include default policy')
  assert(result.statsText.includes('"actual":"bad"'), 'cast audit should include actual value')
  assert(result.statsText.includes('"coercion_failures":2'), 'cast stats should count coercion failures')
  assert(result.statsText.includes('"coercion_nulled":0'), 'cast stats should count nulled coercions')
  assert(result.statsText.includes('"audit_emitted":3'), 'cast stats should count emitted audit records')
})


await test('cast onError policies', async () => {
  const nullStep = ops.cast({ age: 'int' }, { onError: 'null', audit: true, auditLimit: 3 })
  assert(nullStep.args.on_error === 'null', 'cast helper should expose onError')
  const result = await pipeline([
    codec.csv({ batchSize: 2 }),
    nullStep,
    codec.csvEncode(),
  ]).run({ input: 'age\n10\nbad\n12x\n' })
  const lines = result.outputText.split(/\r?\n/)
  assert(lines[0] === 'age', 'cast null policy should keep header')
  assert(lines[1] === '10', 'cast null policy should keep valid value')
  assert(lines[2] === '', 'cast null policy should null invalid integer')
  assert(lines[3] === '', 'cast null policy should null partial integer')
  assert(result.statsText.includes('"on_error":"null"'), 'cast audit should include nulling policy')
  assert(result.statsText.includes('"event":"coercion_failed"'), 'cast audit should record failures')
  assert(result.statsText.includes('"after":null'), 'cast audit should show null output')
  assert(result.statsText.includes('"coercion_failures":2'), 'cast stats should count failures')
  assert(result.statsText.includes('"coercion_nulled":2'), 'cast stats should count nulled values')

  await assertRejects(
    () => pipeline([
      codec.csv({ batchSize: 1 }),
      ops.cast({ age: 'int' }, { onError: 'fail' }),
      codec.csvEncode(),
    ]).run({ input: 'age\nbad\n' }),
    /cast failed at row 1/
  )
})


function makeWideCsv(nCols) {
  const header = Array.from({ length: nCols }, (_, i) => `col${i}`).join(',')
  const row = Array.from({ length: nCols }, (_, i) => `v${i}`).join(',')
  return `${header}\n${row}\n`
}

await test('csv wide columns and maxColumns', async () => {
  const step = codec.csv({ maxColumns: 3 })
  assert(step.args.max_columns === 3, 'helper should expose max_columns')

  const result = await pipeline([
    codec.csv({ batchSize: 1 }),
    codec.csvEncode(),
  ]).run({ input: makeWideCsv(300), chunkSize: 17 })
  assert(result.outputText.includes('col255'), 'should preserve column 255')
  assert(result.outputText.includes('col299'), 'should preserve column 299')
  assert(result.outputText.includes('v255'), 'should preserve value 255')
  assert(result.outputText.includes('v299'), 'should preserve value 299')

  await assertRejects(
    () => pipeline([codec.csv({ maxColumns: 3 }), codec.csvEncode()]).run({ input: 'a,b,c,d\n1,2,3,4\n', chunkSize: 2 }),
    /csv record exceeds max_columns/
  )

  const longName = 'a'.repeat(4097)
  await assertRejects(
    () => pipeline([codec.csv(), codec.csvEncode()]).run({ input: `${longName}\n1\n`, chunkSize: 11 }),
    /column name/
  )
})

await test('csv repair diagnostics', async () => {
  const p = pipeline([
    codec.csv({ batchSize: 2, repair: true, maxErrorBytes: 5, audit: true, auditLimit: 1 }),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'a,b,c\n1,2\n3,4,5,6\n' })
  const text = result.outputText
  assert(text.includes('1,2,'), 'short row should be padded')
  assert(text.includes('3,4,5'), 'long row should be truncated')
  assert(!text.includes('3,4,5,6'), 'extra field should not survive repair')
  const errors = result.errors.toString('utf-8')
  assert(errors.includes('csv_field_count'), 'repair should emit field-count diagnostics')
  assert(errors.includes('"mode":"repair"'), 'diagnostic should include repair mode')
  assert(errors.includes('"action":"repair"'), 'diagnostic should include repair action')
  assert(errors.includes('"line":2'), 'diagnostic should include line number')
  assert(errors.includes('"byte_offset":6'), 'diagnostic should include byte offset')
  assert(errors.includes('"raw":"3,4,5"'), 'diagnostic raw preview should be bounded')
  assert(errors.includes('"truncated":true'), 'diagnostic should mark truncated raw preview')
  assert(result.statsText.includes('"type":"audit"'), 'repair audit should emit audit record')
  assert(result.statsText.includes('"op":"codec.csv.decode"'), 'repair audit should identify codec')
  assert(result.statsText.includes('"event":"row_repaired"'), 'repair audit should identify repaired-row event')
  assert(result.statsText.includes('"reason":"csv_field_count"'), 'repair audit should identify reason')
  assert(result.statsText.includes('"line":2'), 'repair audit should include first repaired line')
  assert(result.statsText.includes('"raw":"1,2"'), 'repair audit should include bounded raw row')
  assert(!result.statsText.includes('"line":3'), 'repair audit should enforce auditLimit')
})

await test('csv max record bytes', async () => {
  const step = codec.csv({ maxRecordBytes: 8 })
  assert(step.args.max_record_bytes === 8, 'helper should expose max_record_bytes')

  const p = pipeline([
    codec.csv({ maxRecordBytes: 8 }),
    codec.csvEncode(),
  ])
  try {
    await p.run({ input: 'a,b\n123456789' })
    assert(false, 'oversized CSV record should fail')
  } catch (err) {
    assert(String(err.message || err).includes('csv record exceeds max_record_bytes'), 'error should mention max_record_bytes')
  }
})

await test('csv quoted record boundaries', async () => {
  const p = pipeline([
    codec.csv({ batchSize: 1 }),
    codec.csvEncode(),
  ])
  const quoted = await p.run({ input: 'id,note\n1,"hello ""world\nline"\n2,ok\n' })
  assert(quoted.outputText.includes('1,"hello ""world\nline"'), 'quoted newline should remain one row')
  assert(quoted.outputText.includes('2,ok'), 'row after quoted newline should survive')

  const unquoted = await p.run({ input: 'a,b\nx"y,1\nz,2\n' })
  assert(unquoted.outputText.includes('"x""y",1'), 'unquoted quote should be ordinary field data')
  assert(unquoted.outputText.includes('z,2'), 'unquoted quote should not merge next record')
})

await test('csv select', async () => {
  const p = pipeline([
    codec.csv(),
    ops.select(['name', 'score']),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'name,age,score\nAlice,30,85\nBob,25,92\n' })
  const text = result.outputText
  assert(text.includes('name,score'), 'should have selected columns')
  assert(!text.includes('age'), 'should not have age column')
})



await test('csv select helpers', async () => {
  const data = 'id,score_math,score_read,name,active\n1,90,80,Alice,true\n2,70,95,Bob,false\n'

  const pPrefix = pipeline([
    codec.csv(),
    ops.select(['starts_with(score_)']),
    codec.csvEncode(),
  ])
  assert((await pPrefix.run({ input: data })).outputText.startsWith('score_math,score_read\n'), 'starts_with should expand score columns')

  const pNegative = pipeline([
    codec.csv(),
    ops.select(['!score_read']),
    codec.csvEncode(),
  ])
  assert((await pNegative.run({ input: data })).outputText.startsWith('id,score_math,name,active\n'), 'negative selector should exclude score_read')

  const pWhere = pipeline([
    codec.csv(),
    ops.select(['where(numeric)']),
    codec.csvEncode(),
  ])
  assert((await pWhere.run({ input: data })).outputText.startsWith('id,score_math,score_read\n'), 'where(numeric) should select numeric columns')

  const pMatches = pipeline([
    codec.csv(),
    ops.select(['matches(^score_)']),
    codec.csvEncode(),
  ])
  assert((await pMatches.run({ input: data })).outputText.startsWith('score_math,score_read\n'), 'matches should select regex columns')

  const pAllOf = pipeline([
    codec.csv(),
    ops.select(['all_of(score_read,name)']),
    codec.csvEncode(),
  ])
  assert((await pAllOf.run({ input: data })).outputText.startsWith('score_read,name\n'), 'all_of should select requested columns in helper order')

  const pAnyOf = pipeline([
    codec.csv(),
    ops.select(["any_of('missing',score_math)"]),
    codec.csvEncode(),
  ])
  assert((await pAnyOf.run({ input: data })).outputText.startsWith('score_math\n'), 'any_of should ignore missing columns')

  const pNegativeAnyOf = pipeline([
    codec.csv(),
    ops.select(['!any_of(score_read,missing)']),
    codec.csvEncode(),
  ])
  assert((await pNegativeAnyOf.run({ input: data })).outputText.startsWith('id,score_math,name,active\n'), 'negative any_of should ignore missing columns while removing existing ones')

  const pMissingAllOf = pipeline([
    codec.csv(),
    ops.select(['all_of(score_read,missing)']),
    codec.csvEncode(),
  ])
  await assertRejects(() => pMissingAllOf.run({ input: data }), /all_of column not found|processing error/)
  const pRange = pipeline([
    codec.csv(),
    ops.select(['id:score_read']),
    codec.csvEncode(),
  ])
  assert((await pRange.run({ input: data })).outputText.startsWith('id,score_math,score_read\n'), 'range selector should include consecutive columns')

  const pReverseRange = pipeline([
    codec.csv(),
    ops.select(['score_read:id']),
    codec.csvEncode(),
  ])
  assert((await pReverseRange.run({ input: data })).outputText.startsWith('score_read,score_math,id\n'), 'reverse range selector should preserve requested direction')

  const pNegativeRange = pipeline([
    codec.csv(),
    ops.select(['!id:score_read']),
    codec.csvEncode(),
  ])
  assert((await pNegativeRange.run({ input: data })).outputText.startsWith('name,active\n'), 'negative range selector should remove consecutive columns')

  const pReadd = pipeline([
    codec.csv(),
    ops.select(['id:score_read', '!score_math', 'score_math']),
    codec.csvEncode(),
  ])
  assert((await pReadd.run({ input: data })).outputText.startsWith('id,score_read,score_math\n'), 'removed columns should re-add at the later selector position')

  const pIntersection = pipeline([
    codec.csv(),
    ops.select(['starts_with(score_)&where(numeric)']),
    codec.csvEncode(),
  ])
  assert((await pIntersection.run({ input: data })).outputText.startsWith('score_math,score_read\n'), 'selector & should intersect selections')

  const pDifference = pipeline([
    codec.csv(),
    ops.select(['starts_with(score_)&!ends_with(read)']),
    codec.csvEncode(),
  ])
  assert((await pDifference.run({ input: data })).outputText.startsWith('score_math\n'), 'selector ! inside & should subtract selected columns')

  const pUnion = pipeline([
    codec.csv(),
    ops.select(['starts_with(score_read)|id']),
    codec.csvEncode(),
  ])
  assert((await pUnion.run({ input: data })).outputText.startsWith('score_read,id\n'), 'selector | should preserve left order then add right-only columns')

  const pComplementExpr = pipeline([
    codec.csv(),
    ops.select(['!(id:score_read)']),
    codec.csvEncode(),
  ])
  assert((await pComplementExpr.run({ input: data })).outputText.startsWith('name,active\n'), 'parenthesized selector complement should use schema order')

  const pMissingRange = pipeline([
    codec.csv(),
    ops.select(['id:missing']),
    codec.csvEncode(),
  ])
  await assertRejects(() => pMissingRange.run({ input: data }), /range endpoint not found|processing error/)

})

await test('csv relocate', async () => {
  const p = pipeline([
    codec.csv(),
    ops.relocate(['score'], { before: 'age' }),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'name,age,score\nAlice,30,85\nBob,25,92\n' })
  const text = result.outputText
  assert(text.includes('name,score,age'), 'should move score before age')
  assert(text.includes('Alice,85,30'), 'should preserve row values')
})


await test('csv relocate helpers', async () => {
  const p = pipeline([
    codec.csv(),
    ops.relocate(['starts_with(score_)'], { after: 'name' }),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'id,name,score_math,score_read,active\n1,Alice,90,80,true\n' })
  assert(result.outputText.startsWith('id,name,score_math,score_read,active\n'), 'relocate should expand selector helpers')
})

await test('csv rename', async () => {
  const p = pipeline([
    codec.csv(),
    ops.rename({ name: 'full_name' }),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'name,age\nAlice,30\n' })
  const text = result.outputText
  assert(text.includes('full_name'), 'should have renamed column')
})

await test('csv head', async () => {
  const p = pipeline([
    codec.csv(),
    ops.head(2),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'name,age\nAlice,30\nBob,25\nCharlie,35\nDiana,28\n' })
  const text = result.outputText
  assert(text.includes('Alice'), 'should have Alice')
  assert(text.includes('Bob'), 'should have Bob')
  assert(!text.includes('Charlie'), 'should not have Charlie')
})

await test('csv combined pipeline', async () => {
  const p = pipeline([
    codec.csv(),
    ops.filter(expr("col('age') > 25")),
    ops.select(['name', 'age']),
    ops.rename({ name: 'person' }),
    ops.head(2),
    codec.csvEncode(),
  ])
  const csv = 'name,age,score\nAlice,30,85\nBob,25,92\nCharlie,35,78\nDiana,28,95\nEve,42,88\n'
  const result = await p.run({ input: csv })
  const text = result.outputText
  assert(text.includes('person,age'), 'should have renamed header')
  assert(text.includes('Alice'), 'Alice passes filter')
  assert(!text.includes('Bob'), 'Bob does not pass filter')
})

console.log('\nJSONL:')

await test('jsonl passthrough', async () => {
  const p = pipeline([
    codec.jsonl(),
    codec.jsonlEncode(),
  ])
  const jsonl = '{"name":"Alice","age":30}\n{"name":"Bob","age":25}\n'
  const result = await p.run({ input: jsonl })
  const text = result.outputText
  assert(text.includes('Alice'), 'should have Alice')
  assert(text.includes('Bob'), 'should have Bob')
})

await test('jsonl malformed records', async () => {
  const step = codec.jsonlDecode({ onError: 'warn', maxErrorBytes: 12 })
  assert(step.args.on_error === 'warn', 'helper should set on_error')
  assert(step.args.max_error_bytes === 12, 'helper should set max_error_bytes')

  const p = pipeline([
    codec.jsonl({ onError: 'warn' }),
    codec.csvEncode(),
  ])
  const data = '{"name":"Alice","age":30}\nnot json\n[1,2]\n{"name":"Bob","age":25}\n'
  const result = await p.run({ input: data, chunkSize: 9 })
  const text = result.outputText
  assert(text.includes('Alice'), 'valid first row should pass')
  assert(text.includes('Bob'), 'valid last row should pass')
  assert(!text.includes('not json'), 'malformed row should not enter main output')
  const errors = result.errors.toString('utf-8')
  assert(errors.includes('jsonl_malformed'), 'should emit malformed JSONL side record')
  assert(errors.includes('"action":"warn"'), 'warn action should be recorded')
  assert(errors.includes('"severity":"warning"'), 'warn severity should be warning')
  assert(errors.includes('"line":2'), 'should report malformed line number')
  assert(errors.includes('"line":3'), 'should report non-object line number')
  assert(errors.includes('JSONL record is not an object'), 'should distinguish non-object JSON')

  const skipped = await pipeline([codec.jsonl(), codec.csvEncode()]).run({ input: data })
  assert(skipped.outputText.includes('Alice'), 'default skip keeps valid rows')
  assert(skipped.outputText.includes('Bob'), 'default skip keeps later valid rows')
  assert(skipped.errors.length === 0, 'default skip should not write errors')

  const quarantined = await pipeline([
    codec.jsonl({ onError: 'quarantine', maxErrorBytes: 8 }),
    codec.csvEncode(),
  ]).run({ input: '{"name":"Alice","age":30}\nnot-json-record-long\n' })
  const qErrors = quarantined.errors.toString('utf-8')
  assert(qErrors.includes('"action":"quarantine"'), 'quarantine action should be recorded')
  assert(qErrors.includes('"severity":"error"'), 'quarantine severity should be error')
  assert(qErrors.includes('"raw_bytes":20'), 'raw byte length should be recorded')
  assert(qErrors.includes('"raw":"not-json"'), 'raw preview should be truncated')
  assert(qErrors.includes('"truncated":true'), 'truncated flag should be present')
  assert(!qErrors.includes('record-long'), 'truncated preview should not include full record')

  await assertRejects(
    () => pipeline([codec.jsonl({ onError: 'fail' }), codec.csvEncode()])
      .run({ input: '{"name":"Alice","age":30}\nnot json\n' }),
    /jsonl decode failed at line 2/
  )
})

await test('jsonl max record bytes', async () => {
  const step = codec.jsonlDecode({ maxRecordBytes: 16, maxErrorBytes: 6 })
  assert(step.args.max_record_bytes === 16, 'helper should expose max_record_bytes')
  assert(step.args.max_error_bytes === 6, 'helper should expose max_error_bytes')

  const p = pipeline([
    codec.jsonl({ maxRecordBytes: 16, maxErrorBytes: 6 }),
    codec.csvEncode(),
  ])
  await assertRejects(
    () => p.run({ input: '{"id":1}\n{"name":"abcdefghijklmnop"}', chunkSize: 7 }),
    /jsonl record exceeds max_record_bytes/
  )

  const longName = 'a'.repeat(4097)
  await assertRejects(
    () => pipeline([codec.jsonl(), codec.csvEncode()]).run({ input: `{"${longName}":1}\n`, chunkSize: 13 }),
    /column name/
  )
})

await test('jsonl filter', async () => {
  const p = pipeline([
    codec.jsonl(),
    ops.filter(expr("col('age') >= 30")),
    codec.jsonlEncode(),
  ])
  const jsonl = '{"name":"Alice","age":30}\n{"name":"Bob","age":25}\n{"name":"Charlie","age":35}\n'
  const result = await p.run({ input: jsonl })
  const text = result.outputText
  assert(text.includes('Alice'), 'Alice age 30 >= 30')
  assert(text.includes('Charlie'), 'Charlie age 35 >= 30')
  assert(!text.includes('Bob'), 'Bob age 25 not >= 30')
})

await test('json-extract text and nested jsonl', async () => {
  const p = pipeline([
    codec.text({ batchSize: 1 }),
    ops.jsonExtract('/user/id', 'user_id', { type: 'int' }),
    ops.jsonExtract('$.user.name', 'name'),
    codec.csvEncode(),
  ])
  const data = '{"user":{"id":42,"name":"Ada"}}\n{"user":{"id":7,"name":"Ben"}}\n'
  const text = (await p.run({ input: data })).outputText
  assert(text.includes('_line,user_id,name'), 'should append extracted columns')
  assert(text.includes('42,Ada'), 'should extract pointer int')
  assert(text.includes('7,Ben'), 'should extract JSONPath string')

  const nested = pipeline([
    codec.jsonl(),
    ops.jsonExtract('/city', 'city', { column: 'payload' }),
    codec.csvEncode(),
  ])
  const nestedInput = '{"id":1,"payload":{"city":"NY","zip":10001}}\n{"id":2,"payload":{"city":"LA","zip":90001}}\n'
  const nestedText = (await nested.run({ input: nestedInput })).outputText
  assert(nestedText.includes('id,payload,city'), 'should preserve nested JSON text column')
  assert(nestedText.includes('NY'), 'should extract NY')
  assert(nestedText.includes('LA'), 'should extract LA')
  assert(nestedText.includes('payload'), 'should keep payload column')
  assert(nestedText.includes('zip'), 'should preserve nested object JSON content')
})

await test('json-filter text and nested jsonl', async () => {
  const p = pipeline([
    codec.text({ batchSize: 1 }),
    ops.jsonFilter('/user/age', { op: 'ge', value: '30', type: 'float' }),
    codec.csvEncode(),
  ])
  const data = '{"user":{"name":"Ada","age":42}}\n{"user":{"name":"Ben","age":7}}\n{"user":{"name":"Cara","age":30}}\n'
  const text = (await p.run({ input: data })).outputText
  assert(text.includes('Ada'), 'should keep Ada')
  assert(text.includes('Cara'), 'should keep Cara')
  assert(!text.includes('Ben'), 'should drop Ben')

  const nested = pipeline([
    codec.jsonl(),
    ops.jsonFilter('$.city', { op: 'eq', value: 'NY', column: 'payload' }),
    codec.csvEncode(),
  ])
  const nestedInput = '{"id":1,"payload":{"city":"NY","zip":10001}}\n{"id":2,"payload":{"city":"LA","zip":90001}}\n'
  const nestedText = (await nested.run({ input: nestedInput })).outputText
  assert(nestedText.includes('NY'), 'should keep NY')
  assert(!nestedText.includes('LA'), 'should drop LA')
})

await test('json-schema filter and annotate', async () => {
  const schema = {
    type: 'object',
    required: ['user'],
    properties: {
      user: {
        type: 'object',
        required: ['name', 'age'],
        properties: {
          name: { type: 'string', minLength: 1 },
          age: { type: 'integer', minimum: 18 },
        },
      },
    },
  }
  const p = pipeline([
    codec.text({ batchSize: 1 }),
    ops.jsonSchema(schema, { mode: 'filter', audit: true, auditLimit: 1 }),
    codec.csvEncode(),
  ])
  const data = '{"user":{"name":"Ada","age":42}}\n{"user":{"name":"Ben","age":7}}\n{"user":{"name":"Cara"}}\nnot json\n'
  const filtered = await p.run({ input: data })
  const text = filtered.outputText
  assert(text.includes('Ada'), 'should keep valid row')
  assert(!text.includes('Ben'), 'should drop invalid age')
  assert(!text.includes('Cara'), 'should drop missing required age')
  assert(!text.includes('not json'), 'should drop malformed JSON')
  assert(filtered.statsText.includes('"type":"audit"'), 'json-schema audit should emit audit record')
  assert(filtered.statsText.includes('"op":"json-schema"'), 'json-schema audit should identify op')
  assert(filtered.statsText.includes('"reason":"json_schema_failed"'), 'json-schema audit should identify failure reason')
  assert(filtered.statsText.includes('"row":2'), 'json-schema audit should include first dropped row index')
  assert(filtered.statsText.includes('schema_mismatch'), 'json-schema audit should include failure class')
  assert(filtered.statsText.includes('Ben'), 'json-schema audit should include first dropped row data')
  assert(!filtered.statsText.includes('Cara'), 'json-schema audit should enforce auditLimit')

  const nestedSchema = {
    type: 'object',
    required: ['city', 'zip'],
    properties: {
      city: { type: 'string', enum: ['NY', 'LA'] },
      zip: { type: 'integer', minimum: 10000 },
    },
  }
  const nested = pipeline([
    codec.jsonl(),
    ops.jsonSchema(nestedSchema, { column: 'payload', result: 'ok' }),
    codec.csvEncode(),
  ])
  const nestedInput = '{"id":1,"payload":{"city":"NY","zip":10001}}\n{"id":2,"payload":{"city":7,"zip":90001}}\n{"id":3,"payload":{"city":"SF","zip":94105}}\n'
  const nestedText = (await nested.run({ input: nestedInput })).outputText
  assert(nestedText.includes('id,payload,ok'), 'should append validation column')
  assert(nestedText.includes('NY'), 'should keep valid nested row')
  assert(nestedText.includes('true'), 'should mark valid rows true')
  assert(nestedText.includes('false'), 'should mark invalid rows false')
})


await test('json-flatten text and nested jsonl', async () => {
  const p = pipeline([
    codec.text({ batchSize: 1 }),
    ops.jsonFlatten([
      { path: '/user/id', name: 'user_id', type: 'int' },
      { path: '$.user.name', name: 'name' },
      { path: '/user/active', name: 'active', type: 'bool' },
      { path: '/scores', name: 'scores' },
    ]),
    codec.csvEncode(),
  ])
  const data = '{"user":{"id":42,"name":"Ada","active":true},"scores":[10,20]}\n{"user":{"id":7,"name":"Ben","active":false}}\nnot json\n'
  const text = (await p.run({ input: data })).outputText
  assert(text.includes('_line,user_id,name,active,scores'), 'should append declared flatten columns')
  assert(text.includes('42,Ada,true'), 'should extract typed first row')
  assert(text.includes('[10,20]'), 'should keep array field as JSON string')
  assert(text.includes('7,Ben,false'), 'should extract typed second row')
  assert(text.includes('not json'), 'should preserve malformed source row with null fields')

  const nested = pipeline([
    codec.jsonl(),
    ops.jsonFlatten([
      { path: '$.city', name: 'city' },
      { path: '/zip', name: 'zip', type: 'int' },
    ], { column: 'payload' }),
    codec.csvEncode(),
  ])
  const nestedInput = '{"id":1,"payload":{"city":"NY","zip":10001}}\n{"id":2,"payload":{"city":"LA","zip":90001}}\n'
  const nestedText = (await nested.run({ input: nestedInput })).outputText
  assert(nestedText.includes('id,payload,city,zip'), 'should append nested flatten columns')
  assert(nestedText.includes('NY,10001'), 'should extract NY payload')
  assert(nestedText.includes('LA,90001'), 'should extract LA payload')
})


console.log('\nFile input:')

await test('file input', async () => {
  const p = pipeline([
    codec.csv(),
    ops.filter(expr("col('age') > 30")),
    codec.csvEncode(),
  ])
  const result = await p.run({ inputFile: join(fixturesDir, 'sample.csv') })
  const text = result.outputText
  assert(text.includes('Charlie'), 'Charlie age 35')
  assert(text.includes('Eve'), 'Eve age 42')
  assert(!text.includes('Alice'), 'Alice age 30 not > 30')
})


await test('multi-file source column', async () => {
  const dir = await mkdtemp(join(tmpdir(), `tranfi-multifile-${process.pid}-`))
  const first = join(dir, 'part-a.csv')
  const second = join(dir, 'part-b.csv')
  try {
    await writeFile(first, 'name\nAlice')
    await writeFile(second, 'Bob\n')

    const p = pipeline('csv | csv')
    const result = await p.run({ inputFiles: [first, second], sourceColumn: 'src', chunkSize: 2 })
    assert(result.outputText.includes('name,src'), 'source column should be appended')
    assert(result.outputText.includes(`Alice,${first}`), 'first file row should keep first path')
    assert(result.outputText.includes(`Bob,${second}`), 'second file row should keep second path')

    const chunks = []
    for await (const chunk of p.iterChunks({ inputFiles: [first, second], sourceColumn: 'src', chunkSize: 3 })) {
      chunks.push(Buffer.from(chunk))
    }
    const text = Buffer.concat(chunks).toString('utf-8')
    assert(chunks.length > 1, 'multi-file iterChunks should stream bounded chunks')
    assert(text.includes(`Alice,${first}`), 'iterChunks should keep first source path')
    assert(text.includes(`Bob,${second}`), 'iterChunks should keep second source path')

    await assertRejects(
      () => p.run({ input: 'name\nAlice\n', sourceColumn: 'src' }),
      /sourceColumn requires inputFile or inputFiles/
    )
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})

await test('stack file append runs without blocking opt-in', async () => {
  const dir = await mkdtemp(join(tmpdir(), `tranfi-stack-${process.pid}-`))
  const append = join(dir, 'append.csv')
  try {
    await writeFile(append, 'name,age\nCara,35\nDan,41\n')
    const result = await pipeline([
      codec.csv(),
      ops.stack(append),
      codec.csvEncode(),
    ]).run({
      input: 'name,age\nAlice,30\nBob,25\n',
      allowFs: true,
      chunkSize: 5,
    })
    assert(result.outputText.includes('Alice,30'), 'stack should keep source row')
    assert(result.outputText.includes('Bob,25'), 'stack should keep second source row')
    assert(result.outputText.includes('Cara,35'), 'stack should append file row')
    assert(result.outputText.includes('Dan,41'), 'stack should append second file row')
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})


await test('multi-file skip repeated CSV header', async () => {
  const dir = await mkdtemp(join(tmpdir(), `tranfi-header-${process.pid}-`))
  const first = join(dir, 'part-a.csv')
  const second = join(dir, 'part-b.csv')
  try {
    await writeFile(first, 'name,age\nAlice,30')
    await writeFile(second, 'name,age\nBob,25\n')

    const p = pipeline([
      codec.csv({ skipRepeatedHeader: true }),
      codec.csvEncode(),
    ])
    const result = await p.run({ inputFiles: [first, second], chunkSize: 2 })
    assert(result.outputText.split('name,age').length - 1 === 1, 'second header should be stripped')
    assert(result.outputText.includes('Alice,30'), 'first data row should be present')
    assert(result.outputText.includes('Bob,25'), 'second data row should be present')

    const chunks = []
    for await (const chunk of p.iterChunks({ inputFiles: [first, second], chunkSize: 3 })) {
      chunks.push(Buffer.from(chunk))
    }
    const text = Buffer.concat(chunks).toString('utf-8')
    assert(chunks.length > 1, 'iterChunks should stream bounded chunks')
    assert(text.split('name,age').length - 1 === 1, 'iterChunks should strip second header')
    assert(text.includes('Bob,25'), 'iterChunks should include second data row')

    const dslResult = await pipeline('csv skipRepeatedHeader=true | csv').run({ inputFiles: [first, second], chunkSize: 2 })
    assert(dslResult.outputText.split('name,age').length - 1 === 1, 'DSL camelCase option should work')
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})


await test('gzip file input', async () => {
  const gzPath = join(tmpdir(), `tranfi-sample-${process.pid}.csv.gz`)
  const csv = 'name,age\nAlice,30\nBob,25\nCharlie,35\nEve,42\n'
  await writeFile(gzPath, gzipSync(Buffer.from(csv)))
  try {
    const p = pipeline('csv | filter "col(age) > 30" | csv')
    const result = await p.run({ inputFile: gzPath, chunkSize: 5 })
    assert(result.outputText.includes('Charlie,35'), 'gzip input should include Charlie')
    assert(result.outputText.includes('Eve,42'), 'gzip input should include Eve')
    assert(!result.outputText.includes('Alice,30'), 'gzip input should filter Alice')

    const forced = await p.run({ inputFile: gzPath, compression: 'gzip', chunkSize: 5 })
    assert(forced.outputText === result.outputText, 'forced gzip should match auto gzip')

    const chunks = []
    for await (const chunk of p.iterChunks({ inputFile: gzPath, chunkSize: 6 })) {
      chunks.push(Buffer.from(chunk))
    }
    const text = Buffer.concat(chunks).toString('utf-8')
    assert(chunks.length > 1, 'gzip iterChunks should yield bounded chunks')
    assert(text.includes('Charlie,35'), 'gzip iterChunks should include Charlie')

    const streamResult = await p.run({
      inputStream: Readable.from([gzipSync(Buffer.from(csv))], { objectMode: false }),
      compression: 'gzip',
      chunkSize: 5
    })
    assert(streamResult.outputText.includes('Eve,42'), 'gzip inputStream should decompress')

    await assertRejects(
      () => p.run({ input: gzipSync(Buffer.from(csv)), compression: 'gzip' }),
      /compression=gzip requires inputFile, inputFiles, or inputStream/
    )
  } finally {
    await unlink(gzPath).catch(() => {})
  }
})

console.log('\nMisc:')

await test('stats channel', async () => {
  const p = pipeline([
    codec.csv(),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'x\n1\n2\n3\n' })
  assert(result.stats.length > 0, 'should have stats')
  assert(result.statsText.includes('rows_in'), 'stats should have rows_in')
  assert(result.statsText.includes('"type":"step_stats"'), 'stats should include step stats')
  assert(result.statsText.includes('"steps":[]'), 'no-transform pipeline should report empty steps')

  const filtered = await pipeline([
    codec.csv(),
    ops.filter(expr("col('x') > 1")),
    codec.csvEncode(),
  ]).run({ input: 'x\n1\n2\n3\n' })
  assert(filtered.statsText.includes('"op":"filter"'), 'step stats should include filter op')
  assert(filtered.statsText.includes('"execution_target":"native"'), 'step stats should include execution target')
  assert(filtered.statsText.includes('"memory_class":"row_local"'), 'step stats should include memory class')
  assert(filtered.statsText.includes('"state_bytes_estimate":null'), 'row-local step should report null state byte estimate')
  assert(filtered.statsText.includes('"warnings":[]'), 'row-local step should report no warnings')
  assert(filtered.statsText.includes('"batches_in":1'), 'step stats should include input batches')
  assert(filtered.statsText.includes('"rows_in":3'), 'step stats should include input rows')
  assert(filtered.statsText.includes('"rows_out":2'), 'step stats should include output rows')

  const audited = await pipeline([
    codec.csv({ batchSize: 1 }),
    ops.filter(expr("col('x') > 1"), { audit: true, auditLimit: 1 }),
    codec.csvEncode(),
  ]).run({ input: 'x,name\n1,Bob\n2,Alice\n0,Dave\n' })
  assert(audited.outputText.includes('Alice'), 'audit filter should keep passing row')
  assert(!audited.outputText.includes('Bob'), 'audit filter should drop first failing row from main output')
  assert(audited.statsText.includes('"type":"audit"'), 'filter audit should emit audit record')
  assert(audited.statsText.includes('"op":"filter"'), 'filter audit should identify filter op')
  assert(audited.statsText.includes('"event":"row_dropped"'), 'filter audit should identify dropped row event')
  assert(audited.statsText.includes('"reason":"predicate_false"'), 'filter audit should identify false predicate')
  assert(audited.statsText.includes('"row":1'), 'filter audit should include source row index')
  assert(audited.statsText.includes('"Bob"'), 'filter audit should include first dropped row data')
  assert(!audited.statsText.includes('"Dave"'), 'filter audit should enforce auditLimit')

  const bounded = await pipeline([
    codec.csv(),
    ops.unique(['city'], { maxKeys: 3 }),
    codec.csvEncode(),
  ]).run({ input: 'city\nNY\nLA\nNY\n' })
  assert(bounded.statsText.includes('"op":"unique"'), 'state byte stats should include unique op')
  assert(bounded.statsText.includes('"state_bytes_estimate":1888'), 'capped unique should report byte estimate')
  assert(!bounded.statsText.includes('state_bytes_reason'), 'capped unique should not need a byte-estimate reason')
  assert(bounded.statsText.includes('"warnings":[]'), 'capped unique should report no warnings')
  assert(bounded.statsText.includes('"tracked_keys":2'), 'capped unique should report tracked keys')
  assert(bounded.statsText.includes('"tracked_key_bytes":14'), 'capped unique should report serialized key bytes')
  assert(bounded.statsText.includes('"retained_state_bytes":'), 'capped unique should report retained state bytes')

  const uncapped = await pipeline([
    codec.csv(),
    ops.unique(['city']),
    codec.csvEncode(),
  ]).run({ input: 'city\nNY\nLA\nNY\n' })
  assert(uncapped.statsText.includes('"state_bytes_estimate":null'), 'uncapped unique should report null byte estimate')
  assert(uncapped.statsText.includes("\"state_bytes_reason\":\"step 'unique' needs max_keys"), 'uncapped unique should explain missing cap')
  assert(uncapped.statsText.includes('"warnings":["unbounded_state"]'), 'uncapped unique should report unbounded warning')
  assert(uncapped.statsText.includes('"tracked_keys":2'), 'uncapped unique should report tracked keys')
  assert(uncapped.statsText.includes('"tracked_key_bytes":14'), 'uncapped unique should report serialized key bytes')
  assert(uncapped.statsText.includes('"retained_state_bytes":'), 'uncapped unique should report retained state bytes')

  const groupData = ['city,sales', 'NY,1', 'LA,2', 'NY,3', ''].join(String.fromCharCode(10))
  const grouped = await pipeline([
    codec.csv(),
    ops.groupAgg(['city'], [{ column: 'sales', func: 'sum', name: 'total' }], { maxGroups: 3 }),
    codec.csvEncode(),
  ]).run({ input: groupData })
  assert(grouped.statsText.includes('"op":"group-agg"'), 'groupAgg stats should include op name')
  assert(grouped.statsText.includes('"tracked_groups":2'), 'groupAgg stats should report tracked groups')
  assert(grouped.statsText.includes('"tracked_key_bytes":14'), 'groupAgg stats should report serialized group-key bytes')
  assert(grouped.statsText.includes('"retained_state_bytes":'), 'groupAgg stats should report retained state bytes')

  const rowidData = ['city', 'NY', 'LA', 'NY', ''].join(String.fromCharCode(10))
  const rowids = await pipeline([
    codec.csv(),
    ops.rowid('city', { result: 'city_row', maxKeys: 3 }),
    codec.csvEncode(),
  ]).run({ input: rowidData })
  assert(rowids.statsText.includes('"op":"rowid"'), 'rowid stats should include op name')
  assert(rowids.statsText.includes('"tracked_keys":2'), 'rowid stats should report tracked keys')
  assert(rowids.statsText.includes('"tracked_key_bytes":26'), 'rowid stats should report serialized key bytes')
  assert(rowids.statsText.includes('"retained_state_bytes":'), 'rowid stats should report retained state bytes')

  const onehot = await pipeline([
    codec.csv(),
    ops.onehot('city', { maxCategories: 3 }),
    codec.csvEncode(),
  ]).run({ input: rowidData })
  assert(onehot.statsText.includes('"op":"onehot"'), 'onehot stats should include op name')
  assert(onehot.statsText.includes('"tracked_categories":2'), 'onehot stats should report tracked categories')
  assert(onehot.statsText.includes('"category_value_bytes":6'), 'onehot stats should report category value bytes')
  assert(onehot.statsText.includes('"category_output_name_bytes":16'), 'onehot stats should report output column-name bytes')
  assert(onehot.statsText.includes('"retained_state_bytes":'), 'onehot stats should report retained state bytes')

  const labels = await pipeline([
    codec.csv(),
    ops.labelEncode('city', { result: 'city_id', maxCategories: 3 }),
    codec.csvEncode(),
  ]).run({ input: rowidData })
  assert(labels.statsText.includes('"op":"label-encode"'), 'labelEncode stats should include op name')
  assert(labels.statsText.includes('"tracked_categories":2'), 'labelEncode stats should report tracked categories')
  assert(labels.statsText.includes('"category_value_bytes":6'), 'labelEncode stats should report category value bytes')
  assert(labels.statsText.includes('"retained_state_bytes":'), 'labelEncode stats should report retained state bytes')
})

await test('scan profile op', async () => {
  const result = await pipeline([
    codec.csv(),
    ops.scan(),
    codec.csvEncode(),
  ]).run({ input: 'name,age\nAlice,30\nBob,\nAlice,35\n' })
  assert(result.outputText.includes('column,count,missing,complete_rate'), 'scan should include profile defaults')
  assert(result.outputText.includes('distinct'), 'scan should include distinct estimate')
  assert(result.outputText.includes('hist'), 'scan should include histogram')
  assert(result.outputText.includes('sample'), 'scan should include bounded sample')
  assert(result.outputText.includes('name'), 'scan should include name column profile')
  assert(result.outputText.includes('age'), 'scan should include age column profile')
  assert(result.statsText.includes('"op":"scan"'), 'step stats should include scan op')
  assert(result.statsText.includes('"emit_class":"on_flush"'), 'scan should be flush-latent')

  const selective = await pipeline([
    codec.csv(),
    ops.stats(['count', 'missing', 'complete_rate']),
    codec.csvEncode(),
  ]).run({ input: 'x,y\n1,a\n,b\n3,\n' })
  assert(selective.outputText.includes('column,count,missing,complete_rate'), 'stats should expose missing profile stats')
  assert(selective.outputText.includes('x,2,1'), 'x should have one missing value')
  assert(selective.outputText.includes('y,2,1'), 'y should have one missing value')
})

await test('between expression filter', async () => {
  const p = pipeline([
    codec.csv(),
    ops.filter(expr("between(col('age'), 25, 35)")),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'name,age\nAlice,30\nBob,20\nCharlie,35\nDiana,40\n' })
  const text = result.outputText
  assert(text.includes('Alice'), 'should keep Alice')
  assert(text.includes('Charlie'), 'should keep Charlie')
  assert(!text.includes('Bob'), 'should drop Bob')
  assert(!text.includes('Diana'), 'should drop Diana')
})

await test('case_when and case_match expressions', async () => {
  const p = pipeline([
    codec.csv(),
    ops.derive({
      age_band: expr("case_when(col('age') < 25, 'young', col('age') < 40, 'adult', 'senior')"),
      city_code: expr("case_match(col('city'), 'NY', 'new-york', 'LA', 'los-angeles', 'other')"),
    }),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'name,age,city\nAlice,30,NY\nBob,20,LA\nCara,45,SF\n' })
  const text = result.outputText
  assert(text.includes('Alice,30,NY,adult,new-york'), 'Alice should map to adult/new-york')
  assert(text.includes('Bob,20,LA,young,los-angeles'), 'Bob should map to young/los-angeles')
  assert(text.includes('Cara,45,SF,senior,other'), 'Cara should map to senior/other')
})

await test('across row-local transforms', async () => {
  const data = 'id,score_math,score_read,name\n1,90.4,80.6,Alice\n2,70.2,95.8,Bob\n'
  const rounded = await pipeline([
    codec.csv({ batchSize: 1 }),
    ops.across(['starts_with(score_)'], { fn: 'round' }),
    codec.csvEncode(),
  ]).run({ input: data })
  assert(rounded.outputText.startsWith('id,score_math,score_read,name\n'), 'across should keep schema order for replacement')
  assert(rounded.outputText.includes('1,90,81,Alice'), 'across round should replace score columns')
  assert(rounded.outputText.includes('2,70,96,Bob'), 'across round should replace second row')

  const appended = await pipeline([
    codec.csv(),
    ops.across(['name'], { functions: ['lower'], replace: false, names: '{col}_{fn}' }),
    codec.csvEncode(),
  ]).run({ input: 'name,score\nAlice,1\nBOB,2\n' })
  assert(appended.outputText.startsWith('name,score,name_lower\n'), 'across append should use name template')
  assert(appended.outputText.includes('Alice,1,alice'), 'across lower should append lowercase value')
  assert(appended.outputText.includes('BOB,2,bob'), 'across lower should append lowercase second row')

  const dsl = await pipeline('csv batch_size=1 | across starts_with(score_) round | csv').run({ input: data })
  assert(dsl.outputText.includes('1,90,81,Alice'), 'DSL across compact form should run')

  await assertRejects(
    () => pipeline([codec.csv(), ops.across(['score'], { fn: 'lower' }), codec.csvEncode()]).run({ input: 'score\n1\n' }),
    /incompatible|processing error/
  )
})


await test('date expression functions', async () => {
  const data = 'd,ts\n2024-03-15,2024-03-15T12:34:56Z\n2023-12-25,2023-12-25T08:09:10Z\n'
  const result = await pipeline([
    codec.csv({ batchSize: 1 }),
    ops.filter(expr("year(col('d')) == 2024")),
    ops.derive({
      d_year: expr("year(col('d'))"),
      d_month: expr("month(col('d'))"),
      d_day: expr("day(col('d'))"),
      d_weekday: expr("weekday(col('d'))"),
      month_start: expr("date_trunc(col('d'), 'month')"),
      hour_start: expr("date_trunc(col('ts'), 'hour')"),
    }),
    codec.csvEncode(),
  ]).run({ input: data })
  assert(result.outputText.includes('2024-03-15,2024-03-15T12:34:56Z,2024,3,15,5,2024-03-01,2024-03-15T12:00:00Z'), 'date expressions should derive components and truncations')
  assert(!result.outputText.includes('2023-12-25'), 'date expression filter should drop 2023 row')

  const dsl = await pipeline("csv batch_size=1 | derive y=year(col('d')) m=date_trunc(col('d'),'month') | csv").run({ input: data })
  assert(dsl.outputText.includes('2024-03-15,2024-03-15T12:34:56Z,2024,2024-03-01'), 'DSL date expression should derive year and month truncation')
  assert(dsl.outputText.includes('2023-12-25,2023-12-25T08:09:10Z,2023,2023-12-01'), 'DSL date expression should keep second row')
})

await test('if_any and if_all expressions', async () => {
  const data = 'id,a,b\n1,0,0\n2,1,0\n3,1,2\n'
  const pAny = pipeline([
    codec.csv(),
    ops.filter(expr("if_any(col('a') > 0, col('b') > 0)")),
    codec.csvEncode(),
  ])
  const anyText = (await pAny.run({ input: data })).outputText
  assert(anyText.includes('2,1,0'), 'any should keep row 2')
  assert(anyText.includes('3,1,2'), 'any should keep row 3')
  assert(!anyText.includes('1,0,0'), 'any should drop row 1')

  const pAll = pipeline([
    codec.csv(),
    ops.filter(expr("if_all(col('a') > 0, col('b') > 0)")),
    codec.csvEncode(),
  ])
  const allText = (await pAll.run({ input: data })).outputText
  assert(allText.includes('3,1,2'), 'all should keep row 3')
  assert(!allText.includes('1,0,0'), 'all should drop row 1')
  assert(!allText.includes('2,1,0'), 'all should drop row 2')
})

await test('run onOutput callback without collecting output', async () => {
  const p = pipeline([
    codec.csv(),
    codec.csvEncode(),
  ])
  const chunks = []
  const result = await p.run({
    input: 'name,age\nAlice,30\nBob,25\nCharlie,35\n',
    chunkSize: 8,
    onOutput: async chunk => chunks.push(Buffer.from(chunk)),
    collectOutput: false
  })
  const text = Buffer.concat(chunks).toString('utf-8')
  assert(result.output.length === 0, 'should not collect main output')
  assert(chunks.length > 1, 'should receive multiple bounded chunks')
  assert(text.includes('name,age'), 'should have header')
  assert(text.includes('Charlie'), 'should have Charlie')
  assert(result.statsText.includes('rows_in'), 'stats should still be collected')
})

await test('iterChunks yields bounded output chunks', async () => {
  const p = pipeline([
    codec.csv(),
    codec.csvEncode(),
  ])
  const chunks = []
  for await (const chunk of p.iterChunks({
    input: 'name,age\nAlice,30\nBob,25\nCharlie,35\n',
    chunkSize: 7
  })) {
    chunks.push(Buffer.from(chunk))
  }
  const text = Buffer.concat(chunks).toString('utf-8')
  assert(chunks.length > 1, 'should yield multiple bounded chunks')
  assert(text.includes('name,age'), 'should have header')
  assert(text.includes('Alice'), 'should have Alice')
  assert(text.includes('Charlie'), 'should have Charlie')
})

await test('run accepts inputStream chunks', async () => {
  const p = pipeline('csv | filter "col(age) >= 30" | csv')
  const inputStream = Readable.from(['name,age\nAli', 'ce,30\nBob,25\nCarol,40\n'], { objectMode: false })
  const result = await p.run({ inputStream, chunkSize: 7 })
  const text = result.outputText
  assert(text.includes('Alice,30'), 'inputStream should include Alice')
  assert(text.includes('Carol,40'), 'inputStream should include Carol')
  assert(!text.includes('Bob,25'), 'inputStream should filter Bob')
})

await test('toReadable yields output as a Node stream', async () => {
  const p = pipeline('csv | filter "col(age) >= 30" | csv')
  const readable = p.toReadable({ input: 'name,age\nAlice,30\nBob,25\nCarol,40\n', chunkSize: 8, highWaterMark: 1 })
  const chunks = []
  for await (const chunk of readable) {
    chunks.push(Buffer.from(chunk))
  }
  const text = Buffer.concat(chunks).toString('utf-8')
  assert(text.includes('Alice,30'), 'toReadable should include Alice')
  assert(text.includes('Carol,40'), 'toReadable should include Carol')
  assert(!text.includes('Bob,25'), 'toReadable should filter Bob')
})

await test('writeTo respects writable backpressure', async () => {
  class SlowSink extends Writable {
    constructor() {
      super({ highWaterMark: 1 })
      this.chunks = []
      this.falseWrites = 0
    }

    write(chunk, encoding, callback) {
      const ok = super.write(chunk, encoding, callback)
      if (!ok) this.falseWrites++
      return ok
    }

    _write(chunk, encoding, callback) {
      this.chunks.push(Buffer.from(chunk))
      setImmediate(callback)
    }
  }

  const sink = new SlowSink()
  const p = pipeline('csv | filter "col(age) >= 30" | csv')
  const result = await p.writeTo(sink, { input: 'name,age\nAlice,30\nBob,25\nCarol,40\n', chunkSize: 8 })
  const text = Buffer.concat(sink.chunks).toString('utf-8')
  assert(result.output.length === 0, 'writeTo should not collect main output')
  assert(sink.falseWrites > 0, 'writeTo should observe writable backpressure')
  assert(sink.writableEnded, 'writeTo should end the sink by default')
  assert(text.includes('Alice,30'), 'writeTo should include Alice')
  assert(text.includes('Carol,40'), 'writeTo should include Carol')
  assert(!text.includes('Bob,25'), 'writeTo should filter Bob')
})

await test('error handling', async () => {
  try {
    const p = pipeline([])
    await p.run({ input: '' })
    assert(false, 'should have thrown')
  } catch {
    // expected
  }
})

await test('native memory policy rejects blocking by default', async () => {
  const p = pipeline([codec.csv(), ops.sort(['age']), codec.csvEncode()])
  await assertRejects(
    () => p.run({ input: 'name,age\nAlice,30\nBob,25\n' }),
    /blocking step 'sort'/
  )
  const result = await p.run({ input: 'name,age\nAlice,30\nBob,25\n', allowBlocking: true })
  assert(result.outputText.includes('Bob,25'), 'allowBlocking should run known-small sort')
})

await test('native spill sort drains during finish', async () => {
  const dir = await mkdtemp(join(tmpdir(), `tranfi-node-spill-${process.pid}-`))
  try {
    const p = pipeline('csv | sort age | csv')
    const chunks = []
    const result = await p.run({
      input: 'name,age\nAlice,30\nBob,25\nCara,35\n',
      spillDir: dir,
      memory: '1KB',
      chunkSize: 8,
      onOutput: chunk => chunks.push(Buffer.from(chunk)),
      collectOutput: false,
    })
    assert(result.output.length === 0, 'spill callback run should not collect output')
    assert(Buffer.concat(chunks).toString('utf8').trim() === 'name,age\nBob,25\nAlice,30\nCara,35', 'spill callback output should be sorted')
    assert(result.statsText.includes('\"spill_bytes\":'), 'spill stats should report spilled bytes')
    assert(result.statsText.includes('\"spill_runs\":1'), 'spill stats should report run count')
    assert(result.statsText.includes('\"spill_output_rows\":3'), 'spill stats should report merge output rows')
    assert((await readdir(dir)).length === 0, 'spill temp files should be removed')

    const iterChunks = []
    for await (const chunk of p.iterChunks({
      input: 'name,age\nAlice,30\nBob,25\nCara,35\n',
      spillDir: dir,
      memory: '1KB',
      chunkSize: 7,
    })) {
      iterChunks.push(Buffer.from(chunk))
    }
    assert(Buffer.concat(iterChunks).toString('utf8').trim() === 'name,age\nBob,25\nAlice,30\nCara,35', 'spill iterChunks output should be sorted')
    assert((await readdir(dir)).length === 0, 'spill iterChunks temp files should be removed')

    const typedCsv = [
      'id,grp,d,ts,score,flag,name',
      'r1,B,2024-01-01,2024-01-01T09:00:00Z,1.5,true,zeta',
      'r2,A,2024-01-02,2024-01-02T07:00:00Z,3.0,false,beta',
      'r3,A,2024-01-02,2024-01-02T07:00:00Z,2.0,true,alpha',
      'r4,A,2024-01-02,2024-01-02T07:00:00Z,NA,false,gamma',
      'r5,A,2024-01-01,2024-01-01T12:00:00Z,0.5,true,delta',
      'r6,B,2024-01-01,2024-01-01T10:00:00Z,1.0,false,eta',
      'r7,A,2024-01-02,2024-01-02T07:00:00Z,2.0,false,aardvark',
      'r8,A,2024-01-02,2024-01-02T07:00:00Z,2.0,false,bravo',
      '',
    ].join('\n')
    const typedPipeline = pipeline('csv batch_size=2 nulls=NA | sort grp -d -ts score -flag name | csv')
    const typedChunks = []
    for await (const chunk of typedPipeline.iterChunks({ input: typedCsv, spillDir: dir, memory: '1KB', chunkSize: 19 })) {
      typedChunks.push(Buffer.from(chunk))
    }
    const typedLines = Buffer.concat(typedChunks).toString('utf8').trim().split('\n')
    assert(JSON.stringify(typedLines.slice(1).map(line => line.split(',')[0])) === JSON.stringify(['r3', 'r7', 'r8', 'r2', 'r4', 'r5', 'r6', 'r1']), 'typed spill sort should preserve multi-key order')
    assert(typedLines.includes('r4,A,2024-01-02,2024-01-02T07:00:00Z,,false,gamma'), 'typed spill sort should keep null score last')
    assert((await readdir(dir)).length === 0, 'typed spill temp files should be removed')

    const uniqueCsv = [
      'id,name,score',
      '3,C,30',
      '1,A,10',
      '2,B,20',
      '1,A2,11',
      '3,C2,31',
      '4,D,40',
      '',
    ].join('\n')
    const uniquePipeline = pipeline('csv batch_size=1 | unique id | csv')
    const uniqueChunks = []
    const uniqueResult = await uniquePipeline.run({
      input: uniqueCsv,
      spillDir: dir,
      memory: '1KB',
      chunkSize: 9,
      onOutput: chunk => uniqueChunks.push(Buffer.from(chunk)),
      collectOutput: false,
    })
    assert(uniqueResult.output.length === 0, 'spill unique callback run should not collect output')
    assert(Buffer.concat(uniqueChunks).toString('utf8').trim() === 'id,name,score\n3,C,30\n1,A,10\n2,B,20\n4,D,40', 'spill unique should preserve first-row input order')
    assert(uniqueResult.statsText.includes('"execution_target":"native_spill"'), 'spill unique stats should report native_spill')
    assert(uniqueResult.statsText.includes('"memory_class":"external"'), 'spill unique stats should report external memory class')
    assert(uniqueResult.statsText.includes('"spill_distinct_rows":4'), 'spill unique stats should report distinct rows')
    assert((await readdir(dir)).length === 0, 'spill unique temp files should be removed')

    const uniqueIterChunks = []
    for await (const chunk of uniquePipeline.iterChunks({ input: uniqueCsv, spillDir: dir, memory: '1KB', chunkSize: 7 })) {
      uniqueIterChunks.push(Buffer.from(chunk))
    }
    assert(Buffer.concat(uniqueIterChunks).toString('utf8').trim() === 'id,name,score\n3,C,30\n1,A,10\n2,B,20\n4,D,40', 'spill unique iterChunks should preserve first-row input order')
    assert((await readdir(dir)).length === 0, 'spill unique iterChunks temp files should be removed')

    const groupCsv = [
      'city,sales',
      'B,10',
      'A,1',
      'C,5',
      'A,2',
      'B,3',
      'D,7',
      '',
    ].join('\n')
    const groupPipeline = pipeline('csv batch_size=1 | group-agg city sum:sales:total count:sales:n count:*:rows | csv')
    const groupChunks = []
    const groupResult = await groupPipeline.run({
      input: groupCsv,
      spillDir: dir,
      memory: '1KB',
      chunkSize: 8,
      onOutput: chunk => groupChunks.push(Buffer.from(chunk)),
      collectOutput: false,
    })
    assert(groupResult.output.length === 0, 'spill group-agg callback run should not collect output')
    assert(Buffer.concat(groupChunks).toString('utf8').trim() === 'city,total,n,rows\nB,13,2,2\nA,3,2,2\nC,5,1,1\nD,7,1,1', 'spill group-agg should preserve first-seen group order')
    assert(groupResult.statsText.includes('"execution_target":"native_spill"'), 'spill group-agg stats should report native_spill')
    assert(groupResult.statsText.includes('"memory_class":"external"'), 'spill group-agg stats should report external memory class')
    assert(groupResult.statsText.includes('"spill_distinct_groups":4'), 'spill group-agg stats should report distinct groups')
    assert((await readdir(dir)).length === 0, 'spill group-agg temp files should be removed')

    const groupIterChunks = []
    for await (const chunk of groupPipeline.iterChunks({ input: groupCsv, spillDir: dir, memory: '1KB', chunkSize: 7 })) {
      groupIterChunks.push(Buffer.from(chunk))
    }
    assert(Buffer.concat(groupIterChunks).toString('utf8').trim() === 'city,total,n,rows\nB,13,2,2\nA,3,2,2\nC,5,1,1\nD,7,1,1', 'spill group-agg iterChunks should preserve first-seen group order')
    assert((await readdir(dir)).length === 0, 'spill group-agg iterChunks temp files should be removed')


    const pivotCsv = [
      'id,metric,value',
      'B,y,4',
      'A,x,1',
      'B,x,3',
      'A,y,2',
      'A,x,5',
      '',
    ].join('\n')
    const pivotPipeline = pipeline('csv batch_size=1 | pivot metric value sum max_categories=2 | csv')
    const pivotChunks = []
    const pivotResult = await pivotPipeline.run({
      input: pivotCsv,
      spillDir: dir,
      memory: '1KB',
      chunkSize: 7,
      onOutput: chunk => pivotChunks.push(Buffer.from(chunk)),
      collectOutput: false,
    })
    assert(pivotResult.output.length === 0, 'spill pivot callback run should not collect output')
    assert(Buffer.concat(pivotChunks).toString('utf8').trim() === 'id,y,x\nB,4,3\nA,2,6', 'spill pivot should preserve first-seen group and category order')
    assert(pivotResult.statsText.includes('"execution_target":"native_spill"'), 'spill pivot stats should report native_spill')
    assert(pivotResult.statsText.includes('"memory_class":"external"'), 'spill pivot stats should report external memory class')
    assert(pivotResult.statsText.includes('"schema_class":"data_dependent"'), 'spill pivot stats should report data-dependent schema')
    assert(pivotResult.statsText.includes('"tracked_categories":2'), 'spill pivot stats should report category count')
    assert(pivotResult.statsText.includes('"spill_distinct_groups":2'), 'spill pivot stats should report distinct groups')
    assert((await readdir(dir)).length === 0, 'spill pivot temp files should be removed')

    const declaredPivot = pipeline('csv batch_size=1 | pivot metric value sum categories=x,y | csv')
    const declaredPivotChunks = []
    for await (const chunk of declaredPivot.iterChunks({ input: pivotCsv, spillDir: dir, memory: '1KB', chunkSize: 7 })) {
      declaredPivotChunks.push(Buffer.from(chunk))
    }
    assert(Buffer.concat(declaredPivotChunks).toString('utf8').trim() === 'id,x,y\nB,3,4\nA,6,2', 'declared spill pivot should preserve declared category order')
    assert((await readdir(dir)).length === 0, 'declared spill pivot temp files should be removed')

    const mutatingLookup = join(tmpdir(), `tranfi-node-mutating-join-lookup-${process.pid}.csv`)
    await writeFile(mutatingLookup, 'id,city,rank\n3,CHI,30\n1,LA,10\n1,SF,11\n5,SEA,50\n9,NY,90\n')
    try {
      const mutatingCsv = [
        'id,name',
        '4,D',
        '1,A',
        '2,B',
        '3,C',
        '1,A2',
        '5,E',
        '',
      ].join('\n')
      const mutatingJoin = pipeline(`csv batch_size=1 | join ${mutatingLookup} on=id max_matches_per_row=2 | csv`)
      const joinChunks = []
      const joinResult = await mutatingJoin.run({
        input: mutatingCsv,
        spillDir: dir,
        memory: '1KB',
        chunkSize: 7,
        onOutput: chunk => joinChunks.push(Buffer.from(chunk)),
        collectOutput: false,
      })
      assert(joinResult.output.length === 0, 'spill mutating join callback run should not collect output')
      assert(Buffer.concat(joinChunks).toString('utf8').trim() === 'id,name,city,rank\n1,A,LA,10\n1,A,SF,11\n3,C,CHI,30\n1,A2,LA,10\n1,A2,SF,11\n5,E,SEA,50', 'spill mutating join should preserve left and match order')
      assert(joinResult.statsText.includes('"execution_target":"native_spill"'), 'spill mutating join stats should report native_spill')
      assert(joinResult.statsText.includes('"memory_class":"external"'), 'spill mutating join stats should report external memory class')
      assert(joinResult.statsText.includes('"schema_class":"data_dependent"'), 'spill mutating join stats should report data-dependent schema')
      assert(joinResult.statsText.includes('"spill_output_rows":6'), 'spill mutating join stats should report output rows')
      assert(joinResult.statsText.includes('"spill_kept_rows":6'), 'spill mutating join stats should report emitted rows')
      assert((await readdir(dir)).length === 0, 'spill mutating join temp files should be removed')

      const mutatingLeft = pipeline(`csv batch_size=1 | join ${mutatingLookup} on=id --left max_matches_per_row=2 | csv`)
      const leftChunks = []
      for await (const chunk of mutatingLeft.iterChunks({ input: mutatingCsv, spillDir: dir, memory: '1KB', chunkSize: 7 })) {
        leftChunks.push(Buffer.from(chunk))
      }
      assert(Buffer.concat(leftChunks).toString('utf8').trim() === 'id,name,city,rank\n4,D,,\n1,A,LA,10\n1,A,SF,11\n2,B,,\n3,C,CHI,30\n1,A2,LA,10\n1,A2,SF,11\n5,E,SEA,50', 'spill left join should null-fill unmatched rows and preserve order')
      assert((await readdir(dir)).length === 0, 'spill left join temp files should be removed')

      await assertRejects(
        () => pipeline(`csv | join ${mutatingLookup} on=id | csv`).run({ input: mutatingCsv, spillDir: dir, memory: '1KB' }),
        /max_matches_per_row/
      )
    } finally {
      await unlink(mutatingLookup).catch(() => {})
    }

    const setLookup = join(tmpdir(), `tranfi-node-set-lookup-${process.pid}.csv`)
    await writeFile(setLookup, 'id,label\n3,c\n1,a\n9,z\n')
    const setCsv = [
      'id,name,score',
      '3,C,30',
      '4,D,40',
      '1,A,10',
      '2,B,20',
      '1,A2,11',
      '5,E,50',
      '',
    ].join('\n')
    try {
      const setPipeline = pipeline(`csv batch_size=1 | intersect ${setLookup} id | csv`)
      const setChunks = []
      const setResult = await setPipeline.run({
        input: setCsv,
        spillDir: dir,
        memory: '1KB',
        chunkSize: 9,
        onOutput: chunk => setChunks.push(Buffer.from(chunk)),
        collectOutput: false,
      })
      assert(setResult.output.length === 0, 'spill intersect callback run should not collect output')
      assert(Buffer.concat(setChunks).toString('utf8').trim() === 'id,name,score\n3,C,30\n1,A,10', 'spill intersect should preserve first-left-row order')
      assert(setResult.statsText.includes('"execution_target":"native_spill"'), 'spill intersect stats should report native_spill')
      assert(setResult.statsText.includes('"memory_class":"external"'), 'spill intersect stats should report external memory class')
      assert(setResult.statsText.includes('"spill_distinct_rows":2'), 'spill intersect stats should report distinct rows')
      assert((await readdir(dir)).length === 0, 'spill intersect temp files should be removed')

      const setdiffPipeline = pipeline(`csv batch_size=1 | setdiff ${setLookup} id | csv`)
      const setdiffChunks = []
      for await (const chunk of setdiffPipeline.iterChunks({ input: setCsv, spillDir: dir, memory: '1KB', chunkSize: 7 })) {
        setdiffChunks.push(Buffer.from(chunk))
      }
      assert(Buffer.concat(setdiffChunks).toString('utf8').trim() === 'id,name,score\n4,D,40\n2,B,20\n5,E,50', 'spill setdiff iterChunks should preserve first-left-row order')
      assert((await readdir(dir)).length === 0, 'spill setdiff temp files should be removed')

      const bagLookup = join(tmpdir(), `tranfi-node-bag-set-lookup-${process.pid}.csv`)
      await writeFile(bagLookup, 'id,label\n1,a\n1,a2\n3,c\n5,e\n9,z\n')
      try {
        const bagCsv = [
          'id,name,score',
          '3,C,30',
          '1,A,10',
          '4,D,40',
          '1,A2,11',
          '1,A3,12',
          '5,E,50',
          '2,B,20',
          '',
        ].join('\n')
        const bagInter = pipeline(`csv batch_size=1 | intersect-all ${bagLookup} id | csv`)
        const bagChunks = []
        const bagResult = await bagInter.run({
          input: bagCsv,
          spillDir: dir,
          memory: '1KB',
          chunkSize: 7,
          onOutput: chunk => bagChunks.push(Buffer.from(chunk)),
          collectOutput: false,
        })
        assert(bagResult.output.length === 0, 'spill intersect-all callback run should not collect output')
        assert(Buffer.concat(bagChunks).toString('utf8').trim() === 'id,name,score\n3,C,30\n1,A,10\n1,A2,11\n5,E,50', 'spill intersect-all should preserve bag counts and left order')
        assert(bagResult.statsText.includes('"execution_target":"native_spill"'), 'spill intersect-all stats should report native_spill')
        assert(bagResult.statsText.includes('"memory_class":"external"'), 'spill intersect-all stats should report external memory class')
        assert(bagResult.statsText.includes('"spill_kept_rows":4'), 'spill intersect-all stats should report kept rows')
        assert((await readdir(dir)).length === 0, 'spill intersect-all temp files should be removed')

        const bagDiff = pipeline(`csv batch_size=1 | setdiff-all ${bagLookup} id | csv`)
        const bagDiffChunks = []
        for await (const chunk of bagDiff.iterChunks({ input: bagCsv, spillDir: dir, memory: '1KB', chunkSize: 7 })) {
          bagDiffChunks.push(Buffer.from(chunk))
        }
        assert(Buffer.concat(bagDiffChunks).toString('utf8').trim() === 'id,name,score\n4,D,40\n1,A3,12\n2,B,20', 'spill setdiff-all should preserve surplus copies and left order')
        assert((await readdir(dir)).length === 0, 'spill setdiff-all temp files should be removed')
      } finally {
        await unlink(bagLookup).catch(() => {})
      }
    } finally {
      await unlink(setLookup).catch(() => {})
    }

    const unionLookup = join(tmpdir(), `tranfi-node-union-lookup-${process.pid}.csv`)
    await writeFile(unionLookup, 'id,name,score\n2,B_file,200\n5,E,50\n3,C_file,300\n6,F,60\n')
    try {
      const unionPipeline = pipeline(`csv batch_size=1 | union ${unionLookup} id | csv`)
      const unionChunks = []
      const unionResult = await unionPipeline.run({
        input: setCsv,
        spillDir: dir,
        memory: '1KB',
        chunkSize: 9,
        onOutput: chunk => unionChunks.push(Buffer.from(chunk)),
        collectOutput: false,
      })
      const unionText = Buffer.concat(unionChunks).toString('utf8').trim()
      assert(unionResult.output.length === 0, 'spill union callback run should not collect output')
      assert(unionText === 'id,name,score\n3,C,30\n4,D,40\n1,A,10\n2,B,20\n5,E,50\n6,F,60', 'spill union should preserve first-seen left-then-file order')
      assert(!unionText.includes('B_file') && !unionText.includes('C_file') && !unionText.includes('A2'), 'spill union should drop duplicate-key rows')
      assert(unionResult.statsText.includes('"execution_target":"native_spill"'), 'spill union stats should report native_spill')
      assert(unionResult.statsText.includes('"memory_class":"external"'), 'spill union stats should report external memory class')
      assert(unionResult.statsText.includes('"spill_distinct_rows":6'), 'spill union stats should report distinct rows')
      assert((await readdir(dir)).length === 0, 'spill union temp files should be removed')

      const unionIterChunks = []
      for await (const chunk of unionPipeline.iterChunks({ input: setCsv, spillDir: dir, memory: '1KB', chunkSize: 7 })) {
        unionIterChunks.push(Buffer.from(chunk))
      }
      assert(Buffer.concat(unionIterChunks).toString('utf8').trim() === 'id,name,score\n3,C,30\n4,D,40\n1,A,10\n2,B,20\n5,E,50\n6,F,60', 'spill union iterChunks should preserve first-seen order')
      assert((await readdir(dir)).length === 0, 'spill union iterChunks temp files should be removed')
    } finally {
      await unlink(unionLookup).catch(() => {})
    }
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})

await test('native memory policy enforces capped key-state estimates', async () => {
  const p = pipeline([codec.csv(), ops.unique(['name'], { maxKeys: 2 }), codec.csvEncode()])
  const result = await p.run({ input: 'name,age\nAlice,30\nBob,25\n', memory: '64KB' })
  assert(result.outputText.includes('Alice,30'), 'capped unique should run under sufficient memory')
  await assertRejects(
    () => p.run({ input: 'name,age\nAlice,30\nBob,25\n', memory: '1KB' }),
    /estimated native key-state memory/
  )
  const byteCapped = pipeline([codec.csv(), ops.unique(['name'], { maxStateBytes: 4096 }), codec.csvEncode()])
  const byteCappedResult = await byteCapped.run({ input: 'name,age\nAlice,30\nBob,25\n', memory: '64KB' })
  assert(byteCappedResult.outputText.includes('Alice,30'), 'byte-capped unique should run under sufficient memory')
  await assertRejects(
    () => pipeline([codec.csv(), ops.unique(['name'], { maxStateBytes: 2048 }), codec.csvEncode()]).run({ input: 'name,age\nAlice,30\n' }),
    /max_state_bytes=2048/
  )
  const approxUnique = pipeline([
    codec.csv({ batchSize: 2 }),
    ops.unique(['city'], { mode: 'approx', bloomBytes: 65536, bloomHashes: 4 }),
    codec.csvEncode()
  ])
  const approxResult = await approxUnique.run({ input: 'city,score\nNY,1\nLA,2\nNY,3\nSF,4\nLA,5\n', memory: '128KB' })
  assert(approxResult.outputText.includes('NY,1'), 'approx unique should emit first observed NY')
  assert(approxResult.outputText.includes('LA,2'), 'approx unique should emit first observed LA')
  assert(approxResult.outputText.includes('SF,4'), 'approx unique should emit first observed SF')
  assert(!approxResult.outputText.includes('NY,3'), 'approx unique should suppress duplicate NY')
  assert(approxResult.statsText.includes('"approximate":true'), 'approx unique stats should mark approximate mode')
  assert(approxResult.statsText.includes('"bloom_bytes":65536'), 'approx unique stats should report Bloom bytes')
  await assertRejects(
    () => approxUnique.run({ input: 'city,score\nNY,1\nLA,2\n', memory: '1KB' }),
    /estimated native key-state memory/
  )
  const groupByteCapped = pipeline([
    codec.csv(),
    ops.groupAgg(['city'], [{ column: 'sales', func: 'sum', name: 'total' }], { maxStateBytes: 8192 }),
    codec.csvEncode()
  ])
  const groupByteResult = await groupByteCapped.run({ input: 'city,sales\nNY,1\nLA,2\n', memory: '64KB' })
  assert(groupByteResult.outputText.includes('NY,1'), 'byte-capped groupAgg should run under sufficient memory')
  await assertRejects(
    () => pipeline([
      codec.csv(),
      ops.groupAgg(['city'], [{ column: 'sales', func: 'sum', name: 'total' }], { maxStateBytes: 4096 }),
      codec.csvEncode()
    ]).run({ input: 'city,sales\nNY,1\n' }),
    /max_state_bytes=4096/
  )
  const freqByteCapped = pipeline([codec.csv(), ops.frequency(['city'], { maxStateBytes: 2048 }), codec.csvEncode()])
  const freqByteResult = await freqByteCapped.run({ input: 'city\nNY\nLA\n', memory: '64KB' })
  assert(freqByteResult.outputText.includes('NY,1'), 'byte-capped frequency should run under sufficient memory')
  await assertRejects(
    () => pipeline([codec.csv(), ops.frequency(['city'], { maxStateBytes: 1024 }), codec.csvEncode()]).run({ input: 'city\nNY\n' }),
    /max_state_bytes=1024/
  )
  const rowidByteCapped = pipeline([codec.csv(), ops.rowid(['name'], { maxStateBytes: 2048 }), codec.csvEncode()])
  const rowidByteResult = await rowidByteCapped.run({ input: 'name\nAlice\nBob\n', memory: '64KB' })
  assert(rowidByteResult.outputText.includes('Alice,1'), 'byte-capped rowid should run under sufficient memory')
  await assertRejects(
    () => pipeline([codec.csv(), ops.rowid(['name'], { maxStateBytes: 1024 }), codec.csvEncode()]).run({ input: 'name\nAlice\n' }),
    /max_state_bytes=1024/
  )
  const onehotByteCapped = pipeline([codec.csv(), ops.onehot('city', { maxStateBytes: 2048 }), codec.csvEncode()])
  const onehotByteResult = await onehotByteCapped.run({ input: 'city\nNY\nLA\n', memory: '64KB' })
  assert(onehotByteResult.outputText.includes('city_NY'), 'byte-capped onehot should run under sufficient memory')
  const labelByteCapped = pipeline([codec.csv(), ops.labelEncode('city', { maxStateBytes: 2048 }), codec.csvEncode()])
  const labelByteResult = await labelByteCapped.run({ input: 'city\nNY\nLA\n', memory: '64KB' })
  assert(labelByteResult.outputText.includes('city_encoded'), 'byte-capped labelEncode should run under sufficient memory')
  const uncapped = pipeline([codec.csv(), ops.unique(['name']), codec.csvEncode()])
  await assertRejects(
    () => uncapped.run({ input: 'name,age\nAlice,30\nBob,25\n', memory: '64KB' }),
    /needs max_keys/
  )

  const sortedUnique = pipeline([codec.csv({ batchSize: 1 }), ops.unique(['name'], { sorted: true }), codec.csvEncode()])
  const sortedResult = await sortedUnique.run({ input: 'name,age\nAlice,30\nAlice,31\nBob,25\n', memory: '64KB' })
  assert(sortedResult.outputText.includes('Alice,30'), 'sorted unique should run under memory policy')
  assert(sortedResult.outputText.includes('Bob,25'), 'sorted unique should keep new adjacent key')


  const sortedGroup = pipeline([
    codec.csv({ batchSize: 1 }),
    ops.groupAgg(['name'], [{ column: 'age', func: 'sum', name: 'total' }], { sorted: true }),
    codec.csvEncode(),
  ])
  const sortedGroupResult = await sortedGroup.run({ input: 'name,age\nAlice,30\nAlice,31\nBob,25\n', memory: '64KB' })
  assert(sortedGroupResult.outputText.includes('Alice,61'), 'sorted groupAgg should run under memory policy')
  assert(sortedGroupResult.outputText.includes('Bob,25'), 'sorted groupAgg should emit final adjacent group')


  const sortedPivot = pipeline([
    codec.csv({ batchSize: 1 }),
    ops.pivot('metric', 'value', { agg: 'sum', categories: ['x', 'y'], sorted: true }),
    codec.csvEncode(),
  ])
  const sortedPivotResult = await sortedPivot.run({ input: 'name,metric,value\nA,x,1\nA,y,2\nB,x,3\n', memory: '64KB' })
  const sortedPivotLines = sortedPivotResult.outputText.trim().split('\n')
  assert(sortedPivotLines[0] === 'name,x,y', 'sorted pivot should keep declared schema')
  assert(sortedPivotLines.includes('A,1,2'), 'sorted pivot should emit completed group')
  assert(sortedPivotLines.includes('B,3,'), 'sorted pivot should emit final group')

})

console.log('\nNew Operators:')

await test('tail', async () => {
  const p = pipeline([
    codec.csv(),
    ops.tail(2),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'name,age\nAlice,30\nBob,25\nCharlie,35\n' })
  const text = result.outputText
  assert(text.includes('Bob'), 'should have Bob')
  assert(text.includes('Charlie'), 'should have Charlie')
  assert(!text.includes('Alice'), 'should not have Alice')
})

await test('clip', async () => {
  const p = pipeline([
    codec.csv(),
    ops.clip('age', { min: 26, max: 34 }),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'name,age\nAlice,30\nBob,25\nCharlie,35\n' })
  const text = result.outputText
  assert(text.includes('26'), 'Bob clipped to 26')
  assert(text.includes('34'), 'Charlie clipped to 34')
})

await test('replace', async () => {
  const p = pipeline([
    codec.csv(),
    ops.replace('name', 'Alice', 'Alicia'),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'name,age\nAlice,30\nBob,25\n' })
  const text = result.outputText
  assert(text.includes('Alicia'), 'should have Alicia')
  assert(text.includes('Bob'), 'should have Bob')
})


await test('replace audit side channel', async () => {
  const p = pipeline([
    codec.csv({ batchSize: 2 }),
    ops.replace('name', 'Alice', 'Alicia', { audit: true, auditLimit: 1 }),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'name\nAlice\nAlice Jones\nBob\n' })
  assert(result.outputText.includes('Alicia'), 'first match should be replaced')
  assert(result.outputText.includes('Alicia Jones'), 'second match should be replaced')
  assert(result.statsText.includes('"op":"replace"'), 'replace audit should identify op')
  assert(result.statsText.includes('"event":"value_changed"'), 'replace audit should identify changed value')
  assert(result.statsText.includes('"row":1'), 'replace audit should include first changed row')
  assert(result.statsText.includes('"before":"Alice"'), 'replace audit should include before value')
  assert(result.statsText.includes('"after":"Alicia"'), 'replace audit should include after value')
  assert(!result.statsText.includes('"row":2'), 'replace audit should enforce auditLimit')
})

await test('normalize audit side channel', async () => {
  const p = pipeline([
    codec.csv({ batchSize: 2 }),
    ops.normalize(['x'], { audit: true, auditLimit: 2 }),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'name,x\nA,10\nB,20\nC,30\n', allowBlocking: true })
  assert(result.outputText.includes('A,0'), 'normalize should emit first normalized row')
  assert(result.outputText.includes('B,0.5'), 'normalize should emit middle normalized row')
  assert(result.outputText.includes('C,1'), 'normalize should emit last normalized row')
  assert(result.statsText.includes('"op":"normalize"'), 'normalize audit should identify op')
  assert(result.statsText.includes('"event":"value_changed"'), 'normalize audit should identify changed value')
  assert(result.statsText.includes('"reason":"normalize_minmax"'), 'normalize audit should identify method reason')
  assert(result.statsText.includes('"method":"minmax"'), 'normalize audit should include method')
  assert(result.statsText.includes('"row":1'), 'normalize audit should include first changed row')
  assert(result.statsText.includes('"row":2'), 'normalize audit should include second changed row')
  assert(!result.statsText.includes('"row":3'), 'normalize audit should enforce auditLimit')
  assert(result.statsText.includes('"before":10'), 'normalize audit should include before value')
  assert(result.statsText.includes('"after":0'), 'normalize audit should include after value')
})

await test('trim', async () => {
  const p = pipeline([
    codec.csv(),
    ops.trim(['name']),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'name,age\n  Alice  ,30\n Bob ,25\n' })
  const text = result.outputText
  const lines = text.trim().split('\n')
  assert(lines[1].startsWith('Alice,'), 'Alice should be trimmed')
  assert(lines[2].startsWith('Bob,'), 'Bob should be trimmed')
})

await test('validate', async () => {
  const p = pipeline([
    codec.csv({ batchSize: 1 }),
    ops.validate(expr("col('age') > 27"), { audit: true, auditLimit: 1, maxFailures: 2, name: 'age_check', message: 'age too low' }),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'name,age\nAlice,30\nBob,25\nCara,20\n' })
  const text = result.outputText
  assert(text.includes('_valid'), 'should have _valid column')
  const lines = text.trim().split('\n')
  assert(lines.length === 4, 'should keep all rows')
  assert(result.statsText.includes('"op":"validate"'), 'validate audit should identify op')
  assert(result.statsText.includes('"event":"validation_failed"'), 'validate audit should identify event')
  assert(result.statsText.includes('"reason":"validate_false"'), 'validate audit should include reason')
  assert(result.statsText.includes('"checked_rows":3'), 'validate stats should count checked rows')
  assert(result.statsText.includes('"passed_rows":1'), 'validate stats should count passed rows')
  assert(result.statsText.includes('"failed_rows":2'), 'validate stats should count failed rows')
  assert(result.statsText.includes('"audit_emitted":1'), 'validate stats should count emitted audits')
  assert(result.statsText.includes('"max_failures":2'), 'validate stats should include maxFailures')
  assert(result.statsText.includes('"name":"age_check"'), 'validate audit should include name')
  assert(result.statsText.includes('"message":"age too low"'), 'validate audit should include message')
  assert(result.statsText.includes('"row":2'), 'validate audit should include first invalid row')
  assert(!result.statsText.includes('"row":3'), 'validate audit should enforce auditLimit')

  let failed = false
  try {
    await pipeline([
      codec.csv({ batchSize: 1 }),
      ops.validate(expr("col('age') > 27"), { maxFailures: 0 }),
      codec.csvEncode(),
    ]).run({ input: 'name,age\nAlice,30\nBob,25\n' })
  } catch (err) {
    failed = String(err.message || err).includes('validate max_failures exceeded')
  }
  assert(failed, 'validate maxFailures should fail once the threshold is exceeded')

  const rateStep = ops.validate(expr("col('age') > 25"), { warnFailureRate: 0.5, maxFailureRate: 0.75, name: 'age_rate' })
  assert(rateStep.args.warn_failure_rate === 0.5, 'validate helper should pass warnFailureRate')
  assert(rateStep.args.max_failure_rate === 0.75, 'validate helper should pass maxFailureRate')
  const rateResult = await pipeline([
    codec.csv({ batchSize: 1 }),
    rateStep,
    codec.csvEncode(),
  ]).run({ input: 'name,age\nAlice,30\nBob,20\nCara,10\n' })
  const rateErrors = rateResult.errors.toString('utf-8')
  assert(rateErrors.includes('"event":"threshold_warning"'), 'validate warnFailureRate should emit warning')
  assert(rateErrors.includes('"reason":"warn_failure_rate_exceeded"'), 'validate warning should include reason')
  assert(rateErrors.includes('"warn_failure_rate":0.5'), 'validate warning should include threshold')
  assert(rateResult.statsText.includes('"failure_rate":'), 'validate stats should include failure rate')
  assert(rateResult.statsText.includes('"warn_failure_rate":0.5'), 'validate stats should include warning threshold')
  assert(rateResult.statsText.includes('"max_failure_rate":0.75'), 'validate stats should include max rate threshold')

  failed = false
  try {
    await pipeline([
      codec.csv({ batchSize: 1 }),
      ops.validate(expr("col('age') > 25"), { maxFailureRate: 0.5 }),
      codec.csvEncode(),
    ]).run({ input: 'name,age\nAlice,30\nBob,20\nCara,10\n' })
  } catch (err) {
    failed = String(err.message || err).includes('validate max_failure_rate exceeded')
  }
  assert(failed, 'validate maxFailureRate should fail at finish when exceeded')
})

await test('validate rules', async () => {
  const rules = [
    { name: 'age_positive', expr: "col('age') > 0", message: 'age must be positive' },
    { name: 'age_under_25', expr: "col('age') < 25" },
  ]
  const step = ops.validate(null, { rules, audit: true, auditLimit: 3, maxFailures: 2, name: 'quality' })
  assert(step.args.rules[0].name === 'age_positive', 'validate helper should pass named rules')
  assert(step.args.rules[1].expr === "col('age') < 25", 'validate helper should pass rule expressions')

  const result = await pipeline([
    codec.csv({ batchSize: 1 }),
    step,
    codec.csvEncode(),
  ]).run({ input: 'name,age\nAlice,30\nBob,-1\nCara,20\n' })
  assert(result.outputText.includes('name,age,_valid'), 'validate rules should append _valid')
  assert(result.outputText.includes('Alice,30,false'), 'validate rules should mark first failed row')
  assert(result.outputText.includes('Bob,-1,false'), 'validate rules should mark second failed row')
  assert(result.outputText.includes('Cara,20,true'), 'validate rules should mark passing row')
  assert(result.statsText.includes('"suite":"quality"'), 'validate rule audits should include suite')
  assert(result.statsText.includes('"name":"age_positive"'), 'validate rule audits should include first rule')
  assert(result.statsText.includes('"name":"age_under_25"'), 'validate rule audits should include second rule')
  assert(result.statsText.includes('age must be positive'), 'validate rule audits should include rule message')
  assert(result.statsText.includes('"checked_rows":3'), 'validate rule stats should count checked rows')
  assert(result.statsText.includes('"passed_rows":1'), 'validate rule stats should count passed rows')
  assert(result.statsText.includes('"failed_rows":2'), 'validate rule stats should count failed rows')
  assert(result.statsText.includes('"audit_emitted":2'), 'validate rule stats should count audit records')
  assert(result.statsText.includes('"rule_count":2'), 'validate rule stats should include rule count')
  assert(result.statsText.includes('"max_failures":2'), 'validate rule stats should include maxFailures')
  assert(result.statsText.includes('"rules":['), 'validate rule stats should include per-rule stats')

  const dir = await mkdtemp(join(tmpdir(), 'tranfi-validate-rules-'))
  const rulesPath = join(dir, 'quality_rules.json')
  await writeFile(rulesPath, JSON.stringify({
    name: 'quality_file',
    audit: true,
    audit_limit: 4,
    warn_failure_rate: 0.25,
    rules: [
      { name: 'age_positive', expr: "col('age') > 0", message: 'age must be positive' },
      { name: 'adult', expr: "col('age') >= 18" },
    ],
  }))
  const fileStep = ops.validate(null, { rulesFile: rulesPath })
  assert(fileStep.args.rules_file === rulesPath, 'validate helper should pass rulesFile')
  await assertRejects(
    () => pipeline([
      codec.csv({ batchSize: 1 }),
      fileStep,
      codec.csvEncode(),
    ]).run({ input: 'name,age\nAlice,30\nBob,-1\nCara,20\n' }),
    /allow_fs=false/
  )
  const fileResult = await pipeline([
    codec.csv({ batchSize: 1 }),
    fileStep,
    codec.csvEncode(),
  ]).run({ input: 'name,age\nAlice,30\nBob,-1\nCara,20\n', allowFs: true, allowRulesFile: true })
  assert(fileResult.outputText.includes('Alice,30,true'), 'validate rulesFile should keep passing row')
  assert(fileResult.outputText.includes('Bob,-1,false'), 'validate rulesFile should mark failed row')
  assert(fileResult.statsText.includes('"suite":"quality_file"'), 'validate rulesFile should use suite name')
  assert(fileResult.statsText.includes('age must be positive'), 'validate rulesFile should use rule message')
  assert(fileResult.statsText.includes('"warn_failure_rate":0.25'), 'validate rulesFile should use file threshold')
  assert(fileResult.errors.toString('utf-8').includes('"event":"threshold_warning"'), 'validate rulesFile should emit file warning threshold')
  await rm(dir, { recursive: true, force: true })
})


await test('quarantine', async () => {
  const data = 'name,age\nAlice,30\nBob,20\nCara,10\nDana,40\n'
  const helper = ops.quarantine(expr("col('age') < 25"), { name: 'young', message: 'too young' })
  assert(helper.op === 'quarantine', 'quarantine helper should set op')
  assert(helper.args.expr === "col('age') < 25", 'quarantine helper should set expr')
  assert(helper.args.name === 'young', 'quarantine helper should set name')
  assert(helper.args.message === 'too young', 'quarantine helper should set message')

  const result = await pipeline([
    codec.csv({ batchSize: 1 }),
    helper,
    codec.csvEncode(),
  ]).run({ input: data })
  assert(result.outputText.includes('Alice,30'), 'quarantine should keep Alice')
  assert(result.outputText.includes('Dana,40'), 'quarantine should keep Dana')
  assert(!result.outputText.includes('Bob'), 'quarantine should drop Bob from main')
  assert(!result.outputText.includes('Cara'), 'quarantine should drop Cara from main')
  const errors = result.errors.toString('utf-8')
  assert(errors.includes('"type":"quarantine"'), 'quarantine should emit quarantine records')
  assert(errors.includes('"event":"row_quarantined"'), 'quarantine should identify event')
  assert(errors.includes('"reason":"predicate_true"'), 'quarantine should identify predicate match')
  assert(errors.includes('"name":"young"'), 'quarantine should include name')
  assert(errors.includes('too young'), 'quarantine should include message')
  assert(errors.includes('"row":2'), 'quarantine should include first row')
  assert(errors.includes('"row":3'), 'quarantine should include second row')
  assert(errors.includes('"Bob"'), 'quarantine should include Bob data')
  assert(errors.includes('"Cara"'), 'quarantine should include Cara data')
  assert(result.statsText.includes('"op":"quarantine"'), 'quarantine stats should identify op')
  assert(result.statsText.includes('"checked_rows":4'), 'quarantine stats should count checked rows')
  assert(result.statsText.includes('"kept_rows":2'), 'quarantine stats should count kept rows')
  assert(result.statsText.includes('"quarantined_rows":2'), 'quarantine stats should count quarantined rows')
})

await test('assert actions', async () => {
  const data = 'name,age\nAlice,30\nBob,20\nCara,40\n'
  const helper = ops.assert(expr("col('age') > 25"), {
    action: 'quarantine',
    name: 'age_check',
    message: 'age too low',
    audit: true,
    auditLimit: 1,
  })
  assert(helper.op === 'assert', 'assert helper should set op')
  assert(helper.args.action === 'quarantine', 'assert helper should set action')
  assert(helper.args.name === 'age_check', 'assert helper should set name')
  assert(helper.args.audit === true, 'assert helper should set audit')
  assert(helper.args.audit_limit === 1, 'assert helper should set audit_limit')

  const annotated = await pipeline([
    codec.csv(),
    ops.assert(expr("col('age') > 25"), { action: 'annotate', result: 'age_ok' }),
    codec.csvEncode(),
  ]).run({ input: data })
  assert(annotated.outputText.includes('name,age,age_ok'), 'annotate should append result column')
  assert(annotated.outputText.includes('Alice,30,true'), 'annotate should mark passing rows')
  assert(annotated.outputText.includes('Bob,20,false'), 'annotate should mark failing rows')

  const filtered = await pipeline([
    codec.csv({ batchSize: 1 }),
    ops.assert(expr("col('age') > 25"), { action: 'filter', name: 'age_check', message: 'age too low', audit: true, auditLimit: 1 }),
    codec.csvEncode(),
  ]).run({ input: data })
  assert(filtered.outputText.includes('Alice'), 'filter should keep Alice')
  assert(filtered.outputText.includes('Cara'), 'filter should keep Cara')
  assert(!filtered.outputText.includes('Bob'), 'filter should drop Bob')
  assert(filtered.errors.length === 0, 'filter action should not write errors')
  assert(filtered.statsText.includes('"type":"audit"'), 'assert filter audit should emit audit record')
  assert(filtered.statsText.includes('"op":"assert"'), 'assert filter audit should identify assert op')
  assert(filtered.statsText.includes('"action":"filter"'), 'assert filter audit should include action')
  assert(filtered.statsText.includes('age_check'), 'assert filter audit should include name')
  assert(filtered.statsText.includes('age too low'), 'assert filter audit should include message')
  assert(filtered.statsText.includes('"Bob"'), 'assert filter audit should include dropped row data')

  const quarantined = await pipeline([
    codec.csv(),
    ops.assert(expr("col('age') > 25"), { action: 'quarantine', name: 'age_check', message: 'age too low' }),
    codec.csvEncode(),
  ]).run({ input: data })
  assert(!quarantined.outputText.includes('Bob'), 'quarantine should drop Bob from main output')
  const errors = quarantined.errors.toString('utf-8')
  assert(errors.includes('assert_failure'), 'quarantine should emit assert failure')
  assert(errors.includes('age_check'), 'quarantine should include rule name')
  assert(errors.includes('age too low'), 'quarantine should include message')
  assert(errors.includes('\"Bob\"'), 'quarantine should include failing row data')

  await assertRejects(
    () => pipeline([
      codec.csv(),
      ops.assert(expr("col('age') > 25"), { action: 'fail', message: 'age rule' }),
      codec.csvEncode(),
    ]).run({ input: data }),
    /assert failed at row 2/
  )

  const aggregateHelper = ops.assert(null, {
    aggregate: 'sum:amount',
    op: '>=',
    value: 100,
    action: 'warn',
    name: 'sales_total',
    message: 'too low',
    tolerance: 1e-9,
    rel: false,
  })
  assert(aggregateHelper.args.aggregate === 'sum:amount', 'aggregate assert helper should set aggregate')
  assert(aggregateHelper.args.op === '>=', 'aggregate assert helper should set comparison')
  assert(aggregateHelper.args.value === 100, 'aggregate assert helper should set value')
  assert(aggregateHelper.args.tolerance === 1e-9, 'aggregate assert helper should set tolerance')
  assert(aggregateHelper.args.rel === false, 'aggregate assert helper should set rel')

  const amountData = 'name,amount\nA,30\nB,20\n'
  const aggregateWarn = await pipeline([
    codec.csv({ batchSize: 1 }),
    ops.assert(null, { aggregate: 'sum:amount', op: '>=', value: 100, action: 'warn', name: 'sales_total', message: 'too low' }),
    codec.csvEncode(),
  ]).run({ input: amountData })
  assert(aggregateWarn.outputText.includes('A,30'), 'aggregate assert should pass through first row')
  assert(aggregateWarn.outputText.includes('B,20'), 'aggregate assert should pass through second row')
  const aggregateErrors = aggregateWarn.errors.toString('utf-8')
  assert(aggregateErrors.includes('aggregate_assert_failed'), 'aggregate assert warning should emit failure record')
  assert(aggregateErrors.includes('"severity":"warning"'), 'aggregate assert warning should be nonfatal')
  assert(aggregateErrors.includes('sales_total'), 'aggregate assert warning should include name')
  assert(aggregateErrors.includes('"actual":50'), 'aggregate assert warning should include actual value')
  assert(aggregateWarn.statsText.includes('"memory_class":"bounded_state"'), 'aggregate assert metadata should be bounded')
  assert(aggregateWarn.statsText.includes('"state_estimate":"O(1) aggregate counters"'), 'aggregate assert should expose state estimate')
  assert(aggregateWarn.statsText.includes('"assert_mode":"aggregate"'), 'aggregate assert stats should include mode')
  assert(aggregateWarn.statsText.includes('"aggregate":"sum"'), 'aggregate assert stats should include aggregate')
  assert(aggregateWarn.statsText.includes('"aggregate_column":"amount"'), 'aggregate assert stats should include column')
  assert(aggregateWarn.statsText.includes('"aggregate_value":50'), 'aggregate assert stats should include value')
  assert(aggregateWarn.statsText.includes('"aggregate_passed":false'), 'aggregate assert stats should include pass/fail')

  const rateData = 'name,score\nA,10\nB,\nC,30\n'
  const missingRateWarn = await pipeline([
    codec.csv({ batchSize: 1 }),
    ops.assert(null, { aggregate: 'missing_rate:score', op: '<=', value: 0.25, action: 'warn', name: 'score_missing_rate' }),
    codec.csvEncode(),
  ]).run({ input: rateData })
  assert(missingRateWarn.outputText.includes('A,10'), 'missing-rate assert should pass through rows')
  const rateErrors = missingRateWarn.errors.toString('utf-8')
  assert(rateErrors.includes('aggregate_assert_failed'), 'missing-rate warning should emit failure')
  assert(rateErrors.includes('"aggregate":"missing_rate"'), 'missing-rate failure should name aggregate')
  assert(rateErrors.includes('"column":"score"'), 'missing-rate failure should name column')
  assert(missingRateWarn.statsText.includes('"aggregate":"missing_rate"'), 'missing-rate stats should name aggregate')
  assert(missingRateWarn.statsText.includes('"aggregate_column":"score"'), 'missing-rate stats should name column')
  assert(missingRateWarn.statsText.includes('"aggregate_rows":3'), 'missing-rate stats should count rows')
  assert(missingRateWarn.statsText.includes('"aggregate_non_null":2'), 'missing-rate stats should count non-null')
  assert(missingRateWarn.statsText.includes('"aggregate_missing":1'), 'missing-rate stats should count missing')
  assert(missingRateWarn.statsText.includes('"aggregate_passed":false'), 'missing-rate stats should fail')

  const completeRatePass = await pipeline([
    codec.csv({ batchSize: 1 }),
    ops.assert(null, { aggregate: 'complete_rate:score', op: '>=', value: 0.66, action: 'fail', name: 'score_complete_rate' }),
    codec.csvEncode(),
  ]).run({ input: rateData })
  assert(completeRatePass.errors.length === 0, 'complete-rate assert should pass')
  assert(completeRatePass.statsText.includes('"aggregate":"complete_rate"'), 'complete-rate stats should name aggregate')
  assert(completeRatePass.statsText.includes('"aggregate_passed":true'), 'complete-rate stats should pass')

  await assertRejects(
    () => pipeline([
      codec.csv(),
      ops.assert(null, { aggregate: 'count', op: '>=', value: 3, action: 'fail' }),
      codec.csvEncode(),
    ]).run({ input: amountData }),
    /assert aggregate failed/
  )
  const largeAmountData = 'amount\n1000000000000000\n'
  const relativePass = await pipeline([
    codec.csv(),
    ops.assert(null, { aggregate: 'sum:amount', op: '==', value: 1000000000000100, action: 'fail' }),
    codec.csvEncode(),
  ]).run({ input: largeAmountData })
  assert(relativePass.statsText.includes('"aggregate_passed":true'), 'relative aggregate equality should pass')
  assert(relativePass.statsText.includes('"relative_tolerance":true'), 'relative aggregate equality should report relative tolerance')
  assert(relativePass.errors.length === 0, 'relative aggregate equality should not warn')

  const absoluteWarn = await pipeline([
    codec.csv(),
    ops.assert(null, { aggregate: 'sum:amount', op: '==', value: 1000000000000100, tolerance: 1e-12, rel: false, action: 'warn' }),
    codec.csvEncode(),
  ]).run({ input: largeAmountData })
  assert(absoluteWarn.errors.toString('utf-8').includes('aggregate_assert_failed'), 'absolute aggregate equality should warn')
  assert(absoluteWarn.statsText.includes('"relative_tolerance":false'), 'absolute aggregate equality should report rel=false')

  const whitespaceValue = await pipeline([
    codec.csv(),
    { op: 'assert', args: { aggregate: 'sum:amount', op: '==', value: '50 ', action: 'fail' } },
    codec.csvEncode(),
  ]).run({ input: amountData })
  assert(whitespaceValue.statsText.includes('"aggregate_passed":true'), 'string aggregate threshold with trailing whitespace should parse')
})


await test('schema actions', async () => {
  const data = 'name,age,city\nAlice,30,NY\nBob,200,SF\nCara,,LA\n'
  const schemaStep = (mode, extra = {}) => ops.schema({
    columns: { name: 'string', age: 'int', city: 'string' },
    nonNull: ['name', 'age'],
    values: { city: ['NY', 'LA'] },
    min: { age: 0 },
    max: { age: 120 },
    mode,
    ...extra,
  })

  const helper = schemaStep('quarantine', { name: 'schema_check', message: 'schema rule' })
  assert(helper.op === 'schema', 'schema helper should set op')
  assert(helper.args.mode === 'quarantine', 'schema helper should set mode')
  assert(helper.args.action === 'quarantine', 'schema helper should set action')
  assert(helper.args.columns.age === 'int', 'schema helper should pass columns')
  assert(helper.args.non_null.length === 2, 'schema helper should pass nonNull as non_null')
  assert(helper.args.values.city[0] === 'NY', 'schema helper should pass values')
  assert(helper.args.max.age === 120, 'schema helper should pass max')
  assert(helper.args.name === 'schema_check', 'schema helper should pass name')

  const baseline = {
    columns: { name: 'string', age: 'int', city: 'string' },
    values: { city: ['NY', 'LA'] },
  }
  const baselineStep = ops.schema({ baseline, mode: 'warn', name: 'delivery_baseline' })
  assert(baselineStep.args.baseline.columns.age === 'int', 'schema helper should pass baseline object')

  const drifted = await pipeline([
    codec.csv({ batchSize: 1 }),
    baselineStep,
    codec.csvEncode(),
  ]).run({ input: 'name,age,city,extra\nAlice,30,NY,x\nBob,31,SF,y\n' })
  assert(drifted.outputText.includes('Alice,30,NY,x'), 'baseline warning should preserve main rows')
  const driftErrors = drifted.errors.toString('utf-8')
  assert(driftErrors.includes('"name":"delivery_baseline"'), 'baseline drift should include check name')
  assert(driftErrors.includes('"rule":"extra_column"'), 'baseline drift should report extra columns')
  assert(driftErrors.includes('"column":"extra"'), 'baseline drift should report extra column name')
  assert(driftErrors.includes('"rule":"values"'), 'baseline drift should report new categories')
  assert(driftErrors.includes('"actual":"SF"'), 'baseline drift should report new category value')
  assert(driftErrors.includes('"rule":"missing_category"'), 'baseline drift should report missing expected categories')
  assert(driftErrors.includes('"expected":"LA"'), 'baseline drift should report missing category value')
  assert(drifted.statsText.includes('"baseline_mode":true'), 'baseline stats should identify baseline mode')
  assert(drifted.statsText.includes('"extra_column_failures":1'), 'baseline stats should count extra columns')
  assert(drifted.statsText.includes('"missing_category_failures":1'), 'baseline stats should count missing categories')

  const annotated = await pipeline([
    codec.csv(),
    schemaStep('annotate', { result: 'schema_ok' }),
    codec.csvEncode(),
  ]).run({ input: data })
  assert(annotated.outputText.includes('name,age,city,schema_ok'), 'annotate should append result column')
  assert(annotated.outputText.includes('Alice,30,NY,true'), 'annotate should mark passing rows')
  assert(annotated.outputText.includes('Bob,200,SF,false'), 'annotate should mark range failure')
  assert(annotated.outputText.includes('Cara,,LA,false'), 'annotate should mark null failure')

  const filtered = await pipeline([
    codec.csv({ batchSize: 1 }),
    schemaStep('filter', { audit: true, auditLimit: 1, name: 'schema_check', message: 'schema rule' }),
    codec.csvEncode(),
  ]).run({ input: data })
  assert(filtered.outputText.includes('Alice'), 'filter should keep valid row')
  assert(!filtered.outputText.includes('Bob'), 'filter should drop range failure')
  assert(!filtered.outputText.includes('Cara'), 'filter should drop null failure')
  assert(filtered.errors.length === 0, 'filter action should not write errors')
  assert(filtered.statsText.includes('"type":"audit"'), 'schema audit should emit audit record')
  assert(filtered.statsText.includes('"op":"schema"'), 'schema audit should identify op')
  assert(filtered.statsText.includes('"event":"row_dropped"'), 'schema audit should identify dropped row event')
  assert(filtered.statsText.includes('"reason":"schema_failed"'), 'schema audit should identify schema failure')
  assert(filtered.statsText.includes('schema_check'), 'schema audit should include rule name')
  assert(filtered.statsText.includes('schema rule'), 'schema audit should include message')
  assert(filtered.statsText.includes('"rule":"max"'), 'schema audit should include first failed rule')
  assert(filtered.statsText.includes('"column":"age"'), 'schema audit should include failed column')
  assert(filtered.statsText.includes('"row":2'), 'schema audit should include first dropped row index')
  assert(filtered.statsText.includes('"Bob"'), 'schema audit should include first dropped row')
  assert(!filtered.statsText.includes('"Cara"'), 'schema audit should enforce auditLimit')
  assert(filtered.statsText.includes('"checked_rows":3'), 'schema stats should count checked rows')
  assert(filtered.statsText.includes('"passed_rows":1'), 'schema stats should count passed rows')
  assert(filtered.statsText.includes('"failed_rows":2'), 'schema stats should count failed rows')
  assert(filtered.statsText.includes('"violation_count":3'), 'schema stats should count all row-local violations')
  assert(filtered.statsText.includes('"max_failures":1'), 'schema stats should count max failures')
  assert(filtered.statsText.includes('"values_failures":1'), 'schema stats should count values failures')
  assert(filtered.statsText.includes('"nullable_failures":1'), 'schema stats should count nullable failures')
  assert(filtered.statsText.includes('"audit_emitted":1'), 'schema stats should count audit records')

  const quarantined = await pipeline([
    codec.csv(),
    schemaStep('quarantine', { name: 'schema_check', message: 'schema rule' }),
    codec.csvEncode(),
  ]).run({ input: data })
  assert(quarantined.outputText.includes('Alice'), 'quarantine should keep valid row')
  assert(!quarantined.outputText.includes('Bob'), 'quarantine should drop range failure')
  assert(!quarantined.outputText.includes('Cara'), 'quarantine should drop null failure')
  const errors = quarantined.errors.toString('utf-8')
  assert(errors.includes('schema_failure'), 'quarantine should emit schema failure')
  assert(errors.includes('schema_check'), 'quarantine should include rule name')
  assert(errors.includes('schema rule'), 'quarantine should include message')
  assert(errors.includes('"severity":"error"'), 'quarantine should emit error severity')
  assert(errors.includes('"rule":"max"'), 'quarantine should include max rule')
  assert(errors.includes('"rule":"values"'), 'quarantine should include values rule')
  assert(errors.includes('"rule":"nullable"'), 'quarantine should include nullable rule')
  assert(errors.includes('"Bob"'), 'quarantine should include failing row data')
  assert(errors.includes('"Cara"'), 'quarantine should include null row data')
  assert(quarantined.statsText.includes('"checked_rows":3'), 'schema quarantine stats should count checked rows')
  assert(quarantined.statsText.includes('"failed_rows":2'), 'schema quarantine stats should count failed rows')
  assert(quarantined.statsText.includes('"violation_count":3'), 'schema quarantine stats should count violations')

  const warned = await pipeline([
    codec.csv(),
    schemaStep('warn'),
    codec.csvEncode(),
  ]).run({ input: data })
  assert(warned.outputText.includes('Alice'), 'warn should keep valid row')
  assert(warned.outputText.includes('Bob'), 'warn should keep failing row')
  assert(warned.outputText.includes('Cara'), 'warn should keep null row')
  const warnErrors = warned.errors.toString('utf-8')
  assert(warnErrors.includes('"severity":"warning"'), 'warn should emit warnings')
  assert(warnErrors.includes('"rule":"values"'), 'warn should emit all row-local rule failures')
  assert(warned.statsText.includes('"violation_count":3'), 'warn stats should count all row-local violations')

  await assertRejects(
    () => pipeline([
      codec.csv(),
      ops.schema({ columns: { name: 'string', age: 'int' }, max: { age: 120 }, mode: 'fail', message: 'schema rule' }),
      codec.csvEncode(),
    ]).run({ input: 'name,age\nAlice,30\nBob,200\n' }),
    /schema failed at row 2: max/
  )
})

await test('schema selectors', async () => {
  const data = 'id,score_a,score_b,code,note\n1,10,20,AA,x\n2,,200,bad,y\n'
  const step = ops.schema({
    columns: { 'starts_with(score_)': 'number', 'ends_with(code)': 'string' },
    nonNull: ['starts_with(score_)'],
    min: { 'where(number)': 0 },
    max: { 'starts_with(score_)': 100 },
    regex: { 'ends_with(code)': '^[A-Z]+$' },
    mode: 'warn',
  })
  assert(step.args.columns['starts_with(score_)'] === 'number', 'schema helper should pass selector columns')
  assert(step.args.non_null[0] === 'starts_with(score_)', 'schema helper should pass selector nonNull')
  assert(step.args.min['where(number)'] === 0, 'schema helper should pass selector min')
  assert(step.args.regex['ends_with(code)'] === '^[A-Z]+$', 'schema helper should pass selector regex')

  const result = await pipeline([
    codec.csv({ batchSize: 1 }),
    step,
    codec.csvEncode(),
  ]).run({ input: data })
  assert(result.outputText.includes('id,score_a,score_b,code,note'), 'schema selectors should keep header')
  assert(result.outputText.includes('1,10,20,AA,x'), 'schema selectors should keep passing row')
  assert(result.outputText.includes('2,,200,bad,y'), 'warn mode should keep failing row')
  const errors = result.errors.toString('utf-8')
  assert(errors.includes('schema_failure'), 'schema selectors should emit warnings')
  assert(errors.includes('"column":"score_a"'), 'schema selectors should flag score_a')
  assert(errors.includes('"rule":"nullable"'), 'schema selectors should flag nullability')
  assert(errors.includes('"column":"score_b"'), 'schema selectors should flag score_b')
  assert(errors.includes('"rule":"max"'), 'schema selectors should flag max')
  assert(errors.includes('"column":"code"'), 'schema selectors should flag code')
  assert(errors.includes('"rule":"regex"'), 'schema selectors should flag regex')
  assert(result.statsText.includes('"checked_rows":2'), 'schema selector stats should count checked rows')
  assert(result.statsText.includes('"passed_rows":1'), 'schema selector stats should count passed rows')
  assert(result.statsText.includes('"failed_rows":1'), 'schema selector stats should count failed rows')
  assert(result.statsText.includes('"violation_count":3'), 'schema selector stats should count all violations')
  assert(result.statsText.includes('"nullable_failures":1'), 'schema selector stats should count nullable failures')
  assert(result.statsText.includes('"max_failures":1'), 'schema selector stats should count max failures')
  assert(result.statsText.includes('"regex_failures":1'), 'schema selector stats should count regex failures')
})


await test('schema regex budgets', async () => {
  const step = ops.schema({
    regex: { code: '^[A-Z]+$' },
    maxRegexPatternBytes: 32,
    maxRegexCellBytes: 3,
    mode: 'warn',
  })
  assert(step.args.max_regex_pattern_bytes === 32, 'schema helper should pass maxRegexPatternBytes')
  assert(step.args.max_regex_cell_bytes === 3, 'schema helper should pass maxRegexCellBytes')

  const result = await pipeline([
    codec.csv({ batchSize: 1 }),
    step,
    codec.csvEncode(),
  ]).run({ input: 'code\nAA\nTOOLONG\n' })
  assert(result.outputText.includes('AA'), 'schema regex budget should keep passing row in warn mode')
  assert(result.outputText.includes('TOOLONG'), 'schema regex budget should keep failing row in warn mode')
  const errors = result.errors.toString('utf-8')
  assert(errors.includes('schema_failure'), 'over-cap regex cell should emit schema failure')
  assert(errors.includes('"rule":"regex"'), 'over-cap regex cell should count as regex failure')
  assert(errors.includes('regex cell within max_regex_cell_bytes'), 'over-cap regex cell should report expected cap')
  assert(errors.includes('cell exceeds 3 bytes'), 'over-cap regex cell should report actual cap failure')
  assert(result.statsText.includes('"checked_rows":2'), 'schema regex budget stats should count rows')
  assert(result.statsText.includes('"passed_rows":1'), 'schema regex budget stats should count passing row')
  assert(result.statsText.includes('"failed_rows":1'), 'schema regex budget stats should count failing row')
  assert(result.statsText.includes('"regex_failures":1'), 'schema regex budget stats should count regex failures')
  assert(result.statsText.includes('"max_regex_pattern_bytes":32'), 'schema regex budget stats should report pattern cap')
  assert(result.statsText.includes('"max_regex_cell_bytes":3'), 'schema regex budget stats should report cell cap')

  await assertRejects(
    () => pipeline([
      codec.csv(),
      ops.schema({ regex: { code: '^[A-Z]+$' }, maxRegexPatternBytes: 3, mode: 'warn' }),
      codec.csvEncode(),
    ]).run({ input: 'code\nAA\n' }),
    /max_regex_pattern_bytes/
  )

  await assertRejects(
    () => pipeline([
      codec.csv(),
      ops.schema({ regex: { 'ends_with(code)': '^[A-Z]+$' }, maxRegexPatternBytes: 3, mode: 'warn' }),
      codec.csvEncode(),
    ]).run({ input: 'code\nAA\n' }),
    /max_regex_pattern_bytes/
  )
})


await test('schema audit privacy controls', async () => {
  const data = 'name,ssn,age\nAliciaSecret,111-22-3333,200\n'
  const step = ops.schema({
    values: { ssn: ['OK'] },
    mode: 'filter',
    audit: true,
    auditLimit: 1,
    auditIncludeRow: true,
    auditColumns: ['ssn'],
    auditRedact: ['ssn'],
    auditHashColumns: ['name'],
    auditMaxBytes: 1024,
    auditMaxCellBytes: 3,
  })
  assert(step.args.audit_include_row === true, 'schema helper should pass auditIncludeRow')
  assert(step.args.audit_columns[0] === 'ssn', 'schema helper should pass auditColumns')
  assert(step.args.audit_redact[0] === 'ssn', 'schema helper should pass auditRedact')
  assert(step.args.audit_hash_columns[0] === 'name', 'schema helper should pass auditHashColumns')
  assert(step.args.audit_max_bytes === 1024, 'schema helper should pass auditMaxBytes')
  assert(step.args.audit_max_cell_bytes === 3, 'schema helper should pass auditMaxCellBytes')

  let result = await pipeline([
    codec.csv({ batchSize: 1 }),
    ops.schema({ max: { age: 120 }, mode: 'filter', audit: true, auditLimit: 1, auditIncludeRow: false }),
    codec.csvEncode(),
  ]).run({ input: data })
  assert(result.statsText.includes('"type":"audit"'), 'schema audit should emit audit record')
  assert(!result.statsText.includes('"data"'), 'auditIncludeRow=false should omit row payload')
  assert(!result.statsText.includes('AliciaSecret'), 'auditIncludeRow=false should not leak name')
  assert(!result.statsText.includes('111-22-3333'), 'auditIncludeRow=false should not leak ssn')

  result = await pipeline([
    codec.csv({ batchSize: 1 }),
    ops.schema({ values: { ssn: ['OK'] }, mode: 'filter', audit: true, auditLimit: 1, auditColumns: ['ssn'], auditRedact: ['ssn'] }),
    codec.csvEncode(),
  ]).run({ input: data })
  assert(result.statsText.includes('[REDACTED]'), 'auditRedact should redact selected column')
  assert(!result.statsText.includes('111-22-3333'), 'auditRedact should not leak raw ssn')
  assert(!result.statsText.includes('AliciaSecret'), 'auditColumns should limit row payload columns')

  result = await pipeline([
    codec.csv({ batchSize: 1 }),
    ops.schema({ values: { ssn: ['OK'] }, mode: 'filter', audit: true, auditLimit: 1, auditHashColumns: ['ssn'] }),
    codec.csvEncode(),
  ]).run({ input: data })
  assert(result.statsText.includes('fnv1a64:'), 'auditHashColumns should hash selected column')
  assert(!result.statsText.includes('111-22-3333'), 'auditHashColumns should not leak raw ssn')

  result = await pipeline([
    codec.csv({ batchSize: 1 }),
    ops.schema({ max: { age: 120 }, mode: 'filter', audit: true, auditLimit: 1, auditMaxCellBytes: 3 }),
    codec.csvEncode(),
  ]).run({ input: data })
  assert(result.statsText.includes('Ali...'), 'auditMaxCellBytes should truncate string cells')
  assert(result.statsText.includes('111...'), 'auditMaxCellBytes should truncate string cells')
  assert(!result.statsText.includes('AliciaSecret'), 'auditMaxCellBytes should not leak full name')
  assert(!result.statsText.includes('111-22-3333'), 'auditMaxCellBytes should not leak full ssn')

  result = await pipeline([
    codec.csv({ batchSize: 1 }),
    ops.schema({ max: { age: 120 }, mode: 'filter', audit: true, auditLimit: 1, auditMaxBytes: 1 }),
    codec.csvEncode(),
  ]).run({ input: data })
  assert(result.statsText.includes('_audit_truncated'), 'auditMaxBytes should replace oversized row payloads')
  assert(!result.statsText.includes('AliciaSecret'), 'auditMaxBytes should not leak name')
  assert(!result.statsText.includes('111-22-3333'), 'auditMaxBytes should not leak ssn')

  result = await pipeline([
    codec.csv({ batchSize: 1 }),
    ops.schema({ regex: { ssn: '^OK$' }, mode: 'warn', auditRedact: ['ssn'] }),
    codec.csvEncode(),
  ]).run({ input: data })
  const errors = result.errors.toString('utf-8')
  assert(errors.includes('schema_failure'), 'warn mode should emit schema failure')
  assert(errors.includes('[REDACTED]'), 'auditRedact should redact error actual values')
  assert(!errors.includes('111-22-3333'), 'auditRedact should not leak raw ssn in errors')
})


await test('audit privacy migrated producers', async () => {
  const data = 'name,ssn,age\nAlice,111-22-3333,20\nBob,222-33-4444,40\n'

  const filterStep = ops.filter(expr("col('age') > 30"), {
    audit: true,
    auditLimit: 1,
    auditIncludeRow: false,
    auditRedact: ['ssn'],
    auditMaxCellBytes: 3,
  })
  assert(filterStep.args.audit_include_row === false, 'filter helper should pass auditIncludeRow')
  assert(filterStep.args.audit_redact[0] === 'ssn', 'filter helper should pass auditRedact')
  assert(filterStep.args.audit_max_cell_bytes === 3, 'filter helper should pass auditMaxCellBytes')
  let result = await pipeline([
    codec.csv({ batchSize: 1 }),
    filterStep,
    codec.csvEncode(),
  ]).run({ input: data })
  assert(result.statsText.includes('"op":"filter"'), 'filter audit should emit')
  assert(!result.statsText.includes('"data"'), 'filter auditIncludeRow=false should omit data')
  assert(!result.statsText.includes('Alice'), 'filter audit should not leak omitted row name')
  assert(!result.statsText.includes('111-22-3333'), 'filter audit should not leak omitted ssn')

  const validateStep = ops.validate(expr("col('age') > 30"), {
    audit: true,
    auditLimit: 1,
    auditColumns: ['ssn'],
    auditRedact: ['ssn'],
  })
  assert(validateStep.args.audit_columns[0] === 'ssn', 'validate helper should pass auditColumns')
  assert(validateStep.args.audit_redact[0] === 'ssn', 'validate helper should pass auditRedact')
  result = await pipeline([
    codec.csv({ batchSize: 1 }),
    validateStep,
    codec.csvEncode(),
  ]).run({ input: data })
  assert(result.statsText.includes('"op":"validate"'), 'validate audit should emit')
  assert(result.statsText.includes('[REDACTED]'), 'validate audit should redact selected column')
  assert(!result.statsText.includes('111-22-3333'), 'validate audit should not leak raw ssn')
  assert(!result.statsText.includes('Alice'), 'validate auditColumns should limit payload')

  const assertStep = ops.assert(expr("col('age') > 30"), {
    action: 'warn',
    auditColumns: ['ssn'],
    auditHashColumns: ['ssn'],
  })
  assert(assertStep.args.audit_columns[0] === 'ssn', 'assert helper should pass auditColumns')
  assert(assertStep.args.audit_hash_columns[0] === 'ssn', 'assert helper should pass auditHashColumns')
  result = await pipeline([
    codec.csv({ batchSize: 1 }),
    assertStep,
    codec.csvEncode(),
  ]).run({ input: data })
  const errors = result.errors.toString('utf-8')
  assert(errors.includes('assert_failure'), 'assert warning should emit failure record')
  assert(errors.includes('fnv1a64:'), 'assert errors should hash selected column')
  assert(!errors.includes('111-22-3333'), 'assert errors should not leak raw ssn')
  assert(!errors.includes('Alice'), 'assert auditColumns should limit row payload')

  const jsonStep = ops.jsonSchema(
    { type: 'object', required: ['user'] },
    { mode: 'filter', audit: true, auditLimit: 1, auditColumns: ['_line'], auditRedact: ['_line'], auditMaxBytes: 2048 }
  )
  assert(jsonStep.args.audit_columns[0] === '_line', 'jsonSchema helper should pass auditColumns')
  assert(jsonStep.args.audit_redact[0] === '_line', 'jsonSchema helper should pass auditRedact')
  assert(jsonStep.args.audit_max_bytes === 2048, 'jsonSchema helper should pass auditMaxBytes')
  result = await pipeline([
    codec.text({ batchSize: 1 }),
    jsonStep,
    codec.textEncode(),
  ]).run({ input: '{"ssn":"111-22-3333"}\n' })
  assert(result.statsText.includes('"op":"json-schema"'), 'jsonSchema audit should emit')
  assert(result.statsText.includes('[REDACTED]'), 'jsonSchema audit should redact selected row text')
  assert(!result.statsText.includes('111-22-3333'), 'jsonSchema audit should not leak raw JSON text')

  const fillStep = ops.fillNull({ note: 'SECRET' }, {
    audit: true,
    auditLimit: 1,
    auditColumns: ['note'],
    auditRedact: ['note'],
  })
  assert(fillStep.args.audit_columns[0] === 'note', 'fillNull helper should pass auditColumns')
  assert(fillStep.args.audit_redact[0] === 'note', 'fillNull helper should pass auditRedact')
  result = await pipeline([
    codec.csv({ batchSize: 1, nulls: ['NA'] }),
    fillStep,
    codec.csvEncode(),
  ]).run({ input: 'name,note\nB,NA\nC,ok\n' })
  assert(result.statsText.includes('"op":"fill-null"'), 'fill-null audit should emit')
  assert(result.statsText.includes('[REDACTED]'), 'fill-null audit should redact filled value')
  assert(!result.statsText.includes('SECRET'), 'fill-null audit should not leak filled value')
  assert(!result.statsText.includes('"B"'), 'fill-null auditColumns should limit row payload')

  const replaceStep = ops.replace('ssn', '111', '999', {
    audit: true,
    auditLimit: 1,
    auditColumns: ['ssn'],
    auditRedact: ['ssn'],
  })
  assert(replaceStep.args.audit_columns[0] === 'ssn', 'replace helper should pass auditColumns')
  assert(replaceStep.args.audit_redact[0] === 'ssn', 'replace helper should pass auditRedact')
  result = await pipeline([
    codec.csv({ batchSize: 1 }),
    replaceStep,
    codec.csvEncode(),
  ]).run({ input: 'name,ssn\nAlice,111-22-3333\nBob,222-33-4444\n' })
  assert(result.statsText.includes('"op":"replace"'), 'replace audit should emit')
  assert(result.statsText.includes('[REDACTED]'), 'replace audit should redact changed values')
  assert(!result.statsText.includes('111'), 'replace audit should not leak raw pattern/source fragment')
  assert(!result.statsText.includes('999'), 'replace audit should not leak raw replacement fragment')
  assert(!result.statsText.includes('Alice'), 'replace auditColumns should limit row payload')


  const castStep = ops.cast({ secret: 'int' }, {
    onError: 'null',
    audit: true,
    auditLimit: 1,
    auditColumns: ['secret'],
    auditRedact: ['secret'],
  })
  assert(castStep.args.on_error === 'null', 'cast helper should pass onError')
  assert(castStep.args.audit_columns[0] === 'secret', 'cast helper should pass auditColumns')
  assert(castStep.args.audit_redact[0] === 'secret', 'cast helper should pass auditRedact')
  result = await pipeline([
    codec.csv({ batchSize: 1 }),
    castStep,
    codec.csvEncode(),
  ]).run({ input: 'name,secret\nAlice,111-22-3333\n' })
  assert(result.statsText.includes('"op":"cast"'), 'cast audit should emit')
  assert(result.statsText.includes('[REDACTED]'), 'cast audit should redact actual/before/after')
  assert(!result.statsText.includes('111'), 'cast audit should not leak raw source fragment')
  assert(!result.statsText.includes('Alice'), 'cast auditColumns should limit row payload')

  const normalizeStep = ops.normalize(['score'], {
    audit: true,
    auditLimit: 1,
    auditColumns: ['score'],
    auditRedact: ['score'],
  })
  assert(normalizeStep.args.audit_columns[0] === 'score', 'normalize helper should pass auditColumns')
  assert(normalizeStep.args.audit_redact[0] === 'score', 'normalize helper should pass auditRedact')
  result = await pipeline([
    codec.csv({ batchSize: 2 }),
    normalizeStep,
    codec.csvEncode(),
  ]).run({ input: 'name,score\nAlice,100\nBob,200\n', allowBlocking: true })
  assert(result.statsText.includes('"op":"normalize"'), 'normalize audit should emit')
  assert(result.statsText.includes('[REDACTED]'), 'normalize audit should redact values and stats')
  assert(!result.statsText.includes('100'), 'normalize audit should not leak min/before value')
  assert(!result.statsText.includes('200'), 'normalize audit should not leak max/source value')
  assert(!result.statsText.includes('Alice'), 'normalize auditColumns should limit row payload')

  const frequencyStep = ops.frequency(['city'], {
    maxValues: 1,
    overflow: 'other',
    audit: true,
    auditLimit: 1,
    auditColumns: ['city'],
    auditRedact: ['city'],
  })
  assert(frequencyStep.args.audit_columns[0] === 'city', 'frequency helper should pass auditColumns')
  assert(frequencyStep.args.audit_redact[0] === 'city', 'frequency helper should pass auditRedact')
  result = await pipeline([
    codec.csv({ batchSize: 1 }),
    frequencyStep,
    codec.csvEncode(),
  ]).run({ input: 'name,city\nAlice,NY\nBob,LA\n' })
  assert(result.statsText.includes('"op":"frequency"'), 'frequency audit should emit')
  assert(result.statsText.includes('"event":"category_overflow"'), 'frequency audit should identify overflow event')
  assert(result.statsText.includes('[REDACTED]'), 'frequency audit should redact overflow value and row data')
  assert(!result.statsText.includes('LA'), 'frequency audit should not leak overflow value')
  assert(!result.statsText.includes('Bob'), 'frequency auditColumns should limit row payload')
  assert(!result.statsText.includes('Alice'), 'frequency auditColumns should limit row payload')
  assert(result.statsText.includes('"data":{"city":"[REDACTED]"}'), 'frequency audit should redact row payload')

  const repairStep = codec.csv({ repair: true, audit: true, auditLimit: 1, auditRedact: ['raw'] })
  assert(repairStep.args.audit_redact[0] === 'raw', 'csv helper should pass auditRedact')
  result = await pipeline([
    repairStep,
    codec.csvEncode(),
  ]).run({ input: 'name,secret\nAlice,SECRET,extra\n' })
  assert(result.statsText.includes('"event":"row_repaired"'), 'csv repair audit should emit')
  assert(result.statsText.includes('[REDACTED]'), 'csv repair audit should redact raw record')
  assert(!result.statsText.includes('SECRET'), 'csv repair audit should not leak raw value')
  assert(!result.statsText.includes('Alice'), 'csv repair audit should not leak raw row')
  const repairErrors = result.errors.toString('utf-8')
  assert(repairErrors.includes('csv_field_count'), 'csv repair diagnostic should emit')
  assert(repairErrors.includes('[REDACTED]'), 'csv repair diagnostic should redact raw record')
  assert(!repairErrors.includes('SECRET'), 'csv repair diagnostic should not leak raw value')
  assert(!repairErrors.includes('Alice'), 'csv repair diagnostic should not leak raw row')

  const teeStep = ops.tee({
    expr: expr("col('age') >= 20"),
    channel: 'audit',
    columns: ['name', 'ssn'],
    limit: 1,
    auditColumns: ['ssn'],
    auditRedact: ['ssn'],
  })
  assert(teeStep.args.audit_columns[0] === 'ssn', 'tee helper should pass auditColumns')
  assert(teeStep.args.audit_redact[0] === 'ssn', 'tee helper should pass auditRedact')
  result = await pipeline([
    codec.csv({ batchSize: 1 }),
    teeStep,
    codec.csvEncode(),
  ]).run({ input: data })
  assert(result.statsText.includes('"op":"tee"'), 'tee audit should emit')
  assert(result.statsText.includes('[REDACTED]'), 'tee audit should redact selected column')
  assert(!result.statsText.includes('111-22-3333'), 'tee audit should not leak raw ssn')
  assert(!result.statsText.includes('Alice'), 'tee auditColumns should limit row payload')

  const quarantineStep = ops.quarantine(expr("col('age') < 30"), {
    auditColumns: ['ssn'],
    auditRedact: ['ssn'],
  })
  assert(quarantineStep.args.audit_columns[0] === 'ssn', 'quarantine helper should pass auditColumns')
  assert(quarantineStep.args.audit_redact[0] === 'ssn', 'quarantine helper should pass auditRedact')
  result = await pipeline([
    codec.csv({ batchSize: 1 }),
    quarantineStep,
    codec.csvEncode(),
  ]).run({ input: data })
  const quarantineErrors = result.errors.toString('utf-8')
  assert(quarantineErrors.includes('"op":"quarantine"'), 'quarantine should emit error record')
  assert(quarantineErrors.includes('[REDACTED]'), 'quarantine should redact selected column')
  assert(!quarantineErrors.includes('111-22-3333'), 'quarantine should not leak raw ssn')
  assert(!quarantineErrors.includes('Alice'), 'quarantine auditColumns should limit row payload')

})


await test('schema infer', async () => {
  const step = ops.schemaInfer({ rows: 2 })
  assert(step.op === 'schema-infer', 'schemaInfer helper should set op')
  assert(step.args.rows === 2, 'schemaInfer helper should pass row limit')

  const result = await pipeline([
    codec.csv({ batchSize: 1 }),
    ops.schemaInfer({ rows: 2 }),
    codec.csvEncode(),
  ]).run({ input: 'name,age,score\nAlice,30,1.5\nBob,,2.25\nCara,40,3.5\n' })
  assert(result.outputText.includes('column,type,nullable,non_null,rows_seen,rows_sampled,missing,non_missing,observed_types,warning'), 'schema infer should emit report header')
  assert(result.outputText.includes('name,string,false,true,3,2,0,2,string:2,sample_limited'), 'schema infer should report string column')
  assert(result.outputText.includes('age,int,true,false,3,2,1,1,int:1;null:1,sample_limited'), 'schema infer should report nullable int column')
  assert(result.outputText.includes('score,float,false,true,3,2,0,2,float:2,sample_limited'), 'schema infer should report float column')
})



await test('tee side channel', async () => {
  const step = ops.tee({ expr: expr("col('age') >= 30"), columns: ['name'], limit: 2, name: 'age_sample' })
  assert(step.op === 'tee', 'tee helper should set op')
  assert(step.args.columns[0] === 'name', 'tee helper should pass columns')
  const result = await pipeline([
    codec.csv({ batchSize: 1 }),
    step,
    codec.csvEncode(),
  ]).run({ input: 'name,age\nAlice,30\nBob,25\nCara,35\nDave,45\n' })
  assert(result.outputText.includes('Alice,30'), 'tee should preserve main Alice')
  assert(result.outputText.includes('Bob,25'), 'tee should preserve main Bob')
  const samples = result.samples.toString('utf-8')
  assert(samples.includes('"type":"tee"'), 'tee should emit sample records')
  assert(samples.includes('age_sample'), 'tee should include name')
  assert(samples.includes('"Alice"'), 'tee should include first matching row')
  assert(samples.includes('"Cara"'), 'tee should include second matching row')
  assert(!samples.includes('"Dave"'), 'tee should enforce limit')
  assert(!samples.includes('"age"'), 'tee should restrict selected columns')
})

await test('explode', async () => {
  const p = pipeline([
    codec.csv(),
    ops.explode('tags', '|'),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'name,tags\nAlice,a|b|c\nBob,x\n' })
  const text = result.outputText
  const lines = text.trim().split('\n')
  assert(lines.length === 5, `should have 5 lines (header + 4), got ${lines.length}`)
})

await test('row expansion caps', async () => {
  const explodeStep = ops.explode('tags', '|', {
    maxTokensPerRow: 2,
    maxOutputRowsPerInputRow: 2,
    maxOutputRowsPerBatch: 3,
    maxTokenBytes: 8,
  })
  assert(explodeStep.args.max_tokens_per_row === 2, 'explode helper should set max_tokens_per_row')
  assert(explodeStep.args.max_output_rows_per_input_row === 2, 'explode helper should set max_output_rows_per_input_row')
  assert(explodeStep.args.max_output_rows_per_batch === 3, 'explode helper should set max_output_rows_per_batch')
  assert(explodeStep.args.max_token_bytes === 8, 'explode helper should set max_token_bytes')
  await assertRejects(
    () => pipeline([codec.csv(), explodeStep, codec.csvEncode()]).run({ input: 'name,tags\nAlice,a|b|c\n' }),
    /max_tokens_per_row=2/
  )
  await assertRejects(
    () => pipeline([codec.csv(), ops.explode('tags', { maxTokenBytes: 1 }), codec.csvEncode()]).run({ input: 'name,tags\nAlice,aa\n' }),
    /max_token_bytes=1/
  )
  const unpivotStep = ops.unpivot(['q1', 'q2'], { maxOutputRowsPerInputRow: 1, maxOutputRowsPerBatch: 2 })
  assert(unpivotStep.args.max_output_rows_per_input_row === 1, 'unpivot helper should set max_output_rows_per_input_row')
  assert(unpivotStep.args.max_output_rows_per_batch === 2, 'unpivot helper should set max_output_rows_per_batch')
  await assertRejects(
    () => pipeline([codec.csv(), unpivotStep, codec.csvEncode()]).run({ input: 'id,q1,q2\n1,10,20\n' }),
    /max_output_rows_per_input_row=1/
  )
})


await test('step running-sum', async () => {
  const p = pipeline([
    codec.csv(),
    ops.step('val', 'running-sum', 'cumsum'),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'val\n10\n20\n30\n' })
  const text = result.outputText
  assert(text.includes('cumsum'), 'should have cumsum column')
  assert(text.includes('60'), 'should have 60 (10+20+30)')
})

await test('frequency', async () => {
  const nl = String.fromCharCode(10)
  const data = ['city', 'NY', 'LA', 'NY', 'NY', 'LA', ''].join(nl)
  const p = pipeline([
    codec.csv(),
    ops.frequency(['city']),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: data })
  const text = result.outputText
  assert(text.includes('value'), 'should have value column')
  assert(text.includes('count'), 'should have count column')
  assert(text.includes('NY'), 'should have NY')
  assert(result.statsText.includes('"tracked_values":2'), 'frequency stats should report tracked values')
  assert(result.statsText.includes('"tracked_key_bytes":6'), 'frequency stats should report serialized key bytes')
  assert(result.statsText.includes('"overflow_count":0'), 'frequency stats should report zero overflow count')
  assert(result.statsText.includes('"retained_state_bytes":'), 'frequency stats should report retained state bytes')

  const overflowData = ['city', 'NY', 'LA', 'SF', 'TX', 'NY', 'SF', ''].join(nl)
  const overflowPipeline = pipeline([
    codec.csv(),
    ops.frequency(['city'], { maxValues: 2, overflow: 'other', other: 'REST', audit: true, auditLimit: 2 }),
    codec.csvEncode(),
  ])
  const overflowResult = await overflowPipeline.run({ input: overflowData })
  const overflow = overflowResult.outputText
  assert(overflow.includes('REST,3'), 'should collapse overflow values into REST')
  assert(overflow.includes('NY,2'), 'should retain exact tracked value NY')
  assert(overflow.includes('LA,1'), 'should retain exact tracked value LA')
  assert(!overflow.includes('SF,'), 'should not emit overflow source SF')
  assert(!overflow.includes('TX,'), 'should not emit overflow source TX')
  assert(overflowResult.statsText.includes('"op":"frequency"'), 'overflow audit should identify frequency op')
  assert(overflowResult.statsText.includes('"event":"category_overflow"'), 'overflow audit should identify category overflow')
  assert(overflowResult.statsText.includes('"value":"SF"'), 'overflow audit should include first overflow value')
  assert(overflowResult.statsText.includes('"value":"TX"'), 'overflow audit should include second overflow value')
  assert(overflowResult.statsText.includes('"bucket":"REST"'), 'overflow audit should include target bucket')
  assert(overflowResult.statsText.includes('"row":3'), 'overflow audit should include first overflow row')
  assert(overflowResult.statsText.includes('"row":4'), 'overflow audit should include second overflow row')
  assert(!overflowResult.statsText.includes('"row":6'), 'overflow audit should enforce auditLimit')
  assert(overflowResult.statsText.includes('"tracked_values":2'), 'overflow frequency stats should report tracked values')
  assert(overflowResult.statsText.includes('"tracked_key_bytes":6'), 'overflow frequency stats should report serialized key bytes')
  assert(overflowResult.statsText.includes('"overflow_count":3'), 'overflow frequency stats should report overflow count')
  assert(overflowResult.statsText.includes('"retained_state_bytes":'), 'overflow frequency stats should report retained state bytes')
})
await test('key-state caps', async () => {
  assert(ops.unique(['city'], { sorted: true }).args.sorted === true, 'unique helper should set sorted')
  assert(ops.dedup(['city'], { sorted: true }).args.sorted === true, 'dedup helper should set sorted')
  assert(ops.unique(['city'], { maxKeys: 2 }).args.max_keys === 2, 'unique helper should set max_keys')
  assert(ops.unique(['city'], { maxStateBytes: 4096 }).args.max_state_bytes === 4096, 'unique helper should set max_state_bytes')
  assert(ops.unique(['city'], { mode: 'approx', bloomBytes: 65536, bloomHashes: 4 }).args.mode === 'approx', 'unique helper should set approx mode')
  assert(ops.unique(['city'], { mode: 'approx', bloomBytes: 65536, bloomHashes: 4 }).args.bloom_bytes === 65536, 'unique helper should set bloom bytes')
  assert(ops.unique(['city'], { approx: true }).args.approx === true, 'unique helper should set approx flag')
  assert(ops.dedup(['city'], { maxStateBytes: 4096 }).args.max_state_bytes === 4096, 'dedup helper should set max_state_bytes')
  assert(ops.dedup(['city'], { mode: 'approx', bloomHashes: 4 }).args.bloom_hashes === 4, 'dedup helper should set bloom hashes')
  assert(ops.groupAgg(['city'], [{ column: 'sales', func: 'sum' }], { maxGroups: 3 }).args.max_groups === 3, 'groupAgg helper should set max_groups')
  assert(ops.groupAgg(['city'], [{ column: 'sales', func: 'sum' }], { maxStateBytes: 8192 }).args.max_state_bytes === 8192, 'groupAgg helper should set max_state_bytes')
  assert(ops.groupAgg(['city'], [{ column: 'sales', func: 'sum' }], { sorted: true }).args.sorted === true, 'groupAgg helper should set sorted')
  assert(ops.rowid(['city'], { maxStateBytes: 2048 }).args.max_state_bytes === 2048, 'rowid helper should set max_state_bytes')
  assert(ops.frequency(['city'], { maxValues: 4 }).args.max_values === 4, 'frequency helper should set max_values')
  assert(ops.frequency(['city'], { maxStateBytes: 4096 }).args.max_state_bytes === 4096, 'frequency helper should set max_state_bytes')
  assert(ops.onehot('city', { maxStateBytes: 4096 }).args.max_state_bytes === 4096, 'onehot helper should set max_state_bytes')
  assert(ops.labelEncode('city', { maxStateBytes: 4096 }).args.max_state_bytes === 4096, 'labelEncode helper should set max_state_bytes')
  assert(ops.intersect('lookup.csv', { maxStateBytes: 4096 }).args.max_state_bytes === 4096, 'intersect helper should set max_state_bytes')
  assert(ops.setdiff('lookup.csv', { maxStateBytes: 4096 }).args.max_state_bytes === 4096, 'setdiff helper should set max_state_bytes')
  assert(ops.intersectAll('lookup.csv', { maxStateBytes: 4096 }).args.max_state_bytes === 4096, 'intersectAll helper should set max_state_bytes')
  assert(ops.intersectAll('lookup.csv', { sorted: true }).args.sorted === true, 'intersectAll helper should set sorted')
  assert(ops.setdiffAll('lookup.csv', { maxStateBytes: 4096 }).args.max_state_bytes === 4096, 'setdiffAll helper should set max_state_bytes')
  assert(ops.setdiffAll('lookup.csv', { sorted: true }).args.sorted === true, 'setdiffAll helper should set sorted')
  assert(ops.union('lookup.csv', { maxStateBytes: 4096 }).args.max_state_bytes === 4096, 'union helper should set max_state_bytes')
  const freqArgs = ops.frequency(['city'], { maxValues: 4, overflow: 'other', other: 'REST', audit: true, auditLimit: 2 }).args
  assert(freqArgs.overflow === 'other', 'frequency helper should set overflow')
  assert(freqArgs.other === 'REST', 'frequency helper should set other label')
  assert(freqArgs.audit === true, 'frequency helper should set audit')
  assert(freqArgs.audit_limit === 2, 'frequency helper should set audit_limit')
  const freqPrivacyArgs = ops.frequency(['city'], { auditColumns: ['city'], auditRedact: ['city'], auditHashColumns: ['city'], auditMaxBytes: 100, auditMaxCellBytes: 8 }).args
  assert(freqPrivacyArgs.audit_columns[0] === 'city', 'frequency helper should set audit_columns')
  assert(freqPrivacyArgs.audit_redact[0] === 'city', 'frequency helper should set audit_redact')
  assert(freqPrivacyArgs.audit_hash_columns[0] === 'city', 'frequency helper should set audit_hash_columns')
  assert(freqPrivacyArgs.audit_max_bytes === 100, 'frequency helper should set audit_max_bytes')
  assert(freqPrivacyArgs.audit_max_cell_bytes === 8, 'frequency helper should set audit_max_cell_bytes')
  const onehotArgs = ops.onehot('city', { drop: true, categories: ['NY', 'LA'], maxCategories: 3, unknown: 'other' }).args
  assert(onehotArgs.drop === true, 'onehot helper should set drop')
  assert(onehotArgs.categories.length === 2, 'onehot helper should set categories')
  assert(onehotArgs.max_categories === 3, 'onehot helper should set max_categories')
  assert(onehotArgs.unknown === 'other', 'onehot helper should set unknown')
  const labelArgs = ops.labelEncode('city', { result: 'city_id', categories: ['NY', 'LA'], maxCategories: 3, unknown: 'null' }).args
  assert(labelArgs.result === 'city_id', 'labelEncode helper should set result')
  assert(labelArgs.categories.length === 2, 'labelEncode helper should set categories')
  assert(labelArgs.max_categories === 3, 'labelEncode helper should set max_categories')
  assert(labelArgs.unknown === 'null', 'labelEncode helper should set unknown')
  const joinArgs = ops.join('lookup.csv', 'city', {
    how: 'left',
    maxLookupRows: 10,
    maxLookupKeys: 8,
    maxLookupBytes: 1024,
    maxStateBytes: 4096,
    maxMatchesPerRow: 2,
    maxOutputRows: 20,
  }).args
  assert(joinArgs.how === 'left', 'join helper should set how')
  assert(joinArgs.max_lookup_rows === 10, 'join helper should set max_lookup_rows')
  assert(joinArgs.max_lookup_keys === 8, 'join helper should set max_lookup_keys')
  assert(joinArgs.max_lookup_bytes === 1024, 'join helper should set max_lookup_bytes')
  assert(joinArgs.max_state_bytes === 4096, 'join helper should set max_state_bytes')
  assert(joinArgs.max_matches_per_row === 2, 'join helper should set max_matches_per_row')
  assert(joinArgs.max_output_rows === 20, 'join helper should set max_output_rows')
  const semiArgs = ops.semiJoin('lookup.csv', 'city', {
    maxLookupRows: 10,
    maxLookupKeys: 8,
    maxLookupBytes: 1024,
    maxStateBytes: 4096,
    maxOutputRows: 20,
  }).args
  assert(semiArgs.max_lookup_rows === 10, 'semiJoin helper should set max_lookup_rows')
  assert(semiArgs.max_lookup_keys === 8, 'semiJoin helper should set max_lookup_keys')
  assert(semiArgs.max_lookup_bytes === 1024, 'semiJoin helper should set max_lookup_bytes')
  assert(semiArgs.max_state_bytes === 4096, 'semiJoin helper should set max_state_bytes')
  assert(semiArgs.max_output_rows === 20, 'semiJoin helper should set max_output_rows')
  const antiArgs = ops.antiJoin('lookup.csv', 'city', { maxLookupBytes: 1024, maxStateBytes: 4096 }).args
  assert(antiArgs.file === 'lookup.csv', 'antiJoin helper should set file')
  assert(antiArgs.on === 'city', 'antiJoin helper should set on')
  assert(antiArgs.max_lookup_bytes === 1024, 'antiJoin helper should set max_lookup_bytes')
  assert(antiArgs.max_state_bytes === 4096, 'antiJoin helper should set max_state_bytes')
  const jsonArgs = ops.jsonExtract('/user/id', 'user_id', { type: 'int' }).args
  assert(jsonArgs.column === '_line', 'jsonExtract helper should default to _line')
  assert(jsonArgs.path === '/user/id', 'jsonExtract helper should set path')
  assert(jsonArgs.result === 'user_id', 'jsonExtract helper should set result')
  assert(jsonArgs.type === 'int', 'jsonExtract helper should set type')
  const jsonFilterArgs = ops.jsonFilter('/user/age', { op: 'ge', value: '30', type: 'float' }).args
  assert(jsonFilterArgs.column === '_line', 'jsonFilter helper should default to _line')
  assert(jsonFilterArgs.path === '/user/age', 'jsonFilter helper should set path')
  assert(jsonFilterArgs.op === 'ge', 'jsonFilter helper should set op')
  assert(jsonFilterArgs.value === '30', 'jsonFilter helper should set value')
  assert(jsonFilterArgs.type === 'float', 'jsonFilter helper should set type')

  async function expectCapError(step, input, expected) {
    let failed = false
    try {
      await pipeline([codec.csv(), step, codec.csvEncode()]).run({ input })
    } catch (e) {
      failed = e.message.includes(expected)
    }
    assert(failed, `expected cap error containing ${expected}`)
  }

  await expectCapError(ops.unique(['city'], { maxKeys: 1 }), 'city\nNY\nLA\n', 'max_keys=1')
  await expectCapError(
    ops.groupAgg(['city'], [{ column: 'sales', func: 'sum', name: 'total' }], { maxGroups: 1 }),
    'city,sales\nNY,1\nLA,2\n',
    'max_groups=1'
  )
  await expectCapError(ops.frequency(['city'], { maxValues: 1 }), 'city\nNY\nLA\n', 'max_values=1')
  await expectCapError(ops.frequency(['city'], { maxStateBytes: 1024 }), 'city\nNY\n', 'max_state_bytes=1024')
  await expectCapError(ops.rowid(['city'], { maxStateBytes: 1024 }), 'city\nNY\n', 'max_state_bytes=1024')
  await expectCapError(ops.onehot('city', { maxCategories: 1 }), 'city\nNY\nLA\n', 'max_categories=1')
  await expectCapError(ops.labelEncode('city', { maxCategories: 1 }), 'city\nNY\nLA\n', 'max_categories=1')
  await expectCapError(ops.onehot('city', { maxStateBytes: 128 }), 'city\nNY\n', 'max_state_bytes=128')
  await expectCapError(ops.labelEncode('city', { maxStateBytes: 128 }), 'city\nNY\n', 'max_state_bytes=128')
})


await test('unique sorted mode', async () => {
  const result = await pipeline([
    codec.csv({ batchSize: 2 }),
    ops.unique(['city'], { sorted: true }),
    codec.csvEncode(),
  ]).run({ input: 'city,val\nA,1\nA,2\nA,3\nB,4\nB,5\nA,6\nA,7\n' })
  const text = result.outputText
  const lines = text.trim().split('\n')
  assert(lines[0] === 'city,val', 'sorted unique header')
  assert(lines[1] === 'A,1', 'sorted unique keeps first A run')
  assert(lines[2] === 'B,4', 'sorted unique keeps first B run')
  assert(lines[3] === 'A,6', 'sorted unique treats later A as new run')
  assert(!text.includes('A,2'), 'sorted unique drops adjacent A duplicate')
  assert(!text.includes('A,3'), 'sorted unique drops cross-batch A duplicate')
  assert(!text.includes('B,5'), 'sorted unique drops adjacent B duplicate')
  assert(!text.includes('A,7'), 'sorted unique drops later adjacent A duplicate')
})

await test('group-agg sorted mode', async () => {
  const result = await pipeline([
    codec.csv({ batchSize: 1 }),
    ops.groupAgg(
      ['city'],
      [
        { column: 'sales', func: 'sum', name: 'total' },
        { column: 'sales', func: 'count', name: 'n' },
      ],
      { sorted: true }
    ),
    codec.csvEncode(),
  ]).run({ input: 'city,sales\nA,1\nA,2\nB,10\nB,5\nA,7\n' })
  const lines = result.outputText.trim().split('\n')
  assert(lines[0] === 'city,total,n', 'sorted groupAgg header')
  assert(lines[1].startsWith('A,3'), 'sorted groupAgg sums first A run')
  assert(lines[2].startsWith('B,15'), 'sorted groupAgg sums B run')
  assert(lines[3].startsWith('A,7'), 'sorted groupAgg treats later A as new run')
})

function assertGroupAggNullSemantics(text) {
  assert(text.includes('city,total,avg,n,rows,min,max'), 'groupAgg null semantics header')
  assert(text.includes('A,15,7.5,2,3,5,10'), 'groupAgg should ignore null numeric values for A')
  assert(text.includes('B,7,7,1,2,7,7'), 'groupAgg should count non-null and row values for B')
  assert(text.includes('C,,,0,1,,'), 'groupAgg should emit null numeric aggs for all-null groups')
}

await test('group-agg null semantics', async () => {
  const input = [
    '{"city":"A","amount":10}',
    '{"city":"A","amount":null}',
    '{"city":"A","amount":5}',
    '{"city":"B","amount":null}',
    '{"city":"B","amount":7}',
    '{"city":"C","amount":null}',
  ].join('\n') + '\n'
  const aggs = [
    { column: 'amount', func: 'sum', name: 'total' },
    { column: 'amount', func: 'avg', name: 'avg' },
    { column: 'amount', func: 'count', name: 'n' },
    { column: '*', func: 'count', name: 'rows' },
    { column: 'amount', func: 'min', name: 'min' },
    { column: 'amount', func: 'max', name: 'max' },
  ]

  let result = await pipeline([
    codec.jsonl({ batchSize: 2 }),
    ops.groupAgg(['city'], aggs),
    codec.csvEncode(),
  ]).run({ input })
  assertGroupAggNullSemantics(result.outputText)

  result = await pipeline([
    codec.jsonl({ batchSize: 2 }),
    ops.groupAgg(['city'], aggs, { sorted: true }),
    codec.csvEncode(),
  ]).run({ input })
  assertGroupAggNullSemantics(result.outputText)
})

await test('category policy ops', async () => {
  let p = pipeline([
    codec.csv(),
    ops.onehot('color', { categories: ['red', 'blue'], unknown: 'null' }),
    codec.csvEncode(),
  ])
  let result = await p.run({ input: 'name,color\nA,red\nB,green\n' })
  assert(result.outputText.includes('color_red,color_blue'), 'onehot should use declared categories')
  assert(result.outputText.includes('B,green,0,0'), 'unknown=null should produce zero onehot columns')

  p = pipeline([
    codec.csv(),
    ops.labelEncode('city', { result: 'city_id', categories: ['Paris', 'London'], unknown: 'other' }),
    codec.csvEncode(),
  ])
  result = await p.run({ input: 'city\nParis\nBerlin\nLondon\n' })
  assert(result.outputText.includes('Paris,0'), 'declared category should keep assigned label')
  assert(result.outputText.includes('Berlin,2'), 'unknown=other should use other label')
  assert(result.outputText.includes('London,1'), 'declared category order should be preserved')
})

await test('join lookup caps', async () => {
  const { writeFileSync, unlinkSync } = require('fs')
  const lookup = '/tmp/tranfi-node-join-caps.csv'
  writeFileSync(lookup, 'id,val\n1,a\n2,b\n')

  async function expectJoinCapError(step, expected) {
    let failed = false
    try {
      await pipeline([codec.csv(), step, codec.csvEncode()]).run({ input: 'id\n1\n', allowFs: true })
    } catch (e) {
      failed = e.message.includes(expected)
    }
    assert(failed, `expected join cap error containing ${expected}`)
  }

  try {
    await expectJoinCapError(ops.join(lookup, 'id', { maxLookupRows: 1 }), 'max_lookup_rows=1')
    await expectJoinCapError(ops.join(lookup, 'id', { maxLookupKeys: 1 }), 'max_lookup_keys=1')
    await expectJoinCapError(ops.join(lookup, 'id', { maxLookupBytes: 8 }), 'max_lookup_bytes=8')
    await expectJoinCapError(ops.join(lookup, 'id', { maxStateBytes: 512 }), 'max_state_bytes=512')
  } finally {
    unlinkSync(lookup)
  }
})

await test('join output caps', async () => {
  const { writeFileSync, unlinkSync } = require('fs')
  const lookup = '/tmp/tranfi-node-join-output-caps.csv'
  writeFileSync(lookup, 'id,val\n1,a\n1,b\n')

  async function expectJoinCapError(step, expected) {
    let failed = false
    try {
      await pipeline([codec.csv({ batchSize: 1 }), step, codec.csvEncode()]).run({ input: 'id\n1\n', allowFs: true })
    } catch (e) {
      failed = e.message.includes(expected)
    }
    assert(failed, `expected join output cap error containing ${expected}`)
  }

  try {
    await expectJoinCapError(ops.join(lookup, 'id', { maxMatchesPerRow: 1 }), 'max_matches_per_row=1')
    await expectJoinCapError(ops.join(lookup, 'id', { maxOutputRows: 1 }), 'max_output_rows=1')
    const result = await pipeline([
      codec.csv({ batchSize: 1 }),
      ops.join(lookup, 'id', { maxMatchesPerRow: 2, maxOutputRows: 2, maxStateBytes: 65536 }),
      codec.csvEncode(),
    ]).run({ input: 'id\n1\n', allowFs: true })
    assert(result.outputText.includes('1,a'), 'join should emit first duplicate match')
    assert(result.outputText.includes('1,b'), 'join should emit second duplicate match')
    assert(result.statsText.includes('"op":"join"'), 'join stats should include op name')
    assert(result.statsText.includes('"lookup_rows":2'), 'join stats should report lookup rows')
    assert(result.statsText.includes('"lookup_keys":1'), 'join stats should report lookup keys')
    assert(result.statsText.includes('"lookup_key_bytes":4'), 'join stats should report serialized lookup key bytes')
    assert(result.statsText.includes('"lookup_row_refs":4'), 'join stats should report lookup row-reference capacity')
    assert(result.statsText.includes('"lookup_batch_bytes":'), 'join stats should report lookup batch bytes')
    assert(result.statsText.includes('"retained_state_bytes":'), 'join stats should report retained state bytes')
    assert(result.statsText.includes('"max_state_bytes":65536'), 'join stats should report max_state_bytes')
  } finally {
    unlinkSync(lookup)
  }
})


await test('join typed keys', async () => {
  const { writeFileSync, unlinkSync } = require('fs')
  const typedLookup = '/tmp/tranfi-node-join-typed-lookup.csv'
  writeFileSync(typedLookup, 'id,val\n1,string-one\nx,other\n')
  let mismatchFailed = false
  try {
    await pipeline([codec.csv({ batchSize: 1 }), ops.join(typedLookup, 'id'), codec.csvEncode()]).run({ input: 'id\n1\n', allowFs: true })
  } catch (e) {
    mismatchFailed = e.message.includes('join key types differ')
  } finally {
    unlinkSync(typedLookup)
  }
  assert(mismatchFailed, 'join should reject mismatched typed key columns')

  const sentinelLookup = '/tmp/tranfi-node-join-sentinel-lookup.csv'
  writeFileSync(sentinelLookup, 'id,val\n\\N,sentinel\nx,other\n')
  try {
    const result = await pipeline([
      codec.csv({ batchSize: 1 }),
      ops.join(sentinelLookup, 'id'),
      codec.csvEncode(),
    ]).run({ input: 'id,name\n,empty\n\\N,literal\n', allowFs: true })
    assert(result.outputText.includes('\\N,literal,sentinel'), 'string sentinel key should match literal string row')
    assert(!result.outputText.includes('empty'), 'null key should not collide with literal sentinel string')
  } finally {
    unlinkSync(sentinelLookup)
  }
})

await test('filtering joins', async () => {
  const { writeFileSync, unlinkSync } = require('fs')
  const lookup = '/tmp/tranfi-node-filtering-join.csv'
  const empty = '/tmp/tranfi-node-filtering-empty-join.csv'
  writeFileSync(lookup, 'id,val\n1,a\n1,b\n3,c\n')
  writeFileSync(empty, 'id,val\n')
  const input = 'id,name\n1,Alice\n2,Bob\n3,Charlie\n'

  try {
    const semi = await pipeline([
      codec.csv(),
      ops.semiJoin(lookup, 'id', { maxLookupBytes: 1024, maxStateBytes: 4096 }),
      codec.csvEncode(),
    ]).run({ input, memory: '64KB', allowFs: true })
    assert(semi.outputText.trim() === 'id,name\n1,Alice\n3,Charlie', 'semi-join should filter matching left rows without duplicates')

    const anti = await pipeline([
      codec.csv(),
      ops.antiJoin(lookup, 'id', { maxLookupBytes: 1024, maxStateBytes: 4096 }),
      codec.csvEncode(),
    ]).run({ input, memory: '64KB', allowFs: true })
    assert(anti.outputText.trim() === 'id,name\n2,Bob', 'anti-join should keep only unmatched left rows')

    const antiEmpty = await pipeline([
      codec.csv(),
      ops.antiJoin(empty, 'id', { maxLookupBytes: 1024 }),
      codec.csvEncode(),
    ]).run({ input, memory: '64KB', allowFs: true })
    assert(antiEmpty.outputText.trim() === 'id,name\n1,Alice\n2,Bob\n3,Charlie', 'anti-join against empty lookup should keep all left rows')
  } finally {
    unlinkSync(lookup)
    unlinkSync(empty)
  }
})

await test('sorted join mode', async () => {
  const { writeFileSync, unlinkSync } = require('fs')
  const lookup = '/tmp/tranfi-node-sorted-join.csv'
  writeFileSync(lookup, 'id,val\n1,a\n2,b\n2,c\n4,d\n')
  const input = 'id,name\n1,Alice\n2,Bob\n2,Beth\n3,Cara\n4,Dave\n'

  try {
    const semi = await pipeline([
      codec.csv({ batchSize: 2 }),
      ops.semiJoin(lookup, 'id', { sorted: true }),
      codec.csvEncode(),
    ]).run({ input, memory: '64KB', allowFs: true })
    assert(semi.outputText.trim() === 'id,name\n1,Alice\n2,Bob\n2,Beth\n4,Dave', 'sorted semi-join should stream lookup keys')

    const anti = await pipeline([
      codec.csv({ batchSize: 2 }),
      ops.antiJoin(lookup, 'id', { sorted: true }),
      codec.csvEncode(),
    ]).run({ input, memory: '64KB', allowFs: true })
    assert(anti.outputText.trim() === 'id,name\n3,Cara', 'sorted anti-join should keep unmatched left rows')

    const inner = await pipeline([
      codec.csv({ batchSize: 2 }),
      ops.join(lookup, 'id', { sorted: true, maxMatchesPerRow: 2 }),
      codec.csvEncode(),
    ]).run({ input, memory: '64KB', allowFs: true })
    assert(inner.outputText.includes('1,Alice,a'), 'sorted inner join should include id=1')
    assert(inner.outputText.includes('2,Bob,b'), 'sorted inner join should include first duplicate match')
    assert(inner.outputText.includes('2,Bob,c'), 'sorted inner join should include second duplicate match')
    assert(inner.outputText.includes('2,Beth,b'), 'sorted inner join should reuse current lookup run')
    assert(!inner.outputText.includes('Cara'), 'sorted inner join should skip unmatched rows')

    const left = await pipeline([
      codec.csv({ batchSize: 2 }),
      ops.join(lookup, 'id', { how: 'left', sorted: true, maxMatchesPerRow: 2 }),
      codec.csvEncode(),
    ]).run({ input, memory: '64KB', allowFs: true })
    assert(left.outputText.includes('3,Cara,'), 'sorted left join should emit nulls for unmatched rows')

    let failed = false
    try {
      await pipeline([
        codec.csv({ batchSize: 1 }),
        ops.semiJoin(lookup, 'id', { sorted: true }),
        codec.csvEncode(),
      ]).run({ input: 'id,name\n2,Bob\n1,Alice\n', memory: '64KB', allowFs: true })
    } catch (e) {
      failed = e.message.includes('left side is not sorted')
    }
    assert(failed, 'sorted join should reject unsorted left input')
  } finally {
    unlinkSync(lookup)
  }
})

await test('set ops', async () => {
  const { writeFileSync, unlinkSync } = require('fs')
  const lookup = '/tmp/tranfi-node-set-lookup.csv'
  const keyLookup = '/tmp/tranfi-node-set-key-lookup.csv'
  writeFileSync(lookup, 'id,name\n1,Alice\n3,Charlie\n')
  writeFileSync(keyLookup, 'id,label\n1,x\n3,y\n')
  const input = 'id,name\n1,Alice\n2,Bob\n1,Alice\n3,Charlie\n3,Other\n'

  try {
    const inter = await pipeline([
      codec.csv(),
      ops.intersect(lookup, { maxLookupBytes: 1024, maxLookupKeys: 10, maxOutputKeys: 10 }),
      codec.csvEncode(),
    ]).run({ input, memory: '64KB', allowFs: true })
    assert(inter.outputText.trim() === 'id,name\n1,Alice\n3,Charlie', 'intersect should keep distinct matching rows')

    const interByte = await pipeline([
      codec.csv(),
      ops.intersect(lookup, { maxLookupBytes: 1024, maxStateBytes: 4096 }),
      codec.csvEncode(),
    ]).run({ input, memory: '64KB', allowFs: true })
    assert(interByte.outputText.trim() === 'id,name\n1,Alice\n3,Charlie', 'intersect should accept maxStateBytes without maxOutputKeys')
    assert(interByte.statsText.includes('"max_state_bytes":4096'), 'intersect stats should report max_state_bytes')

    let interByteCapFailed = false
    try {
      await pipeline([
        codec.csv(),
        ops.intersect(lookup, { maxLookupBytes: 1024, maxStateBytes: 128 }),
        codec.csvEncode(),
      ]).run({ input, memory: '64KB', allowFs: true })
    } catch (e) {
      interByteCapFailed = e.message.includes('max_state_bytes=128')
    }
    assert(interByteCapFailed, 'intersect should enforce maxStateBytes')

    const diff = await pipeline([
      codec.csv(),
      ops.setdiff(lookup, { maxLookupBytes: 1024, maxLookupKeys: 10, maxOutputKeys: 10 }),
      codec.csvEncode(),
    ]).run({ input, memory: '64KB', allowFs: true })
    assert(diff.outputText.trim() === 'id,name\n2,Bob\n3,Other', 'setdiff should keep distinct unmatched rows')

    writeFileSync(lookup, 'id,label\n1,a\n1,a2\n3,c\n')
    const bagInput = 'id,name\n1,A1\n1,A2\n1,A3\n2,B\n3,C\n3,C2\n'
    const interAll = await pipeline([
      codec.csv({ batchSize: 2 }),
      ops.intersectAll(lookup, { columns: ['id'], maxLookupBytes: 1024, maxLookupKeys: 10 }),
      codec.csvEncode(),
    ]).run({ input: bagInput, memory: '64KB', allowFs: true })
    assert(interAll.outputText.trim() === 'id,name\n1,A1\n1,A2\n3,C', 'intersectAll should keep lookup-count matching copies')

    const diffAll = await pipeline([
      codec.csv({ batchSize: 2 }),
      ops.setdiffAll(lookup, { columns: ['id'], maxLookupBytes: 1024, maxLookupKeys: 10 }),
      codec.csvEncode(),
    ]).run({ input: bagInput, memory: '64KB', allowFs: true })
    assert(diffAll.outputText.trim() === 'id,name\n1,A3\n2,B\n3,C2', 'setdiffAll should consume lookup-count copies')

    let bagCapFailed = false
    try {
      await pipeline([
        codec.csv({ batchSize: 1 }),
        ops.intersectAll(lookup, { columns: ['id'], maxLookupBytes: 1024, maxLookupKeys: 1 }),
        codec.csvEncode(),
      ]).run({ input: bagInput, memory: '64KB', allowFs: true })
    } catch (e) {
      bagCapFailed = e.message.includes('max_lookup_keys=1')
    }
    assert(bagCapFailed, 'intersectAll should enforce maxLookupKeys')

    let bagPolicyFailed = false
    try {
      await pipeline([
        codec.csv({ batchSize: 1 }),
        ops.setdiffAll(lookup, { columns: ['id'], maxLookupBytes: 1024 }),
        codec.csvEncode(),
      ]).run({ input: bagInput, memory: '64KB', allowFs: true })
    } catch (e) {
      bagPolicyFailed = e.message.includes('max_lookup_keys or max_state_bytes')
    }
    assert(bagPolicyFailed, 'setdiffAll should need a key or byte cap under memory policy')

    writeFileSync(lookup, 'id,name\n1,Alice\n3,Charlie\n')

    const unionAll = await pipeline([
      codec.csv({ batchSize: 1 }),
      ops.unionAll(lookup),
      codec.csvEncode(),
    ]).run({ input: 'id,name\n1,Alice\n2,Bob\n', memory: '64KB', allowFs: true })
    assert(unionAll.outputText.trim() === 'id,name\n1,Alice\n2,Bob\n1,Alice\n3,Charlie', 'unionAll should append file rows and preserve duplicates')

    const union = await pipeline([
      codec.csv({ batchSize: 1 }),
      ops.union(lookup, { maxOutputKeys: 10 }),
      codec.csvEncode(),
    ]).run({ input, memory: '64KB', allowFs: true })
    assert(union.outputText.trim() === 'id,name\n1,Alice\n2,Bob\n3,Charlie\n3,Other', 'union should keep first distinct rows across left and lookup')
    assert(union.statsText.includes('"emitted_keys":4'), 'union stats should report emitted key count')
    assert(union.statsText.includes('"emitted_key_bytes":'), 'union stats should report retained key bytes')
    assert(union.statsText.includes('"retained_state_bytes":'), 'union stats should report retained state bytes')
    assert(union.statsText.includes('"max_state_bytes":0'), 'union stats should report default max_state_bytes')

    const unionByte = await pipeline([
      codec.csv({ batchSize: 1 }),
      ops.union(lookup, { maxStateBytes: 4096 }),
      codec.csvEncode(),
    ]).run({ input, memory: '64KB', allowFs: true })
    assert(unionByte.outputText.trim() === 'id,name\n1,Alice\n2,Bob\n3,Charlie\n3,Other', 'union should accept maxStateBytes without maxOutputKeys')

    writeFileSync(lookup, 'id,name\n1,A_file\n1,A_file_dup\n3,C_file\n5,E_file\n5,E_file_dup\n')
    const sortedUnion = await pipeline([
      codec.csv({ batchSize: 2 }),
      ops.union(lookup, { columns: ['id'], sorted: true }),
      codec.csvEncode(),
    ]).run({ input: 'id,name\n1,A_left\n1,A_left_dup\n2,B_left\n4,D_left\n5,E_left\n', memory: '64KB', allowFs: true })
    assert(sortedUnion.outputText.trim() === 'id,name\n1,A_left\n2,B_left\n3,C_file\n4,D_left\n5,E_left', 'sorted union should merge sorted inputs and prefer left rows')
    assert(sortedUnion.statsText.includes('"emitted_keys":5'), 'sorted union stats should report emitted keys')

    let unionLeftSortedFailed = false
    try {
      await pipeline([
        codec.csv({ batchSize: 1 }),
        ops.union(lookup, { columns: ['id'], sorted: true }),
        codec.csvEncode(),
      ]).run({ input: 'id,name\n2,B\n1,A\n', memory: '64KB', allowFs: true })
    } catch (e) {
      unionLeftSortedFailed = e.message.includes('union: left side is not sorted')
    }
    assert(unionLeftSortedFailed, 'sorted union should reject unsorted left input')

    writeFileSync(lookup, 'id,name\n3,C\n1,A\n')
    let unionFileSortedFailed = false
    try {
      await pipeline([
        codec.csv({ batchSize: 1 }),
        ops.union(lookup, { columns: ['id'], sorted: true }),
        codec.csvEncode(),
      ]).run({ input: 'id,name\n2,B\n4,D\n', memory: '64KB', allowFs: true })
    } catch (e) {
      unionFileSortedFailed = e.message.includes('union: file side is not sorted')
    }
    assert(unionFileSortedFailed, 'sorted union should reject unsorted file input')

    writeFileSync(lookup, 'id,name\n1,Alice\n3,Charlie\n')

    let unionByteCapFailed = false
    try {
      await pipeline([
        codec.csv({ batchSize: 1 }),
        ops.union(lookup, { maxStateBytes: 128 }),
        codec.csvEncode(),
      ]).run({ input: 'id,name\n1,Alice\n', memory: '64KB', allowFs: true })
    } catch (e) {
      unionByteCapFailed = e.message.includes('max_state_bytes=128')
    }
    assert(unionByteCapFailed, 'union should enforce maxStateBytes')

    let unionCapFailed = false
    try {
      await pipeline([
        codec.csv({ batchSize: 1 }),
        ops.union(lookup, { maxOutputKeys: 2 }),
        codec.csvEncode(),
      ]).run({ input: 'id,name\n1,Alice\n2,Bob\n', memory: '64KB', allowFs: true })
    } catch (e) {
      unionCapFailed = e.message.includes('max_output_keys=2')
    }
    assert(unionCapFailed, 'union should enforce maxOutputKeys')

    const byId = await pipeline([
      codec.csv(),
      ops.intersect(keyLookup, { columns: ['id'], maxLookupBytes: 1024, maxLookupKeys: 10, maxOutputKeys: 10 }),
      codec.csvEncode(),
    ]).run({ input: 'id,name\n1,Alice\n2,Bob\n3,Charlie\n3,Other\n', memory: '64KB', allowFs: true })
    assert(byId.outputText.trim() === 'id,name\n1,Alice\n3,Charlie', 'intersect columns should dedupe by selected key')

    const sortedLookup = '/tmp/tranfi-node-set-sorted-lookup.csv'
    writeFileSync(sortedLookup, 'id,label\n1,x\n3,y\n5,z\n')
    const sortedInput = 'id,name\n1,Alice\n1,Alicia\n2,Bob\n3,Charlie\n3,Other\n4,Dana\n5,Eve\n'
    const sortedInter = await pipeline([
      codec.csv({ batchSize: 2 }),
      ops.intersect(sortedLookup, { columns: ['id'], sorted: true }),
      codec.csvEncode(),
    ]).run({ input: sortedInput, memory: '64KB', allowFs: true })
    assert(sortedInter.outputText.trim() === 'id,name\n1,Alice\n3,Charlie\n5,Eve', 'sorted intersect should stream and dedupe adjacent keys')

    const sortedDiff = await pipeline([
      codec.csv({ batchSize: 2 }),
      ops.setdiff(sortedLookup, { columns: ['id'], sorted: true }),
      codec.csvEncode(),
    ]).run({ input: sortedInput, memory: '64KB', allowFs: true })
    assert(sortedDiff.outputText.trim() === 'id,name\n2,Bob\n4,Dana', 'sorted setdiff should stream and dedupe adjacent keys')

    writeFileSync(sortedLookup, 'id,label\n1,x\n1,x2\n3,y\n5,z\n5,z2\n')
    const sortedBag = await pipeline([
      codec.csv({ batchSize: 2 }),
      ops.intersectAll(sortedLookup, { columns: ['id'], sorted: true }),
      codec.csvEncode(),
    ]).run({ input: sortedInput, memory: '64KB', allowFs: true })
    assert(sortedBag.outputText.trim() === 'id,name\n1,Alice\n1,Alicia\n3,Charlie\n5,Eve', 'sorted intersectAll should emit min left/lookup copies')

    const sortedBagDiff = await pipeline([
      codec.csv({ batchSize: 2 }),
      ops.setdiffAll(sortedLookup, { columns: ['id'], sorted: true }),
      codec.csvEncode(),
    ]).run({ input: sortedInput, memory: '64KB', allowFs: true })
    assert(sortedBagDiff.outputText.trim() === 'id,name\n2,Bob\n3,Other\n4,Dana', 'sorted setdiffAll should emit excess left copies')

    let failedBagSorted = false
    try {
      await pipeline([
        codec.csv({ batchSize: 1 }),
        ops.intersectAll(sortedLookup, { columns: ['id'], sorted: true }),
        codec.csvEncode(),
      ]).run({ input: 'id,name\n2,Bob\n1,Alice\n', memory: '64KB', allowFs: true })
    } catch (e) {
      failedBagSorted = e.message.includes('intersect-all: left side is not sorted')
    }
    assert(failedBagSorted, 'sorted intersectAll should reject unsorted left input')

    let failedSorted = false
    try {
      await pipeline([
        codec.csv({ batchSize: 1 }),
        ops.intersect(sortedLookup, { columns: ['id'], sorted: true }),
        codec.csvEncode(),
      ]).run({ input: 'id,name\n2,Bob\n1,Alice\n', memory: '64KB', allowFs: true })
    } catch (e) {
      failedSorted = e.message.includes('left side is not sorted')
    }
    assert(failedSorted, 'sorted intersect should reject unsorted left input')

    let failed = false
    try {
      await pipeline([
        codec.csv({ batchSize: 1 }),
        ops.intersect(keyLookup, { columns: ['id'], maxLookupKeys: 1 }),
        codec.csvEncode(),
      ]).run({ input: 'id,name\n1,Alice\n', allowFs: true })
    } catch (e) {
      failed = e.message.includes('max_lookup_keys=1')
    }
    assert(failed, 'intersect should enforce maxLookupKeys')
  } finally {
    unlinkSync(lookup)
    unlinkSync(keyLookup)
    try { unlinkSync('/tmp/tranfi-node-set-sorted-lookup.csv') } catch {}
  }
})

await test('top', async () => {
  const p = pipeline([
    codec.csv(),
    ops.top(2, 'score'),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'name,score\nAlice,85\nBob,92\nCharlie,78\n' })
  const text = result.outputText
  assert(text.includes('Bob'), 'Bob has highest score')
  assert(text.includes('Alice'), 'Alice has second highest')
  assert(!text.includes('Charlie'), 'Charlie should be excluded')
})

await test('bottomK and slice helpers', async () => {
  const nl = String.fromCharCode(10)
  const fourRows = ['name,score', 'Alice,85', 'Bob,92', 'Charlie,78', 'Diana,95', ''].join(nl)
  const threeRows = ['name,score', 'Alice,85', 'Bob,92', 'Charlie,78', ''].join(nl)
  const low = pipeline([
    codec.csv(),
    ops.bottomK(2, 'score'),
    codec.csvEncode(),
  ])
  const lowResult = await low.run({ input: fourRows })
  assert(lowResult.outputText.includes('Charlie'), 'Charlie has lowest score')
  assert(lowResult.outputText.includes('Alice'), 'Alice has second lowest score')
  assert(!lowResult.outputText.includes('Bob'), 'Bob should be excluded from bottom 2')

  const max = pipeline([
    codec.csv(),
    ops.sliceMax('score', 2),
    codec.csvEncode(),
  ])
  const maxResult = await max.run({ input: fourRows })
  assert(maxResult.outputText.includes('Diana'), 'Diana has highest score')
  assert(maxResult.outputText.includes('Bob'), 'Bob has second highest score')
  assert(!maxResult.outputText.includes('Charlie'), 'Charlie should be excluded from max 2')
  assert(ops.sliceMin('score', 2, { withTies: false }).args.with_ties === false, 'sliceMin helper should accept withTies=false')
  assert(ops.sliceMax('score', 2, { withTies: false }).args.with_ties === false, 'sliceMax helper should accept withTies=false')
  let rejectedHelperTies = false
  try { ops.sliceMin('score', 2, { withTies: true }) } catch (e) { rejectedHelperTies = String(e.message || e).includes('withTies=true') }
  assert(rejectedHelperTies, 'sliceMin helper should reject withTies=true')

  const head = pipeline([
    codec.csv(),
    ops.sliceHead(2),
    codec.csvEncode(),
  ])
  const headResult = await head.run({ input: threeRows })
  assert(headResult.outputText.trim() === ['name,score', 'Alice,85', 'Bob,92'].join(nl), 'sliceHead should keep first rows')

  const tail = pipeline([
    codec.csv(),
    ops.sliceTail(2),
    codec.csvEncode(),
  ])
  const tailResult = await tail.run({ input: threeRows })
  assert(tailResult.outputText.trim() === ['name,score', 'Bob,92', 'Charlie,78'].join(nl), 'sliceTail should keep last rows')
})
await test('sort head compile rewrite', async () => {
  const recipe = await compileDsl('csv | sort -score | head 2 | csv')
  const data = JSON.parse(recipe)
  const opsSeen = data.steps.map(step => step.op)
  assert(!opsSeen.includes('sort'), 'sort should be rewritten')
  assert(!opsSeen.includes('head'), 'head should be consumed by rewrite')
  assert(data.steps[1].op === 'top', 'descending sort/head rewrites to top')
  assert(data.steps[1].memory_class === 'bounded_state', 'rewrite has bounded memory metadata')
})

await test('datetime', async () => {
  const p = pipeline([
    codec.csv(),
    ops.datetime('date', ['year', 'month']),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'date\n2024-03-15\n2023-12-01\n' })
  const text = result.outputText
  assert(text.includes('date_year'), 'should have date_year')
  assert(text.includes('date_month'), 'should have date_month')
  assert(text.includes('2024'), 'should have 2024')
})

await test('window', async () => {
  const p = pipeline([
    codec.csv(),
    ops.window('val', 2, 'sum', 'val_sum2'),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'val\n10\n20\n30\n' })
  const text = result.outputText
  assert(text.includes('val_sum2'), 'should have val_sum2 column')
  assert(text.includes('50'), 'should have 50 (20+30)')
})


await test('rolling aliases', async () => {
  assert(ops.rollingSum('val', 3, { result: 'sum3' }).op === 'rolling-sum', 'rollingSum helper should use rolling-sum op')
  assert(ops.rollingMean('val', 3, { result: 'mean3' }).op === 'rolling-mean', 'rollingMean helper should use rolling-mean op')
  assert(ops.rollingMin('val', 3, { result: 'min3' }).op === 'rolling-min', 'rollingMin helper should use rolling-min op')
  assert(ops.rollingMax('val', 3, { result: 'max3' }).op === 'rolling-max', 'rollingMax helper should use rolling-max op')
  assert(ops.rollingAny('flag', 3, { result: 'any3' }).op === 'rolling-any', 'rollingAny helper should use rolling-any op')
  assert(ops.rollingAll('flag', 3, { result: 'all3', nulls: 'propagate' }).args.nulls === 'propagate', 'rollingAll helper should set nulls')
  const result = await pipeline([
    codec.csv({ batchSize: 2 }),
    ops.rollingSum('val', 3, { result: 'sum3' }),
    ops.rollingMean('val', 3, { result: 'mean3' }),
    ops.rollingMin('val', 3, { result: 'min3' }),
    ops.rollingMax('val', 3, { result: 'max3' }),
    codec.csvEncode(),
  ]).run({ input: 'val\n10\n20\n30\n5\n' })
  const lines = result.outputText.trim().split('\n')
  assert(lines[0] === 'val,sum3,mean3,min3,max3', 'rolling header')
  assert(lines[1] === '10,10,10,10,10', 'rolling first row')
  assert(lines[2] === '20,30,15,10,20', 'rolling second row')
  assert(lines[3] === '30,60,20,10,30', 'rolling third row')
  assert(lines[4].startsWith('5,55,18.333'), 'rolling crosses chunk boundary')

  const boolResult = await pipeline([
    codec.csv({ batchSize: 2 }),
    ops.rollingAny('flag', 3, { result: 'any3' }),
    ops.rollingAll('flag', 3, { result: 'all3' }),
    codec.csvEncode(),
  ]).run({ input: 'flag\ntrue\nfalse\ntrue\ntrue\n' })
  const boolLines = boolResult.outputText.trim().split('\n')
  assert(boolLines[0] === 'flag,any3,all3', 'rolling bool header')
  assert(boolLines[1] === 'true,true,true', 'rolling bool first row')
  assert(boolLines[2] === 'false,true,false', 'rolling bool second row')
  assert(boolLines[4] === 'true,true,false', 'rolling bool crosses chunk boundary')

  const nullResult = await pipeline([
    codec.csv({ batchSize: 2 }),
    ops.rollingAny('flag', 3, { result: 'any_prop', nulls: 'propagate' }),
    ops.rollingAll('flag', 3, { result: 'all_false', nulls: 'false' }),
    codec.csvEncode(),
  ]).run({ input: 'id,flag\n1,true\n2,\n3,false\n' })
  const nullLines = nullResult.outputText.trim().split('\n')
  assert(nullLines[0] === 'id,flag,any_prop,all_false', 'rolling bool null header')
  assert(nullLines[1] === '1,true,true,true', 'rolling bool null first row')
  assert(nullLines[2] === '2,,,false', 'rolling bool propagates null')
  assert(nullLines[3] === '3,false,,false', 'rolling bool keeps false null policy')
})

await test('hash', async () => {
  const p = pipeline([
    codec.csv(),
    ops.hash(['name']),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'name,age\nAlice,30\nBob,25\n' })
  const text = result.outputText
  assert(text.includes('_hash'), 'should have _hash column')
})

await test('H14 numeric missing/type policies', async () => {
  assert(ops.bin('x', [10], { missing: 'null', onTypeError: 'null' }).args.on_type_error === 'null', 'bin helper should expose type policy')
  assert(ops.ewma('x', 0.5, { missing: 'null' }).args.missing === 'null', 'ewma helper should expose missing policy')
  assert(ops.anomaly('x', { onTypeError: 'null' }).args.on_type_error === 'null', 'anomaly helper should expose type policy')
  assert(ops.window('x', 3, 'avg', { missing: 'null', onTypeError: 'null' }).args.on_type_error === 'null', 'window helper should expose type policy')
  assert(ops.rollingSum('x', 3, { result: 'sum3', missing: 'null', onTypeError: 'null' }).args.missing === 'null', 'rolling helper should expose missing policy')
  assert(ops.interpolate('x', { method: 'forward', missing: 'null', onTypeError: 'null' }).args.on_type_error === 'null', 'interpolate helper should expose type policy')
  assert(ops.datetime('x', { extract: ['year'], missing: 'null', onTypeError: 'null' }).args.on_type_error === 'null', 'datetime helper should expose type policy')
  assert(ops.dateTrunc('x', 'month', { result: 'x_month', missing: 'null', onTypeError: 'null' }).args.missing === 'null', 'dateTrunc helper should expose missing policy')
  assert(ops.normalize(['x'], { missing: 'null', onTypeError: 'null' }).args.on_type_error === 'null', 'normalize helper should expose type policy')
  assert(ops.acf('x', { lags: 2, missing: 'null', onTypeError: 'null' }).args.missing === 'null', 'acf helper should expose missing policy')

  await assertRejects(() => pipeline([codec.csv(), ops.ewma('missing', 0.5), codec.csvEncode()]).run({ input: 'x\n1\n' }), /ewma: column 'missing' not found/)
  let result = await pipeline([codec.csv(), ops.ewma('missing', 0.5, { missing: 'null' }), codec.csvEncode()]).run({ input: 'x\n1\n' })
  assert(JSON.stringify(result.outputText.trim().split('\n')) === JSON.stringify(['x,missing_ewma', '1,']), 'ewma missing=null should append nulls')
  result = await pipeline([codec.csv(), ops.ewma('missing', 0.5, { missing: 'ignore' }), codec.csvEncode()]).run({ input: 'x\n1\n' })
  assert(JSON.stringify(result.outputText.trim().split('\n')) === JSON.stringify(['x', '1']), 'ewma missing=ignore should pass through')
  await assertRejects(() => pipeline([codec.csv(), ops.ewma('x', 0.5), codec.csvEncode()]).run({ input: 'x\na\n' }), /ewma: column 'x' must be numeric/)
  result = await pipeline([codec.csv(), ops.ewma('x', 0.5, { onTypeError: 'null' }), codec.csvEncode()]).run({ input: 'x\na\n' })
  assert(JSON.stringify(result.outputText.trim().split('\n')) === JSON.stringify(['x,x_ewma', 'a,']), 'ewma on_type_error=null should append nulls')

  await assertRejects(() => pipeline([codec.csv(), ops.anomaly('missing'), codec.csvEncode()]).run({ input: 'x\n1\n' }), /anomaly: column 'missing' not found/)
  result = await pipeline([codec.csv(), ops.anomaly('missing', { missing: 'null' }), codec.csvEncode()]).run({ input: 'x\n1\n' })
  assert(JSON.stringify(result.outputText.trim().split('\n')) === JSON.stringify(['x,missing_anomaly', '1,']), 'anomaly missing=null should append nulls')
  await assertRejects(() => pipeline([codec.csv(), ops.anomaly('x'), codec.csvEncode()]).run({ input: 'x\na\n' }), /anomaly: column 'x' must be numeric/)
  result = await pipeline([codec.csv(), ops.anomaly('x', { onTypeError: 'null' }), codec.csvEncode()]).run({ input: 'x\na\n' })
  assert(JSON.stringify(result.outputText.trim().split('\n')) === JSON.stringify(['x,x_anomaly', 'a,']), 'anomaly on_type_error=null should append nulls')

  await assertRejects(() => pipeline([codec.csv(), ops.bin('missing', [10]), codec.csvEncode()]).run({ input: 'x\n1\n' }), /bin: column 'missing' not found/)
  result = await pipeline([codec.csv(), ops.bin('missing', [10], { missing: 'null' }), codec.csvEncode()]).run({ input: 'x\n1\n' })
  assert(JSON.stringify(result.outputText.trim().split('\n')) === JSON.stringify(['x,missing_bin', '1,']), 'bin missing=null should append nulls')
  result = await pipeline([codec.csv(), ops.bin('missing', [10], { missing: 'ignore' }), codec.csvEncode()]).run({ input: 'x\n1\n' })
  assert(JSON.stringify(result.outputText.trim().split('\n')) === JSON.stringify(['x', '1']), 'bin missing=ignore should pass through')
  await assertRejects(() => pipeline([codec.csv(), ops.bin('x', [10]), codec.csvEncode()]).run({ input: 'x\na\n' }), /bin: column 'x' must be numeric/)
  result = await pipeline([codec.csv(), ops.bin('x', [10], { onTypeError: 'null' }), codec.csvEncode()]).run({ input: 'x\na\n' })
  assert(JSON.stringify(result.outputText.trim().split('\n')) === JSON.stringify(['x,x_bin', 'a,']), 'bin on_type_error=null should append nulls')

  await assertRejects(() => pipeline([codec.csv(), ops.window('missing', 3, 'avg'), codec.csvEncode()]).run({ input: 'x\n1\n' }), /window: column 'missing' not found/)
  result = await pipeline([codec.csv(), ops.window('missing', 3, 'avg', { missing: 'null' }), codec.csvEncode()]).run({ input: 'x\n1\n' })
  assert(JSON.stringify(result.outputText.trim().split('\n')) === JSON.stringify(['x,missing_avg3', '1,']), 'window missing=null should append nulls')
  result = await pipeline([codec.csv(), ops.window('missing', 3, 'avg', { missing: 'ignore' }), codec.csvEncode()]).run({ input: 'x\n1\n' })
  assert(JSON.stringify(result.outputText.trim().split('\n')) === JSON.stringify(['x', '1']), 'window missing=ignore should pass through')
  await assertRejects(() => pipeline([codec.csv(), ops.window('x', 3, 'avg'), codec.csvEncode()]).run({ input: 'x\na\n' }), /window: column 'x' must be numeric/)
  result = await pipeline([codec.csv(), ops.window('x', 3, 'avg', { result: 'x_avg', onTypeError: 'null' }), codec.csvEncode()]).run({ input: 'x\na\n' })
  assert(JSON.stringify(result.outputText.trim().split('\n')) === JSON.stringify(['x,x_avg', 'a,']), 'window on_type_error=null should append nulls')
  result = await pipeline([codec.csv(), ops.window('name', 3, 'count', 'name_count'), codec.csvEncode()]).run({ input: 'name\nAlice\nBob\n' })
  assert(JSON.stringify(result.outputText.trim().split('\n')) === JSON.stringify(['name,name_count', 'Alice,1', 'Bob,2']), 'window count should accept nonnumeric input')

  await assertRejects(() => pipeline([codec.csv(), ops.rollingSum('missing', 3, { result: 'sum3' }), codec.csvEncode()]).run({ input: 'x\n1\n' }), /rolling-sum: column 'missing' not found/)
  result = await pipeline([codec.csv(), ops.rollingSum('missing', 3, { result: 'sum3', missing: 'null' }), codec.csvEncode()]).run({ input: 'x\n1\n' })
  assert(JSON.stringify(result.outputText.trim().split('\n')) === JSON.stringify(['x,sum3', '1,']), 'rolling missing=null should append nulls')
  result = await pipeline([codec.csv(), ops.rollingSum('missing', 3, { result: 'sum3', missing: 'ignore' }), codec.csvEncode()]).run({ input: 'x\n1\n' })
  assert(JSON.stringify(result.outputText.trim().split('\n')) === JSON.stringify(['x', '1']), 'rolling missing=ignore should pass through')
  await assertRejects(() => pipeline([codec.csv(), ops.rollingSum('x', 3, { result: 'sum3' }), codec.csvEncode()]).run({ input: 'x\na\n' }), /rolling-sum: column 'x' must be numeric/)
  result = await pipeline([codec.csv(), ops.rollingMean('x', 3, { result: 'mean3', onTypeError: 'null' }), codec.csvEncode()]).run({ input: 'x\na\n' })
  assert(JSON.stringify(result.outputText.trim().split('\n')) === JSON.stringify(['x,mean3', 'a,']), 'rolling on_type_error=null should append nulls')

  await assertRejects(() => pipeline([codec.csv(), ops.interpolate('missing', { method: 'forward' }), codec.csvEncode()]).run({ input: 'x\n1\n', allowBlocking: true }), /interpolate: column 'missing' not found/)
  result = await pipeline([codec.csv(), ops.interpolate('missing', { method: 'forward', missing: 'null' }), codec.csvEncode()]).run({ input: 'x\n1\n', allowBlocking: true })
  assert(JSON.stringify(result.outputText.trim().split('\n')) === JSON.stringify(['x,missing', '1,']), 'interpolate missing=null should append nulls')
  result = await pipeline([codec.csv(), ops.interpolate('missing', { method: 'forward', missing: 'ignore' }), codec.csvEncode()]).run({ input: 'x\n1\n', allowBlocking: true })
  assert(JSON.stringify(result.outputText.trim().split('\n')) === JSON.stringify(['x', '1']), 'interpolate missing=ignore should pass through')
  await assertRejects(() => pipeline([codec.csv(), ops.interpolate('x', { method: 'forward' }), codec.csvEncode()]).run({ input: 'x\na\n', allowBlocking: true }), /interpolate: column 'x' must be numeric/)
  result = await pipeline([codec.csv(), ops.interpolate('x', { method: 'forward', onTypeError: 'null' }), codec.csvEncode()]).run({ input: 'x\na\n', allowBlocking: true })
  assert(JSON.stringify(result.outputText.split('\n').slice(0, 2)) === JSON.stringify(['x', '']), 'interpolate on_type_error=null should null source values')

  await assertRejects(() => pipeline([codec.csv(), ops.datetime('missing', ['year']), codec.csvEncode()]).run({ input: 'x\n1\n' }), /datetime: column 'missing' not found/)
  result = await pipeline([codec.csv(), ops.datetime('missing', { extract: ['year'], missing: 'null' }), codec.csvEncode()]).run({ input: 'x\n1\n' })
  assert(JSON.stringify(result.outputText.trim().split('\n')) === JSON.stringify(['x,missing_year', '1,']), 'datetime missing=null should append nulls')
  result = await pipeline([codec.csv(), ops.datetime('missing', { extract: ['year'], missing: 'ignore' }), codec.csvEncode()]).run({ input: 'x\n1\n' })
  assert(JSON.stringify(result.outputText.trim().split('\n')) === JSON.stringify(['x', '1']), 'datetime missing=ignore should pass through')
  await assertRejects(() => pipeline([codec.jsonl(), ops.datetime('x', ['year']), codec.csvEncode()]).run({ input: '{"x":true}\n' }), /datetime: column 'x' must be string, date, or timestamp/)
  result = await pipeline([codec.jsonl(), ops.datetime('x', { extract: ['year'], onTypeError: 'null' }), codec.csvEncode()]).run({ input: '{"x":true}\n' })
  assert(JSON.stringify(result.outputText.trim().split('\n')) === JSON.stringify(['x,x_year', 'true,']), 'datetime on_type_error=null should append nulls')

  await assertRejects(() => pipeline([codec.csv(), ops.dateTrunc('missing', 'month'), codec.csvEncode()]).run({ input: 'x\n1\n' }), /date-trunc: column 'missing' not found/)
  result = await pipeline([codec.csv(), ops.dateTrunc('missing', 'month', { result: 'month_start', missing: 'null' }), codec.csvEncode()]).run({ input: 'x\n1\n' })
  assert(JSON.stringify(result.outputText.trim().split('\n')) === JSON.stringify(['x,month_start', '1,']), 'dateTrunc missing=null should append nulls')
  result = await pipeline([codec.csv(), ops.dateTrunc('missing', 'month', { missing: 'ignore' }), codec.csvEncode()]).run({ input: 'x\n1\n' })
  assert(JSON.stringify(result.outputText.trim().split('\n')) === JSON.stringify(['x', '1']), 'dateTrunc missing=ignore should pass through')
  await assertRejects(() => pipeline([codec.jsonl(), ops.dateTrunc('x', 'month'), codec.csvEncode()]).run({ input: '{"x":true}\n' }), /date-trunc: column 'x' must be string, date, or timestamp/)
  result = await pipeline([codec.jsonl(), ops.dateTrunc('x', 'month', { onTypeError: 'null' }), codec.csvEncode()]).run({ input: '{"x":true}\n' })
  assert(JSON.stringify(result.outputText.split('\n').slice(0, 2)) === JSON.stringify(['x', '']), 'dateTrunc on_type_error=null should null source values')

  await assertRejects(() => pipeline([codec.csv(), ops.normalize(['missing']), codec.csvEncode()]).run({ input: 'x\n1\n', allowBlocking: true }), /normalize: column 'missing' not found/)
  result = await pipeline([codec.csv(), ops.normalize(['missing'], { missing: 'null' }), codec.csvEncode()]).run({ input: 'x\n1\n', allowBlocking: true })
  assert(JSON.stringify(result.outputText.trim().split('\n')) === JSON.stringify(['x,missing', '1,']), 'normalize missing=null should append nulls')
  result = await pipeline([codec.csv(), ops.normalize(['missing'], { missing: 'ignore' }), codec.csvEncode()]).run({ input: 'x\n1\n', allowBlocking: true })
  assert(JSON.stringify(result.outputText.trim().split('\n')) === JSON.stringify(['x', '1']), 'normalize missing=ignore should pass through')
  await assertRejects(() => pipeline([codec.jsonl(), ops.normalize(['x']), codec.csvEncode()]).run({ input: '{"x":true}\n', allowBlocking: true }), /normalize: column 'x' must be numeric/)
  result = await pipeline([codec.jsonl(), ops.normalize(['x'], { onTypeError: 'null' }), codec.csvEncode()]).run({ input: '{"x":true}\n', allowBlocking: true })
  assert(JSON.stringify(result.outputText.split('\n').slice(0, 2)) === JSON.stringify(['x', '']), 'normalize on_type_error=null should null source values')

  await assertRejects(() => pipeline([codec.csv(), ops.acf('missing', { lags: 2 }), codec.csvEncode()]).run({ input: 'x\n1\n', allowBlocking: true }), /acf: column 'missing' not found/)
  result = await pipeline([codec.csv(), ops.acf('missing', { lags: 2, missing: 'null' }), codec.csvEncode()]).run({ input: 'x\n1\n', allowBlocking: true })
  assert(JSON.stringify(result.outputText.trim().split('\n')) === JSON.stringify(['lag,acf', '0,', '1,', '2,']), 'acf missing=null should emit null acf rows')
  result = await pipeline([codec.csv(), ops.acf('missing', { lags: 2, missing: 'ignore' }), codec.csvEncode()]).run({ input: 'x\n1\n', allowBlocking: true })
  assert(result.outputText === '', 'acf missing=ignore should emit no aggregate')
  await assertRejects(() => pipeline([codec.jsonl(), ops.acf('x', { lags: 2 }), codec.csvEncode()]).run({ input: '{"x":true}\n', allowBlocking: true }), /acf: column 'x' must be numeric/)
  result = await pipeline([codec.jsonl(), ops.acf('x', { lags: 2, onTypeError: 'null' }), codec.csvEncode()]).run({ input: '{"x":true}\n', allowBlocking: true })
  assert(JSON.stringify(result.outputText.trim().split('\n')) === JSON.stringify(['lag,acf', '0,', '1,', '2,']), 'acf on_type_error=null should emit null acf rows')

  await assertRejects(() => pipeline([codec.csv(), ops.ewma('x', 2), codec.csvEncode()]).run({ input: 'x\n1\n' }), /ewma: alpha must be a finite number between 0 and 1/)
  await assertRejects(() => pipeline([codec.csv(), ops.anomaly('x', { threshold: -1 }), codec.csvEncode()]).run({ input: 'x\n1\n' }), /anomaly: threshold must be a non-negative finite number/)
  await assertRejects(() => pipeline([codec.csv(), ops.bin('x', [10, 10]), codec.csvEncode()]).run({ input: 'x\n1\n' }), /bin: boundaries must be finite, strictly increasing numbers/)
  await assertRejects(() => pipeline([codec.csv(), ops.window('x', 0, 'avg'), codec.csvEncode()]).run({ input: 'x\n1\n' }), /window: size must be an integer/)
  await assertRejects(() => pipeline([codec.csv(), ops.window('x', 3, 'nope'), codec.csvEncode()]).run({ input: 'x\n1\n' }), /window: func must be avg, sum, min, max, or count/)
  await assertRejects(() => pipeline([codec.csv(), ops.interpolate('x', { method: 'nearest' }), codec.csvEncode()]).run({ input: 'x\n1\n', allowBlocking: true }), /interpolate: method must be forward, backward, or linear/)
  await assertRejects(() => pipeline([codec.csv(), ops.datetime('x', ['decade']), codec.csvEncode()]).run({ input: 'x\n2024-01-01\n' }), /datetime: extract must contain year/)
  await assertRejects(() => pipeline([codec.csv(), ops.dateTrunc('x', 'decade'), codec.csvEncode()]).run({ input: 'x\n2024-01-01\n' }), /date-trunc: trunc must be year/)
  await assertRejects(() => pipeline([codec.csv(), ops.normalize(['x'], { method: 'bad' }), codec.csvEncode()]).run({ input: 'x\n1\n', allowBlocking: true }), /normalize: method must be minmax or zscore/)
  await assertRejects(() => pipeline([codec.csv(), ops.acf('x', { lags: 0 }), codec.csvEncode()]).run({ input: 'x\n1\n', allowBlocking: true }), /acf: lags must be an integer/)
})

await test('sample seed determinism', async () => {
  const input = 'name\nAlice\nBob\nCharlie\nDiana\nEve\nFrank\nGrace\n'
  const defaultStep = ops.sample(3)
  assert(!Object.prototype.hasOwnProperty.call(defaultStep.args, 'seed'), 'sample default should omit seed')
  const seededStep = ops.sample(3, { seed: 123 })
  assert(seededStep.args.seed === 123, 'sample helper should set numeric seed')
  const randomStep = ops.sample(3, { seed: 'random' })
  assert(randomStep.args.seed === 'random', 'sample helper should set random seed mode')

  const run = async (step, chunkSize) => {
    const result = await pipeline([
      codec.csv({ batchSize: 2 }),
      step,
      codec.csvEncode(),
    ]).run({ input, chunkSize })
    return result.outputText.trim().split('\n')
  }

  const defaultA = await run(defaultStep, 7)
  const defaultB = await run(ops.sample(3), 3)
  assert(JSON.stringify(defaultA) === JSON.stringify(defaultB), 'default sample should be stable across chunk cuts')
  assert(JSON.stringify(defaultA) === JSON.stringify(['name', 'Eve', 'Frank', 'Charlie']), 'default sample rows should be deterministic')

  const seedA = await run(seededStep, 7)
  const seedB = await run(ops.sample(3, { seed: 123 }), 3)
  assert(JSON.stringify(seedA) === JSON.stringify(seedB), 'same seed should be stable across chunk cuts')
  assert(JSON.stringify(seedA) === JSON.stringify(['name', 'Alice', 'Grace', 'Charlie']), 'seeded sample rows should be deterministic')
  const seedOther = await run(ops.sample(3, { seed: 124 }), 7)
  assert(JSON.stringify(seedOther) === JSON.stringify(['name', 'Alice', 'Grace', 'Eve']), 'different seed should produce known rows')
})

console.log('\nText + Grep:')

await test('text passthrough', async () => {
  const p = pipeline([
    codec.text(),
    codec.textEncode(),
  ])
  const result = await p.run({ input: 'hello world\nfoo bar\n' })
  const text = result.outputText
  assert(text.includes('hello world'), 'should have hello world')
  assert(text.includes('foo bar'), 'should have foo bar')
})

await test('text max record bytes', async () => {
  const step = codec.textDecode({ maxRecordBytes: 8, maxErrorBytes: 5 })
  assert(step.args.max_record_bytes === 8, 'helper should expose max_record_bytes')
  assert(step.args.max_error_bytes === 5, 'helper should expose max_error_bytes')

  const p = pipeline([
    codec.text({ maxRecordBytes: 8, maxErrorBytes: 5 }),
    codec.textEncode(),
  ])
  await assertRejects(
    () => p.run({ input: 'ok\n123456789', chunkSize: 4 }),
    /text record exceeds max_record_bytes/
  )
})

await test('text + grep', async () => {
  const p = pipeline([
    codec.text(),
    ops.grep('error'),
    codec.textEncode(),
  ])
  const result = await p.run({ input: 'info: started\nerror: something failed\ninfo: done\nerror: another\n' })
  const text = result.outputText
  assert(text.includes('error: something failed'), 'should have error line')
  assert(text.includes('error: another'), 'should have second error')
  assert(!text.includes('info: started'), 'should not have info lines')
  assert(!text.includes('info: done'), 'should not have info done')
})

await test('text + grep -v', async () => {
  const p = pipeline([
    codec.text(),
    ops.grep('error', { invert: true }),
    codec.textEncode(),
  ])
  const result = await p.run({ input: 'info: started\nerror: failed\ninfo: done\n' })
  const text = result.outputText
  assert(text.includes('info: started'), 'should have info started')
  assert(text.includes('info: done'), 'should have info done')
  assert(!text.includes('error'), 'should not have error lines')
})

await test('text + head', async () => {
  const p = pipeline([
    codec.text(),
    ops.head(2),
    codec.textEncode(),
  ])
  const result = await p.run({ input: 'line1\nline2\nline3\nline4\n' })
  const text = result.outputText
  assert(text.includes('line1'), 'should have line1')
  assert(text.includes('line2'), 'should have line2')
  assert(!text.includes('line3'), 'should not have line3')
})

console.log('\nNew Ops (Phase 2):')

await test('lead offset 1', async () => {
  const p = pipeline([
    codec.csv(),
    ops.lead('val', { result: 'next_val' }),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'val\n10\n20\n30\n' })
  const text = result.outputText
  assert(text.includes('next_val'), 'should have next_val column')
  assert(text.includes('20'), 'first row lead should be 20')
})

await test('lead offset 2', async () => {
  const p = pipeline([
    codec.csv(),
    ops.lead('val', { offset: 2, result: 'val_lead2' }),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'val\n10\n20\n30\n40\n' })
  const text = result.outputText
  assert(text.includes('val_lead2'), 'should have val_lead2 column')
  assert(text.includes('30'), 'first row lead2 should be 30')
})



await test('lag offset 2 preserves chunks', async () => {
  const args = ops.lag('val', { offset: 2, result: 'prev_val' }).args
  assert(args.column === 'val', 'lag helper should set column')
  assert(args.offset === 2, 'lag helper should set offset')
  assert(args.result === 'prev_val', 'lag helper should set result')

  const p = pipeline([
    codec.csv({ batchSize: 2 }),
    ops.lag('val', { offset: 2, result: 'prev_val' }),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'id,val\n1,10\n2,20\n3,30\n4,40\n' })
  const lines = result.outputText.trim().split('\n')
  assert(lines[0] === 'id,val,prev_val', 'should append prev_val')
  assert(lines[1] === '1,10,', 'first lag is null')
  assert(lines[2] === '2,20,', 'second lag is null')
  assert(lines[3] === '3,30,10', 'third row lags first')
  assert(lines[4] === '4,40,20', 'fourth row lags second')
})

await test('shift lead large offset preserves chunks', async () => {
  const args = ops.shift('val', { offset: 3, result: 'next_val', type: 'lead' }).args
  assert(args.type === 'lead', 'shift helper should pass lead type')

  const p = pipeline([
    codec.csv({ batchSize: 2 }),
    ops.shift('val', { offset: 3, result: 'next_val', type: 'lead' }),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'id,val\n1,10\n2,20\n3,30\n4,40\n5,50\n' })
  const lines = result.outputText.trim().split('\n')
  assert(lines[1] === '1,10,40', 'first row leads to fourth')
  assert(lines[2] === '2,20,50', 'second row leads to fifth')
  assert(lines[3] === '3,30,', 'tail lead is null')
})


await test('rowid grouped and sorted modes', async () => {
  const args = ops.rowid(['x'], { result: 'x_row', maxKeys: 3 }).args
  assert(args.columns.length === 1 && args.columns[0] === 'x', 'rowid helper should set columns')
  assert(args.result === 'x_row', 'rowid helper should set result')
  assert(args.max_keys === 3, 'rowid helper should set max_keys')

  const p = pipeline([
    codec.csv({ batchSize: 2 }),
    ops.rowid('x', { result: 'x_row', maxKeys: 3 }),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'x,y\n20,a\n10,a\n10,a\n30,b\n30,b\n20,b\n' })
  const lines = result.outputText.trim().split('\n')
  assert(lines[0] === 'x,y,x_row', 'should append x_row')
  assert(lines[1] === '20,a,1', 'first 20 is row 1')
  assert(lines[2] === '10,a,1', 'first 10 is row 1')
  assert(lines[3] === '10,a,2', 'second 10 is row 2')
  assert(lines[6] === '20,b,2', 'second 20 is row 2 across chunks')

  const sorted = pipeline([
    codec.csv({ batchSize: 2 }),
    ops.rowid('x', { result: 'x_run', sorted: true }),
    codec.csvEncode(),
  ])
  const sortedResult = await sorted.run({ input: 'x\nA\nA\nB\nB\nA\n' })
  const sortedLines = sortedResult.outputText.trim().split('\n')
  assert(sortedLines[1] === 'A,1', 'first sorted A')
  assert(sortedLines[2] === 'A,2', 'second sorted A')
  assert(sortedLines[5] === 'A,1', 'non-consecutive A restarts in sorted mode')
})

await test('rleid chunk-boundary runs', async () => {
  const args = ops.rleid(['city', 'status'], { result: 'run_id' }).args
  assert(args.columns.length === 2, 'rleid helper should set columns')
  assert(args.result === 'run_id', 'rleid helper should set result')

  const p = pipeline([
    codec.csv({ batchSize: 2 }),
    ops.rleid(['city', 'status'], { result: 'run_id' }),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'city,status\nA,on\nA,on\nA,off\nB,off\nA,off\nA,off\n' })
  const lines = result.outputText.trim().split('\n')
  assert(lines[0] === 'city,status,run_id', 'should append run_id column')
  assert(lines[1].endsWith(',1'), 'first run id')
  assert(lines[2].endsWith(',1'), 'same run id across first batch')
  assert(lines[3].endsWith(',2'), 'changed status increments')
  assert(lines[4].endsWith(',3'), 'changed city increments')
  assert(lines[5].endsWith(',4'), 'non-consecutive same city/status is new run')
  assert(lines[6].endsWith(',4'), 'same final run continues')
})

await test('date-trunc to month', async () => {
  const p = pipeline([
    codec.csv(),
    ops.dateTrunc('date', 'month'),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'date\n2024-03-15\n2024-03-28\n' })
  const text = result.outputText
  assert(text.includes('2024-03-01'), 'should truncate to 2024-03-01')
  assert(!text.includes('2024-03-15'), 'should not have original date')
})

await test('table encode', async () => {
  const p = pipeline([
    codec.csv(),
    codec.tableEncode(),
  ])
  const result = await p.run({ input: 'name,age\nAlice,30\nBob,25\n', allowBlocking: true })
  const text = result.outputText
  assert(text.includes('|'), 'should have pipe separators')
  assert(text.includes('name'), 'should have name')
  assert(text.includes('Alice'), 'should have Alice')
})

await test('csv repair', async () => {
  const p = pipeline([
    codec.csv({ repair: true }),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'a,b,c\n1,2\n4,5,6\n' })
  const text = result.outputText
  const lines = text.trim().split('\n')
  assert(lines.length === 3, 'should have all rows')
  assert(lines[1].split(',').length === 3, 'short row should be padded')
})

console.log('\nRecipes:')

await test('compileDsl', async () => {
  const recipe = await compileDsl('csv | head 5 | csv')
  const data = JSON.parse(recipe)
  assert(data.steps.length === 3, 'should have 3 steps')
  assert(data.steps[0].op === 'codec.csv.decode', 'first step is csv decode')
  assert(data.steps[1].op === 'head', 'second step is head')
  assert(data.steps[1].memory_class === 'bounded_state', 'head memory metadata')
  assert(data.steps[1].emit_class === 'per_batch', 'head emit metadata')
  assert(data.steps[1].state_estimate === 'O(1)', 'head state estimate')
  assert(data.steps[1].state_bytes_estimate === null, 'head has no retained byte estimate')
  assert(data.steps[1].schema_class === 'stable', 'head schema metadata')
  assert(data.steps[2].op === 'codec.csv.encode', 'third step is csv encode')

  const aliasPlan = JSON.parse(await compileDsl("csv | mutate total=col('price')*2 | arrange -total | distinct city max_keys=7 | summarize city count:*:rows | csv"))
  assert(aliasPlan.steps.map(step => step.op).join(',') === 'codec.csv.decode,derive,sort,unique,group-agg,codec.csv.encode', 'aliases should normalize to canonical ops')
  assert(aliasPlan.steps[3].args.max_keys === 7, 'distinct alias should preserve unique args')
  assert(aliasPlan.steps[3].state_bytes_estimate === 3040, 'capped distinct alias should expose byte estimate')

  const uncappedUnique = JSON.parse(await compileDsl('csv | unique city | csv'))
  assert(uncappedUnique.steps[1].state_bytes_estimate === null, 'uncapped unique should expose null byte estimate')
  assert(String(uncappedUnique.steps[1].state_bytes_reason || '').includes('needs max_keys'), 'uncapped unique should explain missing byte cap')

  const slicePlan = JSON.parse(await compileDsl('csv | slice_head n=2 | slice-tail 1 | csv'))
  assert(slicePlan.steps[1].op === 'slice-head', 'slice_head alias should normalize')
  assert(slicePlan.steps[1].memory_class === 'bounded_state', 'slice-head memory metadata')
  assert(slicePlan.steps[1].emit_class === 'per_batch', 'slice-head emit metadata')
  assert(slicePlan.steps[2].op === 'slice-tail', 'slice-tail should parse')
  assert(slicePlan.steps[2].memory_class === 'bounded_state', 'slice-tail memory metadata')
  assert(slicePlan.steps[2].emit_class === 'on_flush', 'slice-tail emit metadata')

  const joinPlan = JSON.parse(await compileDsl('csv | join lookup.csv on id max_lookup_bytes=1024 max_state_bytes=4096 | csv'))
  assert(joinPlan.steps[1].op === 'join', 'join should parse')
  assert(joinPlan.steps[1].args.max_state_bytes === 4096, 'join should preserve max_state_bytes')

  const sortedBagPlan = JSON.parse(await compileDsl('csv | intersect-all lookup.csv columns=id sorted=true | csv'))
  assert(sortedBagPlan.steps[1].op === 'intersect-all', 'intersect-all should parse')
  assert(sortedBagPlan.steps[1].args.sorted === true, 'sorted bag set should preserve sorted flag')
  assert(sortedBagPlan.steps[1].memory_class === 'bounded_state', 'sorted bag set should be bounded')
  assert(sortedBagPlan.steps[1].state_estimate === 'O(current_lookup_run + current_left_run)', 'sorted bag set state estimate')

  const sortedUnionPlan = JSON.parse(await compileDsl('csv | union lookup.csv columns=id sorted=true | csv'))
  assert(sortedUnionPlan.steps[1].op === 'union', 'sorted union should parse')
  assert(sortedUnionPlan.steps[1].args.sorted === true, 'sorted union should preserve sorted flag')
  assert(sortedUnionPlan.steps[1].memory_class === 'bounded_state', 'sorted union should be bounded')
  assert(sortedUnionPlan.steps[1].emit_class === 'mixed', 'sorted union emit metadata')

  const bagSpillPlan = JSON.parse(await compileDsl('csv | intersect-all lookup.csv columns=id spill_dir=/tmp/tranfi-spill spill_memory_bytes=1024 | csv'))
  assert(bagSpillPlan.steps[1].op === 'intersect-all', 'bag spill set op should parse')
  assert(bagSpillPlan.steps[1].args.spill_dir === '/tmp/tranfi-spill', 'bag spill set op should preserve spill_dir')
  assert(bagSpillPlan.steps[1].memory_class === 'external', 'bag spill set op should be external')
  assert(bagSpillPlan.steps[1].emit_class === 'on_flush', 'bag spill set op should flush')

  const joinSpillPlan = JSON.parse(await compileDsl('csv | join lookup.csv on=id max_matches_per_row=2 spill_dir=/tmp/tranfi-spill spill_memory_bytes=1024 | csv'))
  assert(joinSpillPlan.steps[1].op === 'join', 'join spill op should parse')
  assert(joinSpillPlan.steps[1].args.spill_dir === '/tmp/tranfi-spill', 'join spill should preserve spill_dir')
  assert(joinSpillPlan.steps[1].args.max_matches_per_row === 2, 'join spill should preserve max_matches_per_row')
  assert(joinSpillPlan.steps[1].memory_class === 'external', 'join spill should be external')
  assert(joinSpillPlan.steps[1].emit_class === 'on_flush', 'join spill should flush')
  assert(joinSpillPlan.steps[1].schema_class === 'data_dependent', 'join spill schema should be data-dependent')

  const rankPlan = JSON.parse(await compileDsl('csv | slice-min score n=2 with_ties=false | csv'))
  assert(rankPlan.steps[1].op === 'slice-min', 'slice-min with_ties=false should parse')
  assert(rankPlan.steps[1].args.with_ties === false, 'slice-min should preserve with_ties=false')
  let rejectedTies = false
  try { await compileDsl('csv | slice-min score n=2 with_ties=true | csv') } catch (e) { rejectedTies = String(e.message || e).includes('with_ties=true') }
  assert(rejectedTies, 'compileDsl should reject with_ties=true')
})

await test('saveRecipe + loadRecipe', async () => {
  const steps = [
    codec.csv(),
    ops.head(1),
    codec.csvEncode(),
  ]
  const path = '/tmp/tranfi_test_recipe.tranfi'
  await saveRecipe(steps, path)
  const p = await loadRecipe(path)
  const result = await p.run({ input: 'x\n1\n2\n3\n' })
  assert(result.outputText.includes('1'), 'should have row 1')
  assert(!result.outputText.includes('3'), 'should not have row 3')
  const { unlinkSync } = require('fs')
  unlinkSync(path)
})

await test('loadRecipe from JSON string', async () => {
  const json = '{"steps":[{"op":"codec.csv.decode","args":{}},{"op":"codec.csv.encode","args":{}}]}'
  const p = await loadRecipe(json)
  const result = await p.run({ input: 'x\n1\n' })
  assert(result.outputText.includes('1'), 'should have 1')
})

await test('loadRecipe from object', async () => {
  const p = await loadRecipe({
    steps: [
      { op: 'codec.csv.decode', args: {} },
      { op: 'codec.csv.encode', args: {} },
    ]
  })
  const result = await p.run({ input: 'x\n1\n' })
  assert(result.outputText.includes('1'), 'should have 1')
})

// Built-in recipes tests
console.log('\nBuilt-in Recipes:')

await test('recipes() returns 22 entries', async () => {
  const r = await recipes()
  assert(r.length === 22, `expected 22 recipes, got ${r.length}`)
  const names = r.map(x => x.name)
  assert(names.includes('profile'), 'should include profile')
  assert(names.includes('sniff'), 'should include sniff')
  assert(names.includes('csv2json'), 'should include csv2json')
  for (const item of r) {
    assert(item.name, 'should have name')
    assert(item.dsl, 'should have dsl')
    assert(item.description, 'should have description')
  }
})

await test('pipeline("preview") recipe', async () => {
  const p = pipeline('preview')
  const result = await p.run({ input: 'name,age\nAlice,30\nBob,25\n' })
  assert(result.outputText.includes('Alice'), 'should have Alice')
  assert(result.outputText.includes('Bob'), 'should have Bob')
})

await test('pipeline("sniff") recipe', async () => {
  const p = pipeline('sniff')
  const result = await p.run({ input: 'name,age,score\nAlice,30,91.5\nBob,,88\n' })
  assert(result.outputText.includes('column,type,nullable,non_null'), 'should emit schema report header')
  assert(result.outputText.includes('name,string,false,true'), 'should report string column')
  assert(result.outputText.includes('age,int,true,false'), 'should report nullable int column')
  assert(result.outputText.includes('score,float,false,true'), 'should report float column')
})

await test('pipeline("dedup") recipe', async () => {
  const p = pipeline('dedup')
  const result = await p.run({ input: 'x\n1\n2\n1\n3\n' })
  const lines = result.outputText.trim().split('\n').filter(l => l)
  assert(lines.length === 4, `expected 4 lines (header + 3 unique), got ${lines.length}`)
})

await test('pipeline("csv2json") recipe', async () => {
  const p = pipeline('csv2json')
  const result = await p.run({ input: 'name,age\nAlice,30\n' })
  assert(result.outputText.includes('"name"'), 'should have "name"')
  assert(result.outputText.includes('"Alice"'), 'should have "Alice"')
})

// Server tests
console.log('\nServer:')

const { startServer } = require('../js/src/server.js')
const { writeFileSync, mkdirSync, rmSync, existsSync } = require('fs')

const testDataDir = '/tmp/tranfi-test-serve'
const testAppDir = '/tmp/tranfi-test-serve-app'

// Set up test data and a minimal app shell so API tests do not depend on a generated app/dist.
if (existsSync(testDataDir)) rmSync(testDataDir, { recursive: true })
if (existsSync(testAppDir)) rmSync(testAppDir, { recursive: true })
mkdirSync(testDataDir, { recursive: true })
mkdirSync(testAppDir, { recursive: true })
writeFileSync(join(testAppDir, 'index.html'), '<!doctype html><html><head></head><body>tranfi test app</body></html>')
writeFileSync(join(testDataDir, 'test.csv'), 'name,age\nAlice,30\nBob,25\nCharlie,35\n')
writeFileSync(join(testDataDir, 'data.jsonl'), '{"x":1}\n{"x":2}\n')

const server = startServer({ port: 0, dataDir: testDataDir, appDir: testAppDir })
const addr = server.address()
const base = `http://localhost:${addr.port}`

  await test('GET /api/version', async () => {
    const res = await fetch(`${base}/api/version`)
    const data = await res.json()
    assert(data.version, 'should have version')
  })

  await test('GET /api/files', async () => {
    const res = await fetch(`${base}/api/files`)
    const data = await res.json()
    assert(Array.isArray(data.files), 'should have files array')
    const names = data.files.map(f => f.name)
    assert(names.includes('test.csv'), 'should include test.csv')
    assert(names.includes('data.jsonl'), 'should include data.jsonl')
  })

  await test('GET /api/file preview', async () => {
    const res = await fetch(`${base}/api/file?name=test.csv&head=2`)
    const data = await res.json()
    assert(data.preview.includes('name,age'), 'should have header')
    assert(data.preview.includes('Alice'), 'should have first row')
    assert(!data.preview.includes('Charlie'), 'should not have third row')
  })

  await test('GET /api/file rejects ..', async () => {
    const res = await fetch(`${base}/api/file?name=../etc/passwd`)
    const data = await res.json()
    assert(data.error, 'should return error')
  })

  await test('POST /api/run passthrough', async () => {
    const res = await fetch(`${base}/api/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ file: 'test.csv', dsl: 'csv | csv' })
    })
    const data = await res.json()
    assert(data.output.includes('Alice'), 'output should have Alice')
    assert(data.output.includes('Bob'), 'output should have Bob')
    assert(data.output.includes('Charlie'), 'output should have Charlie')
  })

  await test('POST /api/run filter', async () => {
    const res = await fetch(`${base}/api/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ file: 'test.csv', dsl: 'csv | filter "age > 28" | csv' })
    })
    const data = await res.json()
    assert(data.output.includes('Alice'), 'output should have Alice (30)')
    assert(data.output.includes('Charlie'), 'output should have Charlie (35)')
    assert(!data.output.includes('Bob'), 'output should not have Bob (25)')
  })

  await test('POST /api/run hosted audit omits row payload by default', async () => {
    const res = await fetch(`${base}/api/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ file: 'test.csv', dsl: 'csv | filter "age > 100" audit audit_limit=2 | csv' })
    })
    const data = await res.json()
    assert(data.stats.includes('audit'), 'stats should include audit records')
    assert(!data.stats.includes('Alice'), 'hosted default should omit Alice row payload')
    assert(!data.stats.includes('Bob'), 'hosted default should omit Bob row payload')
    assert(!data.stats.includes('Charlie'), 'hosted default should omit Charlie row payload')
  })

  await test('POST /api/run hosted audit respects explicit row payload opt-in', async () => {
    const res = await fetch(`${base}/api/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ file: 'test.csv', dsl: 'csv | filter "age > 100" audit audit_limit=1 audit_include_row=true | csv' })
    })
    const data = await res.json()
    assert(data.stats.includes('Alice'), 'explicit audit_include_row=true should include row payload')
  })

  await test('POST /api/run missing file', async () => {
    const res = await fetch(`${base}/api/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ file: 'nope.csv', dsl: 'csv | csv' })
    })
    const data = await res.json()
    assert(data.error, 'should return error for missing file')
  })

  await test('GET /api/recipes', async () => {
    const res = await fetch(`${base}/api/recipes`)
    const data = await res.json()
    assert(Array.isArray(data.recipes), 'should have recipes array')
    assert(data.recipes.length > 0, 'should have recipes')
  })

  await test('index.html has server config', async () => {
    const res = await fetch(base)
    const html = await res.text()
    assert(html.includes('__TRANFI_SERVER__'), 'should inject server config')
  })

server.close()
rmSync(testDataDir, { recursive: true })
rmSync(testAppDir, { recursive: true })

// ---- SQL Transpiler ----

console.log('\nSQL Transpiler:')

await test('compileToSql basic', async () => {
  const sql = await compileToSql('csv | head 10 | csv')
  assert(sql.includes('LIMIT 10'), 'should have LIMIT')
})

await test('compileToSql dialect option', async () => {
  const sql = await compileToSql('csv | head 10 | csv', { dialect: 'duckdb' })
  assert(sql.includes('LIMIT 10'), 'duckdb dialect should compile')
  await assertRejects(
    () => compileToSql('csv | head 10 | csv', { dialect: 'sqlite' }),
    /recognized but not implemented/
  )
  await assertRejects(
    () => compileToSql('csv | head 10 | csv', { dialect: 'mysql' }),
    /unknown SQL dialect/
  )
})

await test('compileToSql slice head/tail', async () => {
  const headSql = await compileToSql('csv | slice-head n=2 | csv')
  assert(headSql.includes('LIMIT 2'), 'slice-head should lower to LIMIT')
  const tailSql = await compileToSql('csv | slice_tail 2 | csv')
  assert(tailSql.includes('ROW_NUMBER() OVER'), 'slice-tail should lower through row-number tail')
})

await test('compileToSql filter', async () => {
  const sql = await compileToSql('csv | filter "col(\'age\') > 25" | csv')
  assert(sql.includes('WHERE'), 'should have WHERE')
  assert(sql.includes('"age"'), 'should quote column')
})

await test('compileToSql grep literal metacharacters', async () => {
  const percentSql = await compileToSql('csv | grep % name | csv')
  assert(percentSql.includes('contains(CAST("name" AS VARCHAR), \'%\')'), 'literal grep should use contains for percent')
  assert(!percentSql.includes(' LIKE '), 'literal grep should not lower through LIKE wildcards')

  const underscoreSql = await compileToSql('csv | grep _ name | csv')
  assert(underscoreSql.includes('contains(CAST("name" AS VARCHAR), \'_\')'), 'literal grep should preserve underscore')

  const backslashSql = await compileToSql(String.raw`csv | grep \ name | csv`)
  assert(backslashSql.includes('contains(CAST("name" AS VARCHAR)'), 'literal grep should lower backslash through contains')
  assert(!backslashSql.includes(' LIKE '), 'backslash grep should not lower through LIKE wildcards')
})

await test('compileToSql rejects sample', async () => {
  await assertRejects(
    () => compileToSql('csv | sample 2 seed=42 | csv'),
    /sample.*cannot be lowered to SQL/
  )
})

await test('compileToSql rejects stats', async () => {
  await assertRejects(
    () => compileToSql('csv | stats count,missing,complete_rate | csv'),
    /stats.*cannot be lowered to SQL/
  )
})

await test('compileToSql between', async () => {
  const sql = await compileToSql('csv | filter "between(col(\'age\'), 25, 35)" | csv')
  assert(sql.includes('"age" BETWEEN 25 AND 35'), 'between should lower to SQL BETWEEN')
})

await test('compileToSql case expressions', async () => {
  const caseWhenSql = await compileToSql('csv | derive band=case_when(col(\'age\')<25,\'young\',col(\'age\')<40,\'adult\',\'senior\') | csv')
  assert(caseWhenSql.includes('CASE WHEN'), 'case_when should lower to searched CASE')
  assert(caseWhenSql.includes('ELSE \'senior\''), 'case_when should include default')

  const caseMatchSql = await compileToSql('csv | derive code=case_match(col(\'city\'),\'NY\',\'new-york\',\'LA\',\'los-angeles\',\'other\') | csv')
  assert(caseMatchSql.includes('CASE "city" WHEN \'NY\' THEN \'new-york\''), 'case_match should lower to simple CASE')
  assert(caseMatchSql.includes('ELSE \'other\''), 'case_match should include default')
})

await test('compileToSql date expressions', async () => {
  const sql = await compileToSql(`csv | derive y=year(col('d')) m=date_trunc(col('d'),'month') | csv`)
  assert(sql.includes('EXTRACT(year FROM "d")'), 'year should lower to SQL EXTRACT')
  assert(sql.includes(`date_trunc('month', "d")`), 'date_trunc should lower to SQL date_trunc')
})

await test('compileToSql if_any and if_all', async () => {
  const anySql = await compileToSql('csv | filter "if_any(col(\'a\') > 0, col(\'b\') > 0)" | csv')
  assert(anySql.includes(' OR '), 'if_any should lower to OR')
  assert(anySql.includes('WHERE'), 'if_any should be valid in filter')

  const allSql = await compileToSql('csv | filter "if_all(col(\'a\') > 0, col(\'b\') > 0)" | csv')
  assert(allSql.includes(' AND '), 'if_all should lower to AND')
  assert(allSql.includes('WHERE'), 'if_all should be valid in filter')
})

await test('compileToSql select', async () => {
  const sql = await compileToSql('csv | select name,age | csv')
  assert(sql.includes('"name"'), 'should have name')
  assert(sql.includes('"age"'), 'should have age')
})

await test('compileToSql trim explicit columns', async () => {
  const sql = await compileToSql('csv | trim name,city | csv')
  assert(sql.includes('trim("name")'), 'explicit trim should lower name column')
  assert(sql.includes('trim("city")'), 'explicit trim should lower city column')
})

await test('compileToSql rejects trim without explicit columns', async () => {
  try {
    await compileToSql('csv | trim | csv')
    assert(false, 'trim without columns should not lower silently')
  } catch (err) {
    assert(String(err.message || err).includes('requires explicit columns'), 'should explain explicit-column requirement')
  }
})

await test('compileToSql rejects frequency overflow other', async () => {
  try {
    await compileToSql('csv | frequency city max_values=2 overflow=other other=REST | csv')
    assert(false, 'frequency overflow SQL lowering should throw')
  } catch (err) {
    assert(String(err.message || err).includes('overflow=other is not supported by SQL lowering'), 'should explain unsupported overflow lowering')
  }
})

await test('compileToSql frequency emits native-shaped value key', async () => {
  const sql = await compileToSql('csv | frequency a,b | csv')
  assert(sql.includes('AS "value"'), 'frequency SQL should project a native-shaped value column')
  assert(sql.includes('chr(1)'), 'multi-column frequency SQL should use the native key separator')
  assert(sql.includes('GROUP BY "value"'), 'frequency SQL should group on the serialized value key')
})

await test('compileToSql rejects frequency without explicit columns', async () => {
  try {
    await compileToSql('csv | frequency | csv')
    assert(false, 'frequency without columns should not lower silently')
  } catch (err) {
    assert(String(err.message || err).includes('requires explicit columns'), 'should explain explicit-column requirement')
  }
})

await test('compileToSql rejects non-global unique modes', async () => {
  try {
    await compileToSql('csv | unique city mode=approx bloom_bytes=65536 | csv')
    assert(false, 'approx unique SQL lowering should throw')
  } catch (err) {
    assert(String(err.message || err).includes('approximate mode cannot be lowered to SQL'), 'should explain approximate unique lowering')
  }
  try {
    await compileToSql('csv | unique city sorted=true | csv')
    assert(false, 'sorted unique SQL lowering should throw')
  } catch (err) {
    assert(String(err.message || err).includes('sorted=true adjacent-run mode cannot be lowered to SQL'), 'should explain sorted unique lowering')
  }
})

await test('compileToSql rejects selector helpers without schema', async () => {
  try {
    await compileToSql('csv | select starts_with(score_) | csv')
    assert(false, 'selector SQL lowering should throw')
  } catch (err) {
    assert(String(err.message || err).includes('selector helpers require known schema'), 'should explain schema requirement')
  }
})

await test('compileToSql relocate default-front', async () => {
  const sql = await compileToSql('csv | relocate score | csv')
  assert(sql.includes('"score", * EXCLUDE ("score")'), 'default relocate should move columns to the front')
})

await test('compileToSql derive', async () => {
  const sql = await compileToSql("csv | derive total=col('a')+col('b') | csv")
  assert(sql.includes('"total"'), 'should have derived column')
})



await test('compileToSql group-agg count star', async () => {
  const sql = await compileToSql('csv | group-agg city count:*:rows count:amount:n | csv')
  assert(sql.includes('COUNT(*) AS "rows"'), 'count:* should lower to COUNT(*)')
  assert(sql.includes('COUNT("amount") AS "n"'), 'count:column should lower to COUNT(column)')
})

await test('compileToSql rowid', async () => {
  const sql = await compileToSql('csv | rowid city,status result=within_key | csv')
  assert(sql.includes('ROW_NUMBER() OVER'), 'rowid should use row_number window function')
  assert(sql.includes('PARTITION BY "city", "status"'), 'rowid should partition by selected columns')
  assert(sql.includes('"within_key"'), 'rowid should name result column')
})

await test('compileToSql lag and shift lead', async () => {
  const lagSql = await compileToSql('csv | lag val 2 prev_val | csv')
  assert(lagSql.includes('LAG("val", 2)'), 'lag should lower to LAG window function')
  assert(lagSql.includes('"prev_val"'), 'lag should name result')
  const leadSql = await compileToSql('csv | shift val 3 next_val type=lead | csv')
  assert(leadSql.includes('LEAD("val", 3)'), 'shift type=lead should lower to LEAD')
  assert(leadSql.includes('"next_val"'), 'shift lead should name result')
})

await test('compileToSql rolling aliases', async () => {
  const sql = await compileToSql('csv | rolling-sum val 3 sum3 | rolling-mean val 3 mean3 | csv')
  assert(sql.includes('SUM("val") OVER'), 'rolling-sum should lower to SUM window')
  assert(sql.includes('AVG("val") OVER'), 'rolling-mean should lower to AVG window')
  assert(sql.includes('ROWS BETWEEN 2 PRECEDING AND CURRENT ROW'), 'rolling aliases should use trailing window frame')
  assert(sql.includes('"sum3"'), 'rolling-sum should name result')
  assert(sql.includes('"mean3"'), 'rolling-mean should name result')

  const boolSql = await compileToSql('csv | rolling-any flag 3 any3 nulls=propagate | rolling-all flag 3 all3 nulls=false | csv')
  assert(boolSql.includes('BOOL_OR("flag") OVER'), 'rolling-any should lower to BOOL_OR window')
  assert(boolSql.includes('COUNT(*) OVER'), 'rolling-any nulls=propagate should guard with COUNT window')
  assert(boolSql.includes('BOOL_AND(COALESCE("flag", FALSE)) OVER'), 'rolling-all nulls=false should coalesce nulls')
  assert(boolSql.includes('"any3"'), 'rolling-any should name result')
  assert(boolSql.includes('"all3"'), 'rolling-all should name result')
})

await test('compileToSql rleid', async () => {
  const sql = await compileToSql('csv | rleid city,status result=run_id | csv')
  assert(sql.includes('SUM(__tf_rleid_changed)'), 'should use cumulative run counter')
  assert(sql.includes('LAG("city")'), 'should compare previous city')
  assert(sql.includes('LAG("status")'), 'should compare previous status')
  assert(sql.includes('"run_id"'), 'should name result column')
})

await test('compileToSql filtering joins', async () => {
  const semi = await compileToSql('csv | semi-join lookup.csv on id | csv')
  assert(semi.includes('EXISTS'), 'semi-join should lower to EXISTS')
  assert(!semi.includes('NOT EXISTS'), 'semi-join should not lower to NOT EXISTS')
  const anti = await compileToSql('csv | anti-join lookup.csv on id | csv')
  assert(anti.includes('NOT EXISTS'), 'anti-join should lower to NOT EXISTS')
  const inter = await compileToSql('csv | intersect lookup.csv | csv')
  assert(inter.includes('INTERSECT'), 'intersect should lower to SQL INTERSECT')
  const diff = await compileToSql('csv | setdiff lookup.csv | csv')
  assert(diff.includes('EXCEPT'), 'setdiff should lower to SQL EXCEPT')
  const interAll = await compileToSql('csv | intersect-all lookup.csv | csv')
  assert(interAll.includes('INTERSECT ALL'), 'intersect-all should lower to SQL INTERSECT ALL')
  const diffAll = await compileToSql('csv | setdiff-all lookup.csv | csv')
  assert(diffAll.includes('EXCEPT ALL'), 'setdiff-all should lower to SQL EXCEPT ALL')
  const union = await compileToSql('csv | union lookup.csv | csv')
  assert(union.includes('UNION'), 'union should lower to SQL UNION')
  assert(!union.includes('UNION ALL'), 'union should not lower to SQL UNION ALL')
  const unionAll = await compileToSql('csv | union-all lookup.csv | csv')
  assert(unionAll.includes('UNION ALL'), 'union-all should lower to SQL UNION ALL')
  let rejected = false
  try {
    await compileToSql('csv | intersect lookup.csv id | csv')
  } catch (e) {
    rejected = e.message.includes('all-column')
  }
  assert(rejected, 'selected-key set op SQL lowering should reject')
})

// ---- DuckDB Engine ----

let hasDuckDB = false
try {
  require('duckdb')
  hasDuckDB = true
} catch {}

if (hasDuckDB) {
  console.log('\nDuckDB Engine:')

  const csvData = 'name,age,score\nAlice,30,85\nBob,20,92\nCharlie,35,78\nDiana,22,95\nEve,28,88\n'

  await test('duckdb filter', async () => {
    const r = await pipeline('csv | filter "col(\'age\') > 25" | csv', { engine: 'duckdb' })
      .run({ input: csvData })
    const text = r.outputText
    assert(text.includes('Alice'), 'should have Alice')
    assert(text.includes('Charlie'), 'should have Charlie')
    assert(!text.includes('Bob'), 'should not have Bob')
  })

  await test('duckdb grep literal metacharacters', async () => {
    const dataRows = text => {
      const trimmed = text.trim()
      if (!trimmed) return []
      return trimmed.split('\n').slice(1)
    }
    const cases = [
      ['csv | grep % name | csv', 'name\na%z\nabc\nplain\n'],
      ['csv | grep _ name | csv', 'name\na_z\nabc\nplain\n'],
      [String.raw`csv | grep \ name | csv`, 'name\na\\z\nabc\nplain\n'],
      ['csv | grep 2 age | csv', 'name,age\na%z,20\nabc,21\n'],
      ['csv | grep -v 2 age | csv', 'name,age\na%z,20\nabc,21\n'],
    ]
    for (const [dsl, input] of cases) {
      const native = await pipeline(dsl).run({ input })
      const duck = await pipeline(dsl, { engine: 'duckdb' }).run({ input })
      if (dsl === 'csv | grep 2 age | csv') {
        assert(dataRows(native.outputText).length === 0, 'native non-string grep should emit no data rows')
        assert(dataRows(duck.outputText).length === 0, 'DuckDB non-string grep should emit no data rows')
      } else {
        assert(duck.outputText === native.outputText,
          `DuckDB grep parity failed for ${dsl}\nnative=${native.outputText}\nduck=${duck.outputText}`)
      }
    }
  })

  await test('duckdb rejects sample', async () => {
    await assertRejects(
      () => pipeline('csv | sample 2 seed=42 | csv', { engine: 'duckdb' }).run({ input: csvData }),
      /sample.*cannot be lowered to SQL/
    )
  })

  await test('duckdb rejects stats', async () => {
    await assertRejects(
      () => pipeline('csv | stats count,missing,complete_rate | csv', { engine: 'duckdb' }).run({ input: csvData }),
      /stats.*cannot be lowered to SQL/
    )
  })

  await test('duckdb select', async () => {
    const r = await pipeline('csv | select name,age | csv', { engine: 'duckdb' })
      .run({ input: csvData })
    assert(r.outputText.includes('name,age'), 'should have header')
    assert(!r.outputText.includes('score'), 'should not have score')
  })

  await test('duckdb head', async () => {
    const r = await pipeline('csv | head 3 | csv', { engine: 'duckdb' })
      .run({ input: csvData })
    const lines = r.outputText.trim().split('\n')
    assert(lines.length === 4, `should have 4 lines (header + 3), got ${lines.length}`)
  })

  await test('duckdb sort', async () => {
    const r = await pipeline('csv | sort age | csv', { engine: 'duckdb' })
      .run({ input: csvData })
    const lines = r.outputText.trim().split('\n')
    assert(lines[1].includes('Bob'), 'first row should be Bob (youngest)')
  })

  await test('duckdb derive', async () => {
    const r = await pipeline("csv | derive age_x2=col('age')*2 | select name,age_x2 | csv", { engine: 'duckdb' })
      .run({ input: csvData })
    assert(r.outputText.includes('age_x2'), 'should have derived column')
    assert(r.outputText.includes('60'), 'Alice 30*2=60')
  })

  await test('duckdb rename', async () => {
    const r = await pipeline('csv | rename age=years | csv', { engine: 'duckdb' })
      .run({ input: csvData })
    const header = r.outputText.split('\n')[0]
    assert(header.includes('years'), 'should have years')
    assert(!header.includes('age'), 'should not have age in header')
  })

  await test('duckdb unique', async () => {
    const data = 'name,city\nAlice,NYC\nBob,LA\nCharlie,NYC\nDiana,LA\n'
    const r = await pipeline('csv | unique city | csv', { engine: 'duckdb' })
      .run({ input: data })
    const lines = r.outputText.trim().split('\n')
    assert(lines.length === 3, `should have 3 lines (header + 2 cities), got ${lines.length}`)
  })

  await test('duckdb parity with native', async () => {
    const dsl = 'csv | filter "col(\'age\') > 25" | select name,age | csv'
    const native = await pipeline(dsl).run({ input: csvData })
    const duck = await pipeline(dsl, { engine: 'duckdb' }).run({ input: csvData })
    const nativeRows = new Set(native.outputText.trim().split('\n').slice(1))
    const duckRows = new Set(duck.outputText.trim().split('\n').slice(1))
    assert(nativeRows.size === duckRows.size, 'same number of rows')
    for (const row of nativeRows) {
      assert(duckRows.has(row), `duck should have row: ${row}`)
    }
  })
} else {
  console.log('\nDuckDB Engine:')
  console.log('  (skipped — duckdb not installed)')
}

// ---- WASM wrapper ----

console.log('\nWASM Wrapper:')

let createTranfi = null
try {
  createTranfi = require('../js/wasm/index.js')
} catch {}

if (createTranfi) {
  const tf = await createTranfi()

  await test('wasm version', async () => {
    const v = tf.version()
    assert(v === '0.1.2', `expected 0.1.2, got ${v}`)
  })

  await test('wasm compileToSql', async () => {
    const sql = tf.compileToSql('csv | filter "col(\'age\') > 25" | csv', { dialect: 'duckdb' })
    assert(sql.includes('WHERE'), 'should have WHERE')
    assert(sql.includes('"age"'), 'should quote column')
    await assertRejects(
      () => tf.compileToSql('csv | head 1 | csv', { dialect: 'postgres' }),
      /recognized but not implemented/
    )
  })

  await test('wasm run native', async () => {
    const result = tf.run('csv | filter "col(\'age\') > 25" | csv',
      'name,age\nAlice,30\nBob,20\nCharlie,35\n')
    assert(result.outputText.includes('Alice'), 'should have Alice')
    assert(result.outputText.includes('Charlie'), 'should have Charlie')
    assert(!result.outputText.includes('Bob'), 'should not have Bob')
  })

  await test('wasm date expression functions', async () => {
    const result = tf.run(
      "csv batch_size=1 | filter \"year(col('d')) == 2024\" | derive y=year(col('d')) m=date_trunc(col('d'),'month') h=date_trunc(col('ts'),'hour') | csv",
      'd,ts\n2024-03-15,2024-03-15T12:34:56Z\n2023-12-25,2023-12-25T08:09:10Z\n'
    )
    assert(result.outputText.includes('2024-03-15,2024-03-15T12:34:56Z,2024,2024-03-01,2024-03-15T12:00:00Z'), 'wasm date expressions should derive components and truncations')
    assert(!result.outputText.includes('2023-12-25'), 'wasm date expression filter should drop 2023 row')
  })

  await test('wasm scan profile op', async () => {
    const result = tf.run('csv | scan | csv', 'name,age\nAlice,30\nBob,\nAlice,35\n')
    assert(result.outputText.includes('column,count,missing,complete_rate'), 'wasm scan should include profile defaults')
    assert(result.outputText.includes('distinct'), 'wasm scan should include distinct estimate')
    assert(result.outputText.includes('hist'), 'wasm scan should include histogram')
    assert(result.outputText.includes('sample'), 'wasm scan should include bounded sample')
    assert(result.statsText.includes('\"op\":\"scan\"'), 'wasm scan should report step stats')
    assert(result.statsText.includes('\"execution_target\":\"wasm\"'), 'wasm scan should report wasm execution target')
  })

  await test('wasm source-name op', async () => {
    const result = tf.run('csv | source-name src default=browser | csv', 'name\nAlice\n', { chunkSize: 2 })
    assert(result.outputText.includes('name,src'), 'wasm source-name should append header')
    assert(result.outputText.includes('Alice,browser'), 'wasm source-name should use default without host source')
  })

  await test('wasm group-agg null semantics', async () => {
    const input = [
      '{"city":"A","amount":10}',
      '{"city":"A","amount":null}',
      '{"city":"A","amount":5}',
      '{"city":"B","amount":null}',
      '{"city":"B","amount":7}',
      '{"city":"C","amount":null}',
    ].join('\n') + '\n'
    const result = tf.run('jsonl batch_size=2 | group-agg city sum:amount:total avg:amount:avg count:amount:n count:*:rows min:amount:min max:amount:max sorted=true | csv', input)
    assertGroupAggNullSemantics(result.outputText)
  })

  await test('wasm jsonl malformed records', async () => {
    const result = tf.run('jsonl on_error=warn max_error_bytes=8 | csv',
      '{"name":"Alice","age":30}\nnot-json-record-long\n{"name":"Bob","age":25}\n')
    assert(result.outputText.includes('Alice'), 'wasm jsonl keeps first valid row')
    assert(result.outputText.includes('Bob'), 'wasm jsonl keeps later valid row')
    const errors = new TextDecoder().decode(result.errors)
    assert(errors.includes('jsonl_malformed'), 'wasm jsonl emits malformed record')
    assert(errors.includes('"action":"warn"'), 'wasm jsonl records warn action')
    assert(errors.includes('"raw":"not-json"'), 'wasm jsonl truncates raw preview')
    assert(errors.includes('"line":2'), 'wasm jsonl records line number')
  })

  await test('wasm json-extract', async () => {
    const result = tf.run('text | json-extract /user/id user_id type=int | csv',
      '{"user":{"id":42}}\n{"user":{"id":7}}\n')
    assert(result.outputText.includes('_line,user_id'), 'wasm json-extract header')
    assert(result.outputText.includes('42'), 'wasm json-extract first row')
    assert(result.outputText.includes('7'), 'wasm json-extract second row')
  })

  await test('wasm json-filter', async () => {
    const result = tf.run('text | json-filter /user/age >= 30 type=float | csv',
      '{"user":{"name":"Ada","age":42}}\n{"user":{"name":"Ben","age":7}}\n')
    assert(result.outputText.includes('Ada'), 'wasm json-filter keeps matching row')
    assert(!result.outputText.includes('Ben'), 'wasm json-filter drops non-matching row')
  })

  await test('wasm json-schema', async () => {
    const result = tf.run('text | json-schema required=user types=user:object mode=filter | csv',
      '{"user":{"name":"Ada"}}\n{"other":1}\nnot json\n')
    assert(result.outputText.includes('Ada'), 'wasm json-schema keeps valid row')
    assert(!result.outputText.includes('other'), 'wasm json-schema drops missing required row')
    assert(!result.outputText.includes('not json'), 'wasm json-schema drops malformed row')
  })


  await test('wasm schema infer', async () => {
    const result = tf.run('csv batch_size=1 | schema infer rows=2 | csv',
      'name,age,score\nAlice,30,1.5\nBob,,2.25\nCara,40,3.5\n')
    assert(result.outputText.includes('column,type,nullable,non_null,rows_seen,rows_sampled,missing,non_missing,observed_types,warning'), 'wasm schema infer should emit report header')
    assert(result.outputText.includes('age,int,true,false,3,2,1,1,int:1;null:1,sample_limited'), 'wasm schema infer should report sampled nullable int column')
  })


  await test('wasm select all_of and any_of', async () => {
    const allOf = tf.run(
      'csv batch_size=1 | select all_of(score_read,name) | csv',
      'id,score_math,score_read,name,active\n1,90,80,Alice,true\n2,70,95,Bob,false\n'
    )
    assert(allOf.outputText.startsWith('score_read,name\n'), 'wasm all_of should select requested columns in helper order')

    const anyOf = tf.run(
      'csv batch_size=1 | select any_of(missing,score_math) | csv',
      'id,score_math,score_read,name,active\n1,90,80,Alice,true\n'
    )
    assert(anyOf.outputText.startsWith('score_math\n'), 'wasm any_of should ignore missing columns')

    const range = tf.run(
      'csv batch_size=1 | select id:score_read | csv',
      'id,score_math,score_read,name,active\n1,90,80,Alice,true\n'
    )
    assert(range.outputText.startsWith('id,score_math,score_read\n'), 'wasm range selector should include consecutive columns')

    const intersection = tf.run(
      'csv batch_size=1 | select starts_with(score_)&where(numeric) | csv',
      'id,score_math,score_read,name,active\n1,90,80,Alice,true\n'
    )
    assert(intersection.outputText.startsWith('score_math,score_read\n'), 'wasm selector & should intersect selections')

    const difference = tf.run(
      'csv batch_size=1 | select starts_with(score_)&!ends_with(read) | csv',
      'id,score_math,score_read,name,active\n1,90,80,Alice,true\n'
    )
    assert(difference.outputText.startsWith('score_math\n'), 'wasm selector ! inside & should subtract selections')

    const complement = tf.run(
      'csv batch_size=1 | select !(id:score_read) | csv',
      'id,score_math,score_read,name,active\n1,90,80,Alice,true\n'
    )
    assert(complement.outputText.startsWith('name,active\n'), 'wasm parenthesized selector complement should resolve')

    assertThrows(
      () => tf.run('csv batch_size=1 | select all_of(score_read,missing) | csv', 'id,score_math,score_read,name\n1,90,80,Alice\n'),
      /all_of column not found|processing error/
    )
  })

  await test('wasm across row-local transforms', async () => {
    const rounded = tf.run(
      'csv batch_size=1 | across starts_with(score_) round | csv',
      'id,score_math,score_read,name\n1,90.4,80.6,Alice\n2,70.2,95.8,Bob\n'
    )
    assert(rounded.outputText.includes('1,90,81,Alice'), 'wasm across should round selected numeric columns')
    assert(rounded.outputText.includes('2,70,96,Bob'), 'wasm across should round selected numeric columns across chunks')

    const appended = tf.run(
      'csv batch_size=1 | across name lower replace=false names={col}_{fn} | csv',
      'name,score\nAlice,1\nBOB,2\n'
    )
    assert(appended.outputText.startsWith('name,score,name_lower\n'), 'wasm across should append templated columns')
    assert(appended.outputText.includes('BOB,2,bob'), 'wasm across lower should append lowercase values')
  })


  await test('wasm schema selectors', async () => {
    const result = tf.run(
      'csv batch_size=1 | schema columns=starts_with(score_):number,code:string non_null=starts_with(score_) min=where(number):0 max=starts_with(score_):100 regex=ends_with(code):^[A-Z]+$ mode=warn | csv',
      'id,score_a,score_b,code,note\n1,10,20,AA,x\n2,,200,bad,y\n'
    )
    assert(result.outputText.includes('1,10,20,AA,x'), 'wasm schema selectors should keep passing row')
    assert(result.outputText.includes('2,,200,bad,y'), 'wasm schema selectors warn mode should keep failing row')
    const errors = new TextDecoder().decode(result.errors)
    assert(errors.includes('"column":"score_a"'), 'wasm schema selectors should flag score_a')
    assert(errors.includes('"rule":"nullable"'), 'wasm schema selectors should flag nullability')
    assert(errors.includes('"column":"score_b"'), 'wasm schema selectors should flag score_b')
    assert(errors.includes('"rule":"max"'), 'wasm schema selectors should flag max')
    assert(errors.includes('"column":"code"'), 'wasm schema selectors should flag code')
    assert(errors.includes('"rule":"regex"'), 'wasm schema selectors should flag regex')
    assert(result.statsText.includes('"checked_rows":2'), 'wasm schema selector stats should count checked rows')
    assert(result.statsText.includes('"violation_count":3'), 'wasm schema selector stats should count all violations')
  })




  await test('wasm schema regex budgets', async () => {
    const result = tf.run(
      'csv batch_size=1 | schema regex=code:^[A-Z]+$ max_regex_pattern_bytes=32 max_regex_cell_bytes=3 mode=warn | csv',
      'code\nAA\nTOOLONG\n'
    )
    assert(result.outputText.includes('AA'), 'wasm schema regex budget should keep passing row in warn mode')
    assert(result.outputText.includes('TOOLONG'), 'wasm schema regex budget should keep failing row in warn mode')
    const errors = new TextDecoder().decode(result.errors)
    assert(errors.includes('schema_failure'), 'wasm over-cap regex cell should emit schema failure')
    assert(errors.includes('"rule":"regex"'), 'wasm over-cap regex cell should count as regex failure')
    assert(errors.includes('cell exceeds 3 bytes'), 'wasm over-cap regex cell should report cap failure')
    assert(result.statsText.includes('"checked_rows":2'), 'wasm schema regex budget stats should count rows')
    assert(result.statsText.includes('"passed_rows":1'), 'wasm schema regex budget stats should count passing row')
    assert(result.statsText.includes('"failed_rows":1'), 'wasm schema regex budget stats should count failing row')
    assert(result.statsText.includes('"regex_failures":1'), 'wasm schema regex budget stats should count regex failures')
    assert(result.statsText.includes('"max_regex_pattern_bytes":32'), 'wasm schema regex budget stats should report pattern cap')
    assert(result.statsText.includes('"max_regex_cell_bytes":3'), 'wasm schema regex budget stats should report cell cap')

    assertThrows(
      () => tf.run('csv | schema regex=code:^[A-Z]+$ max_regex_pattern_bytes=3 mode=warn | csv', 'code\nAA\n'),
      /max_regex_pattern_bytes/
    )
  })


  await test('wasm schema audit privacy controls', async () => {
    const data = 'name,ssn,age\nAliciaSecret,111-22-3333,200\n'
    let result = tf.run(
      'csv batch_size=1 | schema max=age:120 mode=filter audit audit_include_row=false | csv',
      data
    )
    assert(result.statsText.includes('"type":"audit"'), 'wasm schema audit should emit audit record')
    assert(!result.statsText.includes('"data"'), 'wasm audit_include_row=false should omit row payload')
    assert(!result.statsText.includes('AliciaSecret'), 'wasm audit_include_row=false should not leak name')
    assert(!result.statsText.includes('111-22-3333'), 'wasm audit_include_row=false should not leak ssn')

    result = tf.run(
      'csv batch_size=1 | schema values=ssn:OK mode=filter audit audit_columns=ssn audit_redact=ssn | csv',
      data
    )
    assert(result.statsText.includes('[REDACTED]'), 'wasm schema audit should redact selected column')
    assert(!result.statsText.includes('111-22-3333'), 'wasm schema audit should not leak raw ssn')
    assert(!result.statsText.includes('AliciaSecret'), 'wasm schema audit_columns should limit row payload columns')

    result = tf.run(
      'csv batch_size=1 | schema regex=ssn:^OK$ mode=warn audit_redact=ssn | csv',
      data
    )
    const errors = new TextDecoder().decode(result.errors)
    assert(errors.includes('schema_failure'), 'wasm schema warn should emit failure')
    assert(errors.includes('[REDACTED]'), 'wasm schema errors should redact actual values')
    assert(!errors.includes('111-22-3333'), 'wasm schema errors should not leak raw ssn')
  })


  await test('wasm audit privacy migrated producers', async () => {
    const data = 'name,ssn,age\nAlice,111-22-3333,20\nBob,222-33-4444,40\n'
    let result = tf.run(
      'csv batch_size=1 | filter "col(age) > 30" audit audit_include_row=false | csv',
      data
    )
    assert(result.statsText.includes('"op":"filter"'), 'wasm filter audit should emit')
    assert(!result.statsText.includes('"data"'), 'wasm filter audit_include_row=false should omit row payload')
    assert(!result.statsText.includes('Alice'), 'wasm filter audit should not leak omitted row name')
    assert(!result.statsText.includes('111-22-3333'), 'wasm filter audit should not leak omitted ssn')

    result = tf.run(
      'csv batch_size=1 | validate "col(age) > 30" audit audit_columns=ssn audit_redact=ssn | csv',
      data
    )
    assert(result.statsText.includes('"op":"validate"'), 'wasm validate audit should emit')
    assert(result.statsText.includes('[REDACTED]'), 'wasm validate audit should redact selected column')
    assert(!result.statsText.includes('111-22-3333'), 'wasm validate audit should not leak raw ssn')
    assert(!result.statsText.includes('Alice'), 'wasm validate auditColumns should limit payload')

    result = tf.run(
      'csv batch_size=1 | assert "col(age) > 30" action=warn audit_columns=ssn audit_hash_columns=ssn | csv',
      data
    )
    const errors = new TextDecoder().decode(result.errors)
    assert(errors.includes('assert_failure'), 'wasm assert warning should emit failure record')
    assert(errors.includes('fnv1a64:'), 'wasm assert errors should hash selected column')
    assert(!errors.includes('111-22-3333'), 'wasm assert errors should not leak raw ssn')
    assert(!errors.includes('Alice'), 'wasm assert auditColumns should limit payload')

    result = tf.run(
      'text | json-schema required=user mode=filter audit audit_columns=_line audit_redact=_line | text',
      '{"ssn":"111-22-3333"}\n'
    )
    assert(result.statsText.includes('"op":"json-schema"'), 'wasm json-schema audit should emit')
    assert(result.statsText.includes('[REDACTED]'), 'wasm json-schema audit should redact row text')
    assert(!result.statsText.includes('111-22-3333'), 'wasm json-schema audit should not leak raw JSON text')

    result = tf.run(
      'csv batch_size=1 nulls=NA | fill-null note=SECRET audit audit_columns=note audit_redact=note | csv',
      'name,note\nB,NA\nC,ok\n'
    )
    assert(result.statsText.includes('"op":"fill-null"'), 'wasm fill-null audit should emit')
    assert(result.statsText.includes('[REDACTED]'), 'wasm fill-null audit should redact filled value')
    assert(!result.statsText.includes('SECRET'), 'wasm fill-null audit should not leak filled value')
    assert(!result.statsText.includes('"B"'), 'wasm fill-null auditColumns should limit row payload')

    result = tf.run(
      'csv batch_size=1 | replace ssn 111 999 audit audit_columns=ssn audit_redact=ssn | csv',
      'name,ssn\nAlice,111-22-3333\nBob,222-33-4444\n'
    )
    assert(result.statsText.includes('"op":"replace"'), 'wasm replace audit should emit')
    assert(result.statsText.includes('[REDACTED]'), 'wasm replace audit should redact changed values')
    assert(!result.statsText.includes('111'), 'wasm replace audit should not leak raw pattern/source fragment')
    assert(!result.statsText.includes('999'), 'wasm replace audit should not leak raw replacement fragment')
    assert(!result.statsText.includes('Alice'), 'wasm replace auditColumns should limit row payload')


    result = tf.run(
      'csv batch_size=1 | cast secret=int audit audit_columns=secret audit_redact=secret | csv',
      'name,secret\nAlice,111-22-3333\n'
    )
    assert(result.statsText.includes('"op":"cast"'), 'wasm cast audit should emit')
    assert(result.statsText.includes('[REDACTED]'), 'wasm cast audit should redact actual/before/after')
    assert(!result.statsText.includes('111'), 'wasm cast audit should not leak raw source fragment')
    assert(!result.statsText.includes('Alice'), 'wasm cast auditColumns should limit row payload')

    result = tf.run(
      'csv batch_size=2 | normalize score audit audit_columns=score audit_redact=score | csv',
      'name,score\nAlice,100\nBob,200\n',
      { allowBlocking: true }
    )
    assert(result.statsText.includes('"op":"normalize"'), 'wasm normalize audit should emit')
    assert(result.statsText.includes('[REDACTED]'), 'wasm normalize audit should redact values and stats')
    assert(!result.statsText.includes('100'), 'wasm normalize audit should not leak min/before value')
    assert(!result.statsText.includes('200'), 'wasm normalize audit should not leak max/source value')
    assert(!result.statsText.includes('Alice'), 'wasm normalize auditColumns should limit row payload')

    result = tf.run(
      'csv batch_size=1 | frequency city max_values=1 overflow=other audit audit_limit=1 audit_columns=city audit_redact=city | csv',
      'name,city\nAlice,NY\nBob,LA\n'
    )
    assert(result.statsText.includes('"op":"frequency"'), 'wasm frequency audit should emit')
    assert(result.statsText.includes('"event":"category_overflow"'), 'wasm frequency audit should identify overflow event')
    assert(result.statsText.includes('[REDACTED]'), 'wasm frequency audit should redact overflow value and row data')
    assert(!result.statsText.includes('LA'), 'wasm frequency audit should not leak overflow value')
    assert(!result.statsText.includes('Bob'), 'wasm frequency auditColumns should limit row payload')
    assert(!result.statsText.includes('Alice'), 'wasm frequency auditColumns should limit row payload')
    assert(result.statsText.includes('"data":{"city":"[REDACTED]"}'), 'wasm frequency audit should redact row payload')

    result = tf.run(
      'csv batch_size=1 repair=true audit audit_limit=1 audit_redact=raw | csv',
      'name,secret\nAlice,SECRET,extra\n'
    )
    assert(result.statsText.includes('"event":"row_repaired"'), 'wasm csv repair audit should emit')
    assert(result.statsText.includes('[REDACTED]'), 'wasm csv repair audit should redact raw record')
    assert(!result.statsText.includes('SECRET'), 'wasm csv repair audit should not leak raw value')
    assert(!result.statsText.includes('Alice'), 'wasm csv repair audit should not leak raw row')
    const repairErrors = new TextDecoder().decode(result.errors)
    assert(repairErrors.includes('csv_field_count'), 'wasm csv repair diagnostic should emit')
    assert(repairErrors.includes('[REDACTED]'), 'wasm csv repair diagnostic should redact raw record')
    assert(!repairErrors.includes('SECRET'), 'wasm csv repair diagnostic should not leak raw value')
    assert(!repairErrors.includes('Alice'), 'wasm csv repair diagnostic should not leak raw row')

    result = tf.run(
      'csv batch_size=1 | tee "col(age) >= 20" channel=audit columns=name,ssn limit=1 audit_columns=ssn audit_redact=ssn | csv',
      data
    )
    assert(result.statsText.includes('"op":"tee"'), 'wasm tee audit should emit')
    assert(result.statsText.includes('[REDACTED]'), 'wasm tee audit should redact selected column')
    assert(!result.statsText.includes('111-22-3333'), 'wasm tee audit should not leak raw ssn')
    assert(!result.statsText.includes('Alice'), 'wasm tee auditColumns should limit row payload')

    result = tf.run(
      'csv batch_size=1 | quarantine "col(age) < 30" audit_columns=ssn audit_redact=ssn | csv',
      data
    )
    const quarantineErrors = new TextDecoder().decode(result.errors)
    assert(quarantineErrors.includes('"op":"quarantine"'), 'wasm quarantine should emit error record')
    assert(quarantineErrors.includes('[REDACTED]'), 'wasm quarantine should redact selected column')
    assert(!quarantineErrors.includes('111-22-3333'), 'wasm quarantine should not leak raw ssn')
    assert(!quarantineErrors.includes('Alice'), 'wasm quarantine auditColumns should limit row payload')

  })

  await test('wasm tee side channel', async () => {
    const result = tf.run('csv batch_size=1 | tee "col(age) >= 30" columns=name limit=2 name=age_sample | csv',
      'name,age\nAlice,30\nBob,25\nCara,35\nDave,45\n')
    assert(result.outputText.includes('Alice,30'), 'wasm tee should preserve main Alice')
    assert(result.outputText.includes('Bob,25'), 'wasm tee should preserve main Bob')
    const samples = new TextDecoder().decode(result.samples)
    assert(samples.includes('"type":"tee"'), 'wasm tee should emit sample records')
    assert(samples.includes('age_sample'), 'wasm tee should include name')
    assert(samples.includes('"Alice"'), 'wasm tee should include first matching row')
    assert(samples.includes('"Cara"'), 'wasm tee should include second matching row')
    assert(!samples.includes('"Dave"'), 'wasm tee should enforce limit')
  })

  await test('wasm validate rules', async () => {
    const result = tf.run(
      'csv batch_size=1 | validate rule=age_positive:col(age)>0 rule=age_under_25:col(age)<25 audit audit_limit=3 max_failures=2 name=quality | csv',
      'name,age\nAlice,30\nBob,-1\nCara,20\n'
    )
    assert(result.outputText.includes('Alice,30,false'), 'wasm validate rules should mark first failed row')
    assert(result.outputText.includes('Bob,-1,false'), 'wasm validate rules should mark second failed row')
    assert(result.outputText.includes('Cara,20,true'), 'wasm validate rules should mark passing row')
    assert(result.statsText.includes('"suite":"quality"'), 'wasm validate rules should include suite')
    assert(result.statsText.includes('"name":"age_positive"'), 'wasm validate rules should include first rule')
    assert(result.statsText.includes('"name":"age_under_25"'), 'wasm validate rules should include second rule')
    assert(result.statsText.includes('"rule_count":2'), 'wasm validate rules should include rule count')
    assert(result.statsText.includes('"audit_emitted":2'), 'wasm validate rules should count audit records')
  })

  await test('wasm validate failure-rate warning', async () => {
    const result = tf.run(
      'csv batch_size=1 | validate "col(age) > 25" warn_failure_rate=0.5 name=age_rate | csv',
      'name,age\nAlice,30\nBob,20\nCara,10\n'
    )
    const errors = new TextDecoder().decode(result.errors)
    assert(errors.includes('"event":"threshold_warning"'), 'wasm validate should emit failure-rate warning')
    assert(errors.includes('"warn_failure_rate":0.5'), 'wasm validate warning should include threshold')
    assert(result.statsText.includes('"failure_rate":'), 'wasm validate stats should include failure rate')
    assert(result.statsText.includes('"warn_failure_rate":0.5'), 'wasm validate stats should include warning threshold')
  })


  await test('wasm filter audit side channel', async () => {
    const result = tf.run('csv batch_size=1 | filter "col(age) > 25" audit audit_limit=1 | csv',
      'name,age\nAlice,30\nBob,20\nCara,40\nDave,10\n')
    assert(result.outputText.includes('Alice'), 'wasm filter audit keeps Alice')
    assert(result.outputText.includes('Cara'), 'wasm filter audit keeps Cara')
    assert(!result.outputText.includes('Bob'), 'wasm filter audit drops Bob')
    assert(result.statsText.includes('"type":"audit"'), 'wasm filter audit should emit audit record')
    assert(result.statsText.includes('"op":"filter"'), 'wasm filter audit should identify filter op')
    assert(result.statsText.includes('"row":2'), 'wasm filter audit should include row index')
    assert(result.statsText.includes('"Bob"'), 'wasm filter audit should include first dropped row')
    assert(!result.statsText.includes('"Dave"'), 'wasm filter audit should enforce audit_limit')
  })

  await test('wasm json-flatten', async () => {
    const result = tf.run('text | json-flatten fields=/user/id:user_id:int,$.user.name:name:string | csv',
      '{"user":{"id":42,"name":"Ada"}}\n{"user":{"id":7,"name":"Ben"}}\n')
    assert(result.outputText.includes('_line,user_id,name'), 'wasm json-flatten header')
    assert(result.outputText.includes('42,Ada'), 'wasm json-flatten first row')
    assert(result.outputText.includes('7,Ben'), 'wasm json-flatten second row')
  })

  await test('wasm assert quarantine', async () => {
    const result = tf.run('csv | assert "col(age) > 25" action=quarantine name=age_check message=age_low | csv',
      'name,age\nAlice,30\nBob,20\nCara,40\n')
    assert(result.outputText.includes('Alice'), 'wasm assert keeps Alice')
    assert(result.outputText.includes('Cara'), 'wasm assert keeps Cara')
    assert(!result.outputText.includes('Bob'), 'wasm assert quarantines Bob')
    const errors = new TextDecoder().decode(result.errors)
    assert(errors.includes('assert_failure'), 'wasm assert emits failure record')
    assert(errors.includes('age_check'), 'wasm assert includes rule name')
    assert(errors.includes('\"Bob\"'), 'wasm assert includes failing row')
  })

  await test('wasm aggregate assert warning', async () => {
    const result = tf.run(
      'csv batch_size=1 | assert aggregate=sum:amount op=>= value=100 action=warn name=sales_total | csv',
      'name,amount\nA,30\nB,20\n'
    )
    assert(result.outputText.includes('A,30'), 'wasm aggregate assert should pass through first row')
    assert(result.outputText.includes('B,20'), 'wasm aggregate assert should pass through second row')
    const errors = new TextDecoder().decode(result.errors)
    assert(errors.includes('aggregate_assert_failed'), 'wasm aggregate assert should emit warning record')
    assert(errors.includes('"severity":"warning"'), 'wasm aggregate assert should warn')
    assert(errors.includes('"actual":50'), 'wasm aggregate assert should include actual value')
    assert(result.statsText.includes('"memory_class":"bounded_state"'), 'wasm aggregate assert metadata should be bounded')
    assert(result.statsText.includes('"aggregate_value":50'), 'wasm aggregate assert stats should include value')

    const rateResult = tf.run(
      'csv batch_size=1 | assert aggregate=missing_rate:score op=<= value=0.25 action=warn name=score_missing_rate | csv',
      'name,score\nA,10\nB,\nC,30\n'
    )
    const rateErrors = new TextDecoder().decode(rateResult.errors)
    assert(rateErrors.includes('"aggregate":"missing_rate"'), 'wasm missing-rate warning should name aggregate')
    assert(rateResult.statsText.includes('"aggregate_rows":3'), 'wasm missing-rate stats should count rows')
    assert(rateResult.statsText.includes('"aggregate_missing":1'), 'wasm missing-rate stats should count missing')
  })


  await test('wasm schema quarantine', async () => {
    const result = tf.run('csv | schema name:string age:int city:string non_null=name,age max=age:120 values=city:NY,LA mode=quarantine name=schema_check message=schema_rule | csv',
      'name,age,city\nAlice,30,NY\nBob,200,SF\nCara,,LA\n')
    assert(result.outputText.includes('Alice'), 'wasm schema keeps valid row')
    assert(!result.outputText.includes('Bob'), 'wasm schema quarantines range failure')
    assert(!result.outputText.includes('Cara'), 'wasm schema quarantines null failure')
    const errors = new TextDecoder().decode(result.errors)
    assert(errors.includes('schema_failure'), 'wasm schema emits failure record')
    assert(errors.includes('schema_check'), 'wasm schema includes rule name')
    assert(errors.includes('"Bob"'), 'wasm schema includes failing row')
    assert(errors.includes('"rule":"values"'), 'wasm schema includes all row-local rule failures')
    assert(result.statsText.includes('"checked_rows":3'), 'wasm schema stats should count checked rows')
    assert(result.statsText.includes('"failed_rows":2'), 'wasm schema stats should count failed rows')
    assert(result.statsText.includes('"violation_count":3'), 'wasm schema stats should count violations')
  })


  await test('wasm rolling bool aliases', async () => {
    const result = tf.run('csv batch_size=2 | rolling-any flag 3 any3 | rolling-all flag 3 all3 | csv',
      'flag\ntrue\nfalse\ntrue\ntrue\n')
    const lines = result.outputText.trim().split('\n')
    assert(lines[0] === 'flag,any3,all3', 'wasm rolling bool header')
    assert(lines[1] === 'true,true,true', 'wasm rolling bool first row')
    assert(lines[2] === 'false,true,false', 'wasm rolling bool second row')
    assert(lines[4] === 'true,true,false', 'wasm rolling bool crosses chunk boundary')
  })

  await test('wasm H14 numeric missing/type policies', async () => {
    assertThrows(
      () => tf.run('csv | ewma x 0.5 | csv', 'x\na\n'),
      /ewma: column 'x' must be numeric/
    )
    assertThrows(
      () => tf.run('csv | anomaly missing | csv', 'x\n1\n'),
      /anomaly: column 'missing' not found/
    )
    assertThrows(
      () => tf.run('csv | bin x 10 | csv', 'x\na\n'),
      /bin: column 'x' must be numeric/
    )
    assertThrows(
      () => tf.run('csv | window x 3 avg | csv', 'x\na\n'),
      /window: column 'x' must be numeric/
    )
    assertThrows(
      () => tf.run('csv | rolling-sum missing 3 sum3 | csv', 'x\n1\n'),
      /rolling-sum: column 'missing' not found/
    )
    assertThrows(
      () => tf.run('csv | interpolate x forward | csv', 'x\na\n', { allowBlocking: true }),
      /interpolate: column 'x' must be numeric/
    )
    assertThrows(
      () => tf.run('jsonl | datetime x year | csv', '{"x":true}\n'),
      /datetime: column 'x' must be string, date, or timestamp/
    )
    assertThrows(
      () => tf.run('jsonl | date-trunc x month | csv', '{"x":true}\n'),
      /date-trunc: column 'x' must be string, date, or timestamp/
    )
    assertThrows(
      () => tf.run('csv | normalize missing | csv', 'x\n1\n', { allowBlocking: true }),
      /normalize: column 'missing' not found/
    )
    assertThrows(
      () => tf.run('jsonl | acf x 2 | csv', '{"x":true}\n', { allowBlocking: true }),
      /acf: column 'x' must be numeric/
    )
    const result = tf.run('csv | ewma x 0.5 on_type_error=null | anomaly missing missing=null | bin missing 10 missing=null | window missing 3 avg missing=null | rolling-sum missing 3 sum3 missing=null | rolling-mean x 3 mean3 on_type_error=null | interpolate interp forward missing=null | datetime dt year missing=null | date-trunc d month result=d_month missing=null | csv', 'x\na\n', { allowBlocking: true })
    const lines = result.outputText.trim().split('\n')
    assert(lines[0] === 'x,x_ewma,missing_anomaly,missing_bin,missing_avg3,sum3,mean3,interp,dt_year,d_month', 'wasm H14 null policy header')
    assert(lines[1] === 'a,,,,,,,,,', 'wasm H14 null policy row')
    const typeNull = tf.run('csv | interpolate x forward on_type_error=null | csv', 'x\na\n', { allowBlocking: true })
    assert(typeNull.outputText.split('\n')[1] === '', 'wasm interpolate type null should null source value')
    const dateTypeNull = tf.run('jsonl | date-trunc x month on_type_error=null | csv', '{"x":true}\n')
    assert(dateTypeNull.outputText.split('\n')[1] === '', 'wasm date-trunc type null should null source value')
    const normalizeNull = tf.run('csv | normalize missing missing=null | csv', 'x\n1\n', { allowBlocking: true })
    assert(normalizeNull.outputText.trim().split('\n').join('|') === 'x,missing|1,', 'wasm normalize missing=null should append nulls')
    const acfNull = tf.run('csv | acf missing 2 missing=null | csv', 'x\n1\n', { allowBlocking: true })
    assert(acfNull.outputText.trim().split('\n').join('|') === 'lag,acf|0,|1,|2,', 'wasm acf missing=null should emit null acf rows')
  })

  await test('wasm native memory policy', async () => {
    assertThrows(
      () => tf.run('csv | sort age | csv', 'name,age\nAlice,30\nBob,25\n'),
      /blocking step 'sort'/
    )
    const sorted = tf.run('csv | sort age | csv', 'name,age\nAlice,30\nBob,25\n', { allowBlocking: true })
    assert(sorted.outputText.includes('Bob,25'), 'wasm allowBlocking should run known-small sort')
    assertThrows(
      () => tf.run('csv | unique name | csv', 'name,age\nAlice,30\nBob,25\n', { memory: '64KB' }),
      /needs max_keys/
    )
    const bounded = tf.run('csv | unique name max_keys=2 | csv', 'name,age\nAlice,30\nBob,25\n', { memory: '64KB' })
    assert(bounded.outputText.includes('Alice,30'), 'wasm capped unique should run under memory policy')
    const byteBounded = tf.run('csv | unique name max_state_bytes=4096 | csv', 'name,age\nAlice,30\nBob,25\n', { memory: '64KB' })
    assert(byteBounded.outputText.includes('Alice,30'), 'wasm byte-capped unique should run under memory policy')
    const grouped = tf.run('csv | group-agg city sum:sales:total max_state_bytes=8192 | csv', 'city,sales\nNY,1\nLA,2\n', { memory: '64KB' })
    assert(grouped.outputText.includes('NY,1'), 'wasm byte-capped group-agg should run under memory policy')
    const freq = tf.run('csv | frequency city max_state_bytes=2048 | csv', 'city\nNY\nLA\n', { memory: '64KB' })
    assert(freq.outputText.includes('NY,1'), 'wasm byte-capped frequency should run under memory policy')

    const freqAudit = tf.run('csv | frequency city max_values=2 overflow=other other=REST audit audit_limit=1 | csv', 'city\nNY\nLA\nSF\nTX\n')
    assert(freqAudit.outputText.includes('REST,2'), 'wasm overflow frequency should collapse categories')
    assert(freqAudit.statsText.includes('"event":"category_overflow"'), 'wasm overflow audit should emit')
    assert(freqAudit.statsText.includes('"value":"SF"'), 'wasm overflow audit should include first overflow')
    assert(freqAudit.statsText.includes('"bucket":"REST"'), 'wasm overflow audit should include bucket')
    assert(!freqAudit.statsText.includes('"value":"TX"'), 'wasm overflow audit should enforce audit_limit')
    const rowid = tf.run('csv | rowid name max_state_bytes=2048 | csv', 'name\nAlice\nBob\n', { memory: '64KB' })
    assert(rowid.outputText.includes('Alice,1'), 'wasm byte-capped rowid should run under memory policy')
    const onehot = tf.run('csv | onehot city max_state_bytes=2048 | csv', 'city\nNY\nLA\n', { memory: '64KB' })
    assert(onehot.outputText.includes('city_NY'), 'wasm byte-capped onehot should run under memory policy')
    const label = tf.run('csv | label-encode city max_state_bytes=2048 | csv', 'city\nNY\nLA\n', { memory: '64KB' })
    assert(label.outputText.includes('city_encoded'), 'wasm byte-capped label-encode should run under memory policy')
    const pivot = tf.run('csv batch_size=1 | pivot metric value sum categories=x,y sorted=true | csv', 'name,metric,value\nA,x,1\nA,y,2\nB,x,3\n', { memory: '64KB' })
    assert(pivot.outputText.includes('A,1,2'), 'wasm sorted pivot should run under memory policy')
    assert(pivot.outputText.includes('B,3,'), 'wasm sorted pivot should emit final group')
  })

  await test('wasm native spill policy rejects spillDir', async () => {
    assertThrows(
      () => tf.run('csv | sort age | csv', 'name,age\nAlice,30\nBob,25\n', { spillDir: '/tmp/tranfi-wasm-spill', memory: '1KB' }),
      /spillDir is not supported by standalone WASM native execution/
    )
  })

  await test('wasm host policy rejects core file paths', async () => {
    assertThrows(
      () => tf.run('csv | join /tmp/tranfi-wasm-missing.csv on id max_lookup_bytes=1024 max_state_bytes=4096 | csv', 'id\n1\n'),
      /allow_fs=false|host policy export unavailable/
    )
  })

  await test('wasm compileDsl', async () => {
    const json = tf.compileDsl('csv | head 5 | csv')
    const plan = JSON.parse(json)
    assert(plan.steps.length === 3, 'should have 3 steps')
    assert(plan.steps[1].memory_class === 'bounded_state', 'wasm memory metadata')
    assert(plan.steps[1].emit_class === 'per_batch', 'wasm emit metadata')
    assert(plan.steps[1].state_estimate === 'O(1)', 'wasm state estimate')
    const aliasPlan = JSON.parse(tf.compileDsl('csv | summarise city count:*:rows | csv'))
    assert(aliasPlan.steps[1].op === 'group-agg', 'wasm should normalize summarise alias')
    const csvPlan = JSON.parse(tf.compileDsl('csv batch_size=2 nulls=NA,NULL quoted_nulls=false | csv'))
    assert(csvPlan.steps[0].args.nulls === 'NA,NULL', 'wasm compileDsl should keep csv null literals')
    assert(csvPlan.steps[0].args.quoted_nulls === false, 'wasm compileDsl should keep quoted_nulls=false')
    const sortedJoinPlan = JSON.parse(tf.compileDsl('csv | semi-join lookup.csv on id sorted=true | csv'))
    assert(sortedJoinPlan.steps[1].op === 'semi-join', 'wasm compileDsl should parse semi-join')
    assert(sortedJoinPlan.steps[1].args.sorted === true, 'wasm compileDsl should preserve sorted join flag')
    const joinPlan = JSON.parse(tf.compileDsl('csv | join lookup.csv on id max_lookup_bytes=1024 max_state_bytes=4096 | csv'))
    assert(joinPlan.steps[1].args.max_state_bytes === 4096, 'wasm join should preserve max_state_bytes')
    const pivotPlan = JSON.parse(tf.compileDsl('csv | pivot metric value sum categories=x,y sorted=true | csv'))
    assert(pivotPlan.steps[1].op === 'pivot', 'wasm compileDsl should parse pivot')
    assert(pivotPlan.steps[1].memory_class === 'bounded_state', 'wasm sorted pivot metadata')
    assert(pivotPlan.steps[1].emit_class === 'mixed', 'wasm sorted pivot emit metadata')
    const sortedSetPlan = JSON.parse(tf.compileDsl('csv | intersect lookup.csv columns=id sorted=true | csv'))
    assert(sortedSetPlan.steps[1].op === 'intersect', 'wasm compileDsl should parse intersect')
    assert(sortedSetPlan.steps[1].args.sorted === true, 'wasm compileDsl should preserve sorted set flag')
    const sortedBagSetPlan = JSON.parse(tf.compileDsl('csv | setdiff-all lookup.csv columns=id sorted=true | csv'))
    assert(sortedBagSetPlan.steps[1].op === 'setdiff-all', 'wasm compileDsl should parse sorted setdiff-all')
    assert(sortedBagSetPlan.steps[1].memory_class === 'bounded_state', 'wasm sorted bag set metadata')
    const slicePlan = JSON.parse(tf.compileDsl('csv | slice_head n=2 | slice_tail 1 | csv'))
    assert(slicePlan.steps[1].op === 'slice-head', 'wasm compileDsl should normalize slice_head')
    assert(slicePlan.steps[1].memory_class === 'bounded_state', 'wasm slice-head memory metadata')
    assert(slicePlan.steps[1].emit_class === 'per_batch', 'wasm slice-head emit metadata')
    assert(slicePlan.steps[2].op === 'slice-tail', 'wasm compileDsl should normalize slice_tail')
    assert(slicePlan.steps[2].memory_class === 'bounded_state', 'wasm slice-tail memory metadata')
    assert(slicePlan.steps[2].emit_class === 'on_flush', 'wasm slice-tail emit metadata')
    const rankPlan = JSON.parse(tf.compileDsl('csv | slice-min score n=2 with_ties=false | csv'))
    assert(rankPlan.steps[1].args.with_ties === false, 'wasm slice-min should preserve with_ties=false')
    let rejectedTies = false
    try { tf.compileDsl('csv | slice-min score n=2 with_ties=true | csv') } catch (e) { rejectedTies = String(e.message || e).includes('with_ties=true') }
    assert(rejectedTies, 'wasm compileDsl should reject with_ties=true')
    const unionAllPlan = JSON.parse(tf.compileDsl('csv | union_all lookup.csv max_lookup_rows=5 | csv'))
    assert(unionAllPlan.steps[1].op === 'union-all', 'wasm compileDsl should normalize union_all alias')
    assert(unionAllPlan.steps[1].memory_class === 'bounded_state', 'wasm union-all memory metadata')
    const unionPlan = JSON.parse(tf.compileDsl('csv | union lookup.csv max_output_keys=7 max_state_bytes=4096 | csv'))
    assert(unionPlan.steps[1].op === 'union', 'wasm compileDsl should parse union')
    assert(unionPlan.steps[1].memory_class === 'key_state', 'wasm union memory metadata')
    assert(unionPlan.steps[1].args.max_state_bytes === 4096, 'wasm union should preserve max_state_bytes')
    const sortedUnionPlan = JSON.parse(tf.compileDsl('csv | union lookup.csv columns=id sorted=true | csv'))
    assert(sortedUnionPlan.steps[1].memory_class === 'bounded_state', 'wasm sorted union metadata')
    assert(sortedUnionPlan.steps[1].emit_class === 'mixed', 'wasm sorted union emit metadata')
    const bagSpillPlan = JSON.parse(tf.compileDsl('csv | setdiff-all lookup.csv columns=id spill_dir=/tmp/tranfi-spill spill_memory_bytes=1024 | csv'))
    assert(bagSpillPlan.steps[1].memory_class === 'external', 'wasm bag spill metadata')
    assert(bagSpillPlan.steps[1].emit_class === 'on_flush', 'wasm bag spill emit metadata')
    const setBytePlan = JSON.parse(tf.compileDsl('csv | intersect lookup.csv max_lookup_bytes=1024 max_state_bytes=4096 | csv'))
    assert(setBytePlan.steps[1].args.max_state_bytes === 4096, 'wasm intersect should preserve max_state_bytes')

    const validateFilePlan = JSON.parse(tf.compileDsl('csv | validate rules_file=quality_rules.json | csv'))
    assert(validateFilePlan.steps[1].op === 'validate', 'wasm validate rules_file should parse validate op')
    assert(validateFilePlan.steps[1].args.rules_file === 'quality_rules.json', 'wasm validate should preserve rules_file arg')
  })

  await test('wasm csv null literals', async () => {
    const result = tf.run('csv batch_size=2 nulls=NA,NULL | csv', 'name,score,note\nA,10,ok\nB,NA,bad\nC,NULL,NULL\nD,5,NA\n')
    assert(result.outputText.includes('A,10,ok'), 'wasm csv should keep ordinary values')
    assert(result.outputText.includes('B,,bad'), 'wasm csv should null NA score')
    assert(result.outputText.includes('C,,'), 'wasm csv should null NULL score and note')
    assert(result.outputText.includes('D,5,'), 'wasm csv should null NA note')
    assert(!result.outputText.includes('NA'), 'wasm csv null literal should not survive output')
    assert(!result.outputText.includes('NULL'), 'wasm csv null literal should not survive output')

    const quoted = tf.run('csv batch_size=2 nulls=NA quoted_nulls=false | fill-null note=MISSING | csv', 'name,note\nA,"NA"\nB,NA\nC,""\nD,\n')
    assert(quoted.outputText.includes('A,NA'), 'wasm quoted NA should stay a string')
    assert(quoted.outputText.includes('B,MISSING'), 'wasm unquoted NA should become null')
    assert(quoted.outputText.includes('C,'), 'wasm quoted empty should stay non-null')
    assert(!quoted.outputText.includes('C,MISSING'), 'wasm quoted empty should not be filled')
    assert(quoted.outputText.includes('D,MISSING'), 'wasm unquoted empty should remain null')
  })


  await test('wasm fill-null and replace audit side channel', async () => {
    const result = tf.run('csv batch_size=2 nulls=NA | fill-null note=MISSING audit audit_limit=1 | replace name Alice Alicia audit audit_limit=1 | csv', 'name,note\nAlice,NA\nAlice Jones,\nBob,ok\n')
    assert(result.outputText.includes('Alicia,MISSING'), 'wasm audited pipeline should fill and replace')
    assert(result.statsText.includes('"op":"fill-null"'), 'wasm fill-null audit should emit')
    assert(result.statsText.includes('"event":"null_filled"'), 'wasm fill-null audit should identify event')
    assert(result.statsText.includes('"op":"replace"'), 'wasm replace audit should emit')
    assert(result.statsText.includes('"event":"value_changed"'), 'wasm replace audit should identify event')
    assert(result.statsText.includes('"before":null'), 'wasm fill-null audit should include before null')
    assert(result.statsText.includes('"before":"Alice"'), 'wasm replace audit should include before string')

    const castAudit = tf.run('csv batch_size=2 | cast age=int audit audit_limit=3 | csv', 'age\n10\nbad\n12x\n')
    assert(castAudit.outputText.includes('12'), 'wasm cast audit should preserve partial legacy coercion')
    assert(castAudit.statsText.includes('"op":"cast"'), 'wasm cast audit should emit')
    assert(castAudit.statsText.includes('"event":"coercion_failed"'), 'wasm cast audit should identify coercion failures')
    assert(castAudit.statsText.includes('"reason":"invalid_integer"'), 'wasm cast audit should include invalid reason')
    assert(castAudit.statsText.includes('"reason":"trailing_characters"'), 'wasm cast audit should include trailing reason')
    assert(castAudit.statsText.includes('"on_error":"coerce"'), 'wasm cast audit should include default policy')

    const castNull = tf.run('csv batch_size=2 | cast age=int on_error=null audit audit_limit=3 | csv', 'age\n10\nbad\n12x\n')
    const castNullLines = castNull.outputText.split(/\r?\n/)
    assert(castNullLines[1] === '10', 'wasm cast null policy should preserve valid output')
    assert(castNullLines[2] === '', 'wasm cast null policy should null invalid integer')
    assert(castNullLines[3] === '', 'wasm cast null policy should null partial integer')
    assert(castNull.statsText.includes('"on_error":"null"'), 'wasm cast audit should include null policy')
    assert(castNull.statsText.includes('"coercion_nulled":2'), 'wasm cast stats should count nulled values')

    const normalizeAudit = tf.run('csv batch_size=2 | normalize x minmax audit audit_limit=1 | csv', 'x\n10\n20\n30\n', { allowBlocking: true })
    assert(normalizeAudit.outputText.includes('0.5'), 'wasm normalize audit should preserve output')
    assert(normalizeAudit.statsText.includes('"op":"normalize"'), 'wasm normalize audit should emit')
    assert(normalizeAudit.statsText.includes('"event":"value_changed"'), 'wasm normalize audit should identify changes')
    assert(normalizeAudit.statsText.includes('"row":1'), 'wasm normalize audit should include first row')
    assert(!normalizeAudit.statsText.includes('"row":2'), 'wasm normalize audit should enforce audit_limit')

    const validateAudit = tf.run('csv batch_size=1 | validate "col(age) > 27" audit audit_limit=1 max_failures=2 name=age_check message=age_low | csv', 'name,age\nAlice,30\nBob,25\nCara,20\n')
    assert(validateAudit.outputText.includes('_valid'), 'wasm validate audit should keep annotation column')
    assert(validateAudit.outputText.includes('Cara'), 'wasm validate audit should keep all rows')
    assert(validateAudit.statsText.includes('"op":"validate"'), 'wasm validate audit should emit')
    assert(validateAudit.statsText.includes('"event":"validation_failed"'), 'wasm validate audit should identify event')
    assert(validateAudit.statsText.includes('"checked_rows":3'), 'wasm validate stats should count checked rows')
    assert(validateAudit.statsText.includes('"failed_rows":2'), 'wasm validate stats should count failed rows')
    assert(validateAudit.statsText.includes('"audit_emitted":1'), 'wasm validate stats should count emitted audits')
    assert(validateAudit.statsText.includes('"max_failures":2'), 'wasm validate stats should include max_failures')
    assert(validateAudit.statsText.includes('"name":"age_check"'), 'wasm validate audit should include name')
    assert(validateAudit.statsText.includes('"row":2'), 'wasm validate audit should include first invalid row')
    assert(!validateAudit.statsText.includes('"row":3'), 'wasm validate audit should enforce audit_limit')

    const quarantined = tf.run('csv batch_size=1 | quarantine "col(age) < 25" name=young message=too_young | csv', 'name,age\nAlice,30\nBob,20\nCara,10\nDana,40\n')
    assert(quarantined.outputText.includes('Alice,30'), 'wasm quarantine should keep Alice')
    assert(quarantined.outputText.includes('Dana,40'), 'wasm quarantine should keep Dana')
    assert(!quarantined.outputText.includes('Bob'), 'wasm quarantine should drop Bob from main')
    const quarantineErrors = new TextDecoder().decode(quarantined.errors)
    assert(quarantineErrors.includes('"event":"row_quarantined"'), 'wasm quarantine should emit errors')
    assert(quarantineErrors.includes('"Bob"'), 'wasm quarantine should include row data')
    assert(quarantined.statsText.includes('"op":"quarantine"'), 'wasm quarantine stats should identify op')
    assert(quarantined.statsText.includes('"quarantined_rows":2'), 'wasm quarantine stats should count rows')
  })

  await test('wasm csv repair diagnostics', async () => {
    const result = tf.run('csv batch_size=2 mode=repair max_error_bytes=5 audit audit_limit=1 | csv', 'a,b,c\n1,2\n3,4,5,6\n')
    assert(result.outputText.includes('1,2,'), 'wasm repair should pad short row')
    assert(result.outputText.includes('3,4,5'), 'wasm repair should truncate long row')
    assert(!result.outputText.includes('3,4,5,6'), 'wasm repair should drop extra field')
    const errors = new TextDecoder().decode(result.errors)
    assert(errors.includes('csv_field_count'), 'wasm repair should emit diagnostics')
    assert(errors.includes('"mode":"repair"'), 'wasm repair diagnostic should include mode')
    assert(errors.includes('"byte_offset":6'), 'wasm repair diagnostic should include byte offset')
    assert(errors.includes('"raw":"3,4,5"'), 'wasm repair diagnostic should cap raw preview')
    assert(result.statsText.includes('"type":"audit"'), 'wasm repair audit should emit audit record')
    assert(result.statsText.includes('"event":"row_repaired"'), 'wasm repair audit should identify repaired row')
    assert(result.statsText.includes('"line":2'), 'wasm repair audit should include first repaired line')
    assert(result.statsText.includes('"raw":"1,2"'), 'wasm repair audit should include raw preview')
    assert(!result.statsText.includes('"line":3'), 'wasm repair audit should enforce audit_limit')
  })

  await test('wasm csv max record bytes', async () => {
    try {
      tf.run('csv max_record_bytes=8 | csv', 'a,b\n123456789')
      assert(false, 'wasm oversized CSV record should fail')
    } catch (err) {
      assert(String(err.message || err).includes('csv record exceeds max_record_bytes'), 'wasm error should mention max_record_bytes')
    }
  })

  await test('wasm jsonl and text max record bytes', async () => {
    try {
      tf.run('jsonl max_record_bytes=16 | csv', '{"id":1}\n{"name":"abcdefghijklmnop"}')
      assert(false, 'wasm oversized JSONL record should fail')
    } catch (err) {
      assert(String(err.message || err).includes('jsonl record exceeds max_record_bytes'), 'wasm JSONL error should mention max_record_bytes')
    }

    try {
      tf.run('text max_record_bytes=8 | text', 'ok\n123456789')
      assert(false, 'wasm oversized text record should fail')
    } catch (err) {
      assert(String(err.message || err).includes('text record exceeds max_record_bytes'), 'wasm text error should mention max_record_bytes')
    }
  })


  await test('wasm codec batch_size clamp', async () => {
    try {
      tf.run('csv batch_size=65537 | csv', 'a\n1\n')
      assert(false, 'wasm oversized batch_size should fail')
    } catch (err) {
      assert(String(err.message || err).includes('batch_size'), 'wasm error should mention batch_size')
    }
  })

  await test('wasm csv quoted record boundaries', async () => {
    const quoted = tf.run('csv batch_size=1 | csv', 'id,note\n1,"hello ""world\nline"\n2,ok\n')
    assert(quoted.outputText.includes('1,"hello ""world\nline"'), 'wasm quoted newline should remain one row')
    assert(quoted.outputText.includes('2,ok'), 'wasm row after quoted newline should survive')
    const unquoted = tf.run('csv batch_size=1 | csv', 'a,b\nx"y,1\nz,2\n')
    assert(unquoted.outputText.includes('"x""y",1'), 'wasm unquoted quote should be data')
    assert(unquoted.outputText.includes('z,2'), 'wasm unquoted quote should not merge records')
  })

  await test('wasm run onOutput without collecting output', async () => {
    const chunks = []
    const result = tf.run('csv | csv', 'name,age\nAlice,30\nBob,25\nCharlie,35\n', {
      chunkSize: 8,
      onOutput: chunk => chunks.push(chunk.slice()),
      collectOutput: false
    })
    const text = Buffer.from(chunks.reduce((acc, c) => {
      const next = new Uint8Array(acc.length + c.length)
      next.set(acc, 0)
      next.set(c, acc.length)
      return next
    }, new Uint8Array(0))).toString('utf-8')
    assert(result.output.length === 0, 'wasm should not collect main output')
    assert(chunks.length > 1, 'wasm should emit multiple bounded chunks')
    assert(text.includes('name,age'), 'wasm should have header')
    assert(text.includes('Charlie'), 'wasm should have Charlie')
  })


  await test('wasm Worker wrapper streams output and progress', async () => {
    const { Worker } = require('worker_threads')
    const { createWorkerClient } = require('../js/wasm/worker.js')
    const worker = new Worker(join(__dirname, '../js/wasm/worker.js'))
    const client = createWorkerClient(worker, { terminateOnDispose: true })
    try {
      const out = []
      const progress = []
      const result = await client.run('csv batch_size=1 | filter "col(\'age\') > 25" | csv',
        'name,age\nAlice,30\nBob,20\nCharlie,35\n', {
          chunkSize: 8,
          collectOutput: false,
          onOutput: chunk => out.push(Buffer.from(chunk)),
          onProgress: snapshot => progress.push(snapshot)
        })
      const text = Buffer.concat(out).toString('utf-8')
      assert(result.output.length === 0, 'worker run should not collect main output when disabled')
      assert(text.includes('Alice,30'), 'worker output should include Alice')
      assert(text.includes('Charlie,35'), 'worker output should include Charlie')
      assert(!text.includes('Bob,20'), 'worker output should filter Bob')
      assert(result.statsText.includes('"execution_target":"wasm"'), 'worker should return stats side channel')
      assert(progress.some(p => p.phase === 'push'), 'worker progress should include push phase')
      assert(progress.some(p => p.phase === 'pull'), 'worker progress should include pull phase')
      assert(progress.some(p => p.phase === 'done' && p.finished === true), 'worker progress should include done phase')
      const last = progress[progress.length - 1]
      assert(last.bytesIn > 0, 'worker progress should count input bytes')
      assert(last.bytesOut > 0, 'worker progress should count output bytes')
    } finally {
      await client.dispose()
    }
  })

  await test('wasm Worker wrapper propagates errors', async () => {
    const { Worker } = require('worker_threads')
    const { createWorkerClient } = require('../js/wasm/worker.js')
    const worker = new Worker(join(__dirname, '../js/wasm/worker.js'))
    const client = createWorkerClient(worker, { terminateOnDispose: true })
    try {
      await assertRejects(
        () => client.run('csv | sort age | csv', 'name,age\nAlice,30\nBob,25\n'),
        /blocking step 'sort'/
      )
    } finally {
      await client.dispose()
    }
  })

  await test('wasm Worker wrapper supports chunk-stream cancellation', async () => {
    const { Worker } = require('worker_threads')
    const { createWorkerClient } = require('../js/wasm/worker.js')
    const worker = new Worker(join(__dirname, '../js/wasm/worker.js'))
    const client = createWorkerClient(worker, { terminateOnDispose: true })
    const controller = new AbortController()
    async function *chunks() {
      yield 'name,age\nAlice,30\n'
      controller.abort()
      yield 'Bob,25\n'
    }
    try {
      await assertRejects(
        () => client.runChunks('csv | csv', chunks(), { chunkSize: 8, signal: controller.signal }),
        /cancelled/
      )
    } finally {
      await client.dispose()
    }
  })

  await test('wasm iterChunks yields bounded output chunks', async () => {
    const chunks = Array.from(tf.iterChunks('csv | csv', 'name,age\nAlice,30\nBob,25\nCharlie,35\n', { chunkSize: 7 }))
    const text = Buffer.from(chunks.reduce((acc, c) => {
      const next = new Uint8Array(acc.length + c.length)
      next.set(acc, 0)
      next.set(c, acc.length)
      return next
    }, new Uint8Array(0))).toString('utf-8')
    assert(chunks.length > 1, 'wasm should yield multiple bounded chunks')
    assert(text.includes('Alice'), 'wasm chunks should have Alice')
    assert(text.includes('Charlie'), 'wasm chunks should have Charlie')
  })


  await test('app WASM runner keeps bounded output preview', async () => {
    const runnerSource = readFileSync(join(__dirname, '../app/public/wasm/tranfi-runner.js'), 'utf-8')
    const createTranfiCore = require('../js/wasm/tranfi_core.js')
    const sandbox = {
      self: {},
      createTranfi: createTranfiCore,
      TextDecoder,
      TextEncoder,
      Uint8Array,
      ArrayBuffer,
      Number,
      Math,
      Error,
      console
    }
    sandbox.self = sandbox
    vm.createContext(sandbox)
    vm.runInContext(runnerSource, sandbox, { filename: 'tranfi-runner.js' })

    async function *chunks() {
      yield Buffer.from('name,age\nAlice,30\nBob,25\n')
      yield Buffer.from('Charlie,35\nDiana,40\nEve,45\n')
    }
    const progress = []
    const result = await sandbox.tranfiRunner({
      file: { size: 48, [Symbol.asyncIterator]: chunks },
      dsl: 'csv batch_size=1 | csv',
      preview_rows: 2,
      collect_output: false
    }, {
      log() {},
      progress(v) { progress.push(v) }
    })

    assert(result.output.preview === true, 'app runner output should be marked as preview')
    assert(result.output.rows.length === 2, 'app runner should retain only preview rows')
    assert(result.output.rows_seen === 5, 'app runner should count all streamed output rows')
    assert(result.output.truncated === true, 'app runner should mark truncated preview')
    assert(!Object.prototype.hasOwnProperty.call(result, 'output_full'), 'app runner should not collect full output unless requested')
    assert(result.output.rows[0][0] === 'Alice', 'preview should include first data row')
    assert(result.output.rows[1][0] === 'Bob', 'preview should include second data row')
    assert(progress.length > 0, 'app runner should report progress')
  })

  await test('app WASM runner can explicitly collect full output', async () => {
    const runnerSource = readFileSync(join(__dirname, '../app/public/wasm/tranfi-runner.js'), 'utf-8')
    const createTranfiCore = require('../js/wasm/tranfi_core.js')
    const sandbox = {
      self: {},
      createTranfi: createTranfiCore,
      TextDecoder,
      TextEncoder,
      Uint8Array,
      ArrayBuffer,
      Number,
      Math,
      Error,
      console
    }
    sandbox.self = sandbox
    vm.createContext(sandbox)
    vm.runInContext(runnerSource, sandbox, { filename: 'tranfi-runner.js' })

    const result = await sandbox.tranfiRunner({
      file: Buffer.from('name,age\nAlice,30\nBob,25\n'),
      dsl: 'csv | csv',
      preview_rows: 1,
      collect_output: true
    }, { log() {}, progress() {} })

    assert(result.output.rows.length === 1, 'explicit collect should still keep bounded preview table')
    assert(result.output_full.includes('Alice,30'), 'explicit collect should return full output text')
    assert(result.output_full.includes('Bob,25'), 'explicit collect should include all output rows')
  })

  await test('wasm recipes', async () => {
    const r = tf.recipes()
    assert(r.length === 22, `expected 22 wasm recipes, got ${r.length}`)
    assert(r[0].name, 'recipe should have name')
    assert(r.some(x => x.name === 'sniff'), 'wasm recipes should include sniff')
  })
} else {
  console.log('  (skipped — wasm/index.js not found)')
}

console.log(`\n====================`)
console.log(`${passed}/${tests} tests passed`)
process.exit(passed === tests ? 0 : 1)

} // end main

main().catch(err => {
  process.stderr.write(`error: ${err.message}\n`)
  process.exit(1)
})
