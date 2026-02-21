/**
 * test_node.js — Integration tests for the Node.js tranfi package.
 *
 * Run: node test/test_node.js
 */

import { pipeline, codec, ops, expr, compileDsl, compileToSql, loadRecipe, saveRecipe, recipes } from '../js/src/index.js'
import { fileURLToPath } from 'url'
import { dirname, join } from 'path'

const __dirname = dirname(fileURLToPath(import.meta.url))
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

// ---- Tests ----

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

console.log('\nMisc:')

await test('stats channel', async () => {
  const p = pipeline([
    codec.csv(),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'x\n1\n2\n3\n' })
  assert(result.stats.length > 0, 'should have stats')
  assert(result.statsText.includes('rows_in'), 'stats should have rows_in')
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
    codec.csv(),
    ops.validate(expr("col('age') > 27")),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'name,age\nAlice,30\nBob,25\n' })
  const text = result.outputText
  assert(text.includes('_valid'), 'should have _valid column')
  const lines = text.trim().split('\n')
  assert(lines.length === 3, 'should keep all rows')
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
  const p = pipeline([
    codec.csv(),
    ops.frequency(['city']),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'city\nNY\nLA\nNY\nNY\nLA\n' })
  const text = result.outputText
  assert(text.includes('value'), 'should have value column')
  assert(text.includes('count'), 'should have count column')
  assert(text.includes('NY'), 'should have NY')
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

await test('sample', async () => {
  const p = pipeline([
    codec.csv(),
    ops.sample(2),
    codec.csvEncode(),
  ])
  const result = await p.run({ input: 'name\nAlice\nBob\nCharlie\nDiana\nEve\n' })
  const text = result.outputText
  const lines = text.trim().split('\n')
  assert(lines.length === 3, `should have 3 lines (header + 2), got ${lines.length}`)
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
  const result = await p.run({ input: 'name,age\nAlice,30\nBob,25\n' })
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
  assert(data.steps[2].op === 'codec.csv.encode', 'third step is csv encode')
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
  const { unlinkSync } = await import('fs')
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

await test('recipes() returns 21 entries', async () => {
  const r = await recipes()
  assert(r.length === 21, `expected 21 recipes, got ${r.length}`)
  const names = r.map(x => x.name)
  assert(names.includes('profile'), 'should include profile')
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

import { startServer } from '../js/src/server.js'
import { writeFileSync, mkdirSync, rmSync, existsSync } from 'fs'

const testDataDir = '/tmp/tranfi-test-serve'
const testAppDir = join(__dirname, '..', '..', 'app', 'dist')
const hasApp = existsSync(join(testAppDir, 'index.html'))

if (hasApp) {
  // Set up test data
  if (existsSync(testDataDir)) rmSync(testDataDir, { recursive: true })
  mkdirSync(testDataDir, { recursive: true })
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
} else {
  console.log('  (skipped — app/dist/ not found)')
}

// ---- SQL Transpiler ----

console.log('\nSQL Transpiler:')

await test('compileToSql basic', async () => {
  const sql = await compileToSql('csv | head 10 | csv')
  assert(sql.includes('LIMIT 10'), 'should have LIMIT')
})

await test('compileToSql filter', async () => {
  const sql = await compileToSql('csv | filter "col(\'age\') > 25" | csv')
  assert(sql.includes('WHERE'), 'should have WHERE')
  assert(sql.includes('"age"'), 'should quote column')
})

await test('compileToSql select', async () => {
  const sql = await compileToSql('csv | select name,age | csv')
  assert(sql.includes('"name"'), 'should have name')
  assert(sql.includes('"age"'), 'should have age')
})

await test('compileToSql derive', async () => {
  const sql = await compileToSql("csv | derive total=col('a')+col('b') | csv")
  assert(sql.includes('"total"'), 'should have derived column')
})

// ---- DuckDB Engine ----

let hasDuckDB = false
try {
  const { createRequire } = await import('module')
  const req = createRequire(join(__dirname, '..', 'js', 'package.json'))
  req('duckdb')
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
  const mod = await import('../js/wasm/index.js')
  createTranfi = mod.default
} catch {}

if (createTranfi) {
  const tf = await createTranfi()

  await test('wasm version', async () => {
    const v = tf.version()
    assert(v === '0.1.0', `expected 0.1.0, got ${v}`)
  })

  await test('wasm compileToSql', async () => {
    const sql = tf.compileToSql('csv | filter "col(\'age\') > 25" | csv')
    assert(sql.includes('WHERE'), 'should have WHERE')
    assert(sql.includes('"age"'), 'should quote column')
  })

  await test('wasm run native', async () => {
    const result = tf.run('csv | filter "col(\'age\') > 25" | csv',
      'name,age\nAlice,30\nBob,20\nCharlie,35\n')
    assert(result.outputText.includes('Alice'), 'should have Alice')
    assert(result.outputText.includes('Charlie'), 'should have Charlie')
    assert(!result.outputText.includes('Bob'), 'should not have Bob')
  })

  await test('wasm compileDsl', async () => {
    const json = tf.compileDsl('csv | head 5 | csv')
    const plan = JSON.parse(json)
    assert(plan.steps.length === 3, 'should have 3 steps')
  })

  await test('wasm recipes', async () => {
    const r = tf.recipes()
    assert(r.length > 0, 'should have recipes')
    assert(r[0].name, 'recipe should have name')
  })
} else {
  console.log('  (skipped — wasm/index.js not found)')
}

console.log(`\n====================`)
console.log(`${passed}/${tests} tests passed`)
process.exit(passed === tests ? 0 : 1)
