'use strict'
const assert = require('node:assert/strict')
const test = require('node:test')
const { spawnSync } = require('node:child_process')
const { mkdtempSync, readFileSync, writeFileSync, rmSync } = require('node:fs')
const { tmpdir } = require('node:os')
const path = require('node:path')
const input = 'name,age\nBob,30\nAlice,20\n'
function run(args) {
  return spawnSync(process.execPath, [path.join(__dirname, '../js/src/cli.js'), ...args], { input, encoding: 'utf8' })
}
test('CLI rejects blocking unless explicitly allowed', () => {
  const denied = run(['csv | sort age | csv'])
  assert.notEqual(denied.status, 0)
  assert.match(denied.stderr, /blocking step 'sort'/)
  const allowed = run(['--allow-blocking', '-q', 'csv | sort age | csv'])
  assert.equal(allowed.status, 0, allowed.stderr)
  assert.equal(allowed.stdout, 'name,age\nAlice,20\nBob,30\n')
})
test('CLI memory cap, spill and stats follow API policy', () => {
  const denied = run(['--allow-blocking', '--memory', '64KB', 'csv | sort age | csv'])
  assert.notEqual(denied.status, 0)
  assert.match(denied.stderr, /byte caps/)
  const dir = mkdtempSync(path.join(tmpdir(), 'tranfi-cli-'))
  try {
    const stats = path.join(dir, 'stats.jsonl')
    const result = run(['--memory=64KB', '--spill-dir', dir, '--stats-json', stats, '-q', 'csv | sort age | csv'])
    assert.equal(result.status, 0, result.stderr)
    assert.equal(result.stdout, 'name,age\nAlice,20\nBob,30\n')
    assert.match(readFileSync(stats, 'utf8'), /step_stats/)
    assert.equal(result.stderr, '')
  } finally { rmSync(dir, { recursive: true, force: true }) }
})
test('CLI compile targets, recipes and invalid options', () => {
  const sql = run(['--target', 'sql', '--dialect', 'duckdb', 'csv | head 1 | csv'])
  assert.equal(sql.status, 0, sql.stderr)
  assert.match(sql.stdout, /SELECT/)
  const plan = run(['--target=json', 'preview'])
  assert.equal(plan.status, 0, plan.stderr)
  assert.equal(JSON.parse(plan.stdout).steps[1].op, 'head')
  assert.notEqual(run(['--allow-blocking', '--fail-on-blocking', 'csv | csv']).status, 0)
  assert.notEqual(run(['--memory']).status, 0)
})

test('CLI writes files and preserves output when policy rejects a plan', () => {
  const dir = mkdtempSync(path.join(tmpdir(), 'tranfi-cli-output-'))
  try {
    const output = path.join(dir, 'out.csv')
    writeFileSync(output, 'keep me')
    const denied = run(['-o', output, 'csv | sort age | csv'])
    assert.notEqual(denied.status, 0)
    assert.equal(readFileSync(output, 'utf8'), 'keep me')
    const allowed = run(['-q', '-o', output, 'csv | head 1 | csv'])
    assert.equal(allowed.status, 0, allowed.stderr)
    assert.equal(readFileSync(output, 'utf8'), 'name,age\nBob,30\n')
    const missing = run(['-o', path.join(dir, 'missing/out.csv'), 'csv | csv'])
    assert.notEqual(missing.status, 0)
    assert.match(missing.stderr, /error: .*ENOENT/)
    assert.doesNotMatch(missing.stderr, /Unhandled/)
  } finally { rmSync(dir, { recursive: true, force: true }) }
})

for (const native of [false, true]) {
  test(`CLI version follows npm metadata with native=${native}`, () => {
    const cli = path.join(__dirname, '../js/src/cli.js')
    const nativePath = path.join(__dirname, '../js/src/native.js')
    const expected = require('../js/package.json').version
    for (const flag of ['-v', '--version']) {
      // Engine and npm versions can differ for a binding-only patch release.
      const script = `
        require.cache[${JSON.stringify(nativePath)}] = {
          exports: ${native ? "{ version: () => '0.0.0-engine' }" : 'null'}
        }
        process.argv = [process.execPath, ${JSON.stringify(cli)}, ${JSON.stringify(flag)}]
        require(${JSON.stringify(cli)})
      `
      const result = spawnSync(process.execPath, ['-e', script], { encoding: 'utf8' })
      assert.equal(result.status, 0, result.stderr)
      assert.equal(result.stdout, `tranfi ${expected}\n`)
    }
  })
}
