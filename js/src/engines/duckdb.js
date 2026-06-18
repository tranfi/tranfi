/**
 * duckdb.js — DuckDB engine for Node.js.
 *
 * Executes tranfi pipelines via SQL transpilation + duckdb.
 * Install: npm install duckdb
 */

const { writeFile, unlink } = require('fs/promises')
const { tmpdir } = require('os')
const { join } = require('path')
const { randomBytes } = require('crypto')
const { PipelineResult } = require('../pipeline.js')
const { memoryForEngine } = require('../memory_policy.js')

let _compileToSql = null

async function getCompileToSql() {
  if (_compileToSql) return _compileToSql
  const { compileToSql } = require('../pipeline.js')
  _compileToSql = compileToSql
  return _compileToSql
}

class DuckDBEngine {
  async run(dsl, { input, inputFile, memory, spillDir } = {}) {
    let duckdb
    try {
      duckdb = require('duckdb')
    } catch {
      throw new Error(
        "DuckDB engine requires the 'duckdb' package. " +
        'Install it with: npm install duckdb'
      )
    }

    const compileToSql = await getCompileToSql()
    let sql = await compileToSql(dsl)

    const db = new duckdb.Database(':memory:')
    const conn = db.connect()

    let tmpPath = null
    try {
      const memoryLimit = memoryForEngine(memory)
      if (memoryLimit) await runSql(conn, `SET memory_limit = '${sqlString(memoryLimit)}'`)
      if (spillDir) await runSql(conn, `SET temp_directory = '${sqlString(spillDir)}'`)

      if (inputFile) {
        sql = sql.replaceAll('input_data', `read_csv('${sqlString(inputFile)}')`)
      } else if (input) {
        const buf = typeof input === 'string' ? Buffer.from(input, 'utf-8') : input
        tmpPath = join(tmpdir(), `tranfi_${randomBytes(8).toString('hex')}.csv`)
        await writeFile(tmpPath, buf)
        sql = sql.replaceAll('input_data', `read_csv('${sqlString(tmpPath)}')`)
      } else {
        throw new Error('Either input or inputFile must be provided')
      }

      const rows = await queryAll(conn, sql)
      const output = rowsToCsv(rows)

      return new PipelineResult(
        Buffer.from(output, 'utf-8'),
        Buffer.alloc(0),
        Buffer.alloc(0),
        Buffer.alloc(0)
      )
    } finally {
      conn.close()
      db.close()
      if (tmpPath) {
        await unlink(tmpPath).catch(() => {})
      }
    }
  }
}

function sqlString(value) {
  return String(value).replace(/'/g, "''")
}

function runSql(conn, sql) {
  return new Promise((resolve, reject) => {
    conn.run(sql, err => {
      if (err) reject(err)
      else resolve()
    })
  })
}

function queryAll(conn, sql) {
  return new Promise((resolve, reject) => {
    conn.all(sql, (err, rows) => {
      if (err) reject(err)
      else resolve(rows)
    })
  })
}

function rowsToCsv(rows) {
  if (!rows || rows.length === 0) return ''
  const cols = Object.keys(rows[0])
  const lines = [cols.join(',')]
  for (const row of rows) {
    const vals = cols.map(c => {
      const v = row[c]
      if (v === null || v === undefined) return ''
      const s = formatDuckValue(v)
      if (s.includes(',') || s.includes('"') || s.includes('\n')) {
        return '"' + s.replace(/"/g, '""') + '"'
      }
      return s
    })
    lines.push(vals.join(','))
  }
  return lines.join('\n') + '\n'
}

function formatDuckValue(value) {
  if (typeof value !== 'number') return String(value)
  if (!Number.isFinite(value)) return String(value)
  if (Number.isInteger(value) && Math.abs(value) <= Number.MAX_SAFE_INTEGER) {
    return String(value)
  }
  let text = value.toPrecision(17)
  if (!/[eE]/.test(text) && text.includes('.')) {
    text = text.replace(/0+$/, '').replace(/\.$/, '')
  }
  return text
}

let _engine = null

function getEngine(name) {
  if (name === 'duckdb') {
    if (!_engine) _engine = new DuckDBEngine()
    return _engine
  }
  throw new Error(`Unknown engine: '${name}'. Available: 'native', 'duckdb'`)
}

module.exports = { getEngine }
