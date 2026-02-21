/**
 * duckdb.js — DuckDB engine for Node.js.
 *
 * Executes tranfi pipelines via SQL transpilation + duckdb.
 * Install: npm install duckdb
 */

import { writeFile, unlink } from 'fs/promises'
import { tmpdir } from 'os'
import { join } from 'path'
import { randomBytes } from 'crypto'
import { PipelineResult } from '../pipeline.js'

let _compileToSql = null

async function getCompileToSql() {
  if (_compileToSql) return _compileToSql
  const { compileToSql } = await import('../pipeline.js')
  _compileToSql = compileToSql
  return _compileToSql
}

class DuckDBEngine {
  async run(dsl, { input, inputFile } = {}) {
    let duckdb
    try {
      duckdb = (await import('duckdb')).default || await import('duckdb')
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
      if (inputFile) {
        sql = sql.replaceAll('input_data', `read_csv('${inputFile}')`)
      } else if (input) {
        const buf = typeof input === 'string' ? Buffer.from(input, 'utf-8') : input
        tmpPath = join(tmpdir(), `tranfi_${randomBytes(8).toString('hex')}.csv`)
        await writeFile(tmpPath, buf)
        sql = sql.replaceAll('input_data', `read_csv('${tmpPath}')`)
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
      const s = String(v)
      if (s.includes(',') || s.includes('"') || s.includes('\n')) {
        return '"' + s.replace(/"/g, '""') + '"'
      }
      return s
    })
    lines.push(vals.join(','))
  }
  return lines.join('\n') + '\n'
}

let _engine = null

export function getEngine(name) {
  if (name === 'duckdb') {
    if (!_engine) _engine = new DuckDBEngine()
    return _engine
  }
  throw new Error(`Unknown engine: '${name}'. Available: 'native', 'duckdb'`)
}
