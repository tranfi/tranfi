/**
 * compile-schema.js — Converts DSL string to a JSEE schema object.
 *
 * Two modes:
 * - WASM mode (default): async-function model running in web worker
 * - Server mode: POST model sending { file, dsl } to /api/run
 */

export function compileToSchema (dsl, options = {}) {
  return {
    model: {
      type: 'async-function',
      url: '/wasm/tranfi-runner.js',
      name: 'tranfiRunner',
      worker: true,
      timeout: options.timeout || 120000,
      imports: ['/wasm/tranfi_core.js']
    },
    inputs: [
      { name: 'file', type: 'file', stream: true },
      { name: 'dsl', type: 'string', default: dsl || 'csv | csv', display: false }
    ],
    outputs: [
      { name: 'output', type: 'html' },
      { name: 'stats', type: 'html' }
    ],
    design: {
      framework: 'bulma'
    }
  }
}

export function compileToServerSchema (dsl, files, options = {}) {
  return {
    model: {
      type: 'post',
      url: `${options.api || '/api'}/run`
    },
    inputs: [
      { name: 'file', type: 'select', options: files, default: files[0] || '' },
      { name: 'dsl', type: 'string', default: dsl || 'csv | csv', display: false }
    ],
    outputs: [
      { name: 'output', type: 'html' },
      { name: 'stats', type: 'html' }
    ],
    design: {
      framework: 'bulma'
    }
  }
}
