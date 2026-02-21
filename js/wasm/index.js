/**
 * tranfi/wasm — Browser-friendly WASM wrapper.
 *
 * Usage:
 *   import createTranfi from 'tranfi/wasm'
 *   const tf = await createTranfi()
 *
 *   // Compile DSL to SQL
 *   const sql = tf.compileToSql('csv | filter "age > 25" | csv')
 *
 *   // Run with DuckDB-WASM
 *   import * as duckdb from '@duckdb/duckdb-wasm'
 *   const db = new duckdb.AsyncDuckDB(...)
 *   const result = await tf.runDuckDB(db, 'csv | filter "age > 25" | csv', csvBlob)
 */

async function createTranfi() {
  // Dynamic import to handle CJS/ESM interop
  var mod = await import('./tranfi_core.js')
  var createModule = mod.default || mod
  var wasm = await createModule()

  function allocString(str) {
    var len = wasm.lengthBytesUTF8(str)
    var ptr = wasm._malloc(len + 1)
    wasm.stringToUTF8(str, ptr, len + 1)
    return { ptr, len }
  }

  return {
    /** Raw WASM module (for advanced use) */
    _wasm: wasm,

    /** Library version */
    version() {
      return wasm.ccall('wasm_version', 'string', [], [])
    },

    /** Compile DSL string to plan JSON */
    compileDsl(dsl) {
      var s = allocString(dsl)
      var resultPtr = wasm.ccall('wasm_compile_dsl', 'number', ['number', 'number'], [s.ptr, s.len])
      wasm._free(s.ptr)
      if (!resultPtr) {
        var err = wasm.ccall('wasm_pipeline_error', 'string', ['number'], [-1])
        throw new Error('DSL parse failed: ' + (err || 'unknown error'))
      }
      var json = wasm.UTF8ToString(resultPtr)
      wasm._free(resultPtr)
      return json
    },

    /** Compile DSL string to SQL query */
    compileToSql(dsl) {
      var s = allocString(dsl)
      var resultPtr = wasm.ccall('wasm_compile_to_sql', 'number', ['number', 'number'], [s.ptr, s.len])
      wasm._free(s.ptr)
      if (!resultPtr) {
        var err = wasm.ccall('wasm_pipeline_error', 'string', ['number'], [-1])
        throw new Error('SQL compile failed: ' + (err || 'unknown error'))
      }
      var sql = wasm.UTF8ToString(resultPtr)
      wasm._free(resultPtr)
      return sql
    },

    /** Create a native pipeline (push/pull streaming) */
    createPipeline(planJson) {
      var s = allocString(planJson)
      var handle = wasm.ccall('wasm_pipeline_create', 'number', ['number', 'number'], [s.ptr, s.len])
      wasm._free(s.ptr)
      if (handle < 0) {
        var err = wasm.ccall('wasm_pipeline_error', 'string', ['number'], [handle])
        throw new Error('Failed to create pipeline: ' + (err || 'unknown error'))
      }
      return handle
    },

    /** Push data into pipeline */
    push(handle, data) {
      var buf = data instanceof Uint8Array ? data : new TextEncoder().encode(data)
      var ptr = wasm._malloc(buf.length)
      wasm.HEAPU8.set(buf, ptr)
      var rc = wasm.ccall('wasm_pipeline_push', 'number', ['number', 'number', 'number'], [handle, ptr, buf.length])
      wasm._free(ptr)
      if (rc !== 0) {
        var err = wasm.ccall('wasm_pipeline_error', 'string', ['number'], [handle])
        throw new Error('Push failed: ' + (err || 'unknown error'))
      }
    },

    /** Signal end of input */
    finish(handle) {
      var rc = wasm.ccall('wasm_pipeline_finish', 'number', ['number'], [handle])
      if (rc !== 0) {
        var err = wasm.ccall('wasm_pipeline_error', 'string', ['number'], [handle])
        throw new Error('Finish failed: ' + (err || 'unknown error'))
      }
    },

    /** Pull output from pipeline */
    pull(handle, channel) {
      var bufSize = 65536
      var ptr = wasm._malloc(bufSize)
      var chunks = []
      for (;;) {
        var n = wasm.ccall('wasm_pipeline_pull', 'number',
          ['number', 'number', 'number', 'number'], [handle, channel, ptr, bufSize])
        if (n === 0) break
        chunks.push(new Uint8Array(wasm.HEAPU8.buffer, ptr, n).slice())
      }
      wasm._free(ptr)
      var total = chunks.reduce(function(s, c) { return s + c.length }, 0)
      var result = new Uint8Array(total)
      var offset = 0
      for (var i = 0; i < chunks.length; i++) {
        result.set(chunks[i], offset)
        offset += chunks[i].length
      }
      return result
    },

    /** Free pipeline */
    free(handle) {
      wasm.ccall('wasm_pipeline_free', null, ['number'], [handle])
    },

    /** Run pipeline with native streaming engine */
    run(dsl, data) {
      var planJson = this.compileDsl(dsl)
      var handle = this.createPipeline(planJson)
      try {
        var buf = data instanceof Uint8Array ? data : new TextEncoder().encode(data)
        this.push(handle, buf)
        this.finish(handle)
        return {
          output: this.pull(handle, 0),
          errors: this.pull(handle, 1),
          stats: this.pull(handle, 2),
          samples: this.pull(handle, 3),
          get outputText() { return new TextDecoder().decode(this.output) },
          get statsText() { return new TextDecoder().decode(this.stats) }
        }
      } finally {
        this.free(handle)
      }
    },

    /**
     * Run pipeline via DuckDB-WASM.
     *
     * @param {object} db — An initialized @duckdb/duckdb-wasm AsyncDuckDB or DuckDB instance
     * @param {string} dsl — Tranfi DSL string
     * @param {string|Uint8Array|File} data — CSV input data
     * @returns {Promise<{output: Uint8Array, outputText: string, rows: Array}>}
     */
    async runDuckDB(db, dsl, data) {
      var sql = this.compileToSql(dsl)
      var conn = await db.connect()

      try {
        // Register input data as a table
        if (typeof data === 'string' || data instanceof Uint8Array) {
          var buf = data instanceof Uint8Array ? data : new TextEncoder().encode(data)
          await db.registerFileBuffer('input.csv', buf)
          sql = sql.replaceAll('input_data', "read_csv('input.csv')")
        } else if (data && data.name) {
          // File object
          await db.registerFileHandle(data.name, data)
          sql = sql.replaceAll('input_data', "read_csv('" + data.name + "')")
        } else {
          throw new Error('data must be a string, Uint8Array, or File')
        }

        var result = await conn.query(sql)
        var rows = result.toArray().map(function(row) { return row.toJSON() })

        // Convert to CSV
        var output = ''
        if (rows.length > 0) {
          var cols = Object.keys(rows[0])
          output = cols.join(',') + '\n'
          for (var i = 0; i < rows.length; i++) {
            var vals = cols.map(function(c) {
              var v = rows[i][c]
              if (v === null || v === undefined) return ''
              var s = String(v)
              if (s.indexOf(',') >= 0 || s.indexOf('"') >= 0 || s.indexOf('\n') >= 0) {
                return '"' + s.replace(/"/g, '""') + '"'
              }
              return s
            })
            output += vals.join(',') + '\n'
          }
        }

        var outputBytes = new TextEncoder().encode(output)
        return {
          output: outputBytes,
          rows: rows,
          get outputText() { return output }
        }
      } finally {
        await conn.close()
      }
    },

    /** Recipe helpers */
    recipeCount() {
      return wasm.ccall('wasm_recipe_count', 'number', [], [])
    },
    recipeName(index) {
      return wasm.ccall('wasm_recipe_name', 'string', ['number'], [index])
    },
    recipeDsl(index) {
      return wasm.ccall('wasm_recipe_dsl', 'string', ['number'], [index])
    },
    recipeDescription(index) {
      return wasm.ccall('wasm_recipe_description', 'string', ['number'], [index])
    },
    recipeFindDsl(name) {
      var s = allocString(name)
      var result = wasm.ccall('wasm_recipe_find_dsl', 'string', ['number'], [s.ptr])
      wasm._free(s.ptr)
      return result || null
    },
    recipes() {
      var n = this.recipeCount()
      var result = []
      for (var i = 0; i < n; i++) {
        result.push({
          name: this.recipeName(i),
          dsl: this.recipeDsl(i),
          description: this.recipeDescription(i)
        })
      }
      return result
    }
  }
}

// Support both CJS and ESM
if (typeof module !== 'undefined') module.exports = createTranfi
if (typeof exports !== 'undefined') exports.default = createTranfi
