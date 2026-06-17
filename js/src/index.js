/**
 * tranfi — Streaming ETL language + runtime.
 *
 * Usage:
 *   const { pipeline, codec, ops, expr } = require('tranfi')
 *
 *   const p = pipeline([
 *     codec.csv({ delimiter: ',' }),
 *     ops.filter(expr("col('age') > 25")),
 *     ops.select(['name', 'age']),
 *     codec.csvEncode(),
 *   ])
 *
 *   const result = await p.run({ inputFile: './data.csv' })
 *   console.log(result.outputText)
 *
 *   // Or use DSL string:
 *   const p2 = pipeline('csv | filter "col(\'age\') > 25" | csv')
 *   const result2 = await p2.run({ inputFile: './data.csv' })
 */

const { Pipeline, PipelineResult, compileDsl, compileToSql, loadRecipe, saveRecipe, recipes } = require('./pipeline.js')

function pipeline(steps, { engine } = {}) {
  return new Pipeline(steps, { engine })
}

function param(name, defaultValue) {
  const result = { param: name }
  if (defaultValue !== undefined) result.default = defaultValue
  return result
}

function expr(text) {
  return text
}

const codec = {
  csv({ delimiter = ',', header = true, batchSize = 1024, repair = false, mode, strict = false, maxErrorBytes = 4096, maxRecordBytes = 64 * 1024 * 1024, maxColumns = 8192, audit = false, auditLimit, auditIncludeRow, auditColumns, auditRedact, auditHashColumns, auditMaxBytes, auditMaxCellBytes, nulls, na, quotedNulls = true, comment, trimWs = true, skipEmptyRows = false, skipRepeatedHeader = false, skip = 0, nMax, maxRows } = {}) {
    const args = {}
    if (delimiter !== ',') args.delimiter = delimiter
    if (!header) args.header = false
    if (batchSize !== 1024) args.batch_size = batchSize
    if (repair) args.repair = true
    if (mode !== undefined) args.mode = mode
    if (strict) args.strict = true
    if (maxErrorBytes !== 4096) args.max_error_bytes = Number(maxErrorBytes)
    if (maxRecordBytes !== 64 * 1024 * 1024) args.max_record_bytes = Number(maxRecordBytes)
    if (maxColumns !== 8192) args.max_columns = Number(maxColumns)
    if (audit) args.audit = true
    if (auditLimit !== undefined) args.audit_limit = Number(auditLimit)
    if (auditIncludeRow !== undefined) args.audit_include_row = !!auditIncludeRow
    if (auditColumns !== undefined) args.audit_columns = auditColumns
    if (auditRedact !== undefined) args.audit_redact = auditRedact
    if (auditHashColumns !== undefined) args.audit_hash_columns = auditHashColumns
    if (auditMaxBytes !== undefined) args.audit_max_bytes = Number(auditMaxBytes)
    if (auditMaxCellBytes !== undefined) args.audit_max_cell_bytes = Number(auditMaxCellBytes)
    if (nulls !== undefined) args.nulls = nulls
    else if (na !== undefined) args.na = na
    if (quotedNulls !== true) args.quoted_nulls = Boolean(quotedNulls)
    if (comment) args.comment = String(comment)
    if (trimWs !== true) args.trim_ws = Boolean(trimWs)
    if (skipEmptyRows) args.skip_empty_rows = true
    if (skipRepeatedHeader) args.skip_repeated_header = true
    if (skip) args.skip = Number(skip)
    if (nMax !== undefined) args.n_max = Number(nMax)
    else if (maxRows !== undefined) args.max_rows = Number(maxRows)
    return { op: 'codec.csv.decode', args }
  },

  csvDecode(opts = {}) {
    return codec.csv(opts)
  },

  csvEncode({ delimiter = ',' } = {}) {
    const args = {}
    if (delimiter !== ',') args.delimiter = delimiter
    return { op: 'codec.csv.encode', args }
  },

  jsonl({ batchSize = 1024, onError = 'skip', maxErrorBytes = 4096, maxRecordBytes = 64 * 1024 * 1024 } = {}) {
    const args = {}
    if (batchSize !== 1024) args.batch_size = batchSize
    if (onError !== 'skip') args.on_error = onError
    if (maxErrorBytes !== 4096) args.max_error_bytes = Number(maxErrorBytes)
    if (maxRecordBytes !== 64 * 1024 * 1024) args.max_record_bytes = Number(maxRecordBytes)
    return { op: 'codec.jsonl.decode', args }
  },

  jsonlDecode(opts = {}) {
    return codec.jsonl(opts)
  },

  jsonlEncode() {
    return { op: 'codec.jsonl.encode', args: {} }
  },

  text({ batchSize = 1024, maxErrorBytes = 4096, maxRecordBytes = 64 * 1024 * 1024 } = {}) {
    const args = {}
    if (batchSize !== 1024) args.batch_size = batchSize
    if (maxErrorBytes !== 4096) args.max_error_bytes = Number(maxErrorBytes)
    if (maxRecordBytes !== 64 * 1024 * 1024) args.max_record_bytes = Number(maxRecordBytes)
    return { op: 'codec.text.decode', args }
  },

  textDecode(opts = {}) {
    return codec.text(opts)
  },

  textEncode() {
    return { op: 'codec.text.encode', args: {} }
  },

  tableEncode({ maxWidth = 40, maxRows = 0 } = {}) {
    const args = {}
    if (maxWidth !== 40) args.max_width = maxWidth
    if (maxRows !== 0) args.max_rows = maxRows
    return { op: 'codec.table.encode', args }
  }
}

const ops = {
  filter(expression, { audit = false, auditLimit, auditIncludeRow, auditColumns, auditRedact, auditHashColumns, auditMaxBytes, auditMaxCellBytes } = {}) {
    const args = { expr: expression }
    if (audit) args.audit = true
    if (auditLimit !== undefined) args.audit_limit = auditLimit
    if (auditIncludeRow !== undefined) args.audit_include_row = !!auditIncludeRow
    if (auditColumns !== undefined) args.audit_columns = auditColumns
    if (auditRedact !== undefined) args.audit_redact = auditRedact
    if (auditHashColumns !== undefined) args.audit_hash_columns = auditHashColumns
    if (auditMaxBytes !== undefined) args.audit_max_bytes = Number(auditMaxBytes)
    if (auditMaxCellBytes !== undefined) args.audit_max_cell_bytes = Number(auditMaxCellBytes)
    return { op: 'filter', args }
  },

  select(columns) {
    return { op: 'select', args: { columns } }
  },

  relocate(columns, { before, after } = {}) {
    const args = { columns }
    if (before !== undefined) args.before = before
    if (after !== undefined) args.after = after
    return { op: 'relocate', args }
  },

  rename(mapping) {
    return { op: 'rename', args: { mapping } }
  },

  head(n) {
    return { op: 'head', args: { n } }
  },

  skip(n) {
    return { op: 'skip', args: { n } }
  },

  derive(columns) {
    // columns: { name: expr, ... } or [{ name, expr }, ...]
    const cols = Array.isArray(columns)
      ? columns
      : Object.entries(columns).map(([name, e]) => ({ name, expr: e }))
    return { op: 'derive', args: { columns: cols } }
  },

  sourceName({ result = '_source', defaultValue = '' } = {}) {
    const args = {}
    if (result !== '_source') args.result = result
    if (defaultValue !== '') args.default = defaultValue
    return { op: 'source-name', args }
  },


  across(columns, { functions, fn, names, replace } = {}) {
    const args = { columns }
    if (functions !== undefined) args.functions = Array.isArray(functions) ? functions : [functions]
    else if (fn !== undefined) args.fn = fn
    else throw new Error('across requires functions or fn')
    if (names !== undefined) args.names = names
    if (replace !== undefined) args.replace = !!replace
    return { op: 'across', args }
  },

  stats(statsList) {
    const args = {}
    if (statsList) args.stats = statsList
    return { op: 'stats', args }
  },

  scan(statsList) {
    const args = {}
    if (statsList) args.stats = statsList
    return { op: 'scan', args }
  },

  unique(columns, { maxKeys, maxStateBytes, sorted = false, mode, approx = false, bloomBytes, bloomHashes } = {}) {
    const args = {}
    if (columns) args.columns = columns
    if (mode !== undefined) args.mode = mode
    if (approx) args.approx = true
    if (bloomBytes !== undefined) args.bloom_bytes = bloomBytes
    if (bloomHashes !== undefined) args.bloom_hashes = bloomHashes
    if (maxKeys !== undefined) args.max_keys = maxKeys
    if (maxStateBytes !== undefined) args.max_state_bytes = maxStateBytes
    if (sorted) args.sorted = true
    return { op: 'unique', args }
  },

  sort(columns) {
    // columns: ['age', '-name'] or [{ name, desc }, ...]
    const cols = columns.map(c => {
      if (typeof c === 'string') {
        const desc = c.startsWith('-')
        return { name: desc ? c.slice(1) : c, desc }
      }
      return c
    })
    return { op: 'sort', args: { columns: cols } }
  },

  tail(n) {
    return { op: 'tail', args: { n } }
  },

  validate(expression, { audit = false, auditLimit, maxFailures, maxFailureRate, warnFailureRate, name, message, rules, rulesFile, auditIncludeRow, auditColumns, auditRedact, auditHashColumns, auditMaxBytes, auditMaxCellBytes } = {}) {
    const args = {}
    const isBuffer = typeof Buffer !== 'undefined' && Buffer.isBuffer(expression)
    const firstArgIsRules = rules === undefined && expression && typeof expression === 'object' && !isBuffer
    if (rulesFile !== undefined) args.rules_file = rulesFile
    if (rules !== undefined) args.rules = rules
    else if (firstArgIsRules) args.rules = expression
    else if (expression !== undefined && expression !== null) args.expr = expression
    if (args.expr === undefined && args.rules === undefined && args.rules_file === undefined) {
      throw new Error('validate requires expression, rules, or rulesFile')
    }
    if (audit) args.audit = true
    if (auditLimit !== undefined) args.audit_limit = auditLimit
    if (auditIncludeRow !== undefined) args.audit_include_row = !!auditIncludeRow
    if (auditColumns !== undefined) args.audit_columns = auditColumns
    if (auditRedact !== undefined) args.audit_redact = auditRedact
    if (auditHashColumns !== undefined) args.audit_hash_columns = auditHashColumns
    if (auditMaxBytes !== undefined) args.audit_max_bytes = Number(auditMaxBytes)
    if (auditMaxCellBytes !== undefined) args.audit_max_cell_bytes = Number(auditMaxCellBytes)
    if (maxFailures !== undefined) args.max_failures = maxFailures
    if (maxFailureRate !== undefined) args.max_failure_rate = maxFailureRate
    if (warnFailureRate !== undefined) args.warn_failure_rate = warnFailureRate
    if (name !== undefined && name !== null) args.name = name
    if (message) args.message = message
    return { op: 'validate', args }
  },

  assert(expression, { action = 'fail', name = 'assert', message = '', result = '_assert', audit = false, auditLimit, aggregate, op, value, column, tolerance, rel, auditIncludeRow, auditColumns, auditRedact, auditHashColumns, auditMaxBytes, auditMaxCellBytes } = {}) {
    const args = { action }
    if (aggregate !== undefined && aggregate !== null) {
      if (expression !== undefined && expression !== null) throw new Error('assert accepts either expression or aggregate, not both')
      if (op === undefined || value === undefined) throw new Error('aggregate assert requires op and value')
      args.aggregate = aggregate
      args.op = op
      args.value = Number(value)
      if (column !== undefined && column !== null) args.column = column
      if (tolerance !== undefined) args.tolerance = Number(tolerance)
      if (rel !== undefined) args.rel = !!rel
    } else if (expression !== undefined && expression !== null) {
      if (tolerance !== undefined || rel !== undefined) throw new Error('assert tolerance and rel require aggregate')
      args.expr = expression
    } else {
      throw new Error('assert requires expression or aggregate')
    }
    if (name !== undefined && name !== null) args.name = name
    if (message) args.message = message
    if (result !== undefined && result !== null) args.result = result
    if (audit) args.audit = true
    if (auditLimit !== undefined) args.audit_limit = auditLimit
    if (auditIncludeRow !== undefined) args.audit_include_row = !!auditIncludeRow
    if (auditColumns !== undefined) args.audit_columns = auditColumns
    if (auditRedact !== undefined) args.audit_redact = auditRedact
    if (auditHashColumns !== undefined) args.audit_hash_columns = auditHashColumns
    if (auditMaxBytes !== undefined) args.audit_max_bytes = Number(auditMaxBytes)
    if (auditMaxCellBytes !== undefined) args.audit_max_cell_bytes = Number(auditMaxCellBytes)
    return { op: 'assert', args }
  },

  quarantine(expression, { name, message, auditIncludeRow, auditColumns, auditRedact, auditHashColumns, auditMaxBytes, auditMaxCellBytes } = {}) {
    const args = { expr: expression }
    if (name !== undefined && name !== null) args.name = name
    if (message) args.message = message
    if (auditIncludeRow !== undefined) args.audit_include_row = !!auditIncludeRow
    if (auditColumns !== undefined) args.audit_columns = auditColumns
    if (auditRedact !== undefined) args.audit_redact = auditRedact
    if (auditHashColumns !== undefined) args.audit_hash_columns = auditHashColumns
    if (auditMaxBytes !== undefined) args.audit_max_bytes = Number(auditMaxBytes)
    if (auditMaxCellBytes !== undefined) args.audit_max_cell_bytes = Number(auditMaxCellBytes)
    return { op: 'quarantine', args }
  },

  schema({ baseline, columns, required, nonNull, nullable, values, min, max, regex, mode = 'fail', name = 'schema', message = '', result = '_schema', audit = false, auditLimit, maxRegexPatternBytes, maxRegexCellBytes, allowExtraColumns, requireValuesSeen, auditIncludeRow, auditColumns, auditRedact, auditHashColumns, auditMaxBytes, auditMaxCellBytes } = {}) {
    const args = { mode, action: mode }
    if (baseline !== undefined) args.baseline = baseline
    if (columns !== undefined) args.columns = columns
    if (required !== undefined) args.required = required
    if (nonNull !== undefined) args.non_null = nonNull
    if (nullable !== undefined) args.nullable = nullable
    if (values !== undefined) args.values = values
    if (min !== undefined) args.min = min
    if (max !== undefined) args.max = max
    if (regex !== undefined) args.regex = regex
    if (name !== undefined && name !== null) args.name = name
    if (message) args.message = message
    if (result !== undefined && result !== null) args.result = result
    if (audit) args.audit = true
    if (auditLimit !== undefined) args.audit_limit = auditLimit
    if (auditIncludeRow !== undefined) args.audit_include_row = !!auditIncludeRow
    if (auditColumns !== undefined) args.audit_columns = auditColumns
    if (auditRedact !== undefined) args.audit_redact = auditRedact
    if (auditHashColumns !== undefined) args.audit_hash_columns = auditHashColumns
    if (auditMaxBytes !== undefined) args.audit_max_bytes = Number(auditMaxBytes)
    if (auditMaxCellBytes !== undefined) args.audit_max_cell_bytes = Number(auditMaxCellBytes)
    if (maxRegexPatternBytes !== undefined) args.max_regex_pattern_bytes = maxRegexPatternBytes
    if (maxRegexCellBytes !== undefined) args.max_regex_cell_bytes = maxRegexCellBytes
    if (allowExtraColumns !== undefined) args.allow_extra_columns = !!allowExtraColumns
    if (requireValuesSeen !== undefined) args.require_values_seen = !!requireValuesSeen
    return { op: 'schema', args }
  },

  schemaInfer({ rows } = {}) {
    const args = {}
    if (rows !== undefined) args.rows = rows
    return { op: 'schema-infer', args }
  },

  tee({ expr, channel = 'samples', columns, limit = 1000, every = 1, name = 'tee', includeRow = true, auditIncludeRow, auditColumns, auditRedact, auditHashColumns, auditMaxBytes, auditMaxCellBytes } = {}) {
    const args = { channel, limit, every, name, include_row: includeRow }
    if (expr !== undefined && expr !== null) args.expr = expr
    if (columns !== undefined && columns !== null) args.columns = columns
    if (auditIncludeRow !== undefined) args.audit_include_row = !!auditIncludeRow
    if (auditColumns !== undefined) args.audit_columns = auditColumns
    if (auditRedact !== undefined) args.audit_redact = auditRedact
    if (auditHashColumns !== undefined) args.audit_hash_columns = auditHashColumns
    if (auditMaxBytes !== undefined) args.audit_max_bytes = Number(auditMaxBytes)
    if (auditMaxCellBytes !== undefined) args.audit_max_cell_bytes = Number(auditMaxCellBytes)
    return { op: 'tee', args }
  },

  trim(columns) {
    const args = {}
    if (columns) args.columns = columns
    return { op: 'trim', args }
  },

  fillNull(mapping, { audit = false, auditLimit, auditIncludeRow, auditColumns, auditRedact, auditHashColumns, auditMaxBytes, auditMaxCellBytes } = {}) {
    const args = { mapping }
    if (audit) args.audit = true
    if (auditLimit !== undefined) args.audit_limit = auditLimit
    if (auditIncludeRow !== undefined) args.audit_include_row = !!auditIncludeRow
    if (auditColumns !== undefined) args.audit_columns = auditColumns
    if (auditRedact !== undefined) args.audit_redact = auditRedact
    if (auditHashColumns !== undefined) args.audit_hash_columns = auditHashColumns
    if (auditMaxBytes !== undefined) args.audit_max_bytes = Number(auditMaxBytes)
    if (auditMaxCellBytes !== undefined) args.audit_max_cell_bytes = Number(auditMaxCellBytes)
    return { op: 'fill-null', args }
  },

  cast(mapping, { audit = false, auditLimit, auditIncludeRow, auditColumns, auditRedact, auditHashColumns, auditMaxBytes, auditMaxCellBytes } = {}) {
    const args = { mapping }
    if (audit) args.audit = true
    if (auditLimit !== undefined) args.audit_limit = auditLimit
    if (auditIncludeRow !== undefined) args.audit_include_row = !!auditIncludeRow
    if (auditColumns !== undefined) args.audit_columns = auditColumns
    if (auditRedact !== undefined) args.audit_redact = auditRedact
    if (auditHashColumns !== undefined) args.audit_hash_columns = auditHashColumns
    if (auditMaxBytes !== undefined) args.audit_max_bytes = Number(auditMaxBytes)
    if (auditMaxCellBytes !== undefined) args.audit_max_cell_bytes = Number(auditMaxCellBytes)
    return { op: 'cast', args }
  },

  clip(column, { min, max } = {}) {
    const args = { column }
    if (min !== undefined) args.min = min
    if (max !== undefined) args.max = max
    return { op: 'clip', args }
  },

  replace(column, pattern, replacement, { regex = false, audit = false, auditLimit, auditIncludeRow, auditColumns, auditRedact, auditHashColumns, auditMaxBytes, auditMaxCellBytes } = {}) {
    const args = { column, pattern, replacement }
    if (regex) args.regex = true
    if (audit) args.audit = true
    if (auditLimit !== undefined) args.audit_limit = auditLimit
    if (auditIncludeRow !== undefined) args.audit_include_row = !!auditIncludeRow
    if (auditColumns !== undefined) args.audit_columns = auditColumns
    if (auditRedact !== undefined) args.audit_redact = auditRedact
    if (auditHashColumns !== undefined) args.audit_hash_columns = auditHashColumns
    if (auditMaxBytes !== undefined) args.audit_max_bytes = Number(auditMaxBytes)
    if (auditMaxCellBytes !== undefined) args.audit_max_cell_bytes = Number(auditMaxCellBytes)
    return { op: 'replace', args }
  },

  hash(columns) {
    const args = {}
    if (columns) args.columns = columns
    return { op: 'hash', args }
  },

  bin(column, boundaries, { missing, onTypeError } = {}) {
    const args = { column, boundaries }
    if (missing !== undefined) args.missing = missing
    if (onTypeError !== undefined) args.on_type_error = onTypeError
    return { op: 'bin', args }
  },

  ewma(column, alpha, { result, missing, onTypeError } = {}) {
    const args = { column, alpha }
    if (result !== undefined) args.result = result
    if (missing !== undefined) args.missing = missing
    if (onTypeError !== undefined) args.on_type_error = onTypeError
    return { op: 'ewma', args }
  },

  anomaly(column, { threshold = 3.0, result, missing, onTypeError } = {}) {
    const args = { column }
    if (threshold !== undefined) args.threshold = threshold
    if (result !== undefined) args.result = result
    if (missing !== undefined) args.missing = missing
    if (onTypeError !== undefined) args.on_type_error = onTypeError
    return { op: 'anomaly', args }
  },

  fillDown(columns) {
    const args = {}
    if (columns) args.columns = columns
    return { op: 'fill-down', args }
  },

  step(column, func, result) {
    const args = { column, func }
    if (result) args.result = result
    return { op: 'step', args }
  },

  window(column, size, func, resultOrOptions) {
    const args = { column, size, func }
    const options = resultOrOptions && typeof resultOrOptions === 'object' ? resultOrOptions : { result: resultOrOptions }
    const { result, missing, onTypeError } = options || {}
    if (result !== undefined) args.result = result
    if (missing !== undefined) args.missing = missing
    if (onTypeError !== undefined) args.on_type_error = onTypeError
    return { op: 'window', args }
  },

  rollingSum(column, size, { result, missing, onTypeError } = {}) {
    const args = { column, size }
    if (result !== undefined) args.result = result
    if (missing !== undefined) args.missing = missing
    if (onTypeError !== undefined) args.on_type_error = onTypeError
    return { op: 'rolling-sum', args }
  },

  rollingMean(column, size, { result, missing, onTypeError } = {}) {
    const args = { column, size }
    if (result !== undefined) args.result = result
    if (missing !== undefined) args.missing = missing
    if (onTypeError !== undefined) args.on_type_error = onTypeError
    return { op: 'rolling-mean', args }
  },

  rollingMin(column, size, { result, missing, onTypeError } = {}) {
    const args = { column, size }
    if (result !== undefined) args.result = result
    if (missing !== undefined) args.missing = missing
    if (onTypeError !== undefined) args.on_type_error = onTypeError
    return { op: 'rolling-min', args }
  },

  rollingMax(column, size, { result, missing, onTypeError } = {}) {
    const args = { column, size }
    if (result !== undefined) args.result = result
    if (missing !== undefined) args.missing = missing
    if (onTypeError !== undefined) args.on_type_error = onTypeError
    return { op: 'rolling-max', args }
  },

  rollingAny(column, size, { result, nulls = 'ignore' } = {}) {
    const args = { column, size }
    if (result) args.result = result
    if (nulls !== 'ignore') args.nulls = nulls
    return { op: 'rolling-any', args }
  },

  rollingAll(column, size, { result, nulls = 'ignore' } = {}) {
    const args = { column, size }
    if (result) args.result = result
    if (nulls !== 'ignore') args.nulls = nulls
    return { op: 'rolling-all', args }
  },

  explode(column, delimiter, options = {}) {
    if (delimiter && typeof delimiter === 'object') {
      options = delimiter
      delimiter = undefined
    }
    const args = { column }
    if (delimiter && delimiter !== ',') args.delimiter = delimiter
    const { maxTokensPerRow, maxOutputRowsPerInputRow, maxOutputRowsPerBatch, maxTokenBytes } = options || {}
    if (maxTokensPerRow !== undefined) args.max_tokens_per_row = maxTokensPerRow
    if (maxOutputRowsPerInputRow !== undefined) args.max_output_rows_per_input_row = maxOutputRowsPerInputRow
    if (maxOutputRowsPerBatch !== undefined) args.max_output_rows_per_batch = maxOutputRowsPerBatch
    if (maxTokenBytes !== undefined) args.max_token_bytes = maxTokenBytes
    return { op: 'explode', args }
  },

  split(column, names, delimiter) {
    const args = { column, names }
    if (delimiter && delimiter !== ' ') args.delimiter = delimiter
    return { op: 'split', args }
  },

  unpivot(columns, { maxOutputRowsPerInputRow, maxOutputRowsPerBatch } = {}) {
    const args = { columns }
    if (maxOutputRowsPerInputRow !== undefined) args.max_output_rows_per_input_row = maxOutputRowsPerInputRow
    if (maxOutputRowsPerBatch !== undefined) args.max_output_rows_per_batch = maxOutputRowsPerBatch
    return { op: 'unpivot', args }
  },

  pivot(nameColumn, valueColumn, { agg, categories, maxCategories, sorted = false } = {}) {
    const args = { name_column: nameColumn, value_column: valueColumn }
    if (agg !== undefined) args.agg = agg
    if (categories !== undefined) args.categories = categories
    if (maxCategories !== undefined) args.max_categories = maxCategories
    if (sorted) args.sorted = true
    return { op: 'pivot', args }
  },

  interpolate(column, { method = 'linear', missing, onTypeError } = {}) {
    const args = { column, method }
    if (missing !== undefined) args.missing = missing
    if (onTypeError !== undefined) args.on_type_error = onTypeError
    return { op: 'interpolate', args }
  },

  normalize(columns, { method = 'minmax', audit = false, auditLimit, missing, onTypeError, auditIncludeRow, auditColumns, auditRedact, auditHashColumns, auditMaxBytes, auditMaxCellBytes } = {}) {
    const cols = Array.isArray(columns)
      ? columns
      : String(columns).split(',').map(c => c.trim()).filter(Boolean)
    const args = { columns: cols, method }
    if (audit) args.audit = true
    if (auditLimit !== undefined) args.audit_limit = auditLimit
    if (auditIncludeRow !== undefined) args.audit_include_row = !!auditIncludeRow
    if (auditColumns !== undefined) args.audit_columns = auditColumns
    if (auditRedact !== undefined) args.audit_redact = auditRedact
    if (auditHashColumns !== undefined) args.audit_hash_columns = auditHashColumns
    if (auditMaxBytes !== undefined) args.audit_max_bytes = Number(auditMaxBytes)
    if (auditMaxCellBytes !== undefined) args.audit_max_cell_bytes = Number(auditMaxCellBytes)
    if (missing !== undefined) args.missing = missing
    if (onTypeError !== undefined) args.on_type_error = onTypeError
    return { op: 'normalize', args }
  },

  acf(column, { lags = 20, missing, onTypeError } = {}) {
    const args = { column, lags }
    if (missing !== undefined) args.missing = missing
    if (onTypeError !== undefined) args.on_type_error = onTypeError
    return { op: 'acf', args }
  },

  top(n, column, desc = true) {
    return { op: 'top', args: { n, column, desc } }
  },

  topK(n, column) {
    return { op: 'top-k', args: { n, column, desc: true } }
  },

  bottomK(n, column) {
    return { op: 'bottom-k', args: { n, column, desc: false } }
  },

  sliceHead(n) {
    return { op: 'slice-head', args: { n } }
  },

  sliceTail(n) {
    return { op: 'slice-tail', args: { n } }
  },

  sliceMin(column, n = 1, options = {}) {
    const withTies = options && Object.prototype.hasOwnProperty.call(options, 'withTies') ? options.withTies : false
    if (withTies) throw new Error('sliceMin withTies=true is not supported by bounded exact-N execution')
    return { op: 'slice-min', args: { n, column, desc: false, with_ties: false } }
  },

  sliceMax(column, n = 1, options = {}) {
    const withTies = options && Object.prototype.hasOwnProperty.call(options, 'withTies') ? options.withTies : false
    if (withTies) throw new Error('sliceMax withTies=true is not supported by bounded exact-N execution')
    return { op: 'slice-max', args: { n, column, desc: true, with_ties: false } }
  },

  sample(n, { seed = 0 } = {}) {
    const args = { n }
    if (seed !== 0) args.seed = seed
    return { op: 'sample', args }
  },

  groupAgg(groupBy, aggs, { maxGroups, maxStateBytes, sorted = false } = {}) {
    const args = { group_by: groupBy, aggs }
    if (maxGroups !== undefined) args.max_groups = maxGroups
    if (maxStateBytes !== undefined) args.max_state_bytes = maxStateBytes
    if (sorted) args.sorted = true
    return { op: 'group-agg', args }
  },

  frequency(columns, { maxValues, maxStateBytes, overflow, other, audit = false, auditLimit, auditIncludeRow, auditColumns, auditRedact, auditHashColumns, auditMaxBytes, auditMaxCellBytes } = {}) {
    const args = {}
    if (columns) args.columns = columns
    if (maxValues !== undefined) args.max_values = maxValues
    if (maxStateBytes !== undefined) args.max_state_bytes = maxStateBytes
    if (overflow !== undefined) args.overflow = overflow
    if (other !== undefined) args.other = other
    if (audit) args.audit = true
    if (auditLimit !== undefined) args.audit_limit = auditLimit
    if (auditIncludeRow !== undefined) args.audit_include_row = !!auditIncludeRow
    if (auditColumns !== undefined) args.audit_columns = auditColumns
    if (auditRedact !== undefined) args.audit_redact = auditRedact
    if (auditHashColumns !== undefined) args.audit_hash_columns = auditHashColumns
    if (auditMaxBytes !== undefined) args.audit_max_bytes = Number(auditMaxBytes)
    if (auditMaxCellBytes !== undefined) args.audit_max_cell_bytes = Number(auditMaxCellBytes)
    return { op: 'frequency', args }
  },

  onehot(column, { drop = false, categories, maxCategories, unknown, maxStateBytes } = {}) {
    const args = { column }
    if (drop) args.drop = true
    if (categories !== undefined) args.categories = categories
    if (maxCategories !== undefined) args.max_categories = maxCategories
    if (maxStateBytes !== undefined) args.max_state_bytes = maxStateBytes
    if (unknown !== undefined) args.unknown = unknown
    return { op: 'onehot', args }
  },

  labelEncode(column, { result, categories, maxCategories, unknown, maxStateBytes } = {}) {
    const args = { column }
    if (result) args.result = result
    if (categories !== undefined) args.categories = categories
    if (maxCategories !== undefined) args.max_categories = maxCategories
    if (maxStateBytes !== undefined) args.max_state_bytes = maxStateBytes
    if (unknown !== undefined) args.unknown = unknown
    return { op: 'label-encode', args }
  },

  datetime(column, extractOrOptions, options = {}) {
    const args = { column }
    let opts = options || {}
    if (Array.isArray(extractOrOptions) || typeof extractOrOptions === 'string') {
      args.extract = extractOrOptions
    } else if (extractOrOptions && typeof extractOrOptions === 'object') {
      opts = extractOrOptions
    } else if (extractOrOptions !== undefined && extractOrOptions !== null) {
      args.extract = extractOrOptions
    }
    if (opts.extract !== undefined) args.extract = opts.extract
    if (opts.missing !== undefined) args.missing = opts.missing
    if (opts.onTypeError !== undefined) args.on_type_error = opts.onTypeError
    if (opts.on_type_error !== undefined) args.on_type_error = opts.on_type_error
    return { op: 'datetime', args }
  },

  reorder(columns) {
    return { op: 'reorder', args: { columns } }
  },

  dedup(columns, { maxKeys, maxStateBytes, sorted = false, mode, approx = false, bloomBytes, bloomHashes } = {}) {
    const args = {}
    if (columns) args.columns = columns
    if (mode !== undefined) args.mode = mode
    if (approx) args.approx = true
    if (bloomBytes !== undefined) args.bloom_bytes = bloomBytes
    if (bloomHashes !== undefined) args.bloom_hashes = bloomHashes
    if (maxKeys !== undefined) args.max_keys = maxKeys
    if (maxStateBytes !== undefined) args.max_state_bytes = maxStateBytes
    if (sorted) args.sorted = true
    return { op: 'dedup', args }
  },

  grep(pattern, { invert = false, column = '_line', regex = false } = {}) {
    const args = { pattern }
    if (invert) args.invert = true
    if (column !== '_line') args.column = column
    if (regex) args.regex = true
    return { op: 'grep', args }
  },

  flatten() {
    return { op: 'flatten', args: {} }
  },

  jsonExtract(path, result, { column = '_line', type = 'string' } = {}) {
    return { op: 'json-extract', args: { column, path, result, type } }
  },

  jsonFilter(path, { op = 'exists', value, column = '_line', type = 'auto' } = {}) {
    const args = { column, path, op, type }
    if (value !== undefined) args.value = value
    return { op: 'json-filter', args }
  },

  jsonSchema(schema, { column = '_line', mode = 'annotate', result = '_valid', audit = false, auditLimit, auditIncludeRow, auditColumns, auditRedact, auditHashColumns, auditMaxBytes, auditMaxCellBytes } = {}) {
    const args = { column, schema, mode, result }
    if (audit) args.audit = true
    if (auditLimit !== undefined) args.audit_limit = auditLimit
    if (auditIncludeRow !== undefined) args.audit_include_row = !!auditIncludeRow
    if (auditColumns !== undefined) args.audit_columns = auditColumns
    if (auditRedact !== undefined) args.audit_redact = auditRedact
    if (auditHashColumns !== undefined) args.audit_hash_columns = auditHashColumns
    if (auditMaxBytes !== undefined) args.audit_max_bytes = Number(auditMaxBytes)
    if (auditMaxCellBytes !== undefined) args.audit_max_cell_bytes = Number(auditMaxCellBytes)
    return { op: 'json-schema', args }
  },

  jsonFlatten(fields, { column = '_line' } = {}) {
    return { op: 'json-flatten', args: { column, fields } }
  },

  join(file, on, { how = 'inner', maxLookupRows, maxLookupKeys, maxLookupBytes, maxStateBytes, maxMatchesPerRow, maxOutputRows, sorted = false } = {}) {
    const args = { file, on }
    if (how !== 'inner') args.how = how
    if (maxLookupRows !== undefined) args.max_lookup_rows = maxLookupRows
    if (maxLookupKeys !== undefined) args.max_lookup_keys = maxLookupKeys
    if (maxLookupBytes !== undefined) args.max_lookup_bytes = maxLookupBytes
    if (maxStateBytes !== undefined) args.max_state_bytes = maxStateBytes
    if (maxMatchesPerRow !== undefined) args.max_matches_per_row = maxMatchesPerRow
    if (maxOutputRows !== undefined) args.max_output_rows = maxOutputRows
    if (sorted) args.sorted = true
    return { op: 'join', args }
  },

  semiJoin(file, on, { maxLookupRows, maxLookupKeys, maxLookupBytes, maxStateBytes, maxOutputRows, sorted = false } = {}) {
    const args = { file, on }
    if (maxLookupRows !== undefined) args.max_lookup_rows = maxLookupRows
    if (maxLookupKeys !== undefined) args.max_lookup_keys = maxLookupKeys
    if (maxLookupBytes !== undefined) args.max_lookup_bytes = maxLookupBytes
    if (maxStateBytes !== undefined) args.max_state_bytes = maxStateBytes
    if (maxOutputRows !== undefined) args.max_output_rows = maxOutputRows
    if (sorted) args.sorted = true
    return { op: 'semi-join', args }
  },

  antiJoin(file, on, { maxLookupRows, maxLookupKeys, maxLookupBytes, maxStateBytes, maxOutputRows, sorted = false } = {}) {
    const args = { file, on }
    if (maxLookupRows !== undefined) args.max_lookup_rows = maxLookupRows
    if (maxLookupKeys !== undefined) args.max_lookup_keys = maxLookupKeys
    if (maxLookupBytes !== undefined) args.max_lookup_bytes = maxLookupBytes
    if (maxStateBytes !== undefined) args.max_state_bytes = maxStateBytes
    if (maxOutputRows !== undefined) args.max_output_rows = maxOutputRows
    if (sorted) args.sorted = true
    return { op: 'anti-join', args }
  },

  intersect(file, { columns, maxLookupRows, maxLookupKeys, maxLookupBytes, maxOutputKeys, maxStateBytes, sorted = false } = {}) {
    const args = { file }
    if (columns !== undefined) args.columns = columns
    if (maxLookupRows !== undefined) args.max_lookup_rows = maxLookupRows
    if (maxLookupKeys !== undefined) args.max_lookup_keys = maxLookupKeys
    if (maxLookupBytes !== undefined) args.max_lookup_bytes = maxLookupBytes
    if (maxOutputKeys !== undefined) args.max_output_keys = maxOutputKeys
    if (maxStateBytes !== undefined) args.max_state_bytes = maxStateBytes
    if (sorted) args.sorted = true
    return { op: 'intersect', args }
  },

  setdiff(file, { columns, maxLookupRows, maxLookupKeys, maxLookupBytes, maxOutputKeys, maxStateBytes, sorted = false } = {}) {
    const args = { file }
    if (columns !== undefined) args.columns = columns
    if (maxLookupRows !== undefined) args.max_lookup_rows = maxLookupRows
    if (maxLookupKeys !== undefined) args.max_lookup_keys = maxLookupKeys
    if (maxLookupBytes !== undefined) args.max_lookup_bytes = maxLookupBytes
    if (maxOutputKeys !== undefined) args.max_output_keys = maxOutputKeys
    if (maxStateBytes !== undefined) args.max_state_bytes = maxStateBytes
    if (sorted) args.sorted = true
    return { op: 'setdiff', args }
  },

  intersectAll(file, { columns, maxLookupRows, maxLookupKeys, maxLookupBytes, maxStateBytes, sorted = false } = {}) {
    const args = { file }
    if (columns !== undefined) args.columns = columns
    if (maxLookupRows !== undefined) args.max_lookup_rows = maxLookupRows
    if (maxLookupKeys !== undefined) args.max_lookup_keys = maxLookupKeys
    if (maxLookupBytes !== undefined) args.max_lookup_bytes = maxLookupBytes
    if (maxStateBytes !== undefined) args.max_state_bytes = maxStateBytes
    if (sorted) args.sorted = true
    return { op: 'intersect-all', args }
  },

  setdiffAll(file, { columns, maxLookupRows, maxLookupKeys, maxLookupBytes, maxStateBytes, sorted = false } = {}) {
    const args = { file }
    if (columns !== undefined) args.columns = columns
    if (maxLookupRows !== undefined) args.max_lookup_rows = maxLookupRows
    if (maxLookupKeys !== undefined) args.max_lookup_keys = maxLookupKeys
    if (maxLookupBytes !== undefined) args.max_lookup_bytes = maxLookupBytes
    if (maxStateBytes !== undefined) args.max_state_bytes = maxStateBytes
    if (sorted) args.sorted = true
    return { op: 'setdiff-all', args }
  },

  union(file, { columns, maxLookupRows, maxLookupBytes, maxOutputKeys, maxStateBytes, sorted = false } = {}) {
    const args = { file }
    if (columns !== undefined) args.columns = columns
    if (maxLookupRows !== undefined) args.max_lookup_rows = maxLookupRows
    if (maxLookupBytes !== undefined) args.max_lookup_bytes = maxLookupBytes
    if (maxOutputKeys !== undefined) args.max_output_keys = maxOutputKeys
    if (maxStateBytes !== undefined) args.max_state_bytes = maxStateBytes
    if (sorted) args.sorted = true
    return { op: 'union', args }
  },

  unionAll(file, { maxLookupRows, maxLookupBytes } = {}) {
    const args = { file }
    if (maxLookupRows !== undefined) args.max_lookup_rows = maxLookupRows
    if (maxLookupBytes !== undefined) args.max_lookup_bytes = maxLookupBytes
    return { op: 'union-all', args }
  },

  stack(file, { tag, tagValue } = {}) {
    const args = { file }
    if (tag) args.tag = tag
    if (tagValue) args.tag_value = tagValue
    return { op: 'stack', args }
  },

  lead(column, { offset = 1, result } = {}) {
    const args = { column }
    if (offset !== 1) args.offset = offset
    if (result) args.result = result
    return { op: 'lead', args }
  },

  lag(column, { offset = 1, result } = {}) {
    const args = { column }
    if (offset !== 1) args.offset = offset
    if (result) args.result = result
    return { op: 'lag', args }
  },

  shift(column, { offset = 1, result, type = 'lag' } = {}) {
    const args = { column }
    if (offset !== 1) args.offset = offset
    if (result) args.result = result
    if (type !== 'lag') args.type = type
    return { op: 'shift', args }
  },

  rowid(columns, { result, sorted = false, maxKeys, maxStateBytes } = {}) {
    const args = {}
    if (columns !== undefined && columns !== null) args.columns = Array.isArray(columns) ? columns : [columns]
    if (result) args.result = result
    if (sorted) args.sorted = true
    if (maxKeys !== undefined) args.max_keys = maxKeys
    if (maxStateBytes !== undefined) args.max_state_bytes = maxStateBytes
    return { op: 'rowid', args }
  },

  rleid(columns, { result } = {}) {
    const args = { columns: Array.isArray(columns) ? columns : [columns] }
    if (result) args.result = result
    return { op: 'rleid', args }
  },

  dateTrunc(column, trunc, { result, missing, onTypeError, on_type_error } = {}) {
    const args = { column, trunc }
    if (result) args.result = result
    if (missing !== undefined) args.missing = missing
    if (onTypeError !== undefined) args.on_type_error = onTypeError
    if (on_type_error !== undefined) args.on_type_error = on_type_error
    return { op: 'date-trunc', args }
  }
}

const io = {
  read: {
    file(path) {
      return { op: 'io.read.file', args: { path } }
    }
  },
  write: {
    stdout() {
      return { op: 'io.write.stdout', args: {} }
    }
  }
}

module.exports = {
  Pipeline,
  PipelineResult,
  compileDsl,
  compileToSql,
  loadRecipe,
  saveRecipe,
  recipes,
  pipeline,
  param,
  expr,
  codec,
  ops,
  io
}
