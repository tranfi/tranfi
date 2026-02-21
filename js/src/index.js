/**
 * tranfi — Streaming ETL language + runtime.
 *
 * Usage:
 *   import { pipeline, codec, ops, expr } from 'tranfi'
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

import { Pipeline, PipelineResult, compileDsl, loadRecipe, saveRecipe, recipes } from './pipeline.js'

export { Pipeline, PipelineResult, compileDsl, loadRecipe, saveRecipe, recipes }

export function pipeline(steps) {
  return new Pipeline(steps)
}

export function param(name, defaultValue) {
  const result = { param: name }
  if (defaultValue !== undefined) result.default = defaultValue
  return result
}

export function expr(text) {
  return text
}

export const codec = {
  csv({ delimiter = ',', header = true, batchSize = 1024, repair = false } = {}) {
    const args = {}
    if (delimiter !== ',') args.delimiter = delimiter
    if (!header) args.header = false
    if (batchSize !== 1024) args.batch_size = batchSize
    if (repair) args.repair = true
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

  jsonl({ batchSize = 1024 } = {}) {
    const args = {}
    if (batchSize !== 1024) args.batch_size = batchSize
    return { op: 'codec.jsonl.decode', args }
  },

  jsonlDecode(opts = {}) {
    return codec.jsonl(opts)
  },

  jsonlEncode() {
    return { op: 'codec.jsonl.encode', args: {} }
  },

  text({ batchSize = 1024 } = {}) {
    const args = {}
    if (batchSize !== 1024) args.batch_size = batchSize
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

export const ops = {
  filter(expression) {
    return { op: 'filter', args: { expr: expression } }
  },

  select(columns) {
    return { op: 'select', args: { columns } }
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

  stats(statsList) {
    const args = {}
    if (statsList) args.stats = statsList
    return { op: 'stats', args }
  },

  unique(columns) {
    const args = {}
    if (columns) args.columns = columns
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

  validate(expression) {
    return { op: 'validate', args: { expr: expression } }
  },

  trim(columns) {
    const args = {}
    if (columns) args.columns = columns
    return { op: 'trim', args }
  },

  fillNull(mapping) {
    return { op: 'fill-null', args: { mapping } }
  },

  cast(mapping) {
    return { op: 'cast', args: { mapping } }
  },

  clip(column, { min, max } = {}) {
    const args = { column }
    if (min !== undefined) args.min = min
    if (max !== undefined) args.max = max
    return { op: 'clip', args }
  },

  replace(column, pattern, replacement, { regex = false } = {}) {
    const args = { column, pattern, replacement }
    if (regex) args.regex = true
    return { op: 'replace', args }
  },

  hash(columns) {
    const args = {}
    if (columns) args.columns = columns
    return { op: 'hash', args }
  },

  bin(column, boundaries) {
    return { op: 'bin', args: { column, boundaries } }
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

  window(column, size, func, result) {
    const args = { column, size, func }
    if (result) args.result = result
    return { op: 'window', args }
  },

  explode(column, delimiter) {
    const args = { column }
    if (delimiter && delimiter !== ',') args.delimiter = delimiter
    return { op: 'explode', args }
  },

  split(column, names, delimiter) {
    const args = { column, names }
    if (delimiter && delimiter !== ' ') args.delimiter = delimiter
    return { op: 'split', args }
  },

  unpivot(columns) {
    return { op: 'unpivot', args: { columns } }
  },

  top(n, column, desc = true) {
    return { op: 'top', args: { n, column, desc } }
  },

  sample(n) {
    return { op: 'sample', args: { n } }
  },

  groupAgg(groupBy, aggs) {
    return { op: 'group-agg', args: { group_by: groupBy, aggs } }
  },

  frequency(columns) {
    const args = {}
    if (columns) args.columns = columns
    return { op: 'frequency', args }
  },

  datetime(column, extract) {
    const args = { column }
    if (extract) args.extract = extract
    return { op: 'datetime', args }
  },

  reorder(columns) {
    return { op: 'reorder', args: { columns } }
  },

  dedup(columns) {
    const args = {}
    if (columns) args.columns = columns
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

  dateTrunc(column, trunc, { result } = {}) {
    const args = { column, trunc }
    if (result) args.result = result
    return { op: 'date-trunc', args }
  }
}

export const io = {
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
