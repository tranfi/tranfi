import { createBlock } from './block-factory.js'

/**
 * Parse a DSL string back into blocks.
 * Reverse of compileToDsl — splits on ' | ', matches each segment
 * to a block type, and extracts args.
 */

const parsers = {
  csv: parseCSV,
  jsonl: () => ({}),
  filter: parseQuoted('expr'),
  select: parseRest('columns'),
  rename: parseRest('mapping'),
  reorder: parseRest('columns'),
  validate: parseQuoted('expr'),
  head: parseNum('n'),
  skip: parseNum('n'),
  tail: parseNum('n'),
  top: parseTop,
  dedup: parseRest('columns'),
  sample: parseNum('n'),
  derive: parseRest('columns'),
  cast: parseRest('mapping'),
  'fill-null': parseRest('mapping'),
  'fill-down': parseRest('columns'),
  replace: parseReplace,
  trim: parseRest('columns'),
  clip: parseClip,
  bin: parseBin,
  hash: parseHash,
  datetime: parseDatetime,
  sort: parseRest('columns'),
  unique: parseRest('columns'),
  explode: parseExplode,
  split: parseSplit,
  flatten: parseFlatten,
  unpivot: parseUnpivot,
  stats: parseRest('stats'),
  'group-agg': parseGroupAgg,
  frequency: parseRest('column'),
  window: parseWindow,
  step: parseStep,
  join: parseJoin,
  onehot: parseOnehot,
  'label-encode': parseLabelEncode,
  ewma: parseEwma,
  diff: parseDiff,
  anomaly: parseAnomaly,
  'split-data': parseSplitData,
  interpolate: parseInterpolate,
  normalize: parseNormalize,
  acf: parseAcf
}

function parseNum(field) {
  return (rest) => ({ [field]: parseInt(rest) || 10 })
}

function parseRest(field) {
  return (rest) => rest ? { [field]: rest } : {}
}

function parseQuoted(field) {
  return (rest) => {
    const m = rest.match(/^"(.*)"$/)
    return { [field]: m ? m[1] : rest }
  }
}

function parseCSV(rest) {
  const args = { delimiter: ',', header: true }
  if (rest.includes('--no-header')) args.header = false
  const dm = rest.match(/-d\s+"([^"]*)"/)
  if (dm) args.delimiter = dm[1]
  return args
}

function parseTop(rest) {
  const parts = rest.split(/\s+/)
  const args = { n: parseInt(parts[0]) || 10, order: 'desc' }
  if (parts[1]) args.column = parts[1]
  if (parts[2] === 'asc') args.order = 'asc'
  return args
}

function parseReplace(rest) {
  const m = rest.match(/^(\S+)\s+"([^"]*)"\s+"([^"]*)"/)
  if (!m) return { column: rest }
  return { column: m[1], pattern: m[2], replacement: m[3] }
}

function parseClip(rest) {
  const parts = rest.split(/\s+/)
  return { column: parts[0] || '', min: Number(parts[1]) || 0, max: Number(parts[2]) || 100 }
}

function parseBin(rest) {
  const parts = rest.split(/\s+/, 2)
  return { column: parts[0] || '', boundaries: parts[1] || '' }
}

function parseHash(rest) {
  const parts = rest.split(/\s+/)
  return { columns: parts[0] || '', algorithm: parts[1] || 'sha256' }
}

function parseDatetime(rest) {
  const args = { column: '', from: '', to: '' }
  const parts = rest.match(/(\S+)(?:\s+"([^"]*)")?(?:\s+"([^"]*)")?/)
  if (parts) {
    args.column = parts[1]
    if (parts[2]) args.from = parts[2]
    if (parts[3]) args.to = parts[3]
  }
  return args
}

function parseExplode(rest) {
  const m = rest.match(/^(\S+)(?:\s+"([^"]*)")?/)
  return { column: m ? m[1] : '', delimiter: m && m[2] ? m[2] : ',' }
}

function parseSplit(rest) {
  const m = rest.match(/^(\S+)\s+"([^"]*)"(?:\s+(.+))?/)
  if (!m) return { column: rest, delimiter: ' ', names: '' }
  return { column: m[1], delimiter: m[2], names: m[3] || '' }
}

function parseFlatten(rest) {
  const m = rest.match(/^"([^"]*)"/)
  return { separator: m ? m[1] : '.' }
}

function parseUnpivot(rest) {
  const parts = rest.split(/\s+/, 2)
  return { id_columns: parts[0] || '', value_columns: parts[1] || '' }
}

function parseGroupAgg(rest) {
  const parts = rest.split(/\s+/, 2)
  return { keys: parts[0] || '', aggs: parts[1] || '' }
}

function parseWindow(rest) {
  const parts = rest.split(/\s+/, 2)
  return { size: parseInt(parts[0]) || 100, aggs: parts[1] || '' }
}

function parseStep(rest) {
  const parts = rest.split(/\s+/, 2)
  return { column: parts[0] || '', op: parts[1] || 'running-sum' }
}

function parseJoin(rest) {
  const parts = rest.split(/\s+/)
  return { file: parts[0] || '', key: parts[1] || '', type: parts[2] || 'left' }
}

function parseOnehot(rest) {
  const parts = rest.split(/\s+/)
  return { column: parts[0] || '', drop: parts.includes('--drop') }
}

function parseLabelEncode(rest) {
  const parts = rest.split(/\s+/)
  return { column: parts[0] || '', result: parts[1] || '' }
}

function parseEwma(rest) {
  const parts = rest.split(/\s+/)
  return { column: parts[0] || '', alpha: Number(parts[1]) || 0.3, result: parts[2] || '' }
}

function parseDiff(rest) {
  const parts = rest.split(/\s+/)
  const args = { column: parts[0] || '', order: 1, result: '' }
  if (parts[1] && /^\d+$/.test(parts[1])) {
    args.order = parseInt(parts[1])
    if (parts[2]) args.result = parts[2]
  } else if (parts[1]) {
    args.result = parts[1]
  }
  return args
}

function parseAnomaly(rest) {
  const parts = rest.split(/\s+/)
  const args = { column: parts[0] || '', threshold: 3, result: '' }
  if (parts[1] && !isNaN(Number(parts[1]))) {
    args.threshold = Number(parts[1])
    if (parts[2]) args.result = parts[2]
  } else if (parts[1]) {
    args.result = parts[1]
  }
  return args
}

function parseSplitData(rest) {
  const parts = rest.split(/\s+/)
  const args = { ratio: 0.8, seed: 42 }
  let i = 0
  if (parts[i] && !isNaN(Number(parts[i]))) { args.ratio = Number(parts[i]); i++ }
  while (i < parts.length) {
    if (parts[i] === '--seed' && parts[i + 1]) { args.seed = parseInt(parts[i + 1]); i += 2 }
    else i++
  }
  return args
}

function parseInterpolate(rest) {
  const parts = rest.split(/\s+/)
  return { column: parts[0] || '', method: parts[1] || 'linear' }
}

function parseNormalize(rest) {
  const parts = rest.split(/\s+/)
  return { columns: parts[0] || '', method: parts[1] || 'minmax' }
}

function parseAcf(rest) {
  const parts = rest.split(/\s+/)
  return { column: parts[0] || '', lags: parseInt(parts[1]) || 20 }
}

export function parseDsl(dsl) {
  if (!dsl || !dsl.trim()) return null
  const segments = dsl.split(/\s*\|\s*/)
  const blocks = []

  for (let i = 0; i < segments.length; i++) {
    const seg = segments[i].trim()
    if (!seg) continue

    // Match command name (possibly hyphenated)
    const m = seg.match(/^([a-z][-a-z]*)(?:\s+(.*))?$/)
    if (!m) continue

    const cmd = m[1]
    const rest = (m[2] || '').trim()

    // csv/jsonl: first occurrence is decode, last is encode
    if (cmd === 'csv' || cmd === 'jsonl') {
      const isLast = i === segments.length - 1
      const isFirst = blocks.length === 0
      const typeCode = isFirst && !isLast
        ? `${cmd}-decode`
        : isLast && blocks.length > 0
          ? `${cmd}-encode`
          : i === 0
            ? `${cmd}-decode`
            : `${cmd}-encode`

      const block = createBlock(typeCode)
      if (parsers[cmd]) Object.assign(block.args, parsers[cmd](rest))
      blocks.push(block)
      continue
    }

    // Regular transform
    if (parsers[cmd]) {
      const typeCode = cmd
      const block = createBlock(typeCode)
      Object.assign(block.args, parsers[cmd](rest))
      blocks.push(block)
    }
  }

  return blocks.length > 0 ? blocks : null
}
