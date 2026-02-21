const defaults = {
  'csv-decode': { delimiter: ',', header: true },
  'jsonl-decode': {},
  'filter': { expr: '' },
  'select': { columns: '' },
  'rename': { mapping: '' },
  'reorder': { columns: '' },
  'validate': { expr: '' },
  'head': { n: 10 },
  'skip': { n: 10 },
  'tail': { n: 10 },
  'top': { n: 10, column: '', order: 'desc' },
  'dedup': { columns: '' },
  'sample': { n: 100 },
  'derive': { columns: '' },
  'cast': { mapping: '' },
  'fill-null': { mapping: '' },
  'fill-down': { columns: '' },
  'replace': { column: '', pattern: '', replacement: '' },
  'trim': { columns: '' },
  'clip': { column: '', min: 0, max: 100 },
  'bin': { column: '', boundaries: '' },
  'hash': { columns: '', algorithm: 'sha256' },
  'datetime': { column: '', from: '', to: '' },
  'sort': { columns: '' },
  'unique': { columns: '' },
  'explode': { column: '', delimiter: ',' },
  'split': { column: '', delimiter: ' ', names: '' },
  'flatten': { separator: '.' },
  'unpivot': { id_columns: '', value_columns: '' },
  'stats': { stats: '' },
  'group-agg': { keys: '', aggs: '' },
  'frequency': { column: '' },
  'window': { size: 100, aggs: '' },
  'step': { column: '', op: 'running-sum' },
  'join': { file: '', key: '', type: 'left' },
  'csv-encode': { delimiter: ',', header: true },
  'jsonl-encode': {}
}

export function createBlock(typeCode) {
  return {
    id: 'b' + Math.round(Math.random() * 100000000),
    typeCode,
    minimized: false,
    args: { ...(defaults[typeCode] || {}) }
  }
}
