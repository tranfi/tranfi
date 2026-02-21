const compilers = {
  'csv-decode'(a) {
    const p = ['csv']
    if (a.delimiter && a.delimiter !== ',') p.push(`-d "${a.delimiter}"`)
    if (a.header === false) p.push('--no-header')
    return p.join(' ')
  },
  'jsonl-decode': () => 'jsonl',
  'filter': (a) => a.expr ? `filter "${a.expr}"` : null,
  'select': (a) => a.columns ? `select ${a.columns}` : null,
  'rename': (a) => a.mapping ? `rename ${a.mapping}` : null,
  'reorder': (a) => a.columns ? `reorder ${a.columns}` : null,
  'validate': (a) => a.expr ? `validate "${a.expr}"` : null,
  'head': (a) => `head ${a.n || 10}`,
  'skip': (a) => `skip ${a.n || 10}`,
  'tail': (a) => `tail ${a.n || 10}`,
  'top'(a) {
    const p = [`top ${a.n || 10}`]
    if (a.column) p.push(a.column)
    if (a.order === 'asc') p.push('asc')
    return p.join(' ')
  },
  'dedup': (a) => a.columns ? `dedup ${a.columns}` : 'dedup',
  'sample': (a) => `sample ${a.n || 100}`,
  'derive': (a) => a.columns ? `derive ${a.columns}` : null,
  'cast': (a) => a.mapping ? `cast ${a.mapping}` : null,
  'fill-null': (a) => a.mapping ? `fill-null ${a.mapping}` : null,
  'fill-down': (a) => a.columns ? `fill-down ${a.columns}` : 'fill-down',
  'replace'(a) {
    if (!a.column || !a.pattern) return null
    const repl = a.replacement || ''
    return `replace ${a.column} "${a.pattern}" "${repl}"`
  },
  'trim': (a) => a.columns ? `trim ${a.columns}` : 'trim',
  'clip': (a) => a.column ? `clip ${a.column} ${a.min} ${a.max}` : null,
  'bin': (a) => a.column && a.boundaries ? `bin ${a.column} ${a.boundaries}` : null,
  'hash'(a) {
    if (!a.columns) return null
    const p = [`hash ${a.columns}`]
    if (a.algorithm && a.algorithm !== 'sha256') p.push(a.algorithm)
    return p.join(' ')
  },
  'datetime'(a) {
    if (!a.column) return null
    const p = [`datetime ${a.column}`]
    if (a.from) p.push(`"${a.from}"`)
    if (a.to) p.push(`"${a.to}"`)
    return p.join(' ')
  },
  'sort': (a) => a.columns ? `sort ${a.columns}` : null,
  'unique': (a) => a.columns ? `unique ${a.columns}` : null,
  'explode'(a) {
    if (!a.column) return null
    const p = [`explode ${a.column}`]
    if (a.delimiter && a.delimiter !== ',') p.push(`"${a.delimiter}"`)
    return p.join(' ')
  },
  'split'(a) {
    if (!a.column) return null
    const p = [`split ${a.column} "${a.delimiter || ' '}"`]
    if (a.names) p.push(a.names)
    return p.join(' ')
  },
  'flatten': (a) => a.separator !== '.' ? `flatten "${a.separator}"` : 'flatten',
  'unpivot'(a) {
    if (!a.id_columns) return null
    const p = [`unpivot ${a.id_columns}`]
    if (a.value_columns) p.push(a.value_columns)
    return p.join(' ')
  },
  'stats': (a) => a.stats ? `stats ${a.stats}` : 'stats',
  'group-agg'(a) {
    if (!a.keys) return null
    const p = [`group-agg ${a.keys}`]
    if (a.aggs) p.push(a.aggs)
    return p.join(' ')
  },
  'frequency': (a) => a.column ? `frequency ${a.column}` : null,
  'window'(a) {
    const p = [`window ${a.size || 100}`]
    if (a.aggs) p.push(a.aggs)
    return p.join(' ')
  },
  'step'(a) {
    if (!a.column) return null
    return `step ${a.column} ${a.op || 'running-sum'}`
  },
  'join'(a) {
    if (!a.file || !a.key) return null
    const p = [`join ${a.file} ${a.key}`]
    if (a.type && a.type !== 'left') p.push(a.type)
    return p.join(' ')
  },
  'csv-encode'(a) {
    const p = ['csv']
    if (a.delimiter && a.delimiter !== ',') p.push(`-d "${a.delimiter}"`)
    if (a.header === false) p.push('--no-header')
    return p.join(' ')
  },
  'jsonl-encode': () => 'jsonl'
}

export function compileToDsl(blocks) {
  const segments = []
  for (const block of blocks) {
    const compiler = compilers[block.typeCode]
    if (!compiler) continue
    const segment = compiler(block.args)
    if (segment) segments.push(segment)
  }
  return segments.join(' | ')
}
