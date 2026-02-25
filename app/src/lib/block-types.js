// memoryTier: 'green' = streaming O(1)/O(param), 'yellow' = growing/partial buffer, 'red' = full buffer
export const blockTypes = [
  // Codecs
  { typeCode: 'csv-decode', name: 'CSV Input', color: '#a9e7ff', category: 'decode', memoryTier: 'green' },
  { typeCode: 'jsonl-decode', name: 'JSONL Input', color: '#a9e7ff', category: 'decode', memoryTier: 'green' },

  // Row transforms (pink)
  { typeCode: 'filter', name: 'Filter', color: '#ffcdd5', category: 'transform', memoryTier: 'green' },
  { typeCode: 'select', name: 'Select', color: '#ffcdd5', category: 'transform', memoryTier: 'green' },
  { typeCode: 'rename', name: 'Rename', color: '#ffcdd5', category: 'transform', memoryTier: 'green' },
  { typeCode: 'reorder', name: 'Reorder', color: '#ffcdd5', category: 'transform', memoryTier: 'green' },
  { typeCode: 'validate', name: 'Validate', color: '#ffcdd5', category: 'transform', memoryTier: 'green' },

  // Limit / sample (green)
  { typeCode: 'head', name: 'Head', color: '#c7efa7', category: 'limit', memoryTier: 'green' },
  { typeCode: 'skip', name: 'Skip', color: '#c7efa7', category: 'limit', memoryTier: 'green' },
  { typeCode: 'tail', name: 'Tail', color: '#c7efa7', category: 'limit', memoryTier: 'green' },
  { typeCode: 'top', name: 'Top N', color: '#c7efa7', category: 'limit', memoryTier: 'green' },
  { typeCode: 'dedup', name: 'Dedup', color: '#c7efa7', category: 'limit', memoryTier: 'yellow' },
  { typeCode: 'sample', name: 'Sample', color: '#c7efa7', category: 'limit', memoryTier: 'green' },

  // Compute / derive (yellow)
  { typeCode: 'derive', name: 'Derive', color: '#ffdd8c', category: 'compute', memoryTier: 'green' },
  { typeCode: 'cast', name: 'Cast', color: '#ffdd8c', category: 'compute', memoryTier: 'green' },
  { typeCode: 'fill-null', name: 'Fill Null', color: '#ffdd8c', category: 'compute', memoryTier: 'green' },
  { typeCode: 'fill-down', name: 'Fill Down', color: '#ffdd8c', category: 'compute', memoryTier: 'green' },
  { typeCode: 'replace', name: 'Replace', color: '#ffdd8c', category: 'compute', memoryTier: 'green' },
  { typeCode: 'trim', name: 'Trim', color: '#ffdd8c', category: 'compute', memoryTier: 'green' },
  { typeCode: 'clip', name: 'Clip', color: '#ffdd8c', category: 'compute', memoryTier: 'green' },
  { typeCode: 'bin', name: 'Bin', color: '#ffdd8c', category: 'compute', memoryTier: 'green' },
  { typeCode: 'hash', name: 'Hash', color: '#ffdd8c', category: 'compute', memoryTier: 'green' },
  { typeCode: 'datetime', name: 'DateTime', color: '#ffdd8c', category: 'compute', memoryTier: 'green' },

  // Restructure (light teal)
  { typeCode: 'sort', name: 'Sort', color: '#b2dfdb', category: 'restructure', memoryTier: 'red' },
  { typeCode: 'unique', name: 'Unique', color: '#b2dfdb', category: 'restructure', memoryTier: 'yellow' },
  { typeCode: 'explode', name: 'Explode', color: '#b2dfdb', category: 'restructure', memoryTier: 'green' },
  { typeCode: 'split', name: 'Split', color: '#b2dfdb', category: 'restructure', memoryTier: 'green' },
  { typeCode: 'flatten', name: 'Flatten', color: '#b2dfdb', category: 'restructure', memoryTier: 'green' },
  { typeCode: 'unpivot', name: 'Unpivot', color: '#b2dfdb', category: 'restructure', memoryTier: 'green' },

  // Aggregate (purple)
  { typeCode: 'stats', name: 'Stats', color: '#d1c4e9', category: 'aggregate', memoryTier: 'green' },
  { typeCode: 'group-agg', name: 'Group By', color: '#d1c4e9', category: 'aggregate', memoryTier: 'red' },
  { typeCode: 'frequency', name: 'Frequency', color: '#d1c4e9', category: 'aggregate', memoryTier: 'red' },
  { typeCode: 'window', name: 'Window', color: '#d1c4e9', category: 'aggregate', memoryTier: 'green' },
  { typeCode: 'step', name: 'Running', color: '#d1c4e9', category: 'aggregate', memoryTier: 'green' },

  // Data prep / ML (pink)
  { typeCode: 'onehot', name: 'One-Hot', color: '#f8bbd0', category: 'ml', memoryTier: 'yellow' },
  { typeCode: 'label-encode', name: 'Label Encode', color: '#f8bbd0', category: 'ml', memoryTier: 'yellow' },
  { typeCode: 'ewma', name: 'EWMA', color: '#f8bbd0', category: 'ml', memoryTier: 'green' },
  { typeCode: 'diff', name: 'Diff', color: '#f8bbd0', category: 'ml', memoryTier: 'green' },
  { typeCode: 'anomaly', name: 'Anomaly', color: '#f8bbd0', category: 'ml', memoryTier: 'green' },
  { typeCode: 'split-data', name: 'Train/Test', color: '#f8bbd0', category: 'ml', memoryTier: 'green' },
  { typeCode: 'interpolate', name: 'Interpolate', color: '#f8bbd0', category: 'ml', memoryTier: 'yellow' },
  { typeCode: 'normalize', name: 'Normalize', color: '#f8bbd0', category: 'ml', memoryTier: 'red' },
  { typeCode: 'acf', name: 'ACF', color: '#f8bbd0', category: 'ml', memoryTier: 'red' },

  // Join (light blue)
  { typeCode: 'join', name: 'Join', color: '#b3e5fc', category: 'join', memoryTier: 'yellow' },

  // Encoders
  { typeCode: 'csv-encode', name: 'CSV Output', color: '#ababab', category: 'encode', memoryTier: 'green' },
  { typeCode: 'jsonl-encode', name: 'JSONL Output', color: '#ababab', category: 'encode', memoryTier: 'green' }
]

export const blockTypeMap = Object.fromEntries(blockTypes.map(bt => [bt.typeCode, bt]))

export const memoryTierInfo = {
  green:  { icon: 'mdi-arrow-right', label: 'Streaming — constant memory' },
  yellow: { icon: 'mdi-checkbox-blank-outline', label: 'Partial buffer — memory grows with data' },
  red:    { icon: 'mdi-checkbox-blank', label: 'Full buffer — loads all data' }
}

export const categoryLabels = {
  decode: 'Input',
  transform: 'Transform',
  limit: 'Limit / Sample',
  compute: 'Compute',
  restructure: 'Restructure',
  aggregate: 'Aggregate',
  ml: 'Data Prep / ML',
  join: 'Join',
  encode: 'Output'
}

export const groupedBlockTypes = (() => {
  const order = ['decode', 'transform', 'limit', 'compute', 'restructure', 'aggregate', 'ml', 'join', 'encode']
  const groups = {}
  for (const bt of blockTypes) {
    if (!groups[bt.category]) groups[bt.category] = []
    groups[bt.category].push(bt)
  }
  return order.filter(c => groups[c]).map(c => ({
    category: c,
    label: categoryLabels[c],
    items: groups[c]
  }))
})()
