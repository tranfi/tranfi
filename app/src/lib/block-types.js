export const blockTypes = [
  // Codecs
  { typeCode: 'csv-decode', name: 'CSV Input', color: '#a9e7ff', category: 'decode' },
  { typeCode: 'jsonl-decode', name: 'JSONL Input', color: '#a9e7ff', category: 'decode' },

  // Row transforms (pink)
  { typeCode: 'filter', name: 'Filter', color: '#ffcdd5', category: 'transform' },
  { typeCode: 'select', name: 'Select', color: '#ffcdd5', category: 'transform' },
  { typeCode: 'rename', name: 'Rename', color: '#ffcdd5', category: 'transform' },
  { typeCode: 'reorder', name: 'Reorder', color: '#ffcdd5', category: 'transform' },
  { typeCode: 'validate', name: 'Validate', color: '#ffcdd5', category: 'transform' },

  // Limit / sample (green)
  { typeCode: 'head', name: 'Head', color: '#c7efa7', category: 'limit' },
  { typeCode: 'skip', name: 'Skip', color: '#c7efa7', category: 'limit' },
  { typeCode: 'tail', name: 'Tail', color: '#c7efa7', category: 'limit' },
  { typeCode: 'top', name: 'Top N', color: '#c7efa7', category: 'limit' },
  { typeCode: 'dedup', name: 'Dedup', color: '#c7efa7', category: 'limit' },
  { typeCode: 'sample', name: 'Sample', color: '#c7efa7', category: 'limit' },

  // Compute / derive (yellow)
  { typeCode: 'derive', name: 'Derive', color: '#ffdd8c', category: 'compute' },
  { typeCode: 'cast', name: 'Cast', color: '#ffdd8c', category: 'compute' },
  { typeCode: 'fill-null', name: 'Fill Null', color: '#ffdd8c', category: 'compute' },
  { typeCode: 'fill-down', name: 'Fill Down', color: '#ffdd8c', category: 'compute' },
  { typeCode: 'replace', name: 'Replace', color: '#ffdd8c', category: 'compute' },
  { typeCode: 'trim', name: 'Trim', color: '#ffdd8c', category: 'compute' },
  { typeCode: 'clip', name: 'Clip', color: '#ffdd8c', category: 'compute' },
  { typeCode: 'bin', name: 'Bin', color: '#ffdd8c', category: 'compute' },
  { typeCode: 'hash', name: 'Hash', color: '#ffdd8c', category: 'compute' },
  { typeCode: 'datetime', name: 'DateTime', color: '#ffdd8c', category: 'compute' },

  // Restructure (light teal)
  { typeCode: 'sort', name: 'Sort', color: '#b2dfdb', category: 'restructure' },
  { typeCode: 'unique', name: 'Unique', color: '#b2dfdb', category: 'restructure' },
  { typeCode: 'explode', name: 'Explode', color: '#b2dfdb', category: 'restructure' },
  { typeCode: 'split', name: 'Split', color: '#b2dfdb', category: 'restructure' },
  { typeCode: 'flatten', name: 'Flatten', color: '#b2dfdb', category: 'restructure' },
  { typeCode: 'unpivot', name: 'Unpivot', color: '#b2dfdb', category: 'restructure' },

  // Aggregate (purple)
  { typeCode: 'stats', name: 'Stats', color: '#d1c4e9', category: 'aggregate' },
  { typeCode: 'group-agg', name: 'Group By', color: '#d1c4e9', category: 'aggregate' },
  { typeCode: 'frequency', name: 'Frequency', color: '#d1c4e9', category: 'aggregate' },
  { typeCode: 'window', name: 'Window', color: '#d1c4e9', category: 'aggregate' },
  { typeCode: 'step', name: 'Running', color: '#d1c4e9', category: 'aggregate' },

  // Join (light blue)
  { typeCode: 'join', name: 'Join', color: '#b3e5fc', category: 'join' },

  // Encoders
  { typeCode: 'csv-encode', name: 'CSV Output', color: '#ababab', category: 'encode' },
  { typeCode: 'jsonl-encode', name: 'JSONL Output', color: '#ababab', category: 'encode' }
]

export const blockTypeMap = Object.fromEntries(blockTypes.map(bt => [bt.typeCode, bt]))
