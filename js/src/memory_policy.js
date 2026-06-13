/** Host-side memory policy checks for native Tranfi execution. */

const BLOCKING_OPS = new Set([
  'sort',
  'pivot',
  'stack',
  'interpolate',
  'normalize',
  'acf',
  'codec.table.encode'
])

const KEY_STATE_OPS = new Set([
  'rowid',
  'unique',
  'dedup',
  'group-agg',
  'frequency',
  'join',
  'semi-join',
  'anti-join',
  'onehot',
  'label-encode',
  'intersect',
  'setdiff',
  'intersect-all',
  'setdiff-all',
  'union'
])

const SIZE_UNITS = {
  '': 1,
  b: 1,
  k: 1024,
  kb: 1024,
  kib: 1024,
  m: 1024 ** 2,
  mb: 1024 ** 2,
  mib: 1024 ** 2,
  g: 1024 ** 3,
  gb: 1024 ** 3,
  gib: 1024 ** 3,
  t: 1024 ** 4,
  tb: 1024 ** 4,
  tib: 1024 ** 4
}

function parseMemorySize(value) {
  if (value === undefined || value === null) return null
  if (typeof value === 'number') {
    if (!Number.isFinite(value) || value <= 0 || Math.floor(value) !== value) {
      throw new Error('memory must be a positive integer byte count')
    }
    return value
  }
  if (typeof value !== 'string') {
    throw new TypeError('memory must be a positive byte count or size string')
  }
  let text = value.trim()
  const lower = text.toLowerCase()
  if (lower.startsWith('max:') || lower.startsWith('max=')) text = text.slice(4).trim()
  const match = text.match(/^(\d+)\s*([a-zA-Z]*)$/)
  if (!match) throw new Error('memory must look like 64MB or max:64MB')
  const amount = Number(match[1])
  const suffix = match[2].toLowerCase()
  if (amount <= 0) throw new Error('memory must be positive')
  if (!(suffix in SIZE_UNITS)) throw new Error(`unsupported memory size suffix '${suffix}'`)
  return amount * SIZE_UNITS[suffix]
}

function memoryForEngine(value) {
  const n = parseMemorySize(value)
  return n === null ? null : `${n}B`
}

function formatBytes(n) {
  if (n < 1024) return `${n}B`
  if (n < 1024 ** 2) return `${(n / 1024).toFixed(1)}KB`
  if (n < 1024 ** 3) return `${(n / (1024 ** 2)).toFixed(1)}MB`
  return `${(n / (1024 ** 3)).toFixed(1)}GB`
}

function stepsFromPlan(planJson) {
  const plan = typeof planJson === 'string' ? JSON.parse(planJson) : planJson
  if (!plan || !Array.isArray(plan.steps)) throw new Error('plan must contain a steps array')
  return plan.steps
}

function isFilteringJoin(step) {
  const args = argsOf(step)
  return step.op === 'semi-join' || step.op === 'anti-join' ||
    (step.op === 'join' && (args.how === 'semi' || args.how === 'anti'))
}

function memoryClass(step) {
  if (typeof step.memory_class === 'string') return step.memory_class
  const args = argsOf(step)
  if (step.op === 'pivot') {
    if (typeof args.spill_dir === 'string' && args.spill_dir) return 'external'
    if (args.sorted === true && Array.isArray(args.categories) && args.categories.length > 0) return 'bounded_state'
  }
  if (step.op === 'unique' || step.op === 'dedup') {
    if (typeof args.spill_dir === 'string' && args.spill_dir) return 'external'
    if (args.sorted === true) return 'bounded_state'
  }
  if (step.op === 'group-agg') {
    if (typeof args.spill_dir === 'string' && args.spill_dir) return 'external'
    if (args.sorted === true) return 'bounded_state'
  }
  if (step.op === 'join' || step.op === 'semi-join' || step.op === 'anti-join') {
    if (typeof args.spill_dir === 'string' && args.spill_dir) return 'external'
    if (args.sorted === true) return 'bounded_state'
  }
  if (step.op === 'intersect' || step.op === 'setdiff' || step.op === 'intersect-all' || step.op === 'setdiff-all') {
    if (typeof args.spill_dir === 'string' && args.spill_dir) return 'external'
    if (args.sorted === true) return 'bounded_state'
  }
  if (step.op === 'union') {
    if (typeof args.spill_dir === 'string' && args.spill_dir) return 'external'
    if (args.sorted === true) return 'bounded_state'
  }
  if (BLOCKING_OPS.has(step.op)) return 'blocking'
  if (KEY_STATE_OPS.has(step.op)) return 'key_state'
  return null
}

function argsOf(step) {
  return step && step.args && typeof step.args === 'object' ? step.args : {}
}

function positiveInt(value) {
  return typeof value === 'number' && Number.isFinite(value) && value > 0 && Math.floor(value) === value
    ? value
    : null
}

function arrayLen(value) {
  return Array.isArray(value) ? value.length : 0
}

function stringArrayBytes(value) {
  if (!Array.isArray(value)) return 0
  let total = 0
  for (const item of value) {
    if (typeof item === 'string') total += Buffer.byteLength(item, 'utf-8') + 1
  }
  return total
}

function categoryCap(step) {
  const args = argsOf(step)
  const maxCategories = positiveInt(args.max_categories)
  let nCategories = arrayLen(args.categories)
  let literalBytes = stringArrayBytes(args.categories)
  if (args.unknown === 'other') {
    nCategories += 1
    literalBytes += 'other'.length + 1
  }
  if (maxCategories !== null) return [maxCategories, literalBytes]
  if (nCategories > 0) return [nCategories, literalBytes]
  return [null, literalBytes]
}

function estimateKeyStateStepBytes(step) {
  const args = argsOf(step)
  const op = step.op

  if (op === 'rowid') {
    const nCols = arrayLen(args.columns)
    if (nCols === 0 || args.sorted === true) return 2048 + nCols * 128
    const maxStateBytes = positiveInt(args.max_state_bytes)
    if (maxStateBytes !== null) return maxStateBytes
    const maxKeys = positiveInt(args.max_keys)
    if (maxKeys === null) throw new Error("step 'rowid' needs max_keys, max_state_bytes, or sorted=true")
    return 1024 + maxKeys * 224 + nCols * 128
  }

  if (op === 'unique' || op === 'dedup') {
    const nCols = arrayLen(args.columns)
    if (args.sorted === true) return 2048 + nCols * 128
    const maxStateBytes = positiveInt(args.max_state_bytes)
    if (maxStateBytes !== null) return maxStateBytes
    const maxKeys = positiveInt(args.max_keys)
    if (maxKeys === null) throw new Error(`step '${op}' needs max_keys, max_state_bytes, or sorted=true for byte-bounded native execution`)
    return 1024 + maxKeys * (256 + nCols * 32)
  }

  if (op === 'group-agg') {
    const nGroup = arrayLen(args.group_by)
    const nAggs = arrayLen(args.aggs)
    if (typeof args.spill_dir === 'string' && args.spill_dir) {
      const spillBytes = positiveInt(args.spill_memory_bytes)
      if (spillBytes !== null) return spillBytes
      throw new Error("step 'group-agg' spill mode needs spill_memory_bytes for a host byte estimate")
    }
    const maxStateBytes = positiveInt(args.max_state_bytes)
    if (maxStateBytes !== null) return maxStateBytes
    if (args.sorted === true) return 4096 + nGroup * 128 + nAggs * 128
    const maxGroups = positiveInt(args.max_groups)
    if (maxGroups === null) throw new Error("step 'group-agg' needs max_groups, max_state_bytes, or sorted=true for byte-bounded native execution")
    return 2048 + maxGroups * (384 + nGroup * 64 + nAggs * 96)
  }

  if (op === 'frequency') {
    const maxStateBytes = positiveInt(args.max_state_bytes)
    if (maxStateBytes !== null) return maxStateBytes
    const maxValues = positiveInt(args.max_values)
    if (maxValues === null) throw new Error("step 'frequency' needs max_values or max_state_bytes for byte-bounded native execution")
    const cappedValues = args.overflow === 'other' ? maxValues + 1 : maxValues
    return 1024 + cappedValues * (224 + arrayLen(args.columns) * 32)
  }

  if (op === 'onehot' || op === 'label-encode') {
    const maxStateBytes = positiveInt(args.max_state_bytes)
    if (maxStateBytes !== null) return maxStateBytes
    const [cap, literalBytes] = categoryCap(step)
    if (cap === null) throw new Error(`step '${op}' needs max_categories, max_state_bytes, or declared categories`)
    return 1024 + literalBytes + cap * (op === 'onehot' ? 320 : 224)
  }

  if (op === 'union-all') return 4096

  if (op === 'intersect' || op === 'setdiff' || op === 'intersect-all' || op === 'setdiff-all') {
    const nCols = arrayLen(args.columns)
    const bagSetOp = op === 'intersect-all' || op === 'setdiff-all'
    if (typeof args.spill_dir === 'string' && args.spill_dir) {
      const spillBytes = positiveInt(args.spill_memory_bytes)
      if (spillBytes !== null) return spillBytes
      throw new Error(`step '${op}' spill mode needs spill_memory_bytes for a host byte estimate`)
    }
    if (args.sorted === true) {
      return 4096 + nCols * 256
    }
    const maxLookupBytes = positiveInt(args.max_lookup_bytes)
    if (maxLookupBytes === null) throw new Error(`step '${op}' needs max_lookup_bytes for byte-bounded native execution`)
    const maxStateBytes = positiveInt(args.max_state_bytes)
    if (maxStateBytes !== null) return 4096 + maxLookupBytes * 3 + maxStateBytes
    let est = 4096 + maxLookupBytes * 3
    const maxLookupKeys = positiveInt(args.max_lookup_keys)
    if (bagSetOp) {
      if (maxLookupKeys === null) throw new Error(`step '${op}' needs max_lookup_keys or max_state_bytes for bag-count set semantics`)
      return est + maxLookupKeys * (224 + nCols * 32)
    }
    const maxOutputKeys = positiveInt(args.max_output_keys)
    if (maxOutputKeys === null) throw new Error(`step '${op}' needs max_output_keys or max_state_bytes for duplicate-eliminating set semantics`)
    if (maxLookupKeys !== null) est += maxLookupKeys * (192 + nCols * 32)
    est += maxOutputKeys * (192 + nCols * 32)
    return est
  }

  if (op === 'union') {
    if (args.sorted === true) {
      const nCols = arrayLen(args.columns)
      return 4096 + nCols * 256
    }
    if (typeof args.spill_dir === 'string' && args.spill_dir) {
      const spillMemoryBytes = positiveInt(args.spill_memory_bytes)
      if (spillMemoryBytes === null) throw new Error("step 'union' spill needs spill_memory_bytes for byte-bounded native execution")
      return spillMemoryBytes
    }
    const maxStateBytes = positiveInt(args.max_state_bytes)
    if (maxStateBytes !== null) return maxStateBytes
    const nCols = arrayLen(args.columns)
    const maxOutputKeys = positiveInt(args.max_output_keys)
    if (maxOutputKeys === null) throw new Error("step 'union' needs max_output_keys or max_state_bytes for duplicate-eliminating set semantics")
    return 4096 + maxOutputKeys * (192 + nCols * 32)
  }

  if (op === 'join' || op === 'semi-join' || op === 'anti-join') {
    const filtering = isFilteringJoin(step)
    if (typeof args.spill_dir === 'string' && args.spill_dir) {
      if (!filtering) {
        const maxMatches = positiveInt(args.max_matches_per_row)
        if (maxMatches === null) throw new Error(`step '${op}' spill mode needs max_matches_per_row for byte-bounded mutating join`)
      }
      const spillBytes = positiveInt(args.spill_memory_bytes)
      if (spillBytes !== null) return spillBytes
      throw new Error(`step '${op}' spill mode needs spill_memory_bytes for a host byte estimate`)
    }
    if (args.sorted === true) {
      if (filtering) return 4096
      const maxMatches = positiveInt(args.max_matches_per_row)
      if (maxMatches === null) throw new Error(`step '${op}' sorted=true needs max_matches_per_row for byte-bounded mutating join`)
      return 4096 + maxMatches * 512
    }
    const maxLookupBytes = positiveInt(args.max_lookup_bytes)
    if (maxLookupBytes === null) throw new Error(`step '${op}' needs max_lookup_bytes or sorted=true for byte-bounded native execution`)
    let est = 4096 + maxLookupBytes * 4
    const maxStateBytes = positiveInt(args.max_state_bytes)
    if (maxStateBytes !== null) return est + maxStateBytes
    const maxLookupRows = positiveInt(args.max_lookup_rows)
    const maxLookupKeys = positiveInt(args.max_lookup_keys)
    if (maxLookupRows !== null) est += maxLookupRows * 96
    if (maxLookupKeys !== null) est += maxLookupKeys * 192
    return est
  }

  throw new Error(`step '${op}' has no native byte estimator`)
}

function loadPlan(planJson) {
  const plan = typeof planJson === 'string' ? JSON.parse(planJson) : planJson
  if (!plan || !Array.isArray(plan.steps)) throw new Error('plan must contain a steps array')
  return plan
}

function blockingSteps(steps) {
  return steps.filter(step => memoryClass(step) === 'blocking')
}

function keyStateSteps(steps) {
  return steps.filter(step => memoryClass(step) === 'key_state')
}

function pivotStepCanUseNativeSpill(step) {
  const args = argsOf(step)
  if (step.op !== 'pivot' || args.sorted === true) return false
  return (Array.isArray(args.categories) && args.categories.length > 0) || positiveInt(args.max_categories) !== null
}

function blockingStepCanUseNativeSpill(step) {
  return step.op === 'sort' || pivotStepCanUseNativeSpill(step)
}

function blockingPlanCanUseNativeSpill(steps) {
  return steps.length > 0 && steps.every(blockingStepCanUseNativeSpill)
}

function keyStateStepCanUseNativeSpill(step) {
  const args = argsOf(step)
  return (step.op === 'unique' || step.op === 'dedup' || step.op === 'group-agg' || step.op === 'join' || step.op === 'semi-join' || step.op === 'anti-join' || step.op === 'intersect' || step.op === 'setdiff' || step.op === 'intersect-all' || step.op === 'setdiff-all' || step.op === 'union') && args.sorted !== true
}

function injectNativeSpill(plan, spillDir, memoryBytes) {
  for (const step of plan.steps) {
    if (!blockingStepCanUseNativeSpill(step) && !keyStateStepCanUseNativeSpill(step)) continue
    if (!step.args || typeof step.args !== 'object') step.args = {}
    step.args.spill_dir = spillDir
    if (memoryBytes !== null) step.args.spill_memory_bytes = memoryBytes
  }
  return JSON.stringify(plan)
}

function prepareNativePlan(planJson, { allowBlocking = false, memory, spillDir } = {}) {
  const plan = loadPlan(planJson)
  const steps = plan.steps
  const blocking = blockingSteps(steps)
  const firstBlocking = blocking[0] || null
  const keySteps = keyStateSteps(steps)
  const memoryBytes = parseMemorySize(memory)
  const canSpillBlocking = Boolean(spillDir) && blockingPlanCanUseNativeSpill(blocking)
  const spillableKeySteps = spillDir ? keySteps.filter(keyStateStepCanUseNativeSpill) : []
  const keyStepsForEstimate = keySteps.filter(step => !spillableKeySteps.includes(step))

  if (spillDir) {
    const unsupportedKey = keySteps.find(step => !keyStateStepCanUseNativeSpill(step))
    if (unsupportedKey) {
      throw new Error(`spillDir was requested, but native spill is not implemented yet for key-state step '${unsupportedKey.op}'`)
    }
    const unsupported = blocking.find(step => !blockingStepCanUseNativeSpill(step))
    if (unsupported) {
      throw new Error(`spillDir was requested, but native spill is not implemented yet for blocking step '${unsupported.op}'`)
    }
  }

  if (memoryBytes !== null && firstBlocking && !canSpillBlocking) {
    throw new Error(`memory ${formatBytes(memoryBytes)} was requested, but native byte caps are not implemented for blocking step '${firstBlocking.op}'`)
  }

  if (memoryBytes !== null && keyStepsForEstimate.length > 0) {
    let total = 0
    for (const step of keyStepsForEstimate) total += estimateKeyStateStepBytes(step)
    if (total > memoryBytes) {
      throw new Error(`estimated native key-state memory ${formatBytes(total)} exceeds memory ${formatBytes(memoryBytes)}`)
    }
  }

  if (firstBlocking && !allowBlocking && !canSpillBlocking) {
    throw new Error(`blocking step '${firstBlocking.op}' requires full input in native mode; pass allowBlocking: true for known-small data or use an external engine`)
  }

  if (canSpillBlocking || spillableKeySteps.length > 0) return injectNativeSpill(plan, spillDir, memoryBytes)
  return typeof planJson === 'string' ? planJson : JSON.stringify(plan)
}

function validateNativeMemoryPolicy(planJson, { allowBlocking = false, memory, spillDir } = {}) {
  const memoryBytes = parseMemorySize(memory)
  const prepared = prepareNativePlan(planJson, { allowBlocking, memory, spillDir })
  const plan = loadPlan(prepared)
  const keySteps = keyStateSteps(plan.steps)

  if (memoryBytes === null || keySteps.length === 0) return null
  let estimated = 0
  for (const step of keySteps) estimated += estimateKeyStateStepBytes(step)
  return estimated
}

module.exports = {
  parseMemorySize,
  memoryForEngine,
  prepareNativePlan,
  validateNativeMemoryPolicy,
  estimateKeyStateStepBytes
}
