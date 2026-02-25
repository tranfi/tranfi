<script setup>
import { ref, computed, watch, onMounted, onBeforeUnmount, nextTick } from 'vue'
import { BlockList, BlockCard, useBlockList } from './lib/block-editor/index.js'
import { compileToSchema, compileToServerSchema } from './lib/compile-schema.js'
import { compileToDsl } from './lib/compile-dsl.js'
import { blockTypes, blockTypeMap, memoryTierInfo, groupedBlockTypes } from './lib/block-types.js'
import { createBlock } from './lib/block-factory.js'
import { parseDsl } from './lib/parse-dsl.js'
import JSEE from '@jseeio/jsee'
import jseeSource from '@jseeio/jsee/dist/jsee.js?raw'

// Codecs
import TfBlockCsvDecode from './components/blocks/TfBlockCsvDecode.vue'
import TfBlockJsonlDecode from './components/blocks/TfBlockJsonlDecode.vue'
import TfBlockCsvEncode from './components/blocks/TfBlockCsvEncode.vue'
import TfBlockJsonlEncode from './components/blocks/TfBlockJsonlEncode.vue'
// Row transforms
import TfBlockFilter from './components/blocks/TfBlockFilter.vue'
import TfBlockSelect from './components/blocks/TfBlockSelect.vue'
import TfBlockRename from './components/blocks/TfBlockRename.vue'
import TfBlockReorder from './components/blocks/TfBlockReorder.vue'
import TfBlockValidate from './components/blocks/TfBlockValidate.vue'
// Limit / sample
import TfBlockHead from './components/blocks/TfBlockHead.vue'
import TfBlockSkip from './components/blocks/TfBlockSkip.vue'
import TfBlockTail from './components/blocks/TfBlockTail.vue'
import TfBlockTop from './components/blocks/TfBlockTop.vue'
import TfBlockDedup from './components/blocks/TfBlockDedup.vue'
import TfBlockSample from './components/blocks/TfBlockSample.vue'
// Compute / derive
import TfBlockDerive from './components/blocks/TfBlockDerive.vue'
import TfBlockCast from './components/blocks/TfBlockCast.vue'
import TfBlockFillNull from './components/blocks/TfBlockFillNull.vue'
import TfBlockFillDown from './components/blocks/TfBlockFillDown.vue'
import TfBlockReplace from './components/blocks/TfBlockReplace.vue'
import TfBlockTrim from './components/blocks/TfBlockTrim.vue'
import TfBlockClip from './components/blocks/TfBlockClip.vue'
import TfBlockBin from './components/blocks/TfBlockBin.vue'
import TfBlockHash from './components/blocks/TfBlockHash.vue'
import TfBlockDatetime from './components/blocks/TfBlockDatetime.vue'
// Restructure
import TfBlockSort from './components/blocks/TfBlockSort.vue'
import TfBlockUnique from './components/blocks/TfBlockUnique.vue'
import TfBlockExplode from './components/blocks/TfBlockExplode.vue'
import TfBlockSplit from './components/blocks/TfBlockSplit.vue'
import TfBlockFlatten from './components/blocks/TfBlockFlatten.vue'
import TfBlockUnpivot from './components/blocks/TfBlockUnpivot.vue'
// Aggregate
import TfBlockStats from './components/blocks/TfBlockStats.vue'
import TfBlockGroupAgg from './components/blocks/TfBlockGroupAgg.vue'
import TfBlockFrequency from './components/blocks/TfBlockFrequency.vue'
import TfBlockWindow from './components/blocks/TfBlockWindow.vue'
import TfBlockStep from './components/blocks/TfBlockStep.vue'
// Data prep / ML
import TfBlockOnehot from './components/blocks/TfBlockOnehot.vue'
import TfBlockLabelEncode from './components/blocks/TfBlockLabelEncode.vue'
import TfBlockEwma from './components/blocks/TfBlockEwma.vue'
import TfBlockDiff from './components/blocks/TfBlockDiff.vue'
import TfBlockAnomaly from './components/blocks/TfBlockAnomaly.vue'
import TfBlockSplitData from './components/blocks/TfBlockSplitData.vue'
import TfBlockInterpolate from './components/blocks/TfBlockInterpolate.vue'
import TfBlockNormalize from './components/blocks/TfBlockNormalize.vue'
import TfBlockAcf from './components/blocks/TfBlockAcf.vue'
// Join
import TfBlockJoin from './components/blocks/TfBlockJoin.vue'

const bodyComponents = {
  'csv-decode': TfBlockCsvDecode,
  'jsonl-decode': TfBlockJsonlDecode,
  'csv-encode': TfBlockCsvEncode,
  'jsonl-encode': TfBlockJsonlEncode,
  'filter': TfBlockFilter,
  'select': TfBlockSelect,
  'rename': TfBlockRename,
  'reorder': TfBlockReorder,
  'validate': TfBlockValidate,
  'head': TfBlockHead,
  'skip': TfBlockSkip,
  'tail': TfBlockTail,
  'top': TfBlockTop,
  'dedup': TfBlockDedup,
  'sample': TfBlockSample,
  'derive': TfBlockDerive,
  'cast': TfBlockCast,
  'fill-null': TfBlockFillNull,
  'fill-down': TfBlockFillDown,
  'replace': TfBlockReplace,
  'trim': TfBlockTrim,
  'clip': TfBlockClip,
  'bin': TfBlockBin,
  'hash': TfBlockHash,
  'datetime': TfBlockDatetime,
  'sort': TfBlockSort,
  'unique': TfBlockUnique,
  'explode': TfBlockExplode,
  'split': TfBlockSplit,
  'flatten': TfBlockFlatten,
  'unpivot': TfBlockUnpivot,
  'stats': TfBlockStats,
  'group-agg': TfBlockGroupAgg,
  'frequency': TfBlockFrequency,
  'window': TfBlockWindow,
  'step': TfBlockStep,
  'onehot': TfBlockOnehot,
  'label-encode': TfBlockLabelEncode,
  'ewma': TfBlockEwma,
  'diff': TfBlockDiff,
  'anomaly': TfBlockAnomaly,
  'split-data': TfBlockSplitData,
  'interpolate': TfBlockInterpolate,
  'normalize': TfBlockNormalize,
  'acf': TfBlockAcf,
  'join': TfBlockJoin
}

// --- Pipeline / tab state ---

let tabCounter = 0
function createPipeline(name, initialBlocks) {
  const id = tabCounter++
  const b = initialBlocks || [createBlock('csv-decode'), createBlock('csv-encode')]
  return { id, name: name || 'Pipeline ' + (id + 1), blocks: b, dsl: compileToDsl(b) }
}

function initPipelines() {
  const hash = window.location.hash
  if (hash.startsWith('#tabs=')) {
    try {
      const data = JSON.parse(decodeURIComponent(hash.slice(6)))
      return data.map(d => {
        const b = parseDsl(d.dsl) || [createBlock('csv-decode'), createBlock('csv-encode')]
        return createPipeline(d.name, b)
      })
    } catch (e) { /* fall through */ }
  }
  if (hash.startsWith('#dsl=')) {
    try {
      const parsed = parseDsl(decodeURIComponent(hash.slice(5)))
      if (parsed && parsed.length > 0) return [createPipeline('Pipeline 1', parsed)]
    } catch (e) { /* fall through */ }
  }
  return [createPipeline()]
}

const pipelines = ref(initPipelines())
const activeTab = ref(0)

const blocks = computed({
  get: () => pipelines.value[activeTab.value]?.blocks || [],
  set: (val) => { pipelines.value[activeTab.value].blocks = val }
})
const dsl = computed({
  get: () => pipelines.value[activeTab.value]?.dsl || '',
  set: (val) => { pipelines.value[activeTab.value].dsl = val }
})

const { addBlock, removeBlock, cloneBlock, moveToTop, toggleBlock } = useBlockList(blocks, createBlock)

const linkCopied = ref(false)
let dslFromBlocks = false
let dslFromInput = false
let switching = false

// blocks → dsl
watch(blocks, (val) => {
  if (dslFromInput || switching) return
  dslFromBlocks = true
  dsl.value = compileToDsl(val)
  nextTick(() => { dslFromBlocks = false })
}, { deep: true })

// dsl → blocks
watch(dsl, (val) => {
  if (dslFromBlocks || switching) return
  dslFromInput = true
  const parsed = parseDsl(val)
  if (parsed) blocks.value = parsed
  nextTick(() => { dslFromInput = false })
})

const ready = ref(false)
const error = ref('')
const serverMode = ref(false)
const saveWithResults = ref(false)

const jseeInstances = {}
let serverFiles = null
let serverApi = '/api'

onMounted(async () => {
  if (window.__TRANFI_SERVER__) {
    serverMode.value = true
    serverApi = window.__TRANFI_SERVER__.api || '/api'
    try {
      const res = await fetch(`${serverApi}/files`)
      const data = await res.json()
      serverFiles = data.files.map(f => f.name)
    } catch (e) {
      error.value = `Failed to connect to server: ${e.message}`
      return
    }
  }
  await initJseeForPipeline(pipelines.value[0])
})

onBeforeUnmount(() => {
  for (const id of Object.keys(jseeInstances)) {
    jseeInstances[id].destroy()
    delete jseeInstances[id]
  }
})

// Update JSEE's dsl input and URL hash when dsl changes
let hashTimer = null
watch(dsl, (val) => {
  if (switching) return
  const pipeline = pipelines.value[activeTab.value]
  if (!pipeline) return
  const inst = jseeInstances[pipeline.id]
  if (inst) {
    const dslInput = inst.data.inputs.find(i => i.name === 'dsl')
    if (dslInput) dslInput.value = val
  }

  clearTimeout(hashTimer)
  hashTimer = setTimeout(() => updateHash(), 500)
})

function updateHash() {
  if (pipelines.value.length === 1) {
    const encoded = encodeURIComponent(pipelines.value[0].dsl)
    history.replaceState(null, '', '#dsl=' + encoded)
  } else {
    const data = pipelines.value.map(p => ({ name: p.name, dsl: p.dsl }))
    const encoded = encodeURIComponent(JSON.stringify(data))
    history.replaceState(null, '', '#tabs=' + encoded)
  }
}

function copyLink() {
  const hash = pipelines.value.length === 1
    ? '#dsl=' + encodeURIComponent(pipelines.value[0].dsl)
    : '#tabs=' + encodeURIComponent(JSON.stringify(pipelines.value.map(p => ({ name: p.name, dsl: p.dsl }))))
  const url = window.location.origin + window.location.pathname + hash
  navigator.clipboard.writeText(url)
  linkCopied.value = true
  setTimeout(() => { linkCopied.value = false }, 2000)
}

async function initJseeForPipeline(pipeline) {
  if (jseeInstances[pipeline.id]) return
  const containerId = '#jsee-output-' + pipeline.id
  const schema = serverMode.value
    ? compileToServerSchema(pipeline.dsl, serverFiles, { api: serverApi })
    : compileToSchema(pipeline.dsl)
  jseeInstances[pipeline.id] = new JSEE({
    schema,
    container: containerId,
    verbose: false
  })
  await jseeInstances[pipeline.id]._initPromise
  ready.value = true
}

// --- Tab operations ---

function switchTab(idx) {
  if (idx === activeTab.value) return
  if (idx < 0 || idx >= pipelines.value.length) return
  switching = true
  activeTab.value = idx
  nextTick(() => { switching = false })
  const p = pipelines.value[idx]
  if (!jseeInstances[p.id]) nextTick(() => initJseeForPipeline(p))
  updateHash()
}

function addTab() {
  if (pipelines.value.length >= 10) return
  const p = createPipeline()
  pipelines.value.push(p)
  nextTick(() => switchTab(pipelines.value.length - 1))
}

function removeTab(idx) {
  if (pipelines.value.length <= 1) return
  const p = pipelines.value[idx]
  if (jseeInstances[p.id]) {
    jseeInstances[p.id].destroy()
    delete jseeInstances[p.id]
  }
  switching = true
  pipelines.value.splice(idx, 1)
  if (activeTab.value > idx) {
    activeTab.value--
  } else if (activeTab.value >= pipelines.value.length) {
    activeTab.value = pipelines.value.length - 1
  }
  nextTick(() => { switching = false })
  const active = pipelines.value[activeTab.value]
  if (active && !jseeInstances[active.id]) {
    nextTick(() => initJseeForPipeline(active))
  }
  updateHash()
}

function onAddBlock({ typeCode, index }) {
  addBlock(typeCode, index)
}

function getModelResults () {
  const pipeline = pipelines.value[activeTab.value]
  const inst = pipeline ? jseeInstances[pipeline.id] : null
  if (!inst || !inst.data || !inst.data.outputs) return null
  const outputs = inst.data.outputs
  const result = {}
  for (const o of outputs) {
    if ((o.name === 'output' || o.name === 'stats') && o.value) {
      result[o.name] = o.value
    }
  }
  return Object.keys(result).length > 0 ? result : null
}

async function downloadHtml() {
  const pipeline = pipelines.value[activeTab.value]
  const inst = pipeline ? jseeInstances[pipeline.id] : null
  if (!inst) return

  const schema = compileToSchema(dsl.value)
  const withResults = saveWithResults.value
  const results = withResults ? getModelResults() : null

  // Fetch model code, WASM core, and viz renderer for embedding
  const base = import.meta.env.BASE_URL || '/'
  const [runnerCode, coreCode, vizCode] = await Promise.all([
    fetch(`${base}wasm/tranfi-runner.js`).then(r => r.text()),
    fetch(`${base}wasm/tranfi_core.js`).then(r => r.text()),
    fetch(`${base}wasm/tranfi-viz.js`).then(r => r.text())
  ])
  const jseeCode = jseeSource

  const resultsScript = results
    ? `\n    <script type="text/plain" id="tranfi-results">${JSON.stringify(results).replace(/<\//g, '<\\/')}<\/script>`
    : ''

  // When embedding results, add a preload model step
  const exportSchema = JSON.parse(JSON.stringify(schema))
  if (results) {
    exportSchema.model = [
      {
        type: 'function',
        code: 'function tranfiPreload() { var el = document.getElementById("tranfi-results"); return el ? JSON.parse(el.textContent) : {} }',
        name: 'tranfiPreload'
      }
    ]
  }

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Tranfi – ${dsl.value}</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 20px; }
    .header { display: flex; align-items: center; gap: 12px; margin-bottom: 16px; }
    .header h1 { font-size: 20px; font-weight: 600; }
    .header .dsl { font-size: 12px; color: #888; font-family: monospace; }
    #download-btn { margin-left: auto; padding: 6px 14px; border: 1px solid #ccc; border-radius: 4px; background: #fff; cursor: pointer; font-size: 13px; }
    #download-btn:hover { background: #f5f5f5; }
  </style>
</head>
<body>
  <div id="hidden-storage" style="display:none">
    <script type="text/plain" data-src="/wasm/tranfi-runner.js">${runnerCode}<\/script>
    <script type="text/plain" data-src="/wasm/tranfi_core.js">${coreCode}<\/script>
    <script type="text/plain" data-src="/wasm/tranfi-viz.js">${vizCode}<\/script>${resultsScript}
  </div>
  <div class="header">
    <h1>Tranfi</h1>
    <span class="dsl">${dsl.value}</span>
    <button id="download-btn" onclick="env.download('tranfi-pipeline')">Download HTML</button>
  </div>
  <div id="jsee-container"></div>
  <script>${jseeCode}<\/script>
  <script>
    var schema = ${JSON.stringify(exportSchema, null, 2)};
    var env = new JSEE({ container: '#jsee-container', schema: schema });
  <\/script>
</body>
</html>`

  const blob = new Blob([html], { type: 'text/html' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = 'tranfi-pipeline.html'
  a.click()
  URL.revokeObjectURL(url)
}
</script>

<template>
  <v-app>
    <div class="app-layout">
      <!-- Left sidebar: block editor -->
      <div class="sidebar">
        <header>
          <h1>Tranfi</h1>
        </header>

        <div class="tab-bar">
          <v-tabs :model-value="activeTab" density="compact" show-arrows>
            <v-tab
              v-for="(p, idx) in pipelines"
              :key="p.id"
              :value="idx"
              @click="switchTab(idx)"
            >
              {{ p.name }}
              <v-btn
                v-if="pipelines.length > 1"
                icon="mdi-close"
                size="x-small"
                variant="text"
                class="tab-close"
                @click.stop="removeTab(idx)"
              />
            </v-tab>
          </v-tabs>
          <v-btn
            icon="mdi-plus"
            size="small"
            variant="text"
            @click="addTab"
            title="New pipeline"
          />
        </div>

        <BlockList
          v-model="blocks"
          :block-types="blockTypes"
          :grouped-block-types="groupedBlockTypes"
          :memory-tier-info="memoryTierInfo"
          @add="onAddBlock"
        >
          <template #item="{ block, index }">
            <BlockCard
              :title="blockTypeMap[block.typeCode]?.name || block.typeCode"
              :color="blockTypeMap[block.typeCode]?.color || '#ababab'"
              :minimized="block.minimized"
              @toggle="toggleBlock(index)"
              @remove="removeBlock(index)"
              @clone="cloneBlock(index)"
              @move-to-top="moveToTop(index)"
            >
              <component :is="bodyComponents[block.typeCode]" :block="block" />
            </BlockCard>
          </template>
        </BlockList>

        <div class="dsl-display">
          <label>Pipeline</label>
          <textarea
            class="dsl-input"
            v-model="dsl"
            spellcheck="false"
            rows="3"
          ></textarea>
        </div>

        <div class="sidebar-actions" v-if="!serverMode">
          <v-btn
            variant="outlined"
            :disabled="!ready"
            @click="copyLink"
            style="flex: 1"
          >
            <v-icon start>mdi-link-variant</v-icon>
            {{ linkCopied ? 'Copied!' : 'Copy link' }}
          </v-btn>
          <v-btn
            variant="outlined"
            :disabled="!ready"
            @click="downloadHtml"
            style="flex: 1"
          >
            <v-icon start>mdi-download</v-icon>
            Export HTML
          </v-btn>
        </div>
        <label class="save-results-toggle" v-if="!serverMode">
          <input type="checkbox" v-model="saveWithResults" />
          Save with results
        </label>
      </div>

      <!-- Right panel: JSEE handles file upload, run, output, download -->
      <div class="output-panel">
        <div v-if="error" class="errors">{{ error }}</div>
        <div
          v-for="(p, idx) in pipelines"
          :key="p.id"
          :id="'jsee-output-' + p.id"
          :class="['jsee-container', { 'jsee-hidden': idx !== activeTab }]"
        ></div>
        <div v-if="!ready" class="loading-msg">{{ serverMode ? 'Connecting...' : 'Loading WASM...' }}</div>
      </div>
    </div>
  </v-app>
</template>

<style>
.app-layout {
  display: flex;
  min-height: 100vh;
}

.sidebar {
  width: 380px;
  min-width: 380px;
  padding: 20px;
  border-right: 1px solid #e0e0e0;
  overflow-y: auto;
  max-height: 100vh;
  background: #fafafa;
}

.sidebar header {
  display: flex;
  align-items: baseline;
  gap: 12px;
  margin-bottom: 20px;
}

.sidebar header h1 {
  font-size: 24px;
  font-weight: 600;
}

.output-panel {
  flex: 1;
  padding: 20px;
  overflow-y: auto;
  max-height: 100vh;
}

.dsl-display {
  margin-top: 12px;
  padding: 8px 12px;
  background: #f5f5f5;
  border: 1px solid #e0e0e0;
  border-radius: 6px;
  font-size: 12px;
}

.dsl-display label {
  display: block;
  font-size: 10px;
  color: #888;
  margin-bottom: 4px;
}

.dsl-input {
  width: 100%;
  font-family: 'SF Mono', 'Fira Code', monospace;
  font-size: 12px;
  color: #1565c0;
  background: transparent;
  border: none;
  outline: none;
  resize: vertical;
  line-height: 1.5;
  word-break: break-all;
}

.sidebar-actions {
  display: flex;
  gap: 8px;
  margin-top: 12px;
}

.save-results-toggle {
  display: flex;
  align-items: center;
  gap: 6px;
  margin-top: 8px;
  font-size: 12px;
  color: #666;
  cursor: pointer;
}

.save-results-toggle input {
  margin: 0;
}

.tab-bar {
  display: flex;
  align-items: center;
  margin-bottom: 12px;
}

.tab-close {
  margin-left: 2px;
  opacity: 0.3;
}

.tab-close:hover {
  opacity: 1;
}

.jsee-hidden {
  visibility: hidden;
  position: absolute;
  width: 0;
  height: 0;
  overflow: hidden;
}

.loading-msg {
  margin-top: 12px;
  color: #999;
  font-size: 13px;
}

.errors {
  padding: 12px;
  background: #fef2f2;
  border: 1px solid #fca5a5;
  border-radius: 6px;
  color: #dc2626;
  font-family: monospace;
  font-size: 13px;
  white-space: pre-wrap;
  margin-bottom: 16px;
}

@media (max-width: 600px) {
  .app-layout {
    flex-direction: column;
  }
  .sidebar {
    width: 100%;
    min-width: auto;
    max-height: none;
    border-right: none;
    border-bottom: 1px solid #e0e0e0;
    padding: 12px;
  }
  .sidebar header h1 {
    font-size: 20px;
  }
  .output-panel {
    max-height: none;
    padding: 12px;
  }
}
</style>
