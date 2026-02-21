<script setup>
import { ref, computed, watch, onMounted, onBeforeUnmount } from 'vue'
import { BlockList, BlockCard, useBlockList } from '@statsim/block-editor'
import { compileToSchema, compileToServerSchema } from './lib/compile-schema.js'
import { compileToDsl } from './lib/compile-dsl.js'
import { blockTypes, blockTypeMap } from './lib/block-types.js'
import { createBlock } from './lib/block-factory.js'

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
  'join': TfBlockJoin
}

const blocks = ref([
  createBlock('csv-decode'),
  createBlock('csv-encode')
])

const { addBlock, removeBlock, cloneBlock, moveToTop, toggleBlock } = useBlockList(blocks, createBlock)

const dsl = computed(() => compileToDsl(blocks.value))

const ready = ref(false)
const error = ref('')
const serverMode = ref(false)

let jseeInstance = null

onMounted(async () => {
  if (window.__TRANFI_SERVER__) {
    serverMode.value = true
    await initJseeServer()
  } else {
    initJsee()
  }
})

onBeforeUnmount(() => {
  if (jseeInstance) {
    jseeInstance.destroy()
    jseeInstance = null
  }
})

// Update JSEE's dsl input when blocks change
watch(dsl, (val) => {
  if (!jseeInstance) return
  const inputs = jseeInstance.data.inputs
  const dslInput = inputs.find(i => i.name === 'dsl')
  if (dslInput) dslInput.value = val
})

async function initJsee() {
  const JSEE = window.JSEE
  if (!JSEE) {
    error.value = 'JSEE library not loaded'
    return
  }

  const schema = compileToSchema(dsl.value)
  jseeInstance = new JSEE({
    schema,
    container: '#jsee-output',
    verbose: false
  })

  await jseeInstance._initPromise
  ready.value = true
}

async function initJseeServer() {
  const JSEE = window.JSEE
  if (!JSEE) {
    error.value = 'JSEE library not loaded'
    return
  }

  const api = window.__TRANFI_SERVER__.api || '/api'
  try {
    const res = await fetch(`${api}/files`)
    const data = await res.json()
    const files = data.files.map(f => f.name)

    const schema = compileToServerSchema(dsl.value, files, { api })
    jseeInstance = new JSEE({
      schema,
      container: '#jsee-output',
      verbose: false
    })

    await jseeInstance._initPromise
    ready.value = true
  } catch (e) {
    error.value = `Failed to connect to server: ${e.message}`
  }
}

function onAddBlock({ typeCode, index }) {
  addBlock(typeCode, index)
}

async function downloadHtml() {
  if (!jseeInstance) return

  // Build a clean schema with the current DSL baked in
  const schema = compileToSchema(dsl.value)

  // Fetch the JSEE runtime, model code, and imports
  const [jseeCode, runnerCode, coreCode] = await Promise.all([
    fetch('/lib/jsee.js').then(r => r.text()),
    fetch('/wasm/tranfi-runner.js').then(r => r.text()),
    fetch('/wasm/tranfi_core.js').then(r => r.text())
  ])

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
  </div>
  <div class="header">
    <h1>Tranfi</h1>
    <span class="dsl">${dsl.value}</span>
    <button id="download-btn" onclick="env.download('tranfi-pipeline')">Download HTML</button>
  </div>
  <div id="jsee-container"></div>
  <script>${jseeCode}<\/script>
  <script>
    var schema = ${JSON.stringify(schema, null, 2)};
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

        <BlockList
          v-model="blocks"
          :block-types="blockTypes"
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

        <!-- Compiled DSL (read-only) -->
        <div class="dsl-display" v-if="dsl">
          <label>Pipeline</label>
          <code>{{ dsl }}</code>
        </div>

        <v-btn
          v-if="!serverMode"
          variant="outlined"
          block
          :disabled="!ready"
          @click="downloadHtml"
          style="margin-top: 12px"
        >
          <v-icon start>mdi-download</v-icon>
          Export HTML
        </v-btn>
      </div>

      <!-- Right panel: JSEE handles file upload, run, output, download -->
      <div class="output-panel">
        <div v-if="error" class="errors">{{ error }}</div>
        <div id="jsee-output"></div>
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

.dsl-display code {
  font-family: 'SF Mono', 'Fira Code', monospace;
  color: #1565c0;
  word-break: break-all;
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
</style>
