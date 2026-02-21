<script setup>
import { computed } from 'vue'

const props = defineProps({
  text: String
})

const rows = computed(() => {
  if (!props.text) return []
  return props.text.trim().split('\n').slice(0, 200)
})

const header = computed(() => rows.value[0]?.split(',') || [])
const body = computed(() => rows.value.slice(1).map(r => r.split(',')))
const truncated = computed(() => props.text && props.text.trim().split('\n').length > 200)
</script>

<template>
  <div class="preview" v-if="rows.length">
    <div class="label">Output ({{ rows.length - 1 }} rows{{ truncated ? ', showing first 200' : '' }})</div>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th v-for="(col, i) in header" :key="i">{{ col }}</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(row, i) in body" :key="i">
            <td v-for="(cell, j) in row" :key="j">{{ cell }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<style scoped>
.preview {
  border: 1px solid #222;
  border-radius: 8px;
  overflow: hidden;
}

.label {
  padding: 8px 12px;
  background: #1a1a1a;
  font-size: 12px;
  color: #888;
  border-bottom: 1px solid #222;
}

.table-wrap {
  overflow-x: auto;
  max-height: 500px;
  overflow-y: auto;
}

table {
  width: 100%;
  border-collapse: collapse;
  font-size: 13px;
  font-family: 'SF Mono', 'Fira Code', monospace;
}

th, td {
  padding: 6px 12px;
  text-align: left;
  border-bottom: 1px solid #1a1a1a;
  white-space: nowrap;
}

th {
  background: #141414;
  color: #aaa;
  font-weight: 500;
  position: sticky;
  top: 0;
}

td {
  color: #ccc;
}

tr:hover td {
  background: #1a1a1a;
}
</style>
