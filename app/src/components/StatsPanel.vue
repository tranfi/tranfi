<script setup>
import { computed } from 'vue'

const props = defineProps({
  text: String
})

const rows = computed(() => {
  if (!props.text) return []
  return props.text.trim().split('\n')
})

const header = computed(() => rows.value[0]?.split(',') || [])
const body = computed(() => rows.value.slice(1).map(r => r.split(',')))
</script>

<template>
  <div class="stats" v-if="body.length">
    <div class="label">Stats</div>
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
.stats {
  border: 1px solid #1a2a1a;
  border-radius: 8px;
  overflow: hidden;
}

.label {
  padding: 8px 12px;
  background: #0f1a0f;
  font-size: 12px;
  color: #6b8;
  border-bottom: 1px solid #1a2a1a;
}

.table-wrap {
  overflow-x: auto;
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
  border-bottom: 1px solid #0f1a0f;
  white-space: nowrap;
}

th {
  background: #0a140a;
  color: #6b8;
  font-weight: 500;
}

td {
  color: #9cb;
}
</style>
