<template>
  <v-card-text>
    <BlockInput v-model="block.args[field]" type="text" :label="label" :hint="hint" :density="density" />
  </v-card-text>
</template>
<script setup>
import { BlockInput } from '@statsim/block-editor'
defineProps({
  block: Object,
  field: { type: String, default: 'columns' },
  label: { type: String, default: 'Columns' },
  hint: { type: String, default: 'Comma-separated column names' },
  density: { type: String, default: 'compact' }
})
</script>
