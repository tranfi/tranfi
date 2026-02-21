<template>
  <v-card-text>
    <BlockInput v-model="block.args.n" type="number" :label="label" density="compact" />
  </v-card-text>
</template>
<script setup>
import { BlockInput } from '@statsim/block-editor'
defineProps({
  block: Object,
  label: { type: String, default: 'Number of rows' }
})
</script>
