<template>
  <v-card-text>
    <BlockInput v-model="block.args.size" type="number" label="Window size (rows)" density="compact" />
    <BlockInput v-model="block.args.aggs" type="text" label="Aggregations" hint="e.g. avg:price,sum:volume" monospace density="compact" />
  </v-card-text>
</template>
<script setup>
import { BlockInput } from '@statsim/block-editor'
defineProps({ block: Object })
</script>
