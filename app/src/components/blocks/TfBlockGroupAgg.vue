<template>
  <v-card-text>
    <BlockInput v-model="block.args.keys" type="text" label="Group keys" hint="Comma-separated key columns" density="compact" />
    <BlockInput v-model="block.args.aggs" type="text" label="Aggregations" hint="e.g. sum:amount,avg:score,count" monospace density="compact" />
  </v-card-text>
</template>
<script setup>
import { BlockInput } from '@statsim/block-editor'
defineProps({ block: Object })
</script>
