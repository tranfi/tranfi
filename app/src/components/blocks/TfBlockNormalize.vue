<template>
  <v-card-text>
    <BlockInput v-model="block.args.columns" type="text" label="Columns" hint="comma-separated, e.g. price,score" monospace density="compact" />
    <BlockInput v-model="block.args.method" type="select" label="Method" :items="['minmax', 'zscore']" density="compact" />
  </v-card-text>
</template>

<script setup>
import { BlockInput } from '@statsim/block-editor'
defineProps({ block: Object })
</script>
