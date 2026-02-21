<template>
  <v-card-text>
    <BlockInput v-model="block.args.file" type="text" label="Lookup file" hint="Path or URL to CSV/JSONL" density="compact" />
    <BlockInput v-model="block.args.key" type="text" label="Join key" hint="Column name to join on" density="compact" />
    <BlockInput v-model="block.args.type" type="select" label="Join type" :items="['left', 'inner', 'outer']" density="compact" />
  </v-card-text>
</template>
<script setup>
import { BlockInput } from '@statsim/block-editor'
defineProps({ block: Object })
</script>
