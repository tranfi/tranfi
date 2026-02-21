<template>
  <v-card-text>
    <BlockInput v-model="block.args.column" type="text" label="Column" density="compact" />
    <BlockInput v-model="block.args.from" type="text" label="From format" hint='e.g. %Y-%m-%d, epoch' monospace density="compact" />
    <BlockInput v-model="block.args.to" type="text" label="To format" hint='e.g. %s (epoch), %Y-%m-%d' monospace density="compact" />
  </v-card-text>
</template>
<script setup>
import { BlockInput } from '@statsim/block-editor'
defineProps({ block: Object })
</script>
