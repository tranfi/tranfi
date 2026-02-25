<template>
  <v-card-text>
    <BlockInput v-model="block.args.column" type="text" label="Column" density="compact" />
    <BlockInput v-model="block.args.alpha" type="number" label="Alpha (0-1)" hint="smoothing factor" density="compact" />
    <BlockInput v-model="block.args.result" type="text" label="Result column" density="compact" />
  </v-card-text>
</template>

<script setup>
import { BlockInput } from '@statsim/block-editor'
defineProps({ block: Object })
</script>
