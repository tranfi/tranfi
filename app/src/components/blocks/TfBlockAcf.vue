<template>
  <v-card-text>
    <BlockInput v-model="block.args.column" type="text" label="Column" density="compact" />
    <BlockInput v-model="block.args.lags" type="number" label="Max lags" density="compact" />
  </v-card-text>
</template>

<script setup>
import { BlockInput } from '@statsim/block-editor'
defineProps({ block: Object })
</script>
