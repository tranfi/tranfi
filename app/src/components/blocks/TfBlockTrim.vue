<template>
  <v-card-text>
    <BlockInput v-model="block.args.columns" type="text" label="Columns" hint="Columns to trim whitespace (empty = all)" density="compact" />
  </v-card-text>
</template>
<script setup>
import { BlockInput } from '@statsim/block-editor'
defineProps({ block: Object })
</script>
