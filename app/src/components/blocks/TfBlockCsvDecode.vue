<template>
  <v-card-text>
    <BlockInput
      v-model="block.args.delimiter"
      type="text"
      label="Delimiter"
      density="compact"
    />
    <BlockInput
      v-model="block.args.header"
      type="checkbox"
      label="Has header row"
      density="compact"
    />
  </v-card-text>
</template>

<script setup>
import { BlockInput } from '@statsim/block-editor'
defineProps({ block: Object })
</script>
