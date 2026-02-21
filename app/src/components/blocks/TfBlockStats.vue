<template>
  <v-card-text>
    <BlockInput
      v-model="block.args.stats"
      type="text"
      label="Stats"
      hint="Comma-separated stats (leave empty for all)"
      density="compact"
    />
  </v-card-text>
</template>

<script setup>
import { BlockInput } from '@statsim/block-editor'
defineProps({ block: Object })
</script>
