<template>
  <v-card-text>
    <BlockInput
      v-model="block.args.n"
      type="number"
      label="Number of rows"
      density="compact"
    />
  </v-card-text>
</template>

<script setup>
import { BlockInput } from '@statsim/block-editor'
defineProps({ block: Object })
</script>
