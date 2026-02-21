<template>
  <v-card-text>
    <BlockInput
      v-model="block.args.columns"
      type="text"
      label="Columns"
      hint='name=expr, e.g. bmi=col(weight)/col(height)^2'
      monospace
      density="compact"
    />
  </v-card-text>
</template>

<script setup>
import { BlockInput } from '@statsim/block-editor'
defineProps({ block: Object })
</script>
