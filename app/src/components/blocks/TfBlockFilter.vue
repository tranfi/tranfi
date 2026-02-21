<template>
  <v-card-text>
    <BlockInput
      v-model="block.args.expr"
      type="expression"
      label="Expression"
      hint='e.g. col(age) > 25'
      density="compact"
    />
  </v-card-text>
</template>

<script setup>
import { BlockInput } from '@statsim/block-editor'
defineProps({ block: Object })
</script>
