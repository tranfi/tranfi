<template>
  <v-card-text>
    <BlockInput v-model="block.args.expr" type="expression" label="Validation rule" hint="Rows failing this expression are flagged" density="compact" />
  </v-card-text>
</template>
<script setup>
import { BlockInput } from '@statsim/block-editor'
defineProps({ block: Object })
</script>
