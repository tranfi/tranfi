<template>
  <v-card-text>
    <BlockInput v-model="block.args.column" type="text" label="Column" density="compact" />
    <BlockInput v-model="block.args.result" type="text" label="Result column" hint="default: column_encoded" density="compact" />
  </v-card-text>
</template>

<script setup>
import { BlockInput } from '@statsim/block-editor'
defineProps({ block: Object })
</script>
