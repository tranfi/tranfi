<template>
  <v-card-text>
    <BlockInput v-model="block.args.column" type="text" label="Column" density="compact" />
    <BlockInput v-model="block.args.delimiter" type="text" label="Delimiter" density="compact" />
    <BlockInput v-model="block.args.names" type="text" label="New column names" hint="Comma-separated" density="compact" />
  </v-card-text>
</template>
<script setup>
import { BlockInput } from '@statsim/block-editor'
defineProps({ block: Object })
</script>
