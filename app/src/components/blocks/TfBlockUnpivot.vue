<template>
  <v-card-text>
    <BlockInput v-model="block.args.id_columns" type="text" label="ID columns" hint="Columns to keep as-is" density="compact" />
    <BlockInput v-model="block.args.value_columns" type="text" label="Value columns" hint="Columns to melt (empty = all others)" density="compact" />
  </v-card-text>
</template>
<script setup>
import { BlockInput } from '@statsim/block-editor'
defineProps({ block: Object })
</script>
