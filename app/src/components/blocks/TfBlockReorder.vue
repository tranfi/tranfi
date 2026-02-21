<template>
  <v-card-text>
    <BlockInput v-model="block.args.columns" type="text" label="Column order" hint="Comma-separated column names in desired order" density="compact" />
  </v-card-text>
</template>
<script setup>
import { BlockInput } from '@statsim/block-editor'
defineProps({ block: Object })
</script>
