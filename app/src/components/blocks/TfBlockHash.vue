<template>
  <v-card-text>
    <BlockInput v-model="block.args.columns" type="text" label="Columns" hint="Comma-separated" density="compact" />
    <BlockInput v-model="block.args.algorithm" type="select" label="Algorithm" :items="['sha256', 'md5', 'crc32']" density="compact" />
  </v-card-text>
</template>
<script setup>
import { BlockInput } from '@statsim/block-editor'
defineProps({ block: Object })
</script>
