<template>
  <v-card-text>
    <BlockInput v-model="block.args.column" type="text" label="Column" hint="Column to count frequencies" density="compact" />
  </v-card-text>
</template>
<script setup>
import { BlockInput } from '@statsim/block-editor'
defineProps({ block: Object })
</script>
