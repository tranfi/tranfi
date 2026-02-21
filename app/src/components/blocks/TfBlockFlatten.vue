<template>
  <v-card-text>
    <BlockInput v-model="block.args.separator" type="text" label="Separator" hint='Default: .' density="compact" />
  </v-card-text>
</template>
<script setup>
import { BlockInput } from '@statsim/block-editor'
defineProps({ block: Object })
</script>
