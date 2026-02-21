<template>
  <v-card-text>
    <BlockInput v-model="block.args.mapping" type="text" label="Defaults" hint='col=value, e.g. age=0,name="unknown"' monospace density="compact" />
  </v-card-text>
</template>
<script setup>
import { BlockInput } from '@statsim/block-editor'
defineProps({ block: Object })
</script>
