<template>
  <v-card-text>
    <BlockInput v-model="block.args.mapping" type="text" label="Type mapping" hint="col:type, e.g. age:int,score:float" monospace density="compact" />
  </v-card-text>
</template>
<script setup>
import { BlockInput } from '@statsim/block-editor'
defineProps({ block: Object })
</script>
