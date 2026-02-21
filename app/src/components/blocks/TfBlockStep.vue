<template>
  <v-card-text>
    <BlockInput v-model="block.args.column" type="text" label="Column" density="compact" />
    <BlockInput v-model="block.args.op" type="select" label="Operation" :items="['running-sum', 'running-avg', 'running-min', 'running-max', 'running-count', 'delta', 'lag', 'ratio']" density="compact" />
  </v-card-text>
</template>
<script setup>
import { BlockInput } from '@statsim/block-editor'
defineProps({ block: Object })
</script>
