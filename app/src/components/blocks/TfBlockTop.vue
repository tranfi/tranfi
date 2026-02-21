<template>
  <v-card-text>
    <BlockInput v-model="block.args.n" type="number" label="N" density="compact" />
    <BlockInput v-model="block.args.column" type="text" label="Sort column" density="compact" />
    <BlockInput v-model="block.args.order" type="select" label="Order" :items="['desc', 'asc']" density="compact" />
  </v-card-text>
</template>
<script setup>
import { BlockInput } from '@statsim/block-editor'
defineProps({ block: Object })
</script>
