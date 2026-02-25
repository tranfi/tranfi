<template>
  <v-card-text>
    <BlockInput v-model="block.args.ratio" type="number" label="Train ratio (0-1)" hint="0.8 = 80% train" density="compact" />
    <BlockInput v-model="block.args.seed" type="number" label="Random seed" density="compact" />
  </v-card-text>
</template>

<script setup>
import { BlockInput } from '@statsim/block-editor'
defineProps({ block: Object })
</script>
