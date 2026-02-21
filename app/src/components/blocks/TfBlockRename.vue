<template>
  <v-card-text>
    <BlockInput
      v-model="block.args.mapping"
      type="text"
      label="Mapping"
      hint="old=new,old2=new2"
      density="compact"
    />
  </v-card-text>
</template>

<script setup>
import { BlockInput } from '@statsim/block-editor'
defineProps({ block: Object })
</script>
