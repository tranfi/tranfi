<template>
  <v-checkbox
    v-if="type === 'checkbox'"
    :model-value="modelValue"
    @update:model-value="$emit('update:modelValue', $event)"
    :label="label"
    :hint="hint"
    :persistent-hint="!!hint"
    :density="density"
    hide-details="auto"
  ></v-checkbox>

  <v-select
    v-else-if="type === 'select'"
    :model-value="modelValue"
    @update:model-value="$emit('update:modelValue', $event)"
    :label="label"
    :items="items"
    :hint="hint"
    :persistent-hint="!!hint"
    :density="density"
  ></v-select>

  <v-textarea
    v-else-if="type === 'textarea'"
    :model-value="modelValue"
    @update:model-value="$emit('update:modelValue', $event)"
    :label="label"
    :hint="hint"
    :persistent-hint="!!hint"
    :density="density"
    :style="monospace ? { fontFamily: 'monospace' } : {}"
    auto-grow
    rows="2"
  ></v-textarea>

  <v-text-field
    v-else-if="type === 'number'"
    :model-value="modelValue"
    @update:model-value="$emit('update:modelValue', Number($event))"
    :label="label"
    :hint="hint"
    :persistent-hint="!!hint"
    :density="density"
    type="number"
  ></v-text-field>

  <v-text-field
    v-else
    :model-value="modelValue"
    @update:model-value="$emit('update:modelValue', $event)"
    :label="label"
    :hint="hint"
    :persistent-hint="!!hint"
    :density="density"
    :style="monospace || type === 'expression' ? { fontFamily: 'monospace' } : {}"
  ></v-text-field>
</template>

<script setup>
defineProps({
  modelValue: { type: [String, Number, Boolean, Array], default: '' },
  type: { type: String, default: 'text' },
  label: { type: String, default: '' },
  items: { type: Array, default: () => [] },
  hint: { type: String, default: '' },
  monospace: { type: Boolean, default: false },
  density: { type: String, default: 'compact' }
})

defineEmits(['update:modelValue'])
</script>
