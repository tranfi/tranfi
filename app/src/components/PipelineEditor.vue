<script setup>
defineProps({
  modelValue: String,
  disabled: Boolean
})

const emit = defineEmits(['update:modelValue', 'run'])

function onKeydown(e) {
  if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
    emit('run')
  }
}
</script>

<template>
  <div class="editor">
    <input
      type="text"
      :value="modelValue"
      @input="emit('update:modelValue', $event.target.value)"
      @keydown="onKeydown"
      :disabled="disabled"
      placeholder="csv | filter &quot;col(age) > 25&quot; | csv"
      spellcheck="false"
    >
    <span class="hint">Ctrl+Enter to run</span>
  </div>
</template>

<style scoped>
.editor {
  position: relative;
}

.editor input {
  width: 100%;
  padding: 12px 16px;
  background: #1a1a1a;
  border: 1px solid #333;
  border-radius: 8px;
  color: #e0e0e0;
  font-family: 'SF Mono', 'Fira Code', monospace;
  font-size: 15px;
}

.editor input:focus {
  outline: none;
  border-color: #2563eb;
}

.editor input:disabled {
  opacity: 0.5;
}

.hint {
  position: absolute;
  right: 12px;
  top: 50%;
  transform: translateY(-50%);
  font-size: 11px;
  color: #555;
}
</style>
