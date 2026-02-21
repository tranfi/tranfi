<script setup>
import { ref } from 'vue'

const props = defineProps({
  fileName: String
})

const emit = defineEmits(['file-loaded'])
const dragging = ref(false)

function handleDrop(e) {
  dragging.value = false
  const file = e.dataTransfer?.files[0]
  if (file) readFile(file)
}

function handleInput(e) {
  const file = e.target.files[0]
  if (file) readFile(file)
}

function readFile(file) {
  // Emit raw File object (JSEE streaming wraps it in ChunkedReader)
  emit('file-loaded', { file, name: file.name })
}
</script>

<template>
  <div
    class="dropzone"
    :class="{ dragging }"
    @dragover.prevent="dragging = true"
    @dragleave="dragging = false"
    @drop.prevent="handleDrop"
    @click="$refs.input.click()"
  >
    <input ref="input" type="file" hidden @change="handleInput">
    <span v-if="fileName">{{ fileName }}</span>
    <span v-else>Drop file or click to browse</span>
  </div>
</template>

<style scoped>
.dropzone {
  padding: 20px;
  border: 2px dashed #333;
  border-radius: 8px;
  text-align: center;
  cursor: pointer;
  font-size: 14px;
  color: #888;
  transition: border-color 0.2s;
}

.dropzone:hover,
.dropzone.dragging {
  border-color: #2563eb;
  color: #bbb;
}
</style>
