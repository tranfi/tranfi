<style scoped>
.handle {
  cursor: move;
  position: absolute;
  left: 0;
  top: 15px;
  font-size: 18px;
  color: rgba(0,0,0,0.3);
}
.handle:hover {
  color: black;
}
.v-toolbar-title {
  margin-top: -2px;
}
.v-toolbar-subtitle {
  position: absolute;
  left: 17px;
  bottom: 2px;
  font-size: 8px;
  color: rgba(0,0,0,0.4);
}
.block-badge {
  text-align: right;
  font-size: 11px;
  opacity: 0.6;
  background: white;
  display: inline-block;
  border-radius: 30px;
  padding: 1px 5px;
}
</style>

<template>
  <v-card class="mx-auto">
    <v-toolbar
      density="compact"
      :style="{ background: color }"
      @dblclick="$emit('toggle')"
    >
      <span :class="[handleClass, 'mdi', 'mdi-drag-vertical']"></span>
      <v-toolbar-title class="text-h6">
        {{ title }}
      </v-toolbar-title>
      <div v-if="subtitle" class="v-toolbar-subtitle">
        {{ subtitle.length > 30 ? subtitle.substring(0, 30) + '...' : subtitle }}
      </div>
      <div v-if="badge" class="block-badge">{{ badge }}</div>
      <slot name="toolbar-append"></slot>
      <v-btn
        size="small"
        :icon="minimized ? 'mdi-arrow-expand-vertical' : 'mdi-arrow-collapse-vertical'"
        @click="$emit('toggle')"
      ></v-btn>
      <template v-slot:append>
        <v-menu>
          <template v-slot:activator="{ props }">
            <v-btn icon="mdi-dots-vertical" v-bind="props"></v-btn>
          </template>
          <v-list>
            <slot name="menu-prepend"></slot>
            <v-list-item key="0" @click="$emit('move-to-top')">
              <v-list-item-title>
                <v-icon size="x-small" color="#DDD">mdi-arrow-up</v-icon>
                Move to top
              </v-list-item-title>
            </v-list-item>
            <v-list-item key="1" @click="$emit('clone')">
              <v-list-item-title>
                <v-icon size="x-small" color="#DDD">mdi-content-copy</v-icon>
                Clone block
              </v-list-item-title>
            </v-list-item>
            <v-list-item key="2" @click="$emit('remove')">
              <v-list-item-title>
                <v-icon size="x-small" color="#DDD">mdi-delete</v-icon>
                Delete
              </v-list-item-title>
            </v-list-item>
          </v-list>
        </v-menu>
      </template>
    </v-toolbar>

    <template v-if="!minimized">
      <slot></slot>
    </template>
  </v-card>
</template>

<script setup>
defineProps({
  title: { type: String, default: '' },
  subtitle: { type: String, default: '' },
  color: { type: String, default: '#ababab' },
  minimized: { type: Boolean, default: false },
  handleClass: { type: String, default: 'handle' },
  badge: { type: [String, Number], default: '' }
})

defineEmits(['toggle', 'remove', 'clone', 'move-to-top'])
</script>
