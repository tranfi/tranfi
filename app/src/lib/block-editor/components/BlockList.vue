<style scoped>
.add-block-here {
  height: 10px;
  width: 100%;
  background-color: white;
  margin: 2px 0 0 0;
  padding: 2px;
  line-height: 6px;
  font-size: 10px;
  display: block;
  text-align: center;
  border-radius: 2px;
  color: #EEE;
  cursor: pointer;
}
.add-block-here:hover {
  background-color: rgb(241, 241, 241);
  color: rgb(114, 114, 114);
}
.block-menu-list {
  max-height: 70vh;
  overflow-y: auto;
}
.block-menu-subheader {
  font-size: 11px;
  font-weight: 600;
  color: #666;
  padding: 6px 16px 2px;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}
.block-menu-subheader:not(:first-child) {
  border-top: 1px solid #eee;
  margin-top: 4px;
  padding-top: 8px;
}
.block-menu-item {
  min-height: 32px !important;
}
.block-menu-item .v-list-item-title {
  font-size: 13px !important;
  display: flex;
  align-items: center;
  gap: 6px;
}
.mem-icon {
  margin-left: auto;
}
.block-color-swatch {
  display: inline-block;
  width: 10px;
  height: 10px;
  border-radius: 2px;
  flex-shrink: 0;
}
</style>

<template>
  <div>
    <draggable
      :model-value="modelValue"
      @update:model-value="$emit('update:modelValue', $event)"
      :group="group"
      :item-key="itemKey"
      :handle="'.' + handleClass"
    >
      <template #item="{ element, index }">
        <v-row dense>
          <v-col cols="12" style="padding-bottom: 0 !important; padding-top: 0 !important;">
            <slot name="item" :block="element" :index="index"></slot>
            <v-menu v-if="groupedBlockTypes.length && index < modelValue.length - 1">
              <template v-slot:activator="{ props }">
                <a v-bind="props" class="add-block-here"> + </a>
              </template>
              <v-list class="block-menu-list" density="compact">
                <template v-for="group in groupedBlockTypes" :key="group.category">
                  <div class="block-menu-subheader">{{ group.label }}</div>
                  <v-list-item
                    class="block-menu-item"
                    v-for="bt in group.items"
                    :key="bt.typeCode"
                    :value="bt.typeCode"
                    @click="$emit('add', { typeCode: bt.typeCode, index })"
                  >
                    <v-list-item-title>
                      <span class="block-color-swatch" :style="{ background: bt.color }"></span>
                      <span>{{ bt.name }}</span>
                      <v-icon
                        v-if="bt.memoryTier && memoryTierInfo[bt.memoryTier]"
                        class="mem-icon"
                        size="12"
                        color="rgba(0,0,0,0.25)"
                        :icon="memoryTierInfo[bt.memoryTier].icon"
                        :title="memoryTierInfo[bt.memoryTier].label"
                      />
                    </v-list-item-title>
                  </v-list-item>
                </template>
              </v-list>
            </v-menu>
          </v-col>
        </v-row>
      </template>
    </draggable>

    <div v-if="groupedBlockTypes.length" style="margin-top: 15px;">
      <v-menu>
        <template v-slot:activator="{ props }">
          <v-btn
            id="btn-add-block"
            v-bind="props"
            size="small"
            variant="outlined"
            block
            style="border: 1px dashed #CCC; height: 48px"
          >
            <v-icon>mdi-plus</v-icon>
            Add block
          </v-btn>
        </template>
        <v-list class="block-menu-list" density="compact">
          <template v-for="group in groupedBlockTypes" :key="group.category">
            <div class="block-menu-subheader">{{ group.label }}</div>
            <v-list-item
              class="block-menu-item"
              v-for="bt in group.items"
              :key="bt.typeCode"
              :value="bt.typeCode"
              @click="$emit('add', { typeCode: bt.typeCode })"
            >
              <v-list-item-title>
                <span class="block-color-swatch" :style="{ background: bt.color }"></span>
                <span>{{ bt.name }}</span>
                <v-icon
                  v-if="bt.memoryTier && memoryTierInfo[bt.memoryTier]"
                  class="mem-icon"
                  size="12"
                  color="rgba(0,0,0,0.25)"
                  :icon="memoryTierInfo[bt.memoryTier].icon"
                  :title="memoryTierInfo[bt.memoryTier].label"
                />
              </v-list-item-title>
            </v-list-item>
          </template>
        </v-list>
      </v-menu>
    </div>
  </div>
</template>

<script setup>
import draggable from 'vuedraggable'

defineProps({
  modelValue: { type: Array, required: true },
  blockTypes: { type: Array, default: () => [] },
  groupedBlockTypes: { type: Array, default: () => [] },
  memoryTierInfo: { type: Object, default: () => ({}) },
  itemKey: { type: String, default: 'id' },
  handleClass: { type: String, default: 'handle' },
  group: { type: String, default: 'blocks' }
})

defineEmits(['update:modelValue', 'add'])
</script>
