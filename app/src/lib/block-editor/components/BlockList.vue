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
            <v-menu v-if="blockTypes.length && index < modelValue.length - 1">
              <template v-slot:activator="{ props }">
                <a v-bind="props" class="add-block-here"> + </a>
              </template>
              <v-list>
                <v-list-item
                  v-for="bt in blockTypes"
                  :key="bt.typeCode"
                  :value="bt.typeCode"
                  @click="$emit('add', { typeCode: bt.typeCode, index })"
                >
                  <v-list-item-title>
                    <v-icon size="x-small" color="#DDD">mdi-plus</v-icon>
                    {{ bt.name }}
                  </v-list-item-title>
                </v-list-item>
              </v-list>
            </v-menu>
          </v-col>
        </v-row>
      </template>
    </draggable>

    <div v-if="blockTypes.length" style="margin-top: 15px;">
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
        <v-list>
          <v-list-item
            class="menu-add-block"
            v-for="bt in blockTypes"
            :key="bt.typeCode"
            :value="bt.typeCode"
            @click="$emit('add', { typeCode: bt.typeCode })"
          >
            <v-list-item-title>
              <v-icon size="x-small" color="#DDD">mdi-plus</v-icon>
              {{ bt.name }}
            </v-list-item-title>
          </v-list-item>
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
  itemKey: { type: String, default: 'id' },
  handleClass: { type: String, default: 'handle' },
  group: { type: String, default: 'blocks' }
})

defineEmits(['update:modelValue', 'add'])
</script>
