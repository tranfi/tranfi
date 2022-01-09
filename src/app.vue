<template>
  <div>

  <it-drawer placement="left" v-model="modalAddTransforms">
    <nav class="panel">
      <!--
      <p class="panel-tabs">
        <a class="is-active">All</a>
        <a>Public</a>
        <a>Private</a>
      </p>
      -->
      <a class="panel-block" v-for="(Transform, i) in transforms" :key="i" @click="addTransform(Transform)">
        <span class="panel-icon">
          <i class="mdi mdi-import" aria-hidden="true" v-if="Transform.type === 'loader'"></i>
          <i class="mdi mdi-shape" aria-hidden="true" v-if="Transform.type === 'parser'"></i>
          <i class="mdi mdi-play-box-outline" aria-hidden="true" v-if="Transform.type === 'output'"></i>
        </span>
        {{ Transform.name }}
      </a>
    </nav>
  </it-drawer>

  <nav class="mainbar" role="navigation" aria-label="main navigation" style="background: #e6e6e6; position: fixed; width: 100%; top: 0; left: 0; z-index: 100">
    <div class="columns">
      <div class="column is-3 mainbar-left" v-if="!preview">
        <div class="navbar-brand">
          <span class="logo">Tranfi</span>
          <button class="button is-primary is-outlined" @click="modalAddTransforms = true">
            <span class="icon is-small">
              <i class="mdi mdi-playlist-plus" aria-hidden="true"></i>
            </span>
          </button>
        </div>
      </div>
      <div class="column mainbar-right" v-bind:class="{ 'is-9': !preview, 'is-12': preview }">
        <div class="navbar-start">
          <div class="navbar-item mainbar-title">
            <span v-if="preview">{{ title }}</span>
            <input class="input" type="text" v-model="title" placeholder="Title..." v-else>
          </div>
        </div>
        <div class="navbar-end">
          <div class="navbar-item">
            <div class="buttons">
              <a class="button is-light is-outlined mobile-flexible" v-if="pipeline.length">
                <span class="icon is-small">
                  <i class="mdi mdi-content-save" aria-hidden="true"></i>
                </span>
                <span class="desktop-only">
                  Save pipeline
                </span>
              </a>
              <a class="button is-light is-outlined preview-button mobile-flexible" @click="preview=true" v-if="!preview">
                <span class="icon is-small">
                  <i class="mdi mdi-eye" aria-hidden="true"></i>
                </span>
                <span class="desktop-only">
                  View
                </span>
              </a>
              <a class="button is-light is-outlined preview-button mobile-flexible" @click="preview=false" v-if="preview">
                <span class="icon is-small">
                  <i class="mdi mdi-pencil" aria-hidden="true"></i>
                </span>
                <span class="desktop-only">
                  Edit
                </span>
              </a>
            </div>
          </div>
        </div>
      </div>
    </div>
  </nav>

  <div class="columns" style="margin-top: 77px">
    <div class="pipeline column is-3" v-if="!preview">

      <draggable 
        v-model="pipeline" 
        @start="drag=true" 
        @end="drag=false" 
        item-key="id"
        handle=".card-header"
      >
        <template #item="{element}">
          <div>
            <div class="card" v-bind:class="{ active: element.expanded }">
              <header class="card-header">
                <p class="card-header-title">
                  {{ element.name }}
                </p>
                <button class="card-header-icon" aria-label="more options" v-on:click="element.expanded = !element.expanded">
                  <span class="icon">
                    <i class="mdi mdi-chevron-up" aria-hidden="true" v-if="element.expanded"></i>
                    <i class="mdi mdi-chevron-down" aria-hidden="true" v-else></i>
                  </span>
                </button>
              </header>
              <div class="card-content" >
                <ul>
                  <li v-for="(input, index) in element.inputs">
                    <vue-input v-bind:input="input"></vue-input>
                  </li>
                  <li v-if="element.inputs.length">
                    <div class="field">
                      <label v-bind:for="'disp' + element.id" class="is-size-7">Display on preview:</label>
                      <div class="control">
                        <div class="select is-multiple">
                          <select
                            v-model="element.displayInputs"
                            v-bind:id="'disp' + element.id"
                            multiple
                            size="3"
                          >
                            <option v-for="(input, index) in element.inputs" v-bind:value="index">
                              {{ input.name }}
                            </option>
                          </select>
                        </div>
                      </div>
                    </div>
                  </li>
                  <li>
                    <pre style="font-size: 10px;">{{ element }}</pre>
                  </li>
                </ul>
              </div>
              <footer class="card-footer">
                <a class="card-footer-item" @click="cloneTransform(element)">Clone</a>
                <a class="card-footer-item" @click="resetTransform(element)">Reset</a>
                <a class="card-footer-item" @click="deleteTransform(element)">Delete</a>
              </footer>
            </div>
            <p style="text-align: center; font-size: 11px; color: #AAA;">
              <span> ↓ </span>
              <small>{{ element.outputType }}</small>
            </p>
          </div>
        </template>
      </draggable>

      <button
        @click="modalAddTransforms = true"
        class="button is-primary is-outlined is-fullwidth" 
        style="margin-bottom: 10px; min-height: 45px;"
      >
        <span class="icon is-small">
          <i class="mdi mdi-playlist-plus" aria-hidden="true"></i>
        </span>
        <span>
          Add stream
        </span>
      </button>

    </div>

    <div class="column preview-content" v-bind:class="{ 'is-9': !preview, 'is-12': preview }">

      <nav class="breadcrumb has-arrow-separator" aria-label="breadcrumbs">
        <ul>
          <li v-for="transform in pipeline">&nbsp;{{ transform.name }}&nbsp;</li>
        </ul>
      </nav>

      <hr v-if="pipeline.length">

      <nav class="panel preview-panel" v-if="pipeline.length">
        <div class="panel-block" v-for="(input, index) in displayInputs">
          <vue-input v-bind:input="input"></vue-input>
        </div>
        <div class="panel-block" v-if="pipeline.length">
          <button class="button is-primary is-outlined is-fullwidth" @click="run(true)">
            <span class="icon is-small">
              <i class="mdi mdi-play-pause" aria-hidden="true"></i>
            </span>
            <span>
              Load batch
            </span>
          </button>
        </div>
        <div class="panel-block" v-if="pipeline.length">
          <button class="button is-primary is-large is-fullwidth" @click="run(false)">
            <span class="icon is-small">
              <i class="mdi mdi-play" aria-hidden="true"></i>
            </span>
            <span>
              Load all
            </span>
          </button>
        </div>
      </nav>

      <hr v-if="pipeline.length">

      <pre id="text-output" style="display: none;"></pre>

      <hr v-if="pipeline.length && (!loadedAllData)">

      <nav class="panel preview-panel" v-if="pipeline.length && (!loadedAllData)">
        <div class="panel-block" v-if="pipeline.length">
          <button class="button is-primary is-outlined is-fullwidth" @click="runMore">
            <span class="icon is-small">
              <i class="mdi mdi-play-pause" aria-hidden="true"></i>
            </span>
            <span>
              Load more
            </span>
          </button>
        </div>
      </nav>


      <!--
      <it-tag>Neutral</it-tag><it-tag>Neutral</it-tag><it-tag>Neutral</it-tag><it-tag>Neutral</it-tag>
      <it-button @click="showMessage('Warning')" type="warning">Warning message</it-button>
      <it-tabs box style="flex: 1">
        <it-tab title="Tab 1">First tab</it-tab>
        <it-tab title="Tab 2">Second tab</it-tab>
        <it-tab title="Tab 3" :disabled="disabledTab">Third tab</it-tab>
      </it-tabs>
      -->
    </div>
  </div>

  <!--
  <nav class="bottombar" role="navigation" style="background: none; position: fixed; width: 100%; bottom: 0; left: 0; z-index: 100">
    <div class="columns">
      <div class="column is-3" v-if="!preview"></div>
      <div class="column bottombar-right" v-bind:class="{ 'is-9': !preview, 'is-12': preview }" style="">
        <div class="buttons" v-if="pipeline.length">
          <button class="button is-primary" @click="run">
            <span class="icon is-small">
              <i class="mdi mdi-subdirectory-arrow-right" aria-hidden="true"></i>
            </span>
            <span>
              Run
            </span>
          </button>
        </div>
      </div>
    </div>
  </nav>
  -->

  </div>
</template>
<style src="./app.css"></style>
<script src="./app.js" scoped></script>
