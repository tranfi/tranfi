import { defineConfig } from 'vite'
import { resolve } from 'path'
import vue from '@vitejs/plugin-vue'
import vuetify from 'vite-plugin-vuetify'

export default defineConfig(({ mode }) => ({
  base: process.env.VITE_BASE || '/',
  plugins: [
    vue(),
    vuetify({ autoImport: true })
  ],
  build: {
    target: 'esnext'
  },
  resolve: {
    alias: {
      '@statsim/block-editor': resolve(__dirname, 'src/lib/block-editor/index.js')
    },
    dedupe: ['vue', 'vuetify', 'vuedraggable'],
    preserveSymlinks: true
  },
  optimizeDeps: {
    exclude: ['tranfi'],
    include: ['vuedraggable']
  }
}))
