import { createApp } from 'vue'

import App from './app.vue'

import Equal from 'equal-vue'
import 'equal-vue/dist/style.css'

import bulmaInput from './bulma-input.vue'
import draggable from 'vuedraggable'

const app = createApp(App)
app.use(Equal)

app.component('vue-input', bulmaInput)
app.component('draggable', draggable)

app.mount('#tranfi-container')

