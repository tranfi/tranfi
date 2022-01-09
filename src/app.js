const through2 = require('through2')
const transforms = require('./transforms')

async function run (pipeline, isBatch) {
  let readStream
  let writeStream

  for (const transform of pipeline) {
    // Convert inputs to params object
    const params = {}
    transform.inputs.forEach(i => {
      params[i.name] = i.type === 'file' ? i.file : i.value
    })
    // Init stream
    let nextStream = await transforms[transform.name].initStream(params)
    if (typeof readStream === 'undefined') {
      readStream = writeStream = nextStream
      if ('File' in params) {
        let size = params['File'].size
        let progressBytes = 0
        let progressBar = document.getElementById('progress')
        progressBar.style.display = 'initial'
        const progressStream = through2(function (chunk, enc, callback) {
          progressBytes += chunk.length
          let progressPercents = ((progressBytes / size) * 100).toFixed(1)
          progressBar.style.width = progressPercents + '%'
          this.push(chunk)
          callback()
        })
        writeStream = writeStream.pipe(progressStream)
      }
    } else {
      writeStream = writeStream.pipe(nextStream) //, {end: false})
    }
  }

  writeStream.on('data', (piece) => {
    if (isBatch) {
      console.log('Pausing stream')
      readStream.pause()
    }
  })

  return {readStream, writeStream}
}

function resetInputs (inputs) {
  inputs.forEach(input => {
    if (input.default) {
      input.value = input.default
    } else {
      switch (input.type) {
        case 'int':
        case 'float':
        case 'number':
          input.value = 0
          break
        case 'string':
        case 'text':
          input.value = ''
          break
        case 'color':
          input.value = '#000000'
          break
        case 'categorical':
        case 'select':
          input.value = input.options ? input.options[0] : ''
          break
        case 'bool':
        case 'checkbox':
          input.value = false
          break
        case 'file':
          input.file = null
          input.value = ''
          break
        case 'group':
          resetInputs(input.elements)
          break
        default:
          input.value = ''
      }
    }
  })
}

let readStream = null
let writeStream = null
export default {
  components: {
//    draggable,
//    'vue-input': bulmaInput
  },
  data() {
    return {
      drag: false,
      preview: false,
      loadedAllData: true,
      title: '',
      showResults: false,
      modalAddTransforms: false,
      counter: 0,
      transforms: transforms,
      pipeline: []
    }
  },
  methods: {
    getTransformIndex (t) {
      return this.pipeline.findIndex((el) => el.id === t.id)
    },
    cloneTransform (t) {
      const i = this.getTransformIndex(t)
      const newTransform = JSON.parse(JSON.stringify(t))
      newTransform.id = this.counter
      this.counter += 1
      this.pipeline.splice(i + 1, 0, newTransform)
    },
    resetTransform (t) {
      resetInputs(t.inputs)
    },
    deleteTransform (t) {
      const i = this.getTransformIndex(t)
      this.pipeline.splice(i, 1)
    },
    showLoading (p) {
      this.$Loading.update(p)
    },
    addTransform (Transform) {
      const newTransform = new Transform(this.counter)
      this.resetTransform(newTransform)
      this.pipeline.push(newTransform)
      this.modalAddTransforms = false
      this.counter += 1
    },
    showMessage (type) {
      switch (type) {
        case 'success':
          this.$Message.success({ text: 'Success message!' })
          break
        case 'danger':
          this.$Message.danger({ text: 'Danger message!' })
          break
        case 'warning':
          this.$Message.warning({ text: 'Success message!' })
          break
        default:
          this.$Message({ text: 'Primary message!' })
          break
      }
    },
    run (isBatch) {
      run(this.pipeline, isBatch).then(streams => {
        this.loadedAllData = false
        readStream = streams.readStream
        readStream.on('end', function () {
          this.loadedAllData = true
          console.log('End of stream')
          let progressBar = document.getElementById('progress')
          progressBar.style.display = 'none'
        })
      })
    },
    runMore () {
      readStream.resume()
    }
  },
  watch: {
    // pipeline: {
    //   handler (pipeline) {
    //     this.env.data.inputs = pipeline.map((v, i) => ({type: 'int', name: v.name}))
    //   },
    //   deep: true
    // }
  },
  computed: {
    // a computed getter
    displayInputs() {
      const inputs = []
      this.pipeline.forEach(transform => {
        transform.displayInputs.forEach(i => {
          inputs.push(transform.inputs[i])
        })
      })
      return inputs
    }
  },
  mounted () {
    // const tranfi = this
    // this.env = new JSEE({
    //   model: {
    //     code: function run (params) {
    //       tranfi.run(params)
    //     },
    //     worker: false
    //   },
    //   design: {
    //     grid: [6, 12]
    //   },
    //   inputs: []
    // }, "#jsee-container")
  }
}

