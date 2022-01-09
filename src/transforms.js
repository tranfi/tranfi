const ReadStream = require('filestream').read
const ParseStream = require('csv-parse')
const JSONStream = require('JSONStream')
const HTTPStream = require('stream-http')
const through2 = require('through2')

let FileLoader = class FileLoader {
  static inputType = 'text'

  static outputType = 'text'

  static type = 'loader'

  static initStream (params) {
    const stream = new ReadStream(params['File'], {chunkSize: params['Chunk size']})
    stream.setEncoding('utf8')
    // if (params['Encoding'] === 'UTF-8') {}
    return stream
  }

  constructor (id) {
    this.name = this.constructor.name
    this.inputs = [
      { name: 'File', type: 'file' },
      // { name: 'Encoding', type: 'categorical', options: ['UTF-8', 'Buffer'], default: 'UTF-8' },
      { name: 'Chunk size', type: 'int', default: 10000 },
    ]
    this.inputType = this.constructor.inputType
    this.outputType = this.constructor.outputType
    this.id = id
    this.expanded = false
    this.displayInputs = [0]
  }
}

let HTTPLoader = class HTTPLoader {
  static inputType = 'text'

  static outputType = 'text'

  static type = 'loader'

  static initStream (params) {
    return new Promise((resolve, reject) => {
      HTTPStream.get(params['URL'], function (stream) {
        stream.setEncoding('utf8')
        // if (params['Encoding'] === 'UTF-8') {}
        // stream.setEncoding(params['Encoding'] === 'UTF-8' ? 'utf8' : 'buffer')
        resolve(stream)
      })
    })
  }

  constructor (id) {
    this.name = this.constructor.name
    this.inputs = [
      { name: 'URL', type: 'string' },
      // { name: 'Encoding', type: 'categorical', options: ['UTF-8', 'Buffer'], default: 'UTF-8' },
    ]
    this.inputType = this.constructor.inputType
    this.outputType = this.constructor.outputType
    this.id = id
    this.expanded = false
    this.displayInputs = [0]
  }
}

let CSVParser = class CSVParser {
  static inputType = 'text'

  static outputType = 'object'

  static type = 'parser'

  static initStream (params) {
     const stream = ParseStream({
      'delimiter': params['Delimiter'],
      'cast': true,
      'columns': true,
      'comment': '#',
      'trim': params['Trim'],
      'relax_column_count': true,
      'skip_empty_lines': params['Skip empty lines'],
      'skip_lines_with_error': true
    })
    return stream
  }

  constructor (id) {
    this.name = this.constructor.name
    this.inputs = [
      { name: 'Delimiter', type: 'string', default: ',' },
      { name: 'Trim', type: 'bool', default: true },
      { name: 'Skip empty lines', type: 'bool', default: true }
    ]
    this.inputType = this.constructor.inputType
    this.outputType = this.constructor.outputType
    this.id = id
    this.expanded = false
    this.displayInputs = []
  }
}

let JSONParser = class JSONParser {
  static inputType = 'text'

  static outputType = 'object'

  static type = 'parser'

  static initStream (params) {
    const stream = JSONStream.parse(params['Path'])
    return stream
  }

  constructor (id) {
    this.name = this.constructor.name
    this.inputs = [
      { name: 'Path', type: 'string', default: '*' },
    ]
    this.inputType = this.constructor.inputType
    this.outputType = this.constructor.outputType
    this.id = id
    this.expanded = false
    this.displayInputs = []
  }
}

let TextOutput = class TextOutput {
  static inputType = 'text'

  static outputType = 'html'

  static type = 'output'

  static initStream (params) {
    const textOutput = document.getElementById('text-output')
    textOutput.style.display = 'block'
    textOutput.innerText = ''
    const stream = through2(function (chunk, enc, callback) {
      textOutput.innerText += chunk
      this.push(chunk)
      callback()
    })
    return stream
  }

  constructor (id) {
    this.name = this.constructor.name
    this.inputs = []
    this.inputType = this.constructor.inputType
    this.outputType = this.constructor.outputType
    this.id = id
    this.expanded = false
    this.displayInputs = []
  }
}
module.exports = {
  FileLoader,
  HTTPLoader,
  CSVParser,
  JSONParser,
  TextOutput
}


