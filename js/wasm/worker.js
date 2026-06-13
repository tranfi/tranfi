"use strict"

const createTranfi = require('./index.js')

const CHAN_MAIN = 0
const CHAN_ERRORS = 1
const CHAN_STATS = 2
const CHAN_SAMPLES = 3
const DEFAULT_CHUNK_SIZE = 64 * 1024

function textEncoder() {
  return new TextEncoder()
}

function textDecoder() {
  return new TextDecoder()
}

function isArrayBuffer(value) {
  return value instanceof ArrayBuffer || Object.prototype.toString.call(value) === '[object ArrayBuffer]'
}

function asBytes(value) {
  if (value === undefined || value === null) return new Uint8Array(0)
  if (value instanceof Uint8Array) return value
  if (isArrayBuffer(value)) return new Uint8Array(value)
  if (ArrayBuffer.isView(value)) return new Uint8Array(value.buffer, value.byteOffset, value.byteLength)
  if (typeof value === 'string') return textEncoder().encode(value)
  return textEncoder().encode(String(value))
}

function ownedBytes(value) {
  var view = asBytes(value)
  var out = new Uint8Array(view.byteLength)
  out.set(view)
  return out
}

function concatUint8(chunks) {
  var total = chunks.reduce(function(sum, chunk) { return sum + chunk.length }, 0)
  var out = new Uint8Array(total)
  var offset = 0
  for (var i = 0; i < chunks.length; i++) {
    out.set(chunks[i], offset)
    offset += chunks[i].length
  }
  return out
}

function makeResult(output, errors, stats, samples) {
  return {
    output: output || new Uint8Array(0),
    errors: errors || new Uint8Array(0),
    stats: stats || new Uint8Array(0),
    samples: samples || new Uint8Array(0),
    get outputText() { return textDecoder().decode(this.output) },
    get statsText() { return textDecoder().decode(this.stats) }
  }
}

function normalizeMessage(event) {
  return event && event.data !== undefined ? event.data : event
}

function postMessage(endpoint, msg, transfers) {
  endpoint.postMessage(msg, transfers && transfers.length ? transfers : undefined)
}

function addWorkerMessageListener(worker, fn) {
  if (worker && typeof worker.addEventListener === 'function') {
    var listener = function(event) { fn(normalizeMessage(event)) }
    worker.addEventListener('message', listener)
    return function() { worker.removeEventListener('message', listener) }
  }
  if (worker && typeof worker.on === 'function') {
    worker.on('message', fn)
    return function() {
      if (typeof worker.off === 'function') worker.off('message', fn)
      else if (typeof worker.removeListener === 'function') worker.removeListener('message', fn)
    }
  }
  throw new TypeError('worker must be a Web Worker or worker_threads Worker')
}

function defaultServerEndpoint() {
  if (typeof require === 'function') {
    try {
      var wt = require('worker_threads')
      if (!wt.isMainThread && wt.parentPort) {
        return {
          postMessage: function(msg, transfers) { wt.parentPort.postMessage(msg, transfers || []) },
          onMessage: function(fn) {
            wt.parentPort.on('message', fn)
            return function() { wt.parentPort.off('message', fn) }
          }
        }
      }
    } catch (_) {}
  }
  if (typeof self !== 'undefined' && typeof self.postMessage === 'function') {
    return {
      postMessage: function(msg, transfers) { self.postMessage(msg, transfers || []) },
      onMessage: function(fn) {
        var listener = function(event) { fn(normalizeMessage(event)) }
        self.addEventListener('message', listener)
        return function() { self.removeEventListener('message', listener) }
      }
    }
  }
  throw new Error('no Worker endpoint available')
}

function progressSnapshot(job, phase, finished) {
  return {
    bytesIn: job.bytesIn,
    bytesOut: job.bytesOut,
    chunksIn: job.chunksIn,
    chunksOut: job.chunksOut,
    phase: phase,
    finished: finished ? true : false
  }
}

async function runWorkerServer(endpoint) {
  endpoint = endpoint || defaultServerEndpoint()
  var tf = await createTranfi()
  var jobs = Object.create(null)

  function send(msg, transfers) {
    postMessage(endpoint, msg, transfers)
  }

  function fail(id, err) {
    send({ id: id, type: 'error', error: String(err && err.message ? err.message : err || 'unknown error') })
  }

  function requireJob(id) {
    var job = jobs[id]
    if (!job) throw new Error('unknown or cancelled worker pipeline')
    if (job.cancelled) throw new Error('worker pipeline cancelled')
    return job
  }

  function sendProgress(job, phase, finished) {
    send({ id: job.id, type: 'progress', progress: progressSnapshot(job, phase, finished) })
  }

  function drainMain(job) {
    for (;;) {
      var chunk = tf.pullChunk(job.handle, CHAN_MAIN, job.chunkSize)
      if (chunk.length === 0) break
      job.bytesOut += chunk.length
      job.chunksOut += 1
      send({ id: job.id, type: 'output', channel: CHAN_MAIN, chunk: chunk }, [chunk.buffer])
      sendProgress(job, 'pull', false)
    }
  }

  function startJob(msg) {
    if (!msg || msg.id === undefined || msg.id === null) throw new Error('worker message missing id')
    if (jobs[msg.id]) throw new Error('worker pipeline id already exists')
    var options = msg.options || {}
    var chunkSize = options.chunkSize || DEFAULT_CHUNK_SIZE
    if (chunkSize <= 0) throw new Error('chunkSize must be positive')
    var planJson = tf.compileDsl(msg.dsl || '')
    if (typeof tf.prepareNativePlan === 'function') {
      planJson = tf.prepareNativePlan(planJson, {
        allowBlocking: options.allowBlocking === true,
        memory: options.memory,
        spillDir: options.spillDir
      })
    }
    var handle = tf.createPipeline(planJson)
    var job = {
      id: msg.id,
      handle: handle,
      chunkSize: chunkSize,
      bytesIn: 0,
      bytesOut: 0,
      chunksIn: 0,
      chunksOut: 0,
      cancelled: false
    }
    jobs[msg.id] = job
    sendProgress(job, 'start', false)
    return job
  }

  function pushJob(job, chunk) {
    var bytes = asBytes(chunk)
    tf.push(job.handle, bytes)
    job.bytesIn += bytes.length
    job.chunksIn += 1
    sendProgress(job, 'push', false)
    drainMain(job)
  }

  function finishJob(job) {
    for (;;) {
      var done = tf.finishStep(job.handle)
      sendProgress(job, 'finish', false)
      drainMain(job)
      if (done) break
      if (job.cancelled) throw new Error('worker pipeline cancelled')
    }
    var errors = tf.pull(job.handle, CHAN_ERRORS)
    var stats = tf.pull(job.handle, CHAN_STATS)
    var samples = tf.pull(job.handle, CHAN_SAMPLES)
    sendProgress(job, 'done', true)
    send({
      id: job.id,
      type: 'done',
      progress: progressSnapshot(job, 'done', true),
      errors: errors,
      stats: stats,
      samples: samples
    }, [errors.buffer, stats.buffer, samples.buffer])
    tf.free(job.handle)
    delete jobs[job.id]
  }

  async function handle(msg) {
    msg = normalizeMessage(msg)
    if (!msg || !msg.type) return
    if (msg.type === 'start') {
      var job = startJob(msg)
      send({ id: msg.id, type: 'started' })
      return
    }
    if (msg.type === 'push') {
      var pushTarget = requireJob(msg.id)
      pushJob(pushTarget, msg.chunk)
      send({ id: msg.id, type: 'pushed' })
      return
    }
    if (msg.type === 'finish') {
      finishJob(requireJob(msg.id))
      return
    }
    if (msg.type === 'run') {
      var runJob = startJob(msg)
      var options = msg.options || {}
      var data = asBytes(msg.data)
      var chunkSize = runJob.chunkSize
      for (var i = 0; i < data.length; i += chunkSize) {
        if (runJob.cancelled) throw new Error('worker pipeline cancelled')
        pushJob(runJob, data.subarray(i, Math.min(i + chunkSize, data.length)))
      }
      finishJob(runJob)
      return
    }
    if (msg.type === 'cancel') {
      var cancelJob = jobs[msg.id]
      if (cancelJob) {
        cancelJob.cancelled = true
        tf.free(cancelJob.handle)
        delete jobs[msg.id]
      }
      send({ id: msg.id, type: 'cancelled' })
    }
  }

  endpoint.onMessage(function(msg) {
    Promise.resolve(handle(msg)).catch(function(err) {
      var id = msg && msg.id !== undefined ? msg.id : null
      if (id !== null && jobs[id]) {
        try { tf.free(jobs[id].handle) } catch (_) {}
        delete jobs[id]
      }
      fail(id, err)
    })
  })
}

function createWorkerClient(workerOrUrl, options) {
  options = options || {}
  var worker = workerOrUrl
  var ownsWorker = options.terminateOnDispose === true
  if ((typeof workerOrUrl === 'string' || (typeof URL !== 'undefined' && workerOrUrl instanceof URL)) && typeof Worker !== 'undefined') {
    worker = new Worker(workerOrUrl, options.workerOptions)
    ownsWorker = true
  }
  if (!worker || typeof worker.postMessage !== 'function') throw new TypeError('worker must be a Worker instance or URL')

  var nextId = 1
  var jobs = new Map()
  var transfer = options.transfer !== false

  function send(msg, transfers) {
    worker.postMessage(msg, transfer && transfers && transfers.length ? transfers : undefined)
  }

  function rejectWaiters(state, err) {
    var waiters = state.waiters.splice(0)
    for (var i = 0; i < waiters.length; i++) waiters[i].reject(err)
  }

  function resolveWaiter(state, msg) {
    for (var i = 0; i < state.waiters.length; i++) {
      if (state.waiters[i].types.indexOf(msg.type) !== -1) {
        var waiter = state.waiters.splice(i, 1)[0]
        waiter.resolve(msg)
        return true
      }
    }
    return false
  }

  function waitFor(state, types) {
    return new Promise(function(resolve, reject) {
      state.waiters.push({ types: types, resolve: resolve, reject: reject })
    })
  }

  function abortState(state) {
    if (state.done || state.cancelled) return
    state.cancelled = true
    try { send({ id: state.id, type: 'cancel' }) } catch (_) {}
    rejectWaiters(state, new Error('worker pipeline cancelled'))
  }

  function onMessage(raw) {
    var msg = normalizeMessage(raw)
    if (!msg || msg.id === undefined || msg.id === null) return
    var state = jobs.get(msg.id)
    if (!state) return
    if (msg.type === 'output') {
      var chunk = ownedBytes(msg.chunk)
      if (state.onOutput) state.outputCallbacks.push(Promise.resolve().then(function() { return state.onOutput(chunk) }))
      if (state.collectOutput) state.outputChunks.push(chunk)
      return
    }
    if (msg.type === 'progress') {
      if (state.onProgress) state.progressCallbacks.push(Promise.resolve().then(function() { return state.onProgress(msg.progress) }))
      return
    }
    if (msg.type === 'error') {
      var err = new Error(msg.error || 'worker pipeline failed')
      state.done = true
      rejectWaiters(state, err)
      return
    }
    if (msg.type === 'done') {
      state.done = true
      resolveWaiter(state, msg)
      return
    }
    resolveWaiter(state, msg)
  }

  var removeListener = addWorkerMessageListener(worker, onMessage)

  async function runChunks(dsl, chunks, runOptions) {
    runOptions = runOptions || {}
    var chunkSize = runOptions.chunkSize || DEFAULT_CHUNK_SIZE
    if (chunkSize <= 0) throw new Error('chunkSize must be positive')
    if (runOptions.onOutput !== undefined && typeof runOptions.onOutput !== 'function') throw new TypeError('onOutput must be a function')
    if (runOptions.onProgress !== undefined && typeof runOptions.onProgress !== 'function') throw new TypeError('onProgress must be a function')

    var id = nextId++
    var state = {
      id: id,
      collectOutput: runOptions.collectOutput !== false,
      outputChunks: [],
      waiters: [],
      done: false,
      cancelled: false,
      onOutput: runOptions.onOutput || null,
      onProgress: runOptions.onProgress || null,
      outputCallbacks: [],
      progressCallbacks: []
    }
    jobs.set(id, state)

    var signal = runOptions.signal
    var abortListener = null
    function throwIfAborted() {
      if (signal && signal.aborted) {
        abortState(state)
        throw new Error('worker pipeline cancelled')
      }
    }
    if (signal && typeof signal.addEventListener === 'function') {
      abortListener = function() { abortState(state) }
      signal.addEventListener('abort', abortListener, { once: true })
    }

    try {
      throwIfAborted()
      send({
        id: id,
        type: 'start',
        dsl: dsl,
        options: {
          chunkSize: chunkSize,
          allowBlocking: runOptions.allowBlocking === true,
          memory: runOptions.memory,
          spillDir: runOptions.spillDir
        }
      })
      await waitFor(state, ['started'])
      for await (var chunk of chunks) {
        throwIfAborted()
        var bytes = ownedBytes(chunk)
        send({ id: id, type: 'push', chunk: bytes }, [bytes.buffer])
        await waitFor(state, ['pushed'])
      }
      throwIfAborted()
      send({ id: id, type: 'finish' })
      var done = await waitFor(state, ['done'])
      await Promise.all(state.outputCallbacks)
      await Promise.all(state.progressCallbacks)
      var output = state.collectOutput ? concatUint8(state.outputChunks) : new Uint8Array(0)
      return makeResult(output, ownedBytes(done.errors), ownedBytes(done.stats), ownedBytes(done.samples))
    } catch (err) {
      if (!state.done) abortState(state)
      throw err
    } finally {
      if (signal && abortListener && typeof signal.removeEventListener === 'function') signal.removeEventListener('abort', abortListener)
      jobs.delete(id)
    }
  }

  async function *dataChunks(data, chunkSize) {
    var bytes = asBytes(data)
    for (var i = 0; i < bytes.length; i += chunkSize) {
      yield bytes.subarray(i, Math.min(i + chunkSize, bytes.length))
    }
  }

  async function *fileChunks(file, chunkSize) {
    if (!file) return
    if (typeof file.stream === 'function') {
      var stream = file.stream()
      if (stream && typeof stream.getReader === 'function') {
        var reader = stream.getReader()
        try {
          for (;;) {
            var next = await reader.read()
            if (next.done) break
            var bytes = asBytes(next.value)
            for (var i = 0; i < bytes.length; i += chunkSize) yield bytes.subarray(i, Math.min(i + chunkSize, bytes.length))
          }
        } finally {
          if (typeof reader.releaseLock === 'function') reader.releaseLock()
        }
        return
      }
      if (stream && (typeof stream[Symbol.asyncIterator] === 'function' || typeof stream[Symbol.iterator] === 'function')) {
        for await (var chunk of stream) {
          var streamBytes = asBytes(chunk)
          for (var j = 0; j < streamBytes.length; j += chunkSize) yield streamBytes.subarray(j, Math.min(j + chunkSize, streamBytes.length))
        }
        return
      }
    }
    if (typeof file.slice === 'function' && typeof file.size === 'number') {
      for (var offset = 0; offset < file.size; offset += chunkSize) {
        var part = file.slice(offset, Math.min(offset + chunkSize, file.size))
        var buf = await part.arrayBuffer()
        yield new Uint8Array(buf)
      }
      return
    }
    throw new TypeError('file must be a Blob/File with stream() or slice()')
  }

  return {
    run: function(dsl, data, runOptions) {
      runOptions = runOptions || {}
      var chunkSize = runOptions.chunkSize || DEFAULT_CHUNK_SIZE
      return runChunks(dsl, dataChunks(data || '', chunkSize), runOptions)
    },
    runChunks: runChunks,
    runFile: function(dsl, file, runOptions) {
      runOptions = runOptions || {}
      var chunkSize = runOptions.chunkSize || DEFAULT_CHUNK_SIZE
      return runChunks(dsl, fileChunks(file, chunkSize), runOptions)
    },
    cancelAll: function() {
      for (var state of jobs.values()) abortState(state)
    },
    dispose: function() {
      this.cancelAll()
      removeListener()
      if (ownsWorker && worker && typeof worker.terminate === 'function') return worker.terminate()
      return undefined
    }
  }
}

if (typeof module !== 'undefined') module.exports = { createWorkerClient: createWorkerClient, runWorkerServer: runWorkerServer }
if (typeof exports !== 'undefined') {
  exports.createWorkerClient = createWorkerClient
  exports.runWorkerServer = runWorkerServer
}

if (typeof require === 'function') {
  try {
    var workerThreads = require('worker_threads')
    if (!workerThreads.isMainThread && workerThreads.parentPort) {
      runWorkerServer().catch(function(err) {
        workerThreads.parentPort.postMessage({ id: null, type: 'error', error: String(err && err.message ? err.message : err) })
      })
    }
  } catch (_) {}
}
