"use strict"

const createTranfi = require('./index.js')
const { TranfiTransformError } = require('../src/transform_error.js')

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
    send({
      id: id,
      type: 'error',
      error: String(err && err.message ? err.message : err || 'unknown error'),
      errorName: err && err.name,
      errorCode: err && Number.isInteger(err.code) ? err.code : undefined
    })
  }

  function preparedCancelToken(msg) {
    if (!msg.cancelBuffer) return null
    var flag = new Int32Array(msg.cancelBuffer)
    return tf.createTransformCancelToken({
      sharedFlag: flag,
      limits: msg.limits
    })
  }

  function reportNativePoll(token, id, phase) {
    if (token) {
      token._observeNextPoll(function() {
        send({ id: id, type: 'transform-ready', phase: phase })
      })
    } else {
      send({ id: id, type: 'transform-ready', phase: phase })
    }
  }

  function handleTransformAnalyze(msg) {
    var token = null
    var recipe = null
    var analyzer = null
    var plan = null
    try {
      token = preparedCancelToken(msg)
      recipe = tf.TransformRecipe.fromJSON(msg.recipe, { limits: msg.limits })
      analyzer = recipe.analyzer(msg.schema, {
        limits: msg.limits,
        cancelToken: token
      })
      var tables = Array.isArray(msg.tables) ? msg.tables : []
      for (var i = 0; i < tables.length; i++) {
        reportNativePoll(token, msg.id, 'analyze')
        analyzer.push(tables[i])
      }
      reportNativePoll(token, msg.id, 'finalize')
      plan = analyzer.finalize()
      var planBytes = plan.toBytes({ limits: msg.limits })
      send({ id: msg.id, type: 'transform-plan', planBytes: planBytes }, [planBytes.buffer])
    } finally {
      if (plan) plan.close()
      if (analyzer) analyzer.close()
      if (recipe) recipe.close()
      if (token) token.close()
    }
  }

  function handleTransformApply(msg) {
    var token = null
    var plan = null
    var apply = null
    try {
      token = preparedCancelToken(msg)
      reportNativePoll(token, msg.id, 'import')
      plan = tf.TransformPlan.fromBytes(asBytes(msg.planBytes), {
        limits: msg.limits,
        cancelToken: token
      })
      apply = plan.apply(msg.schema, {
        limits: msg.limits,
        cancelToken: token
      })
      reportNativePoll(token, msg.id, 'apply')
      var result = apply.run(msg.table)
      send({
        id: msg.id,
        type: 'transform-result',
        result: {
          rows: result.rows,
          columns: result.columns,
          data: result.data
        }
      }, [result.data.buffer])
    } finally {
      if (apply) apply.close()
      if (plan) plan.close()
      if (token) token.close()
    }
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
    var handle = tf.createPipeline(planJson, {
      allowFs: options.allowFs === undefined ? Boolean(options.spillDir) : Boolean(options.allowFs),
      allowSpill: Boolean(options.spillDir),
      allowRulesFile: Boolean(options.allowRulesFile),
      workspaceRoot: options.workspaceRoot
    })
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
    if (msg.type === 'transform-analyze') {
      handleTransformAnalyze(msg)
      return
    }
    if (msg.type === 'transform-apply') {
      handleTransformApply(msg)
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
  var workerFactory = typeof options.workerFactory === 'function'
    ? options.workerFactory
    : null
  if (workerFactory) ownsWorker = true
  if ((typeof workerOrUrl === 'string' || (typeof URL !== 'undefined' && workerOrUrl instanceof URL)) && typeof Worker !== 'undefined') {
    workerFactory = function() { return new Worker(workerOrUrl, options.workerOptions) }
    worker = workerFactory()
    ownsWorker = true
  }
  if (!worker || typeof worker.postMessage !== 'function') throw new TypeError('worker must be a Worker instance or URL')

  var nextId = 1
  var jobs = new Map()
  var transfer = options.transfer !== false
  var disposed = false
  var removeListener = null
  var restartPromise = null
  var restartError = null

  function send(msg, transfers) {
    if (!worker) throw new Error('worker client has no live worker; recreate the client')
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
    if (msg.type === 'transform-ready') {
      if (state.onTransformReady) state.onTransformReady(msg.phase)
      return
    }
    if (msg.type === 'error') {
      var err = msg.errorName === 'TranfiTransformError' && Number.isInteger(msg.errorCode)
        ? new TranfiTransformError(msg.errorCode, msg.error || 'prepared transform failed')
        : new Error(msg.error || 'worker pipeline failed')
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

  function attachWorker(nextWorker) {
    if (!nextWorker || typeof nextWorker.postMessage !== 'function') {
      throw new TypeError('workerFactory must return a Worker instance')
    }
    worker = nextWorker
    restartError = null
    removeListener = addWorkerMessageListener(worker, onMessage)
  }

  async function ensureWorker() {
    var pending = restartPromise
    if (pending) await pending
    if (restartError) throw restartError
    if (!worker) throw new Error('worker client has no live worker; recreate the client')
  }

  function terminateForCancellation(error) {
    var oldWorker = worker
    if (!oldWorker || typeof oldWorker.terminate !== 'function') {
      throw new Error('whole-worker cancellation requires an owned terminable worker')
    }
    if (removeListener) {
      removeListener()
      removeListener = null
    }
    worker = null
    for (var state of jobs.values()) {
      if (!state.done) {
        state.done = true
        state.cancelled = true
        rejectWaiters(state, error)
      }
    }
    var terminated
    try {
      terminated = oldWorker.terminate()
    } catch (_) {
      terminated = undefined
    }
    if (!disposed && workerFactory) {
      restartPromise = Promise.resolve(terminated).catch(function() {}).then(function() {
        if (disposed) return
        try {
          attachWorker(workerFactory())
        } catch (error) {
          restartError = error
          worker = null
        }
      }).finally(function() {
        restartPromise = null
      })
      return restartPromise
    }
    return terminated
  }

  attachWorker(worker)

  async function runChunks(dsl, chunks, runOptions) {
    await ensureWorker()
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

  async function runTransformRequest(type, payload, runOptions) {
    runOptions = runOptions || {}
    var signal = runOptions.signal
    if (signal && signal.aborted) {
      throw new TranfiTransformError(109, 'prepared transform worker request cancelled')
    }
    await ensureWorker()
    if (signal && signal.aborted) {
      throw new TranfiTransformError(109, 'prepared transform worker request cancelled')
    }
    var cancelFlag = null
    var terminateOnAbort = false
    if (signal) {
      var sharedCancellation = options.sharedCancellation !== false
        && typeof SharedArrayBuffer !== 'undefined'
        && typeof Atomics !== 'undefined'
      if (!sharedCancellation) {
        if (!workerFactory || !worker || typeof worker.terminate !== 'function') {
          throw new Error('prepared-transform cancellation without SharedArrayBuffer requires a worker URL or workerFactory for termination and recreation')
        }
        terminateOnAbort = true
      } else {
        cancelFlag = new Int32Array(new SharedArrayBuffer(4))
      }
    }
    var id = nextId++
    var state = {
      id: id,
      waiters: [],
      done: false,
      cancelled: false,
      outputCallbacks: [],
      progressCallbacks: [],
      onTransformReady: typeof runOptions.onReady === 'function'
        ? runOptions.onReady
        : null
    }
    jobs.set(id, state)
    var abortListener = null
    if (signal && typeof signal.addEventListener === 'function') {
      abortListener = terminateOnAbort
        ? function() {
            terminateForCancellation(new TranfiTransformError(
              109, 'prepared transform worker request cancelled by worker termination'
            ))
          }
        : function() { Atomics.store(cancelFlag, 0, 1) }
      signal.addEventListener('abort', abortListener, { once: true })
    }
    try {
      send(Object.assign({
        id: id,
        type: type,
        cancelBuffer: cancelFlag ? cancelFlag.buffer : null
      }, payload))
      var responseType = type === 'transform-analyze'
        ? 'transform-plan'
        : 'transform-result'
      return await waitFor(state, [responseType])
    } finally {
      state.done = true
      if (signal && abortListener && typeof signal.removeEventListener === 'function') {
        signal.removeEventListener('abort', abortListener)
      }
      jobs.delete(id)
    }
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
    analyzeTransform: async function(recipe, schema, tables, runOptions) {
      runOptions = runOptions || {}
      var response = await runTransformRequest('transform-analyze', {
        recipe: recipe,
        schema: schema,
        tables: Array.isArray(tables) ? tables : [tables],
        limits: runOptions.limits
      }, runOptions)
      return ownedBytes(response.planBytes)
    },
    applyTransform: async function(planBytes, schema, table, runOptions) {
      runOptions = runOptions || {}
      var response = await runTransformRequest('transform-apply', {
        planBytes: ownedBytes(planBytes),
        schema: schema,
        table: table,
        limits: runOptions.limits
      }, runOptions)
      return {
        rows: response.result.rows,
        columns: response.result.columns,
        data: new Float64Array(
          response.result.data.buffer,
          response.result.data.byteOffset,
          response.result.data.length
        )
      }
    },
    cancelAll: function() {
      for (var state of jobs.values()) abortState(state)
    },
    dispose: function() {
      var pendingRestart = restartPromise
      disposed = true
      this.cancelAll()
      if (removeListener) {
        removeListener()
        removeListener = null
      }
      if (ownsWorker && worker && typeof worker.terminate === 'function') {
        var activeWorker = worker
        worker = null
        return activeWorker.terminate()
      }
      return pendingRestart || undefined
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
