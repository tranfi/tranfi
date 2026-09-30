'use strict'

const { spawnSync } = require('child_process')
const path = require('path')

function describeFailure(label, result, logger) {
  if (result && result.error) {
    logger.error(`tranfi: ${label} failed: ${result.error.message}`)
  } else if (result && result.signal) {
    logger.error(`tranfi: ${label} terminated by signal ${result.signal}`)
  } else {
    logger.error(`tranfi: ${label} exited with status ${result ? result.status : 'unknown'}`)
  }
}

function successful(result) {
  return Boolean(result) && result.error === undefined && result.signal === null
    && result.status === 0
}

function installNative({
  env = process.env,
  execPath = process.execPath,
  platform = process.platform,
  spawn = spawnSync,
  logger = console,
  packageRoot = path.resolve(__dirname, '..')
} = {}) {
  // Installation must leave a working portable runtime even if native fails.
  const wasmResult = spawn(execPath, [path.join(__dirname, 'check-wasm.js')], {
    cwd: packageRoot,
    encoding: 'utf8',
    stdio: 'pipe'
  })
  if (!successful(wasmResult)) {
    describeFailure('packaged WASM validation', wasmResult, logger)
    if (wasmResult && wasmResult.stderr) logger.error(wasmResult.stderr.trim())
    return Number.isInteger(wasmResult && wasmResult.status) && wasmResult.status !== 0
      ? wasmResult.status
      : 1
  }

  if (env.TRANFI_SKIP_NATIVE_BUILD === '1' || platform === 'win32') {
    logger.warn('tranfi: using packaged WASM; native addon build skipped')
    return 0
  }

  const syncResult = spawn(execPath, [path.join(__dirname, 'sync-csrc.js')], {
    cwd: packageRoot,
    stdio: 'inherit'
  })
  if (!successful(syncResult)) {
    describeFailure('C-source synchronization', syncResult, logger)
    return Number.isInteger(syncResult && syncResult.status) && syncResult.status !== 0
      ? syncResult.status
      : 1
  }

  const nodeGyp = env.npm_config_node_gyp || 'node-gyp'
  const nodeGypIsScript = /\.[cm]?js$/i.test(nodeGyp)
  const buildCommand = nodeGypIsScript ? execPath : nodeGyp
  const buildArgs = nodeGypIsScript ? [nodeGyp, 'rebuild'] : ['rebuild']
  const buildResult = spawn(buildCommand, buildArgs, {
    cwd: packageRoot,
    encoding: 'utf8',
    stdio: 'pipe',
    shell: platform === 'win32' && !nodeGypIsScript
  })
  if (!successful(buildResult)) {
    logger.warn(
      'tranfi: native addon build unavailable; using packaged WASM. ' +
      "Prepared transforms: require('tranfi/wasm'). Run npm run build:native for diagnostics."
    )
    return 0
  }

  return 0
}

if (require.main === module) {
  process.exitCode = installNative()
}

module.exports = { installNative }
