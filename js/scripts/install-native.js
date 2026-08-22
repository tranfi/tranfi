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
  if (env.TRANFI_SKIP_NATIVE_BUILD === '1') {
    logger.warn(
      'tranfi: skipping the native addon because TRANFI_SKIP_NATIVE_BUILD=1; ' +
      "use require('tranfi/wasm') explicitly because prepared transforms on " +
      "require('tranfi') need the native addon"
    )
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
    stdio: 'inherit',
    shell: platform === 'win32' && !nodeGypIsScript
  })
  if (!successful(buildResult)) {
    describeFailure('native addon build', buildResult, logger)
    logger.error(
      'tranfi: installation requires a working native toolchain for the default ' +
      "Node entry; set TRANFI_SKIP_NATIVE_BUILD=1 only for explicit 'tranfi/wasm' use"
    )
    return Number.isInteger(buildResult && buildResult.status) && buildResult.status !== 0
      ? buildResult.status
      : 1
  }

  return 0
}

if (require.main === module) {
  process.exitCode = installNative()
}

module.exports = { installNative }
