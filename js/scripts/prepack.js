'use strict'

const fs = require('fs')
const path = require('path')
const { spawnSync } = require('child_process')

const scriptsDir = __dirname
const pkgDir = path.resolve(scriptsDir, '..')
const repoDir = path.resolve(pkgDir, '..')

function run(cmd, args, cwd, extraEnv = {}) {
  const result = spawnSync(cmd, args, {
    cwd,
    stdio: 'inherit',
    env: { ...process.env, ...extraEnv }
  })
  if (result.status !== 0) {
    process.exit(result.status || 1)
  }
}

run(process.execPath, [path.join(scriptsDir, 'sync-csrc.js')], pkgDir)

const wasmFile = path.join(pkgDir, 'wasm', 'tranfi_core.js')
if (!fs.existsSync(wasmFile)) {
  const env = { EM_CACHE: process.env.EM_CACHE || path.join(repoDir, 'build-wasm', '.emcache') }
  if (!process.env.EMSDK_PYTHON && fs.existsSync('/usr/bin/python3')) {
    env.EMSDK_PYTHON = '/usr/bin/python3'
  }
  run('bash', [path.join(repoDir, 'scripts', 'build-wasm.sh')], repoDir, env)
}

const appSource = path.join(repoDir, 'app', 'dist')
const appDest = path.join(pkgDir, 'app')
if (fs.existsSync(appSource)) {
  fs.rmSync(appDest, { recursive: true, force: true })
  fs.cpSync(appSource, appDest, { recursive: true })
  for (const extra of ['wasm', 'lib']) {
    fs.rmSync(path.join(appDest, extra), { recursive: true, force: true })
  }
}

const required = [
  path.join(pkgDir, 'csrc'),
  path.join(pkgDir, 'wasm', 'tranfi_core.js'),
  path.join(pkgDir, 'app', 'index.html')
]

for (const target of required) {
  if (!fs.existsSync(target)) {
    console.error('prepack: missing required publish artifact: ' + target)
    process.exit(1)
  }
}
