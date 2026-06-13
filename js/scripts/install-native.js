'use strict'

const { spawnSync } = require('child_process')
const path = require('path')

if (process.env.TRANFI_SKIP_NATIVE_BUILD === '1') {
  process.exit(0)
}

spawnSync(process.execPath, [path.join(__dirname, 'sync-csrc.js')], {
  cwd: path.resolve(__dirname, '..'),
  stdio: 'ignore'
})

const result = spawnSync('node-gyp', ['rebuild'], {
  stdio: 'ignore',
  shell: process.platform === 'win32'
})

process.exit(0)
