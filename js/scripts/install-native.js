'use strict'

const { spawnSync } = require('child_process')

if (process.env.TRANFI_SKIP_NATIVE_BUILD === '1') {
  process.exit(0)
}

const result = spawnSync('node-gyp', ['rebuild'], {
  stdio: 'ignore',
  shell: process.platform === 'win32'
})

process.exit(0)
